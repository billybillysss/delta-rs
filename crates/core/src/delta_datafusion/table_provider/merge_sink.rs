use std::{fmt, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DFSchema, Result},
    execution::{
        SendableRecordBatchStream, TaskContext,
        context::{SessionContext, SessionState},
    },
    logical_expr::{
        Expr, TableProviderFilterPushDown, TableType,
        dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind},
    },
    physical_expr::PhysicalExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan,
        limit::GlobalLimitExec,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        projection::ProjectionExec,
    },
};
use datafusion_datasource::sink::DataSink;
use futures::TryStreamExt as _;

use crate::{
    kernel::EagerSnapshot, logstore::LogStoreRef, operations::merge::MergeBuilder,
};

/// Exposes a pre-planned DataFusion source as a logical table without collecting it.
#[derive(Debug)]
struct ExecutionPlanTableProvider {
    source: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlanTableProvider {
    fn new(source: Arc<dyn ExecutionPlan>) -> Self {
        Self { source }
    }
}

#[async_trait::async_trait]
impl TableProvider for ExecutionPlanTableProvider {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plan = self.source.clone();

        if let Some(projection) = projection {
            let current_projection = (0..plan.schema().fields().len()).collect::<Vec<_>>();
            if projection != &current_projection {
                let schema = DFSchema::try_from(plan.schema().as_ref().clone())?;
                let fields: Result<Vec<(Arc<dyn PhysicalExpr>, String)>> = projection
                    .iter()
                    .map(|index| {
                        let (qualifier, field) = schema.qualified_field(*index);
                        session
                            .create_physical_expr(
                                Expr::Column(datafusion::common::Column::from((qualifier, field))),
                                &schema,
                            )
                            .map(|expr| (expr, field.name().clone()))
                    })
                    .collect();
                plan = Arc::new(ProjectionExec::try_new(fields?, plan)?);
            }
        }

        if let Some(limit) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit)));
        }

        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }
}

/// A DataFusion sink that performs a Delta MERGE when its execution plan is consumed.
#[derive(Debug)]
pub struct DeltaMergeSink {
    log_store: LogStoreRef,
    snapshot: EagerSnapshot,
    source: Arc<dyn ExecutionPlan>,
    on: Expr,
    clauses: Vec<MergeIntoClause>,
    source_alias: Option<String>,
    target_alias: Option<String>,
    session: SessionState,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaMergeSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_store: LogStoreRef,
        snapshot: EagerSnapshot,
        source: Arc<dyn ExecutionPlan>,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
        source_alias: Option<String>,
        target_alias: Option<String>,
        session: SessionState,
    ) -> Self {
        Self {
            log_store,
            schema: snapshot.read_schema(),
            snapshot,
            source,
            on,
            clauses,
            source_alias,
            target_alias,
            session,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    async fn execute_merge(&self) -> Result<u64> {
        let context = SessionContext::new_with_state(self.session.clone());
        let source = context.read_table(Arc::new(ExecutionPlanTableProvider::new(
            self.source.clone(),
        )))?;
        let mut merge = MergeBuilder::new(
            self.log_store.clone(),
            Some(self.snapshot.clone()),
            self.on.clone(),
            source,
        )
        .with_session_state(Arc::new(self.session.clone()))
        .with_streaming(true);

        if let Some(alias) = &self.source_alias {
            merge = merge.with_source_alias(alias);
        }
        if let Some(alias) = &self.target_alias {
            merge = merge.with_target_alias(alias);
        }

        for clause in self.clauses.clone() {
            let MergeIntoClause {
                kind,
                predicate,
                action,
            } = clause;
            let kind = kind.canonical();
            merge = match (kind, action) {
                (MergeIntoClauseKind::Matched, MergeIntoAction::Update(assignments)) => merge
                    .when_matched_update(move |mut update| {
                        if let Some(predicate) = predicate {
                            update = update.predicate(predicate);
                        }
                        for (column, expression) in assignments {
                            update = update.update(column, expression);
                        }
                        update
                    }),
                (MergeIntoClauseKind::Matched, MergeIntoAction::Delete) => merge
                    .when_matched_delete(move |mut delete| {
                        if let Some(predicate) = predicate {
                            delete = delete.predicate(predicate);
                        }
                        delete
                    }),
                (MergeIntoClauseKind::NotMatchedByTarget, MergeIntoAction::Insert {
                    columns,
                    values,
                }) => {
                    let columns = if columns.is_empty() {
                        self.schema
                            .fields()
                            .iter()
                            .map(|field| field.name().clone())
                            .collect()
                    } else {
                        columns
                    };
                    if columns.len() != values.len() {
                        return Err(datafusion::error::DataFusionError::Plan(format!(
                            "MERGE INSERT has {} columns but {} values",
                            columns.len(),
                            values.len()
                        )));
                    }
                    merge.when_not_matched_insert(move |mut insert| {
                        if let Some(predicate) = predicate {
                            insert = insert.predicate(predicate);
                        }
                        for (column, expression) in columns.into_iter().zip(values) {
                            insert = insert.set(column, expression);
                        }
                        insert
                    })
                }
                (MergeIntoClauseKind::NotMatchedBySource, MergeIntoAction::Update(assignments)) => {
                    merge.when_not_matched_by_source_update(move |mut update| {
                        if let Some(predicate) = predicate {
                            update = update.predicate(predicate);
                        }
                        for (column, expression) in assignments {
                            update = update.update(column, expression);
                        }
                        update
                    })
                }
                (MergeIntoClauseKind::NotMatchedBySource, MergeIntoAction::Delete) => merge
                    .when_not_matched_by_source_delete(move |mut delete| {
                        if let Some(predicate) = predicate {
                            delete = delete.predicate(predicate);
                        }
                        delete
                    }),
                (kind, action) => {
                    return Err(datafusion::error::DataFusionError::Plan(format!(
                        "unsupported MERGE clause combination: {kind:?} with {action:?}"
                    )));
                }
            }
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
        }

        let (_, metrics) = merge
            .await
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
        let count = metrics
            .num_target_rows_inserted
            .checked_add(metrics.num_target_rows_updated)
            .and_then(|count| count.checked_add(metrics.num_target_rows_deleted))
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "MERGE affected row count overflowed usize".into(),
                )
            })?;
        u64::try_from(count).map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "MERGE affected row count did not fit u64".into(),
            )
        })
    }
}

#[async_trait::async_trait]
impl DataSink for DeltaMergeSink {
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        while data.try_next().await?.is_some() {}
        self.execute_merge().await
    }
}

impl DisplayAs for DeltaMergeSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaMergeSink")
    }
}
