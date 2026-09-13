use std::{fmt, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, TaskContext,
        context::SessionState,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
    prelude::Expr,
};
use datafusion_datasource::sink::DataSink;
use futures::TryStreamExt as _;

use crate::{
    kernel::EagerSnapshot, logstore::LogStoreRef, operations::update::UpdateBuilder,
};

/// A DataFusion sink that performs a Delta UPDATE when its execution plan is consumed.
#[derive(Debug)]
pub struct DeltaUpdateSink {
    log_store: LogStoreRef,
    snapshot: EagerSnapshot,
    assignments: Vec<(String, Expr)>,
    predicate: Option<Expr>,
    session: SessionState,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaUpdateSink {
    pub fn new(
        log_store: LogStoreRef,
        snapshot: EagerSnapshot,
        assignments: Vec<(String, Expr)>,
        predicate: Option<Expr>,
        session: SessionState,
    ) -> Self {
        Self {
            log_store,
            schema: snapshot.read_schema(),
            snapshot,
            assignments,
            predicate,
            session,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait::async_trait]
impl DataSink for DeltaUpdateSink {
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
    ) -> datafusion::common::Result<u64> {
        // DataSinkExec owns the execution boundary. Drain its (empty) input before
        // starting the transaction so planning can never mutate the table.
        while data.try_next().await?.is_some() {}

        let mut update = UpdateBuilder::new(
            self.log_store.clone(),
            Some(self.snapshot.clone()),
        )
        .with_session_state(Arc::new(self.session.clone()));

        if let Some(predicate) = &self.predicate {
            update = update.with_predicate(predicate.clone());
        }
        for (column, expression) in &self.assignments {
            update = update.with_update(column.clone(), expression.clone());
        }

        let (_, metrics) = update
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        u64::try_from(metrics.num_updated_rows)
            .map_err(|_| DataFusionError::Execution("UPDATE row count did not fit u64".into()))
    }
}

impl DisplayAs for DeltaUpdateSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaUpdateSink")
    }
}
