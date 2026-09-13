use std::{fmt, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext, context::SessionState},
    physical_plan::{
        DisplayAs, DisplayFormatType,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
    prelude::Expr,
};
use datafusion_datasource::sink::DataSink;
use futures::TryStreamExt as _;

use crate::{kernel::EagerSnapshot, logstore::LogStoreRef, operations::delete::DeleteBuilder};

/// A DataFusion sink that performs a Delta DELETE when its execution plan is consumed.
#[derive(Debug)]
pub struct DeltaDeleteSink {
    log_store: LogStoreRef,
    snapshot: EagerSnapshot,
    predicate: Option<Expr>,
    session: SessionState,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaDeleteSink {
    pub fn new(
        log_store: LogStoreRef,
        snapshot: EagerSnapshot,
        predicate: Option<Expr>,
        session: SessionState,
        schema: SchemaRef,
    ) -> Self {
        Self {
            log_store,
            schema,
            snapshot,
            predicate,
            session,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait::async_trait]
impl DataSink for DeltaDeleteSink {
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

        let mut delete = DeleteBuilder::new(self.log_store.clone(), Some(self.snapshot.clone()))
            .with_session_state(Arc::new(self.session.clone()))
            .with_exact_row_count();

        if let Some(predicate) = &self.predicate {
            delete = delete.with_predicate(predicate.clone());
        }

        let (_, metrics) = delete
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let count = metrics.num_deleted_rows.ok_or_else(|| {
            DataFusionError::Execution("DELETE did not produce an exact row count".into())
        })?;
        u64::try_from(count)
            .map_err(|_| DataFusionError::Execution("DELETE row count did not fit u64".into()))
    }
}

impl DisplayAs for DeltaDeleteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaDeleteSink")
    }
}
