use std::{fmt, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, TaskContext,
        context::{SessionContext, SessionState},
    },
    physical_plan::{
        DisplayAs, DisplayFormatType,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
    prelude::Expr,
};
use datafusion_datasource::sink::DataSink;
use futures::TryStreamExt as _;

use super::next::DeltaScan;
use crate::{
    delta_datafusion::DeltaScanConfig,
    kernel::EagerSnapshot,
    logstore::LogStoreRef,
    operations::delete::DeleteBuilder,
};

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
    ) -> Self {
        Self {
            log_store,
            schema: snapshot.read_schema(),
            snapshot,
            predicate,
            session,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    async fn count_matching_rows(&self) -> datafusion::common::Result<u64> {
        let provider = DeltaScan::new(
            self.snapshot.clone(),
            DeltaScanConfig::new_from_session(&self.session),
        )?
        .with_log_store(self.log_store.clone());
        let context = SessionContext::new_with_state(self.session.clone());
        let mut dataframe = context.read_table(Arc::new(provider))?;
        if let Some(predicate) = &self.predicate {
            dataframe = dataframe.filter(predicate.clone())?;
        }

        let mut stream = dataframe.execute_stream().await?;
        let mut count = 0_u64;
        while let Some(batch) = stream.try_next().await? {
            count = count
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| DataFusionError::Execution("DELETE row count overflowed u64".into()))?;
        }
        Ok(count)
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

        let mut delete = DeleteBuilder::new(
            self.log_store.clone(),
            Some(self.snapshot.clone()),
        )
        .with_session_state(Arc::new(self.session.clone()));

        if let Some(predicate) = &self.predicate {
            delete = delete.with_predicate(predicate.clone());
        }

        let (_, metrics) = delete
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        match metrics.num_deleted_rows {
            Some(count) => u64::try_from(count).map_err(|_| {
                DataFusionError::Execution("DELETE row count did not fit u64".into())
            }),
            None => self.count_matching_rows().await,
        }
    }
}

impl DisplayAs for DeltaDeleteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaDeleteSink")
    }
}
