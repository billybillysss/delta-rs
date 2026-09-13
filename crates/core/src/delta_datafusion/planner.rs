//! Custom planners for datafusion so that you can convert custom nodes, can be used
//! to trace custom metrics in an operation
//!
//! # Example
//!
//! #[derive(Clone)]
//! struct MergeMetricExtensionPlanner {}
//!
//! #[macro@async_trait]
//! impl ExtensionPlanner for MergeMetricExtensionPlanner {
//!     async fn plan_extension(
//!         &self,
//!         planner: &dyn PhysicalPlanner,
//!         node: &dyn UserDefinedLogicalNode,
//!         _logical_inputs: &[&LogicalPlan],
//!         physical_inputs: &[Arc<dyn ExecutionPlan>],
//!         session_state: &SessionState,
//!     ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {}
//!
//! let merge_planner = DeltaPlanner::<MergeMetricExtensionPlanner> {
//!     extension_planner: MergeMetricExtensionPlanner {}
//! };
//!
//! let state = state.with_query_planner(Arc::new(merge_planner));
use std::{
    any::TypeId,
    sync::{Arc, LazyLock},
};

use arrow_array::{RecordBatch, UInt64Array};
use async_trait::async_trait;
use datafusion::datasource::{memory::MemorySourceConfig, source_as_provider};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode, WriteOp};
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::{
    catalog::Session,
    execution::context::QueryPlanner,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner},
};

use crate::delta_datafusion::data_validation::DataValidationExtensionPlanner;
use crate::delta_datafusion::{DataFusionResult, DeltaScanNext};
use crate::operations::delete::DeleteMetricExtensionPlanner;
use crate::operations::merge::MergeMetricExtensionPlanner;
use crate::operations::update::UpdateMetricExtensionPlanner;
use crate::operations::write::metrics::WriteMetricExtensionPlanner;

static DELTA_EXTENSION_PLANNERS: LazyLock<Vec<Arc<dyn ExtensionPlanner + Send + Sync>>> =
    LazyLock::new(|| {
        vec![
            MergeMetricExtensionPlanner::new(),
            WriteMetricExtensionPlanner::new(),
            DeleteMetricExtensionPlanner::new(),
            UpdateMetricExtensionPlanner::new(),
            DataValidationExtensionPlanner::new(),
        ]
    });

static DELTA_PLANNER: LazyLock<Arc<DeltaPlanner>> = LazyLock::new(|| Arc::new(DeltaPlanner));

/// Deltaplanner
#[derive(Debug)]
pub struct DeltaPlanner;

impl DeltaPlanner {
    /// Return the shared, lazily-initialized [`DeltaPlanner`] instance.
    ///
    /// The planner is stateless, so a single cached instance is reused rather than
    /// allocating a new one per query.
    pub fn new() -> Arc<Self> {
        DELTA_PLANNER.clone()
    }
}

#[async_trait]
impl QueryPlanner for DeltaPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if let LogicalPlan::Dml(dml) = logical_plan
            && dml.op == WriteOp::Delete
            && source_as_provider(&dml.target)
                .is_ok_and(|provider| provider.as_ref().type_id() == TypeId::of::<DeltaScanNext>())
        {
            if contains_limit(&dml.input) {
                return Err(datafusion::error::DataFusionError::Plan(
                    "DELETE with LIMIT is not supported".to_string(),
                ));
            }

            if matches!(dml.input.as_ref(), LogicalPlan::EmptyRelation(empty) if !empty.produce_one_row)
            {
                let schema = Arc::new(dml.output_schema.as_arrow().clone());
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from(vec![0]))],
                )?;
                return Ok(MemorySourceConfig::try_new_exec(
                    &[vec![batch]],
                    schema,
                    None,
                )?);
            }
        }

        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![DeltaExtensionPlanner::new()],
        )));
        planner.create_physical_plan(logical_plan, session).await
    }
}

fn contains_limit(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Limit(_)) || plan.inputs().into_iter().any(contains_limit)
}

/// Extension [`PhysicalPlanner`](datafusion::physical_planner::PhysicalPlanner) that knows
/// how to lower delta-rs custom logical nodes into executable physical plans.
pub struct DeltaExtensionPlanner;

impl DeltaExtensionPlanner {
    /// Construct a new extension planner.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ExtensionPlanner for DeltaExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        for ext_planner in DELTA_EXTENSION_PLANNERS.iter() {
            if let Some(plan) = ext_planner
                .plan_extension(
                    planner,
                    node,
                    logical_inputs,
                    physical_inputs,
                    session_state,
                    planning_ctx,
                )
                .await?
            {
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }
}
