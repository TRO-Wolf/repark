use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch, RecordBatchOptions, TimestampMicrosecondArray};
use arrow::datatypes::{SchemaRef, TimestampMicrosecondType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use super::schema::external;
use crate::discover::ScanSource;
use crate::error::ConnectError;
use crate::pool::PostgresPool;
use crate::read::postgres::{ScanMeter, ScanOptions, ScanRequest, scan_metered};

pub trait WallClockLocaliser: Send + Sync + fmt::Debug {
    #[allow(clippy::missing_errors_doc)]
    fn localise(
        &self,
        wall: &TimestampMicrosecondArray,
    ) -> crate::error::Result<TimestampMicrosecondArray>;

    fn zone_label(&self) -> Arc<str>;
}

pub(crate) struct ScanPlan {
    pub(crate) source: Arc<str>,
    pub(crate) target: ScanSource,
    pub(crate) schema: SchemaRef,
    pub(crate) pushed: Vec<Expr>,
    pub(crate) residual: Vec<Expr>,
    pub(crate) limit: Option<u64>,
    pub(crate) request: ScanRequest,
    pub(crate) options: ScanOptions,
    pub(crate) pool: Arc<PostgresPool>,
    pub(crate) placed: Vec<(usize, Arc<str>)>,
    pub(crate) localiser: Arc<dyn WallClockLocaliser>,
}

pub struct PostgresScanExec {
    plan: Arc<ScanPlan>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for PostgresScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresScanExec")
            .field("source", &self.plan.source)
            .field("pushed_filters", &self.plan.pushed)
            .field("residual_filters", &self.plan.residual)
            .field("pushed_limit", &self.plan.limit)
            .finish_non_exhaustive()
    }
}

impl PostgresScanExec {
    pub(crate) fn new(plan: ScanPlan) -> PostgresScanExec {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&plan.schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        PostgresScanExec {
            plan: Arc::new(plan),
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(properties),
        }
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.plan.source
    }

    #[must_use]
    pub fn pushed_filters(&self) -> &[Expr] {
        &self.plan.pushed
    }

    #[must_use]
    pub fn residual_filters(&self) -> &[Expr] {
        &self.plan.residual
    }

    #[must_use]
    pub fn pushed_limit(&self) -> Option<u64> {
        self.plan.limit
    }

    #[must_use]
    pub fn request(&self) -> &ScanRequest {
        &self.plan.request
    }

    fn target(&self) -> String {
        match &self.plan.target {
            ScanSource::Relation(relation) => format!("relation={relation}"),
            ScanSource::Query(query) => format!("query={query}"),
        }
    }
}

fn listed(filters: &[Expr]) -> String {
    filters
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl DisplayAs for PostgresScanExec {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plan = &self.plan;
        let projection = plan
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let limit = plan
            .limit
            .map_or_else(|| "None".to_string(), |limit| limit.to_string());
        let (pushed, residual) = (listed(&plan.pushed), listed(&plan.residual));
        if format == DisplayFormatType::TreeRender {
            writeln!(f, "source={}", plan.source)?;
            writeln!(f, "{}", self.target())?;
            writeln!(f, "pushed_filters=[{pushed}]")?;
            writeln!(f, "residual_filters=[{residual}]")?;
            return writeln!(f, "pushed_limit={limit}");
        }
        write!(
            f,
            "PostgresScanExec: source={}, {}, projection=[{projection}], \
             pushed_filters=[{pushed}], residual_filters=[{residual}], pushed_limit={limit}",
            plan.source,
            self.target()
        )?;
        if format == DisplayFormatType::Verbose {
            let statement = plan.request.statement();
            write!(
                f,
                ", remote_sql={}, bound_values={}",
                statement.copy,
                statement.settings.len()
            )?;
        }
        Ok(())
    }
}

fn place(plan: &ScanPlan, batch: &RecordBatch) -> crate::error::Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    for (output, zone) in &plan.placed {
        let refused = |reason| ConnectError::Arrow { message: reason };
        let column = columns
            .get_mut(*output)
            .ok_or_else(|| refused(format!("no column {output} to place")))?;
        let wall = column
            .as_primitive_opt::<TimestampMicrosecondType>()
            .ok_or_else(|| refused(format!("column {output} is not a timestamp")))?;
        let placed = plan.localiser.localise(wall).map_err(|error| match error {
            ConnectError::UnrepresentableValue { index, reason, .. } => {
                ConnectError::UnrepresentableValue {
                    column: plan
                        .schema
                        .fields()
                        .get(*output)
                        .map_or_else(|| "".into(), |field| field.name().as_str().into()),
                    postgres_type: "timestamp",
                    index,
                    reason,
                }
            }
            other => other,
        })?;
        *column = Arc::new(placed.with_timezone(Arc::clone(zone)));
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(&plan.schema), columns, &options).map_err(
        |error| ConnectError::Arrow {
            message: error.to_string(),
        },
    )
}

impl ExecutionPlan for PostgresScanExec {
    fn name(&self) -> &'static str {
        "PostgresScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            return Ok(self);
        }
        Err(DataFusionError::Internal(
            "PostgresScanExec has no children".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "PostgresScanExec has one partition, not {partition}"
            )));
        }
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let meter = ScanMeter {
            bytes_received: MetricBuilder::new(&self.metrics).counter("bytes_received", partition),
            time_to_first_byte: MetricBuilder::new(&self.metrics)
                .subset_time("time_to_first_byte", partition),
            decode: baseline.elapsed_compute().clone(),
        };
        let reservation = MemoryConsumer::new("PostgresScan").register(context.memory_pool());
        let plan = Arc::clone(&self.plan);
        let rows = scan_metered(
            Arc::clone(&plan.pool),
            plan.request.clone(),
            plan.options,
            meter,
        );
        let placed = Box::pin(rows.map(move |batch| {
            let batch = batch
                .and_then(|batch| place(&plan, &batch))
                .map_err(external)?;
            reservation.try_resize(batch.get_array_memory_size())?;
            Ok(batch.record_output(&baseline))
        }));
        let stream = futures::stream::unfold(Some(placed), |state| async move {
            let mut placed = state?;
            let next = placed.next().await?;
            let rest = next.is_ok().then_some(placed);
            Some((next, rest))
        });
        let schema = Arc::clone(&self.plan.schema);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
