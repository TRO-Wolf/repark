use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, collect};

pub type ChangelogRowTransform = Arc<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>;

#[derive(Clone)]
pub struct ChangelogViewProvider {
    source: Arc<dyn TableProvider>,
    transform: ChangelogRowTransform,
}

impl Debug for ChangelogViewProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChangelogViewProvider")
            .field("schema", &self.source.schema())
            .finish_non_exhaustive()
    }
}

impl ChangelogViewProvider {
    #[must_use]
    pub fn new(source: Arc<dyn TableProvider>, transform: ChangelogRowTransform) -> Self {
        Self { source, transform }
    }
}

#[async_trait]
impl TableProvider for ChangelogViewProvider {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source_plan = self.source.scan(state, None, &[], None).await?;
        let full_schema = self.schema();
        let output_schema = match projection {
            None => Arc::clone(&full_schema),
            Some(indices) => Arc::new(full_schema.project(indices)?),
        };
        let partition = Arc::new(ChangelogViewPartition {
            source_plan,
            transform: Arc::clone(&self.transform),
            full_schema,
            projection: projection.cloned(),
            schema: Arc::clone(&output_schema),
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![partition],
            None,
            vec![],
            false,
            limit,
        )?))
    }
}

struct ChangelogViewPartition {
    source_plan: Arc<dyn ExecutionPlan>,
    transform: ChangelogRowTransform,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
}

impl Debug for ChangelogViewPartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChangelogViewPartition")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for ChangelogViewPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let source_plan = Arc::clone(&self.source_plan);
        let transform = Arc::clone(&self.transform);
        let full_schema = Arc::clone(&self.full_schema);
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let stream = futures::stream::once(async move {
            let batches = collect(source_plan, ctx).await?;
            let raw = concat_batches(&full_schema, &batches)?;
            let transformed = transform(&raw)?;
            match projection {
                None => Ok(transformed),
                Some(indices) => transformed.project(&indices).map_err(Into::into),
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}
