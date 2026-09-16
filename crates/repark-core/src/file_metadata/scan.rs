use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;

#[derive(Clone, Debug)]
pub enum FileKind {
    Parquet,
    Csv { options: HashMap<String, String> },
    Json { options: HashMap<String, String> },
    Text,
}

#[derive(Debug)]
pub(crate) struct FileMetadataScan {
    inner: Arc<dyn TableProvider>,
    kind: FileKind,
}

impl FileMetadataScan {
    pub(crate) fn new(inner: Arc<dyn TableProvider>, kind: FileKind) -> Self {
        Self { inner, kind }
    }

    pub(crate) fn kind(&self) -> &FileKind {
        &self.kind
    }

    pub(crate) fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[async_trait::async_trait]
impl TableProvider for FileMetadataScan {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}

pub(crate) struct FileHit {
    pub(crate) read_path: String,
    pub(crate) file_path: String,
    pub(crate) file_name: String,
    pub(crate) file_size: i64,
    pub(crate) modification_nanos: i64,
    pub(crate) partition_names: Vec<String>,
    pub(crate) partition_values: Vec<ScalarValue>,
}

pub(crate) fn collect_file_hits(plan: &Arc<dyn ExecutionPlan>) -> Vec<FileHit> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    collect_file_hits_inner(plan, &mut seen, &mut out);
    out
}

fn collect_file_hits_inner(
    plan: &Arc<dyn ExecutionPlan>,
    seen: &mut HashSet<String>,
    out: &mut Vec<FileHit>,
) {
    if let Some(exec) = plan.as_ref().downcast_ref::<DataSourceExec>()
        && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        let url: &url::Url = config.object_store_url.as_ref();
        let store_url = url.as_str().to_string();
        let local = url.scheme() == "file";
        let partition_names = config
            .table_partition_cols()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        for group in &config.file_groups {
            for file in group.iter() {
                let location: &str = file.object_meta.location.as_ref();
                if !seen.insert(location.to_string()) {
                    continue;
                }
                let base = store_url.trim_end_matches('/');
                let joined = format!("{base}/{}", location.trim_start_matches('/'));
                let (read_path, file_path) = if local && location.starts_with('/') {
                    (location.to_string(), format!("file:{location}"))
                } else if local {
                    let absolute = joined.strip_prefix("file://").unwrap_or(joined.as_str());
                    let absolute = absolute.strip_prefix("file:").unwrap_or(absolute);
                    (joined.clone(), format!("file:{absolute}"))
                } else {
                    (joined.clone(), joined)
                };
                let file_name = location.rsplit('/').next().unwrap_or(location).to_string();
                let file_size = i64::try_from(file.object_meta.size).unwrap_or(i64::MAX);
                let modification_nanos = file
                    .object_meta
                    .last_modified
                    .timestamp_nanos_opt()
                    .unwrap_or(0);
                out.push(FileHit {
                    read_path,
                    file_path,
                    file_name,
                    file_size,
                    modification_nanos,
                    partition_names: partition_names.clone(),
                    partition_values: file.partition_values.clone(),
                });
            }
        }
    }
    for child in plan.children() {
        collect_file_hits_inner(child, seen, out);
    }
}
