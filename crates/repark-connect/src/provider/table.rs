use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use super::catalog::{Mounted, PostgresSource};
use super::scan::{PostgresScanExec, ScanPlan};
use crate::copy_binary::{BatchLimits, DEFAULT_BATCH_BYTES, DEFAULT_BATCH_ROWS};
use crate::discover::{ResolvedSource, ScanColumn};
use crate::pushdown::Pushdown;
use crate::read::postgres::{MAX_PARAM_SLOTS, ScanOptions, ScanRequest};
use crate::types::postgres::PostgresMapping;

fn places(column: &ScanColumn) -> bool {
    column.planned.mapping() == PostgresMapping::Timestamp
}

#[derive(Debug)]
pub struct PostgresTable {
    source: Arc<PostgresSource>,
    mounted: Arc<Mounted>,
    resolved: Arc<ResolvedSource>,
    schema: SchemaRef,
    zone: Option<Arc<str>>,
    pushdown: Pushdown,
}

impl PostgresTable {
    pub(crate) fn new(
        source: Arc<PostgresSource>,
        mounted: Arc<Mounted>,
        resolved: ResolvedSource,
    ) -> PostgresTable {
        let zone = (!mounted.settings.prefer_timestamp_ntz && resolved.columns.iter().any(places))
            .then(|| source.localiser().zone_label());
        let fields = resolved
            .columns
            .iter()
            .map(|column| match &zone {
                Some(zone) if places(column) => column.planned.field().with_data_type(
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(zone))),
                ),
                _ => column.planned.field(),
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let pushdown = Pushdown::new(&resolved, &schema, mounted.settings.pushdown_predicate);
        PostgresTable {
            source,
            mounted,
            resolved: Arc::new(resolved),
            schema,
            zone,
            pushdown,
        }
    }

    #[must_use]
    pub fn resolved(&self) -> &Arc<ResolvedSource> {
        &self.resolved
    }

    #[must_use]
    pub fn pushdown(&self) -> &Pushdown {
        &self.pushdown
    }
}

#[async_trait]
impl TableProvider for PostgresTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| self.pushdown.support(filter))
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let (pushed, residual) = self.pushdown.split(filters);
        let request = ScanRequest::new(Arc::clone(&self.resolved))
            .project(&projection)
            .ok_or_else(|| {
                DataFusionError::Internal(format!("projection {projection:?} is out of range"))
            })?;
        let request = self.pushdown.push(&pushed, request).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the filters pushed to Postgres source `{}` bind more than {MAX_PARAM_SLOTS} \
                 values; narrow the filter or set `pushdown_predicate` to false (registry row \
                 CONNECT-DECL-pg-bound-values in docs/spark-sql-iceberg-parity.md)",
                self.source.name()
            ))
        })?;
        let limit = limit
            .filter(|_| self.mounted.settings.pushdown_limit && residual.is_empty())
            .and_then(|limit| i64::try_from(limit).ok())
            .and_then(|limit| u64::try_from(limit).ok());
        let request = match limit {
            Some(limit) => request.limit(limit),
            None => request,
        };
        let settings = &self.mounted.settings;
        let rows = settings.batch_rows.unwrap_or_else(|| {
            NonZeroUsize::new(state.config().batch_size()).unwrap_or(DEFAULT_BATCH_ROWS)
        });
        let options = ScanOptions {
            read_timeout: settings.read_timeout,
            batch: BatchLimits::new(rows, DEFAULT_BATCH_BYTES),
        };
        let schema = Arc::new(self.schema.project(&projection)?);
        let placed = self.zone.as_ref().map_or_else(Vec::new, |zone| {
            projection
                .iter()
                .enumerate()
                .filter(|(_, index)| self.resolved.columns.get(**index).is_some_and(places))
                .map(|(output, _)| (output, Arc::clone(zone)))
                .collect()
        });
        Ok(Arc::new(PostgresScanExec::new(ScanPlan {
            source: Arc::clone(self.source.name()),
            target: self.resolved.source.clone(),
            schema,
            pushed,
            residual,
            limit,
            request,
            options,
            pool: Arc::clone(&self.mounted.pool),
            placed,
            localiser: Arc::clone(self.source.localiser()),
        })))
    }
}
