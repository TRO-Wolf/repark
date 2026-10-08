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
use super::partitioned::Partitioned;
use super::scan::{PostgresScanExec, ScanPlan};
use crate::copy_binary::{BatchLimits, DEFAULT_BATCH_BYTES, DEFAULT_BATCH_ROWS};
use crate::discover::{ResolvedSource, ScanColumn};
use crate::error::ConnectError;
use crate::partition::{PartitionRefusal, PartitionSpec, Stride, stride_cuts, strides};
use crate::pushdown::Pushdown;
use crate::read::postgres::{MAX_PARAM_SLOTS, ScanOptions, ScanRequest};
use crate::types::postgres::PostgresMapping;

fn places(column: &ScanColumn) -> bool {
    column.planned.mapping() == PostgresMapping::Timestamp
}

fn refused<T>(refusal: PartitionRefusal) -> crate::error::Result<T> {
    Err(ConnectError::PartitionedRead { refusal })
}

fn partition_column(resolved: &ResolvedSource, wanted: &str) -> crate::error::Result<usize> {
    let names: Vec<&str> = resolved
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let quoted = wanted
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .map(|inner| inner.replace("\"\"", "\""));
    let exact = quoted.as_deref().unwrap_or(wanted);
    if let Some(index) = names.iter().position(|name| *name == exact) {
        return Ok(index);
    }
    let folded: Vec<usize> = names
        .iter()
        .enumerate()
        .filter(|(_, name)| quoted.is_none() && name.eq_ignore_ascii_case(wanted))
        .map(|(index, _)| index)
        .collect();
    match folded.as_slice() {
        [index] => Ok(*index),
        [] => refused(PartitionRefusal::ColumnNotFound {
            column: wanted.to_string(),
            columns: names.iter().map(ToString::to_string).collect(),
        }),
        several => refused(PartitionRefusal::AmbiguousColumn {
            column: wanted.to_string(),
            matches: several
                .iter()
                .filter_map(|index| names.get(*index))
                .map(ToString::to_string)
                .collect(),
        }),
    }
}

fn partition_type(column: &ScanColumn) -> crate::error::Result<()> {
    let found = match column.planned.mapping() {
        PostgresMapping::Int16 | PostgresMapping::Int32 | PostgresMapping::Int64 => return Ok(()),
        PostgresMapping::Boolean => "boolean",
        PostgresMapping::Binary => "binary",
        PostgresMapping::Utf8
        | PostgresMapping::Uuid
        | PostgresMapping::Json
        | PostgresMapping::Jsonb
        | PostgresMapping::ServerText
        | PostgresMapping::Declared { .. } => "string",
        PostgresMapping::Float32
        | PostgresMapping::Float64
        | PostgresMapping::Numeric(_)
        | PostgresMapping::Date
        | PostgresMapping::Timestamp
        | PostgresMapping::Timestamptz => {
            return refused(PartitionRefusal::DeclaredColumnType {
                column: column.name.as_str().to_string(),
                postgres_type: column.planned.postgres_type(),
            });
        }
    };
    refused(PartitionRefusal::ColumnType { found })
}

#[derive(Debug)]
struct Partitioning {
    column: usize,
    name: Arc<str>,
    strides: Vec<Stride>,
}

#[derive(Debug)]
pub struct PostgresTable {
    source: Arc<PostgresSource>,
    mounted: Arc<Mounted>,
    resolved: Arc<ResolvedSource>,
    schema: SchemaRef,
    zone: Option<Arc<str>>,
    pushdown: Pushdown,
    partitioning: Option<Partitioning>,
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
            partitioning: None,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn partitioned(mut self, spec: &PartitionSpec) -> crate::error::Result<PostgresTable> {
        let column = partition_column(&self.resolved, &spec.column)?;
        let scanned = self.resolved.columns.get(column);
        scanned.map_or(Ok(()), partition_type)?;
        let (lower, upper) = spec.bounds()?;
        let cuts = stride_cuts(lower, upper, spec.num_partitions)?;
        self.partitioning = scanned
            .filter(|_| !cuts.is_empty())
            .map(|scanned| Partitioning {
                column,
                name: scanned.name.as_str().into(),
                strides: strides(&cuts),
            });
        Ok(self)
    }

    #[must_use]
    pub fn strides(&self) -> &[Stride] {
        self.partitioning
            .as_ref()
            .map_or(&[], |partitioning| &partitioning.strides)
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
        let too_many = || {
            DataFusionError::Plan(format!(
                "the filters and partition bounds pushed to Postgres source `{}` bind more than \
                 {MAX_PARAM_SLOTS} values; narrow the filter or set `pushdown_predicate` to \
                 false (registry row CONNECT-DECL-pg-bound-values in \
                 docs/spark-sql-iceberg-parity.md)",
                self.source.name()
            ))
        };
        let settings = &self.mounted.settings;
        let partition = match &self.partitioning {
            None => None,
            Some(partitioning) => Some(Partitioned {
                column: Arc::clone(&partitioning.name),
                strides: partitioning
                    .strides
                    .iter()
                    .map(|stride| request.clone().stride(partitioning.column, *stride))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(too_many)?,
                max_connections: partitioning.strides.len().min(settings.pool_max_size),
            }),
        };
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
            partition,
        })))
    }
}
