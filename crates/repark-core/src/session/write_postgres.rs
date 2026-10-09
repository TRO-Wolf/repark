//! One Postgres sink write shared by the SQL doors and the Spark writer.
//!
//! The driver resolves the target with C-2 discovery, checks the frame width
//! against the written columns, casts each batch to the encoder's Arrow types,
//! turns session-zone instants back into wall clocks for `timestamp` columns,
//! and commits. Both SQL doors and the `df.write.jdbc` binding call it; step
//! 1's selector answers the taken path from the returned report.

use std::collections::BTreeMap;
#[cfg(feature = "postgres")]
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;

use super::ReparkSession;
use crate::catalog_state::CatalogRegistry;

#[cfg(feature = "postgres")]
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, TimestampMicrosecondArray};
#[cfg(feature = "postgres")]
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
#[cfg(feature = "postgres")]
use repark_common::{SourceKind, spark_error};
#[cfg(feature = "postgres")]
use repark_connect::postgres::PostgresMapping;
#[cfg(feature = "postgres")]
use repark_connect::{PostgresSettings, ResolvedSource};

/// A Postgres write target: a mounted source relation or a JDBC-style URL.
#[derive(Debug, Clone)]
pub enum PostgresWriteTarget {
    /// A `[database.postgres.<source>]` relation by source, schema and table.
    Mounted {
        /// The mounted source name.
        source: String,
        /// The schema holding the table.
        schema: String,
        /// The table receiving the rows.
        table: String,
    },
    /// A URL write: the URL, the `dbtable` value and the remaining properties.
    Url {
        /// The JDBC-style URL.
        url: String,
        /// The `dbtable` value; a parenthesised subquery refuses.
        dbtable: String,
        /// The connection properties, `url` and `dbtable` aside.
        properties: BTreeMap<String, String>,
    },
}

/// The requested sink path: step 1's selector answers the taken one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresWritePath {
    /// COPY FROM STDIN BINARY, with the row fallback for uncarriable columns.
    Bulk,
    /// Multi-row INSERT for every column.
    Row,
}

/// What one write did: the taken path and the committed row count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresWriteReport {
    /// The path the selector took.
    pub path: PostgresWritePath,
    /// The rows committed.
    pub rows: u64,
}

/// One Postgres write: its target, its columns and its requested path.
#[derive(Debug, Clone)]
pub struct PostgresWrite {
    /// Where the rows go.
    pub target: PostgresWriteTarget,
    /// The written columns in batch order, or `None` for all in table order.
    pub columns: Option<Vec<String>>,
    /// Whether the door matches column names case-insensitively.
    pub case_insensitive: bool,
    /// The requested sink path.
    pub path: PostgresWritePath,
}

impl ReparkSession {
    /// Write one planned frame to Postgres through step 1's selector.
    ///
    /// # Errors
    ///
    /// Connection, discovery, arity, column-resolution, cast, encode and
    /// commit failures, each in the engine's DataFusion taxonomy.
    pub async fn write_postgres(
        &self,
        frame: DataFrame,
        write: PostgresWrite,
    ) -> Result<PostgresWriteReport> {
        let zone = self.session_time_zone().id().to_string();
        execute_postgres_write(&self.catalogs_snapshot(), frame, write, &zone).await
    }
}

/// Write one planned frame to Postgres through step 1's selector.
///
/// # Errors
///
/// Connection, discovery, arity, column-resolution, cast, encode and commit
/// failures, each in the engine's DataFusion taxonomy.
#[cfg(feature = "postgres")]
pub async fn execute_postgres_write(
    catalogs: &CatalogRegistry,
    frame: DataFrame,
    write: PostgresWrite,
    session_zone: &str,
) -> Result<PostgresWriteReport> {
    use futures::StreamExt;
    use repark_connect::{
        PoolLimits, PostgresConnector, QueryPool, WriteOptions, WritePath, WriteRequest, discover,
    };

    let (props, door, display, scan) = resolve_target(catalogs, write.target)?;
    let settings =
        PostgresSettings::from_props(&props, door).map_err(|error| external(&display, error))?;
    let connector = PostgresConnector::new(&settings).map_err(|error| external(&display, error))?;
    let pool = QueryPool::new(connector, PoolLimits::from_settings(&settings));
    let resolved = discover(&pool, &scan, settings.read_timeout)
        .await
        .map_err(|error| external(&display, error))?;
    let table_columns: Vec<String> = resolved
        .columns
        .iter()
        .map(|column| column.name.as_str().to_string())
        .collect();
    let expected: Vec<String> = write.columns.clone().unwrap_or(table_columns.clone());
    let indexes = resolve_columns(&table_columns, &expected, write.case_insensitive)?;
    let frame_width = frame.schema().fields().len();
    if frame_width < indexes.len() {
        return Err(DataFusionError::Plan(arity_message(
            &display,
            &expected,
            frame_width,
        )));
    }
    if frame_width > indexes.len() {
        return Err(DataFusionError::Plan(
            "Column count doesn't match insert query!".to_string(),
        ));
    }
    let request = WriteRequest::new(&resolved).map_err(|error| external(&display, error))?;
    let request_path = match write.path {
        PostgresWritePath::Bulk => WritePath::Bulk,
        PostgresWritePath::Row => WritePath::Row,
    };
    let selector = request.columns(&indexes).ok_or_else(|| {
        DataFusionError::Plan("Column count doesn't match insert query!".to_string())
    })?;
    let mut writer = selector
        .open(&pool, request_path, WriteOptions::from_settings(&settings))
        .await
        .map_err(|error| {
            DataFusionError::External(Box::new(error))
                .context(format!("database source `{display}`"))
        })?;
    let shaped = shape_columns(&resolved, &indexes)?;
    let mut stream = frame.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let output = shape_batch(&batch, &shaped, &settings, session_zone, &display)?;
        writer.write(&output).await.map_err(|error| {
            DataFusionError::External(Box::new(error))
                .context(format!("database source `{display}`"))
        })?;
    }
    let report = writer.commit().await.map_err(|error| {
        DataFusionError::External(Box::new(error)).context(format!("database source `{display}`"))
    })?;
    Ok(PostgresWriteReport {
        path: match report.path {
            WritePath::Bulk => PostgresWritePath::Bulk,
            WritePath::Row => PostgresWritePath::Row,
        },
        rows: report.rows,
    })
}

/// The shared refusal for a non-append write: only append lands on Postgres.
#[must_use]
pub fn postgres_write_modes_refusal(what: &str) -> String {
    format!(
        "{what} refuses: a Postgres source takes append writes only (registry row \
         CONNECT-DECL-pg-write-modes in docs/spark-sql-iceberg-parity.md)"
    )
}

/// The shared refusal for a row-changing write that is not an append.
#[must_use]
pub fn postgres_write_upsert_refusal(what: &str) -> String {
    format!(
        "{what} refuses: a Postgres source takes append writes only; changing stored rows is \
         not implemented (registry row CONNECT-DECL-pg-write-upsert in \
         docs/spark-sql-iceberg-parity.md)"
    )
}

/// Write one planned frame to Postgres through step 1's selector.
///
/// # Errors
///
/// Always refuses: the Postgres connector is not compiled into this build.
#[cfg(not(feature = "postgres"))]
pub async fn execute_postgres_write(
    catalogs: &CatalogRegistry,
    frame: DataFrame,
    write: PostgresWrite,
    session_zone: &str,
) -> Result<PostgresWriteReport> {
    let _ = (catalogs, frame, write, session_zone);
    Err(DataFusionError::NotImplemented(
        "the Postgres connector is not compiled into this build".to_string(),
    ))
}

#[cfg(feature = "postgres")]
fn resolve_columns(
    table: &[String],
    expected: &[String],
    case_insensitive: bool,
) -> Result<Vec<usize>> {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    expected
        .iter()
        .map(|name| {
            if !seen.insert(name.clone()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate column `{name}` in the column list"
                )));
            }
            if let Some(index) = table.iter().position(|candidate| candidate == name) {
                return Ok(index);
            }
            if case_insensitive {
                let mut matches = table
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| candidate.eq_ignore_ascii_case(name))
                    .map(|(index, _)| index);
                if let Some(index) = matches.next() {
                    if matches.next().is_none() {
                        return Ok(index);
                    }
                    return Err(DataFusionError::Plan(format!(
                        "column `{name}` is ambiguous"
                    )));
                }
            }
            Err(DataFusionError::Plan(format!(
                "column `{name}` not found in the Postgres table"
            )))
        })
        .collect()
}

#[cfg(feature = "postgres")]
fn arity_message(display: &str, expected: &[String], width: usize) -> String {
    let quote = |name: &str| format!("`{}`", name.replace('`', "``"));
    let table_columns = expected
        .iter()
        .map(|name| quote(name))
        .collect::<Vec<_>>()
        .join(", ");
    let data_columns = (1..=width)
        .map(|index| quote(&format!("col{index}")))
        .collect::<Vec<_>>()
        .join(", ");
    spark_error::message(
        spark_error::INSERT_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS,
        &[
            ("tableName", display),
            ("tableColumns", &table_columns),
            ("dataColumns", &data_columns),
        ],
    )
}

#[cfg(feature = "postgres")]
fn external(name: &str, error: repark_connect::ConnectError) -> DataFusionError {
    DataFusionError::External(Box::new(error)).context(format!("database source `{name}`"))
}

#[cfg(feature = "postgres")]
type ResolvedTarget = (
    BTreeMap<String, String>,
    repark_connect::SettingsDoor,
    String,
    repark_connect::ScanSource,
);

#[cfg(feature = "postgres")]
fn resolve_target(
    catalogs: &CatalogRegistry,
    target: PostgresWriteTarget,
) -> Result<ResolvedTarget> {
    use repark_connect::{
        LOWER_BOUND_KEY, NUM_PARTITIONS_KEY, PARTITION_COLUMN_KEY, PgIdent, QualifiedRelation,
        ScanSource, SettingsDoor, UPPER_BOUND_KEY,
    };

    use super::read_postgres::READ_POSTGRES_SOURCE;

    match target {
        PostgresWriteTarget::Mounted {
            source,
            schema,
            table,
        } => {
            let spec = catalogs.database_source(&source).ok_or_else(|| {
                DataFusionError::Plan(format!("unknown database source `{source}`"))
            })?;
            if spec.identity.kind != SourceKind::Postgres {
                return Err(DataFusionError::Plan(format!(
                    "database source `{source}` is not a Postgres source"
                )));
            }
            let name = spec.identity.name.clone();
            let relation = QualifiedRelation::new(
                PgIdent::new(schema).map_err(|error| external(&name, error))?,
                PgIdent::new(table).map_err(|error| external(&name, error))?,
            );
            Ok((
                spec.props.clone(),
                SettingsDoor::ReparkToml,
                name,
                ScanSource::Relation(relation),
            ))
        }
        PostgresWriteTarget::Url {
            url,
            dbtable,
            properties,
        } => {
            let mut props = properties;
            props.retain(|key, _| {
                !(key.eq_ignore_ascii_case("dbtable")
                    || key.eq_ignore_ascii_case("predicates")
                    || key.eq_ignore_ascii_case(PARTITION_COLUMN_KEY)
                    || key.eq_ignore_ascii_case(LOWER_BOUND_KEY)
                    || key.eq_ignore_ascii_case(UPPER_BOUND_KEY)
                    || key.eq_ignore_ascii_case(NUM_PARTITIONS_KEY))
            });
            props.insert("url".to_string(), url);
            let scan = ScanSource::from_dbtable(&dbtable)
                .map_err(|error| external(READ_POSTGRES_SOURCE, error))?;
            Ok((
                props,
                SettingsDoor::ReadPostgres,
                READ_POSTGRES_SOURCE.to_string(),
                scan,
            ))
        }
    }
}

#[cfg(feature = "postgres")]
fn primitive_of<'a>(name: &str, array: &'a ArrayRef) -> Result<&'a TimestampMicrosecondArray> {
    array
        .as_primitive_opt::<TimestampMicrosecondType>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("column `{name}` is not a microsecond timestamp"))
        })
}

#[cfg(feature = "postgres")]
fn place_wall(name: &str, array: &ArrayRef, session_zone: &str, display: &str) -> Result<ArrayRef> {
    use crate::session::zone_localiser::localise_at;
    use crate::session_time_zone::canonical_session_zone_id;

    let wall = primitive_of(name, array)?;
    let localised = localise_at(session_zone, wall).map_err(|error| external(display, error))?;
    Ok(Arc::new(localised.with_timezone(canonical_session_zone_id(session_zone))) as ArrayRef)
}

#[cfg(feature = "postgres")]
struct ShapedColumn {
    name: String,
    mapping: PostgresMapping,
    expected: DataType,
}

#[cfg(feature = "postgres")]
fn shape_columns(resolved: &ResolvedSource, indexes: &[usize]) -> Result<Vec<ShapedColumn>> {
    indexes
        .iter()
        .map(|&index| {
            let column = resolved.columns.get(index).ok_or_else(|| {
                DataFusionError::Plan("Column count doesn't match insert query!".to_string())
            })?;
            Ok(ShapedColumn {
                name: column.name.as_str().to_string(),
                mapping: column.planned.mapping(),
                expected: column.planned.field().data_type().clone(),
            })
        })
        .collect()
}

#[cfg(feature = "postgres")]
fn shape_batch(
    batch: &RecordBatch,
    shaped: &[ShapedColumn],
    settings: &PostgresSettings,
    session_zone: &str,
    display: &str,
) -> Result<RecordBatch> {
    use arrow::compute::cast;

    use crate::session::zone_localiser::unlocalise;

    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(shaped.len());
    for (position, column) in shaped.iter().enumerate() {
        let name = column.name.as_str();
        let array = batch.columns().get(position).ok_or_else(|| {
            DataFusionError::Plan("Column count doesn't match insert query!".to_string())
        })?;
        let cast_to = |target: &DataType| {
            cast(array, target).map_err(|error| {
                DataFusionError::Execution(format!(
                    "cannot cast column `{name}` from {} to {target}: {error}",
                    array.data_type()
                ))
            })
        };
        let placed = match column.mapping {
            PostgresMapping::Timestamp if !settings.prefer_timestamp_ntz => {
                match array.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                        let zoned = primitive_of(name, array)?;
                        let walls = unlocalise(session_zone, zoned)
                            .map_err(|error| external(display, error))?;
                        Arc::new(walls) as ArrayRef
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::clone(array),
                    _ => cast_to(&naive)?,
                }
            }
            PostgresMapping::Timestamptz => match array.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Arc::clone(array),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    place_wall(name, array, session_zone, display)?
                }
                _ => {
                    let naive_array = cast_to(&naive)?;
                    place_wall(name, &naive_array, session_zone, display)?
                }
            },
            _ => {
                if array.data_type() == &column.expected {
                    Arc::clone(array)
                } else {
                    cast_to(&column.expected)?
                }
            }
        };
        arrays.push(placed);
    }
    let fields = shaped
        .iter()
        .zip(arrays.iter())
        .map(|(column, array)| Field::new(column.name.clone(), array.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Schema::new(fields).into(), arrays)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}
