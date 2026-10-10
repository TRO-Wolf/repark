use std::collections::BTreeMap;
#[cfg(feature = "postgres")]
use std::sync::Arc;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;

use super::ReparkSession;
use crate::catalog_state::CatalogRegistry;

#[cfg(feature = "postgres")]
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray,
};
#[cfg(feature = "postgres")]
use arrow::compute::{CastOptions, cast_with_options};
#[cfg(feature = "postgres")]
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
#[cfg(feature = "postgres")]
use repark_common::{SourceKind, spark_error};
#[cfg(feature = "postgres")]
use repark_connect::postgres::PostgresMapping;
#[cfg(feature = "postgres")]
use repark_connect::{PostgresSettings, ResolvedSource};

#[derive(Debug, Clone)]
pub enum PostgresWriteTarget {
    Mounted {
        source: String,
        schema: String,
        table: String,
    },
    Url {
        url: String,
        dbtable: String,
        properties: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresWritePath {
    Bulk,
    Row,
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_write_path_option(raw: Option<&str>) -> Result<PostgresWritePath> {
    match raw {
        None => Ok(PostgresWritePath::Bulk),
        Some(value) if value.trim().eq_ignore_ascii_case("bulk") => Ok(PostgresWritePath::Bulk),
        Some(value) if value.trim().eq_ignore_ascii_case("row") => Ok(PostgresWritePath::Row),
        Some(value) => Err(DataFusionError::Configuration(format!(
            "write.path must be 'bulk' or 'row', got '{value}'"
        ))),
    }
}

#[derive(Debug, Clone, Default)]
pub struct LastPostgresWriteReport {
    report: std::sync::Arc<std::sync::Mutex<Option<PostgresWriteReport>>>,
}

impl ConfigExtension for LastPostgresWriteReport {
    const PREFIX: &'static str = "repark.pg_write_report";
}

impl ExtensionOptions for LastPostgresWriteReport {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the last-write report is a test hook the \
             sink writes; read it back, never set it",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

impl LastPostgresWriteReport {
    pub fn record(&self, report: PostgresWriteReport) {
        *self
            .report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
    }

    pub fn take(&self) -> Option<PostgresWriteReport> {
        self.report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

#[must_use]
pub fn with_last_postgres_write_report(
    config: datafusion::prelude::SessionConfig,
) -> datafusion::prelude::SessionConfig {
    let mut config = config;
    config
        .options_mut()
        .extensions
        .insert(LastPostgresWriteReport::default());
    config
}

pub fn record_postgres_write_report(
    ctx: &datafusion::prelude::SessionContext,
    report: PostgresWriteReport,
) {
    let state = ctx.state();
    if let Some(carrier) = state
        .config()
        .options()
        .extensions
        .get::<LastPostgresWriteReport>()
    {
        carrier.record(report);
    }
}

#[must_use]
pub fn take_postgres_write_report(
    ctx: &datafusion::prelude::SessionContext,
) -> Option<PostgresWriteReport> {
    let state = ctx.state();
    state
        .config()
        .options()
        .extensions
        .get::<LastPostgresWriteReport>()?
        .take()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresWriteReport {
    pub path: PostgresWritePath,
    pub rows: u64,
    pub fallback: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PostgresWrite {
    pub target: PostgresWriteTarget,
    pub columns: Option<Vec<String>>,
    pub case_insensitive: bool,
    pub path: PostgresWritePath,
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn write_postgres(
        &self,
        frame: DataFrame,
        write: PostgresWrite,
    ) -> Result<PostgresWriteReport> {
        let zone = self.session_time_zone().id().to_string();
        execute_postgres_write(&self.catalogs_snapshot(), frame, write, &zone).await
    }
}

#[cfg(feature = "postgres")]
#[allow(clippy::missing_errors_doc)]
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
    let taken = writer.path();
    let fallback = writer.fallback().map(|reason| reason.to_string());
    let shaped = shape_columns(&resolved, &indexes)?;
    let mut stream = frame.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let output = shape_batch(
            &batch,
            &shaped,
            settings.prefer_timestamp_ntz,
            session_zone,
            &display,
        )?;
        writer.write(&output).await.map_err(|error| {
            DataFusionError::External(Box::new(error))
                .context(format!("database source `{display}`"))
        })?;
    }
    let report = writer.commit().await.map_err(|error| {
        DataFusionError::External(Box::new(error)).context(format!("database source `{display}`"))
    })?;
    Ok(PostgresWriteReport {
        path: match taken {
            WritePath::Bulk => PostgresWritePath::Bulk,
            WritePath::Row => PostgresWritePath::Row,
        },
        rows: report.rows,
        fallback,
    })
}

#[must_use]
pub fn postgres_write_modes_refusal(what: &str) -> String {
    format!(
        "{what} refuses: a Postgres source takes append writes only (registry row \
         CONNECT-DECL-pg-write-modes in docs/spark-sql-iceberg-parity.md)"
    )
}

#[must_use]
pub fn postgres_write_upsert_refusal(what: &str) -> String {
    format!(
        "{what} refuses: a Postgres source takes append writes only; changing stored rows is \
         not implemented (registry row CONNECT-DECL-pg-write-upsert in \
         docs/spark-sql-iceberg-parity.md)"
    )
}

#[cfg(not(feature = "postgres"))]
#[allow(clippy::missing_errors_doc)]
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
pub(crate) struct ShapedColumn {
    name: String,
    mapping: PostgresMapping,
    expected: DataType,
}

#[cfg(all(test, feature = "postgres"))]
impl ShapedColumn {
    pub(crate) fn for_test(name: &str, mapping: PostgresMapping, expected: DataType) -> Self {
        Self {
            name: name.to_string(),
            mapping,
            expected,
        }
    }
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
fn plain_values_type(given: &DataType) -> DataType {
    match given {
        DataType::Dictionary(_, values) => plain_values_type(values),
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        plain => plain.clone(),
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn shape_batch(
    batch: &RecordBatch,
    shaped: &[ShapedColumn],
    prefer_timestamp_ntz: bool,
    session_zone: &str,
    display: &str,
) -> Result<RecordBatch> {
    use crate::session::zone_localiser::unlocalise;

    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(shaped.len());
    for (position, column) in shaped.iter().enumerate() {
        let name = column.name.as_str();
        let array = batch.columns().get(position).ok_or_else(|| {
            DataFusionError::Plan("Column count doesn't match insert query!".to_string())
        })?;
        let cast_to = |target: &DataType| {
            cast_with_options(array, target, &strict_cast_options())
                .map_err(|_| cast_refusal(name, array, target))
        };
        let placed = match column.mapping {
            PostgresMapping::Timestamp if !prefer_timestamp_ntz => match array.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                    let zoned = primitive_of(name, array)?;
                    let walls = unlocalise(session_zone, zoned)
                        .map_err(|error| external(display, error))?;
                    Arc::new(walls) as ArrayRef
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::clone(array),
                _ => cast_to(&naive)?,
            },
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
                if plain_values_type(array.data_type()) == column.expected {
                    Arc::clone(array)
                } else if column.mapping == PostgresMapping::Utf8
                    && matches!(
                        array.data_type(),
                        DataType::Timestamp(TimeUnit::Microsecond, _)
                    )
                {
                    render_session_strings(name, array, session_zone, display)?
                } else if column.mapping == PostgresMapping::Date
                    && matches!(
                        array.data_type(),
                        DataType::Timestamp(TimeUnit::Microsecond, _)
                    )
                {
                    session_zone_date(name, array, session_zone, display)?
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

#[cfg(feature = "postgres")]
fn wall_clocks(
    name: &str,
    array: &ArrayRef,
    session_zone: &str,
    display: &str,
) -> Result<TimestampMicrosecondArray> {
    use crate::session::zone_localiser::unlocalise;

    let stamps = primitive_of(name, array)?;
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            unlocalise(session_zone, stamps).map_err(|error| external(display, error))
        }
        _ => Ok(stamps.clone()),
    }
}

#[cfg(feature = "postgres")]
fn session_zone_date(
    name: &str,
    array: &ArrayRef,
    session_zone: &str,
    display: &str,
) -> Result<ArrayRef> {
    let walls: ArrayRef = Arc::new(wall_clocks(name, array, session_zone, display)?);
    cast_with_options(&walls, &DataType::Date32, &strict_cast_options())
        .map_err(|_| cast_refusal(name, &walls, &DataType::Date32))
}

#[cfg(feature = "postgres")]
fn spark_wall_text(column: &str, source: &DataType, micros: i64) -> Result<String> {
    use chrono::{DateTime, Datelike, Timelike};

    let Some(wall) = DateTime::from_timestamp_micros(micros).map(|zoned| zoned.naive_utc()) else {
        return Err(cast_overflow(
            column,
            &spark_cast_type_name(source),
            "STRING",
        ));
    };
    let mut text = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        wall.year(),
        wall.month(),
        wall.day(),
        wall.hour(),
        wall.minute(),
        wall.second()
    );
    let fraction = wall.nanosecond() / 1000;
    if fraction != 0 {
        let mut digits = format!("{fraction:06}");
        while digits.ends_with('0') {
            digits.pop();
        }
        text.push('.');
        text.push_str(&digits);
    }
    Ok(text)
}

#[cfg(feature = "postgres")]
fn render_session_strings(
    name: &str,
    array: &ArrayRef,
    session_zone: &str,
    display: &str,
) -> Result<ArrayRef> {
    let walls = wall_clocks(name, array, session_zone, display)?;
    let mut rendered = Vec::with_capacity(walls.len());
    for wall in &walls {
        match wall {
            None => rendered.push(None),
            Some(micros) => rendered.push(Some(spark_wall_text(name, array.data_type(), micros)?)),
        }
    }
    Ok(Arc::new(StringArray::from(rendered)))
}

#[cfg(feature = "postgres")]
fn strict_cast_options() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

#[cfg(feature = "postgres")]
fn spark_cast_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_string(),
        DataType::Null => "VOID".to_string(),
        DataType::Dictionary(_, values) => spark_cast_type_name(values),
        other => format!("{other:?}"),
    }
}

#[cfg(feature = "postgres")]
fn is_text_source(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, values) => is_text_source(values),
        _ => false,
    }
}

#[cfg(feature = "postgres")]
fn is_bytes_source(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
        DataType::Dictionary(_, values) => is_bytes_source(values),
        _ => false,
    }
}

#[cfg(feature = "postgres")]
fn decoded_values(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Dictionary(_, values) => cast_with_options(array, values, &strict_cast_options())
            .unwrap_or_else(|_| Arc::clone(array)),
        _ => Arc::clone(array),
    }
}

#[cfg(feature = "postgres")]
fn failing_row(array: &ArrayRef, target: &DataType) -> Option<usize> {
    if array.is_empty() {
        return None;
    }
    let options = strict_cast_options();
    let mut offset = 0;
    let mut len = array.len();
    while len > 1 {
        let half = len / 2;
        if cast_with_options(&array.slice(offset, half), target, &options).is_err() {
            len = half;
        } else {
            offset += half;
            len -= half;
        }
    }
    Some(offset)
}

#[cfg(feature = "postgres")]
fn text_at(array: &ArrayRef, index: usize) -> Option<String> {
    if array.is_null(index) {
        return None;
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Some(values.value(index).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(values.value(index).to_string());
    }
    array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .map(|values| values.value(index).to_string())
}

#[cfg(feature = "postgres")]
fn bytes_at(array: &ArrayRef, index: usize) -> Option<Vec<u8>> {
    if array.is_null(index) {
        return None;
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return Some(values.value(index).to_vec());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Some(values.value(index).to_vec());
    }
    array
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .map(|values| values.value(index).to_vec())
}

#[cfg(feature = "postgres")]
fn cast_overflow(column: &str, from: &str, to: &str) -> DataFusionError {
    DataFusionError::Execution(spark_error::message(
        spark_error::CAST_OVERFLOW_IN_TABLE_INSERT,
        &[
            ("fromType", from),
            ("toType", to),
            ("columnName", &format!("`{}`", column.replace('`', "``"))),
        ],
    ))
}

#[cfg(feature = "postgres")]
const INVALID_INPUT_VALUE_CHARS: usize = 200;

#[cfg(feature = "postgres")]
fn capped_text(value: &str) -> String {
    let mut shown: String = value.chars().take(INVALID_INPUT_VALUE_CHARS).collect();
    if value.chars().count() > INVALID_INPUT_VALUE_CHARS {
        shown.push('…');
    }
    shown
}

#[cfg(feature = "postgres")]
fn cast_invalid_input(column: &str, shown: &str, from: &str, to: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "column `{column}`: {}",
        spark_error::message(
            spark_error::CAST_INVALID_INPUT,
            &[("value", shown), ("fromType", from), ("toType", to)],
        )
    ))
}

#[cfg(feature = "postgres")]
fn invalid_string_input(
    column: &str,
    value: &str,
    from: &str,
    to: &str,
    target: &DataType,
) -> DataFusionError {
    if matches!(target, DataType::Decimal128(..) | DataType::Decimal256(..))
        && value.parse::<f64>().is_ok()
    {
        return cast_overflow(column, from, to);
    }
    let escaped = capped_text(value).replace('\'', "''");
    cast_invalid_input(column, &format!("'{escaped}'"), from, to)
}

#[cfg(feature = "postgres")]
fn invalid_bytes_input(column: &str, bytes: &[u8], from: &str, to: &str) -> DataFusionError {
    use std::fmt::Write as _;

    let mut hex = String::new();
    for byte in bytes.iter().take(100) {
        let _ = write!(hex, "{byte:02X}");
    }
    if bytes.len() > 100 {
        hex.push('…');
    }
    cast_invalid_input(column, &format!("X'{hex}'"), from, to)
}

#[cfg(feature = "postgres")]
fn cast_refusal(name: &str, array: &ArrayRef, target: &DataType) -> DataFusionError {
    let source = array.data_type();
    let from = spark_cast_type_name(source);
    let to = spark_cast_type_name(target);
    if is_text_source(source) {
        let decoded = decoded_values(array);
        if let Some(row) = failing_row(&decoded, target)
            && let Some(value) = text_at(&decoded, row)
        {
            return invalid_string_input(name, &value, &from, &to, target);
        }
    }
    if is_bytes_source(source)
        && matches!(
            target,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    {
        let decoded = decoded_values(array);
        if let Some(row) = failing_row(&decoded, target)
            && let Some(bytes) = bytes_at(&decoded, row)
        {
            return invalid_bytes_input(name, &bytes, &from, &to);
        }
    }
    cast_overflow(name, &from, &to)
}
