use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::catalog::Session;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use repark_common::java_case::string_lower_equal;
use repark_common::{Error, Result};
use url::Url;

use crate::engine_err;
use crate::object_store_s3;
use crate::partition_discovery::{
    HIVE_DEFAULT_PARTITION, conflicting_names_message, parse_decimal_scaled,
    parse_timestamp_micros_zone, unescape_partition_value,
};
use crate::session::ReparkSession;

use super::{
    FloatBoundsInexactFormat, promote_parquet_null_types, read_parquet_float_bounds_inexact,
    relax_schema_to_nullable,
};

pub const PARTITION_COLUMN_TYPE_INFERENCE_KEY: &str =
    "spark.sql.sources.partitionColumnTypeInference.enabled";

pub const DEFAULT_PARTITION_COLUMN_TYPE_INFERENCE: bool = true;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumnTypeInferenceConfig {
    pub enabled: bool,
}

impl Default for PartitionColumnTypeInferenceConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_PARTITION_COLUMN_TYPE_INFERENCE,
        }
    }
}

impl ConfigExtension for PartitionColumnTypeInferenceConfig {
    const PREFIX: &'static str = "repark.partition-type-inference";
}

impl ExtensionOptions for PartitionColumnTypeInferenceConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> DataFusionResult<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: use `{PARTITION_COLUMN_TYPE_INFERENCE_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_partition_column_type_inference(raw: &str) -> DataFusionResult<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => {
            let masked = repark_common::redaction::mask_value_credentials(raw);
            Err(DataFusionError::Configuration(format!(
                "[INVALID_CONF_VALUE.TYPE] The value '{masked}' in the config \
                 \"{PARTITION_COLUMN_TYPE_INFERENCE_KEY}\" is invalid. It should be a/an \
                 'boolean' value. SQLSTATE: 22022"
            )))
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_runtime_partition_column_type_inference(raw: &str) -> Result<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        let masked = repark_common::redaction::mask_value_credentials(raw);
        Err(Error::IllegalArgument(format!(
            "[INVALID_CONF_VALUE.TYPE] The value '{masked}' in the config \
             \"{PARTITION_COLUMN_TYPE_INFERENCE_KEY}\" is invalid. It should be a/an \
             'boolean' value. SQLSTATE: 22022"
        )))
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn partition_column_type_inference_from_config_map<S>(
    config: &HashMap<String, String, S>,
) -> DataFusionResult<bool>
where
    S: BuildHasher,
{
    match config.get(PARTITION_COLUMN_TYPE_INFERENCE_KEY) {
        Some(raw) => parse_partition_column_type_inference(raw),
        None => Ok(DEFAULT_PARTITION_COLUMN_TYPE_INFERENCE),
    }
}

#[must_use]
pub fn with_partition_column_type_inference(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(PartitionColumnTypeInferenceConfig { enabled })
}

#[must_use]
pub fn partition_column_type_inference_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<PartitionColumnTypeInferenceConfig>()
        .map_or(DEFAULT_PARTITION_COLUMN_TYPE_INFERENCE, |extension| {
            extension.enabled
        })
}

#[must_use]
pub fn partition_column_type_inference_from_ctx(ctx: &SessionContext) -> bool {
    partition_column_type_inference_from_options(ctx.copied_config().options())
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub fn set_partition_column_type_inference(
        &self,
        raw: &str,
        strict_boolean: bool,
    ) -> Result<()> {
        let enabled = if strict_boolean {
            parse_runtime_partition_column_type_inference(raw)?
        } else {
            parse_partition_column_type_inference(raw).map_err(|error| match error {
                DataFusionError::Configuration(message) => Error::IllegalArgument(message),
                other => Error::IllegalArgument(other.to_string()),
            })?
        };
        let state_lock = self.context().state_ref();
        let mut state = state_lock.write();
        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(PartitionColumnTypeInferenceConfig { enabled });
        Ok(())
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn read_parquet_with_case_sensitivity(
        &self,
        path: &str,
        case_sensitive: bool,
    ) -> Result<DataFrame> {
        if let Some((_scheme, bucket)) = object_store_s3::parse_s3_bucket(path) {
            self.ensure_s3_bucket_registered(&bucket)?;
        }
        let session_zone = self.session_time_zone();
        let canonical = crate::canonical_session_zone_id(session_zone.id());
        let type_inference = partition_column_type_inference_from_ctx(self.context());
        read_parquet_discovered(
            self.context(),
            path,
            canonical.as_str(),
            case_sensitive,
            type_inference,
        )
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InferredPartType {
    Null,
    Int32,
    Int64,
    Decimal(u8),
    Float64,
    Date32,
    Timestamp,
    Utf8,
}

fn parse_java_integral_decimal(text: &str) -> Option<(i128, u8)> {
    if text.is_empty() || text.bytes().any(|byte| byte <= b' ') {
        return None;
    }
    let (neg, rest) = if let Some(tail) = text.strip_prefix('-') {
        (true, tail)
    } else if let Some(tail) = text.strip_prefix('+') {
        (false, tail)
    } else {
        (false, text)
    };
    let (mantissa, exp) = if let Some(pos) = rest.find(['e', 'E']) {
        let exp_str = &rest[pos + 1..];
        if exp_str.is_empty()
            || !exp_str
                .strip_prefix(['+', '-'])
                .unwrap_or(exp_str)
                .bytes()
                .all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        let exp_val = exp_str.parse::<i64>().ok()?;
        (&rest[..pos], exp_val)
    } else {
        (rest, 0_i64)
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((left, right)) => (left, right),
        None => (mantissa, ""),
    };
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|byte| byte.is_ascii_digit())
        || !frac_part.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let frac_len = i64::try_from(frac_part.len()).ok()?;
    let raw_scale = frac_len.checked_sub(exp)?;
    if raw_scale > 0 {
        return None;
    }
    let zeros = usize::try_from(raw_scale.checked_neg()?).ok()?;
    let mut digits = String::with_capacity(int_part.len() + frac_part.len() + zeros.min(40));
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        return Some((0, 1));
    }
    let precision = trimmed.len().checked_add(zeros)?;
    if precision > 38 {
        return None;
    }
    let mut scaled = String::with_capacity(precision + usize::from(neg));
    if neg {
        scaled.push('-');
    }
    scaled.push_str(trimmed);
    for _ in 0..zeros {
        scaled.push('0');
    }
    let value = scaled.parse::<i128>().ok()?;
    let prec_u8 = u8::try_from(precision).ok()?;
    Some((value, prec_u8))
}

fn parse_java_double(text: &str) -> Option<f64> {
    let trimmed = text.trim_matches(|ch: char| (ch as u32) <= 0x20);
    if trimmed.is_empty() {
        return None;
    }
    match trimmed {
        "NaN" => return Some(f64::NAN),
        "Infinity" | "+Infinity" => return Some(f64::INFINITY),
        "-Infinity" => return Some(f64::NEG_INFINITY),
        _ => {}
    }
    let lower_unsigned = trimmed
        .strip_prefix(['+', '-'])
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    if matches!(lower_unsigned.as_str(), "nan" | "inf" | "infinity") {
        return None;
    }
    let body = trimmed
        .strip_suffix(['d', 'D', 'f', 'F'])
        .unwrap_or(trimmed);
    if body.is_empty()
        || !body.bytes().any(|byte| byte.is_ascii_digit())
        || body.bytes().any(|byte| byte == b'_' || byte <= b' ')
    {
        return None;
    }
    body.parse::<f64>().ok()
}

fn looks_like_spark_partition_date(text: &str) -> bool {
    text.len() == 10 && NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
}

fn looks_like_spark_partition_timestamp(text: &str) -> bool {
    if text.len() == 19 {
        return NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").is_ok();
    }
    if text.len() == 21 {
        let bytes = text.as_bytes();
        return bytes[19] == b'.'
            && bytes[20].is_ascii_digit()
            && NaiveDateTime::parse_from_str(&text[..19], "%Y-%m-%d %H:%M:%S").is_ok();
    }
    false
}

fn infer_single_part_value(text: &str) -> InferredPartType {
    if text.parse::<i32>().is_ok() {
        InferredPartType::Int32
    } else if text.parse::<i64>().is_ok() {
        InferredPartType::Int64
    } else if let Some((_, precision)) = parse_java_integral_decimal(text) {
        InferredPartType::Decimal(precision)
    } else if parse_java_double(text).is_some() {
        InferredPartType::Float64
    } else if looks_like_spark_partition_date(text) {
        InferredPartType::Date32
    } else if looks_like_spark_partition_timestamp(text) {
        InferredPartType::Timestamp
    } else {
        InferredPartType::Utf8
    }
}

fn unify_part_type(left: InferredPartType, right: InferredPartType) -> InferredPartType {
    match (left, right) {
        (InferredPartType::Null, other) | (other, InferredPartType::Null) => other,
        (a, b) if a == b => a,
        (InferredPartType::Int32, InferredPartType::Int64)
        | (InferredPartType::Int64, InferredPartType::Int32) => InferredPartType::Int64,
        (InferredPartType::Int32 | InferredPartType::Int64, InferredPartType::Float64)
        | (InferredPartType::Float64, InferredPartType::Int32 | InferredPartType::Int64) => {
            InferredPartType::Float64
        }
        (InferredPartType::Decimal(p1), InferredPartType::Decimal(p2)) => {
            InferredPartType::Decimal(p1.max(p2))
        }
        (InferredPartType::Int32, InferredPartType::Decimal(precision))
        | (InferredPartType::Decimal(precision), InferredPartType::Int32) => {
            InferredPartType::Decimal(precision.max(10))
        }
        (InferredPartType::Int64, InferredPartType::Decimal(precision))
        | (InferredPartType::Decimal(precision), InferredPartType::Int64) => {
            InferredPartType::Decimal(precision.max(20))
        }
        _ => InferredPartType::Utf8,
    }
}

fn infer_column_data_type(
    values: &[Option<&str>],
    session_zone: &str,
    type_inference: bool,
) -> DataType {
    let mut has_non_null = false;
    let mut acc = InferredPartType::Null;
    for candidate in values {
        let Some(text) = candidate else {
            continue;
        };
        has_non_null = true;
        if !type_inference {
            return DataType::Utf8;
        }
        acc = unify_part_type(acc, infer_single_part_value(text));
        if acc == InferredPartType::Utf8 {
            return DataType::Utf8;
        }
    }
    if !has_non_null {
        return DataType::Null;
    }
    match acc {
        InferredPartType::Null => DataType::Null,
        InferredPartType::Int32 => DataType::Int32,
        InferredPartType::Int64 => DataType::Int64,
        InferredPartType::Decimal(precision) => DataType::Decimal128(precision, 0),
        InferredPartType::Float64 => DataType::Float64,
        InferredPartType::Date32 => DataType::Date32,
        InferredPartType::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_zone.into()))
        }
        InferredPartType::Utf8 => DataType::Utf8,
    }
}

fn parse_date32(text: &str) -> Option<i32> {
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}

fn typed_null_scalar(data_type: &DataType) -> ScalarValue {
    ScalarValue::try_from(data_type).unwrap_or(ScalarValue::Null)
}

fn scalar_from_partition_text(raw: Option<&str>, data_type: &DataType, zone: Tz) -> ScalarValue {
    let Some(text) = raw else {
        return typed_null_scalar(data_type);
    };
    match data_type {
        DataType::Null => ScalarValue::Null,
        DataType::Int32 => ScalarValue::Int32(text.parse::<i32>().ok()),
        DataType::Int64 => ScalarValue::Int64(text.parse::<i64>().ok()),
        DataType::Decimal128(precision, scale) => {
            let value = if *scale == 0 {
                parse_java_integral_decimal(text)
                    .map(|(val, _)| val)
                    .or_else(|| parse_decimal_scaled(text, *precision, *scale))
            } else {
                parse_decimal_scaled(text, *precision, *scale)
            };
            ScalarValue::Decimal128(value, *precision, *scale)
        }
        DataType::Float64 => ScalarValue::Float64(parse_java_double(text)),
        DataType::Date32 => ScalarValue::Date32(parse_date32(text)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(parse_timestamp_micros_zone(text, zone), tz.clone())
        }
        _ => ScalarValue::Utf8(Some(text.to_string())),
    }
}

fn names_match(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        string_lower_equal(left, right)
    }
}

fn parent_display_path(table_url: &ListingTableUrl, meta: &ObjectMeta) -> PathBuf {
    let parent = meta
        .location
        .parent()
        .map_or_else(String::new, |path| path.to_string());
    if table_url.get_url().scheme() == "file" {
        PathBuf::from(format!("/{parent}"))
    } else {
        PathBuf::from(parent)
    }
}

fn parse_relative_specs(
    relative: &[&str],
    location: &str,
) -> Result<Vec<(String, Option<String>)>> {
    let dirs = &relative[..relative.len().saturating_sub(1)];
    let mut specs = Vec::new();
    for segment in dirs {
        if let Some((key, raw)) = segment.split_once('=') {
            if key.is_empty() {
                return Err(Error::Iceberg(format!(
                    "[INTERNAL_ERROR] Empty partition column name in segment `{segment}` in path \
                     '{location}'"
                )));
            }
            if raw.is_empty() {
                return Err(Error::Iceberg(format!(
                    "[INTERNAL_ERROR] Empty partition value for partition column `{key}` in path \
                     '{location}'"
                )));
            }
            let value = if raw == HIVE_DEFAULT_PARTITION {
                None
            } else {
                Some(unescape_partition_value(raw))
            };
            specs.push((key.to_string(), value));
        }
    }
    Ok(specs)
}

#[derive(Debug)]
pub(crate) struct PartitionedParquetProvider {
    table_url: ListingTableUrl,
    file_schema: SchemaRef,
    partition_fields: Vec<FieldRef>,
    table_schema: SchemaRef,
    files: Vec<PartitionedFile>,
    format: Arc<dyn FileFormat>,
    target_partitions: usize,
}

impl PartitionedParquetProvider {
    pub(crate) fn table_url(&self) -> &ListingTableUrl {
        &self.table_url
    }
}

fn expr_uses_only_columns(col_names: &[&str], expr: &Expr) -> bool {
    let mut applicable = true;
    let _ = expr.apply(|node| match node {
        Expr::Column(Column { name, .. }) => {
            applicable &= col_names.contains(&name.as_str());
            if applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(_, _)
        | Expr::Alias(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::ScalarVariable(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::BinaryExpr(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::InList(_)
        | Expr::Case(_) => Ok(TreeNodeRecursion::Continue),
        Expr::ScalarFunction(func) => match func.func.signature().volatility {
            Volatility::Immutable => Ok(TreeNodeRecursion::Continue),
            Volatility::Stable | Volatility::Volatile => {
                applicable = false;
                Ok(TreeNodeRecursion::Stop)
            }
        },
        _ => {
            applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });
    applicable
}

fn strip_column_qualifiers(expr: Expr) -> Expr {
    expr.transform(|node| match node {
        Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            column.name,
        )))),
        other => Ok(Transformed::no(other)),
    })
    .map_or_else(
        |_| Expr::Literal(ScalarValue::Boolean(Some(true)), None),
        |t| t.data,
    )
}

fn prune_partitioned_files(
    state: &dyn Session,
    files: &[PartitionedFile],
    partition_fields: &[FieldRef],
    filters: &[Expr],
) -> Vec<PartitionedFile> {
    if partition_fields.is_empty() || filters.is_empty() {
        return files.to_vec();
    }
    let part_names: Vec<&str> = partition_fields
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let part_filters: Vec<Expr> = filters
        .iter()
        .filter(|expr| expr_uses_only_columns(&part_names, expr))
        .cloned()
        .map(strip_column_qualifiers)
        .collect();
    let Some(combined) = conjunction(part_filters) else {
        return files.to_vec();
    };
    let Ok(part_df_schema) = DFSchema::from_unqualified_fields(
        partition_fields.iter().cloned().collect(),
        HashMap::new(),
    ) else {
        return files.to_vec();
    };
    let Ok(physical) = state.create_physical_expr(combined, &part_df_schema) else {
        return files.to_vec();
    };
    let part_arrow_schema = Arc::clone(part_df_schema.inner());
    let mut kept = Vec::with_capacity(files.len());
    for file in files {
        let arrays: DataFusionResult<Vec<ArrayRef>> = file
            .partition_values
            .iter()
            .map(|value| value.to_array_of_size(1))
            .collect();
        let Ok(arrays) = arrays else {
            kept.push(file.clone());
            continue;
        };
        let Ok(batch) = RecordBatch::try_new(Arc::clone(&part_arrow_schema), arrays) else {
            kept.push(file.clone());
            continue;
        };
        let evaluated = physical
            .evaluate(&batch)
            .and_then(|value| value.into_array(1));
        let Ok(array) = evaluated else {
            kept.push(file.clone());
            continue;
        };
        if let Some(bools) = array.as_any().downcast_ref::<BooleanArray>() {
            if bools.len() == 1 && bools.is_valid(0) && bools.value(0) {
                kept.push(file.clone());
            }
        } else {
            kept.push(file.clone());
        }
    }
    kept
}

#[async_trait]
impl TableProvider for PartitionedParquetProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let kept = prune_partitioned_files(state, &self.files, &self.partition_fields, filters);
        if kept.is_empty() {
            let projected = match projection {
                None => Arc::clone(&self.table_schema),
                Some(indices) => Arc::new(self.table_schema.project(indices)?),
            };
            return Ok(Arc::new(EmptyExec::new(projected)));
        }
        let target = self.target_partitions.max(1);
        let file_groups = FileGroup::new(kept).split_files(target);
        let table_schema =
            TableSchema::new(Arc::clone(&self.file_schema), self.partition_fields.clone());
        let file_source = self.format.file_source(table_schema);
        let config = FileScanConfigBuilder::new(self.table_url.object_store(), file_source)
            .with_file_groups(file_groups)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        self.format.create_physical_plan(state, config).await
    }
}

async fn read_parquet_listing_fallback(
    context: &SessionContext,
    resolved: &str,
) -> Result<DataFrame> {
    let inferred = context
        .read_parquet(resolved, ParquetReadOptions::default())
        .await
        .map_err(engine_err)?;
    let relaxed = relax_schema_to_nullable(inferred.schema().as_arrow());
    let sparked = promote_parquet_null_types(&relaxed);
    read_parquet_float_bounds_inexact(context, resolved, &sparked).map_err(engine_err)
}

type RawPartitionPairs = Vec<(String, Option<String>)>;
type DiscoveredEntry = (ObjectMeta, RawPartitionPairs);

fn normalize_file_dir_url(path: String) -> String {
    if path.starts_with("file://")
        && !path.ends_with('/')
        && let Ok(parsed) = Url::parse(&path)
        && parsed.to_file_path().is_ok_and(|local| local.is_dir())
    {
        return format!("{path}/");
    }
    path
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn read_parquet_discovered(
    context: &SessionContext,
    path: &str,
    session_zone: &str,
    case_sensitive: bool,
    type_inference: bool,
) -> Result<DataFrame> {
    let resolved =
        normalize_file_dir_url(object_store_s3::resolve_s3_prefix_for_read(context, path).await?);
    let table_url = ListingTableUrl::parse(&resolved).map_err(engine_err)?;
    if !table_url.is_collection() || table_url.get_glob().is_some() {
        return read_parquet_listing_fallback(context, &resolved).await;
    }
    let Ok(store) = context.runtime_env().object_store(&table_url) else {
        return read_parquet_listing_fallback(context, &resolved).await;
    };
    let prefix = (!table_url.prefix().as_ref().is_empty()).then(|| table_url.prefix().clone());
    let Ok(mut listed) = store.list(prefix.as_ref()).try_collect::<Vec<_>>().await else {
        return read_parquet_listing_fallback(context, &resolved).await;
    };
    listed.sort_by(|left, right| {
        left.location
            .as_ref()
            .to_lowercase()
            .cmp(&right.location.as_ref().to_lowercase())
            .then_with(|| left.location.as_ref().cmp(right.location.as_ref()))
    });
    let mut had_hidden_parquet = false;
    let mut visible: Vec<DiscoveredEntry> = Vec::new();
    for meta in listed {
        let Some(rel_iter) = table_url.strip_prefix(&meta.location) else {
            continue;
        };
        let relative: Vec<&str> = rel_iter.collect();
        if relative.is_empty() {
            continue;
        }
        let is_parquet = relative
            .last()
            .is_some_and(|name| name.ends_with(".parquet"));
        let is_hidden = relative
            .iter()
            .any(|segment| segment.starts_with('.') || segment.starts_with('_'));
        if is_hidden {
            if is_parquet {
                had_hidden_parquet = true;
            }
            continue;
        }
        if !is_parquet || meta.size == 0 {
            continue;
        }
        let specs = parse_relative_specs(&relative, meta.location.as_ref())?;
        visible.push((meta, specs));
    }
    if visible.is_empty() {
        return read_parquet_listing_fallback(context, &resolved).await;
    }
    let any_partitioned = visible.iter().any(|(_, specs)| !specs.is_empty());
    if !any_partitioned && !had_hidden_parquet {
        return read_parquet_listing_fallback(context, &resolved).await;
    }
    let kept: Vec<DiscoveredEntry> = if any_partitioned {
        visible
            .into_iter()
            .filter(|(_, specs)| !specs.is_empty())
            .collect()
    } else {
        visible
    };
    let mut seen_norm: Vec<Vec<String>> = Vec::new();
    let mut display_by_norm: HashMap<Vec<String>, (Vec<String>, PathBuf)> = HashMap::new();
    for (meta, specs) in &kept {
        let keys: Vec<String> = specs.iter().map(|(key, _)| key.clone()).collect();
        let norm: Vec<String> = if case_sensitive {
            keys.clone()
        } else {
            keys.iter().map(|key| key.to_lowercase()).collect()
        };
        if !display_by_norm.contains_key(&norm) {
            display_by_norm.insert(norm.clone(), (keys, parent_display_path(&table_url, meta)));
            seen_norm.push(norm);
        }
    }
    if seen_norm.len() > 1 {
        let mut lists: Vec<Vec<String>> = Vec::with_capacity(seen_norm.len());
        let mut dir_by_list: HashMap<Vec<String>, PathBuf> = HashMap::new();
        for norm in &seen_norm {
            if let Some((keys, dir)) = display_by_norm.get(norm) {
                lists.push(keys.clone());
                dir_by_list.insert(keys.clone(), dir.clone());
            }
        }
        lists.sort_by(|left, right| {
            left.len()
                .cmp(&right.len())
                .then_with(|| left.join(", ").cmp(&right.join(", ")))
        });
        return Err(Error::Iceberg(conflicting_names_message(
            &lists,
            &dir_by_list,
        )));
    }
    let canonical_names: Vec<String> = seen_norm
        .first()
        .and_then(|norm| display_by_norm.get(norm))
        .map(|(keys, _)| keys.clone())
        .unwrap_or_default();
    let zone: Tz = session_zone.parse().map_err(|error| {
        Error::Analysis(format!(
            "parquet read cannot parse session time zone {session_zone:?}: {error}"
        ))
    })?;
    let mut partition_fields: Vec<FieldRef> = Vec::with_capacity(canonical_names.len());
    for (col_index, name) in canonical_names.iter().enumerate() {
        let col_values: Vec<Option<&str>> = kept
            .iter()
            .map(|(_, specs)| specs.get(col_index).and_then(|(_, val)| val.as_deref()))
            .collect();
        let data_type = infer_column_data_type(&col_values, session_zone, type_inference);
        partition_fields.push(Arc::new(Field::new(name, data_type, true)));
    }
    let kept_metas: Vec<ObjectMeta> = kept.iter().map(|(meta, _)| meta.clone()).collect();
    let listing_options = ParquetReadOptions::default()
        .to_listing_options(&context.copied_config(), context.copied_table_options());
    let format: Arc<dyn FileFormat> =
        Arc::new(FloatBoundsInexactFormat::new(listing_options.format));
    let inferred_file_schema = format
        .infer_schema(&context.state(), &store, &kept_metas)
        .await
        .map_err(engine_err)?;
    let relaxed_file_schema = relax_schema_to_nullable(inferred_file_schema.as_ref());
    let sparked_file_schema = promote_parquet_null_types(&relaxed_file_schema);
    let mut scan_file_fields: Vec<Field> = Vec::new();
    let mut final_order: Vec<String> = Vec::new();
    let mut placed_parts: Vec<bool> = vec![false; partition_fields.len()];
    for field in sparked_file_schema.fields() {
        if let Some((part_index, part_field)) = partition_fields
            .iter()
            .enumerate()
            .find(|(_, part)| names_match(field.name(), part.name(), case_sensitive))
        {
            if !placed_parts[part_index] {
                final_order.push(part_field.name().clone());
                placed_parts[part_index] = true;
            }
        } else {
            scan_file_fields.push(field.as_ref().clone());
            final_order.push(field.name().clone());
        }
    }
    for (part_index, part_field) in partition_fields.iter().enumerate() {
        if !placed_parts[part_index] {
            final_order.push(part_field.name().clone());
        }
    }
    let scan_file_schema: SchemaRef = Arc::new(Schema::new_with_metadata(
        scan_file_fields.clone(),
        sparked_file_schema.metadata().clone(),
    ));
    let mut provider_fields: Vec<Field> = scan_file_fields;
    provider_fields.extend(partition_fields.iter().map(|field| field.as_ref().clone()));
    let provider_schema: SchemaRef = Arc::new(Schema::new_with_metadata(
        provider_fields,
        sparked_file_schema.metadata().clone(),
    ));
    let provider_names: Vec<String> = provider_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let partitioned_files: Vec<PartitionedFile> = kept
        .into_iter()
        .map(|(meta, specs)| {
            let values: Vec<ScalarValue> = partition_fields
                .iter()
                .enumerate()
                .map(|(col_index, field)| {
                    let raw = specs.get(col_index).and_then(|(_, val)| val.as_deref());
                    scalar_from_partition_text(raw, field.data_type(), zone)
                })
                .collect();
            let mut file: PartitionedFile = meta.into();
            file.partition_values = values;
            file
        })
        .collect();
    let provider = Arc::new(PartitionedParquetProvider {
        table_url,
        file_schema: scan_file_schema,
        partition_fields,
        table_schema: provider_schema,
        files: partitioned_files,
        format,
        target_partitions: listing_options.target_partitions,
    });
    let frame = context.read_table(provider).map_err(engine_err)?;
    if final_order == provider_names {
        return Ok(frame);
    }
    let projection: Vec<Expr> = final_order
        .into_iter()
        .map(|name| Expr::Column(Column::new_unqualified(name)))
        .collect();
    frame.select(projection).map_err(engine_err)
}
