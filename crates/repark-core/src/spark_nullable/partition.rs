use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use repark_common::java_case::{string_lower_equal, string_lowered};
use repark_common::{Error, Result};
use url::Url;

use crate::engine_err;
use crate::object_store_s3;
use crate::partition_discovery::{HIVE_DEFAULT_PARTITION, conflicting_names_message};
use crate::session::ReparkSession;

use super::partition_infer::{
    infer_column_data_type, scalar_from_partition_text, unescape_spark_path_name,
};
use super::partition_prune::prune_partitioned_files;
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
    const PREFIX: &'static str = "repark.part_infer";
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
            "`{}.{key}` is not a settable option; use `{PARTITION_COLUMN_TYPE_INFERENCE_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPruningAnsiConfig {
    pub enabled: bool,
}

impl Default for PartitionPruningAnsiConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl ConfigExtension for PartitionPruningAnsiConfig {
    const PREFIX: &'static str = "repark.part_ansi";
}

impl ExtensionOptions for PartitionPruningAnsiConfig {
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
            "`{}.{key}` is not a settable option; use `spark.sql.ansi.enabled`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn with_partition_pruning_ansi(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(PartitionPruningAnsiConfig { enabled })
}

#[must_use]
pub fn partition_pruning_ansi_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<PartitionPruningAnsiConfig>()
        .is_none_or(|carrier| carrier.enabled)
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_partition_column_type_inference(raw: &str) -> DataFusionResult<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \"{PARTITION_COLUMN_TYPE_INFERENCE_KEY}\" is invalid. \
             {PARTITION_COLUMN_TYPE_INFERENCE_KEY} should be boolean, but was {raw}"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_runtime_partition_column_type_inference(raw: &str) -> Result<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => {
            let masked = crate::redaction::mask_value_credentials(raw);
            Err(Error::IllegalArgument(format!(
                "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{masked}' in the config \
                 \"{PARTITION_COLUMN_TYPE_INFERENCE_KEY}\" is invalid. \
                 {PARTITION_COLUMN_TYPE_INFERENCE_KEY} should be boolean, but was {masked}. \
                 SQLSTATE: 22022"
            )))
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn partition_column_type_inference_from_config_map<S: BuildHasher>(
    conf: &HashMap<String, String, S>,
) -> DataFusionResult<bool> {
    match conf.get(PARTITION_COLUMN_TYPE_INFERENCE_KEY) {
        None => Ok(DEFAULT_PARTITION_COLUMN_TYPE_INFERENCE),
        Some(raw) => parse_partition_column_type_inference(raw),
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
        .map_or(DEFAULT_PARTITION_COLUMN_TYPE_INFERENCE, |carrier| {
            carrier.enabled
        })
}

#[must_use]
pub fn partition_column_type_inference_from_ctx(ctx: &SessionContext) -> bool {
    let state = ctx.state();
    partition_column_type_inference_from_options(state.config_options())
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

fn is_hidden_directory_segment(segment: &str) -> bool {
    segment.starts_with('.') || (segment.starts_with('_') && !segment.contains('='))
}

fn is_hidden_file_name(name: &str) -> bool {
    name.starts_with('.') || name.starts_with('_')
}

fn parse_relative_specs(relative: &[&str], location: &str) -> Result<(RawPartitionPairs, String)> {
    let dirs = &relative[..relative.len().saturating_sub(1)];
    let mut specs = Vec::new();
    let mut base_len = 0;
    for (index, segment) in dirs.iter().enumerate().rev() {
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
            let unescaped_key = unescape_spark_path_name(key);
            let value = if raw == HIVE_DEFAULT_PARTITION {
                None
            } else {
                Some(unescape_spark_path_name(raw))
            };
            specs.push((unescaped_key, value));
        } else if !specs.is_empty() {
            base_len = index + 1;
            break;
        }
    }
    if specs.is_empty() {
        base_len = dirs.len();
    }
    specs.reverse();
    Ok((specs, dirs[..base_len].join("/")))
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
        let kept = prune_partitioned_files(
            state,
            &self.files,
            &self.partition_fields,
            &self.table_schema,
            filters,
        )?;
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
type DiscoveredEntry = (ObjectMeta, RawPartitionPairs, usize, String);

fn footer_file(entries: &[DiscoveredEntry]) -> Option<&ObjectMeta> {
    entries
        .iter()
        .map(|(meta, _, _, _)| meta)
        .min_by(|left, right| left.location.as_ref().cmp(right.location.as_ref()))
}

fn refuse_zero_byte_footer(footer: Option<&ObjectMeta>) -> Result<()> {
    if let Some(empty) = footer.filter(|meta| meta.size == 0) {
        return Err(Error::Iceberg(format!(
            "[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER] Could not read footer for file: '{}' \
             (0 bytes). SQLSTATE: KD001",
            empty.location
        )));
    }
    Ok(())
}

fn base_display_path(table_url: &ListingTableUrl, base: &str) -> String {
    let root = table_url.prefix().as_ref().trim_end_matches('/');
    let joined = match (root.is_empty(), base.is_empty()) {
        (_, true) => root.to_string(),
        (true, false) => base.to_string(),
        (false, false) => format!("{root}/{base}"),
    };
    let url = table_url.get_url();
    if url.scheme() == "file" {
        format!("file:/{joined}")
    } else {
        format!(
            "{}://{}/{joined}",
            url.scheme(),
            url.host_str().unwrap_or_default()
        )
    }
}

fn refuse_conflicting_directory_structures(
    table_url: &ListingTableUrl,
    entries: &[DiscoveredEntry],
) -> Result<()> {
    let mut bases: Vec<String> = Vec::new();
    for (_, _, _, base) in entries {
        let lowered = string_lowered(&base_display_path(table_url, base));
        if !bases.contains(&lowered) {
            bases.push(lowered);
        }
    }
    if bases.len() < 2 {
        return Ok(());
    }
    Err(Error::Iceberg(format!(
        "[CONFLICTING_DIRECTORY_STRUCTURES] Conflicting directory structures detected.\n\
         Suspicious paths:\n\n\t{}\n\n\
         If provided paths are partition directories, please set \"basePath\" in the options \
         of the data source to specify the root directory of the table.\n\
         If there are multiple root directories, please load them separately and then union \
         them. SQLSTATE: KD009",
        bases.join("\n\t")
    )))
}

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
        let Some((&file_name, dirs)) = relative.split_last() else {
            continue;
        };
        let is_parquet = file_name.ends_with(".parquet");
        let is_hidden = is_hidden_file_name(file_name)
            || dirs
                .iter()
                .any(|segment| is_hidden_directory_segment(segment));
        if is_hidden {
            if is_parquet {
                had_hidden_parquet = true;
            }
            continue;
        }
        if !is_parquet {
            continue;
        }
        let depth = relative.len();
        let (specs, base) = parse_relative_specs(&relative, meta.location.as_ref())?;
        visible.push((meta, specs, depth, base));
    }
    if visible.is_empty() {
        return read_parquet_listing_fallback(context, &resolved).await;
    }
    let any_partitioned = visible.iter().any(|(_, specs, _, _)| !specs.is_empty());
    let (kept, footer): (Vec<DiscoveredEntry>, Option<ObjectMeta>) = if any_partitioned {
        refuse_conflicting_directory_structures(&table_url, &visible)?;
        let footer = footer_file(&visible).cloned();
        let partitioned: Vec<DiscoveredEntry> = visible
            .into_iter()
            .filter(|(_, specs, _, _)| !specs.is_empty())
            .collect();
        (partitioned, footer)
    } else {
        let root_only: Vec<DiscoveredEntry> = visible
            .into_iter()
            .filter(|(_, _, depth, _)| *depth == 1)
            .collect();
        let footer = footer_file(&root_only).cloned();
        refuse_zero_byte_footer(footer.as_ref())?;
        if !had_hidden_parquet {
            return read_parquet_listing_fallback(context, &resolved).await;
        }
        if root_only.is_empty() {
            return Err(Error::Analysis(format!(
                "Error during planning: No files found at {resolved}"
            )));
        }
        (root_only, footer)
    };
    let mut seen_norm: Vec<Vec<String>> = Vec::new();
    let mut display_by_norm: HashMap<Vec<String>, (Vec<String>, PathBuf)> = HashMap::new();
    for (meta, specs, _, _) in &kept {
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
    refuse_zero_byte_footer(footer.as_ref())?;
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
            .map(|(_, specs, _, _)| specs.get(col_index).and_then(|(_, val)| val.as_deref()))
            .collect();
        let data_type = infer_column_data_type(&col_values, session_zone, type_inference);
        partition_fields.push(Arc::new(Field::new(name, data_type, true)));
    }
    let scanned: Vec<DiscoveredEntry> = kept
        .into_iter()
        .filter(|(meta, _, _, _)| meta.size > 0)
        .collect();
    let mut kept_metas: Vec<ObjectMeta> =
        scanned.iter().map(|(meta, _, _, _)| meta.clone()).collect();
    if kept_metas.is_empty() {
        kept_metas.extend(footer);
    }
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
    let partitioned_files: Vec<PartitionedFile> = scanned
        .into_iter()
        .map(|(meta, specs, _, _)| {
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
