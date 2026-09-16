use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, StringBuilder, new_null_array,
};
use arrow::compute::cast as arrow_cast;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone as _, Utc};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::DataFrame;
use orc_rust::projection::ProjectionMask;
use orc_rust::schema::TimestampPrecision;
use orc_rust::{ArrowReader, ArrowReaderBuilder};

use crate::orc_footer::file_writer_tz;
use crate::orc_schema::{apply_user_orc_schema, infer_orc_schema, orc_types_equal};
use crate::partition_discovery::{
    DiscoveredPartitions, PartitionValue, canonical_partition_text, discover_partitions,
    finish_partition_columns, partition_path_specs,
};
use crate::text_glob::{expand_text_glob, has_glob_meta, is_hidden_name, match_file_name_glob};
use crate::{Error, Result, engine_err};

const ORC_BATCH_ROWS: usize = 8192;

const ORC_SCAN_PARTITIONS: usize = 8;

pub(crate) const ORC_TZ_UTC: &str = "UTC";

#[derive(Debug, Clone, Default)]
pub struct OrcReadOptions {
    pub merge_schema: bool,
    pub path_glob_filter: Option<String>,
    pub recursive_file_lookup: bool,
    pub modified_before: Option<String>,
    pub modified_after: Option<String>,
    pub base_path: Option<String>,
    pub ignore_corrupt_files: bool,
    pub user_schema: Option<Vec<(String, String)>>,
}

fn missing_orc_path(path: &str) -> Error {
    Error::Analysis(format!(
        "[PATH_NOT_FOUND] Path does not exist: file:{path}. SQLSTATE: 42K03"
    ))
}

pub(crate) fn orc_schema_error() -> Error {
    Error::Analysis(
        "[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for ORC. It must be specified manually. SQLSTATE: 42KD9".to_string(),
    )
}

pub(crate) fn orc_footer_error(path: &Path, detail: &str) -> Error {
    Error::Analysis(format!(
        "[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER] Encountered error while reading file file:{}. Could not read footer. {detail} SQLSTATE: KD001",
        path.display()
    ))
}

pub(crate) fn orc_merge_error(name: &str, first: &DataType, next: &DataType) -> Error {
    Error::Analysis(format!(
        "[CANNOT_MERGE_SCHEMAS] Failed to merge ORC schemas: column `{name}` has conflicting types ({first} and {next})"
    ))
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
}

fn parse_spark_modified(raw: &str, what: &str) -> Result<std::time::SystemTime> {
    const FORMATS: [&str; 5] = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];
    for format in FORMATS {
        if let Ok(naive) = NaiveDateTime::parse_from_str(raw, format) {
            return Ok(Utc.from_utc_datetime(&naive).into());
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let naive = date.and_hms_opt(0, 0, 0).ok_or_else(|| {
            Error::Analysis(format!("orc read cannot parse {what} timestamp {raw:?}"))
        })?;
        return Ok(Utc.from_utc_datetime(&naive).into());
    }
    Err(Error::Analysis(format!(
        "orc read cannot parse {what} timestamp {raw:?}"
    )))
}

fn file_modified_time(path: &Path) -> Result<std::time::SystemTime> {
    path.metadata()
        .and_then(|meta| meta.modified())
        .map_err(|error| {
            Error::Analysis(format!("orc read cannot stat {}: {error}", path.display()))
        })
}

fn keep_modified(
    files: Vec<PathBuf>,
    before: Option<std::time::SystemTime>,
    after: Option<std::time::SystemTime>,
) -> Result<Vec<PathBuf>> {
    let mut kept = Vec::with_capacity(files.len());
    for file in files {
        let modified = file_modified_time(&file)?;
        if before.is_some_and(|bound| modified >= bound) {
            continue;
        }
        if after.is_some_and(|bound| modified <= bound) {
            continue;
        }
        kept.push(file);
    }
    Ok(kept)
}

fn is_orc_data_file(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|text| text.len() > 4 && text[text.len() - 4..].eq_ignore_ascii_case(".orc"))
}

fn push_orc_dir(dir: &Path, display: &str, recursive: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let entries = std::fs::read_dir(&current).map_err(|error| {
            Error::Analysis(format!(
                "orc read cannot list directory {display:?}: {error}"
            ))
        })?;
        let mut ordered: Vec<std::fs::DirEntry> = Vec::new();
        for entry in entries {
            ordered.push(entry.map_err(|error| {
                Error::Analysis(format!(
                    "orc read cannot list directory {display:?}: {error}"
                ))
            })?);
        }
        ordered.sort_by_key(std::fs::DirEntry::path);
        for entry in ordered.into_iter().rev() {
            if is_hidden_name(&entry.file_name()) {
                continue;
            }
            let candidate = entry.path();
            let kind = entry.file_type().map_err(|error| {
                Error::Analysis(format!(
                    "orc read cannot list directory {display:?}: {error}"
                ))
            })?;
            if kind.is_file() || (kind.is_symlink() && candidate.is_file()) {
                if is_orc_data_file(&entry.file_name()) {
                    out.push(candidate);
                }
            } else if kind.is_dir() {
                let partitioned = entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains('='));
                if partitioned || recursive {
                    stack.push(candidate);
                }
            }
        }
    }
    Ok(())
}

fn keep_partitioned_only(root: &Path, files: Vec<PathBuf>) -> Vec<PathBuf> {
    let any = files
        .iter()
        .any(|file| !partition_path_specs(root, file).is_empty());
    if !any {
        return files;
    }
    files
        .into_iter()
        .filter(|file| !partition_path_specs(root, file).is_empty())
        .collect()
}

pub(crate) fn expand_orc_paths(
    paths: &[String],
    options: &OrcReadOptions,
    session_zone: &str,
) -> Result<(Vec<PathBuf>, DiscoveredPartitions)> {
    let before = options
        .modified_before
        .as_deref()
        .map(|raw| parse_spark_modified(raw, "modifiedBefore"))
        .transpose()?;
    let after = options
        .modified_after
        .as_deref()
        .map(|raw| parse_spark_modified(raw, "modifiedAfter"))
        .transpose()?;
    let mut files: Vec<PathBuf> = Vec::new();
    let mut partitions = DiscoveredPartitions::default();
    for path in paths {
        let (mut item_files, item_partitions) =
            expand_single_orc_path(path, options, session_zone)?;
        files.append(&mut item_files);
        partitions = merge_orc_partitions(partitions, item_partitions);
    }
    files.sort();
    finish_expansion(files, partitions, options, before, after)
}

fn expand_single_orc_path(
    path: &str,
    options: &OrcReadOptions,
    session_zone: &str,
) -> Result<(Vec<PathBuf>, DiscoveredPartitions)> {
    if is_remote_path(path) {
        return Err(Error::Analysis(format!(
            "orc read over {path:?} is not supported by repark yet (local files and directories only)"
        )));
    }
    if has_glob_meta(path) {
        let files = expand_text_glob(path)?;
        let data: Vec<PathBuf> = files
            .into_iter()
            .filter(|file| file.file_name().is_some_and(is_orc_data_file))
            .collect();
        return base_or_default_partitions(data, options, session_zone);
    }
    let fs_path = Path::new(path);
    if fs_path.is_file() {
        return Ok((vec![fs_path.to_path_buf()], DiscoveredPartitions::default()));
    }
    if fs_path.is_dir() {
        let mut listed: Vec<PathBuf> = Vec::new();
        push_orc_dir(fs_path, path, options.recursive_file_lookup, &mut listed)?;
        listed.sort();
        let kept = keep_partitioned_only(fs_path, listed);
        if let Some(base) = options.base_path.as_deref() {
            let root = Path::new(base);
            let partitions = discover_partitions(root, &kept, session_zone)?;
            return Ok((kept, partitions));
        }
        let partitions = discover_partitions(fs_path, &kept, session_zone)?;
        return Ok((kept, partitions));
    }
    Err(missing_orc_path(path))
}

fn base_or_default_partitions(
    files: Vec<PathBuf>,
    options: &OrcReadOptions,
    session_zone: &str,
) -> Result<(Vec<PathBuf>, DiscoveredPartitions)> {
    if let Some(base) = options.base_path.as_deref() {
        let root = Path::new(base);
        let kept = keep_partitioned_only(root, files);
        let partitions = discover_partitions(root, &kept, session_zone)?;
        return Ok((kept, partitions));
    }
    Ok((files, DiscoveredPartitions::default()))
}

fn merge_orc_partitions(
    mut merged: DiscoveredPartitions,
    next: DiscoveredPartitions,
) -> DiscoveredPartitions {
    if merged.fields.is_empty() {
        return next;
    }
    if next.fields.is_empty() {
        return merged;
    }
    let mut position: HashMap<String, usize> = HashMap::new();
    for (index, field) in merged.fields.iter().enumerate() {
        position.insert(field.name().to_lowercase(), index);
    }
    for field in &next.fields {
        position
            .entry(field.name().to_lowercase())
            .or_insert_with(|| {
                merged.fields.push(field.clone());
                merged.fields.len() - 1
            });
    }
    let width = merged.fields.len();
    for values in merged.values.values_mut() {
        values.resize(width, PartitionValue::Null);
    }
    for raws in merged.raw.values_mut() {
        raws.resize(width, None);
    }
    let mut next_position: HashMap<String, usize> = HashMap::new();
    for (index, field) in next.fields.iter().enumerate() {
        next_position.insert(field.name().to_lowercase(), index);
    }
    for (file, values) in next.values {
        let mut aligned: Vec<PartitionValue> = Vec::with_capacity(width);
        for field in &merged.fields {
            let value = next_position
                .get(&field.name().to_lowercase())
                .and_then(|index| values.get(*index).cloned())
                .unwrap_or(PartitionValue::Null);
            aligned.push(value);
        }
        merged.values.insert(file, aligned);
    }
    for (file, raws) in next.raw {
        let mut aligned: Vec<Option<String>> = Vec::with_capacity(width);
        for field in &merged.fields {
            let raw = next_position
                .get(&field.name().to_lowercase())
                .and_then(|index| raws.get(*index).cloned())
                .unwrap_or(None);
            aligned.push(raw);
        }
        merged.raw.insert(file, aligned);
    }
    merged
}

fn finish_expansion(
    files: Vec<PathBuf>,
    partitions: DiscoveredPartitions,
    options: &OrcReadOptions,
    before: Option<std::time::SystemTime>,
    after: Option<std::time::SystemTime>,
) -> Result<(Vec<PathBuf>, DiscoveredPartitions)> {
    let mut kept = files;
    if let Some(filter) = options.path_glob_filter.as_deref() {
        kept.retain(|file| {
            file.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| match_file_name_glob(filter, name))
        });
    }
    kept = keep_modified(kept, before, after)?;
    if kept.is_empty() {
        return Err(orc_schema_error());
    }
    Ok((kept, partitions))
}

fn group_orc_files(files: &[PathBuf]) -> Vec<Vec<PathBuf>> {
    if files.is_empty() {
        return vec![Vec::new()];
    }
    let width = files.len().div_ceil(ORC_SCAN_PARTITIONS).max(1);
    files.chunks(width).map(<[PathBuf]>::to_vec).collect()
}

#[derive(Debug, Clone)]
pub(crate) struct OrcTableProvider {
    files: Vec<PathBuf>,
    data_schema: SchemaRef,
    schema: SchemaRef,
    partition_fields: Vec<Field>,
    partition_values: Arc<HashMap<PathBuf, Vec<PartitionValue>>>,
}

#[async_trait]

impl TableProvider for OrcTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            None => Arc::clone(&self.schema),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };
        if projection.is_some_and(|indices| {
            indices
                .iter()
                .any(|index| *index >= self.schema.fields().len())
        }) {
            return Err(DataFusionError::Execution(
                "orc scan got an out-of-range projection".to_string(),
            ));
        }
        let data_count = self.data_schema.fields().len();
        let mut need_data: Vec<usize> = Vec::new();
        let mut need_parts: Vec<usize> = Vec::new();
        match projection {
            None => {
                need_data = (0..data_count).collect();
                need_parts = (0..self.partition_fields.len()).collect();
            }
            Some(indices) => {
                for index in indices {
                    if *index < data_count {
                        if !need_data.contains(index) {
                            need_data.push(*index);
                        }
                    } else {
                        let part = index - data_count;
                        if !need_parts.contains(&part) {
                            need_parts.push(part);
                        }
                    }
                }
            }
        }
        if need_data.is_empty() && data_count > 0 {
            need_data.push(0);
        }
        let order: Vec<OrcOutput> = match projection {
            None => (0..data_count)
                .map(OrcOutput::Data)
                .chain((0..self.partition_fields.len()).map(OrcOutput::Part))
                .collect(),
            Some(indices) => indices
                .iter()
                .map(|index| {
                    if *index < data_count {
                        OrcOutput::Data(*index)
                    } else {
                        OrcOutput::Part(index - data_count)
                    }
                })
                .collect(),
        };
        let part_types: Vec<DataType> = need_parts
            .iter()
            .map(|part| self.partition_fields[*part].data_type().clone())
            .collect();
        let partitions: Vec<Arc<dyn PartitionStream>> = group_orc_files(&self.files)
            .into_iter()
            .map(|files| {
                Arc::new(OrcPartition {
                    files,
                    data_schema: Arc::clone(&self.data_schema),
                    need_data: need_data.clone(),
                    need_parts: need_parts.clone(),
                    part_types: part_types.clone(),
                    output_schema: Arc::clone(&output_schema),
                    order: order.clone(),
                    partition_values: Arc::clone(&self.partition_values),
                    limit,
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            partitions,
            None,
            vec![],
            false,
            limit,
        )?))
    }
}

#[derive(Debug, Clone, Copy)]
enum OrcOutput {
    Data(usize),
    Part(usize),
}

#[derive(Debug)]
struct OrcPartition {
    files: Vec<PathBuf>,
    data_schema: SchemaRef,
    need_data: Vec<usize>,
    need_parts: Vec<usize>,
    part_types: Vec<DataType>,
    output_schema: SchemaRef,
    order: Vec<OrcOutput>,
    partition_values: Arc<HashMap<PathBuf, Vec<PartitionValue>>>,
    limit: Option<usize>,
}

impl PartitionStream for OrcPartition {
    fn schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let outcome = self.read_partitions();
        match outcome {
            Ok(batches) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                futures::stream::iter(batches.into_iter().map(Ok)),
            )),
            Err(error) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                futures::stream::once(futures::future::ready(Err(error))),
            )),
        }
    }
}

fn orc_projection_names(
    path: &Path,
    need: &[usize],
    data_schema: &Schema,
) -> std::result::Result<(Vec<String>, HashSet<String>), DataFusionError> {
    let file = File::open(path).map_err(|error| {
        DataFusionError::Execution(format!("orc read cannot open {}: {error}", path.display()))
    })?;
    let builder = ArrowReaderBuilder::try_new(file).map_err(|error| {
        DataFusionError::Execution(orc_footer_error(path, &format!("{error}")).to_string())
    })?;
    let root = builder.file_metadata().root_data_type().clone();
    let mut present: HashMap<String, String> = HashMap::new();
    for child in root.children() {
        present.insert(child.name().to_lowercase(), child.name().to_string());
    }
    let mut names: Vec<String> = Vec::new();
    let mut missing: HashSet<String> = HashSet::new();
    for position in need {
        let want = data_schema.field(*position).name().to_lowercase();
        match present.get(&want) {
            Some(exact) => names.push(exact.clone()),
            None => {
                missing.insert(want);
            }
        }
    }
    Ok((names, missing))
}

fn attr_struct_names(fields: &Fields) -> Vec<&str> {
    fields.iter().map(|field| field.name().as_str()).collect()
}

fn writer_wall_to_utc(value: i64, zone: Tz) -> std::result::Result<i64, DataFusionError> {
    let wall = DateTime::from_timestamp_micros(value)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("orc read found an out-of-range timestamp {value}"))
        })?
        .naive_utc();
    zone.from_local_datetime(&wall)
        .single()
        .or_else(|| zone.from_local_datetime(&wall).earliest())
        .map(|moment| moment.timestamp_micros())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "orc read cannot place timestamp {value} in its writer zone"
            ))
        })
}

fn correct_writer_wall(
    column: &ArrayRef,
    target: &DataType,
    zone: Option<Tz>,
) -> std::result::Result<ArrayRef, DataFusionError> {
    let Some(zone) = zone else {
        return arrow_cast(column, target).map_err(DataFusionError::from);
    };
    let micros = column
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("orc read found a non-microsecond timestamp".to_string())
        })?;
    let mut fixed: Vec<Option<i64>> = Vec::with_capacity(micros.len());
    for value in micros {
        match value {
            None => fixed.push(None),
            Some(stamp) => fixed.push(Some(writer_wall_to_utc(stamp, zone)?)),
        }
    }
    let array = arrow::array::TimestampMicrosecondArray::from_iter(fixed);
    let fixed_ref: ArrayRef = Arc::new(array);
    arrow_cast(&fixed_ref, target).map_err(DataFusionError::from)
}

fn convert_orc_column(
    column: &ArrayRef,
    target: &DataType,
    zone: Option<Tz>,
) -> std::result::Result<ArrayRef, DataFusionError> {
    let source = column.data_type();
    if orc_types_equal(source, target) {
        return Ok(Arc::clone(column));
    }
    match (source, target) {
        (DataType::Timestamp(batch_unit, None), DataType::Timestamp(provider_unit, Some(_)))
            if batch_unit == provider_unit =>
        {
            correct_writer_wall(column, target, zone)
        }
        (DataType::Timestamp(batch_unit, Some(_)), DataType::Timestamp(provider_unit, None))
            if batch_unit == provider_unit =>
        {
            arrow_cast(column, target).map_err(DataFusionError::from)
        }
        (DataType::Int64, DataType::Timestamp(TimeUnit::Microsecond, None))
        | (DataType::Int32, DataType::Date32) => {
            arrow_cast(column, target).map_err(DataFusionError::from)
        }
        (DataType::Struct(batch_fields), DataType::Struct(provider_fields))
            if attr_struct_names(batch_fields) == attr_struct_names(provider_fields) =>
        {
            let batch_struct = column
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("orc read lost its struct".to_string())
                })?;
            let mut children: Vec<ArrayRef> = Vec::with_capacity(provider_fields.len());
            for (position, field) in provider_fields.iter().enumerate() {
                children.push(convert_orc_column(
                    batch_struct.column(position),
                    field.data_type(),
                    zone,
                )?);
            }
            arrow::array::StructArray::try_new(
                provider_fields.clone(),
                children,
                batch_struct.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(DataFusionError::from)
        }
        (DataType::List(_), DataType::List(provider_inner)) => {
            let batch_list = column
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| DataFusionError::Execution("orc read lost its list".to_string()))?;
            let values = convert_orc_column(batch_list.values(), provider_inner.data_type(), zone)?;
            arrow::array::ListArray::try_new(
                Arc::clone(provider_inner),
                batch_list.offsets().clone(),
                values,
                batch_list.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(DataFusionError::from)
        }
        (DataType::Map(_, left_sorted), DataType::Map(provider_entries, right_sorted))
            if left_sorted == right_sorted
                && matches!(provider_entries.data_type(), DataType::Struct(_)) =>
        {
            let batch_map = column
                .as_any()
                .downcast_ref::<arrow::array::MapArray>()
                .ok_or_else(|| DataFusionError::Execution("orc read lost its map".to_string()))?;
            let batch_entries: ArrayRef = Arc::new(batch_map.entries().clone());
            let entries = convert_orc_column(&batch_entries, provider_entries.data_type(), zone)?;
            let entries_struct = entries
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| DataFusionError::Execution("orc read lost its map".to_string()))?
                .clone();
            arrow::array::MapArray::try_new(
                Arc::clone(provider_entries),
                batch_map.offsets().clone(),
                entries_struct,
                batch_map.nulls().cloned(),
                *right_sorted,
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(DataFusionError::from)
        }
        _ => Err(DataFusionError::Execution(format!(
            "orc read found conflicting types for `{source}` and `{target}`"
        ))),
    }
}

fn align_orc_batch(
    batch: &RecordBatch,
    data_schema: &Schema,
    need: &[usize],
    writer_zone: Option<Tz>,
) -> std::result::Result<Vec<ArrayRef>, DataFusionError> {
    let mut by_name: HashMap<String, ArrayRef> = HashMap::new();
    for position in 0..batch.num_columns() {
        by_name.insert(
            batch.schema().field(position).name().to_lowercase(),
            batch.column(position).clone(),
        );
    }
    let mut aligned: Vec<ArrayRef> = Vec::with_capacity(data_schema.fields().len());
    for field in data_schema.fields() {
        match by_name.get(&field.name().to_lowercase()) {
            Some(column) => {
                aligned.push(
                    convert_orc_column(column, field.data_type(), writer_zone).map_err(
                        |error| match error {
                            DataFusionError::Execution(detail)
                                if detail.starts_with("orc read found conflicting types for `") =>
                            {
                                DataFusionError::Execution(format!(
                                    "orc read found conflicting types for column `{}`",
                                    field.name()
                                ))
                            }
                            other => other,
                        },
                    )?,
                );
            }
            None => aligned.push(new_null_array(field.data_type(), batch.num_rows())),
        }
    }
    Ok(need
        .iter()
        .map(|position| Arc::clone(&aligned[*position]))
        .collect())
}

fn part_arrays_for_rows(
    values: &[PartitionValue],
    need_parts: &[usize],
    part_types: &[DataType],
    rows: usize,
) -> std::result::Result<Vec<ArrayRef>, DataFusionError> {
    let texts: Vec<Option<String>> = values.iter().map(canonical_partition_text).collect();
    let mut builders: Vec<StringBuilder> =
        part_types.iter().map(|_| StringBuilder::new()).collect();
    for _ in 0..rows {
        for (slot, part) in need_parts.iter().enumerate() {
            match texts.get(*part).and_then(|value| value.as_ref()) {
                None => builders[slot].append_null(),
                Some(text) => builders[slot].append_value(text),
            }
        }
    }
    finish_partition_columns(builders, part_types, rows)
}

fn assemble_orc_batch(
    aligned: &[ArrayRef],
    need: &[usize],
    parts: &[ArrayRef],
    need_parts: &[usize],
    order: &[OrcOutput],
    output_schema: &SchemaRef,
) -> std::result::Result<RecordBatch, DataFusionError> {
    let mut position_of: HashMap<usize, usize> = HashMap::new();
    for (slot, data) in need.iter().enumerate() {
        position_of.insert(*data, slot);
    }
    let mut part_position: HashMap<usize, usize> = HashMap::new();
    for (slot, part) in need_parts.iter().enumerate() {
        part_position.insert(*part, slot);
    }
    if order.is_empty() {
        let mut rows = 0;
        if let Some(column) = aligned.first() {
            rows = column.len();
        } else if let Some(column) = parts.first() {
            rows = column.len();
        }
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        return RecordBatch::try_new_with_options(Arc::clone(output_schema), Vec::new(), &options)
            .map_err(DataFusionError::from);
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(order.len());
    for output in order {
        match output {
            OrcOutput::Data(data) => {
                let slot = position_of.get(data).ok_or_else(|| {
                    DataFusionError::Internal("orc scan lost its data column".to_string())
                })?;
                columns.push(Arc::clone(&aligned[*slot]));
            }
            OrcOutput::Part(part) => {
                let slot = part_position.get(part).ok_or_else(|| {
                    DataFusionError::Internal("orc scan lost its partition column".to_string())
                })?;
                columns.push(Arc::clone(&parts[*slot]));
            }
        }
    }
    RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(DataFusionError::from)
}

impl OrcPartition {
    fn read_partitions(&self) -> DataFusionResult<Vec<RecordBatch>> {
        let mut out: Vec<RecordBatch> = Vec::new();
        let mut emitted = 0usize;
        let empty_values: Vec<PartitionValue> = Vec::new();
        for file in &self.files {
            if self.limit.is_some_and(|max| emitted >= max) {
                break;
            }
            let writer_zone: Option<Tz> = file_writer_tz(file).and_then(|name| name.parse().ok());
            let (names, _) = orc_projection_names(file, &self.need_data, &self.data_schema)?;
            let source = File::open(file).map_err(|error| {
                DataFusionError::Execution(format!(
                    "orc read cannot open {}: {error}",
                    file.display()
                ))
            })?;
            let reader = ArrowReaderBuilder::try_new(source)
                .map_err(|error| {
                    DataFusionError::Execution(
                        orc_footer_error(file, &format!("{error}")).to_string(),
                    )
                })?
                .with_timestamp_precision(TimestampPrecision::Microsecond);
            let root = reader.file_metadata().root_data_type().clone();
            let mask = ProjectionMask::named_roots(&root, &names);
            let values = self.partition_values.get(file).unwrap_or(&empty_values);
            let stream: ArrowReader<File> = reader
                .with_projection(mask)
                .with_batch_size(ORC_BATCH_ROWS)
                .build();
            for batch in stream {
                let batch = batch.map_err(|error| {
                    DataFusionError::Execution(format!(
                        "orc read of {} failed: {error}",
                        file.display()
                    ))
                })?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let mut rows = batch.num_rows();
                if let Some(max) = self.limit {
                    let remaining = max.saturating_sub(emitted);
                    if remaining == 0 {
                        break;
                    }
                    rows = rows.min(remaining);
                }
                let batch = batch.slice(0, rows);
                let aligned =
                    align_orc_batch(&batch, &self.data_schema, &self.need_data, writer_zone)?;
                let parts = part_arrays_for_rows(values, &self.need_parts, &self.part_types, rows)?;
                out.push(assemble_orc_batch(
                    &aligned,
                    &self.need_data,
                    &parts,
                    &self.need_parts,
                    &self.order,
                    &self.output_schema,
                )?);
                emitted += rows;
            }
        }
        Ok(out)
    }
}

impl crate::ReparkSession {
    #[expect(
        clippy::unused_async,
        reason = "symmetric with read_text; remote reads will await"
    )]
    #[allow(clippy::missing_errors_doc)]
    pub async fn read_orc(&self, paths: &[String], options: OrcReadOptions) -> Result<DataFrame> {
        let session_zone = self.session_time_zone();
        let canonical = crate::canonical_session_zone_id(session_zone.id());
        let zone = canonical.as_str();
        let (files, partitions) = expand_orc_paths(paths, &options, zone)?;
        let (data_schema, valid) =
            infer_orc_schema(&files, options.merge_schema, options.ignore_corrupt_files)?;
        let mut fields: Vec<Field> = data_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.extend(partitions.fields.iter().cloned());
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let data = Arc::clone(&data_schema);
        let part_fields = partitions.fields.clone();
        let provider = Arc::new(OrcTableProvider {
            files: valid,
            data_schema,
            schema,
            partition_fields: partitions.fields,
            partition_values: Arc::new(partitions.values),
        });
        let frame = self.context().read_table(provider).map_err(engine_err)?;
        match options.user_schema {
            None => Ok(frame),
            Some(user) => apply_user_orc_schema(frame, &user, &data, &part_fields, zone),
        }
    }
}
