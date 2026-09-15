use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayBuilder, ArrayRef, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::exec_datafusion_err;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use datafusion::prelude::DataFrame;
use futures::Stream;

use crate::text_schema::apply_user_text_schema;
use crate::{Error, Result, engine_err};

const TEXT_BATCH_ROWS: usize = 8192;

const TEXT_READ_CHUNK: usize = 65536;

const TEXT_SCAN_PARTITIONS: usize = 8;

fn is_remote_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
}

use crate::partition_discovery::{
    DiscoveredPartitions, PartitionValue, TextRowSink, canonical_partition_text,
    discover_partitions, emit_text_row, finish_partition_columns, order_batch_columns,
    partition_path_specs, plan_partition_slots,
};
use crate::text_glob::{expand_text_glob, has_glob_meta, is_hidden_name};

fn push_text_dir(dir: &Path, display: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let entries = std::fs::read_dir(&current).map_err(|error| {
            Error::Analysis(format!(
                "text read cannot list directory {display:?}: {error}"
            ))
        })?;
        let mut ordered: Vec<std::fs::DirEntry> = Vec::new();
        for entry in entries {
            ordered.push(entry.map_err(|error| {
                Error::Analysis(format!(
                    "text read cannot list directory {display:?}: {error}"
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
                    "text read cannot list directory {display:?}: {error}"
                ))
            })?;
            if kind.is_file() || (kind.is_symlink() && candidate.is_file()) {
                out.push(candidate);
            } else if kind.is_dir()
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains('='))
            {
                stack.push(candidate);
            }
        }
    }
    Ok(())
}

fn missing_text_path(path: &str) -> Error {
    Error::Analysis(format!(
        "[PATH_NOT_FOUND] Path does not exist: file:{path}. SQLSTATE: 42K03"
    ))
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

pub(crate) fn expand_text_paths(
    path: &str,
    base_path: Option<&str>,
) -> Result<(Vec<PathBuf>, DiscoveredPartitions)> {
    if is_remote_path(path) {
        return Err(Error::Analysis(format!(
            "text read over {path:?} is not supported by repark yet (local files and directories only)"
        )));
    }
    if has_glob_meta(path) {
        let files = expand_text_glob(path)?;
        if files.is_empty() {
            return Err(missing_text_path(path));
        }
        if let Some(base) = base_path {
            let root = Path::new(base);
            let kept = keep_partitioned_only(root, files);
            let partitions = discover_partitions(root, &kept)?;
            return Ok((kept, partitions));
        }
        return Ok((files, DiscoveredPartitions::default()));
    }
    let fs_path = Path::new(path);
    if fs_path.is_file() {
        return Ok((vec![fs_path.to_path_buf()], DiscoveredPartitions::default()));
    }
    if fs_path.is_dir() {
        let mut files: Vec<PathBuf> = Vec::new();
        push_text_dir(fs_path, path, &mut files)?;
        files.sort();
        if let Some(base) = base_path {
            let root = Path::new(base);
            let kept = keep_partitioned_only(root, files);
            let partitions = discover_partitions(root, &kept)?;
            return Ok((kept, partitions));
        }
        let kept = keep_partitioned_only(fs_path, files);
        let partitions = discover_partitions(fs_path, &kept)?;
        return Ok((kept, partitions));
    }
    Err(missing_text_path(path))
}

fn group_text_files(files: &[PathBuf]) -> Vec<Vec<PathBuf>> {
    if files.is_empty() {
        return vec![Vec::new()];
    }
    let width = files.len().div_ceil(TEXT_SCAN_PARTITIONS).max(1);
    files.chunks(width).map(<[PathBuf]>::to_vec).collect()
}

#[derive(Debug, Clone)]
pub(crate) struct TextTableProvider {
    files: Vec<PathBuf>,
    wholetext: bool,
    line_sep: Option<String>,
    schema: SchemaRef,
    partition_fields: Vec<Field>,
    partition_values: Arc<HashMap<PathBuf, Vec<PartitionValue>>>,
}

#[async_trait]
impl TableProvider for TextTableProvider {
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
        let order: Vec<usize> = match projection {
            None => (0..self.schema.fields().len()).collect(),
            Some(indices) => indices.clone(),
        };
        let plan: Vec<Option<usize>> = order
            .into_iter()
            .map(|position| position.checked_sub(1))
            .collect();
        let types: Vec<DataType> = self
            .partition_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let (slots, included_types) =
            plan_partition_slots(&plan, self.partition_fields.len(), &types);
        let partitions: Vec<Arc<dyn PartitionStream>> = group_text_files(&self.files)
            .into_iter()
            .map(|files| {
                Arc::new(TextPartition {
                    files,
                    wholetext: self.wholetext,
                    line_sep: self.line_sep.clone(),
                    schema: Arc::clone(&output_schema),
                    plan: plan.clone(),
                    slots: slots.clone(),
                    included_types: included_types.clone(),
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

#[derive(Debug)]
struct TextPartition {
    files: Vec<PathBuf>,
    wholetext: bool,
    line_sep: Option<String>,
    schema: SchemaRef,
    plan: Vec<Option<usize>>,
    slots: Vec<Option<usize>>,
    included_types: Vec<DataType>,
    partition_values: Arc<HashMap<PathBuf, Vec<PartitionValue>>>,
    limit: Option<usize>,
}

impl PartitionStream for TextPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        if self.line_sep.as_deref().is_some_and(str::is_empty) {
            return Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::once(futures::future::ready(Err(exec_datafusion_err!(
                    "text lineSep must be a non-empty string"
                )))),
            ));
        }
        let chunk = vec![0; TEXT_READ_CHUNK];
        let value_included = self.plan.contains(&None);
        let included = self.included_types.len();
        Box::pin(TextLineStream {
            files: self.files.clone(),
            file_index: 0,
            current: None,
            reader: None,
            chunk,
            carry: Vec::new(),
            value_builder: value_included.then(StringBuilder::new),
            part_builders: (0..included).map(|_| StringBuilder::new()).collect(),
            part_slots: self.slots.clone(),
            part_current: Vec::new(),
            part_types: self.included_types.clone(),
            part_plan: self.plan.clone(),
            partition_values: Arc::clone(&self.partition_values),
            blank_rows: 0,
            wholetext: self.wholetext,
            separator: self.line_sep.clone().map(String::into_bytes),
            schema: Arc::clone(&self.schema),
            limit: self.limit,
            emitted: 0,
            done: false,
            bytes_read: 0,
        })
    }
}

struct TextLineStream {
    files: Vec<PathBuf>,
    file_index: usize,
    current: Option<PathBuf>,
    reader: Option<File>,
    chunk: Vec<u8>,
    carry: Vec<u8>,
    value_builder: Option<StringBuilder>,
    part_builders: Vec<StringBuilder>,
    part_slots: Vec<Option<usize>>,
    part_current: Vec<Option<String>>,
    part_types: Vec<DataType>,
    part_plan: Vec<Option<usize>>,
    partition_values: Arc<HashMap<PathBuf, Vec<PartitionValue>>>,
    blank_rows: usize,
    wholetext: bool,
    separator: Option<Vec<u8>>,
    schema: SchemaRef,
    limit: Option<usize>,
    emitted: usize,
    done: bool,
    bytes_read: usize,
}

impl TextLineStream {
    fn pending_rows(&self) -> usize {
        if let Some(builder) = self.value_builder.as_ref() {
            builder.len()
        } else if let Some(builder) = self.part_builders.first() {
            builder.len()
        } else {
            self.blank_rows
        }
    }

    fn limit_reached(&self) -> bool {
        self.limit
            .is_some_and(|max| self.emitted + self.pending_rows() >= max)
    }

    fn open_next(&mut self) -> DataFusionResult<bool> {
        let Some(path) = self.files.get(self.file_index).cloned() else {
            return Ok(false);
        };
        let file = File::open(&path).map_err(|error| {
            DataFusionError::Execution(format!("text read cannot open {}: {error}", path.display()))
        })?;
        let width = self.part_slots.len();
        let row = self.partition_values.get(&path).map_or_else(
            || vec![None; width],
            |values| values.iter().map(canonical_partition_text).collect(),
        );
        self.current = Some(path);
        self.reader = Some(file);
        self.part_current = row;
        self.carry.clear();
        Ok(true)
    }

    fn slurp_current(&mut self) -> DataFusionResult<()> {
        let path = self.current.clone().unwrap_or_default();
        let reader = self.reader.as_mut().ok_or_else(|| {
            DataFusionError::Internal("text read lost its file handle".to_string())
        })?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).map_err(|error| {
            DataFusionError::Execution(format!("text read of {} failed: {error}", path.display()))
        })?;
        let mut sink = TextRowSink {
            value: self.value_builder.as_mut(),
            parts: &mut self.part_builders,
            slots: &self.part_slots,
            current: &self.part_current,
            blank: &mut self.blank_rows,
        };
        emit_text_row(&mut sink, &bytes, self.limit, self.emitted);
        self.reader = None;
        self.current = None;
        self.file_index += 1;
        Ok(())
    }

    fn scan_carry(&mut self, final_scan: bool) {
        if self.separator.is_none() {
            self.scan_universal(final_scan);
        } else {
            self.scan_custom(final_scan);
        }
    }

    fn scan_universal(&mut self, final_scan: bool) {
        let Self {
            carry,
            value_builder,
            part_builders,
            part_slots,
            part_current,
            blank_rows,
            limit,
            emitted,
            ..
        } = self;
        let mut start = 0usize;
        let mut index = 0usize;
        let hold_trailing_cr = !final_scan && carry.last().is_some_and(|last| *last == b'\r');
        let end = if hold_trailing_cr {
            carry.len().saturating_sub(1)
        } else {
            carry.len()
        };
        let mut sink = TextRowSink {
            value: value_builder.as_mut(),
            parts: part_builders,
            slots: part_slots,
            current: part_current,
            blank: blank_rows,
        };
        while index < end {
            let byte = carry[index];
            if byte == b'\n' {
                if !emit_text_row(&mut sink, &carry[start..index], *limit, *emitted) {
                    break;
                }
                index += 1;
                start = index;
            } else if byte == b'\r' {
                let paired = index + 1 < carry.len() && carry[index + 1] == b'\n';
                if paired {
                    if !emit_text_row(&mut sink, &carry[start..index], *limit, *emitted) {
                        break;
                    }
                    index += 2;
                    start = index;
                } else if index + 1 == carry.len() && !final_scan {
                    break;
                } else {
                    if !emit_text_row(&mut sink, &carry[start..index], *limit, *emitted) {
                        break;
                    }
                    index += 1;
                    start = index;
                }
            } else {
                index += 1;
            }
        }
        carry.drain(..start);
    }

    fn scan_custom(&mut self, final_scan: bool) {
        let Self {
            carry,
            separator,
            value_builder,
            part_builders,
            part_slots,
            part_current,
            blank_rows,
            limit,
            emitted,
            ..
        } = self;
        let empty: Vec<u8> = Vec::new();
        let active = separator.as_ref().unwrap_or(&empty);
        let hold = if final_scan {
            0
        } else {
            active.len().saturating_sub(1)
        };
        let end = carry.len().saturating_sub(hold);
        let mut start = 0usize;
        let mut index = 0usize;
        let mut sink = TextRowSink {
            value: value_builder.as_mut(),
            parts: part_builders,
            slots: part_slots,
            current: part_current,
            blank: blank_rows,
        };
        while index + active.len() <= end {
            if &carry[index..index + active.len()] == active.as_slice() {
                if !emit_text_row(&mut sink, &carry[start..index], *limit, *emitted) {
                    break;
                }
                index += active.len();
                start = index;
            } else {
                index += 1;
            }
        }
        carry.drain(..start);
    }

    fn finish_file(&mut self) {
        if !self.carry.is_empty() {
            let tail = std::mem::take(&mut self.carry);
            let mut sink = TextRowSink {
                value: self.value_builder.as_mut(),
                parts: &mut self.part_builders,
                slots: &self.part_slots,
                current: &self.part_current,
                blank: &mut self.blank_rows,
            };
            emit_text_row(&mut sink, &tail, self.limit, self.emitted);
        }
        self.carry.clear();
    }

    fn take_batch(&mut self) -> DataFusionResult<RecordBatch> {
        let pending = self.pending_rows();
        let mut count = pending;
        if let Some(limit) = self.limit {
            let remaining = limit.saturating_sub(self.emitted);
            if count > remaining {
                count = remaining;
                self.done = true;
            }
        }
        self.emitted += count;
        if self.value_builder.is_none() && self.part_builders.is_empty() {
            self.blank_rows -= count;
            return RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(count)),
            )
            .map_err(DataFusionError::from);
        }
        let value_array: Option<ArrayRef> = self.value_builder.as_mut().map(|builder| {
            let finished = std::mem::replace(builder, StringBuilder::new()).finish();
            let array: ArrayRef = if finished.len() == count {
                Arc::new(finished)
            } else {
                Arc::new(finished.slice(0, count))
            };
            array
        });
        let builders = std::mem::take(&mut self.part_builders);
        let finished_parts = finish_partition_columns(builders, &self.part_types, count)?;
        self.part_builders = finished_parts
            .iter()
            .map(|_| StringBuilder::new())
            .collect();
        let columns = order_batch_columns(
            &self.part_plan,
            value_array.as_ref(),
            &finished_parts,
            &self.part_slots,
        )?;
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(DataFusionError::from)
    }
}

impl Stream for TextLineStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            if let Some(limit) = this.limit
                && this.emitted >= limit
            {
                this.done = true;
                return Poll::Ready(None);
            }
            let pending = this.pending_rows();
            if pending >= TEXT_BATCH_ROWS || (pending > 0 && this.limit_reached()) {
                return Poll::Ready(Some(this.take_batch()));
            }
            if this.reader.is_none() {
                match this.open_next() {
                    Err(error) => {
                        this.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Ok(false) => {
                        this.done = true;
                        if this.pending_rows() == 0 {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(this.take_batch()));
                    }
                    Ok(true) => {
                        if this.wholetext
                            && let Err(error) = this.slurp_current()
                        {
                            this.done = true;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
                continue;
            }
            let Some(reader) = this.reader.as_mut() else {
                this.done = true;
                return Poll::Ready(Some(Err(DataFusionError::Internal(
                    "text read lost its file handle".to_string(),
                ))));
            };
            let outcome = reader.read(&mut this.chunk);
            let read = match outcome {
                Err(error) => {
                    let path = this.current.clone().unwrap_or_default();
                    Err(DataFusionError::Execution(format!(
                        "text read of {} failed: {error}",
                        path.display()
                    )))
                }
                Ok(bytes) => Ok(bytes),
            };
            match read {
                Err(error) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Ok(0) => {
                    this.scan_carry(true);
                    this.finish_file();
                    this.reader = None;
                    this.current = None;
                    this.file_index += 1;
                }
                Ok(bytes) => {
                    this.bytes_read += bytes;
                    this.carry.extend_from_slice(&this.chunk[..bytes]);
                    this.scan_carry(false);
                }
            }
        }
    }
}

impl RecordBatchStream for TextLineStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl crate::ReparkSession {
    #[expect(
        clippy::unused_async,
        reason = "symmetric with read_csv/read_json; remote reads will await"
    )]
    #[allow(clippy::missing_errors_doc)]
    pub async fn read_text(
        &self,
        path: &str,
        wholetext: bool,
        line_sep: Option<&str>,
        user_schema: Option<Vec<(String, String)>>,
        base_path: Option<&str>,
    ) -> Result<DataFrame> {
        if line_sep.is_some_and(str::is_empty) {
            return Err(Error::Analysis(
                "text lineSep must be a non-empty string".to_string(),
            ));
        }
        let (files, partitions) = expand_text_paths(path, base_path)?;
        let zone = self.session_time_zone().id();
        let (schema, fields, values) =
            apply_user_text_schema(files.clone(), partitions, user_schema, zone)?;
        let provider = Arc::new(TextTableProvider {
            files,
            wholetext,
            line_sep: line_sep.map(str::to_string),
            schema,
            partition_fields: fields,
            partition_values: Arc::new(values),
        });
        self.context().read_table(provider).map_err(engine_err)
    }
}

#[cfg(test)]
mod tests;
