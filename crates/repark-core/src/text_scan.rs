use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayBuilder, ArrayRef, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

use crate::{Error, Result, engine_err};

const TEXT_BATCH_ROWS: usize = 8192;

const TEXT_READ_CHUNK: usize = 65536;

const TEXT_SCAN_PARTITIONS: usize = 8;

fn text_schema_with_partitions(partitions: &[Field]) -> SchemaRef {
    let mut fields = Vec::with_capacity(partitions.len() + 1);
    fields.push(Field::new("value", DataType::Utf8, true));
    fields.extend(partitions.iter().cloned());
    Arc::new(Schema::new(fields))
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
}

use crate::partition_discovery::{
    DiscoveredPartitions, PartitionValue, TextRowSink, canonical_partition_text,
    discover_partitions, emit_text_row, finish_partition_columns, order_batch_columns,
    plan_partition_slots,
};
use crate::text_glob::{expand_text_glob, has_glob_meta, is_hidden_name};

fn push_text_dir(dir: &Path, display: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|error| {
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
    for entry in &ordered {
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
            push_text_dir(&candidate, display, out)?;
        }
    }
    Ok(())
}

fn missing_text_path(path: &str) -> Error {
    Error::Analysis(format!(
        "[PATH_NOT_FOUND] Path does not exist: file:{path}. SQLSTATE: 42K03"
    ))
}

pub(crate) fn expand_text_paths(path: &str) -> Result<(Vec<PathBuf>, DiscoveredPartitions)> {
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
        let partitions = discover_partitions(fs_path, &files);
        return Ok((files, partitions));
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
    partition_values: HashMap<PathBuf, Vec<PartitionValue>>,
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
                    partition_values: self.partition_values.clone(),
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
    partition_values: HashMap<PathBuf, Vec<PartitionValue>>,
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
            partition_values: self.partition_values.clone(),
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
    partition_values: HashMap<PathBuf, Vec<PartitionValue>>,
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
    ) -> Result<DataFrame> {
        if line_sep.is_some_and(str::is_empty) {
            return Err(Error::Analysis(
                "text lineSep must be a non-empty string".to_string(),
            ));
        }
        let (files, partitions) = expand_text_paths(path)?;
        let provider = Arc::new(TextTableProvider {
            files,
            wholetext,
            line_sep: line_sep.map(str::to_string),
            schema: text_schema_with_partitions(&partitions.fields),
            partition_fields: partitions.fields,
            partition_values: partitions.values,
        });
        self.context().read_table(provider).map_err(engine_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    async fn text_rows(frame: &DataFrame) -> Vec<Option<String>> {
        let batches = frame.clone().collect().await.unwrap();
        let mut rows: Vec<Option<String>> = Vec::new();
        for batch in &batches {
            let column = batch.column(0);
            let values = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push(if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row).to_string())
                });
            }
        }
        rows
    }

    async fn text_values(frame: &DataFrame) -> Vec<String> {
        let mut rows = text_rows(frame).await;
        rows.sort();
        rows.into_iter().flatten().collect()
    }

    #[tokio::test]
    async fn text_split_drops_one_trailing_terminator() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "x\n\ny\r\nz").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        let rows = text_rows(&frame).await;
        let plain: Vec<&str> = rows
            .iter()
            .map(|row| row.as_deref().unwrap_or("<null>"))
            .collect();
        assert_eq!(plain, vec!["x", "", "y", "z"]);
    }

    #[tokio::test]
    async fn text_custom_separator_splits_only_on_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "a;b;c").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, Some(";"))
            .await
            .unwrap();
        let rows = text_rows(&frame).await;
        let plain: Vec<&str> = rows.iter().map(|row| row.as_deref().unwrap()).collect();
        assert_eq!(plain, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn text_wholetext_reads_one_row_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "a\nb\n").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), true, None)
            .await
            .unwrap();
        let rows = text_rows(&frame).await;
        assert_eq!(rows, vec![Some("a\nb\n".to_string())]);
    }

    #[tokio::test]
    async fn text_lone_carriage_return_splits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "a\rb\rc").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert_eq!(text_values(&frame).await, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn text_crlf_split_across_read_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        let mut bytes = vec![b'A'; TEXT_READ_CHUNK - 1];
        bytes.extend_from_slice(b"\r\nZ\n");
        std::fs::write(&path, &bytes).unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        let rows = text_rows(&frame).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].as_deref().unwrap().len(), TEXT_READ_CHUNK - 1);
        assert_eq!(rows[1].as_deref(), Some("Z"));
    }

    #[tokio::test]
    async fn text_two_trailing_terminators_keep_empty_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "x\n\n").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert_eq!(text_values(&frame).await, vec!["", "x"]);
    }

    #[tokio::test]
    async fn text_empty_file_reads_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert!(text_rows(&frame).await.is_empty());
    }

    #[tokio::test]
    async fn text_invalid_utf8_decodes_lossy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.txt");
        std::fs::write(&path, b"ok\n\xff\xfe\nmore\ncafe\xe2\x82").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        let rows = text_rows(&frame).await;
        let plain: Vec<&str> = rows.iter().map(|row| row.as_deref().unwrap()).collect();
        assert_eq!(plain, vec!["ok", "��", "more", "cafe�"]);
    }

    #[tokio::test]
    async fn text_missing_path_reports_not_found() {
        let session = test_session();
        let missing = std::env::temp_dir().join("repark-no-such-text-path-1290");
        let Err(error) = session
            .read_text(missing.to_str().unwrap(), false, None)
            .await
        else {
            panic!("a missing path must refuse the text read");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "[PATH_NOT_FOUND] Path does not exist: file:{}. SQLSTATE: 42K03",
                missing.to_str().unwrap()
            )
        );
    }

    #[tokio::test]
    async fn text_empty_line_sep_refuses() {
        let session = test_session();
        let Err(error) = session.read_text("any.txt", false, Some("")).await else {
            panic!("an empty lineSep must refuse the text read");
        };
        assert!(error.to_string().contains("lineSep"));
    }

    #[tokio::test]
    async fn text_glob_star_matches_txt_only() {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in [
            ("list_1.txt", "one\n"),
            ("list_2.txt", "two\n"),
            ("other.log", "log\n"),
            ("foo[bar].txt", "brackets\n"),
            ("foob.txt", "b-class\n"),
        ] {
            std::fs::write(dir.path().join(name), body).unwrap();
        }
        let session = test_session();
        let pattern = dir.path().join("*.txt");
        let frame = session
            .read_text(pattern.to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert_eq!(
            text_values(&frame).await,
            vec!["b-class", "brackets", "one", "two"]
        );
    }

    #[tokio::test]
    async fn text_glob_question_matches_one_char() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("list_1.txt"), "one\n").unwrap();
        std::fs::write(dir.path().join("list_2.txt"), "two\n").unwrap();
        std::fs::write(dir.path().join("other.log"), "log\n").unwrap();
        let session = test_session();
        let pattern = dir.path().join("list_?.txt");
        let frame = session
            .read_text(pattern.to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert_eq!(text_values(&frame).await, vec!["one", "two"]);
    }

    #[tokio::test]
    async fn text_glob_brackets_are_a_class() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("foo[bar].txt"), "brackets\n").unwrap();
        std::fs::write(dir.path().join("foob.txt"), "b-class\n").unwrap();
        let session = test_session();
        let pattern = dir.path().join("foo[bar].txt");
        let frame = session
            .read_text(pattern.to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert_eq!(text_values(&frame).await, vec!["b-class"]);
    }

    #[tokio::test]
    async fn text_dir_descends_partition_dirs_only() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("top.txt"), "top\n").unwrap();
        let nested = dir.path().join("sub");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("leaf.txt"), "leaf\n").unwrap();
        let keyed = dir.path().join("k=x");
        std::fs::create_dir(&keyed).unwrap();
        std::fs::write(keyed.join("part-00000.txt"), "hello\n").unwrap();
        let session = test_session();
        let frame = session
            .read_text(dir.path().to_str().unwrap(), false, None)
            .await
            .unwrap();
        assert_eq!(text_values(&frame).await, vec!["hello", "top"]);
    }

    #[tokio::test]
    async fn text_limit_stops_after_enough_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "a\nb\nc\nd\ne\n").unwrap();
        let session = test_session();
        let frame = session
            .read_text(path.to_str().unwrap(), false, None)
            .await
            .unwrap();
        let limited = frame.limit(0, Some(2)).unwrap();
        let batches = limited.collect().await.unwrap();
        let count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(count, 2);
    }

    #[test]
    fn text_limit_stops_appending_at_limit() {
        let mut stream = TextLineStream {
            files: Vec::new(),
            file_index: 0,
            current: None,
            reader: None,
            chunk: Vec::new(),
            carry: Vec::new(),
            value_builder: Some(StringBuilder::new()),
            part_builders: Vec::new(),
            part_slots: Vec::new(),
            part_current: Vec::new(),
            part_types: Vec::new(),
            part_plan: vec![None],
            partition_values: HashMap::new(),
            blank_rows: 0,
            wholetext: false,
            separator: None,
            schema: text_schema_with_partitions(&[]),
            limit: Some(10),
            emitted: 0,
            done: false,
            bytes_read: 0,
        };
        let line = vec![b'x'; 10 * 1024];
        for _ in 0..2000 {
            stream.carry.extend_from_slice(&line);
            stream.carry.push(b'\n');
        }
        stream.scan_carry(false);
        assert_eq!(
            stream.value_builder.as_ref().map(ArrayBuilder::len),
            Some(10)
        );
        let batch = stream.take_batch().unwrap();
        assert_eq!(batch.num_rows(), 10);
    }

    #[test]
    fn text_grouping_caps_partitions() {
        let files: Vec<PathBuf> = (0..20)
            .map(|index| PathBuf::from(format!("{index}.txt")))
            .collect();
        let groups = group_text_files(&files);
        assert!(groups.len() <= TEXT_SCAN_PARTITIONS);
        let flat: Vec<PathBuf> = groups.into_iter().flatten().collect();
        assert_eq!(flat, files);
        assert_eq!(group_text_files(&[]), vec![Vec::<PathBuf>::new()]);
    }

    #[test]
    fn text_expand_paths_missing_reports_not_found() {
        let error = expand_text_paths("/no/such/repark-text-path").unwrap_err();
        assert!(error.to_string().starts_with("[PATH_NOT_FOUND]"));
        assert!(error.to_string().contains("SQLSTATE: 42K03"));
    }
}
