//! Spark `text` file-format scan and writer over local files.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
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

const TEXT_BATCH_ROWS: usize = 1024;

const TEXT_READ_CHUNK: usize = 65536;

/// The Spark text schema: one nullable string column named `value`.
fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
}

fn is_hidden_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|text| text.starts_with('.') || text.starts_with('_'))
}

/// Resolve a text read path to sorted data files.
///
/// A file reads alone; a directory reads every non-hidden regular file in sorted order
/// (Spark skips `_` / `.` sidecars the same way). Globs and remote paths fail loud.
/// # Errors
/// Returns [`Error::Analysis`] for remote paths, globs, missing paths, and unreadable dirs.
pub(crate) fn expand_text_paths(path: &str) -> Result<Vec<PathBuf>> {
    if is_remote_path(path) {
        return Err(Error::Analysis(format!(
            "text read over {path:?} is not supported by repark yet (local files and directories only)"
        )));
    }
    if path.contains(['*', '?', '[']) {
        return Err(Error::Analysis(format!(
            "text read path glob {path:?} is not supported by repark yet (pass a file, a directory, or a list of paths)"
        )));
    }
    let fs_path = Path::new(path);
    if fs_path.is_file() {
        return Ok(vec![fs_path.to_path_buf()]);
    }
    if fs_path.is_dir() {
        let mut files: Vec<PathBuf> = Vec::new();
        let entries = std::fs::read_dir(fs_path).map_err(|error| {
            Error::Analysis(format!("text read cannot list directory {path:?}: {error}"))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                Error::Analysis(format!("text read cannot list directory {path:?}: {error}"))
            })?;
            let candidate = entry.path();
            let listed_file = entry
                .file_type()
                .is_ok_and(|kind| kind.is_file() || kind.is_symlink())
                && candidate.is_file();
            if !listed_file {
                continue;
            }
            if is_hidden_name(entry.file_name().as_os_str()) {
                continue;
            }
            files.push(candidate);
        }
        files.sort();
        return Ok(files);
    }
    Err(Error::Analysis(format!(
        "text read path does not exist: {path:?}"
    )))
}

/// Render an Arrow type with the Spark SQL name the text error text carries.
fn spark_text_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "VOID".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Duration(_) | DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _) => "STRING".to_string(),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _) => "ARRAY".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Map(_, _) => "MAP".to_string(),
        DataType::Union(_, _) => "UNION".to_string(),
    }
}

/// Build Spark's `UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE` text for one column.
fn text_unsupported_column(column: &str, data_type: &DataType) -> Error {
    Error::Analysis(format!(
        "[UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `{column}` of the type \"{}\". SQLSTATE: 0A000",
        spark_text_type_name(data_type)
    ))
}

/// Require exactly one string column before any byte is written.
/// # Errors
/// Returns [`Error::Analysis`] naming the first non-string column (Spark's text),
/// or a column-count refusal when every column is a string but there is not one.
fn check_text_write_schema(schema: &DFSchema) -> Result<()> {
    let mut offender: Option<(&str, &DataType)> = None;
    for field in schema.fields() {
        let string = matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        if !string && offender.is_none() {
            offender = Some((field.name().as_str(), field.data_type()));
        }
    }
    if schema.fields().len() == 1 && offender.is_none() {
        return Ok(());
    }
    if let Some((name, data_type)) = offender {
        return Err(text_unsupported_column(name, data_type));
    }
    Err(Error::Analysis(format!(
        "text write requires a single string column, got {} columns ({})",
        schema.fields().len(),
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Render one collected batch as optional strings (NULL stays empty at the writer).
/// # Errors
/// Returns [`DataFusionError::Internal`] when the checked column is not a string array.
fn text_batch_values(batch: &RecordBatch) -> DataFusionResult<Vec<Option<String>>> {
    let column = batch.column(0);
    let len = batch.num_rows();
    let mut rows: Vec<Option<String>> = Vec::with_capacity(len);
    match column.data_type() {
        DataType::Utf8 => {
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("text write column is not a Utf8 array".to_string())
                })?;
            for row in 0..len {
                rows.push(if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row).to_string())
                });
            }
        }
        DataType::LargeUtf8 => {
            let values = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "text write column is not a LargeUtf8 array".to_string(),
                    )
                })?;
            for row in 0..len {
                rows.push(if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row).to_string())
                });
            }
        }
        DataType::Utf8View => {
            let values = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "text write column is not a Utf8View array".to_string(),
                    )
                })?;
            for row in 0..len {
                rows.push(if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row).to_string())
                });
            }
        }
        other => {
            return Err(exec_datafusion_err!(
                "text write reached an unsupported column type: {other}"
            ));
        }
    }
    Ok(rows)
}

/// Write one collected frame as `part-*.txt` files (NULL rows write empty lines).
/// # Errors
/// Returns [`Error::Analysis`] before any write when the schema is not one string
/// column, and on any file-system failure while writing.
pub async fn write_text_frame(frame: &DataFrame, dir: &Path, line_sep: &str) -> Result<()> {
    check_text_write_schema(frame.schema())?;
    let batches = frame.clone().collect().await.map_err(engine_err)?;
    std::fs::create_dir_all(dir).map_err(|error| {
        Error::Analysis(format!(
            "text write cannot create directory {}: {error}",
            dir.display()
        ))
    })?;
    let mut parts_written = 0usize;
    for (index, batch) in batches.iter().enumerate() {
        if batch.num_rows() == 0 {
            continue;
        }
        let values = text_batch_values(batch).map_err(engine_err)?;
        let part = dir.join(format!("part-{index:05}.txt"));
        let file = File::create(&part).map_err(|error| {
            Error::Analysis(format!(
                "text write cannot create {}: {error}",
                part.display()
            ))
        })?;
        let mut writer = BufWriter::new(file);
        for value in &values {
            if let Some(text) = value {
                writer.write_all(text.as_bytes()).map_err(|error| {
                    Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                })?;
            }
            writer.write_all(line_sep.as_bytes()).map_err(|error| {
                Error::Analysis(format!("text write to {} failed: {error}", part.display()))
            })?;
        }
        writer.flush().map_err(|error| {
            Error::Analysis(format!("text write to {} failed: {error}", part.display()))
        })?;
        parts_written += 1;
    }
    if parts_written == 0 {
        let part = dir.join("part-00000.txt");
        File::create(&part).map_err(|error| {
            Error::Analysis(format!(
                "text write cannot create {}: {error}",
                part.display()
            ))
        })?;
    }
    Ok(())
}

/// Streaming text table: one partition walks every file in sorted order.
#[derive(Debug, Clone)]
pub(crate) struct TextTableProvider {
    files: Vec<PathBuf>,
    wholetext: bool,
    line_sep: Option<String>,
    schema: SchemaRef,
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
        let partition = Arc::new(TextPartition {
            files: self.files.clone(),
            wholetext: self.wholetext,
            line_sep: self.line_sep.clone(),
            schema: Arc::clone(&output_schema),
            values: !output_schema.fields().is_empty(),
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![partition],
            None,
            vec![],
            false,
            limit,
        )?))
    }
}

/// One streaming partition over the text files.
#[derive(Debug)]
struct TextPartition {
    files: Vec<PathBuf>,
    wholetext: bool,
    line_sep: Option<String>,
    schema: SchemaRef,
    values: bool,
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
        Box::pin(TextLineStream {
            files: self.files.clone(),
            file_index: 0,
            current_path: None,
            reader: None,
            carry: Vec::new(),
            pending: Vec::new(),
            wholetext: self.wholetext,
            separator: self.line_sep.clone().map(String::into_bytes),
            schema: Arc::clone(&self.schema),
            values: self.values,
            done: false,
        })
    }
}

/// Decode one line piece as UTF-8 (repark reads UTF-8 text only).
fn decode_piece(bytes: &[u8], path: &Path) -> DataFusionResult<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        DataFusionError::Execution(format!(
            "text read of {} is not valid UTF-8 (repark reads UTF-8 text only)",
            path.display()
        ))
    })
}

/// A line stream over local files: chunked reads, bounded batches, one row per line.
struct TextLineStream {
    files: Vec<PathBuf>,
    file_index: usize,
    current_path: Option<PathBuf>,
    reader: Option<BufReader<File>>,
    carry: Vec<u8>,
    pending: Vec<String>,
    wholetext: bool,
    separator: Option<Vec<u8>>,
    schema: SchemaRef,
    values: bool,
    done: bool,
}

impl TextLineStream {
    fn open_next(&mut self) -> DataFusionResult<bool> {
        let Some(path) = self.files.get(self.file_index).cloned() else {
            return Ok(false);
        };
        let file = File::open(&path).map_err(|error| {
            DataFusionError::Execution(format!("text read cannot open {}: {error}", path.display()))
        })?;
        self.current_path = Some(path);
        self.reader = Some(BufReader::new(file));
        self.carry.clear();
        Ok(true)
    }

    fn scan_carry(&mut self, final_scan: bool) -> DataFusionResult<()> {
        if self.separator.is_none() {
            self.scan_universal(final_scan)
        } else {
            self.scan_custom(final_scan)
        }
    }

    fn scan_universal(&mut self, final_scan: bool) -> DataFusionResult<()> {
        let path = self.current_path.clone().unwrap_or_default();
        let mut start = 0usize;
        let mut index = 0usize;
        let hold_trailing_cr = !final_scan && self.carry.last().is_some_and(|last| *last == b'\r');
        let end = if hold_trailing_cr {
            self.carry.len().saturating_sub(1)
        } else {
            self.carry.len()
        };
        while index < end {
            let byte = self.carry[index];
            if byte == b'\n' {
                self.pending
                    .push(decode_piece(&self.carry[start..index], &path)?);
                index += 1;
                start = index;
            } else if byte == b'\r' {
                let paired = index + 1 < self.carry.len() && self.carry[index + 1] == b'\n';
                if paired {
                    self.pending
                        .push(decode_piece(&self.carry[start..index], &path)?);
                    index += 2;
                    start = index;
                } else if index + 1 == self.carry.len() && !final_scan {
                    break;
                } else {
                    self.pending
                        .push(decode_piece(&self.carry[start..index], &path)?);
                    index += 1;
                    start = index;
                }
            } else {
                index += 1;
            }
        }
        self.carry.drain(..start);
        Ok(())
    }

    fn scan_custom(&mut self, final_scan: bool) -> DataFusionResult<()> {
        let path = self.current_path.clone().unwrap_or_default();
        let empty: Vec<u8> = Vec::new();
        let separator = self.separator.as_ref().unwrap_or(&empty);
        let hold = if final_scan {
            0
        } else {
            separator.len().saturating_sub(1)
        };
        let end = self.carry.len().saturating_sub(hold);
        let mut start = 0usize;
        let mut index = 0usize;
        while index + separator.len() <= end {
            if &self.carry[index..index + separator.len()] == separator.as_slice() {
                self.pending
                    .push(decode_piece(&self.carry[start..index], &path)?);
                index += separator.len();
                start = index;
            } else {
                index += 1;
            }
        }
        self.carry.drain(..start);
        Ok(())
    }

    fn finish_file(&mut self) -> DataFusionResult<()> {
        let path = self.current_path.clone().unwrap_or_default();
        if self.wholetext || !self.carry.is_empty() {
            self.pending.push(decode_piece(&self.carry, &path)?);
        }
        self.carry.clear();
        Ok(())
    }

    fn take_batch(&mut self) -> DataFusionResult<RecordBatch> {
        let count = self.pending.len().min(TEXT_BATCH_ROWS);
        let rows: Vec<String> = self.pending.drain(..count).collect();
        if !self.values {
            return RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )
            .map_err(DataFusionError::from);
        }
        let array: ArrayRef = Arc::new(StringArray::from(rows));
        RecordBatch::try_new(Arc::clone(&self.schema), vec![array]).map_err(DataFusionError::from)
    }
}

impl Stream for TextLineStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }
            if self.pending.len() >= TEXT_BATCH_ROWS {
                return Poll::Ready(Some(self.take_batch()));
            }
            if self.reader.is_none() {
                match self.open_next() {
                    Err(error) => {
                        self.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Ok(false) => {
                        self.done = true;
                        if self.pending.is_empty() {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(self.take_batch()));
                    }
                    Ok(true) => {}
                }
            }
            let path = self.current_path.clone().unwrap_or_default();
            let mut chunk = vec![0u8; TEXT_READ_CHUNK];
            let read = self
                .reader
                .as_mut()
                .ok_or_else(|| {
                    DataFusionError::Internal("text read lost its file handle".to_string())
                })
                .and_then(|reader| {
                    reader.read(&mut chunk).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "text read of {} failed: {error}",
                            path.display()
                        ))
                    })
                });
            match read {
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Ok(0) => {
                    let flushed = if self.wholetext {
                        self.finish_file()
                    } else {
                        self.scan_carry(true).and_then(|()| self.finish_file())
                    };
                    if let Err(error) = flushed {
                        self.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    self.reader = None;
                    self.current_path = None;
                    self.file_index += 1;
                    if !self.pending.is_empty() {
                        return Poll::Ready(Some(self.take_batch()));
                    }
                }
                Ok(bytes) => {
                    self.carry.extend_from_slice(&chunk[..bytes]);
                    if self.wholetext {
                        continue;
                    }
                    if let Err(error) = self.scan_carry(false) {
                        self.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
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
    /// Read a text file or directory as one `value` string column.
    /// # Errors
    /// Returns [`Error::Analysis`] for an empty separator, a remote or glob or missing
    /// path, and for unreadable directories; [`Error::DataFusion`] on plan failure.
    #[expect(
        clippy::unused_async,
        reason = "symmetric with read_csv/read_json; remote reads will await"
    )]
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
        let files = expand_text_paths(path)?;
        let provider = Arc::new(TextTableProvider {
            files,
            wholetext,
            line_sep: line_sep.map(str::to_string),
            schema: text_schema(),
        });
        self.context().read_table(provider).map_err(engine_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    async fn text_rows(frame: &DataFrame) -> Vec<Option<String>> {
        let batches = frame.clone().collect().await.unwrap();
        let mut rows: Vec<Option<String>> = Vec::new();
        for batch in &batches {
            rows.extend(text_batch_values(batch).unwrap());
        }
        rows
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
    async fn text_write_names_first_non_string_column() {
        let session = test_session();
        let frame = session
            .sql("SELECT 'x' AS key, CAST(1 AS INT) AS a, 2 AS b")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        let Err(error) = write_text_frame(&frame, &target, "\n").await else {
            panic!("a multi-column frame must refuse the text write");
        };
        assert_eq!(
            error.to_string(),
            "[UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `a` of the type \"INT\". SQLSTATE: 0A000"
        );
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn text_write_round_trip_keeps_null_empty() {
        let session = test_session();
        let frame = session
            .sql("SELECT * FROM (VALUES ('a'), (NULL)) AS t(value)")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        write_text_frame(&frame, &target, "\n").await.unwrap();
        let part = target.join("part-00000.txt");
        assert_eq!(std::fs::read_to_string(&part).unwrap(), "a\n\n");
        let back = session
            .read_text(target.to_str().unwrap(), false, None)
            .await
            .unwrap();
        let rows = text_rows(&back).await;
        assert_eq!(rows, vec![Some("a".to_string()), Some(String::new())]);
    }
}
