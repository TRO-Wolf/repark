use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::Session;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{GetExt, Result, Statistics, not_impl_err};
use datafusion::datasource::file_format::csv::{CsvFormat, CsvFormatFactory, CsvSerializer};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::{JsonFormat, JsonFormatFactory, JsonSerializer};
use datafusion::datasource::file_format::write::BatchSerializer;
use datafusion::datasource::file_format::write::demux::DemuxedStreamReceiver;
use datafusion::datasource::file_format::write::orchestration::spawn_writer_tasks_and_join;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSink, FileSinkConfig, FileSource};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr_common::sort_expr::LexRequirement;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore};

use crate::ReparkSession;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SpikeKind {
    Csv,
    Json,
}

impl SpikeKind {
    fn stored_as(self) -> &'static str {
        match self {
            Self::Csv => "spike_text_csv",
            Self::Json => "spike_text_json",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
        }
    }
}

struct SpikeSerializer {
    inner: Arc<dyn BatchSerializer>,
    fail: Option<String>,
}

impl BatchSerializer for SpikeSerializer {
    fn serialize(&self, batch: RecordBatch, initial: bool) -> Result<Bytes> {
        if let Some(message) = &self.fail {
            return Err(datafusion::common::DataFusionError::Execution(
                message.clone(),
            ));
        }
        self.inner.serialize(batch, initial)
    }
}

struct SpikeSink {
    config: FileSinkConfig,
    kind: SpikeKind,
    csv_options: Option<datafusion::common::file_options::csv_writer::CsvWriterOptions>,
    json_options: Option<datafusion::common::file_options::json_writer::JsonWriterOptions>,
    fail: Option<String>,
}

impl Debug for SpikeSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpikeSink").finish()
    }
}

impl DisplayAs for SpikeSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpikeSink")
    }
}

#[async_trait]
impl FileSink for SpikeSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let (serializer, compression, compression_level): (Arc<dyn BatchSerializer>, _, _) =
            match self.kind {
                SpikeKind::Csv => {
                    let options = self
                        .csv_options
                        .as_ref()
                        .unwrap_or_else(|| panic!("spike csv sink needs csv writer options"));
                    let builder = options.writer_options.clone();
                    let header = builder.header();
                    let inner = Arc::new(
                        CsvSerializer::new()
                            .with_builder(builder)
                            .with_header(header),
                    ) as _;
                    let wrapped = SpikeSerializer {
                        inner,
                        fail: self.fail.clone(),
                    };
                    (
                        Arc::new(wrapped),
                        options.compression.into(),
                        options.compression_level,
                    )
                }
                SpikeKind::Json => {
                    let options = self
                        .json_options
                        .as_ref()
                        .unwrap_or_else(|| panic!("spike json sink needs json writer options"));
                    let inner = Arc::new(JsonSerializer::new()) as _;
                    let wrapped = SpikeSerializer {
                        inner,
                        fail: self.fail.clone(),
                    };
                    (
                        Arc::new(wrapped),
                        options.compression.into(),
                        options.compression_level,
                    )
                }
            };
        spawn_writer_tasks_and_join(
            context,
            serializer,
            compression,
            compression_level,
            object_store,
            demux_task,
            file_stream_rx,
        )
        .await
    }
}

#[async_trait]
impl DataSink for SpikeSink {
    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}

struct SpikeFormat {
    inner: Arc<dyn FileFormat>,
    kind: SpikeKind,
    fail: Option<String>,
}

impl Debug for SpikeFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpikeFormat").finish()
    }
}

#[async_trait]
impl FileFormat for SpikeFormat {
    fn get_ext(&self) -> String {
        self.kind.extension().to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for spike");
        }
        let sink = match self.kind {
            SpikeKind::Csv => {
                let Some(csv) = self.inner.downcast_ref::<CsvFormat>() else {
                    return not_impl_err!("spike csv inner must be CsvFormat");
                };
                let has_header = csv
                    .options()
                    .has_header
                    .unwrap_or_else(|| state.config_options().catalog.has_header);
                let newlines_in_values = csv
                    .options()
                    .newlines_in_values
                    .unwrap_or_else(|| state.config_options().catalog.newlines_in_values);
                let options = csv
                    .options()
                    .clone()
                    .with_has_header(has_header)
                    .with_newlines_in_values(newlines_in_values);
                let writer_options =
                    datafusion::common::file_options::csv_writer::CsvWriterOptions::try_from(
                        &options,
                    )?;
                Arc::new(SpikeSink {
                    config: conf,
                    kind: self.kind,
                    csv_options: Some(writer_options),
                    json_options: None,
                    fail: self.fail.clone(),
                })
            }
            SpikeKind::Json => {
                let Some(json) = self.inner.downcast_ref::<JsonFormat>() else {
                    return not_impl_err!("spike json inner must be JsonFormat");
                };
                let writer_options =
                    datafusion::common::file_options::json_writer::JsonWriterOptions::try_from(
                        json.options(),
                    )?;
                Arc::new(SpikeSink {
                    config: conf,
                    kind: self.kind,
                    csv_options: None,
                    json_options: Some(writer_options),
                    fail: self.fail.clone(),
                })
            }
        };
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

struct SpikeFactory {
    kind: SpikeKind,
    strip: bool,
    fail: Option<String>,
    seen_options: Arc<Mutex<HashMap<String, String>>>,
}

impl Debug for SpikeFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpikeFactory").finish()
    }
}

impl GetExt for SpikeFactory {
    fn get_ext(&self) -> String {
        self.kind.stored_as().to_string()
    }
}

impl FileFormatFactory for SpikeFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        *self
            .seen_options
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = format_options.clone();
        let rest: HashMap<String, String> = if self.strip {
            format_options
                .iter()
                .filter(|(key, _)| !key.starts_with("spike.text."))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        } else {
            format_options.clone()
        };
        let inner = match self.kind {
            SpikeKind::Csv => CsvFormatFactory::new().create(state, &rest)?,
            SpikeKind::Json => JsonFormatFactory::new().create(state, &rest)?,
        };
        Ok(Arc::new(SpikeFormat {
            inner,
            kind: self.kind,
            fail: self.fail.clone(),
        }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        match self.kind {
            SpikeKind::Csv => Arc::new(CsvFormat::default()),
            SpikeKind::Json => Arc::new(JsonFormat::default()),
        }
    }
}

fn register_spike(session: &ReparkSession, factory: SpikeFactory) {
    session
        .context()
        .state_ref()
        .write()
        .register_file_format(Arc::new(factory), false)
        .unwrap();
}

fn spike_factory(
    kind: SpikeKind,
    strip: bool,
    fail: Option<String>,
) -> (SpikeFactory, Arc<Mutex<HashMap<String, String>>>) {
    let seen_options = Arc::new(Mutex::new(HashMap::new()));
    (
        SpikeFactory {
            kind,
            strip,
            fail,
            seen_options: Arc::clone(&seen_options),
        },
        seen_options,
    )
}

fn part_files(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn spike_custom_stored_as_csv_keeps_csv_extension() {
    let session = ReparkSession::new().unwrap();
    let (factory, _) = spike_factory(SpikeKind::Csv, true, None);
    register_spike(&session, factory);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out");
    let sql = format!(
        "COPY (SELECT 1 AS a, 'x' AS b) TO '{}' STORED AS spike_text_csv",
        target.to_string_lossy()
    );
    session.sql(&sql).await.unwrap().collect().await.unwrap();
    let names = part_files(&target);
    assert_eq!(names.len(), 1, "one part file, got {names:?}");
    assert!(
        ObjectPath::from(names[0].as_str()).extension() == Some("csv"),
        "csv extension kept, got {names:?}"
    );
    let body = std::fs::read_to_string(target.join(&names[0])).unwrap();
    assert_eq!(body, "a,b\n1,x\n");
}

#[tokio::test]
async fn spike_custom_stored_as_json_keeps_json_extension() {
    let session = ReparkSession::new().unwrap();
    let (factory, _) = spike_factory(SpikeKind::Json, true, None);
    register_spike(&session, factory);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out");
    let sql = format!(
        "COPY (SELECT 1 AS a, 'x' AS b) TO '{}' STORED AS spike_text_json",
        target.to_string_lossy()
    );
    session.sql(&sql).await.unwrap().collect().await.unwrap();
    let names = part_files(&target);
    assert_eq!(names.len(), 1, "one part file, got {names:?}");
    assert!(
        ObjectPath::from(names[0].as_str()).extension() == Some("json"),
        "json extension kept, got {names:?}"
    );
    let body = std::fs::read_to_string(target.join(&names[0])).unwrap();
    assert_eq!(body, "{\"a\":1,\"b\":\"x\"}\n");
}

#[tokio::test]
async fn spike_custom_options_reach_factory_create_verbatim() {
    let session = ReparkSession::new().unwrap();
    let (factory, seen) = spike_factory(SpikeKind::Csv, true, None);
    register_spike(&session, factory);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out");
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS spike_text_csv OPTIONS ('spike.text.note' 'MiXeD-''Q''uote\\Back', 'format.has_header' 'false')",
        target.to_string_lossy()
    );
    session.sql(&sql).await.unwrap().collect().await.unwrap();
    let seen = seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        seen.get("spike.text.note").map(String::as_str),
        Some("MiXeD-'Q'uote\\Back")
    );
    assert_eq!(
        seen.get("format.has_header").map(String::as_str),
        Some("false")
    );
    let names = part_files(&target);
    assert_eq!(names.len(), 1, "one part file, got {names:?}");
    let body = std::fs::read_to_string(target.join(&names[0])).unwrap();
    assert_eq!(body, "1\n");
}

#[tokio::test]
async fn spike_unstripped_custom_options_fail_inner_factory() {
    let session = ReparkSession::new().unwrap();
    let (factory, _) = spike_factory(SpikeKind::Csv, false, None);
    register_spike(&session, factory);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out");
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS spike_text_csv OPTIONS ('spike.text.note' 'x')",
        target.to_string_lossy()
    );
    let outcome = session.sql(&sql).await.unwrap().collect().await;
    let error = outcome.expect_err("unstripped custom keys must fail");
    assert!(
        error.to_string().contains("\"spike\""),
        "error rejects the spike namespace, got {error}"
    );
}

#[tokio::test]
async fn spike_serializer_error_surfaces_with_message_intact() {
    let session = ReparkSession::new().unwrap();
    let (factory, _) = spike_factory(
        SpikeKind::Csv,
        true,
        Some("spike boom: Unsupported field: X".to_string()),
    );
    register_spike(&session, factory);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out");
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS spike_text_csv",
        target.to_string_lossy()
    );
    let outcome = session.sql(&sql).await.unwrap().collect().await;
    let error = outcome.expect_err("failing serializer must fail the copy");
    assert!(
        error
            .to_string()
            .contains("spike boom: Unsupported field: X"),
        "serializer message intact, got {error}"
    );
}
