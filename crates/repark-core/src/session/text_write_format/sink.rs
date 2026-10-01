use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::file_options::csv_writer::CsvWriterOptions;
use datafusion::common::file_options::json_writer::JsonWriterOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{Result, not_impl_err};
use datafusion::datasource::file_format::csv::CsvSerializer;
use datafusion::datasource::file_format::json::JsonSerializer;
use datafusion::datasource::file_format::write::BatchSerializer;
use datafusion::datasource::file_format::write::demux::DemuxedStreamReceiver;
use datafusion::datasource::file_format::write::orchestration::spawn_writer_tasks_and_join;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

use super::file_format::TextKind;
use super::serializer::ReparkTextSerializer;
use super::spec::TextWriteSpec;

pub(crate) struct ReparkTextSink {
    config: FileSinkConfig,
    kind: TextKind,
    csv_options: Option<CsvWriterOptions>,
    json_options: Option<JsonWriterOptions>,
    spec: Arc<TextWriteSpec>,
    created: Arc<Mutex<Vec<ObjectPath>>>,
}

impl ReparkTextSink {
    pub(crate) fn for_csv(
        config: FileSinkConfig,
        options: CsvWriterOptions,
        spec: Arc<TextWriteSpec>,
        created: Arc<Mutex<Vec<ObjectPath>>>,
    ) -> Self {
        Self {
            config,
            kind: TextKind::Csv,
            csv_options: Some(options),
            json_options: None,
            spec,
            created,
        }
    }

    pub(crate) fn for_json(
        config: FileSinkConfig,
        options: JsonWriterOptions,
        spec: Arc<TextWriteSpec>,
        created: Arc<Mutex<Vec<ObjectPath>>>,
    ) -> Self {
        Self {
            config,
            kind: TextKind::Json,
            csv_options: None,
            json_options: Some(options),
            spec,
            created,
        }
    }

    fn skip_columns(&self) -> Arc<HashSet<String>> {
        Arc::new(
            self.config
                .table_partition_cols
                .iter()
                .map(|(name, _)| name.to_lowercase())
                .collect(),
        )
    }
}

impl Debug for ReparkTextSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReparkTextSink").finish()
    }
}

impl DisplayAs for ReparkTextSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ReparkTextSink(kind={:?})", self.kind)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: {}", self.kind.extension())?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

#[async_trait]
impl FileSink for ReparkTextSink {
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
        let skip = self.skip_columns();
        let (serializer, compression, compression_level): (Arc<dyn BatchSerializer>, _, _) =
            match self.kind {
                TextKind::Csv => {
                    let Some(options) = self.csv_options.as_ref() else {
                        return not_impl_err!("text csv sink is missing its writer options");
                    };
                    let builder = options.writer_options.clone();
                    let header = builder.header();
                    let inner = Arc::new(
                        CsvSerializer::new()
                            .with_builder(builder)
                            .with_header(header),
                    ) as _;
                    (
                        Arc::new(ReparkTextSerializer::new(
                            inner,
                            Arc::clone(&self.spec),
                            skip,
                        )),
                        options.compression.into(),
                        options.compression_level,
                    )
                }
                TextKind::Json => {
                    let Some(options) = self.json_options.as_ref() else {
                        return not_impl_err!("text json sink is missing its writer options");
                    };
                    let inner = Arc::new(JsonSerializer::new()) as _;
                    (
                        Arc::new(ReparkTextSerializer::new(
                            inner,
                            Arc::clone(&self.spec),
                            skip,
                        )),
                        options.compression.into(),
                        options.compression_level,
                    )
                }
            };
        let (forward_send, forward_recv) = tokio::sync::mpsc::unbounded_channel();
        let created = Arc::clone(&self.created);
        let mut incoming = file_stream_rx;
        let forward = async move {
            while let Some((location, stream)) = incoming.recv().await {
                if let Ok(mut guard) = created.lock() {
                    guard.push(location.clone());
                }
                if forward_send.send((location, stream)).is_err() {
                    break;
                }
            }
        };
        let write = spawn_writer_tasks_and_join(
            context,
            serializer,
            compression,
            compression_level,
            object_store,
            demux_task,
            forward_recv,
        );
        let (outcome, ()) = futures::future::join(write, forward).await;
        outcome
    }
}

#[async_trait]
impl DataSink for ReparkTextSink {
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
