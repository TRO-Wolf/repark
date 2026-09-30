use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::file_options::csv_writer::CsvWriterOptions;
use datafusion::common::file_options::json_writer::JsonWriterOptions;
use datafusion::common::{GetExt, Result, Statistics, not_impl_err};
use datafusion::datasource::file_format::csv::{CsvFormat, CsvFormatFactory};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::{JsonFormat, JsonFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr_common::sort_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use object_store::{ObjectMeta, ObjectStore};

use super::sink::ReparkTextSink;
use super::spec::TextWriteSpec;

pub(crate) const TEXT_CSV_FORMAT_NAME: &str = "repark_text_csv";
pub(crate) const TEXT_JSON_FORMAT_NAME: &str = "repark_text_json";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TextKind {
    Csv,
    Json,
}

impl TextKind {
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
        }
    }
}

pub(crate) struct ReparkTextFormat {
    inner: Arc<dyn FileFormat>,
    kind: TextKind,
    spec: Arc<TextWriteSpec>,
}

impl Debug for ReparkTextFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReparkTextFormat").finish()
    }
}

#[async_trait]
impl FileFormat for ReparkTextFormat {
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
            return match self.kind {
                TextKind::Csv => not_impl_err!("Overwrites are not implemented yet for CSV"),
                TextKind::Json => not_impl_err!("Overwrites are not implemented yet for Json"),
            };
        }
        let sink = match self.kind {
            TextKind::Csv => {
                let Some(csv) = self.inner.downcast_ref::<CsvFormat>() else {
                    return not_impl_err!("text csv inner format must be CsvFormat");
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
                let writer_options = CsvWriterOptions::try_from(&options)?;
                Arc::new(ReparkTextSink::for_csv(
                    conf,
                    writer_options,
                    Arc::clone(&self.spec),
                ))
            }
            TextKind::Json => {
                let Some(json) = self.inner.downcast_ref::<JsonFormat>() else {
                    return not_impl_err!("text json inner format must be JsonFormat");
                };
                let writer_options = JsonWriterOptions::try_from(json.options())?;
                Arc::new(ReparkTextSink::for_json(
                    conf,
                    writer_options,
                    Arc::clone(&self.spec),
                ))
            }
        };
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

pub(crate) struct ReparkTextFormatFactory {
    kind: TextKind,
}

impl ReparkTextFormatFactory {
    fn csv() -> Self {
        Self {
            kind: TextKind::Csv,
        }
    }

    fn json() -> Self {
        Self {
            kind: TextKind::Json,
        }
    }
}

impl Debug for ReparkTextFormatFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReparkTextFormatFactory")
            .field("kind", &self.kind)
            .finish()
    }
}

impl GetExt for ReparkTextFormatFactory {
    fn get_ext(&self) -> String {
        match self.kind {
            TextKind::Csv => TEXT_CSV_FORMAT_NAME.to_string(),
            TextKind::Json => TEXT_JSON_FORMAT_NAME.to_string(),
        }
    }
}

impl FileFormatFactory for ReparkTextFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let (spec, rest) = TextWriteSpec::from_format_options(format_options)?;
        let inner = match self.kind {
            TextKind::Csv => CsvFormatFactory::new().create(state, &rest)?,
            TextKind::Json => JsonFormatFactory::new().create(state, &rest)?,
        };
        Ok(Arc::new(ReparkTextFormat {
            inner,
            kind: self.kind,
            spec: Arc::new(spec),
        }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        match self.kind {
            TextKind::Csv => Arc::new(CsvFormat::default()),
            TextKind::Json => Arc::new(JsonFormat::default()),
        }
    }
}

pub fn register_text_write_formats(context: &SessionContext) -> Result<()> {
    context
        .state_ref()
        .write()
        .register_file_format(Arc::new(ReparkTextFormatFactory::csv()), false)?;
    context
        .state_ref()
        .write()
        .register_file_format(Arc::new(ReparkTextFormatFactory::json()), false)?;
    Ok(())
}
