use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::file_format::write::BatchSerializer;

use super::spec::TextWriteSpec;
use super::udf::format_batch_for_sink;

pub(crate) type DisplayHeader = Arc<HashMap<String, String>>;

pub(crate) struct ReparkTextSerializer {
    inner: Arc<dyn BatchSerializer>,
    spec: Arc<TextWriteSpec>,
    skip: Arc<HashSet<String>>,
    header: Option<DisplayHeader>,
}

impl ReparkTextSerializer {
    pub(crate) fn new(
        inner: Arc<dyn BatchSerializer>,
        spec: Arc<TextWriteSpec>,
        skip: Arc<HashSet<String>>,
        header: Option<DisplayHeader>,
    ) -> Self {
        Self {
            inner,
            spec,
            skip,
            header,
        }
    }
}

pub(crate) fn with_display_header(
    batch: &RecordBatch,
    header: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| match header.get(field.name()) {
            Some(display) => Arc::new(Field::new(
                display,
                field.data_type().clone(),
                field.is_nullable(),
            )),
            None => Arc::clone(field),
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

impl BatchSerializer for ReparkTextSerializer {
    fn serialize(&self, batch: RecordBatch, initial: bool) -> Result<Bytes> {
        let formatted = format_batch_for_sink(&batch, &self.spec, &self.skip)?;
        match self.header.as_deref() {
            Some(header) => self
                .inner
                .serialize(with_display_header(&formatted, header)?, initial),
            None => self.inner.serialize(formatted, initial),
        }
    }
}
