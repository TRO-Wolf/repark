use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::file_format::write::BatchSerializer;

use super::spec::TextWriteSpec;
use super::udf::format_batch_for_sink;
use crate::session::df_guards::duplicate_names::display_name;

pub(crate) struct ReparkTextSerializer {
    inner: Arc<dyn BatchSerializer>,
    spec: Arc<TextWriteSpec>,
    skip: Arc<HashSet<String>>,
}

impl ReparkTextSerializer {
    pub(crate) fn new(
        inner: Arc<dyn BatchSerializer>,
        spec: Arc<TextWriteSpec>,
        skip: Arc<HashSet<String>>,
    ) -> Self {
        Self { inner, spec, skip }
    }
}

pub(crate) fn with_display_header(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| match display_name(field.name()) {
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
        if self.spec.display_header {
            return self
                .inner
                .serialize(with_display_header(&formatted)?, initial);
        }
        self.inner.serialize(formatted, initial)
    }
}
