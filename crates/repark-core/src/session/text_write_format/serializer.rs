use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use datafusion::common::Result;
use datafusion::datasource::file_format::write::BatchSerializer;

use super::spec::TextWriteSpec;
use super::udf::format_batch_for_sink;

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

impl BatchSerializer for ReparkTextSerializer {
    fn serialize(&self, batch: RecordBatch, initial: bool) -> Result<Bytes> {
        let formatted = format_batch_for_sink(&batch, &self.spec, &self.skip)?;
        self.inner.serialize(formatted, initial)
    }
}
