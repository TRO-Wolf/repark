use std::pin::Pin;
use std::time::Duration;

use arrow::array::RecordBatch;
use bytes::Bytes;
use futures::SinkExt;
use tokio_postgres::{Client, CopyInSink};

use super::{WriteOptions, WriteRequest, asked, encoders};
use crate::copy_binary::COPY_SIGNATURE;
use crate::error::Result;
use crate::ident::QualifiedRelation;
use crate::types::postgres::{ColumnEncoder, PlannedColumn};

const HEADER_WORDS: [u8; 8] = [0; 8];
const NULL_FIELD: i32 = -1;
const TRAILER: i16 = -1;
const LENGTH_BYTES: usize = 4;

pub struct CopyBinaryEncoder {
    columns: Vec<PlannedColumn>,
    chunk_bytes: usize,
    started: bool,
    rows: usize,
}

impl CopyBinaryEncoder {
    #[must_use]
    pub fn new(columns: Vec<PlannedColumn>, chunk_bytes: usize) -> CopyBinaryEncoder {
        CopyBinaryEncoder {
            columns,
            chunk_bytes,
            started: false,
            rows: 0,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn batch<'a>(&'a mut self, batch: &'a RecordBatch) -> Result<CopyChunks<'a>> {
        let fields = encoders(&self.columns, batch)?;
        Ok(CopyChunks {
            encoder: self,
            fields,
            row: 0,
            rows: batch.num_rows(),
        })
    }

    #[must_use]
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = self.opening();
        out.extend_from_slice(&TRAILER.to_be_bytes());
        out
    }

    fn opening(&mut self) -> Vec<u8> {
        if std::mem::replace(&mut self.started, true) {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(COPY_SIGNATURE.len() + HEADER_WORDS.len());
        out.extend_from_slice(&COPY_SIGNATURE);
        out.extend_from_slice(&HEADER_WORDS);
        out
    }
}

pub struct CopyChunks<'a> {
    encoder: &'a mut CopyBinaryEncoder,
    fields: Vec<ColumnEncoder<'a>>,
    row: usize,
    rows: usize,
}

impl CopyChunks<'_> {
    #[allow(clippy::missing_errors_doc)]
    pub fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if self.row == self.rows {
            return Ok(None);
        }
        let mut out = self.encoder.opening();
        let width = i16::try_from(self.fields.len()).unwrap_or(i16::MAX);
        loop {
            out.extend_from_slice(&width.to_be_bytes());
            for (field, column) in self.fields.iter().zip(&self.encoder.columns) {
                if field.is_null(self.row) {
                    out.extend_from_slice(&NULL_FIELD.to_be_bytes());
                    continue;
                }
                let start = out.len();
                out.extend_from_slice(&[0; LENGTH_BYTES]);
                field
                    .append(self.row, &mut out)
                    .map_err(|reason| column.unwritable(self.encoder.rows, reason))?;
                let length = i32::try_from(out.len() - start - LENGTH_BYTES).unwrap_or(i32::MAX);
                out[start..start + LENGTH_BYTES].copy_from_slice(&length.to_be_bytes());
            }
            self.row += 1;
            self.encoder.rows += 1;
            if self.row == self.rows || out.len() >= self.encoder.chunk_bytes {
                break;
            }
        }
        Ok(Some(out))
    }
}

struct CopyWire {
    sink: Pin<Box<CopyInSink<Bytes>>>,
    relation: QualifiedRelation,
    read_timeout: Duration,
}

impl CopyWire {
    async fn send(&mut self, chunk: Vec<u8>) -> Result<()> {
        let sending = self.sink.send(Bytes::from(chunk));
        asked(self.read_timeout, &self.relation, sending).await
    }
}

pub(crate) struct CopyLane {
    wire: CopyWire,
    encoder: CopyBinaryEncoder,
}

impl CopyLane {
    pub(crate) async fn open(
        client: &Client,
        request: &WriteRequest,
        options: WriteOptions,
    ) -> Result<CopyLane> {
        let relation = request.relation().clone();
        let statement = request.copy_statement();
        let sink = client.copy_in::<_, Bytes>(statement.as_str());
        let sink = asked(options.read_timeout, &relation, sink).await?;
        Ok(CopyLane {
            wire: CopyWire {
                sink: Box::pin(sink),
                relation,
                read_timeout: options.read_timeout,
            },
            encoder: CopyBinaryEncoder::new(request.planned(), options.copy_chunk_bytes),
        })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut chunks = self.encoder.batch(batch)?;
        while let Some(chunk) = chunks.next_chunk()? {
            self.wire.send(chunk).await?;
        }
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<u64> {
        let trailer = self.encoder.finish();
        self.wire.send(trailer).await?;
        let finishing = self.wire.sink.as_mut().finish();
        asked(self.wire.read_timeout, &self.wire.relation, finishing).await
    }
}
