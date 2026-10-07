use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};

use crate::error::{ConnectError, ProtocolViolation, Result};
use crate::types::postgres::{ColumnAppender, PlannedColumn};

pub const COPY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";
pub const DEFAULT_BATCH_ROWS: NonZeroUsize = match NonZeroUsize::new(8192) {
    Some(rows) => rows,
    None => NonZeroUsize::MIN,
};
pub const DEFAULT_BATCH_BYTES: NonZeroUsize = match NonZeroUsize::new(64 << 20) {
    Some(bytes) => bytes,
    None => NonZeroUsize::MIN,
};
pub const MAX_FIELD_BYTES: usize = 1 << 30;
pub const MAX_BATCH_BYTES: usize = (1 << 30) - 1;

const HEADER_BYTES: usize = 19;
const OID_FLAG: u32 = 1 << 16;
const CRITICAL_FLAGS: u32 = 0xFFFE_0000;
const TRAILER: i16 = -1;
const NULL_LENGTH: i32 = -1;
const MAX_INITIAL_CAPACITY: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    rows: NonZeroUsize,
    bytes: NonZeroUsize,
}

impl BatchLimits {
    #[must_use]
    pub fn new(rows: NonZeroUsize, bytes: NonZeroUsize) -> BatchLimits {
        let capped = bytes.get().min(MAX_BATCH_BYTES);
        BatchLimits {
            rows,
            bytes: NonZeroUsize::new(capped).unwrap_or(bytes),
        }
    }

    #[must_use]
    pub fn rows(self) -> NonZeroUsize {
        self.rows
    }

    #[must_use]
    pub fn bytes(self) -> NonZeroUsize {
        self.bytes
    }
}

impl Default for BatchLimits {
    fn default() -> BatchLimits {
        BatchLimits::new(DEFAULT_BATCH_ROWS, DEFAULT_BATCH_BYTES)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Header,
    HeaderExtension { remaining: usize },
    TupleStart,
    FieldLength { column: usize },
    FieldValue { column: usize, length: usize },
    Done,
}

pub struct CopyBinaryDecoder {
    columns: Vec<PlannedColumn>,
    appenders: Vec<ColumnAppender>,
    schema: SchemaRef,
    limits: BatchLimits,
    state: State,
    carry: Vec<u8>,
    rows: usize,
    bytes: usize,
    tuples: usize,
    poisoned: Option<ConnectError>,
}

impl CopyBinaryDecoder {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(columns: Vec<PlannedColumn>, limits: BatchLimits) -> Result<CopyBinaryDecoder> {
        let capacity = limits.rows.get().min(MAX_INITIAL_CAPACITY);
        let appenders = columns
            .iter()
            .map(|column| ColumnAppender::new(column, capacity))
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(
            columns.iter().map(PlannedColumn::field).collect::<Vec<_>>(),
        ));
        Ok(CopyBinaryDecoder {
            columns,
            appenders,
            schema,
            limits,
            state: State::Header,
            carry: Vec::new(),
            rows: 0,
            bytes: 0,
            tuples: 0,
            poisoned: None,
        })
    }

    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        self.rows
    }

    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.bytes
            .saturating_add(self.validity_bytes())
            .saturating_add(self.carry.capacity())
    }

    #[must_use]
    pub fn tuples(&self) -> usize {
        self.tuples
    }

    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn decode(&mut self, chunk: &mut &[u8]) -> Result<Option<RecordBatch>> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        let input: &[u8] = chunk;
        let mut pos = 0;
        let outcome = self.run(input, &mut pos);
        *chunk = input.get(pos..).unwrap_or_default();
        if let Err(error) = &outcome {
            self.poisoned = Some(error.clone());
        }
        outcome
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn finish(&mut self) -> Result<()> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        if self.state == State::Done {
            Ok(())
        } else {
            let error = ConnectError::Disconnected;
            self.poisoned = Some(error.clone());
            Err(error)
        }
    }

    fn run(&mut self, input: &[u8], pos: &mut usize) -> Result<Option<RecordBatch>> {
        loop {
            match self.state {
                State::Header => {
                    let Some(header) = self.take_fixed::<HEADER_BYTES>(input, pos) else {
                        return Ok(None);
                    };
                    self.state = State::HeaderExtension {
                        remaining: read_header(&header)?,
                    };
                }
                State::HeaderExtension { remaining } => {
                    let skipped = remaining.min(input.len().saturating_sub(*pos));
                    *pos += skipped;
                    if skipped < remaining {
                        self.state = State::HeaderExtension {
                            remaining: remaining - skipped,
                        };
                        return Ok(None);
                    }
                    self.state = State::TupleStart;
                }
                State::TupleStart => {
                    let Some(count) = self.take_fixed::<2>(input, pos) else {
                        return Ok(None);
                    };
                    let count = i16::from_be_bytes(count);
                    if count == TRAILER {
                        self.state = State::Done;
                        if self.rows > 0 {
                            return self.flush().map(Some);
                        }
                        continue;
                    }
                    if usize::try_from(count).ok() != Some(self.columns.len()) {
                        return Err(protocol(ProtocolViolation::FieldCount {
                            expected: self.columns.len(),
                            actual: count,
                        }));
                    }
                    if self.columns.is_empty() {
                        if let Some(batch) = self.end_tuple()? {
                            return Ok(Some(batch));
                        }
                    } else {
                        self.state = State::FieldLength { column: 0 };
                    }
                }
                State::FieldLength { column } => {
                    let Some(length) = self.take_fixed::<4>(input, pos) else {
                        return Ok(None);
                    };
                    let length = i32::from_be_bytes(length);
                    if length == NULL_LENGTH {
                        self.append_null(column)?;
                        if let Some(batch) = self.end_field(column)? {
                            return Ok(Some(batch));
                        }
                        continue;
                    }
                    let length = field_length(length)?;
                    self.state = State::FieldValue { column, length };
                }
                State::FieldValue { column, length } => {
                    let available = input.len().saturating_sub(*pos);
                    if self.carry.is_empty() && available >= length {
                        let bytes = input.get(*pos..*pos + length).unwrap_or_default();
                        *pos += length;
                        self.append_value(column, bytes)?;
                    } else {
                        let taken = (length - self.carry.len()).min(available);
                        self.carry
                            .extend_from_slice(input.get(*pos..*pos + taken).unwrap_or_default());
                        *pos += taken;
                        if self.carry.len() < length {
                            return Ok(None);
                        }
                        let carried = std::mem::take(&mut self.carry);
                        let appended = self.append_value(column, &carried);
                        self.carry = carried;
                        self.carry.clear();
                        if self.carry.capacity() > self.limits.bytes.get() {
                            self.carry = Vec::new();
                        }
                        appended?;
                    }
                    if let Some(batch) = self.end_field(column)? {
                        return Ok(Some(batch));
                    }
                }
                State::Done => {
                    if *pos < input.len() {
                        return Err(protocol(ProtocolViolation::TrailingBytes));
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn take_fixed<const N: usize>(&mut self, input: &[u8], pos: &mut usize) -> Option<[u8; N]> {
        let available = input.get(*pos..).unwrap_or_default();
        if self.carry.is_empty()
            && let Some(head) = available.get(..N)
        {
            *pos += N;
            return <[u8; N]>::try_from(head).ok();
        }
        let taken = N.saturating_sub(self.carry.len()).min(available.len());
        self.carry
            .extend_from_slice(available.get(..taken).unwrap_or_default());
        *pos += taken;
        if self.carry.len() < N {
            return None;
        }
        let complete = <[u8; N]>::try_from(self.carry.as_slice()).ok();
        self.carry.clear();
        complete
    }

    fn append_value(&mut self, column: usize, bytes: &[u8]) -> Result<()> {
        let index = self.tuples;
        let (Some(appender), Some(planned)) =
            (self.appenders.get_mut(column), self.columns.get(column))
        else {
            return Err(self.column_count_violation());
        };
        self.bytes += appender
            .append(bytes)
            .map_err(|error| error.at(planned, index))?;
        Ok(())
    }

    fn append_null(&mut self, column: usize) -> Result<()> {
        let (Some(appender), Some(planned)) =
            (self.appenders.get_mut(column), self.columns.get(column))
        else {
            return Err(self.column_count_violation());
        };
        if !planned.nullable() {
            return Err(protocol(ProtocolViolation::NullInNotNullColumn { column }));
        }
        self.bytes += appender.append_null();
        Ok(())
    }

    fn end_field(&mut self, column: usize) -> Result<Option<RecordBatch>> {
        if column + 1 < self.columns.len() {
            self.state = State::FieldLength { column: column + 1 };
            return Ok(None);
        }
        self.end_tuple()
    }

    fn end_tuple(&mut self) -> Result<Option<RecordBatch>> {
        self.state = State::TupleStart;
        self.rows += 1;
        self.tuples += 1;
        let charged = self.bytes.saturating_add(self.validity_bytes());
        if self.rows >= self.limits.rows.get() || charged >= self.limits.bytes.get() {
            return self.flush().map(Some);
        }
        Ok(None)
    }

    fn validity_bytes(&self) -> usize {
        self.rows.saturating_mul(self.columns.len()).div_ceil(8)
    }

    fn flush(&mut self) -> Result<RecordBatch> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        let arrays: Vec<ArrayRef> = self
            .appenders
            .iter_mut()
            .map(ColumnAppender::finish)
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(self.rows));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &options)
            .map_err(|error| ConnectError::Arrow {
            message: error.to_string(),
        })?;
        self.rows = 0;
        self.bytes = 0;
        Ok(batch)
    }

    fn column_count_violation(&self) -> ConnectError {
        protocol(ProtocolViolation::FieldCount {
            expected: self.columns.len(),
            actual: i16::MAX,
        })
    }
}

fn field_length(length: i32) -> Result<usize> {
    let length =
        usize::try_from(length).map_err(|_| protocol(ProtocolViolation::FieldLength { length }))?;
    if length > MAX_FIELD_BYTES {
        return Err(protocol(ProtocolViolation::FieldTooLong {
            length,
            max: MAX_FIELD_BYTES,
        }));
    }
    Ok(length)
}

fn read_header(header: &[u8; HEADER_BYTES]) -> Result<usize> {
    let (signature, rest) = header.split_at(COPY_SIGNATURE.len());
    if signature != COPY_SIGNATURE {
        return Err(protocol(ProtocolViolation::Signature));
    }
    let (flags, extension) = rest.split_at(4);
    let flags = u32::from_be_bytes(<[u8; 4]>::try_from(flags).unwrap_or_default());
    if flags & OID_FLAG != 0 {
        return Err(protocol(ProtocolViolation::OidColumns));
    }
    if flags & CRITICAL_FLAGS != 0 {
        return Err(protocol(ProtocolViolation::CriticalFlags { flags }));
    }
    let length = i32::from_be_bytes(<[u8; 4]>::try_from(extension).unwrap_or_default());
    usize::try_from(length).map_err(|_| protocol(ProtocolViolation::HeaderExtension { length }))
}

fn protocol(violation: ProtocolViolation) -> ConnectError {
    ConnectError::Protocol { violation }
}
