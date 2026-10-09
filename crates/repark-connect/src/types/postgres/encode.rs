use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampMicrosecondType,
};

use super::{DecimalTarget, PlannedColumn, PostgresMapping as Mapping, WireValue};
use super::{numeric, temporal, text_like};
use crate::error::{ConnectError, Result, WriteValueRefusal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteCarriage {
    CopyBinary,
    RowText,
}

pub(crate) enum ColumnEncoder {
    Boolean(BooleanArray),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Text(StringArray),
    Binary(BinaryArray),
    Numeric(Decimal128Array, DecimalTarget),
    Date(Date32Array),
    Timestamp(TimestampMicrosecondArray),
    Uuid(StringArray),
    Jsonb(StringArray),
}

fn plain_type(given: &DataType) -> DataType {
    match given {
        DataType::Dictionary(_, values) => plain_type(values),
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        plain => plain.clone(),
    }
}

impl ColumnEncoder {
    pub(crate) fn new(column: &PlannedColumn, given: &dyn Array) -> Result<ColumnEncoder> {
        let mismatch = || ConnectError::ArrowType {
            postgres_name: column.postgres_type,
            expected: column.data_type.clone(),
            actual: given.data_type().clone(),
        };
        let plain = plain_type(given.data_type());
        let instant = matches!(
            (column.mapping, &plain),
            (
                Mapping::Timestamptz,
                DataType::Timestamp(TimeUnit::Microsecond, Some(_))
            )
        );
        if !instant && plain != column.data_type {
            return Err(mismatch());
        }
        let unpacked;
        let array = if &plain == given.data_type() {
            given
        } else {
            unpacked = cast(given, &plain).map_err(|error| ConnectError::Arrow {
                message: error.to_string(),
            })?;
            unpacked.as_ref()
        };
        match column.mapping {
            Mapping::Boolean => array.as_boolean_opt().cloned().map(Self::Boolean),
            Mapping::Int16 => array
                .as_primitive_opt::<Int16Type>()
                .cloned()
                .map(Self::Int16),
            Mapping::Int32 => array
                .as_primitive_opt::<Int32Type>()
                .cloned()
                .map(Self::Int32),
            Mapping::Int64 => array
                .as_primitive_opt::<Int64Type>()
                .cloned()
                .map(Self::Int64),
            Mapping::Float32 => array
                .as_primitive_opt::<Float32Type>()
                .cloned()
                .map(Self::Float32),
            Mapping::Float64 => array
                .as_primitive_opt::<Float64Type>()
                .cloned()
                .map(Self::Float64),
            Mapping::Utf8 | Mapping::Json | Mapping::ServerText => {
                array.as_string_opt::<i32>().cloned().map(Self::Text)
            }
            Mapping::Binary => array.as_binary_opt::<i32>().cloned().map(Self::Binary),
            Mapping::Numeric(target) => array
                .as_primitive_opt::<Decimal128Type>()
                .cloned()
                .map(|typed| Self::Numeric(typed, target)),
            Mapping::Date => array
                .as_primitive_opt::<Date32Type>()
                .cloned()
                .map(Self::Date),
            Mapping::Timestamp | Mapping::Timestamptz => array
                .as_primitive_opt::<TimestampMicrosecondType>()
                .cloned()
                .map(Self::Timestamp),
            Mapping::Uuid => array.as_string_opt::<i32>().cloned().map(Self::Uuid),
            Mapping::Jsonb => array.as_string_opt::<i32>().cloned().map(Self::Jsonb),
            Mapping::Declared { registry_row } => {
                return Err(ConnectError::Declared {
                    postgres_name: column.postgres_type,
                    registry_row,
                });
            }
        }
        .ok_or_else(mismatch)
    }

    fn array(&self) -> &dyn Array {
        match self {
            Self::Boolean(array) => array,
            Self::Int16(array) => array,
            Self::Int32(array) => array,
            Self::Int64(array) => array,
            Self::Float32(array) => array,
            Self::Float64(array) => array,
            Self::Text(array) | Self::Uuid(array) | Self::Jsonb(array) => array,
            Self::Binary(array) => array,
            Self::Numeric(array, _) => array,
            Self::Date(array) => array,
            Self::Timestamp(array) => array,
        }
    }

    pub(crate) fn is_null(&self, row: usize) -> bool {
        self.array().is_null(row)
    }

    pub(crate) fn append(
        &self,
        row: usize,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), WriteValueRefusal> {
        match self {
            Self::Boolean(array) => out.push(u8::from(array.value(row))),
            Self::Int16(array) => out.extend_from_slice(&array.value(row).to_be_bytes()),
            Self::Int32(array) => out.extend_from_slice(&array.value(row).to_be_bytes()),
            Self::Int64(array) => out.extend_from_slice(&array.value(row).to_be_bytes()),
            Self::Float32(array) => out.extend_from_slice(&array.value(row).to_be_bytes()),
            Self::Float64(array) => out.extend_from_slice(&array.value(row).to_be_bytes()),
            Self::Text(array) => out.extend_from_slice(array.value(row).as_bytes()),
            Self::Binary(array) => out.extend_from_slice(array.value(row)),
            Self::Numeric(array, target) => numeric::encode(array.value(row), *target, out),
            Self::Date(array) => out.extend_from_slice(&temporal::date_wire(array.value(row))?),
            Self::Timestamp(array) => {
                out.extend_from_slice(&temporal::timestamp_wire(array.value(row))?);
            }
            Self::Uuid(array) => {
                let raw =
                    text_like::uuid_wire(array.value(row)).ok_or(WriteValueRefusal::UuidSyntax)?;
                out.extend_from_slice(&raw);
            }
            Self::Jsonb(array) => {
                out.push(text_like::JSONB_VERSION);
                out.extend_from_slice(array.value(row).as_bytes());
            }
        }
        Ok(())
    }
}

impl PlannedColumn {
    #[must_use]
    pub fn carriage(&self) -> WriteCarriage {
        match self.mapping {
            Mapping::ServerText => WriteCarriage::RowText,
            _ => WriteCarriage::CopyBinary,
        }
    }

    pub(crate) fn unwritable(&self, index: usize, reason: WriteValueRefusal) -> ConnectError {
        ConnectError::UnwritableValue {
            column: self.name.clone(),
            postgres_type: self.postgres_type,
            index,
            reason,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn encode(&self, array: &dyn Array) -> Result<Vec<WireValue>> {
        let encoder = ColumnEncoder::new(self, array)?;
        (0..array.len())
            .map(|row| {
                if encoder.is_null(row) {
                    return Ok(None);
                }
                let mut wire = Vec::new();
                encoder
                    .append(row, &mut wire)
                    .map_err(|reason| self.unwritable(row, reason))?;
                Ok(Some(wire))
            })
            .collect()
    }
}
