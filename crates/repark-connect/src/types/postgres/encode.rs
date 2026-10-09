use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
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

pub(crate) enum ColumnEncoder<'a> {
    Boolean(&'a BooleanArray),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Text(&'a StringArray),
    Binary(&'a BinaryArray),
    Numeric(&'a Decimal128Array, DecimalTarget),
    Date(&'a Date32Array),
    Timestamp(&'a TimestampMicrosecondArray),
    Uuid(&'a StringArray),
    Jsonb(&'a StringArray),
}

impl<'a> ColumnEncoder<'a> {
    pub(crate) fn new(column: &PlannedColumn, array: &'a dyn Array) -> Result<ColumnEncoder<'a>> {
        let mismatch = || ConnectError::ArrowType {
            postgres_name: column.postgres_type,
            expected: column.data_type.clone(),
            actual: array.data_type().clone(),
        };
        let instant = matches!(
            (column.mapping, array.data_type()),
            (
                Mapping::Timestamptz,
                DataType::Timestamp(TimeUnit::Microsecond, Some(_))
            )
        );
        if !instant && array.data_type() != &column.data_type {
            return Err(mismatch());
        }
        match column.mapping {
            Mapping::Boolean => array.as_boolean_opt().map(Self::Boolean),
            Mapping::Int16 => array.as_primitive_opt::<Int16Type>().map(Self::Int16),
            Mapping::Int32 => array.as_primitive_opt::<Int32Type>().map(Self::Int32),
            Mapping::Int64 => array.as_primitive_opt::<Int64Type>().map(Self::Int64),
            Mapping::Float32 => array.as_primitive_opt::<Float32Type>().map(Self::Float32),
            Mapping::Float64 => array.as_primitive_opt::<Float64Type>().map(Self::Float64),
            Mapping::Utf8 | Mapping::Json | Mapping::ServerText => {
                array.as_string_opt::<i32>().map(Self::Text)
            }
            Mapping::Binary => array.as_binary_opt::<i32>().map(Self::Binary),
            Mapping::Numeric(target) => array
                .as_primitive_opt::<Decimal128Type>()
                .map(|typed| Self::Numeric(typed, target)),
            Mapping::Date => array.as_primitive_opt::<Date32Type>().map(Self::Date),
            Mapping::Timestamp | Mapping::Timestamptz => array
                .as_primitive_opt::<TimestampMicrosecondType>()
                .map(Self::Timestamp),
            Mapping::Uuid => array.as_string_opt::<i32>().map(Self::Uuid),
            Mapping::Jsonb => array.as_string_opt::<i32>().map(Self::Jsonb),
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
            Self::Boolean(array) => *array,
            Self::Int16(array) => *array,
            Self::Int32(array) => *array,
            Self::Int64(array) => *array,
            Self::Float32(array) => *array,
            Self::Float64(array) => *array,
            Self::Text(array) | Self::Uuid(array) | Self::Jsonb(array) => *array,
            Self::Binary(array) => *array,
            Self::Numeric(array, _) => *array,
            Self::Date(array) => *array,
            Self::Timestamp(array) => *array,
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
