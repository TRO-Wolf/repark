use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, PrimitiveArray, StringArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};

const REGISTRY: &str = "docs/spark-sql-iceberg-parity.md";

pub type WireValue = Option<Vec<u8>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresMapping {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Declared { registry_row: &'static str },
}

impl PostgresMapping {
    #[must_use]
    pub fn data_type(self) -> Option<DataType> {
        match self {
            PostgresMapping::Boolean => Some(DataType::Boolean),
            PostgresMapping::Int16 => Some(DataType::Int16),
            PostgresMapping::Int32 => Some(DataType::Int32),
            PostgresMapping::Int64 => Some(DataType::Int64),
            PostgresMapping::Float32 => Some(DataType::Float32),
            PostgresMapping::Float64 => Some(DataType::Float64),
            PostgresMapping::Utf8 => Some(DataType::Utf8),
            PostgresMapping::Binary => Some(DataType::Binary),
            PostgresMapping::Declared { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresTypeRow {
    pub postgres_name: &'static str,
    pub mapping: PostgresMapping,
    pub pin: &'static str,
}

const DECLARED_PIN: &str = "declared_types_refuse_naming_their_row";

const fn mapped(
    postgres_name: &'static str,
    mapping: PostgresMapping,
    pin: &'static str,
) -> PostgresTypeRow {
    PostgresTypeRow {
        postgres_name,
        mapping,
        pin,
    }
}

const fn declared(postgres_name: &'static str, registry_row: &'static str) -> PostgresTypeRow {
    PostgresTypeRow {
        postgres_name,
        mapping: PostgresMapping::Declared { registry_row },
        pin: DECLARED_PIN,
    }
}

pub const POSTGRES_TYPES: &[PostgresTypeRow] = &[
    mapped("bool", PostgresMapping::Boolean, "bool_round_trips"),
    mapped("int2", PostgresMapping::Int16, "int2_round_trips"),
    mapped("int4", PostgresMapping::Int32, "int4_round_trips"),
    mapped("int8", PostgresMapping::Int64, "int8_round_trips"),
    mapped("float4", PostgresMapping::Float32, "float4_round_trips"),
    mapped("float8", PostgresMapping::Float64, "float8_round_trips"),
    mapped("text", PostgresMapping::Utf8, "text_round_trips"),
    mapped("varchar", PostgresMapping::Utf8, "varchar_round_trips"),
    mapped("bpchar", PostgresMapping::Utf8, "bpchar_round_trips"),
    mapped("bytea", PostgresMapping::Binary, "bytea_round_trips"),
    declared("numeric", "CONNECT-DECL-pg-numeric"),
    declared("date", "CONNECT-DECL-pg-date"),
    declared("time", "CONNECT-DECL-pg-time"),
    declared("timestamp", "CONNECT-DECL-pg-timestamp"),
    declared("timestamptz", "CONNECT-DECL-pg-timestamptz"),
    declared("interval", "CONNECT-DECL-pg-interval"),
    declared("uuid", "CONNECT-DECL-pg-uuid"),
    declared("json", "CONNECT-DECL-pg-json"),
    declared("jsonb", "CONNECT-DECL-pg-jsonb"),
];

#[must_use]
pub fn postgres_type(postgres_name: &str) -> Option<&'static PostgresTypeRow> {
    POSTGRES_TYPES
        .iter()
        .find(|row| row.postgres_name == postgres_name)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeMapError {
    #[error(
        "Postgres type `{postgres_name}` has no Arrow mapping: declared \
         (registry row {registry_row} in {REGISTRY})"
    )]
    Declared {
        postgres_name: &'static str,
        registry_row: &'static str,
    },

    #[error(
        "an Arrow {actual} array cannot encode as Postgres `{postgres_name}`, which maps to {expected}"
    )]
    ArrowType {
        postgres_name: &'static str,
        expected: DataType,
        actual: DataType,
    },

    #[error(
        "Postgres `{postgres_name}` wire value at row {index} is {actual} bytes; expected {expected}"
    )]
    WireLength {
        postgres_name: &'static str,
        index: usize,
        expected: usize,
        actual: usize,
    },

    #[error("Postgres `{postgres_name}` wire value at row {index} is not valid UTF-8")]
    InvalidUtf8 {
        postgres_name: &'static str,
        index: usize,
    },
}

impl PostgresTypeRow {
    #[allow(clippy::missing_errors_doc)]
    pub fn encode(&self, array: &dyn Array) -> Result<Vec<WireValue>, TypeMapError> {
        match self.mapping {
            PostgresMapping::Boolean => {
                let typed = array
                    .as_boolean_opt()
                    .ok_or_else(|| self.mismatch(array, DataType::Boolean))?;
                Ok(typed
                    .iter()
                    .map(|value| value.map(|flag| vec![u8::from(flag)]))
                    .collect())
            }
            PostgresMapping::Int16 => {
                self.encode_primitive::<Int16Type, 2>(array, i16::to_be_bytes)
            }
            PostgresMapping::Int32 => {
                self.encode_primitive::<Int32Type, 4>(array, i32::to_be_bytes)
            }
            PostgresMapping::Int64 => {
                self.encode_primitive::<Int64Type, 8>(array, i64::to_be_bytes)
            }
            PostgresMapping::Float32 => {
                self.encode_primitive::<Float32Type, 4>(array, f32::to_be_bytes)
            }
            PostgresMapping::Float64 => {
                self.encode_primitive::<Float64Type, 8>(array, f64::to_be_bytes)
            }
            PostgresMapping::Utf8 => {
                let typed = array
                    .as_string_opt::<i32>()
                    .ok_or_else(|| self.mismatch(array, DataType::Utf8))?;
                Ok(typed
                    .iter()
                    .map(|value| value.map(|text| text.as_bytes().to_vec()))
                    .collect())
            }
            PostgresMapping::Binary => {
                let typed = array
                    .as_binary_opt::<i32>()
                    .ok_or_else(|| self.mismatch(array, DataType::Binary))?;
                Ok(typed
                    .iter()
                    .map(|value| value.map(<[u8]>::to_vec))
                    .collect())
            }
            PostgresMapping::Declared { registry_row } => Err(self.declared(registry_row)),
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn decode(&self, values: &[Option<&[u8]>]) -> Result<ArrayRef, TypeMapError> {
        match self.mapping {
            PostgresMapping::Boolean => {
                let flags = values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        value
                            .map(|bytes| self.fixed::<1>(index, bytes).map(|[byte]| byte != 0))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(BooleanArray::from(flags)))
            }
            PostgresMapping::Int16 => {
                self.decode_primitive::<Int16Type, 2>(values, i16::from_be_bytes)
            }
            PostgresMapping::Int32 => {
                self.decode_primitive::<Int32Type, 4>(values, i32::from_be_bytes)
            }
            PostgresMapping::Int64 => {
                self.decode_primitive::<Int64Type, 8>(values, i64::from_be_bytes)
            }
            PostgresMapping::Float32 => {
                self.decode_primitive::<Float32Type, 4>(values, f32::from_be_bytes)
            }
            PostgresMapping::Float64 => {
                self.decode_primitive::<Float64Type, 8>(values, f64::from_be_bytes)
            }
            PostgresMapping::Utf8 => {
                let texts = values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        value
                            .map(|bytes| {
                                std::str::from_utf8(bytes).map_err(|_| TypeMapError::InvalidUtf8 {
                                    postgres_name: self.postgres_name,
                                    index,
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(StringArray::from(texts)))
            }
            PostgresMapping::Binary => Ok(Arc::new(BinaryArray::from(values.to_vec()))),
            PostgresMapping::Declared { registry_row } => Err(self.declared(registry_row)),
        }
    }

    fn encode_primitive<T: ArrowPrimitiveType, const N: usize>(
        &self,
        array: &dyn Array,
        to_bytes: fn(T::Native) -> [u8; N],
    ) -> Result<Vec<WireValue>, TypeMapError> {
        let typed = array
            .as_primitive_opt::<T>()
            .ok_or_else(|| self.mismatch(array, T::DATA_TYPE))?;
        Ok(typed
            .iter()
            .map(|value| value.map(|native| to_bytes(native).to_vec()))
            .collect())
    }

    fn decode_primitive<T: ArrowPrimitiveType, const N: usize>(
        &self,
        values: &[Option<&[u8]>],
        from_bytes: fn([u8; N]) -> T::Native,
    ) -> Result<ArrayRef, TypeMapError> {
        let natives = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .map(|bytes| self.fixed::<N>(index, bytes).map(from_bytes))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(natives.into_iter().collect::<PrimitiveArray<T>>()))
    }

    fn fixed<const N: usize>(&self, index: usize, bytes: &[u8]) -> Result<[u8; N], TypeMapError> {
        <[u8; N]>::try_from(bytes).map_err(|_| TypeMapError::WireLength {
            postgres_name: self.postgres_name,
            index,
            expected: N,
            actual: bytes.len(),
        })
    }

    fn mismatch(&self, array: &dyn Array, expected: DataType) -> TypeMapError {
        TypeMapError::ArrowType {
            postgres_name: self.postgres_name,
            expected,
            actual: array.data_type().clone(),
        }
    }

    fn declared(&self, registry_row: &'static str) -> TypeMapError {
        TypeMapError::Declared {
            postgres_name: self.postgres_name,
            registry_row,
        }
    }
}
