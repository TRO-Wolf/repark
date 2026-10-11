mod encode;
mod numeric;
mod temporal;
mod text_like;

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, TimeUnit};

use self::PostgresMapping as Mapping;
use crate::error::{ConnectError, ProtocolViolation, Result, UNMAPPED_ROW, ValueRefusal};

#[cfg(feature = "postgres")]
pub(crate) use encode::ColumnEncoder;
pub use encode::WriteCarriage;
pub use numeric::DecimalTarget;
pub use temporal::{POSTGRES_EPOCH_DAYS, POSTGRES_EPOCH_MICROS, UTC_ZONE_LABEL};

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
    Numeric(DecimalTarget),
    Date,
    Timestamp,
    Timestamptz,
    Uuid,
    Json,
    Jsonb,
    ServerText,
    Declared { registry_row: &'static str },
}

impl PostgresMapping {
    #[must_use]
    pub fn data_type(self) -> Option<DataType> {
        Some(match self {
            Mapping::Boolean => DataType::Boolean,
            Mapping::Int16 => DataType::Int16,
            Mapping::Int32 => DataType::Int32,
            Mapping::Int64 => DataType::Int64,
            Mapping::Float32 => DataType::Float32,
            Mapping::Float64 => DataType::Float64,
            Mapping::Utf8
            | Mapping::Uuid
            | Mapping::Json
            | Mapping::Jsonb
            | Mapping::ServerText => DataType::Utf8,
            Mapping::Binary => DataType::Binary,
            Mapping::Numeric(target) => DataType::Decimal128(target.precision(), target.scale()),
            Mapping::Date => DataType::Date32,
            Mapping::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Mapping::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_ZONE_LABEL.into()))
            }
            Mapping::Declared { .. } => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeMod(i32);

impl TypeMod {
    pub const NONE: TypeMod = TypeMod(-1);

    #[must_use]
    pub fn new(atttypmod: i32) -> TypeMod {
        TypeMod(atttypmod)
    }

    #[must_use]
    pub fn numeric(precision: i32, scale: i32) -> TypeMod {
        TypeMod(((precision << 16) | (scale & 0x7ff)) + 4)
    }

    #[must_use]
    pub fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTypeKind {
    Base,
    Enum,
    Other,
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
        mapping: Mapping::Declared { registry_row },
        pin: DECLARED_PIN,
    }
}

pub const POSTGRES_TYPES: &[PostgresTypeRow] = &[
    mapped("bool", Mapping::Boolean, "bool_round_trips"),
    mapped("int2", Mapping::Int16, "int2_round_trips"),
    mapped("int4", Mapping::Int32, "int4_round_trips"),
    mapped("int8", Mapping::Int64, "int8_round_trips"),
    mapped("float4", Mapping::Float32, "float4_round_trips"),
    mapped("float8", Mapping::Float64, "float8_round_trips"),
    mapped("text", Mapping::Utf8, "text_round_trips"),
    mapped("varchar", Mapping::Utf8, "varchar_round_trips"),
    mapped("bpchar", Mapping::Utf8, "bpchar_round_trips"),
    mapped("bytea", Mapping::Binary, "bytea_round_trips"),
    mapped(
        "numeric",
        Mapping::Numeric(DecimalTarget::UNCONSTRAINED),
        "numeric_anchors_round_trip",
    ),
    mapped("date", Mapping::Date, "date_anchors_round_trip"),
    declared("time", "CONNECT-DECL-pg-time"),
    mapped(
        "timestamp",
        Mapping::Timestamp,
        "timestamp_ntz_anchors_round_trip",
    ),
    mapped(
        "timestamptz",
        Mapping::Timestamptz,
        "timestamptz_anchors_round_trip",
    ),
    mapped(
        "interval",
        Mapping::ServerText,
        "json_and_interval_are_text_verbatim",
    ),
    mapped("uuid", Mapping::Uuid, "uuid_renders_lowercase_canonical"),
    mapped("json", Mapping::Json, "json_and_interval_are_text_verbatim"),
    mapped(
        "jsonb",
        Mapping::Jsonb,
        "jsonb_strips_version_one_and_refuses_others",
    ),
];

#[must_use]
pub fn postgres_type(postgres_name: &str) -> Option<&'static PostgresTypeRow> {
    POSTGRES_TYPES
        .iter()
        .find(|row| row.postgres_name == postgres_name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedColumn {
    name: Arc<str>,
    postgres_type: &'static str,
    mapping: PostgresMapping,
    data_type: DataType,
    nullable: bool,
}

impl PlannedColumn {
    #[allow(clippy::missing_errors_doc)]
    pub fn resolve(
        name: Arc<str>,
        typname: &str,
        kind: PgTypeKind,
        typmod: TypeMod,
    ) -> Result<PlannedColumn> {
        let unmapped = |postgres_type: String, row: &'static str| ConnectError::UnmappedType {
            column: Arc::clone(&name),
            postgres_type,
            row,
        };
        let row = match kind {
            PgTypeKind::Enum => return PlannedColumn::new(name, "enum", Mapping::Utf8),
            PgTypeKind::Other => None,
            PgTypeKind::Base => postgres_type(typname),
        }
        .ok_or_else(|| unmapped(typname.to_string(), UNMAPPED_ROW))?;
        let mapping = match row.mapping {
            Mapping::Declared { registry_row } => {
                return Err(unmapped(typname.to_string(), registry_row));
            }
            Mapping::Numeric(_) => {
                let modifier = numeric::modifier(typmod.get());
                let target = DecimalTarget::from_modifier(modifier).ok_or_else(|| {
                    unmapped(format!("{typname}{}", modifier.render()), UNMAPPED_ROW)
                })?;
                Mapping::Numeric(target)
            }
            mapping => mapping,
        };
        PlannedColumn::new(name, row.postgres_name, mapping)
    }

    fn new(
        name: Arc<str>,
        postgres_type: &'static str,
        mapping: PostgresMapping,
    ) -> Result<PlannedColumn> {
        let data_type = mapping.data_type().ok_or(ConnectError::Declared {
            postgres_name: postgres_type,
            registry_row: UNMAPPED_ROW,
        })?;
        Ok(PlannedColumn {
            name,
            postgres_type,
            mapping,
            data_type,
            nullable: true,
        })
    }

    #[must_use]
    pub fn with_nullable(mut self, nullable: bool) -> PlannedColumn {
        self.nullable = nullable;
        self
    }

    #[must_use]
    pub fn mapping(&self) -> PostgresMapping {
        self.mapping
    }

    #[must_use]
    pub fn postgres_type(&self) -> &'static str {
        self.postgres_type
    }

    #[must_use]
    pub fn nullable(&self) -> bool {
        self.nullable
    }

    #[must_use]
    pub fn field(&self) -> Field {
        Field::new(self.name.as_ref(), self.data_type.clone(), self.nullable)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn decode(&self, values: &[Option<&[u8]>]) -> Result<ArrayRef> {
        let mut appender = ColumnAppender::new(self, values.len())?;
        for (index, value) in values.iter().enumerate() {
            if let Some(bytes) = value {
                appender
                    .append(bytes)
                    .map_err(|error| error.at(self, index))?;
            } else {
                appender.append_null();
            }
        }
        Ok(appender.finish())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodecError {
    WireLength { expected: usize, actual: usize },
    InvalidUtf8,
    Refused(ValueRefusal),
    Malformed(ProtocolViolation),
}

impl CodecError {
    pub(crate) fn at(self, column: &PlannedColumn, index: usize) -> ConnectError {
        let postgres_name = column.postgres_type;
        match self {
            CodecError::WireLength { expected, actual } => ConnectError::WireLength {
                postgres_name,
                index,
                expected,
                actual,
            },
            CodecError::InvalidUtf8 => ConnectError::InvalidUtf8 {
                postgres_name,
                index,
            },
            CodecError::Refused(reason) => ConnectError::UnrepresentableValue {
                column: Arc::clone(&column.name),
                postgres_type: postgres_name,
                index,
                reason,
            },
            CodecError::Malformed(violation) => ConnectError::Protocol { violation },
        }
    }
}

fn fixed<const N: usize>(bytes: &[u8]) -> std::result::Result<[u8; N], CodecError> {
    <[u8; N]>::try_from(bytes).map_err(|_| CodecError::WireLength {
        expected: N,
        actual: bytes.len(),
    })
}

macro_rules! each_builder {
    ($appender:expr, $builder:ident => $body:expr) => {
        match $appender {
            ColumnAppender::Boolean($builder) => $body,
            ColumnAppender::Int16($builder) => $body,
            ColumnAppender::Int32($builder) => $body,
            ColumnAppender::Int64($builder) => $body,
            ColumnAppender::Float32($builder) => $body,
            ColumnAppender::Float64($builder) => $body,
            ColumnAppender::Binary($builder) => $body,
            ColumnAppender::Numeric($builder, _) => $body,
            ColumnAppender::Date($builder) => $body,
            ColumnAppender::Utf8($builder)
            | ColumnAppender::Uuid($builder)
            | ColumnAppender::Json($builder)
            | ColumnAppender::Jsonb($builder)
            | ColumnAppender::ServerText($builder) => $body,
            ColumnAppender::Timestamp($builder) | ColumnAppender::Timestamptz($builder) => $body,
        }
    };
}

pub(crate) enum ColumnAppender {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Numeric(Decimal128Builder, DecimalTarget),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Timestamptz(TimestampMicrosecondBuilder),
    Uuid(StringBuilder),
    Json(StringBuilder),
    Jsonb(StringBuilder),
    ServerText(StringBuilder),
}

impl ColumnAppender {
    pub(crate) fn new(column: &PlannedColumn, capacity: usize) -> Result<ColumnAppender> {
        Ok(match column.mapping {
            Mapping::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            Mapping::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            Mapping::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            Mapping::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            Mapping::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            Mapping::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            Mapping::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, 0)),
            Mapping::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, 0)),
            Mapping::Numeric(target) => Self::Numeric(numeric::builder(capacity, target)?, target),
            Mapping::Date => Self::Date(Date32Builder::with_capacity(capacity)),
            Mapping::Timestamp => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            Mapping::Timestamptz => Self::Timestamptz(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone(UTC_ZONE_LABEL),
            ),
            Mapping::Uuid => Self::Uuid(StringBuilder::with_capacity(capacity, 0)),
            Mapping::Json => Self::Json(StringBuilder::with_capacity(capacity, 0)),
            Mapping::Jsonb => Self::Jsonb(StringBuilder::with_capacity(capacity, 0)),
            Mapping::ServerText => Self::ServerText(StringBuilder::with_capacity(capacity, 0)),
            Mapping::Declared { registry_row } => {
                return Err(ConnectError::Declared {
                    postgres_name: column.postgres_type,
                    registry_row,
                });
            }
        })
    }

    pub(crate) fn arrow_width(&self) -> Option<usize> {
        match self {
            Self::Boolean(_) => Some(1),
            Self::Int16(_) => Some(2),
            Self::Int32(_) | Self::Float32(_) | Self::Date(_) => Some(4),
            Self::Int64(_) | Self::Float64(_) | Self::Timestamp(_) | Self::Timestamptz(_) => {
                Some(8)
            }
            Self::Numeric(_, _) => Some(16),
            Self::Utf8(_)
            | Self::Binary(_)
            | Self::Uuid(_)
            | Self::Json(_)
            | Self::Jsonb(_)
            | Self::ServerText(_) => None,
        }
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> std::result::Result<usize, CodecError> {
        let width = self.arrow_width();
        match self {
            Self::Boolean(builder) => {
                let [byte] = fixed::<1>(bytes)?;
                builder.append_value(byte != 0);
            }
            Self::Int16(builder) => builder.append_value(i16::from_be_bytes(fixed(bytes)?)),
            Self::Int32(builder) => builder.append_value(i32::from_be_bytes(fixed(bytes)?)),
            Self::Int64(builder) => builder.append_value(i64::from_be_bytes(fixed(bytes)?)),
            Self::Float32(builder) => builder.append_value(f32::from_be_bytes(fixed(bytes)?)),
            Self::Float64(builder) => builder.append_value(f64::from_be_bytes(fixed(bytes)?)),
            Self::Utf8(builder) | Self::Json(builder) | Self::ServerText(builder) => {
                builder.append_value(text_like::text(bytes)?);
            }
            Self::Binary(builder) => builder.append_value(bytes),
            Self::Numeric(builder, target) => {
                builder.append_value(numeric::decode(bytes, *target)?);
            }
            Self::Date(builder) => builder.append_value(temporal::date(bytes)?),
            Self::Timestamp(builder) | Self::Timestamptz(builder) => {
                builder.append_value(temporal::timestamp(bytes)?);
            }
            Self::Uuid(builder) => {
                let mut rendered = [0_u8; text_like::UUID_TEXT_BYTES];
                builder.append_value(text_like::uuid(bytes, &mut rendered)?);
                return Ok(text_like::UUID_TEXT_BYTES + OFFSET_BYTES);
            }
            Self::Jsonb(builder) => {
                let body = text_like::jsonb(bytes)?;
                builder.append_value(body);
                return Ok(body.len() + OFFSET_BYTES);
            }
        }
        Ok(width.unwrap_or(bytes.len() + OFFSET_BYTES))
    }

    pub(crate) fn append_null(&mut self) -> usize {
        each_builder!(self, builder => builder.append_null());
        self.arrow_width().unwrap_or(OFFSET_BYTES)
    }

    pub(crate) fn finish(&mut self) -> ArrayRef {
        each_builder!(self, builder => Arc::new(builder.finish()))
    }
}

const OFFSET_BYTES: usize = 4;

impl PostgresTypeRow {
    #[allow(clippy::missing_errors_doc)]
    pub fn encode(&self, array: &dyn Array) -> Result<Vec<WireValue>> {
        self.planned()?.encode(array)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn decode(&self, values: &[Option<&[u8]>]) -> Result<ArrayRef> {
        self.planned()?.decode(values)
    }

    fn planned(&self) -> Result<PlannedColumn> {
        if let Mapping::Declared { registry_row } = self.mapping {
            return Err(self.declared(registry_row));
        }
        PlannedColumn::new(
            Arc::from(self.postgres_name),
            self.postgres_name,
            self.mapping,
        )
    }

    fn declared(&self, registry_row: &'static str) -> ConnectError {
        ConnectError::Declared {
            postgres_name: self.postgres_name,
            registry_row,
        }
    }
}
