use std::borrow::Cow;
use std::fmt::{self, Display};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};

mod parse;

#[cfg(test)]
mod tests;

pub use parse::{parse_ddl, sql_type_from_token};

pub(crate) const SPARK_TYPE_NAME_MAX_DEPTH: usize = 32;

pub(crate) const SPARK_TYPE_NAME_DEPTH_FALLBACK: &str = "...";

pub const DEFAULT_COLLATION: &str = "UTF8_BINARY";

pub const SPATIAL_MIXED_SRID: i64 = -1;

pub(crate) const GEOMETRY_SRIDS: &[i64] = &[0, 3857, 4326];

pub(crate) const GEOGRAPHY_SRIDS: &[i64] = &[4326];

#[derive(Debug, Clone)]
pub enum TypeTableError {
    Message(String),
    IntegerOverflow,
    DecimalPrecision { precision: i64 },
    DecimalScale { precision: i64, scale: i64 },
}

impl Display for TypeTableError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => out.write_str(message),
            Self::IntegerOverflow => out.write_str("integer parameter beyond i64 range"),
            Self::DecimalPrecision { .. } => out.write_str("precision should be between 1 and 38"),
            Self::DecimalScale { scale, .. } => {
                write!(out, "scale {scale} is outside the Arrow i8 storage range")
            }
        }
    }
}

impl std::error::Error for TypeTableError {}

#[derive(Debug, Clone)]
pub struct SparkField {
    pub name: String,
    pub data_type: SparkDataType,
    pub nullable: bool,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SparkDataType {
    Null,
    SparkString {
        collation: Cow<'static, str>,
    },
    Char {
        length: i64,
    },
    Varchar {
        length: i64,
    },
    Binary,
    Boolean,
    Date,
    Timestamp,
    TimestampNtz,
    Time {
        precision: i64,
    },
    Decimal {
        precision: i64,
        scale: i64,
    },
    Double,
    Float,
    Byte,
    Integer,
    Long,
    Short,
    CalendarInterval,
    DayTimeInterval {
        start: String,
        end: String,
    },
    YearMonthInterval {
        start: String,
        end: String,
    },
    Variant,
    Geometry {
        srid: i64,
    },
    Geography {
        srid: i64,
    },
    Array {
        element: Box<SparkDataType>,
        contains_null: bool,
    },
    Map {
        key: Box<SparkDataType>,
        value: Box<SparkDataType>,
        value_contains_null: bool,
    },
    Struct(Vec<SparkField>),
    Field(Box<SparkField>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowNameSurface {
    Describe,
    LogicalKey,
}

#[must_use]
pub fn arrow_name_at_depth(
    data_type: &ArrowDataType,
    surface: ArrowNameSurface,
    depth: usize,
) -> String {
    if depth >= SPARK_TYPE_NAME_MAX_DEPTH {
        return SPARK_TYPE_NAME_DEPTH_FALLBACK.to_string();
    }
    let describe = ArrowNameSurface::Describe;
    match data_type {
        ArrowDataType::Int8 => {
            if surface == describe {
                "tinyint".to_string()
            } else {
                "byte".to_string()
            }
        }
        ArrowDataType::Int16 => {
            if surface == describe {
                "smallint".to_string()
            } else {
                "short".to_string()
            }
        }
        ArrowDataType::Int32
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32 => "int".to_string(),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => {
            if surface == describe {
                "bigint".to_string()
            } else {
                "long".to_string()
            }
        }
        ArrowDataType::Float16 | ArrowDataType::Float32 => "float".to_string(),
        ArrowDataType::Float64 => "double".to_string(),
        ArrowDataType::Boolean => "boolean".to_string(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            "string".to_string()
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
            "binary".to_string()
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => "date".to_string(),
        ArrowDataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        ArrowDataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            let element = arrow_name_at_depth(field.data_type(), describe, depth + 1);
            format!("array<{element}>")
        }
        ArrowDataType::Map(entries, _) => {
            if let ArrowDataType::Struct(fields) = entries.data_type()
                && fields.len() >= 2
            {
                let key = arrow_name_at_depth(fields[0].data_type(), describe, depth + 1);
                let value = arrow_name_at_depth(fields[1].data_type(), describe, depth + 1);
                return format!("map<{key},{value}>");
            }
            format!("{data_type:?}")
        }
        ArrowDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| {
                    let child = arrow_name_at_depth(field.data_type(), describe, depth + 1);
                    format!("{}:{child}", field.name())
                })
                .collect();
            format!("struct<{}>", parts.join(","))
        }
        ArrowDataType::Null => "void".to_string(),
        other => format!("{other:?}"),
    }
}

#[must_use]
pub fn logical_type_key(data_type: &ArrowDataType) -> String {
    arrow_name_at_depth(data_type, ArrowNameSurface::LogicalKey, 0)
}

#[must_use]
pub fn spark_type_from_arrow(data_type: &ArrowDataType) -> SparkDataType {
    match data_type {
        ArrowDataType::Int8 => SparkDataType::Byte,
        ArrowDataType::Int16 => SparkDataType::Short,
        ArrowDataType::Int32 => SparkDataType::Integer,
        ArrowDataType::Int64 => SparkDataType::Long,
        ArrowDataType::Float16 | ArrowDataType::Float64 => SparkDataType::Double,
        ArrowDataType::Float32 => SparkDataType::Float,
        ArrowDataType::Boolean => SparkDataType::Boolean,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            SparkDataType::SparkString {
                collation: Cow::Borrowed(DEFAULT_COLLATION),
            }
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => SparkDataType::Binary,
        ArrowDataType::Date32 | ArrowDataType::Date64 => SparkDataType::Date,
        ArrowDataType::Timestamp(_, None) => SparkDataType::TimestampNtz,
        ArrowDataType::Timestamp(_, Some(_)) => SparkDataType::Timestamp,
        ArrowDataType::Decimal32(precision, scale)
        | ArrowDataType::Decimal64(precision, scale)
        | ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => SparkDataType::Decimal {
            precision: i64::from(*precision),
            scale: i64::from(*scale),
        },
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => SparkDataType::Array {
            element: Box::new(spark_type_from_arrow(field.data_type())),
            contains_null: true,
        },
        ArrowDataType::Map(entries, _) => {
            if let ArrowDataType::Struct(fields) = entries.data_type()
                && fields.len() >= 2
            {
                return SparkDataType::Map {
                    key: Box::new(spark_type_from_arrow(fields[0].data_type())),
                    value: Box::new(spark_type_from_arrow(fields[1].data_type())),
                    value_contains_null: true,
                };
            }
            SparkDataType::SparkString {
                collation: Cow::Borrowed(DEFAULT_COLLATION),
            }
        }
        ArrowDataType::Struct(fields) => SparkDataType::Struct(
            fields
                .iter()
                .map(|field| SparkField {
                    name: field.name().clone(),
                    data_type: spark_type_from_arrow(field.data_type()),
                    nullable: field.is_nullable(),
                    metadata: None,
                })
                .collect(),
        ),
        ArrowDataType::Null => SparkDataType::Null,
        _ => SparkDataType::SparkString {
            collation: Cow::Borrowed(DEFAULT_COLLATION),
        },
    }
}

#[must_use]
pub fn spark_struct_from_arrow(schema: &Schema) -> SparkDataType {
    SparkDataType::Struct(
        schema
            .fields()
            .iter()
            .map(|field| SparkField {
                name: field.name().clone(),
                data_type: spark_type_from_arrow(field.data_type()),
                nullable: field.is_nullable(),
                metadata: None,
            })
            .collect(),
    )
}

#[allow(clippy::missing_errors_doc)]
pub fn arrow_type_from_spark(data_type: &SparkDataType) -> Result<ArrowDataType, TypeTableError> {
    Ok(match data_type {
        SparkDataType::Null => ArrowDataType::Null,
        SparkDataType::Boolean => ArrowDataType::Boolean,
        SparkDataType::Byte => ArrowDataType::Int8,
        SparkDataType::Short => ArrowDataType::Int16,
        SparkDataType::Integer => ArrowDataType::Int32,
        SparkDataType::Long => ArrowDataType::Int64,
        SparkDataType::Float => ArrowDataType::Float32,
        SparkDataType::Double => ArrowDataType::Float64,
        SparkDataType::Binary => ArrowDataType::Binary,
        SparkDataType::Date => ArrowDataType::Date32,
        SparkDataType::Timestamp => {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }
        SparkDataType::TimestampNtz => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        SparkDataType::Decimal { precision, scale } => {
            if !(1..=38).contains(precision) {
                return Err(TypeTableError::DecimalPrecision {
                    precision: *precision,
                });
            }
            if !(i64::from(i8::MIN)..=i64::from(i8::MAX)).contains(scale) {
                return Err(TypeTableError::DecimalScale {
                    precision: *precision,
                    scale: *scale,
                });
            }
            ArrowDataType::Decimal128(
                u8::try_from(*precision).map_err(|_| TypeTableError::DecimalPrecision {
                    precision: *precision,
                })?,
                i8::try_from(*scale).map_err(|_| TypeTableError::DecimalScale {
                    precision: *precision,
                    scale: *scale,
                })?,
            )
        }
        SparkDataType::Array { element, .. } => ArrowDataType::List(Arc::new(Field::new(
            "item",
            arrow_type_from_spark(element)?,
            true,
        ))),
        SparkDataType::Map { key, value, .. } => ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", arrow_type_from_spark(key)?, false),
                    Field::new("value", arrow_type_from_spark(value)?, true),
                ])),
                false,
            )),
            false,
        ),
        SparkDataType::Struct(fields) => {
            let mut arrow_fields: Vec<Field> = Vec::with_capacity(fields.len());
            for field in fields {
                arrow_fields.push(Field::new(
                    field.name.as_str(),
                    arrow_type_from_spark(&field.data_type)?,
                    true,
                ));
            }
            ArrowDataType::Struct(Fields::from(arrow_fields))
        }
        SparkDataType::Geometry { .. } | SparkDataType::Geography { .. } => {
            return Err(TypeTableError::Message(format!(
                "repark does not support the {} column type: spatial values have no Arrow representation (V3-GEO-1)",
                simple_string(data_type)
            )));
        }
        _ => ArrowDataType::Utf8,
    })
}

#[must_use]
pub fn simple_string(data_type: &SparkDataType) -> String {
    match data_type {
        SparkDataType::Null => "void".to_string(),
        SparkDataType::SparkString { collation } => {
            if collation.as_ref() == DEFAULT_COLLATION {
                "string".to_string()
            } else {
                format!("string collate {collation}")
            }
        }
        SparkDataType::Char { length } => format!("char({length})"),
        SparkDataType::Varchar { length } => format!("varchar({length})"),
        SparkDataType::Binary => "binary".to_string(),
        SparkDataType::Boolean => "boolean".to_string(),
        SparkDataType::Date => "date".to_string(),
        SparkDataType::Timestamp => "timestamp".to_string(),
        SparkDataType::TimestampNtz => "timestamp_ntz".to_string(),
        SparkDataType::Time { precision } => format!("time({precision})"),
        SparkDataType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        SparkDataType::Double => "double".to_string(),
        SparkDataType::Float => "float".to_string(),
        SparkDataType::Byte => "tinyint".to_string(),
        SparkDataType::Integer => "int".to_string(),
        SparkDataType::Long => "bigint".to_string(),
        SparkDataType::Short => "smallint".to_string(),
        SparkDataType::CalendarInterval => "interval".to_string(),
        SparkDataType::DayTimeInterval { start, end }
        | SparkDataType::YearMonthInterval { start, end } => interval_simple_string(start, end),
        SparkDataType::Variant => "variant".to_string(),
        SparkDataType::Geometry { srid } => spatial_simple_string("geometry", *srid),
        SparkDataType::Geography { srid } => spatial_simple_string("geography", *srid),
        SparkDataType::Array { element, .. } => format!("array<{}>", simple_string(element)),
        SparkDataType::Map { key, value, .. } => {
            format!("map<{},{}>", simple_string(key), simple_string(value))
        }
        SparkDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| format!("{}:{}", field.name, simple_string(&field.data_type)))
                .collect();
            format!("struct<{}>", parts.join(","))
        }
        SparkDataType::Field(field) => {
            format!("{}:{}", field.name, simple_string(&field.data_type))
        }
    }
}

#[must_use]
pub fn spatial_srid_supported(geography: bool, srid: i64) -> bool {
    if srid == SPATIAL_MIXED_SRID {
        return true;
    }
    if geography {
        GEOGRAPHY_SRIDS.contains(&srid)
    } else {
        GEOMETRY_SRIDS.contains(&srid)
    }
}

fn spatial_simple_string(prefix: &str, srid: i64) -> String {
    if srid == SPATIAL_MIXED_SRID {
        format!("{prefix}(any)")
    } else {
        format!("{prefix}({srid})")
    }
}

fn interval_simple_string(start: &str, end: &str) -> String {
    if start == end {
        format!("interval {start}")
    } else {
        format!("interval {start} to {end}")
    }
}

#[must_use]
pub fn engine_token(data_type: &SparkDataType) -> String {
    match data_type {
        SparkDataType::SparkString { .. }
        | SparkDataType::Char { .. }
        | SparkDataType::Varchar { .. } => "string".to_string(),
        SparkDataType::Byte => "byte".to_string(),
        SparkDataType::Short => "short".to_string(),
        SparkDataType::Long => "long".to_string(),
        SparkDataType::Array { element, .. } => format!("array<{}>", engine_token(element)),
        SparkDataType::Map { key, value, .. } => {
            format!("map<{},{}>", engine_token(key), engine_token(value))
        }
        SparkDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| format!("{}:{}", field.name, engine_token(&field.data_type)))
                .collect();
            format!("struct<{}>", parts.join(","))
        }
        SparkDataType::Field(field) => format!("{}:{}", field.name, engine_token(&field.data_type)),
        other => simple_string(other),
    }
}

#[must_use]
pub fn ddl_token(data_type: &SparkDataType) -> String {
    match data_type {
        SparkDataType::SparkString { .. } => "STRING".to_string(),
        SparkDataType::Char { length } => format!("CHAR({length})"),
        SparkDataType::Varchar { length } => format!("VARCHAR({length})"),
        SparkDataType::Integer => "INT".to_string(),
        SparkDataType::Long => "BIGINT".to_string(),
        SparkDataType::Short => "SMALLINT".to_string(),
        SparkDataType::Byte => "TINYINT".to_string(),
        SparkDataType::Double => "DOUBLE".to_string(),
        SparkDataType::Float => "FLOAT".to_string(),
        SparkDataType::Boolean => "BOOLEAN".to_string(),
        SparkDataType::Binary => "BINARY".to_string(),
        SparkDataType::Date => "DATE".to_string(),
        SparkDataType::Timestamp => "TIMESTAMP".to_string(),
        SparkDataType::TimestampNtz => "TIMESTAMP_NTZ".to_string(),
        SparkDataType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        SparkDataType::Null => "VOID".to_string(),
        SparkDataType::Array { element, .. } => format!("ARRAY<{}>", ddl_token(element)),
        SparkDataType::Map { key, value, .. } => {
            format!("MAP<{},{}>", ddl_token(key), ddl_token(value))
        }
        SparkDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| format!("{}:{}", field.name, ddl_token(&field.data_type)))
                .collect();
            format!("STRUCT<{}>", parts.join(","))
        }
        other => simple_string(other).to_uppercase(),
    }
}

#[must_use]
pub fn sql_marker_token(data_type: &SparkDataType) -> String {
    match data_type {
        SparkDataType::Integer => "INT".to_string(),
        SparkDataType::Long => "BIGINT".to_string(),
        SparkDataType::Short => "SMALLINT".to_string(),
        SparkDataType::Byte => "TINYINT".to_string(),
        SparkDataType::Double => "DOUBLE".to_string(),
        SparkDataType::Float => "FLOAT".to_string(),
        SparkDataType::Boolean => "BOOLEAN".to_string(),
        SparkDataType::SparkString { .. }
        | SparkDataType::Char { .. }
        | SparkDataType::Varchar { .. } => "STRING".to_string(),
        SparkDataType::Binary => "BINARY".to_string(),
        SparkDataType::Date => "DATE".to_string(),
        SparkDataType::Timestamp => "TIMESTAMP".to_string(),
        SparkDataType::TimestampNtz => "TIMESTAMP_NTZ".to_string(),
        SparkDataType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        SparkDataType::Null => "VOID".to_string(),
        SparkDataType::Array { element, .. } => {
            format!("ARRAY<{}>", sql_marker_token(element))
        }
        SparkDataType::Map { key, value, .. } => {
            format!("MAP<{},{}>", sql_marker_token(key), sql_marker_token(value))
        }
        SparkDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| format!("{}:{}", field.name, sql_marker_token(&field.data_type)))
                .collect();
            format!("STRUCT<{}>", parts.join(","))
        }
        other => engine_token(other),
    }
}

#[must_use]
pub fn struct_field_ddl(data_type: &SparkDataType) -> String {
    let SparkDataType::Struct(fields) = data_type else {
        return ddl_token(data_type);
    };
    fields
        .iter()
        .map(|field| {
            let suffix = if field.nullable { "" } else { " NOT NULL" };
            format!("{} {}{}", field.name, ddl_token(&field.data_type), suffix)
        })
        .collect::<Vec<String>>()
        .join(",")
}

#[must_use]
pub fn csv_sql_cast_token(engine_type: &str) -> String {
    match engine_type {
        "long" => "bigint".to_string(),
        "int" | "boolean" | "double" | "date" | "timestamp" => engine_type.to_string(),
        engine if engine.starts_with("decimal") => engine.to_string(),
        _ => "varchar".to_string(),
    }
}

#[must_use]
pub fn csv_rung_type(
    rung: &str,
    precision: Option<i64>,
    scale: Option<i64>,
    timestamp_ntz: bool,
) -> SparkDataType {
    match rung {
        "bool" => SparkDataType::Boolean,
        "int32" => SparkDataType::Integer,
        "int64" => SparkDataType::Long,
        "decimal128" => SparkDataType::Decimal {
            precision: match precision {
                Some(0) | None => 10,
                Some(value) => value,
            },
            scale: scale.unwrap_or(0),
        },
        "float64" => SparkDataType::Double,
        "date" => SparkDataType::Date,
        "timestamp" => default_timestamp_type(timestamp_ntz),
        _ => SparkDataType::SparkString {
            collation: Cow::Borrowed(DEFAULT_COLLATION),
        },
    }
}

#[must_use]
pub fn default_timestamp_type(timestamp_ntz: bool) -> SparkDataType {
    if timestamp_ntz {
        SparkDataType::TimestampNtz
    } else {
        SparkDataType::Timestamp
    }
}
