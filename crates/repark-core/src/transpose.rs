use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, new_empty_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::prelude::{Column, DataFrame, Expr, SessionContext};

use crate::{Error, engine_err};

pub const TRANSPOSE_OUTPUT_PREFIX: &str = "__repark_transpose_";

pub enum TransposeError {
    Spark(SparkTransposeError),
    Engine(Error),
}

impl From<Error> for TransposeError {
    fn from(err: Error) -> Self {
        Self::Engine(err)
    }
}

impl From<DataFusionError> for TransposeError {
    fn from(err: DataFusionError) -> Self {
        Self::Engine(engine_err(err))
    }
}

impl From<arrow::error::ArrowError> for TransposeError {
    fn from(err: arrow::error::ArrowError) -> Self {
        Self::Engine(engine_err(err.into()))
    }
}

pub struct SparkTransposeError {
    pub message: String,
    pub error_class: &'static str,
    pub message_parameters: Vec<(String, String)>,
    pub sql_state: &'static str,
}

pub struct TransposeOutcome {
    pub frame: DataFrame,
    pub display_names: Vec<String>,
}

fn invalid_index(reason: String) -> TransposeError {
    TransposeError::Spark(SparkTransposeError {
        message: format!(
            "[TRANSPOSE_INVALID_INDEX_COLUMN] Invalid index column for TRANSPOSE because: \
             {reason} SQLSTATE: 42804"
        ),
        error_class: "TRANSPOSE_INVALID_INDEX_COLUMN",
        message_parameters: vec![("reason".to_string(), reason)],
        sql_state: "42804",
    })
}

fn no_common_type(left: &CatalystType, right: &CatalystType) -> TransposeError {
    let dt1 = format!("\"{}\"", left.sql_name());
    let dt2 = format!("\"{}\"", right.sql_name());
    TransposeError::Spark(SparkTransposeError {
        message: format!(
            "[TRANSPOSE_NO_LEAST_COMMON_TYPE] Transpose requires non-index columns to share a \
             least common type, but {dt1} and {dt2} do not. SQLSTATE: 42K09"
        ),
        error_class: "TRANSPOSE_NO_LEAST_COMMON_TYPE",
        message_parameters: vec![("dt1".to_string(), dt1), ("dt2".to_string(), dt2)],
        sql_state: "42K09",
    })
}

fn exceed_row_limit(max_values: usize) -> TransposeError {
    TransposeError::Spark(SparkTransposeError {
        message: format!(
            "[TRANSPOSE_EXCEED_ROW_LIMIT] Number of rows exceeds the allowed limit of \
             {max_values} for TRANSPOSE. If this was intended, set \
             spark.sql.transposeMaxValues to at least the current row count. SQLSTATE: 54006"
        ),
        error_class: "TRANSPOSE_EXCEED_ROW_LIMIT",
        message_parameters: vec![
            (
                "config".to_string(),
                "spark.sql.transposeMaxValues".to_string(),
            ),
            ("maxValues".to_string(), max_values.to_string()),
        ],
        sql_state: "54006",
    })
}

#[expect(
    clippy::missing_errors_doc,
    reason = "TransposeError is a Spark AnalysisException or engine error; see this module's map.md row"
)]
pub async fn transpose_frame(
    frame: DataFrame,
    index_column: &str,
    key_names: Vec<String>,
    max_values: usize,
) -> std::result::Result<TransposeOutcome, TransposeError> {
    let fields: Vec<FieldRef> = frame.schema().fields().iter().cloned().collect();
    let index_position = fields
        .iter()
        .position(|field| field.name() == index_column)
        .ok_or_else(|| {
            TransposeError::Engine(Error::Analysis(format!(
                "transpose index column `{index_column}` missing from analyzed schema"
            )))
        })?;
    let index_type = CatalystType::from_arrow(fields[index_position].data_type());
    if !index_type.is_atomic() {
        return Err(invalid_index(format!(
            "Index column must be of atomic type, but found: {}",
            index_type.scala_name()
        )));
    }
    let value_types: Vec<CatalystType> = fields
        .iter()
        .enumerate()
        .filter(|(position, _)| *position != index_position)
        .map(|(_, field)| CatalystType::from_arrow(field.data_type()))
        .collect();
    let mut common: Option<CatalystType> = None;
    for value_type in &value_types {
        common = Some(match common {
            None => value_type.clone(),
            Some(acc) => match tightest_common(&acc, value_type) {
                Some(merged) => merged,
                None => return Err(no_common_type(&acc, value_type)),
            },
        });
    }
    let common_type = common.unwrap_or(CatalystType::Str);
    let collected = frame
        .clone()
        .filter(Expr::Column(Column::new_unqualified(index_column.to_string())).is_not_null())
        .map_err(engine_err)?
        .limit(0, Some(max_values + 1))
        .map_err(engine_err)?
        .collect()
        .await
        .map_err(engine_err)?;
    let total_rows: usize = collected.iter().map(RecordBatch::num_rows).sum();
    if total_rows > max_values {
        return Err(exceed_row_limit(max_values));
    }
    let (state, _) = frame.into_parts();
    if total_rows == 0 {
        return Ok(TransposeOutcome {
            frame: empty_transpose_frame(state, &key_names, &common_type)?,
            display_names: vec!["key".to_string()],
        });
    }
    build_output(
        state,
        &collected,
        &fields,
        index_position,
        &key_names,
        &common_type,
    )
}

fn build_output(
    state: datafusion::execution::session_state::SessionState,
    collected: &[RecordBatch],
    fields: &[FieldRef],
    index_position: usize,
    key_names: &[String],
    common_type: &CatalystType,
) -> std::result::Result<TransposeOutcome, TransposeError> {
    let index_scalars = collect_column_scalars(collected, index_position)?;
    let mut order: Vec<usize> = (0..index_scalars.len()).collect();
    let mut sort_failure = Ok(());
    order.sort_by(|left, right| {
        index_scalars[*left]
            .try_cmp(&index_scalars[*right])
            .unwrap_or_else(|err| {
                sort_failure = Err(err);
                Ordering::Equal
            })
    });
    sort_failure?;
    let index_strings = cast(&concat_column(collected, index_position)?, &DataType::Utf8)?;
    let index_names = index_strings
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            TransposeError::Engine(Error::Analysis(
                "transpose index cast to string did not produce Utf8".to_string(),
            ))
        })?;
    let common_arrow = common_type.to_arrow();
    let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(fields.len().saturating_sub(1));
    for position in 0..fields.len() {
        if position == index_position {
            continue;
        }
        let merged = concat_column(collected, position)?;
        let casted = cast(&merged, &common_arrow)?;
        value_columns.push(casted);
    }
    let output_rows = value_columns.len();
    let mut out_fields: Vec<FieldRef> = Vec::with_capacity(order.len() + 1);
    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(order.len() + 1);
    let mut display_names = Vec::with_capacity(order.len() + 1);
    out_fields.push(Arc::new(Field::new(
        format!("{TRANSPOSE_OUTPUT_PREFIX}0"),
        DataType::Utf8,
        false,
    )));
    out_columns.push(Arc::new(StringArray::from_iter_values(
        key_names.iter().map(String::as_str),
    )));
    display_names.push("key".to_string());
    for (index, row) in order.iter().enumerate() {
        out_fields.push(Arc::new(Field::new(
            format!("{TRANSPOSE_OUTPUT_PREFIX}{}", index + 1),
            common_arrow.clone(),
            true,
        )));
        display_names.push(index_names.value(*row).to_string());
        if output_rows == 0 {
            out_columns.push(new_empty_array(&common_arrow));
            continue;
        }
        let cells: Vec<ScalarValue> = value_columns
            .iter()
            .map(|column| ScalarValue::try_from_array(column.as_ref(), *row))
            .collect::<DataFusionResult<Vec<_>>>()?;
        out_columns.push(ScalarValue::iter_to_array(cells)?);
    }
    let schema: SchemaRef = Arc::new(Schema::new(out_fields));
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        out_columns,
        &RecordBatchOptions::new().with_row_count(Some(output_rows)),
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let context = SessionContext::new_with_state(state);
    let out = context.read_table(Arc::new(table))?;
    Ok(TransposeOutcome {
        frame: out,
        display_names,
    })
}

fn empty_transpose_frame(
    state: datafusion::execution::session_state::SessionState,
    key_names: &[String],
    _common_type: &CatalystType,
) -> std::result::Result<DataFrame, TransposeError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        format!("{TRANSPOSE_OUTPUT_PREFIX}0"),
        DataType::Utf8,
        false,
    )]));
    let key_column: ArrayRef = Arc::new(StringArray::from_iter_values(
        key_names.iter().map(String::as_str),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![key_column])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let context = SessionContext::new_with_state(state);
    Ok(context.read_table(Arc::new(table))?)
}

fn collect_column_scalars(
    batches: &[RecordBatch],
    position: usize,
) -> std::result::Result<Vec<ScalarValue>, TransposeError> {
    let merged = concat_column(batches, position)?;
    Ok((0..merged.len())
        .map(|row| ScalarValue::try_from_array(merged.as_ref(), row))
        .collect::<DataFusionResult<Vec<_>>>()?)
}

fn concat_column(batches: &[RecordBatch], position: usize) -> DataFusionResult<ArrayRef> {
    let columns: Vec<ArrayRef> = batches
        .iter()
        .map(|batch| batch.column(position).clone())
        .collect();
    let refs: Vec<&dyn arrow::array::Array> = columns.iter().map(AsRef::as_ref).collect();
    arrow::compute::concat(&refs).map_err(DataFusionError::from)
}

#[derive(Debug, Clone, PartialEq)]
enum CatalystType {
    Null,
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    Float,
    Double,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Str,
    Binary,
    Date,
    Timestamp,
    TimestampNtz,
    Time(u8),
    DayTimeInterval,
    YearMonthInterval,
    CalendarInterval,
    Array {
        element: Box<CatalystType>,
        contains_null: bool,
    },
    Map {
        key: Box<CatalystType>,
        value: Box<CatalystType>,
        value_contains_null: bool,
    },
    Struct(Vec<CatalystField>),
    Other(DataType),
}

#[derive(Debug, Clone, PartialEq)]
struct CatalystField {
    name: String,
    data_type: CatalystType,
    nullable: bool,
}

impl CatalystType {
    fn from_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 | DataType::UInt8 => Self::Byte,
            DataType::Int16 | DataType::UInt16 => Self::Short,
            DataType::Int32 | DataType::UInt32 => Self::Int,
            DataType::Int64 | DataType::UInt64 => Self::Long,
            DataType::Float16 | DataType::Float32 => Self::Float,
            DataType::Float64 => Self::Double,
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                Self::Decimal {
                    precision: *precision,
                    scale: *scale,
                }
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Self::Str,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Self::Binary,
            DataType::Date32 | DataType::Date64 => Self::Date,
            DataType::Timestamp(_, Some(_)) => Self::Timestamp,
            DataType::Timestamp(_, None) => Self::TimestampNtz,
            DataType::Time32(unit) | DataType::Time64(unit) => Self::Time(match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            }),
            DataType::Interval(IntervalUnit::DayTime) => Self::DayTimeInterval,
            DataType::Interval(IntervalUnit::YearMonth) => Self::YearMonthInterval,
            DataType::Interval(IntervalUnit::MonthDayNano) => Self::CalendarInterval,
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => Self::Array {
                element: Box::new(Self::from_arrow(field.data_type())),
                contains_null: field.is_nullable(),
            },
            DataType::Map(field, _) => Self::map_from_entries(field),
            DataType::Struct(fields) => Self::Struct(
                fields
                    .iter()
                    .map(|field| CatalystField {
                        name: field.name().clone(),
                        data_type: Self::from_arrow(field.data_type()),
                        nullable: field.is_nullable(),
                    })
                    .collect(),
            ),
            other => Self::Other(other.clone()),
        }
    }

    fn map_from_entries(field: &FieldRef) -> Self {
        if let DataType::Struct(entries) = field.data_type()
            && entries.len() == 2
        {
            return Self::Map {
                key: Box::new(Self::from_arrow(entries[0].data_type())),
                value: Box::new(Self::from_arrow(entries[1].data_type())),
                value_contains_null: entries[1].is_nullable(),
            };
        }
        Self::Other(field.data_type().clone())
    }

    fn is_atomic(&self) -> bool {
        !matches!(
            self,
            Self::Null | Self::Array { .. } | Self::Map { .. } | Self::Struct(_)
        ) && !matches!(self, Self::Other(_))
    }

    fn scala_name(&self) -> String {
        match self {
            Self::Null => "NullType".to_string(),
            Self::Boolean => "BooleanType".to_string(),
            Self::Byte => "ByteType".to_string(),
            Self::Short => "ShortType".to_string(),
            Self::Int => "IntegerType".to_string(),
            Self::Long => "LongType".to_string(),
            Self::Float => "FloatType".to_string(),
            Self::Double => "DoubleType".to_string(),
            Self::Decimal { precision, scale } => format!("DecimalType({precision},{scale})"),
            Self::Str => "StringType".to_string(),
            Self::Binary => "BinaryType".to_string(),
            Self::Date => "DateType".to_string(),
            Self::Timestamp => "TimestampType".to_string(),
            Self::TimestampNtz => "TimestampNTZType".to_string(),
            Self::Time(precision) => format!("TimeType({precision})"),
            Self::DayTimeInterval => "DayTimeIntervalType(0,3)".to_string(),
            Self::YearMonthInterval => "YearMonthIntervalType(0,2)".to_string(),
            Self::CalendarInterval => "CalendarIntervalType".to_string(),
            Self::Array {
                element,
                contains_null,
            } => format!("ArrayType({},{})", element.scala_name(), contains_null),
            Self::Map {
                key,
                value,
                value_contains_null,
            } => format!(
                "MapType({},{},{})",
                key.scala_name(),
                value.scala_name(),
                value_contains_null
            ),
            Self::Struct(fields) => {
                let inner = fields
                    .iter()
                    .map(|field| {
                        format!(
                            "StructField({},{},{})",
                            field.name,
                            field.data_type.scala_name(),
                            field.nullable
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!("StructType({inner})")
            }
            Self::Other(data_type) => format!("{data_type}"),
        }
    }

    fn sql_name(&self) -> String {
        match self {
            Self::Null => "VOID".to_string(),
            Self::Boolean => "BOOLEAN".to_string(),
            Self::Byte => "TINYINT".to_string(),
            Self::Short => "SMALLINT".to_string(),
            Self::Int => "INT".to_string(),
            Self::Long => "BIGINT".to_string(),
            Self::Float => "FLOAT".to_string(),
            Self::Double => "DOUBLE".to_string(),
            Self::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            Self::Str => "STRING".to_string(),
            Self::Binary => "BINARY".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Timestamp => "TIMESTAMP".to_string(),
            Self::TimestampNtz => "TIMESTAMP_NTZ".to_string(),
            Self::Time(precision) => format!("TIME({precision})"),
            Self::DayTimeInterval => "INTERVAL DAY TO SECOND".to_string(),
            Self::YearMonthInterval => "INTERVAL YEAR TO MONTH".to_string(),
            Self::CalendarInterval => "INTERVAL".to_string(),
            Self::Array { element, .. } => format!("ARRAY<{}>", element.sql_name()),
            Self::Map { key, value, .. } => {
                format!("MAP<{}, {}>", key.sql_name(), value.sql_name())
            }
            Self::Struct(fields) => {
                let inner = fields
                    .iter()
                    .map(|field| format!("{}: {}", field.name, field.data_type.sql_name()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("STRUCT<{inner}>")
            }
            Self::Other(data_type) => format!("{data_type}"),
        }
    }

    fn to_arrow(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean => DataType::Boolean,
            Self::Byte => DataType::Int8,
            Self::Short => DataType::Int16,
            Self::Int => DataType::Int32,
            Self::Long => DataType::Int64,
            Self::Float => DataType::Float32,
            Self::Double => DataType::Float64,
            Self::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Str => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            Self::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Time(precision) => match precision {
                0 => DataType::Time32(TimeUnit::Second),
                3 => DataType::Time32(TimeUnit::Millisecond),
                6 => DataType::Time64(TimeUnit::Microsecond),
                _ => DataType::Time64(TimeUnit::Nanosecond),
            },
            Self::DayTimeInterval => DataType::Interval(IntervalUnit::DayTime),
            Self::YearMonthInterval => DataType::Interval(IntervalUnit::YearMonth),
            Self::CalendarInterval => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::Array {
                element,
                contains_null,
            } => DataType::List(Arc::new(Field::new(
                "item",
                element.to_arrow(),
                *contains_null,
            ))),
            Self::Map {
                key,
                value,
                value_contains_null,
            } => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key.to_arrow(), false)),
                            Arc::new(Field::new("value", value.to_arrow(), *value_contains_null)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            Self::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|field| {
                        Arc::new(Field::new(
                            field.name.clone(),
                            field.data_type.to_arrow(),
                            field.nullable,
                        )) as FieldRef
                    })
                    .collect(),
            ),
            Self::Other(data_type) => data_type.clone(),
        }
    }
}

fn decimal_for_integral(integral: &CatalystType) -> CatalystType {
    let precision = match integral {
        CatalystType::Byte => 3,
        CatalystType::Short => 5,
        CatalystType::Int => 10,
        CatalystType::Long => 20,
        CatalystType::Float => 14,
        CatalystType::Double => 30,
        _ => 0,
    };
    CatalystType::Decimal {
        precision,
        scale: 0,
    }
}

fn decimal_wider_than(decimal: &CatalystType, other: &CatalystType) -> bool {
    let CatalystType::Decimal { precision, scale } = decimal else {
        return false;
    };
    let precision = i32::from(*precision);
    let scale = i32::from(*scale);
    match other {
        CatalystType::Decimal {
            precision: other_precision,
            scale: other_scale,
        } => {
            precision - scale >= i32::from(*other_precision) - i32::from(*other_scale)
                && scale >= i32::from(*other_scale)
        }
        CatalystType::Byte | CatalystType::Short | CatalystType::Int | CatalystType::Long => {
            decimal_wider_than(decimal, &decimal_for_integral(other))
        }
        _ => false,
    }
}

fn force_nullable(from: &CatalystType, to: &CatalystType) -> bool {
    match (from, to) {
        (CatalystType::Null, _)
        | (CatalystType::Str, CatalystType::Binary | CatalystType::Str)
        | (CatalystType::Timestamp, CatalystType::Date)
        | (CatalystType::Date, CatalystType::Timestamp)
        | (_, CatalystType::Str) => false,
        (left, right) if left == right => false,
        (CatalystType::Str | CatalystType::Date, _)
        | (CatalystType::Timestamp, CatalystType::Byte | CatalystType::Short | CatalystType::Int)
        | (CatalystType::Time(_), CatalystType::Byte | CatalystType::Short)
        | (
            CatalystType::Float | CatalystType::Double,
            CatalystType::Timestamp
            | CatalystType::Byte
            | CatalystType::Short
            | CatalystType::Int
            | CatalystType::Long,
        )
        | (_, CatalystType::Date | CatalystType::CalendarInterval) => true,
        (
            _,
            CatalystType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
        ) => !can_null_safe_cast_to_decimal(from, *to_precision, *to_scale),
        _ => false,
    }
}

fn can_null_safe_cast_to_decimal(from: &CatalystType, to_precision: u8, to_scale: i8) -> bool {
    match from {
        CatalystType::Boolean => decimal_wider_than(
            &CatalystType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
            &CatalystType::Decimal {
                precision: 1,
                scale: 0,
            },
        ),
        CatalystType::Byte
        | CatalystType::Short
        | CatalystType::Int
        | CatalystType::Long
        | CatalystType::Float
        | CatalystType::Double => decimal_wider_than(
            &CatalystType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
            from,
        ),
        CatalystType::Decimal {
            precision: from_precision,
            scale: from_scale,
        } => {
            i32::from(to_precision) - i32::from(to_scale)
                > i32::from(*from_precision) - i32::from(*from_scale)
        }
        _ => false,
    }
}

fn find_wider_datetime(left: &CatalystType, right: &CatalystType) -> Option<CatalystType> {
    match (left, right) {
        (CatalystType::Timestamp, CatalystType::Date | CatalystType::TimestampNtz)
        | (CatalystType::Date | CatalystType::TimestampNtz, CatalystType::Timestamp) => {
            Some(CatalystType::Timestamp)
        }
        (CatalystType::TimestampNtz, CatalystType::Date)
        | (CatalystType::Date, CatalystType::TimestampNtz) => Some(CatalystType::TimestampNtz),
        _ => None,
    }
}

fn find_type_for_complex(left: &CatalystType, right: &CatalystType) -> Option<CatalystType> {
    match (left, right) {
        (
            CatalystType::Array {
                element: left_element,
                contains_null: left_contains,
            },
            CatalystType::Array {
                element: right_element,
                contains_null: right_contains,
            },
        ) => tightest_common(left_element, right_element).map(|element| CatalystType::Array {
            contains_null: *left_contains
                || *right_contains
                || force_nullable(left_element, &element)
                || force_nullable(right_element, &element),
            element: Box::new(element),
        }),
        (
            CatalystType::Map {
                key: left_key,
                value: left_value,
                value_contains_null: left_value_null,
            },
            CatalystType::Map {
                key: right_key,
                value: right_value,
                value_contains_null: right_value_null,
            },
        ) => tightest_common(left_key, right_key)
            .filter(|key| !force_nullable(left_key, key) && !force_nullable(right_key, key))
            .and_then(|key| {
                tightest_common(left_value, right_value).map(|value| CatalystType::Map {
                    value_contains_null: *left_value_null
                        || *right_value_null
                        || force_nullable(left_value, &value)
                        || force_nullable(right_value, &value),
                    key: Box::new(key),
                    value: Box::new(value),
                })
            }),
        (CatalystType::Struct(left_fields), CatalystType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            left_fields
                .iter()
                .zip(right_fields.iter())
                .try_fold(Vec::new(), |mut merged, (left_field, right_field)| {
                    if !left_field.name.eq_ignore_ascii_case(&right_field.name) {
                        return None;
                    }
                    tightest_common(&left_field.data_type, &right_field.data_type).map(
                        |data_type| {
                            merged.push(CatalystField {
                                name: left_field.name.clone(),
                                nullable: left_field.nullable
                                    || right_field.nullable
                                    || force_nullable(&left_field.data_type, &data_type)
                                    || force_nullable(&right_field.data_type, &data_type),
                                data_type,
                            });
                            merged
                        },
                    )
                })
                .map(CatalystType::Struct)
        }
        _ => None,
    }
}

fn tightest_common(left: &CatalystType, right: &CatalystType) -> Option<CatalystType> {
    match (left, right) {
        (t1, t2) if t1 == t2 => Some(t1.clone()),
        (CatalystType::Null, t) | (t, CatalystType::Null) => Some(t.clone()),
        (CatalystType::Str, CatalystType::Str) => Some(CatalystType::Str),
        (integral, decimal @ CatalystType::Decimal { .. })
            if matches!(
                integral,
                CatalystType::Byte | CatalystType::Short | CatalystType::Int | CatalystType::Long
            ) && decimal_wider_than(decimal, integral) =>
        {
            Some(decimal.clone())
        }
        (decimal @ CatalystType::Decimal { .. }, integral)
            if matches!(
                integral,
                CatalystType::Byte | CatalystType::Short | CatalystType::Int | CatalystType::Long
            ) && decimal_wider_than(decimal, integral) =>
        {
            Some(decimal.clone())
        }
        (left_numeric, right_numeric)
            if matches!(
                left_numeric,
                CatalystType::Byte
                    | CatalystType::Short
                    | CatalystType::Int
                    | CatalystType::Long
                    | CatalystType::Float
                    | CatalystType::Double
            ) && matches!(
                right_numeric,
                CatalystType::Byte
                    | CatalystType::Short
                    | CatalystType::Int
                    | CatalystType::Long
                    | CatalystType::Float
                    | CatalystType::Double
            ) =>
        {
            let precedence = [
                CatalystType::Byte,
                CatalystType::Short,
                CatalystType::Int,
                CatalystType::Long,
                CatalystType::Float,
                CatalystType::Double,
            ];
            let wider = precedence
                .iter()
                .rev()
                .find(|kind| *kind == left_numeric || *kind == right_numeric)
                .cloned()?;
            if wider == CatalystType::Float {
                Some(CatalystType::Double)
            } else {
                Some(wider)
            }
        }
        (left_dt, right_dt)
            if matches!(
                left_dt,
                CatalystType::Date
                    | CatalystType::Timestamp
                    | CatalystType::TimestampNtz
                    | CatalystType::Time(_)
            ) && matches!(
                right_dt,
                CatalystType::Date
                    | CatalystType::Timestamp
                    | CatalystType::TimestampNtz
                    | CatalystType::Time(_)
            ) =>
        {
            find_wider_datetime(left_dt, right_dt)
        }
        (CatalystType::DayTimeInterval, CatalystType::DayTimeInterval) => {
            Some(CatalystType::DayTimeInterval)
        }
        (CatalystType::YearMonthInterval, CatalystType::YearMonthInterval) => {
            Some(CatalystType::YearMonthInterval)
        }
        _ => find_type_for_complex(left, right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_and_double_merge_to_double() {
        assert_eq!(
            tightest_common(&CatalystType::Int, &CatalystType::Double),
            Some(CatalystType::Double)
        );
    }

    #[test]
    fn int_and_float_merge_to_double() {
        assert_eq!(
            tightest_common(&CatalystType::Int, &CatalystType::Float),
            Some(CatalystType::Double)
        );
    }

    #[test]
    fn int_and_string_fail() {
        assert_eq!(
            tightest_common(&CatalystType::Int, &CatalystType::Str),
            None
        );
    }

    #[test]
    fn long_and_narrow_decimal_fail() {
        let decimal = CatalystType::Decimal {
            precision: 5,
            scale: 2,
        };
        assert_eq!(tightest_common(&CatalystType::Long, &decimal), None);
    }

    #[test]
    fn int_and_wide_decimal_merge() {
        let decimal = CatalystType::Decimal {
            precision: 25,
            scale: 4,
        };
        assert_eq!(
            tightest_common(&CatalystType::Long, &decimal),
            Some(decimal)
        );
    }

    #[test]
    fn date_and_timestamp_merge() {
        assert_eq!(
            tightest_common(&CatalystType::Date, &CatalystType::Timestamp),
            Some(CatalystType::Timestamp)
        );
    }

    #[test]
    fn date_and_timestamp_ntz_merge() {
        assert_eq!(
            tightest_common(&CatalystType::Date, &CatalystType::TimestampNtz),
            Some(CatalystType::TimestampNtz)
        );
    }

    #[test]
    fn null_merges_to_other() {
        assert_eq!(
            tightest_common(&CatalystType::Null, &CatalystType::Int),
            Some(CatalystType::Int)
        );
    }

    #[test]
    fn struct_fields_merge_case_insensitively() {
        let left = CatalystType::Struct(vec![CatalystField {
            name: "X".to_string(),
            data_type: CatalystType::Int,
            nullable: true,
        }]);
        let right = CatalystType::Struct(vec![CatalystField {
            name: "x".to_string(),
            data_type: CatalystType::Double,
            nullable: false,
        }]);
        assert!(matches!(
            tightest_common(&left, &right),
            Some(CatalystType::Struct(_))
        ));
    }

    #[test]
    fn scala_name_renders_array() {
        let arrow = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(
            CatalystType::from_arrow(&arrow).scala_name(),
            "ArrayType(IntegerType,true)"
        );
    }
}
