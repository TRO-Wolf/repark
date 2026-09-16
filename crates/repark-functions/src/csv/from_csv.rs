use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    NullBufferBuilder, StringArray, StringBuilder, StructArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::{
    CsvMode, CsvOptions, options_from_map, parse_csv_date, parse_csv_timestamp, read_options_array,
    split_csv_record, string_column, token_is_null, unsupported_datatype,
};
use crate::json::ddl::parse_schema;

pub(crate) const FROM_CSV_UDF: &str = "from_csv";

#[must_use]
pub fn from_csv_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFromCsv::new()))
}

fn wrong_num_args(got: usize) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `from_csv` requires [2, 3] parameters but the \
         actual number is {got}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn literal_text(scalar: Option<&ScalarValue>) -> Option<String> {
    match scalar {
        Some(ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value)) => value.clone(),
        Some(ScalarValue::Utf8View(Some(value))) => Some(value.clone()),
        _ => None,
    }
}

pub(crate) fn csv_struct_type(schema_text: &str) -> Result<(DataType, Vec<(String, DataType)>)> {
    let parsed_schema =
        parse_schema(schema_text).map_err(|report| DataFusionError::Plan(report.to_string()))?;
    let DataType::Struct(parsed) = parsed_schema else {
        return plan_err!(
            "[INVALID_SCHEMA.NON_STRING_LITERAL] The input schema is not a valid schema string."
        );
    };
    let mut fields: Vec<(String, DataType)> = Vec::with_capacity(parsed.len());
    for field in &parsed {
        let data_type = field.data_type();
        if matches!(
            data_type,
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_)
        ) {
            return Err(unsupported_datatype(field.name(), data_type));
        }
        fields.push((field.name().clone(), data_type.clone()));
    }
    let arrow_fields: Vec<FieldRef> = fields
        .iter()
        .map(|(name, data_type)| Arc::new(Field::new(name, data_type.clone(), true)))
        .collect();
    Ok((DataType::Struct(Fields::from(arrow_fields)), fields))
}

fn input_type_error(
    name: &str,
    schema: &str,
    data_type: &DataType,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"from_csv({name}, {schema})\" \
         due to data type mismatch: The first parameter requires the \"STRING\" type, however \
         \"{name}\" has the type \"{}\". SQLSTATE: 42K09",
        super::spark_type_name(data_type)
    ))
}

#[derive(Debug)]
struct SparkFromCsv {
    signature: Signature,
}

impl SparkFromCsv {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFromCsv {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFromCsv {}

impl Hash for SparkFromCsv {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkFromCsv {
    fn name(&self) -> &str {
        FROM_CSV_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 3 {
            return Err(wrong_num_args(args.arg_fields.len()));
        }
        let Some(schema_text) = literal_text(args.scalar_arguments[1]) else {
            return Ok(Arc::new(Field::new(self.name(), DataType::Null, true)));
        };
        if !is_string_type(args.arg_fields[0].data_type()) {
            return Err(input_type_error(
                args.arg_fields[0].name(),
                &schema_text,
                args.arg_fields[0].data_type(),
            ));
        }
        let nullable = args.arg_fields[0].is_nullable();
        if args.arg_fields.len() == 3 {
            match args.arg_fields[2].data_type() {
                DataType::Map(entry, _) => {
                    let DataType::Struct(pair) = entry.data_type() else {
                        return plan_err!(
                            "[INVALID_OPTIONS.NON_MAP_FUNCTION] Must use the `map()` function \
                             for options."
                        );
                    };
                    let strings = pair.iter().all(|field| is_string_type(field.data_type()));
                    if !strings || pair.len() != 2 {
                        return plan_err!(
                            "[INVALID_OPTIONS.NON_STRING_TYPE] The options expression must be a \
                             map of strings."
                        );
                    }
                }
                _ => {
                    return plan_err!(
                        "[INVALID_OPTIONS.NON_MAP_FUNCTION] Must use the `map()` function for \
                         options."
                    );
                }
            }
            if let Some(scalar @ ScalarValue::Map(_)) = args.scalar_arguments[2] {
                options_from_map(scalar)?;
            }
        }
        let (data_type, _) = csv_struct_type(&schema_text)?;
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(wrong_num_args(arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return Err(wrong_num_args(args.args.len()));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let documents = string_column(&arrays[0])?;
        let schemas = string_column(&arrays[1])?;
        if schemas.is_null(0) {
            return plan_err!(
                "[INVALID_SCHEMA.NON_STRING_LITERAL] The input schema is not a valid schema \
                 string. The input expression must be string literal and not null."
            );
        }
        let (_, fields) = csv_struct_type(schemas.value(0))?;
        let options = if arrays.len() == 3 {
            read_options_array(&arrays[2])?
        } else {
            CsvOptions::default()
        };
        decode_records(&documents, &fields, &options)
    }
}

enum FieldBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Decimal(Decimal128Builder),
    Binary(BinaryBuilder),
}

fn field_builder(data_type: &DataType, capacity: usize) -> FieldBuilder {
    match data_type {
        DataType::Boolean => FieldBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 | DataType::UInt8 => {
            FieldBuilder::Int8(Int8Builder::with_capacity(capacity))
        }
        DataType::Int16 | DataType::UInt16 => {
            FieldBuilder::Int16(Int16Builder::with_capacity(capacity))
        }
        DataType::Int32 | DataType::UInt32 => {
            FieldBuilder::Int32(Int32Builder::with_capacity(capacity))
        }
        DataType::Int64 | DataType::UInt64 => {
            FieldBuilder::Int64(Int64Builder::with_capacity(capacity))
        }
        DataType::Float32 => FieldBuilder::Float32(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => FieldBuilder::Float64(Float64Builder::with_capacity(capacity)),
        DataType::Date32 | DataType::Date64 => {
            FieldBuilder::Date32(Date32Builder::with_capacity(capacity))
        }
        DataType::Timestamp(_, _) => {
            FieldBuilder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Decimal128(precision, scale) => match Decimal128Builder::with_capacity(capacity)
            .with_precision_and_scale(*precision, *scale)
        {
            Ok(builder) => FieldBuilder::Decimal(builder),
            Err(_) => FieldBuilder::Decimal(Decimal128Builder::with_capacity(capacity)),
        },
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            FieldBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 8))
        }
        _ => FieldBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 8)),
    }
}

fn append_null(builder: &mut FieldBuilder) {
    match builder {
        FieldBuilder::Boolean(inner) => inner.append_null(),
        FieldBuilder::Int8(inner) => inner.append_null(),
        FieldBuilder::Int16(inner) => inner.append_null(),
        FieldBuilder::Int32(inner) => inner.append_null(),
        FieldBuilder::Int64(inner) => inner.append_null(),
        FieldBuilder::Float32(inner) => inner.append_null(),
        FieldBuilder::Float64(inner) => inner.append_null(),
        FieldBuilder::Utf8(inner) => inner.append_null(),
        FieldBuilder::Date32(inner) => inner.append_null(),
        FieldBuilder::Timestamp(inner) => inner.append_null(),
        FieldBuilder::Decimal(inner) => inner.append_null(),
        FieldBuilder::Binary(inner) => inner.append_null(),
    }
}

fn finish_builder(builder: &mut FieldBuilder) -> ArrayRef {
    match builder {
        FieldBuilder::Boolean(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Int8(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Int16(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Int32(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Int64(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Float32(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Float64(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Utf8(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Date32(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Timestamp(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Decimal(inner) => Arc::new(inner.finish()) as ArrayRef,
        FieldBuilder::Binary(inner) => Arc::new(inner.finish()) as ArrayRef,
    }
}

fn null_token(mark: impl FnOnce()) -> bool {
    mark();
    true
}

fn append_token(
    builder: &mut FieldBuilder,
    data_type: &DataType,
    token: &str,
    options: &CsvOptions,
) -> bool {
    if token_is_null(token, options) {
        append_null(builder);
        return false;
    }
    match (builder, data_type) {
        (FieldBuilder::Boolean(inner), _) => match token.to_ascii_lowercase().as_str() {
            "true" => inner.append_value(true),
            "false" => inner.append_value(false),
            _ => return null_token(|| inner.append_null()),
        },
        (FieldBuilder::Int8(inner), _) => {
            if let Ok(value) = token.parse::<i8>() {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Int16(inner), _) => {
            if let Ok(value) = token.parse::<i16>() {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Int32(inner), _) => {
            if let Ok(value) = token.parse::<i32>() {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Int64(inner), _) => {
            if let Ok(value) = token.parse::<i64>() {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Float32(inner), _) => {
            if let Ok(value) = token.parse::<f32>() {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Float64(inner), _) => {
            if let Ok(value) = token.parse::<f64>() {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Utf8(inner), _) => inner.append_value(token),
        (FieldBuilder::Date32(inner), _) => {
            if let Some(date) = parse_csv_date(token, options.date_format.as_deref()) {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(date);
                if let Ok(days) = i32::try_from(date.signed_duration_since(epoch).num_days()) {
                    inner.append_value(days);
                } else {
                    return null_token(|| inner.append_null());
                }
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Timestamp(inner), _) => {
            if let Some(micros) = parse_csv_timestamp(
                token,
                options
                    .timestamp_format
                    .as_deref()
                    .or(options.date_format.as_deref()),
            ) {
                inner.append_value(micros);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Decimal(inner), DataType::Decimal128(_, scale)) => {
            if let Some(value) = parse_decimal(token, *scale) {
                inner.append_value(value);
            } else {
                return null_token(|| inner.append_null());
            }
        }
        (FieldBuilder::Binary(inner), _) => inner.append_value(token.as_bytes()),
        (fallback, _) => {
            append_null(fallback);
            return true;
        }
    }
    false
}

fn parse_decimal(token: &str, scale: i8) -> Option<i128> {
    let token = token.strip_prefix('+').unwrap_or(token);
    let (negative, digits) = match token.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, token),
    };
    let (whole, fraction) = match digits.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (digits, ""),
    };
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.chars().all(|found| found.is_ascii_digit())
        || !fraction.chars().all(|found| found.is_ascii_digit())
    {
        return None;
    }
    let scale_usize = usize::try_from(scale).ok()?;
    let mut scaled = format!("{whole}{fraction}");
    if fraction.len() > scale_usize {
        return None;
    }
    scaled.push_str(&"0".repeat(scale_usize - fraction.len()));
    let mut value: i128 = scaled.parse().ok()?;
    if negative {
        value = -value;
    }
    Some(value)
}

fn decode_records(
    documents: &StringArray,
    fields: &[(String, DataType)],
    options: &CsvOptions,
) -> Result<ColumnarValue> {
    let corrupt_index = options
        .corrupt_record_column
        .as_ref()
        .and_then(|name| fields.iter().position(|(field, _)| field == name));
    let mut builders: Vec<FieldBuilder> = fields
        .iter()
        .map(|(_, data_type)| field_builder(data_type, documents.len()))
        .collect();
    let mut validity = NullBufferBuilder::new(documents.len());
    for row in 0..documents.len() {
        if documents.is_null(row) {
            for builder in &mut builders {
                append_null(builder);
            }
            validity.append_null();
            continue;
        }
        validity.append_non_null();
        let tokens = split_csv_record(documents.value(row), options);
        let mut malformed = tokens.len() > fields.len();
        let mut shown: Vec<String> = Vec::with_capacity(fields.len());
        for (index, ((_, data_type), builder)) in fields.iter().zip(builders.iter_mut()).enumerate()
        {
            let is_corrupt_field = Some(index) == corrupt_index;
            if is_corrupt_field {
                shown.push("null".to_string());
                continue;
            }
            let Some(token) = tokens.get(index) else {
                append_null(builder);
                shown.push("null".to_string());
                malformed = true;
                continue;
            };
            let bad = append_token(builder, data_type, token, options);
            if bad {
                malformed = true;
                shown.push("null".to_string());
            } else if token_is_null(token, options) {
                shown.push("null".to_string());
            } else {
                shown.push((*token).clone());
            }
        }
        if options.mode == CsvMode::FailFast && malformed {
            return exec_err!(
                "[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected \
                 in record parsing: [{}].",
                shown.join(",")
            );
        }
        let mark = malformed && options.mode == CsvMode::Permissive;
        if let Some(index) = corrupt_index {
            if mark {
                let FieldBuilder::Utf8(inner) = &mut builders[index] else {
                    return plan_err!("the corrupt record column is a string field");
                };
                inner.append_value(documents.value(row));
            } else {
                append_null(&mut builders[index]);
            }
        }
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(builders.len());
    for builder in &mut builders {
        columns.push(finish_builder(builder));
    }
    let names: Vec<FieldRef> = fields
        .iter()
        .map(|(name, data_type)| Arc::new(Field::new(name, data_type.clone(), true)))
        .collect();
    let validity = validity.finish();
    Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
        Fields::from(names),
        columns,
        validity,
    )?)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn run(sql: &str) -> datafusion::common::Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async { ctx.sql(sql).await?.collect().await })
    }

    fn first(sql: &str) -> String {
        let batches = run(sql).expect("from_csv");
        datafusion::common::ScalarValue::try_from_array(batches[0].column(0), 0)
            .expect("scalar")
            .to_string()
    }

    #[test]
    fn permissive_pads_nulls_and_parses_typed_fields() {
        assert_eq!(
            first("SELECT from_csv('1,abc,2.5', 'a INT, b STRING, c DOUBLE')"),
            "{a:1,b:abc,c:2.5}"
        );
        assert_eq!(
            first("SELECT from_csv('x,y,z', 'a INT, b STRING, c DOUBLE')"),
            "{a:,b:y,c:}"
        );
        assert_eq!(
            first("SELECT from_csv(',,', 'a INT, b STRING, c DOUBLE')"),
            "{a:,b:,c:}"
        );
        assert_eq!(
            first("SELECT from_csv('8', 'a INT, b STRING, c DOUBLE')"),
            "{a:8,b:,c:}"
        );
    }

    #[test]
    fn failfast_throws_on_the_first_bad_row() {
        let error = run("SELECT from_csv('1,abc,2.5', 'a INT, b INT', map('mode', 'FAILFAST'))")
            .unwrap_err();
        assert!(format!("{error}").contains("MALFORMED_RECORD_IN_PARSING"));
    }

    #[test]
    fn corrupt_column_captures_the_whole_record() {
        assert_eq!(
            first(
                "SELECT from_csv('x,y,z', 'a INT, b STRING, c DOUBLE, _corrupt_record STRING', \
                 map('columnNameOfCorruptRecord', '_corrupt_record'))"
            ),
            "{a:,b:y,c:,_corrupt_record:x,y,z}"
        );
    }

    #[test]
    fn quoted_separator_stays_inside_the_field() {
        assert_eq!(
            first("SELECT from_csv('7,\"q,r\",1e3', 'a INT, b STRING, c DOUBLE')"),
            "{a:7,b:q,r,c:1000.0}"
        );
    }

    #[test]
    fn sliced_documents_decode_without_a_panic() {
        use datafusion::arrow::array::{Array, ArrayRef, StringArray};
        use datafusion::common::config::ConfigOptions;
        use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
        use std::sync::Arc;
        let full = StringArray::from(vec!["1,x", "bad,row,extra", "3,z", "skip"]);
        let documents: ArrayRef = Arc::new(full.slice(1, 2));
        let schemas: ArrayRef = Arc::new(StringArray::from(vec![
            "a INT, b STRING",
            "a INT, b STRING",
        ]));
        let udf = super::from_csv_udf();
        let (output, _) = super::csv_struct_type("a INT, b STRING").expect("schema");
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(documents),
                ColumnarValue::Array(schemas),
            ],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "from_csv", output, true,
            )),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let decoded = udf.invoke_with_args(args).expect("sliced decodes");
        let ColumnarValue::Array(array) = decoded else {
            panic!("from_csv answers an array");
        };
        assert_eq!(array.len(), 2);
        let shown = datafusion::common::ScalarValue::try_from_array(&array, 0)
            .expect("scalar")
            .to_string();
        assert_eq!(shown, "{a:,b:row}");
    }

    #[test]
    fn null_documents_answer_a_null_struct() {
        assert_eq!(
            first("SELECT from_csv(CAST(NULL AS STRING), 'a INT')"),
            "NULL"
        );
    }
}
