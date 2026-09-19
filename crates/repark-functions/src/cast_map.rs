use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, MapArray, StructArray, new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{CastOptions, can_cast_types, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, lit,
};
use datafusion::prelude::SessionContext;

use crate::ansi::spark_ansi_enabled_from_options;

mod rewrite;

pub use rewrite::{map_cast_target, map_cast_token, rewrite_map_casts};

pub const CAST_MAP_NAME: &str = "__repark_cast_map__";
pub const EMPTY_MAP_CAST_NAME: &str = "__repark_empty_map_cast__";

static CAST_MAP: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkCastMap::new(false))));
static EMPTY_MAP_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkCastMap::new(true))));

#[must_use]
pub fn cast_map_udf() -> Arc<ScalarUDF> {
    Arc::clone(&CAST_MAP)
}

#[must_use]
pub fn empty_map_cast_udf() -> Arc<ScalarUDF> {
    Arc::clone(&EMPTY_MAP_CAST)
}

pub fn register(ctx: &SessionContext) {
    ctx.register_udf(cast_map_udf().as_ref().clone());
    ctx.register_udf(empty_map_cast_udf().as_ref().clone());
}

#[must_use]
pub fn cast_map_expr(value: Expr, target: &DataType, try_cast: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        cast_map_udf(),
        vec![value, lit(target.to_string()), lit(try_cast)],
    ))
}

#[must_use]
pub fn contains_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            contains_map(field.data_type())
        }
        DataType::Struct(fields) => fields.iter().any(|field| contains_map(field.data_type())),
        _ => false,
    }
}

#[must_use]
pub fn spark_sql_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "VOID".to_owned(),
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 => "TINYINT".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INT".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_owned(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_owned(),
        DataType::Date32 | DataType::Date64 => "DATE".to_owned(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_owned(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_owned(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_sql_name(field.data_type()))
        }
        DataType::Map(entries, _) => match map_entry_types(entries) {
            Some((key, value)) => {
                format!("MAP<{}, {}>", spark_sql_name(key), spark_sql_name(value))
            }
            None => entries.data_type().to_string(),
        },
        DataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), spark_sql_name(field.data_type())))
                .collect();
            format!("STRUCT<{}>", parts.join(", "))
        }
        other => other.to_string(),
    }
}

fn map_entry_types(entries: &FieldRef) -> Option<(&DataType, &DataType)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => {
            Some((fields[0].data_type(), fields[1].data_type()))
        }
        _ => None,
    }
}

fn is_complex(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Map(_, _)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
    )
}

fn spark_can_cast(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Null, _) => true,
        (DataType::Map(source_entries, _), DataType::Map(target_entries, _)) => {
            match (
                map_entry_types(source_entries),
                map_entry_types(target_entries),
            ) {
                (Some((source_key, source_value)), Some((target_key, target_value))) => {
                    spark_can_cast(source_key, target_key)
                        && spark_can_cast(source_value, target_value)
                }
                _ => false,
            }
        }
        (
            DataType::List(source_field)
            | DataType::LargeList(source_field)
            | DataType::FixedSizeList(source_field, _),
            DataType::List(target_field) | DataType::LargeList(target_field),
        ) => spark_can_cast(source_field.data_type(), target_field.data_type()),
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            source_fields.len() == target_fields.len()
                && source_fields
                    .iter()
                    .zip(target_fields.iter())
                    .all(|(from, to)| spark_can_cast(from.data_type(), to.data_type()))
        }
        _ if is_complex(source) || is_complex(target) => false,
        _ => can_cast_types(source, target),
    }
}

fn cast_mismatch(operand: &str, source: &DataType, target: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"{operand}\" due to data \
         type mismatch: cannot cast \"{}\" to \"{}\". SQLSTATE: 42K09",
        spark_sql_name(source),
        spark_sql_name(target)
    ))
}

fn cast_invalid_input(value: &str, source: &DataType, target: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"{}\" cannot be cast to \"{}\" \
         because it is malformed. Correct the value as per the syntax, or change its target \
         type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: \
         22018",
        spark_sql_name(source),
        spark_sql_name(target)
    ))
}

fn literal_text(scalar: Option<&ScalarValue>) -> Option<&str> {
    match scalar? {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Some(text.as_str()),
        _ => None,
    }
}

fn literal_flag(scalar: Option<&ScalarValue>) -> Option<bool> {
    match scalar? {
        ScalarValue::Boolean(Some(flag)) => Some(*flag),
        _ => None,
    }
}

fn expr_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value),
        _ => None,
    }
}

fn parse_target(name: &str, text: Option<&str>) -> Result<DataType> {
    let Some(text) = text else {
        return plan_err!("'{name}' needs a literal target type");
    };
    DataType::from_str(text)
        .map_err(|error| DataFusionError::Plan(format!("'{name}' target type {text}: {error}")))
}

fn empty_map(target: &DataType, rows: usize) -> Result<ArrayRef> {
    let DataType::Map(entries_field, sorted) = target else {
        return exec_err!("'{EMPTY_MAP_CAST_NAME}' needs a map target, got {target}");
    };
    let entries = new_empty_array(entries_field.data_type());
    let entries: StructArray = entries.as_struct().clone();
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(entries_field),
        OffsetBuffer::new_zeroed(rows),
        entries,
        None,
        *sorted,
    )?))
}

fn first_malformed(
    source: &ArrayRef,
    target: &DataType,
) -> Result<Option<(String, DataType, DataType)>> {
    match (source.data_type(), target) {
        (DataType::Map(_, _), DataType::Map(target_entries, _)) => {
            let Some((key, value)) = map_entry_types(target_entries) else {
                return Ok(None);
            };
            let map = source.as_map();
            if let Some(found) = first_malformed(map.keys(), key)? {
                return Ok(Some(found));
            }
            first_malformed(map.values(), value)
        }
        (DataType::List(_), DataType::List(field) | DataType::LargeList(field)) => {
            first_malformed(source.as_list::<i32>().values(), field.data_type())
        }
        (DataType::LargeList(_), DataType::List(field) | DataType::LargeList(field)) => {
            first_malformed(source.as_list::<i64>().values(), field.data_type())
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            for (column, field) in source.as_struct().columns().iter().zip(fields.iter()) {
                if let Some(found) = first_malformed(column, field.data_type())? {
                    return Ok(Some(found));
                }
            }
            Ok(None)
        }
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, _) => {
            let options = CastOptions {
                safe: true,
                ..CastOptions::default()
            };
            let lenient = cast_with_options(source.as_ref(), target, &options)?;
            let Some(index) =
                (0..source.len()).find(|&row| source.is_valid(row) && lenient.is_null(row))
            else {
                return Ok(None);
            };
            let value = array_value_to_string(source.as_ref(), index)?;
            Ok(Some((value, source.data_type().clone(), target.clone())))
        }
        _ => Ok(None),
    }
}

fn cast_values(source: &ArrayRef, target: &DataType, safe: bool) -> Result<ArrayRef> {
    if source.data_type() == &DataType::Null {
        return Ok(new_null_array(target, source.len()));
    }
    let options = CastOptions {
        safe,
        ..CastOptions::default()
    };
    match cast_with_options(source.as_ref(), target, &options) {
        Ok(cast) => Ok(cast),
        Err(error) if !safe => match first_malformed(source, target)? {
            Some((value, from, to)) => Err(cast_invalid_input(&value, &from, &to)),
            None => Err(error.into()),
        },
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkCastMap {
    signature: Signature,
    empty_source: bool,
}

impl SparkCastMap {
    fn new(empty_source: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            empty_source,
        }
    }
}

impl ScalarUDFImpl for SparkCastMap {
    fn name(&self) -> &str {
        if self.empty_source {
            EMPTY_MAP_CAST_NAME
        } else {
            CAST_MAP_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [value, target, try_cast] = args else {
            return plan_err!("'{}' takes three arguments", self.name());
        };
        let target = parse_target(self.name(), literal_text(expr_literal(target)))?;
        let keyword = if literal_flag(expr_literal(try_cast)) == Some(true) {
            "TRY_CAST"
        } else {
            "CAST"
        };
        let operand = if self.empty_source {
            "map()".to_owned()
        } else {
            value.schema_name().to_string()
        };
        Ok(format!(
            "{keyword}({operand} AS {})",
            spark_sql_name(&target)
        ))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!(
            "'{}' resolves its type from its literal target",
            self.name()
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let target = parse_target(
            self.name(),
            literal_text(args.scalar_arguments.get(1).copied().flatten()),
        )?;
        let Some(try_cast) = literal_flag(args.scalar_arguments.get(2).copied().flatten()) else {
            return plan_err!("'{}' needs a literal try_cast flag", self.name());
        };
        let Some(source) = args.arg_fields.first() else {
            return plan_err!("'{}' takes three arguments", self.name());
        };
        if self.empty_source {
            if !matches!(target, DataType::Map(_, _)) {
                let empty = DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Null, false),
                                Field::new("value", DataType::Null, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                );
                return Err(cast_mismatch("map()", &empty, &target));
            }
            return Ok(Arc::new(Field::new(self.name(), target, try_cast)));
        }
        if !spark_can_cast(source.data_type(), &target) {
            return Err(cast_mismatch(source.name(), source.data_type(), &target));
        }
        let nullable = source.is_nullable() || try_cast;
        Ok(Arc::new(Field::new(self.name(), target, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let target = args.return_field.data_type().clone();
        if self.empty_source {
            return Ok(ColumnarValue::Array(empty_map(&target, args.number_rows)?));
        }
        let [value, _, try_cast] = args.args.as_slice() else {
            return exec_err!("'{}' takes three arguments", self.name());
        };
        let try_cast = match try_cast {
            ColumnarValue::Scalar(flag) => literal_flag(Some(flag)).unwrap_or(false),
            ColumnarValue::Array(_) => false,
        };
        let safe = try_cast || !spark_ansi_enabled_from_options(&args.config_options);
        match value {
            ColumnarValue::Array(source) => {
                Ok(ColumnarValue::Array(cast_values(source, &target, safe)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let source = scalar.to_array_of_size(1)?;
                let cast = cast_values(&source, &target, safe)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &cast, 0,
                )?))
            }
        }
    }
}

#[cfg(test)]
mod tests;
