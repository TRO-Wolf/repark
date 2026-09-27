use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use arrow::compute::kernels::cast_utils::string_to_datetime;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, TimeUnit};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::ansi::spark_ansi_enabled_from_options;
use crate::timestamp_cast::format_spark_timestamp_string;
use crate::timestamp_ltz_ntz::wall_without_zone;

pub const TIMESTAMP_NTZ_CAST_NAME: &str = "__repark_cast_timestamp_ntz__";
pub const TRY_TIMESTAMP_NTZ_CAST_NAME: &str = "__repark_try_cast_timestamp_ntz__";
pub const TIMESTAMP_NTZ_LITERAL_NAME: &str = "__repark_timestamp_ntz__";

static TIMESTAMP_NTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampNtzCast::new(false))));
static TRY_TIMESTAMP_NTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampNtzCast::new(true))));
static TIMESTAMP_NTZ_LITERAL: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampNtzLiteral::new())));

#[must_use]
pub fn timestamp_ntz_cast_udf(try_cast: bool) -> Arc<ScalarUDF> {
    if try_cast {
        Arc::clone(&TRY_TIMESTAMP_NTZ_CAST)
    } else {
        Arc::clone(&TIMESTAMP_NTZ_CAST)
    }
}

#[must_use]
pub fn timestamp_ntz_cast_expr(expr: Expr, try_cast: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        timestamp_ntz_cast_udf(try_cast),
        vec![expr],
    ))
}

#[must_use]
pub fn timestamp_ntz_literal_udf() -> Arc<ScalarUDF> {
    Arc::clone(&TIMESTAMP_NTZ_LITERAL)
}

#[must_use]
pub(crate) fn rewrite_ntz_target_cast(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let Expr::Cast(cast) = expr else {
        return None;
    };
    if !matches!(
        cast.field.data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ) {
        return None;
    }
    let source = cast.expr.get_type(schema).ok()?;
    let retarget = match &source {
        DataType::Timestamp(unit, None) => !matches!(unit, TimeUnit::Microsecond),
        DataType::Timestamp(_, Some(_))
        | DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        _ => false,
    };
    retarget.then(|| timestamp_ntz_cast_expr((*cast.expr).clone(), false))
}

#[must_use]
pub fn parse_timestamp_ntz_wall(text: &str) -> Option<i64> {
    let wall = wall_without_zone(text);
    let parsed = string_to_datetime(&Utc, wall.trim()).ok()?;
    parsed
        .timestamp_nanos_opt()
        .map(|nanos| nanos.div_euclid(1_000))
}

fn ntz_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn is_string_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_cast_source(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Null)
        || is_string_source(data_type)
        || matches!(
            data_type,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        )
}

fn spark_source_name(data_type: &DataType, scalar: Option<&ScalarValue>) -> String {
    if let Some(ScalarValue::Int64(Some(value))) = scalar
        && i32::try_from(*value).is_ok()
    {
        return "INT".to_string();
    }
    crate::cast_map::spark_sql_name(data_type)
}

fn cast_refusal(
    try_cast: bool,
    arg: &str,
    source: &DataType,
    scalar: Option<&ScalarValue>,
) -> DataFusionError {
    let keyword = if try_cast { "TRY_CAST" } else { "CAST" };
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"{keyword}({arg} AS \
         TIMESTAMP_NTZ)\" due to data type mismatch: cannot cast \"{}\" to \"TIMESTAMP_NTZ\". \
         SQLSTATE: 42K09",
        spark_source_name(source, scalar)
    ))
}

#[derive(Debug)]
struct SparkTimestampNtzCast {
    try_cast: bool,
    signature: Signature,
}

impl SparkTimestampNtzCast {
    fn new(try_cast: bool) -> Self {
        Self {
            try_cast,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkTimestampNtzCast {
    fn eq(&self, other: &Self) -> bool {
        self.try_cast == other.try_cast
    }
}

impl Eq for SparkTimestampNtzCast {}

impl Hash for SparkTimestampNtzCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTimestampNtzCast {
    fn name(&self) -> &str {
        if self.try_cast {
            TRY_TIMESTAMP_NTZ_CAST_NAME
        } else {
            TIMESTAMP_NTZ_CAST_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(source) = arg_types.first()
            && !is_cast_source(source)
        {
            return Err(cast_refusal(self.try_cast, "value", source, None));
        }
        Ok(ntz_timestamp_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = match args.arg_fields.first() {
            Some(field) => {
                if !is_cast_source(field.data_type()) {
                    let scalar = args.scalar_arguments.first().copied().flatten();
                    let arg = scalar.map_or_else(|| field.name().to_owned(), ToString::to_string);
                    return Err(cast_refusal(self.try_cast, &arg, field.data_type(), scalar));
                }
                field.is_nullable() || self.try_cast
            }
            None => true,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            ntz_timestamp_type(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let tolerate = self.try_cast || !ansi;
        match args.args.first() {
            Some(ColumnarValue::Array(array)) => {
                crate::timestamp_ltz_ntz::ntz_single(array, &args, tolerate)
            }
            Some(ColumnarValue::Scalar(scalar)) => {
                let single =
                    crate::timestamp_ltz_ntz::ntz_single(&scalar.to_array()?, &args, tolerate)?;
                let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(&single))?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &arrays[0], 0,
                )?))
            }
            None => Err(DataFusionError::Plan(format!(
                "'{}' expects one argument",
                self.name()
            ))),
        }
    }
}

#[derive(Debug)]
struct SparkTimestampNtzLiteral {
    signature: Signature,
}

impl SparkTimestampNtzLiteral {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkTimestampNtzLiteral {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkTimestampNtzLiteral {}

impl Hash for SparkTimestampNtzLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn literal_wall_name(arg: &Expr) -> String {
    let wall = match arg {
        Expr::Literal(ScalarValue::Int64(Some(micros)), _) => {
            DateTime::from_timestamp_micros(*micros).map(|utc| utc.naive_utc())
        }
        _ => None,
    };
    match wall {
        Some(naive) => format!("TIMESTAMP_NTZ '{}'", format_spark_timestamp_string(naive)),
        None => format!("TIMESTAMP_NTZ '{}'", arg.schema_name()),
    }
}

#[must_use]
pub(crate) fn literal_display_name(expr: &Expr) -> Option<String> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func.name() == TIMESTAMP_NTZ_LITERAL_NAME {
        return function.args.first().map(literal_wall_name);
    }
    if function.func.name() == crate::decimal_cast::DECIMAL_CAST_NULLABLE_NAME {
        return function.args.first().and_then(literal_display_name);
    }
    let keyword = if function.func.name() == TIMESTAMP_NTZ_CAST_NAME {
        "CAST"
    } else if function.func.name() == TRY_TIMESTAMP_NTZ_CAST_NAME {
        "TRY_CAST"
    } else {
        return None;
    };
    let arg = function.args.first().and_then(cast_arg_name)?;
    Some(format!("{keyword}({arg} AS TIMESTAMP_NTZ)"))
}

fn cast_arg_name(arg: &Expr) -> Option<String> {
    match arg {
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => Some(text.clone()),
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

impl ScalarUDFImpl for SparkTimestampNtzLiteral {
    fn name(&self) -> &str {
        TIMESTAMP_NTZ_LITERAL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        match args.first() {
            Some(arg) => Ok(literal_wall_name(arg)),
            None => Err(DataFusionError::Plan(format!(
                "'{TIMESTAMP_NTZ_LITERAL_NAME}' expects one argument"
            ))),
        }
    }

    #[allow(deprecated)]
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        self.schema_name(args)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ntz_timestamp_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            ntz_timestamp_type(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.first() {
            Some(ColumnarValue::Array(array)) => {
                let ticks = array.as_primitive::<Int64Type>();
                let mut builder = TimestampMicrosecondArray::builder(ticks.len());
                for row in 0..ticks.len() {
                    if ticks.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(ticks.value(row));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            Some(ColumnarValue::Scalar(ScalarValue::Int64(micros))) => Ok(ColumnarValue::Scalar(
                ScalarValue::TimestampMicrosecond(*micros, None),
            )),
            Some(ColumnarValue::Scalar(other)) => Err(DataFusionError::Plan(format!(
                "'{TIMESTAMP_NTZ_LITERAL_NAME}' expects an Int64 wall, got {other}"
            ))),
            None => Err(DataFusionError::Plan(format!(
                "'{TIMESTAMP_NTZ_LITERAL_NAME}' expects one argument"
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
