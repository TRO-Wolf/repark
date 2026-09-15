use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, StringArray};
use datafusion::arrow::compute::{cast, unary};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::bitmap_agg::{spark_type_name, utf8_strings};

const DEGREES_PER_RADIAN: f64 = 180.0 / std::f64::consts::PI;
const RADIANS_PER_DEGREE: f64 = std::f64::consts::PI / 180.0;

#[must_use]
pub fn degrees_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkAngleConvert::degrees()))
}

#[must_use]
pub fn radians_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkAngleConvert::radians()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![degrees_udf(), radians_udf()]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AngleOp {
    Degrees,
    Radians,
}

#[derive(Debug)]
struct SparkAngleConvert {
    signature: Signature,
    op: AngleOp,
}

impl SparkAngleConvert {
    fn degrees() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            op: AngleOp::Degrees,
        }
    }

    fn radians() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            op: AngleOp::Radians,
        }
    }

    fn display(&self) -> &'static str {
        match self.op {
            AngleOp::Degrees => "DEGREES",
            AngleOp::Radians => "RADIANS",
        }
    }

    fn factor(&self) -> f64 {
        match self.op {
            AngleOp::Degrees => DEGREES_PER_RADIAN,
            AngleOp::Radians => RADIANS_PER_DEGREE,
        }
    }
}

impl PartialEq for SparkAngleConvert {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
    }
}

impl Eq for SparkAngleConvert {}

impl Hash for SparkAngleConvert {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkAngleConvert {
    fn name(&self) -> &str {
        match self.op {
            AngleOp::Degrees => "degrees",
            AngleOp::Radians => "radians",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let field = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{}' expects one argument", self.name()))
        })?;
        refuse_unless_angle_input(self.display(), Some(field.name()), field.data_type())?;
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [data_type] = arg_types else {
            return exec_err!(
                "'{}' expects one argument, got {}",
                self.name(),
                arg_types.len()
            );
        };
        Ok(vec![data_type.clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'{}' expects one argument", self.name());
        };
        let argument = args.arg_fields.first().map(|field| field.name().clone());
        let array = arg.to_array(args.number_rows)?;
        refuse_unless_angle_input(self.display(), argument.as_deref(), array.data_type())?;
        let ansi = crate::ansi::spark_ansi_enabled_from_options(&args.config_options);
        Ok(ColumnarValue::Array(convert_array(
            self.display(),
            self.factor(),
            argument.as_deref(),
            ansi,
            &array,
        )?))
    }
}

fn is_angle_input(data_type: &DataType) -> bool {
    data_type.is_numeric()
        || matches!(
            data_type,
            DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
}

fn refuse_unless_angle_input(
    display: &str,
    argument: Option<&str>,
    data_type: &DataType,
) -> Result<()> {
    if is_angle_input(data_type) {
        return Ok(());
    }
    let got = spark_type_name(data_type);
    if let Some(argument) = argument {
        plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{display}({argument})\" \
             due to data type mismatch: The first parameter requires the \"DOUBLE\" type, \
             however \"{argument}\" has the type \"{got}\". SQLSTATE: 42K09"
        )
    } else {
        plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{display}\" due to \
             data type mismatch: The first parameter requires the \"DOUBLE\" type, however \
             the argument has the type \"{got}\". SQLSTATE: 42K09"
        )
    }
}

fn malformed_double_cast(value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"DOUBLE\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    ))
}

fn convert_array(
    display: &str,
    factor: f64,
    argument: Option<&str>,
    ansi: bool,
    array: &ArrayRef,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Null => Ok(datafusion::arrow::array::new_null_array(
            &DataType::Float64,
            array.len(),
        )),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            convert_strings(display, factor, argument, ansi, array)
        }
        _ => {
            let doubles = cast(array, &DataType::Float64)
                .map_err(|_| angle_refusal_for(display, argument, array.data_type()))?;
            Ok(Arc::new(unary::<Float64Type, _, Float64Type>(
                doubles.as_primitive::<Float64Type>(),
                |value| value * factor,
            )))
        }
    }
}

fn angle_refusal_for(
    display: &str,
    argument: Option<&str>,
    data_type: &DataType,
) -> DataFusionError {
    let got = spark_type_name(data_type);
    match argument {
        Some(argument) => DataFusionError::Execution(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \
             \"{display}({argument})\" due to data type mismatch: The first parameter \
             requires the \"DOUBLE\" type, however \"{argument}\" has the type \"{got}\". \
             SQLSTATE: 42K09"
        )),
        None => DataFusionError::Execution(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{display}\" due to \
             data type mismatch: The first parameter requires the \"DOUBLE\" type, however \
             the argument has the type \"{got}\". SQLSTATE: 42K09"
        )),
    }
}

fn convert_strings(
    display: &str,
    factor: f64,
    argument: Option<&str>,
    ansi: bool,
    array: &ArrayRef,
) -> Result<ArrayRef> {
    let strings = utf8_strings(array)?;
    let values = strings
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| angle_refusal_for(display, argument, array.data_type()))?;
    let mut out = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            out.push(None);
            continue;
        }
        let raw = values.value(row);
        if let Ok(parsed) = raw.trim().parse::<f64>() {
            out.push(Some(parsed * factor));
        } else if ansi {
            return Err(malformed_double_cast(raw));
        } else {
            out.push(None);
        }
    }
    Ok(Arc::new(Float64Array::from(out)))
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    fn ctx_ansi_off() -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), false);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> ScalarValue {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"))
    }

    async fn error_text(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .err()
                .unwrap_or_else(|| panic!("{sql} should refuse"))
                .to_string(),
        }
    }

    #[tokio::test]
    async fn degrees_answers_spark_values_on_numeric_inputs() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT degrees(CAST(1 AS INT))").await,
            ScalarValue::Float64(Some(57.295_779_513_082_32))
        );
        assert_eq!(
            one(&ctx, "SELECT radians(CAST(1 AS INT))").await,
            ScalarValue::Float64(Some(0.017_453_292_519_943_295))
        );
        assert_eq!(
            one(&ctx, "SELECT degrees(CAST(1.5 AS FLOAT))").await,
            ScalarValue::Float64(Some(85.943_669_269_623_48))
        );
        assert_eq!(
            one(&ctx, "SELECT degrees(CAST(NULL AS DOUBLE))").await,
            ScalarValue::Float64(None)
        );
    }

    #[tokio::test]
    async fn degrees_refuses_every_non_double_family_type() {
        let ctx = ctx();
        for (literal, spark_type) in [
            ("true", "BOOLEAN"),
            ("DATE'2020-01-01'", "DATE"),
            ("TIMESTAMP'2020-01-01 00:00:00'", "TIMESTAMP"),
            ("X'01'", "BINARY"),
        ] {
            for function in ["degrees", "radians"] {
                let message = error_text(
                    &ctx,
                    &format!("SELECT {function}(x) AS r FROM (SELECT {literal} AS x)"),
                )
                .await;
                assert!(
                    message.contains("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]"),
                    "{function}({literal}): {message}"
                );
                assert!(
                    message.contains("requires the \"DOUBLE\" type"),
                    "{function}({literal}): {message}"
                );
                assert!(
                    message.contains(&format!("has the type \"{spark_type}\"")),
                    "{function}({literal}): {message}"
                );
                assert!(
                    message.contains("SQLSTATE: 42K09"),
                    "{function}({literal}): {message}"
                );
            }
        }
    }

    #[tokio::test]
    async fn degrees_malformed_string_raises_ansi_and_nulls_legacy() {
        let ctx = ctx();
        let message = error_text(&ctx, "SELECT degrees('abc')").await;
        assert!(message.contains("[CAST_INVALID_INPUT]"), "{message}");
        assert!(
            message.contains("cannot be cast to \"DOUBLE\""),
            "{message}"
        );
        assert!(message.contains("SQLSTATE: 22018"), "{message}");
        let legacy = ctx_ansi_off();
        assert_eq!(
            one(&legacy, "SELECT degrees('abc')").await,
            ScalarValue::Float64(None)
        );
        assert_eq!(
            one(&legacy, "SELECT radians('')").await,
            ScalarValue::Float64(None)
        );
        assert_eq!(
            one(&ctx, "SELECT degrees('1.0')").await,
            ScalarValue::Float64(Some(57.295_779_513_082_32))
        );
    }

    #[tokio::test]
    async fn degrees_keeps_bit_identity_on_edge_floats() {
        let ctx = ctx();
        match one(&ctx, "SELECT degrees(0.0)").await {
            ScalarValue::Float64(Some(value)) => {
                assert_eq!(value.to_bits(), 0x0);
            }
            other => panic!("expected positive zero, got {other:?}"),
        }
        match one(&ctx, "SELECT degrees(-0.0)").await {
            ScalarValue::Float64(Some(value)) => {
                assert_eq!(value.to_bits(), 0x8000_0000_0000_0000);
            }
            other => panic!("expected negative zero, got {other:?}"),
        }
        let nan = one(&ctx, "SELECT degrees('NaN')").await;
        match nan {
            ScalarValue::Float64(Some(value)) => {
                assert!(value.is_nan());
                assert_eq!(value.to_bits(), 0x7ff8_0000_0000_0000);
            }
            other => panic!("expected NaN float, got {other:?}"),
        }
        assert_eq!(
            one(&ctx, "SELECT degrees('Infinity')").await,
            ScalarValue::Float64(Some(f64::INFINITY))
        );
        assert_eq!(
            one(&ctx, "SELECT radians('Infinity')").await,
            ScalarValue::Float64(Some(f64::INFINITY))
        );
        let subnormal = one(&ctx, "SELECT degrees(5e-324)").await;
        match subnormal {
            ScalarValue::Float64(Some(value)) => {
                assert_eq!(value.to_bits(), 0x39);
            }
            other => panic!("expected subnormal float, got {other:?}"),
        }
        let underflow = one(&ctx, "SELECT radians(5e-324)").await;
        match underflow {
            ScalarValue::Float64(Some(value)) => {
                assert_eq!(value.to_bits(), 0x0);
            }
            other => panic!("expected positive zero, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn degrees_single_multiply_matches_java_factor() {
        let ctx = ctx();
        for text in [
            "1.0",
            "-1.0",
            "0.5",
            "3.141592653589793",
            "1e-300",
            "1e300",
            "123.456",
        ] {
            let answered = one(&ctx, &format!("SELECT degrees({text})")).await;
            let parsed: f64 = text.parse().unwrap_or_else(|_| panic!("bad probe {text}"));
            match answered {
                ScalarValue::Float64(Some(value)) => {
                    let wanted = parsed * (180.0 / std::f64::consts::PI);
                    assert_eq!(value.to_bits(), wanted.to_bits(), "{text}");
                }
                other => panic!("expected float for {text}, got {other:?}"),
            }
        }
    }
}
