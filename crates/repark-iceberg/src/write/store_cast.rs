use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use crate::write::store_assign::normalize_for_assignment;

pub const STORE_INT8_NAME: &str = "__repark_store_int8__";
pub const STORE_INT16_NAME: &str = "__repark_store_int16__";
pub const STORE_INT32_NAME: &str = "__repark_store_int32__";
pub const STORE_INT64_NAME: &str = "__repark_store_int64__";
pub const STORE_INT_GUARD_NAME: &str = "__repark_store_int_guard__";

pub(crate) fn spark_store_type_name(data_type: &DataType) -> Option<String> {
    match normalize_for_assignment(data_type) {
        DataType::Float32 => Some("FLOAT".to_string()),
        DataType::Float64 => Some("DOUBLE".to_string()),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Some(format!("DECIMAL({precision},{scale})"))
        }
        DataType::Int8 => Some("TINYINT".to_string()),
        DataType::Int16 => Some("SMALLINT".to_string()),
        DataType::Int32 => Some("INT".to_string()),
        DataType::Int64 => Some("BIGINT".to_string()),
        _ => None,
    }
}

pub(crate) fn store_int_target(name: &str) -> Option<DataType> {
    match name {
        STORE_INT8_NAME => Some(DataType::Int8),
        STORE_INT16_NAME => Some(DataType::Int16),
        STORE_INT32_NAME => Some(DataType::Int32),
        STORE_INT64_NAME => Some(DataType::Int64),
        _ => None,
    }
}

pub(crate) fn is_overflow_store_pair(source: &DataType, target: &DataType) -> bool {
    let source = normalize_for_assignment(source);
    let float_or_decimal = matches!(
        source,
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) | DataType::Decimal256(..)
    );
    float_or_decimal && target.is_integer()
}

fn quote_store_column(column: &str) -> String {
    format!("`{}`", column.replace('`', "``"))
}

pub(crate) fn store_overflow_message(column: &str, source: &str, target: &str) -> String {
    repark_common::spark_error::message(
        repark_common::spark_error::CAST_OVERFLOW_IN_TABLE_INSERT,
        &[
            ("fromType", source),
            ("toType", target),
            ("columnName", &quote_store_column(column)),
        ],
    )
}

pub(crate) fn store_overflow_error(column: &str, source: &str, target: &str) -> DataFusionError {
    DataFusionError::Execution(store_overflow_message(column, source, target))
}

fn strict_options() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

#[expect(clippy::cast_precision_loss)]
fn float_store_bounds(target: &DataType) -> Option<(f64, f64)> {
    match target {
        DataType::Int8 => Some((f64::from(i8::MIN), f64::from(i8::MAX))),
        DataType::Int16 => Some((f64::from(i16::MIN), f64::from(i16::MAX))),
        DataType::Int32 => Some((f64::from(i32::MIN), f64::from(i32::MAX))),
        DataType::Int64 => Some((i64::MIN as f64, i64::MAX as f64)),
        _ => None,
    }
}

fn check_float_store_value(
    column: &str,
    source: &str,
    target_name: &str,
    value: f64,
    bounds: (f64, f64),
) -> Result<()> {
    let truncated = value.trunc();
    if value.is_nan() || value.is_infinite() || truncated > bounds.1 || truncated < bounds.0 {
        return Err(store_overflow_error(column, source, target_name));
    }
    Ok(())
}

macro_rules! float_store_array {
    ($name:ident, $float:ident, $int:ident, $scalar:ident) => {
        #[expect(clippy::cast_possible_truncation)]
        fn $name(
            column: &str,
            source: &str,
            target_name: &str,
            array: &$float,
            bounds: (f64, f64),
        ) -> Result<ArrayRef> {
            let mut out = Vec::with_capacity(array.len());
            for index in 0..array.len() {
                if array.is_null(index) {
                    out.push(None);
                    continue;
                }
                let value: f64 = array.value(index).into();
                check_float_store_value(column, source, target_name, value, bounds)?;
                out.push(Some(value as $scalar));
            }
            Ok(Arc::new($int::from(out)))
        }
    };
}

float_store_array!(store_f32_to_i8, Float32Array, Int8Array, i8);
float_store_array!(store_f32_to_i16, Float32Array, Int16Array, i16);
float_store_array!(store_f32_to_i32, Float32Array, Int32Array, i32);
float_store_array!(store_f32_to_i64, Float32Array, Int64Array, i64);
float_store_array!(store_f64_to_i8, Float64Array, Int8Array, i8);
float_store_array!(store_f64_to_i16, Float64Array, Int16Array, i16);
float_store_array!(store_f64_to_i32, Float64Array, Int32Array, i32);
float_store_array!(store_f64_to_i64, Float64Array, Int64Array, i64);

fn cast_float_store_array(
    column: &str,
    source: &str,
    target_name: &str,
    array: &ArrayRef,
    target: &DataType,
) -> Result<ArrayRef> {
    let bounds = float_store_bounds(target).ok_or_else(|| {
        DataFusionError::Internal(format!("store cast to non-integer target {target:?}"))
    })?;
    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        return match target {
            DataType::Int8 => store_f64_to_i8(column, source, target_name, floats, bounds),
            DataType::Int16 => store_f64_to_i16(column, source, target_name, floats, bounds),
            DataType::Int32 => store_f64_to_i32(column, source, target_name, floats, bounds),
            DataType::Int64 => store_f64_to_i64(column, source, target_name, floats, bounds),
            _ => Err(DataFusionError::Internal(format!(
                "store cast to non-integer target {target:?}"
            ))),
        };
    }
    if let Some(floats) = array.as_any().downcast_ref::<Float32Array>() {
        return match target {
            DataType::Int8 => store_f32_to_i8(column, source, target_name, floats, bounds),
            DataType::Int16 => store_f32_to_i16(column, source, target_name, floats, bounds),
            DataType::Int32 => store_f32_to_i32(column, source, target_name, floats, bounds),
            DataType::Int64 => store_f32_to_i64(column, source, target_name, floats, bounds),
            _ => Err(DataFusionError::Internal(format!(
                "store cast to non-integer target {target:?}"
            ))),
        };
    }
    Err(DataFusionError::Internal(format!(
        "store cast float kernel saw {}",
        array.data_type()
    )))
}

fn decode_store_input(value: &ColumnarValue) -> Result<(ColumnarValue, DataType)> {
    match value {
        ColumnarValue::Scalar(scalar) => {
            if matches!(scalar.data_type(), DataType::Dictionary(_, _)) {
                let array = scalar.to_array_of_size(1)?;
                let decoded = cast_with_options(
                    array.as_ref(),
                    normalize_for_assignment(&scalar.data_type()),
                    &strict_options(),
                )?;
                let back = ScalarValue::try_from_array(&decoded, 0)?;
                let data_type = back.data_type();
                return Ok((ColumnarValue::Scalar(back), data_type));
            }
            let data_type = scalar.data_type();
            Ok((ColumnarValue::Scalar(scalar.clone()), data_type))
        }
        ColumnarValue::Array(array) => {
            if matches!(array.data_type(), DataType::Dictionary(_, _)) {
                let decoded = cast_with_options(
                    array.as_ref(),
                    normalize_for_assignment(array.data_type()),
                    &strict_options(),
                )?;
                let data_type = decoded.data_type().clone();
                return Ok((ColumnarValue::Array(Arc::new(decoded)), data_type));
            }
            let data_type = array.data_type().clone();
            Ok((ColumnarValue::Array(Arc::clone(array)), data_type))
        }
    }
}

fn typed_null_scalar(target: &DataType) -> ScalarValue {
    match target {
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        _ => ScalarValue::Null,
    }
}

pub(crate) fn cast_store_value(
    column: &str,
    value: &ColumnarValue,
    target: &DataType,
) -> Result<ColumnarValue> {
    let (value, source) = decode_store_input(value)?;
    let (Some(from), Some(to)) = (
        spark_store_type_name(&source),
        spark_store_type_name(target),
    ) else {
        return Err(DataFusionError::Internal(format!(
            "store cast saw unexpected {source:?} to {target:?} for column `{column}`"
        )));
    };
    if !is_overflow_store_pair(&source, target) {
        return Err(DataFusionError::Internal(format!(
            "store cast saw unexpected {source:?} to {target:?} for column `{column}`"
        )));
    }
    match value {
        ColumnarValue::Scalar(scalar) => {
            if scalar.is_null() {
                return Ok(ColumnarValue::Scalar(typed_null_scalar(target)));
            }
            let array = scalar.to_array_of_size(1)?;
            let casted = cast_store_array(column, &from, &to, &Arc::new(array), target)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &casted, 0,
            )?))
        }
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_store_array(
            column, &from, &to, &array, target,
        )?)),
    }
}

fn cast_store_array(
    column: &str,
    source: &str,
    target_name: &str,
    array: &ArrayRef,
    target: &DataType,
) -> Result<ArrayRef> {
    if array.data_type().is_floating() {
        return cast_float_store_array(column, source, target_name, array, target);
    }
    cast_with_options(array.as_ref(), target, &strict_options())
        .map_err(|_| store_overflow_error(column, source, target_name))
}

fn utf8_lit(args: &[ColumnarValue], index: usize, what: &str) -> Result<String> {
    let Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(text)))) = args.get(index) else {
        return Err(DataFusionError::Internal(format!(
            "store cast expected a Utf8 literal for {what}"
        )));
    };
    Ok(text.clone())
}

#[derive(Debug)]
struct StoreIntCast {
    signature: Signature,
    target: DataType,
}

impl StoreIntCast {
    fn new(target: DataType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            target,
        }
    }
}

impl PartialEq for StoreIntCast {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl Eq for StoreIntCast {}

macro_rules! store_int_cast_impl {
    ($type_name:ident, $name:expr, $target:expr) => {
        #[derive(Debug)]
        struct $type_name {
            inner: StoreIntCast,
        }

        impl $type_name {
            fn new() -> Self {
                Self {
                    inner: StoreIntCast::new($target),
                }
            }
        }

        impl PartialEq for $type_name {
            fn eq(&self, _other: &Self) -> bool {
                true
            }
        }

        impl Eq for $type_name {}

        impl Hash for $type_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.name().hash(state);
            }
        }

        impl ScalarUDFImpl for $type_name {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.inner.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($target)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let Some(value) = args.args.first() else {
                    return Err(DataFusionError::Execution(format!(
                        "'{}' expects a value argument",
                        $name
                    )));
                };
                let column = utf8_lit(&args.args, 1, "column")?;
                cast_store_value(&column, value, &self.inner.target)
            }
        }
    };
}

store_int_cast_impl!(StoreInt8Cast, STORE_INT8_NAME, DataType::Int8);
store_int_cast_impl!(StoreInt16Cast, STORE_INT16_NAME, DataType::Int16);
store_int_cast_impl!(StoreInt32Cast, STORE_INT32_NAME, DataType::Int32);
store_int_cast_impl!(StoreInt64Cast, STORE_INT64_NAME, DataType::Int64);

#[derive(Debug)]
struct StoreIntGuard {
    signature: Signature,
}

impl StoreIntGuard {
    fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl PartialEq for StoreIntGuard {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for StoreIntGuard {}

impl Hash for StoreIntGuard {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn guard_is_numeric_zero(value: &ScalarValue) -> bool {
    if value.is_null() {
        return false;
    }
    match ScalarValue::new_zero(&value.data_type()) {
        Ok(zero) => *value == zero,
        Err(_) => false,
    }
}

impl ScalarUDFImpl for StoreIntGuard {
    fn name(&self) -> &str {
        STORE_INT_GUARD_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{STORE_INT_GUARD_NAME}' expects a divisor argument"
            ))
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return Err(DataFusionError::Execution(format!(
                "'{STORE_INT_GUARD_NAME}' expects a divisor argument"
            )));
        };
        let column = utf8_lit(&args.args, 1, "column")?;
        let source = utf8_lit(&args.args, 2, "source type")?;
        let target = utf8_lit(&args.args, 3, "target type")?;
        let refuse = || store_overflow_error(&column, &source, &target);
        match first {
            ColumnarValue::Scalar(scalar) => {
                if guard_is_numeric_zero(scalar) {
                    return Err(refuse());
                }
                Ok(ColumnarValue::Scalar(scalar.clone()))
            }
            ColumnarValue::Array(array) => {
                for row in 0..array.len() {
                    if array.is_null(row) {
                        continue;
                    }
                    let scalar = ScalarValue::try_from_array(array.as_ref(), row)?;
                    if guard_is_numeric_zero(&scalar) {
                        return Err(refuse());
                    }
                }
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
        }
    }
}

pub(crate) fn store_int8_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StoreInt8Cast::new()))
}

pub(crate) fn store_int16_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StoreInt16Cast::new()))
}

pub(crate) fn store_int32_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StoreInt32Cast::new()))
}

pub(crate) fn store_int64_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StoreInt64Cast::new()))
}

pub(crate) fn store_int_guard_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StoreIntGuard::new()))
}

pub(crate) fn store_cast_udf_for_target(target: &DataType) -> Option<Arc<ScalarUDF>> {
    match target {
        DataType::Int8 => Some(store_int8_udf()),
        DataType::Int16 => Some(store_int16_udf()),
        DataType::Int32 => Some(store_int32_udf()),
        DataType::Int64 => Some(store_int64_udf()),
        _ => None,
    }
}

pub fn register_store_cast_udfs(ctx: &SessionContext) {
    ctx.register_udf(store_int8_udf().as_ref().clone());
    ctx.register_udf(store_int16_udf().as_ref().clone());
    ctx.register_udf(store_int32_udf().as_ref().clone());
    ctx.register_udf(store_int64_udf().as_ref().clone());
    ctx.register_udf(store_int_guard_udf().as_ref().clone());
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float32Array, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn type_names_follow_spark_spelling() {
        assert_eq!(
            spark_store_type_name(&DataType::Float32).as_deref(),
            Some("FLOAT")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Float64).as_deref(),
            Some("DOUBLE")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Decimal128(38, 0)).as_deref(),
            Some("DECIMAL(38,0)")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Decimal128(8, 6)).as_deref(),
            Some("DECIMAL(8,6)")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Int8).as_deref(),
            Some("TINYINT")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Int16).as_deref(),
            Some("SMALLINT")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Int32).as_deref(),
            Some("INT")
        );
        assert_eq!(
            spark_store_type_name(&DataType::Int64).as_deref(),
            Some("BIGINT")
        );
        assert_eq!(spark_store_type_name(&DataType::Utf8), None);
        assert_eq!(spark_store_type_name(&DataType::Boolean), None);
    }

    #[test]
    fn overflow_pairs_cover_float_and_decimal_sources_into_integers() {
        assert!(is_overflow_store_pair(&DataType::Float64, &DataType::Int64));
        assert!(is_overflow_store_pair(&DataType::Float32, &DataType::Int32));
        assert!(is_overflow_store_pair(
            &DataType::Decimal128(38, 0),
            &DataType::Int32
        ));
        assert!(is_overflow_store_pair(&DataType::Float64, &DataType::Int8));
        assert!(!is_overflow_store_pair(&DataType::Int64, &DataType::Int32));
        assert!(!is_overflow_store_pair(&DataType::Utf8, &DataType::Int32));
        assert!(!is_overflow_store_pair(
            &DataType::Float64,
            &DataType::Float64
        ));
        assert!(!is_overflow_store_pair(&DataType::Float64, &DataType::Utf8));
    }

    #[test]
    fn dictionary_sources_name_their_value_type() {
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64));
        assert_eq!(spark_store_type_name(&dict).as_deref(), Some("DOUBLE"));
        assert!(is_overflow_store_pair(&dict, &DataType::Int64));
    }

    #[test]
    fn overflow_errors_carry_sparks_condition_sqlstate_and_column() {
        let error = store_overflow_error("v", "DOUBLE", "BIGINT");
        assert_eq!(
            error.to_string(),
            "Execution error: [CAST_OVERFLOW_IN_TABLE_INSERT] Fail to assign a value of \
             \"DOUBLE\" type to the \"BIGINT\" type column or variable `v` due to an overflow. \
             Use `try_cast` on the input value to tolerate overflow and return NULL instead. \
             SQLSTATE: 22003"
        );
    }

    fn lit_utf8(text: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(text.to_string())))
    }

    fn invoke_cast(udf: &ScalarUDF, value: ColumnarValue, column: &str) -> Result<ColumnarValue> {
        let target = udf
            .return_type(&[DataType::Float64, DataType::Utf8])
            .expect("store udf types");
        let args = ScalarFunctionArgs {
            args: vec![value, lit_utf8(column)],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Float64, true)),
                Arc::new(Field::new("c", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", target, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        };
        udf.invoke_with_args(args)
    }

    #[test]
    fn checked_cast_stores_in_range_and_refuses_overflow() {
        let udf = store_int64_udf();
        let ok = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(42.0))),
            "v",
        )
        .expect("in range stores");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(42)))
        ));
        let error = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(1e19))),
            "v",
        )
        .expect_err("out of range refuses");
        assert!(
            error
                .to_string()
                .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{error}"
        );
        assert!(error.to_string().contains("SQLSTATE: 22003"), "{error}");
    }

    #[test]
    fn checked_cast_refuses_nan_and_infinity() {
        let udf = store_int64_udf();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = invoke_cast(
                &udf,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect_err("NaN and infinities refuse");
            assert!(
                error
                    .to_string()
                    .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
                "{value}: {error}"
            );
        }
    }

    #[test]
    fn checked_cast_passes_null_through() {
        let udf = store_int64_udf();
        let ok = invoke_cast(&udf, ColumnarValue::Scalar(ScalarValue::Float64(None)), "v")
            .expect("null passes");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int64(None))
        ));
    }

    #[test]
    fn checked_cast_names_decimal_sources() {
        let udf = store_int32_udf();
        let error = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10_i128.pow(30)), 38, 0)),
            "v",
        )
        .expect_err("decimal overflow refuses");
        assert!(error.to_string().contains("\"DECIMAL(38,0)\""), "{error}");
        assert!(error.to_string().contains("\"INT\""), "{error}");
    }

    #[test]
    fn checked_cast_arrays_fail_on_the_first_bad_row() {
        let udf = store_int64_udf();
        let array = Arc::new(Float64Array::from(vec![Some(1.0), Some(1e19)]));
        let error = invoke_cast(&udf, ColumnarValue::Array(array), "v").expect_err("arrays refuse");
        assert!(
            error
                .to_string()
                .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{error}"
        );
        let clean = Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0)]));
        let ok = invoke_cast(&udf, ColumnarValue::Array(clean), "v").expect("clean stores");
        let ColumnarValue::Array(out) = ok else {
            panic!("arrays stay arrays");
        };
        let ints = out.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!((ints.value(0), ints.value(1)), (1, 2));
    }

    #[test]
    fn exact_powers_of_two_at_the_int64_edge_store_like_spark() {
        let udf = store_int64_udf();
        for (value, stored) in [
            (9.223_372_036_854_776e18, i64::MAX),
            (-9.223_372_036_854_776e18, i64::MIN),
            (f64::from(2.0_f32.powi(63)), i64::MAX),
        ] {
            let ok = invoke_cast(
                &udf,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect("the edge stores");
            assert!(
                matches!(ok, ColumnarValue::Scalar(ScalarValue::Int64(Some(got))) if got == stored),
                "{value}: {ok:?}"
            );
        }
        let udf32 = store_int32_udf();
        let error = invoke_cast(
            &udf32,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(9.223_372_036_854_776e18))),
            "v",
        )
        .expect_err("2^63 refuses into INT");
        assert!(
            error
                .to_string()
                .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{error}"
        );
    }

    #[test]
    fn float32_sources_check_in_float64_like_spark() {
        let udf = store_int64_udf();
        let ok = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float32(Some(9.223_372e18))),
            "v",
        )
        .expect("f32 2^63 stores");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX)))
        ));
        let udf32 = store_int32_udf();
        let error = invoke_cast(
            &udf32,
            ColumnarValue::Scalar(ScalarValue::Float32(Some(3.402_823_5e38))),
            "v",
        )
        .expect_err("f32 MAX refuses into INT");
        assert!(error.to_string().contains("\"FLOAT\""), "{error}");
        assert!(error.to_string().contains("\"INT\""), "{error}");
    }

    #[test]
    fn decimals_refuse_exactly_with_no_float_edge() {
        let udf = store_int64_udf();
        let error = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Decimal128(
                Some(9_223_372_036_854_775_808),
                38,
                0,
            )),
            "v",
        )
        .expect_err("decimal 2^63 refuses");
        assert!(
            error
                .to_string()
                .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{error}"
        );
        let ok = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Decimal128(
                Some(9_223_372_036_854_775_807),
                38,
                0,
            )),
            "v",
        )
        .expect("decimal MAX stores");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(9_223_372_036_854_775_807)))
        ));
    }

    #[test]
    fn tiny_targets_refuse_like_spark() {
        let udf = store_int8_udf();
        let error = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(300.0))),
            "v",
        )
        .expect_err("300 refuses into TINYINT");
        assert!(error.to_string().contains("\"TINYINT\""), "{error}");
        let ok = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(42.0))),
            "v",
        )
        .expect("42 stores");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int8(Some(42)))
        ));
    }

    #[test]
    fn fractional_values_past_the_bound_store_the_truncated_bound() {
        let udf8 = store_int8_udf();
        for (value, stored) in [(127.5, 127i8), (127.999, 127), (-128.9, -128)] {
            let ok = invoke_cast(
                &udf8,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect("the truncated bound stores");
            assert!(
                matches!(ok, ColumnarValue::Scalar(ScalarValue::Int8(Some(got))) if got == stored),
                "{value}: {ok:?}"
            );
        }
        for value in [128.0, -129.0] {
            invoke_cast(
                &udf8,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect_err("the next whole value refuses");
        }
        let ok = invoke_cast(
            &udf8,
            ColumnarValue::Scalar(ScalarValue::Float32(Some(127.9_f32))),
            "v",
        )
        .expect("float32 truncates too");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int8(Some(127)))
        ));
        let udf16 = store_int16_udf();
        for (value, stored) in [(32767.9, 32767i16), (-32768.9, -32768)] {
            let ok = invoke_cast(
                &udf16,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect("the truncated bound stores");
            assert!(
                matches!(ok, ColumnarValue::Scalar(ScalarValue::Int16(Some(got))) if got == stored),
                "{value}: {ok:?}"
            );
        }
        for value in [32768.0, -32769.0] {
            invoke_cast(
                &udf16,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect_err("the next whole value refuses");
        }
        let udf32 = store_int32_udf();
        for (value, stored) in [
            (2_147_483_647.999_9, 2_147_483_647i32),
            (-2_147_483_648.9, -2_147_483_648),
        ] {
            let ok = invoke_cast(
                &udf32,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect("the truncated bound stores");
            assert!(
                matches!(ok, ColumnarValue::Scalar(ScalarValue::Int32(Some(got))) if got == stored),
                "{value}: {ok:?}"
            );
        }
        for value in [2_147_483_648.0, -2_147_483_649.0] {
            invoke_cast(
                &udf32,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                "v",
            )
            .expect_err("the next whole value refuses");
        }
        let ok = invoke_cast(
            &udf32,
            ColumnarValue::Scalar(ScalarValue::Float32(Some(2_147_483_520.0_f32))),
            "v",
        )
        .expect("float32 truncates too");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2_147_483_520)))
        ));
        let udf64 = store_int64_udf();
        let ok = invoke_cast(
            &udf64,
            ColumnarValue::Scalar(ScalarValue::Float32(Some(2.0_f32.powi(63)))),
            "v",
        )
        .expect("float32 2^63 stores");
        assert!(matches!(
            ok,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX)))
        ));
    }

    #[test]
    fn float_arrays_store_rowwise() {
        let udf = store_int32_udf();
        let array = Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-2.5)]));
        let ok = invoke_cast(&udf, ColumnarValue::Array(array), "v").expect("stores");
        let ColumnarValue::Array(out) = ok else {
            panic!("arrays stay arrays");
        };
        let ints = out
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("Int32");
        assert_eq!(
            (ints.value(0), ints.is_null(1), ints.value(2)),
            (1, true, -2)
        );
    }

    fn invoke_guard(
        udf: &ScalarUDF,
        divisor: ColumnarValue,
        divisor_type: DataType,
    ) -> Result<ColumnarValue> {
        let args = ScalarFunctionArgs {
            args: vec![
                divisor,
                lit_utf8("v"),
                lit_utf8("DOUBLE"),
                lit_utf8("BIGINT"),
            ],
            arg_fields: vec![
                Arc::new(Field::new("d", divisor_type, true)),
                Arc::new(Field::new("c", DataType::Utf8, true)),
                Arc::new(Field::new("s", DataType::Utf8, true)),
                Arc::new(Field::new("t", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Float64, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        };
        udf.invoke_with_args(args)
    }

    #[test]
    fn store_guard_refuses_zero_and_passes_the_rest() {
        let udf = store_int_guard_udf();
        let error = invoke_guard(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
            DataType::Float64,
        )
        .expect_err("zero refuses");
        assert!(
            error
                .to_string()
                .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{error}"
        );
        assert!(error.to_string().contains("`v`"), "{error}");
        for divisor in [
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Float64(None),
            ScalarValue::Int32(Some(0)),
        ] {
            let data_type = divisor.data_type();
            let result = invoke_guard(&udf, ColumnarValue::Scalar(divisor.clone()), data_type);
            if divisor == ScalarValue::Int32(Some(0)) {
                assert!(result.is_err(), "int zero refuses too");
            } else {
                assert!(result.is_ok(), "{divisor:?} passes");
            }
        }
    }
}
