use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use crate::write::store_assign::normalize_for_assignment;

pub(crate) const STORE_INT32_NAME: &str = "__repark_store_int32__";
pub(crate) const STORE_INT64_NAME: &str = "__repark_store_int64__";
pub(crate) const STORE_INT_GUARD_NAME: &str = "__repark_store_int_guard__";

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

pub(crate) fn is_overflow_store_pair(source: &DataType, target: &DataType) -> bool {
    let source = normalize_for_assignment(source);
    let float_or_decimal = matches!(
        source,
        DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
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

#[allow(clippy::missing_errors_doc)]
pub(crate) fn map_store_cast_error(
    column: &str,
    source: &DataType,
    target: &DataType,
    error: datafusion::arrow::error::ArrowError,
) -> DataFusionError {
    if is_overflow_store_pair(source, target)
        && let (Some(from), Some(to)) = (
            spark_store_type_name(source),
            spark_store_type_name(target),
        )
    {
        return store_overflow_error(column, &from, &to);
    }
    DataFusionError::ArrowError(Box::new(error), None)
}

fn strict_options() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn cast_store_value(
    column: &str,
    value: &ColumnarValue,
    target: &DataType,
) -> Result<ColumnarValue> {
    let source = value.data_type();
    match value {
        ColumnarValue::Scalar(scalar) => {
            if scalar.is_null() {
                let null = match target {
                    DataType::Int32 => ScalarValue::Int32(None),
                    DataType::Int64 => ScalarValue::Int64(None),
                    _ => ScalarValue::Null,
                };
                return Ok(ColumnarValue::Scalar(null));
            }
            let source_name = spark_store_type_name(&source);
            let target_name = spark_store_type_name(target);
            let array = scalar.to_array_of_size(1).map_err(DataFusionError::from)?;
            let casted = cast_with_options(&array, target, &strict_options()).map_err(|error| {
                match (source_name, target_name) {
                    (Some(from), Some(to)) if is_overflow_store_pair(&source, target) => {
                        store_overflow_error(column, &from, &to)
                    }
                    _ => DataFusionError::ArrowError(Box::new(error), None),
                }
            })?;
            Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&casted, 0).map_err(DataFusionError::from)?,
            ))
        }
        ColumnarValue::Array(array) => {
            let casted =
                cast_with_options(array.as_ref(), target, &strict_options()).map_err(|error| {
                    map_store_cast_error(column, &source, target, error)
                })?;
            Ok(ColumnarValue::Array(Arc::new(casted)))
        }
    }
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
            DataFusionError::Plan(format!("'{STORE_INT_GUARD_NAME}' expects a divisor argument"))
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
        DataType::Int32 => Some(store_int32_udf()),
        DataType::Int64 => Some(store_int64_udf()),
        _ => None,
    }
}

pub fn register_store_cast_udfs(ctx: &SessionContext) {
    ctx.register_udf(store_int32_udf().as_ref().clone());
    ctx.register_udf(store_int64_udf().as_ref().clone());
    ctx.register_udf(store_int_guard_udf().as_ref().clone());
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array};
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
        let dict = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Float64),
        );
        assert_eq!(spark_store_type_name(&dict).as_deref(), Some("DOUBLE"));
        assert!(is_overflow_store_pair(&dict, &DataType::Int64));
    }

    fn arrow_error(message: &str) -> datafusion::arrow::error::ArrowError {
        datafusion::arrow::error::ArrowError::CastError(message.to_string())
    }

    #[test]
    fn mapped_errors_carry_sparks_condition_sqlstate_and_column() {
        let error = map_store_cast_error(
            "v",
            &DataType::Float64,
            &DataType::Int64,
            arrow_error("Can't cast value 1e19 to type Int64"),
        );
        assert_eq!(
            error.to_string(),
            "Execution error: [CAST_OVERFLOW_IN_TABLE_INSERT] Fail to assign a value of \
             \"DOUBLE\" type to the \"BIGINT\" type column or variable `v` due to an overflow. \
             Use `try_cast` on the input value to tolerate overflow and return NULL instead. \
             SQLSTATE: 22003"
        );
    }

    #[test]
    fn non_overflow_pairs_keep_the_arrow_error() {
        for (source, target) in [
            (DataType::Int64, DataType::Int32),
            (DataType::Utf8, DataType::Int32),
            (DataType::Float64, DataType::Float64),
            (DataType::Utf8, DataType::Utf8),
        ] {
            let error = map_store_cast_error("v", &source, &target, arrow_error("boom"));
            assert!(
                error.to_string().contains("boom"),
                "{source:?} -> {target:?}: {error}"
            );
            assert!(
                !error.to_string().contains("CAST_OVERFLOW"),
                "{source:?} -> {target:?}: {error}"
            );
        }
    }

    fn lit_utf8(text: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(text.to_string())))
    }

    fn invoke_cast(
        udf: &ScalarUDF,
        value: ColumnarValue,
        column: &str,
    ) -> Result<ColumnarValue> {
        let args = ScalarFunctionArgs {
            args: vec![value, lit_utf8(column)],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Float64, true)),
                Arc::new(Field::new("c", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
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
            error.to_string().contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
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
                error.to_string().contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
                "{value}: {error}"
            );
        }
    }

    #[test]
    fn checked_cast_passes_null_through() {
        let udf = store_int64_udf();
        let ok = invoke_cast(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Float64(None)),
            "v",
        )
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
        assert!(
            error.to_string().contains("\"DECIMAL(38,0)\""),
            "{error}"
        );
        assert!(error.to_string().contains("\"INT\""), "{error}");
    }

    #[test]
    fn checked_cast_arrays_fail_on_the_first_bad_row() {
        let udf = store_int64_udf();
        let array = Arc::new(Float64Array::from(vec![Some(1.0), Some(1e19)]));
        let error = invoke_cast(&udf, ColumnarValue::Array(array), "v")
            .expect_err("arrays refuse");
        assert!(
            error.to_string().contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
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
            error.to_string().contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{error}"
        );
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
