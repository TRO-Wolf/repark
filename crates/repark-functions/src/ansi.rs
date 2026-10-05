//! Spark-door `spark.sql.ansi.enabled` carrier and ANSI `/0` / `% 0` raise kernel.

use std::any::Any;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type, i256,
};
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionConfig;

/// Canonical Spark `SQLConf` key.
pub const SPARK_SQL_ANSI_ENABLED_KEY: &str = "spark.sql.ansi.enabled";

/// Spark 4 / owner Q10=A default.
pub const DEFAULT_SPARK_SQL_ANSI_ENABLED: bool = true;

/// Embedded UDF name.
pub(crate) const ANSI_NONZERO_DIVISOR_NAME: &str = "__repark_ansi_nonzero_divisor__";

/// Session-scoped ANSI flag the Spark analyzer reads out of [`ConfigOptions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkAnsiConfig {
    /// `true` → `/0` and `% 0` raise; `false` → NULL (legacy `nullif` wrap).
    pub enabled: bool,
}

impl Default for SparkAnsiConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_SPARK_SQL_ANSI_ENABLED,
        }
    }
}

impl ConfigExtension for SparkAnsiConfig {
    /// Two segments keep the carrier unreachable through `SET`.
    const PREFIX: &'static str = "repark.ansi";
}

impl ExtensionOptions for SparkAnsiConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// Refuse because the knob is set on the session builder.
    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: ANSI mode is set with \
             `{SPARK_SQL_ANSI_ENABLED_KEY}` on the session builder; change it at runtime with \
             `SET spark.sql.ansi.enabled`",
            Self::PREFIX
        )))
    }

    /// Keep the carrier out of `SET` listings.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// Parse `spark.sql.ansi.enabled`.
/// # Errors
/// A present value that is not a boolean token.
pub fn parse_spark_sql_ansi_enabled(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \"{SPARK_SQL_ANSI_ENABLED_KEY}\" is invalid. \
             {SPARK_SQL_ANSI_ENABLED_KEY} should be boolean, but was {raw}"
        ))),
    }
}

/// Read the builder conf map.
/// # Errors
/// Present but unparsable value (the `notabool` fail-loud).
pub fn spark_ansi_from_config_map<S>(config: &HashMap<String, String, S>) -> Result<bool>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_ANSI_ENABLED_KEY) {
        Some(raw) => parse_spark_sql_ansi_enabled(raw),
        None => Ok(DEFAULT_SPARK_SQL_ANSI_ENABLED),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_runtime_spark_sql_ansi_enabled(raw: &str) -> Result<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{raw}' in the config \
             \"{SPARK_SQL_ANSI_ENABLED_KEY}\" is invalid. It should be a/an 'boolean' value. \
             SQLSTATE: 22022"
        )))
    }
}

/// Attach the ANSI flag to a [`SessionConfig`] (Spark door `configure` hook).
#[must_use]
pub fn with_spark_ansi_config(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(SparkAnsiConfig { enabled })
}

/// Analyzer accessor.
#[must_use]
pub fn spark_ansi_enabled_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SparkAnsiConfig>()
        .map_or(DEFAULT_SPARK_SQL_ANSI_ENABLED, |extension| {
            extension.enabled
        })
}

/// Wrap `divisor` in the embedded raise-on-zero UDF (ANSI ON path of `guard_zero_divisor`).
#[must_use]
pub fn guard_nonzero_divisor(divisor: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        ansi_nonzero_divisor_udf(),
        vec![divisor],
    ))
}

/// The embedded UDF instance.
#[must_use]
pub fn ansi_nonzero_divisor_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(AnsiNonzeroDivisor::new()))
}

/// Pass-through numeric kernel: zero → Spark-shaped `DIVIDE_BY_ZERO`; NULL / nonzero pass.
#[derive(Debug)]
struct AnsiNonzeroDivisor {
    signature: Signature,
}

impl AnsiNonzeroDivisor {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for AnsiNonzeroDivisor {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for AnsiNonzeroDivisor {}

impl Hash for AnsiNonzeroDivisor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for AnsiNonzeroDivisor {
    crate::shim_udf_boilerplate!("__repark_ansi_nonzero_divisor__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let data_type = arg_types.first().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{ANSI_NONZERO_DIVISOR_NAME}' expects one numeric argument"
            ))
        })?;
        if !data_type.is_numeric() {
            return Err(DataFusionError::Plan(format!(
                "'{ANSI_NONZERO_DIVISOR_NAME}' expects a numeric argument, got {data_type}"
            )));
        }
        Ok(data_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return Err(DataFusionError::Execution(format!(
                "'{ANSI_NONZERO_DIVISOR_NAME}' expects one argument"
            )));
        };
        match first {
            ColumnarValue::Scalar(scalar) => {
                refuse_if_numeric_zero(scalar)?;
                Ok(ColumnarValue::Scalar(scalar.clone()))
            }
            ColumnarValue::Array(array) => {
                refuse_if_array_has_numeric_zero(array)?;
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
        }
    }
}

fn refuse_if_array_has_numeric_zero(array: &ArrayRef) -> Result<()> {
    let has_zero = match array.data_type() {
        DataType::Int8 => primitive_has_zero(array.as_primitive::<Int8Type>(), |value| value == 0),
        DataType::Int16 => {
            primitive_has_zero(array.as_primitive::<Int16Type>(), |value| value == 0)
        }
        DataType::Int32 => {
            primitive_has_zero(array.as_primitive::<Int32Type>(), |value| value == 0)
        }
        DataType::Int64 => {
            primitive_has_zero(array.as_primitive::<Int64Type>(), |value| value == 0)
        }
        DataType::UInt8 => {
            primitive_has_zero(array.as_primitive::<UInt8Type>(), |value| value == 0)
        }
        DataType::UInt16 => {
            primitive_has_zero(array.as_primitive::<UInt16Type>(), |value| value == 0)
        }
        DataType::UInt32 => {
            primitive_has_zero(array.as_primitive::<UInt32Type>(), |value| value == 0)
        }
        DataType::UInt64 => {
            primitive_has_zero(array.as_primitive::<UInt64Type>(), |value| value == 0)
        }
        DataType::Float16 => primitive_has_zero(array.as_primitive::<Float16Type>(), |value| {
            value.to_bits() == 0
        }),
        DataType::Float32 => primitive_has_zero(array.as_primitive::<Float32Type>(), |value| {
            value.to_bits() == 0
        }),
        DataType::Float64 => primitive_has_zero(array.as_primitive::<Float64Type>(), |value| {
            value.to_bits() == 0
        }),
        DataType::Decimal32(_, _) => {
            primitive_has_zero(array.as_primitive::<Decimal32Type>(), |value| value == 0)
        }
        DataType::Decimal64(_, _) => {
            primitive_has_zero(array.as_primitive::<Decimal64Type>(), |value| value == 0)
        }
        DataType::Decimal128(_, _) => {
            primitive_has_zero(array.as_primitive::<Decimal128Type>(), |value| value == 0)
        }
        DataType::Decimal256(_, _) => {
            primitive_has_zero(array.as_primitive::<Decimal256Type>(), |value| {
                value == i256::ZERO
            })
        }
        _ => return refuse_if_rows_have_numeric_zero(array),
    };
    if has_zero {
        return Err(divide_by_zero_error());
    }
    Ok(())
}

fn primitive_has_zero<T, F>(array: &PrimitiveArray<T>, is_zero: F) -> bool
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> bool,
{
    let values = array.values();
    if array.null_count() == 0 {
        return values.iter().any(|value| is_zero(*value));
    }
    if let Some(nulls) = array.nulls() {
        return values
            .iter()
            .enumerate()
            .any(|(index, value)| !nulls.is_null(index) && is_zero(*value));
    }
    values.iter().any(|value| is_zero(*value))
}

fn refuse_if_rows_have_numeric_zero(array: &ArrayRef) -> Result<()> {
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let scalar = ScalarValue::try_from_array(array.as_ref(), row)?;
        refuse_if_numeric_zero(&scalar)?;
    }
    Ok(())
}

fn refuse_if_numeric_zero(value: &ScalarValue) -> Result<()> {
    if is_numeric_zero(value) {
        return Err(divide_by_zero_error());
    }
    Ok(())
}

fn is_numeric_zero(value: &ScalarValue) -> bool {
    if value.is_null() {
        return false;
    }
    match ScalarValue::new_zero(&value.data_type()) {
        Ok(zero) => *value == zero,
        Err(_) => false,
    }
}

fn divide_by_zero_error() -> DataFusionError {
    DataFusionError::Execution(
        "[DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate divisor being 0 \
         and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
         to bypass this error. (ArithmeticException)"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::compute::cast;

    #[test]
    fn parse_accepts_boolean_tokens() {
        assert!(parse_spark_sql_ansi_enabled("true").unwrap());
        assert!(parse_spark_sql_ansi_enabled("TRUE").unwrap());
        assert!(parse_spark_sql_ansi_enabled("1").unwrap());
        assert!(!parse_spark_sql_ansi_enabled("false").unwrap());
        assert!(!parse_spark_sql_ansi_enabled("FALSE").unwrap());
        assert!(!parse_spark_sql_ansi_enabled("0").unwrap());
    }

    #[test]
    fn runtime_parse_accepts_only_true_false_case_insensitive() {
        assert!(parse_runtime_spark_sql_ansi_enabled("true").unwrap());
        assert!(parse_runtime_spark_sql_ansi_enabled("TRUE").unwrap());
        assert!(parse_runtime_spark_sql_ansi_enabled("True").unwrap());
        assert!(!parse_runtime_spark_sql_ansi_enabled("false").unwrap());
        assert!(!parse_runtime_spark_sql_ansi_enabled("FALSE").unwrap());
    }

    #[test]
    fn runtime_parse_refuses_1_yes_and_padded_with_sparks_message() {
        for raw in ["1", "yes", "0", "no", " true ", "maybe"] {
            let error = parse_runtime_spark_sql_ansi_enabled(raw)
                .expect_err("runtime must refuse the value");
            let message = error.to_string();
            assert!(
                message.contains("[INVALID_CONF_VALUE.TYPE_MISMATCH]"),
                "refusal must carry Spark's class: {message}"
            );
            assert!(
                message.contains(SPARK_SQL_ANSI_ENABLED_KEY),
                "refusal must name the key: {message}"
            );
            assert!(
                message.contains("SQLSTATE: 22022"),
                "refusal must carry the SQLSTATE: {message}"
            );
        }
    }

    #[test]
    fn parse_notabool_names_the_spark_needle() {
        let error = parse_spark_sql_ansi_enabled("notabool")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("should be boolean, but was notabool"),
            "Spark message needle missing: {error}"
        );
        assert!(
            error.contains(SPARK_SQL_ANSI_ENABLED_KEY),
            "error must name the key: {error}"
        );
    }

    #[test]
    fn missing_map_key_defaults_true() {
        let config = HashMap::<String, String>::new();
        assert!(spark_ansi_from_config_map(&config).unwrap());
    }

    #[test]
    fn map_false_is_legacy() {
        let mut config = HashMap::new();
        config.insert(SPARK_SQL_ANSI_ENABLED_KEY.to_string(), "false".to_string());
        assert!(!spark_ansi_from_config_map(&config).unwrap());
    }

    #[test]
    fn missing_extension_defaults_true() {
        let options = ConfigOptions::new();
        assert!(spark_ansi_enabled_from_options(&options));
    }

    #[test]
    fn installed_false_is_readable() {
        let config = with_spark_ansi_config(SessionConfig::new(), false);
        assert!(!spark_ansi_enabled_from_options(config.options()));
    }

    fn oracle_refuse_if_rows_have_numeric_zero(array: &ArrayRef) -> Result<()> {
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let scalar = ScalarValue::try_from_array(array.as_ref(), row)?;
            refuse_if_numeric_zero(&scalar)?;
        }
        Ok(())
    }

    fn assert_matches_scalar_path(array: &ArrayRef) {
        let expected =
            oracle_refuse_if_rows_have_numeric_zero(array).map_err(|error| error.to_string());
        let actual = refuse_if_array_has_numeric_zero(array).map_err(|error| error.to_string());
        assert_eq!(
            actual,
            expected,
            "typed scan must match the scalar path for {}",
            array.data_type()
        );
    }

    fn assert_refuses_with_divide_by_zero(array: &ArrayRef) {
        let error = refuse_if_array_has_numeric_zero(array).expect_err("zero divisor must refuse");
        assert!(
            error.to_string().contains("DIVIDE_BY_ZERO"),
            "refusal must carry the Spark class: {error}"
        );
    }

    fn float16_array(values: Vec<Option<f32>>) -> ArrayRef {
        let source = Float32Array::from(values);
        cast(&source, &DataType::Float16).expect("f32 casts to f16")
    }

    fn decimal32_array(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(
            Decimal32Array::from(values)
                .with_precision_and_scale(9, 2)
                .expect("decimal32 precision fits"),
        )
    }

    fn decimal64_array(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(
            Decimal64Array::from(values)
                .with_precision_and_scale(18, 4)
                .expect("decimal64 precision fits"),
        )
    }

    fn decimal128_array(values: Vec<Option<i128>>) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 9)
                .expect("decimal128 precision fits"),
        )
    }

    fn decimal256_array(values: Vec<Option<i256>>) -> ArrayRef {
        Arc::new(
            Decimal256Array::from(values)
                .with_precision_and_scale(76, 10)
                .expect("decimal256 precision fits"),
        )
    }

    #[test]
    fn ansi_guard_vectorised_matches_scalar_path() {
        let refuse: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![1i8, 0, -3])),
            Arc::new(Int16Array::from(vec![1i16, 0, -3])),
            Arc::new(Int32Array::from(vec![1i32, 0, -3])),
            Arc::new(Int64Array::from(vec![1i64, 0, -3])),
            Arc::new(UInt8Array::from(vec![1u8, 0, 3])),
            Arc::new(UInt16Array::from(vec![1u16, 0, 3])),
            Arc::new(UInt32Array::from(vec![1u32, 0, 3])),
            Arc::new(UInt64Array::from(vec![1u64, 0, 3])),
            Arc::new(Float32Array::from(vec![1.5f32, 0.0, -3.25])),
            Arc::new(Float64Array::from(vec![1.5f64, 0.0, -3.25])),
            float16_array(vec![Some(1.5f32), Some(0.0), Some(-3.25)]),
            decimal32_array(vec![Some(1i32), Some(0), Some(-3)]),
            decimal64_array(vec![Some(1i64), Some(0), Some(-3)]),
            decimal128_array(vec![Some(1i128), Some(0), Some(-3)]),
            decimal256_array(vec![
                Some(i256::from_i128(1)),
                Some(i256::ZERO),
                Some(i256::from_i128(-3)),
            ]),
        ];
        for array in &refuse {
            assert_refuses_with_divide_by_zero(array);
            assert_matches_scalar_path(array);
        }
        let pass: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(1i8), None, Some(-3)])),
            Arc::new(Int16Array::from(vec![Some(1i16), None, Some(-3)])),
            Arc::new(Int32Array::from(vec![Some(1i32), None, Some(-3)])),
            Arc::new(Int64Array::from(vec![Some(1i64), None, Some(-3)])),
            Arc::new(UInt8Array::from(vec![Some(1u8), None, Some(3)])),
            Arc::new(UInt16Array::from(vec![Some(1u16), None, Some(3)])),
            Arc::new(UInt32Array::from(vec![Some(1u32), None, Some(3)])),
            Arc::new(UInt64Array::from(vec![Some(1u64), None, Some(3)])),
            Arc::new(Float32Array::from(vec![
                Some(1.5f32),
                None,
                Some(f32::NAN),
                Some(-3.25),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.5f64),
                None,
                Some(f64::NAN),
                Some(-3.25),
            ])),
            float16_array(vec![Some(1.5f32), None, Some(f32::NAN), Some(-3.25)]),
            decimal32_array(vec![Some(1i32), None, Some(-3)]),
            decimal64_array(vec![Some(1i64), None, Some(-3)]),
            decimal128_array(vec![Some(1i128), None, Some(-3)]),
            decimal256_array(vec![
                Some(i256::from_i128(1)),
                None,
                Some(i256::from_i128(-3)),
            ]),
            Arc::new(Int32Array::from(vec![None, None])),
            Arc::new(Float64Array::from(vec![None, None])),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ];
        for array in &pass {
            refuse_if_array_has_numeric_zero(array).expect("nonzero divisor must pass");
            assert_matches_scalar_path(array);
        }
        let negative_zero: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![-0.0f32])),
            Arc::new(Float64Array::from(vec![-0.0f64])),
            float16_array(vec![Some(-0.0f32)]),
        ];
        for array in &negative_zero {
            refuse_if_array_has_numeric_zero(array).expect("negative zero passes, as today");
            assert_matches_scalar_path(array);
        }
    }

    #[test]
    fn ansi_guard_nulls_masked() {
        let masked = Int64Array::from(vec![Some(5i64), None]);
        assert_eq!(
            masked.values()[1],
            0,
            "the null slot must hold zero for the pin to bite"
        );
        let array: ArrayRef = Arc::new(masked);
        refuse_if_array_has_numeric_zero(&array).expect("zero under a null bit passes");
        assert_matches_scalar_path(&array);
        let sliced = array.slice(1, 1);
        refuse_if_array_has_numeric_zero(&sliced).expect("zero under a null bit passes in a slice");
        assert_matches_scalar_path(&sliced);
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(2.5f64)]));
        assert_eq!(
            floats.as_primitive::<Float64Type>().values()[0].to_bits(),
            0u64,
            "the null slot must hold zero for the pin to bite"
        );
        refuse_if_array_has_numeric_zero(&floats).expect("zero under a null bit passes");
        assert_matches_scalar_path(&floats);
        let float_slice = floats.slice(0, 1);
        refuse_if_array_has_numeric_zero(&float_slice)
            .expect("zero under a null bit passes in a slice");
        assert_matches_scalar_path(&float_slice);
        let real_zero: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.0f64), None]));
        assert_refuses_with_divide_by_zero(&real_zero);
        assert_matches_scalar_path(&real_zero);
    }

    #[test]
    fn ansi_guard_sliced_array_offsets() {
        let base: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 5, 7]));
        let after_zero = base.slice(1, 2);
        refuse_if_array_has_numeric_zero(&after_zero).expect("zero before the slice passes");
        assert_matches_scalar_path(&after_zero);
        let only_zero = base.slice(0, 1);
        assert_refuses_with_divide_by_zero(&only_zero);
        assert_matches_scalar_path(&only_zero);
        let tail: ArrayRef = Arc::new(Int64Array::from(vec![5i64, 7, 0]));
        let before_zero = tail.slice(0, 2);
        refuse_if_array_has_numeric_zero(&before_zero).expect("zero after the slice passes");
        assert_matches_scalar_path(&before_zero);
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.0f64, 0.0, 2.0]));
        let head = floats.slice(0, 1);
        refuse_if_array_has_numeric_zero(&head).expect("zero after the slice passes");
        assert_matches_scalar_path(&head);
        let middle = floats.slice(1, 1);
        assert_refuses_with_divide_by_zero(&middle);
        assert_matches_scalar_path(&middle);
    }

    #[test]
    fn ansi_guard_dictionary_falls_back_to_scalar_path() {
        let keys = Int32Array::from(vec![0i32, 1, 0]);
        let clean: ArrayRef = Arc::new(DictionaryArray::new(
            keys,
            Arc::new(Int64Array::from(vec![4i64, 5])),
        ));
        refuse_if_array_has_numeric_zero(&clean).expect("nonzero dictionary passes");
        assert_matches_scalar_path(&clean);
        let zero_keys = Int32Array::from(vec![0i32, 1, 0]);
        let zeroed: ArrayRef = Arc::new(DictionaryArray::new(
            zero_keys,
            Arc::new(Int64Array::from(vec![0i64, 5])),
        ));
        refuse_if_array_has_numeric_zero(&zeroed).expect("dictionary with a zero passes, as today");
        assert_matches_scalar_path(&zeroed);
    }
}
