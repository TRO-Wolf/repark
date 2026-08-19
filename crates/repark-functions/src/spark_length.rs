//! Spark `bit_length` / `octet_length` — stringifies non-binary inputs (G5).
//!
//! Spark 4.1.2 `bit_length` / `octet_length` accept STRING and BINARY, and
//! stringify every other type (`bit_length(12)` is `16`, `bit_length(true)` is
//! `32`). DataFusion's kernels are Utf8/Binary-exact, so an int/bool/float
//! column fails. This shim coerces non-binary to `Utf8` then counts **bytes**
//! (Spark `octet_length`) or `8 * bytes` (`bit_length`). `Binary` /
//! `LargeBinary` / `BinaryView` / `FixedSizeBinary` pass through so `unhex`
//! payloads stay bytes, not a stringified dump.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Field, FieldRef,
};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// ===========================================================================================
/// Spark `bit_length` UDF (overwrites DataFusion's Utf8/Binary-exact kernel).
/// ===========================================================================================
#[must_use]
pub fn bit_length_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkBitLength::new()))
}

/// ===========================================================================================
/// Spark `octet_length` UDF (overwrites DataFusion's Utf8/Binary-exact kernel).
/// ===========================================================================================
#[must_use]
pub fn octet_length_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkOctetLength::new()))
}

#[derive(Debug)]
struct SparkBitLength {
    signature: Signature,
}

impl SparkBitLength {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkBitLength {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkBitLength {}

impl Hash for SparkBitLength {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkBitLength {
    crate::shim_udf_boilerplate!("bit_length");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            "bit_length",
            DataType::Int32,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_length_arg(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_length(&args, LengthKind::Bits)
    }
}

#[derive(Debug)]
struct SparkOctetLength {
    signature: Signature,
}

impl SparkOctetLength {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkOctetLength {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkOctetLength {}

impl Hash for SparkOctetLength {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkOctetLength {
    crate::shim_udf_boilerplate!("octet_length");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            "octet_length",
            DataType::Int32,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_length_arg(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_length(&args, LengthKind::Octets)
    }
}

#[derive(Clone, Copy)]
enum LengthKind {
    Bits,
    Octets,
}

fn is_nested_container(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => true,
        DataType::Dictionary(_, value_type) => is_nested_container(value_type),
        _ => false,
    }
}

fn coerce_length_arg(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let Some(data_type) = arg_types.first() else {
        return exec_err!("bit_length/octet_length expects 1 argument");
    };
    if is_nested_container(data_type) {
        return Err(DataFusionError::Plan(format!(
            "bit_length/octet_length requires STRING or BINARY, got {data_type}"
        )));
    }
    let coerced = match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => data_type.clone(),
        _ => DataType::Utf8,
    };
    Ok(vec![coerced])
}

fn invoke_length(args: &ScalarFunctionArgs, kind: LengthKind) -> Result<ColumnarValue> {
    let Some(arg) = args.args.first() else {
        return exec_err!("bit_length/octet_length expects 1 argument");
    };
    let array = arg.to_array(args.number_rows)?;
    let bytes = byte_lengths(array.as_ref())?;
    let values = match kind {
        LengthKind::Octets => bytes,
        LengthKind::Bits => bit_lengths_from_octets(&bytes)?,
    };
    Ok(ColumnarValue::Array(Arc::new(values)))
}

fn bit_lengths_from_octets(bytes: &Int32Array) -> Result<Int32Array> {
    let mut values = Vec::with_capacity(bytes.len());
    for index in 0..bytes.len() {
        if bytes.is_null(index) {
            values.push(None);
            continue;
        }
        let octets = bytes.value(index);
        let Some(bits) = octets.checked_mul(8) else {
            return exec_err!("bit_length exceeds Spark INT");
        };
        values.push(Some(bits));
    }
    Ok(values.into_iter().collect())
}

fn byte_lengths(array: &dyn Array) -> Result<Int32Array> {
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => utf8_byte_lengths(array),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            binary_byte_lengths(array)
        }
        DataType::FixedSizeBinary(width) => {
            let values: Int32Array = (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(*width)
                    }
                })
                .collect();
            Ok(values)
        }
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => decimal_byte_lengths(array, *scale),
        _ => {
            let as_utf8: ArrayRef = cast(array, &DataType::Utf8)?;
            utf8_byte_lengths(as_utf8.as_ref())
        }
    }
}

/// Spark `CAST(decimal AS STRING)`: keep scale padding (`12.50` not `12.5`).
fn format_unscaled_i128(unscaled: i128, scale: i8) -> String {
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    if scale <= 0 {
        let extra = match scale.checked_neg() {
            Some(zeros) => usize::try_from(zeros).unwrap_or(0),
            None => 0,
        };
        let mut body = digits;
        body.extend(std::iter::repeat_n('0', extra));
        if negative { format!("-{body}") } else { body }
    } else {
        let scale_n = usize::try_from(scale).unwrap_or(0);
        let mut padded = digits;
        if padded.len() <= scale_n {
            padded = format!("{padded:0>width$}", width = scale_n + 1);
        }
        let split_at = padded.len().saturating_sub(scale_n);
        let (integer_part, fraction) = padded.split_at(split_at);
        if negative {
            format!("-{integer_part}.{fraction}")
        } else {
            format!("{integer_part}.{fraction}")
        }
    }
}

fn decimal_byte_lengths(array: &dyn Array, scale: i8) -> Result<Int32Array> {
    let mut values = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        if array.is_null(index) {
            values.push(None);
            continue;
        }
        let formatted = match array.data_type() {
            DataType::Decimal32(_, _) => {
                let unscaled = i128::from(array.as_primitive::<Decimal32Type>().value(index));
                format_unscaled_i128(unscaled, scale)
            }
            DataType::Decimal64(_, _) => {
                let unscaled = i128::from(array.as_primitive::<Decimal64Type>().value(index));
                format_unscaled_i128(unscaled, scale)
            }
            DataType::Decimal128(_, _) => {
                let unscaled = array.as_primitive::<Decimal128Type>().value(index);
                format_unscaled_i128(unscaled, scale)
            }
            DataType::Decimal256(_, _) => {
                let wide = array.as_primitive::<Decimal256Type>().value(index);
                match wide.to_i128() {
                    Some(unscaled) => format_unscaled_i128(unscaled, scale),
                    None => {
                        return exec_err!("decimal256 value does not fit Spark STRING length path");
                    }
                }
            }
            other => {
                return exec_err!("decimal_byte_lengths on non-decimal {other}");
            }
        };
        values.push(Some(spark_int_len(formatted.len())?));
    }
    Ok(values.into_iter().collect())
}

fn spark_int_len(bytes: usize) -> Result<i32> {
    i32::try_from(bytes).map_err(|_| {
        datafusion::common::DataFusionError::Execution("octet_length exceeds Spark INT".to_owned())
    })
}

fn utf8_byte_lengths(array: &dyn Array) -> Result<Int32Array> {
    let mut values = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        if array.is_null(index) {
            values.push(None);
            continue;
        }
        let bytes = match array.data_type() {
            DataType::Utf8 => array.as_string::<i32>().value(index).len(),
            DataType::LargeUtf8 => array.as_string::<i64>().value(index).len(),
            DataType::Utf8View => array.as_string_view().value(index).len(),
            _ => {
                values.push(None);
                continue;
            }
        };
        values.push(Some(spark_int_len(bytes)?));
    }
    Ok(values.into_iter().collect())
}

fn binary_byte_lengths(array: &dyn Array) -> Result<Int32Array> {
    let mut values = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        if array.is_null(index) {
            values.push(None);
            continue;
        }
        let bytes = match array.data_type() {
            DataType::Binary => array.as_binary::<i32>().value(index).len(),
            DataType::LargeBinary => array.as_binary::<i64>().value(index).len(),
            DataType::BinaryView => array.as_binary_view().value(index).len(),
            _ => {
                values.push(None);
                continue;
            }
        };
        values.push(Some(spark_int_len(bytes)?));
    }
    Ok(values.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(bit_length_udf().as_ref().clone());
        ctx.register_udf(octet_length_udf().as_ref().clone());
        ctx
    }

    async fn one_i32(ctx: &SessionContext, sql: &str) -> Option<i32> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|err| panic!("plan {sql}: {err}"))
            .collect()
            .await
            .unwrap_or_else(|err| panic!("exec {sql}: {err}"));
        let array = batches[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        if array.is_null(0) {
            None
        } else {
            Some(array.value(0))
        }
    }

    #[tokio::test]
    async fn string_and_unicode_match_spark() {
        let ctx = ctx();
        assert_eq!(one_i32(&ctx, "SELECT octet_length('ab')").await, Some(2));
        assert_eq!(one_i32(&ctx, "SELECT bit_length('ab')").await, Some(16));
        assert_eq!(one_i32(&ctx, "SELECT octet_length('🐈')").await, Some(4));
        assert_eq!(one_i32(&ctx, "SELECT bit_length('🐈')").await, Some(32));
    }

    #[tokio::test]
    async fn stringify_int_and_bool_match_spark() {
        let ctx = ctx();
        // Spark: octet_length(12)=2 ("12"), bit_length(true)=32 ("true").
        assert_eq!(one_i32(&ctx, "SELECT octet_length(12)").await, Some(2));
        assert_eq!(one_i32(&ctx, "SELECT bit_length(12)").await, Some(16));
        assert_eq!(one_i32(&ctx, "SELECT octet_length(true)").await, Some(4));
        assert_eq!(one_i32(&ctx, "SELECT bit_length(true)").await, Some(32));
        assert_eq!(one_i32(&ctx, "SELECT octet_length(1.5)").await, Some(3));
        assert_eq!(one_i32(&ctx, "SELECT bit_length(1.5)").await, Some(24));
    }

    #[test]
    fn spark_int_overflow_is_fail_loud() {
        assert!(spark_int_len((i32::MAX as usize) + 1).is_err());
        let too_many_octets = Int32Array::from(vec![Some(i32::MAX)]);
        assert!(bit_lengths_from_octets(&too_many_octets).is_err());
    }

    #[tokio::test]
    async fn nested_container_is_fail_loud() {
        let ctx = ctx();
        for sql in ["SELECT bit_length([1, 2])", "SELECT octet_length([1, 2])"] {
            let result = ctx.sql(sql).await;
            assert!(
                result.is_err(),
                "Spark refuses nested input: {sql} -> {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn decimal_preserves_scale_padding() {
        let ctx = ctx();
        // Spark 4.1.2: CAST(12.50 AS DECIMAL(4,2)) → '12.50' → octet 5 / bit 40.
        assert_eq!(
            one_i32(&ctx, "SELECT octet_length(CAST(12.50 AS DECIMAL(4, 2)))").await,
            Some(5)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT bit_length(CAST(12.50 AS DECIMAL(4, 2)))").await,
            Some(40)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT octet_length(CAST(1.2 AS DECIMAL(5, 2)))").await,
            Some(4)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT octet_length(CAST(-12.50 AS DECIMAL(5, 2)))").await,
            Some(6)
        );
    }

    #[test]
    fn format_unscaled_matches_spark_cast_string() {
        assert_eq!(format_unscaled_i128(1250, 2), "12.50");
        assert_eq!(format_unscaled_i128(120, 2), "1.20");
        assert_eq!(format_unscaled_i128(50, 2), "0.50");
        assert_eq!(format_unscaled_i128(-1250, 2), "-12.50");
        assert_eq!(format_unscaled_i128(12, 0), "12");
    }

    #[tokio::test]
    async fn null_in_null_out() {
        let ctx = ctx();
        assert_eq!(
            one_i32(&ctx, "SELECT bit_length(CAST(NULL AS VARCHAR))").await,
            None
        );
        assert_eq!(
            one_i32(&ctx, "SELECT octet_length(CAST(NULL AS VARCHAR))").await,
            None
        );
    }
}
