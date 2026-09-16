use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::spark_math::unexpected_input_type;

#[must_use]
pub fn conv_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkConv::new()))
}

#[must_use]
pub fn call_conv(args: Vec<Expr>) -> Expr {
    crate::expr_fn::call(crate::spark_math::conv_udf(), args)
}

#[derive(Debug)]
struct SparkConv {
    signature: Signature,
}

impl SparkConv {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkConv {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkConv {}

impl Hash for SparkConv {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_conv_input(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) || data_type.is_numeric()
}

fn is_conv_base(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn plan_conv(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 3 {
        return exec_err!(
            "'conv' expects (col, fromBase, toBase), got {} argument(s)",
            arg_types.len()
        );
    }
    if !is_conv_input(&arg_types[0]) {
        return Err(unexpected_input_type("conv", "STRING", &arg_types[0]));
    }
    if !is_conv_base(&arg_types[1]) {
        return Err(unexpected_input_type("conv", "INT", &arg_types[1]));
    }
    if !is_conv_base(&arg_types[2]) {
        return Err(unexpected_input_type("conv", "INT", &arg_types[2]));
    }
    Ok(())
}

fn digit_value(byte: u8) -> Option<u64> {
    if byte.is_ascii_digit() {
        Some(u64::from(byte - b'0'))
    } else if byte.is_ascii_lowercase() {
        Some(u64::from(byte - b'a') + 10)
    } else if byte.is_ascii_uppercase() {
        Some(u64::from(byte - b'A') + 10)
    } else {
        None
    }
}

fn render_unsigned(mut value: u64, base: u32, out: &mut String) {
    if value == 0 {
        out.push('0');
        return;
    }
    let mut digits = [0u8; 64];
    let mut len = 0;
    while value > 0 {
        let digit = (value % u64::from(base)) as u8;
        digits[len] = if digit < 10 {
            b'0' + digit
        } else {
            b'A' + digit - 10
        };
        len += 1;
        value /= u64::from(base);
    }
    for index in (0..len).rev() {
        out.push(digits[index] as char);
    }
}

fn convert_row(text: &str, from_base: i32, to_base: i32, ansi: bool) -> Result<Option<String>> {
    let to_magnitude = to_base.unsigned_abs();
    if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_magnitude) {
        return Ok(None);
    }
    let bytes = text.as_bytes();
    let mut cursor = 0;
    let mut negative = false;
    if bytes.first() == Some(&b'-') {
        negative = true;
        cursor = 1;
    } else if bytes.first() == Some(&b'+') {
        cursor = 1;
    }
    let mut magnitude: u64 = 0;
    let mut overflowed = false;
    while cursor < bytes.len() {
        let Some(digit) = digit_value(bytes[cursor]) else {
            break;
        };
        if digit >= from_base as u64 {
            break;
        }
        match magnitude
            .checked_mul(from_base as u64)
            .and_then(|scaled| scaled.checked_add(digit))
        {
            Some(next) => magnitude = next,
            None => {
                overflowed = true;
                break;
            }
        }
        cursor += 1;
    }
    if overflowed {
        if ansi {
            return Err(crate::spark_math::overflow_error("conv"));
        }
        magnitude = u64::MAX;
    }
    let unsigned = if negative {
        0u64.wrapping_sub(magnitude)
    } else {
        magnitude
    };
    let mut rendered = String::new();
    if to_base < 0 {
        let signed = unsigned as i64;
        if signed < 0 {
            rendered.push('-');
            render_unsigned(signed.unsigned_abs(), to_magnitude, &mut rendered);
        } else {
            render_unsigned(unsigned, to_magnitude, &mut rendered);
        }
    } else {
        render_unsigned(unsigned, to_magnitude, &mut rendered);
    }
    Ok(Some(rendered))
}

impl ScalarUDFImpl for SparkConv {
    crate::shim_udf_boilerplate!("conv");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        plan_conv(&declared)?;
        Ok(Arc::new(Field::new("conv", DataType::Utf8, true)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() != 3 {
            return exec_err!(
                "'conv' expects (col, fromBase, toBase), got {} argument(s)",
                args.len()
            );
        }
        let parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        Ok(format!("conv({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_conv(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            number_rows,
            config_options,
            ..
        } = args;
        if arg_values.len() != 3 {
            return exec_err!(
                "'conv' expects (col, fromBase, toBase), got {} argument(s)",
                arg_values.len()
            );
        }
        let texts = shaped_utf8(&arg_values[0].to_array(number_rows)?)?;
        let from_col = shaped_int32(&arg_values[1].to_array(number_rows)?)?;
        let to_col = shaped_int32(&arg_values[2].to_array(number_rows)?)?;
        let ansi = crate::ansi::spark_ansi_enabled_from_options(config_options.as_ref());
        let mut out = StringBuilder::new();
        for row in 0..number_rows {
            if texts.is_null(row) || from_col.is_null(row) || to_col.is_null(row) {
                out.append_null();
                continue;
            }
            match convert_row(
                texts.value(row),
                from_col.value(row),
                to_col.value(row),
                ansi,
            )? {
                Some(rendered) => out.append_value(rendered),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn shaped_utf8(array: &ArrayRef) -> Result<StringArray> {
    if array.data_type() == &DataType::Utf8 {
        return array
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("conv needs utf8 values".to_owned())
            });
    }
    let shaped = cast(array.as_ref(), &DataType::Utf8)?;
    shaped
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution("conv needs utf8 values".to_owned())
        })
}

fn shaped_int32(array: &ArrayRef) -> Result<Int32Array> {
    if array.data_type() == &DataType::Int32 {
        return array
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("conv needs int32 bases".to_owned())
            });
    }
    let shaped = cast(array.as_ref(), &DataType::Int32)?;
    shaped
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution("conv needs int32 bases".to_owned())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    async fn values_of(ctx: &SessionContext, sql: &str) -> Vec<Option<String>> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output")
            .iter()
            .map(|cell| cell.map(str::to_owned))
            .collect()
    }

    #[tokio::test]
    async fn conv_number_converter_shapes() {
        use datafusion::prelude::SessionConfig;
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), false);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        assert_eq!(
            values_of(&ctx, "SELECT conv('100', 10, 16)").await,
            vec![Some("64".to_owned())]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('-10', 10, 16)").await,
            vec![Some("FFFFFFFFFFFFFFF6".to_owned())]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('ff', 16, 10)").await,
            vec![Some("255".to_owned())]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('zz', 36, 10)").await,
            vec![Some("1295".to_owned())]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('100', 10, 37)").await,
            vec![None]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('9', 2, 10)").await,
            vec![Some("0".to_owned())]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('-17', 10, -16)").await,
            vec![Some("-11".to_owned())]
        );
        assert_eq!(
            values_of(&ctx, "SELECT conv('18446744073709551616', 10, 10)").await,
            vec![Some("18446744073709551615".to_owned())]
        );
    }

    #[tokio::test]
    async fn conv_overflow_raises_under_ansi() {
        use datafusion::prelude::SessionConfig;
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), true);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        let error = ctx
            .sql("SELECT conv('18446744073709551616', 10, 10)")
            .await
            .expect("plan conv overflow")
            .collect()
            .await
            .expect_err("ANSI conv overflow must raise");
        assert!(error.to_string().contains("ARITHMETIC_OVERFLOW"), "{error}");
    }
}
