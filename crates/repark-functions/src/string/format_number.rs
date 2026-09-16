use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::spark_math::unexpected_input_type;

#[must_use]
pub fn format_number_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFormatNumber::new()))
}

#[must_use]
pub fn call_format_number(args: Vec<Expr>) -> Expr {
    crate::expr_fn::call(crate::string::format_number_udf(), args)
}

#[derive(Debug)]
struct SparkFormatNumber {
    signature: Signature,
}

impl SparkFormatNumber {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFormatNumber {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFormatNumber {}

impl Hash for SparkFormatNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_format_input(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
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

fn is_format_d(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) || data_type.is_numeric()
}

fn plan_format_number(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 2 {
        return exec_err!(
            "'format_number' expects (col, d), got {} argument(s)",
            arg_types.len()
        );
    }
    if !is_format_input(&arg_types[0]) {
        return Err(unexpected_input_type(
            "format_number",
            "DOUBLE",
            &arg_types[0],
        ));
    }
    if !is_format_d(&arg_types[1]) {
        return Err(unexpected_input_type("format_number", "INT", &arg_types[1]));
    }
    Ok(())
}

fn insert_grouping(integer: &str, size: usize) -> String {
    if integer.len() <= size || size == 0 {
        return integer.to_owned();
    }
    let mut out = String::with_capacity(integer.len() + integer.len() / size);
    let first = integer.len() % size;
    let (head, tail) = integer.split_at(first);
    out.push_str(head);
    for (index, group) in tail.as_bytes().chunks(size).enumerate() {
        if index > 0 || !head.is_empty() {
            out.push(',');
        }
        out.push_str(std::str::from_utf8(group).unwrap_or(""));
    }
    out
}

fn render_scaled(whole: &str, frac: &str, negative: bool, group: usize) -> String {
    let grouped = insert_grouping(whole, group);
    if frac.is_empty() {
        if negative {
            format!("-{grouped}")
        } else {
            grouped
        }
    } else if negative {
        format!("-{grouped}.{frac}")
    } else {
        format!("{grouped}.{frac}")
    }
}

fn round_decimal_str(digits: &str, scale: i32, decimals: usize) -> (String, String) {
    let stripped = digits.trim_start_matches('0');
    let digits = if stripped.is_empty() { "0" } else { stripped };
    let (whole_raw, frac_raw) = if scale <= 0 {
        let mut whole = digits.to_owned();
        for _ in 0..scale.checked_neg().unwrap_or(0).max(0) {
            whole.push('0');
        }
        (whole, String::new())
    } else {
        let width = usize::try_from(scale).unwrap_or(0);
        let mut padded = digits.to_owned();
        while padded.len() <= width {
            padded.insert(0, '0');
        }
        let (whole, frac) = padded.split_at(padded.len() - width);
        (whole.to_owned(), frac.to_owned())
    };
    if frac_raw.len() <= decimals {
        let mut frac = frac_raw;
        while frac.len() < decimals {
            frac.push('0');
        }
        let whole_trimmed = whole_raw.trim_start_matches('0');
        let whole_final = if whole_trimmed.is_empty() {
            "0".to_owned()
        } else {
            whole_trimmed.to_owned()
        };
        return (whole_final, frac);
    }
    let (keep, tail) = frac_raw.split_at(decimals);
    let joint = format!("{whole_raw}{keep}");
    let (rounded, _) = round_half_even_digits(&joint, tail);
    let (whole_out, frac_out) = rounded.split_at(rounded.len() - decimals);
    let frac_out = frac_out.to_owned();
    let whole_trimmed = whole_out.trim_start_matches('0');
    let whole_final = if whole_trimmed.is_empty() {
        "0".to_owned()
    } else {
        whole_trimmed.to_owned()
    };
    (whole_final, frac_out)
}

fn round_half_even_digits(joint: &str, tail: &str) -> (String, bool) {
    let mut digits: Vec<u8> = joint.bytes().map(|byte| byte - b'0').collect();
    if digits.is_empty() {
        digits.push(0);
    }
    let first_dropped = tail.bytes().next().map_or(0, |byte| byte - b'0');
    let rest_nonzero = tail.bytes().skip(1).any(|byte| byte != b'0');
    let round_up = first_dropped > 5
        || (first_dropped == 5
            && (rest_nonzero || digits.last().is_some_and(|last| last % 2 == 1)));
    if round_up {
        let mut index = digits.len();
        loop {
            if index == 0 {
                digits.insert(0, 1);
                break;
            }
            index -= 1;
            if digits[index] < 9 {
                digits[index] += 1;
                break;
            }
            digits[index] = 0;
        }
    }
    let text: String = digits.iter().map(|digit| (b'0' + digit) as char).collect();
    (text, round_up)
}

fn format_decimal_value(unscaled: i128, scale: i8, decimals: usize, negative: bool) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let (whole, frac) = round_decimal_str(&digits, i32::from(scale), decimals);
    render_scaled(&whole, &frac, negative, 3)
}

fn format_int_value(value: i128, negative: bool, decimals: usize) -> String {
    let digits = value.unsigned_abs().to_string();
    let (whole, frac) = round_decimal_str(&digits, 0, decimals);
    render_scaled(&whole, &frac, negative, 3)
}

fn format_double_value(value: f64, decimals: usize) -> Option<String> {
    if value.is_nan() {
        return Some("NaN".to_owned());
    }
    if value.is_infinite() {
        return Some(if value.is_sign_positive() {
            "∞".to_owned()
        } else {
            "-∞".to_owned()
        });
    }
    if decimals > 1000 {
        return None;
    }
    let text = format!("{value:.decimals$}");
    let (sign, rest) = text
        .strip_prefix('-')
        .map_or(("", text.as_str()), |tail| ("-", tail));
    let (whole, frac) = rest.split_once('.').unwrap_or((rest, ""));
    Some(format!("{sign}{}", render_scaled(whole, frac, false, 3)))
}

struct NumberPattern {
    prefix: String,
    suffix: String,
    group: usize,
    min_int: usize,
    min_frac: usize,
    max_frac: usize,
}

fn parse_pattern(pattern: &str) -> NumberPattern {
    let chars: Vec<char> = pattern.chars().collect();
    let mut first = chars.len();
    let mut last = 0;
    for (index, char) in chars.iter().enumerate() {
        if matches!(char, '#' | '0' | ',' | '.') {
            first = first.min(index);
            last = index;
        }
    }
    if first > last {
        return NumberPattern {
            prefix: pattern.to_owned(),
            suffix: String::new(),
            group: 3,
            min_int: 0,
            min_frac: 0,
            max_frac: 0,
        };
    }
    let prefix: String = chars[..first].iter().collect();
    let suffix: String = chars[last + 1..].iter().collect();
    let core: String = chars[first..=last].iter().collect();
    let (int_part, frac_part) = core.split_once('.').unwrap_or((core.as_str(), ""));
    let mut group = 0;
    if int_part.contains(',') {
        let tail = int_part.rsplit(',').next().unwrap_or("");
        group = tail
            .chars()
            .filter(|char| matches!(char, '#' | '0'))
            .count();
    }
    NumberPattern {
        prefix,
        suffix,
        group,
        min_int: int_part.chars().filter(|char| *char == '0').count(),
        min_frac: frac_part.chars().filter(|char| *char == '0').count(),
        max_frac: frac_part
            .chars()
            .filter(|char| matches!(char, '#' | '0'))
            .count(),
    }
}

impl ScalarUDFImpl for SparkFormatNumber {
    crate::shim_udf_boilerplate!("format_number");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        plan_format_number(&declared)?;
        Ok(Arc::new(Field::new("format_number", DataType::Utf8, true)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() != 2 {
            return exec_err!(
                "'format_number' expects (col, d), got {} argument(s)",
                args.len()
            );
        }
        let parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        Ok(format!("format_number({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_format_number(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            number_rows,
            ..
        } = args;
        if arg_values.len() != 2 {
            return exec_err!(
                "'format_number' expects (col, d), got {} argument(s)",
                arg_values.len()
            );
        }
        let values = arg_values[0].to_array(number_rows)?;
        let dials = arg_values[1].to_array(number_rows)?;
        let mut out = StringBuilder::new();
        for row in 0..number_rows {
            if values.is_null(row) || dials.is_null(row) {
                out.append_null();
                continue;
            }
            match render_cell(&values, &dials, row)? {
                Some(text) => out.append_value(text),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn render_cell(values: &ArrayRef, dials: &ArrayRef, row: usize) -> Result<Option<String>> {
    match dials.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let patterns = cast(dials.as_ref(), &DataType::Utf8)?;
            let patterns = patterns
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs utf8 patterns".to_owned(),
                    )
                })?;
            Ok(Some(render_with_pattern(
                values,
                row,
                &parse_pattern(patterns.value(row)),
            )?))
        }
        _ => {
            let shaped = cast(dials.as_ref(), &DataType::Int32)?;
            let scales = shaped
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs an integral d".to_owned(),
                    )
                })?;
            let scale = scales.value(row);
            if scale < 0 {
                return Ok(None);
            }
            let decimals = usize::try_from(scale).unwrap_or(0);
            Ok(Some(render_with_scale(values, row, decimals)?))
        }
    }
}

fn render_float_scale(values: &ArrayRef, row: usize, decimals: usize) -> Result<String> {
    let value = match values.data_type() {
        DataType::Float32 => f64::from(
            values
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs float values".to_owned(),
                    )
                })?
                .value(row),
        ),
        _ => values
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "format_number needs float values".to_owned(),
                )
            })?
            .value(row),
    };
    format_double_value(value, decimals)
        .ok_or_else(|| unexpected_input_type("format_number", "DOUBLE", values.data_type()))
}

fn render_with_scale(values: &ArrayRef, row: usize, decimals: usize) -> Result<String> {
    match values.data_type() {
        DataType::Float32 | DataType::Float64 => render_float_scale(values, row, decimals),
        DataType::Decimal128(_, scale) => {
            let decimals_array = values
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs decimal values".to_owned(),
                    )
                })?;
            let unscaled = decimals_array.value(row);
            Ok(format_decimal_value(
                unscaled,
                *scale,
                decimals,
                unscaled < 0,
            ))
        }
        DataType::Int8 => {
            let value = values
                .as_primitive::<datafusion::arrow::datatypes::Int8Type>()
                .value(row);
            Ok(format_int_value(i128::from(value), value < 0, decimals))
        }
        DataType::Int16 => {
            let value = values
                .as_primitive::<datafusion::arrow::datatypes::Int16Type>()
                .value(row);
            Ok(format_int_value(i128::from(value), value < 0, decimals))
        }
        DataType::Int32 => {
            let value = values
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .value(row);
            Ok(format_int_value(i128::from(value), value < 0, decimals))
        }
        DataType::Int64 => {
            let value = values
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(row);
            Ok(format_int_value(i128::from(value), value < 0, decimals))
        }
        DataType::UInt8 => Ok(format_int_value(
            i128::from(
                values
                    .as_primitive::<datafusion::arrow::datatypes::UInt8Type>()
                    .value(row),
            ),
            false,
            decimals,
        )),
        DataType::UInt16 => Ok(format_int_value(
            i128::from(
                values
                    .as_primitive::<datafusion::arrow::datatypes::UInt16Type>()
                    .value(row),
            ),
            false,
            decimals,
        )),
        DataType::UInt32 => Ok(format_int_value(
            i128::from(
                values
                    .as_primitive::<datafusion::arrow::datatypes::UInt32Type>()
                    .value(row),
            ),
            false,
            decimals,
        )),
        DataType::UInt64 => {
            let value = values
                .as_primitive::<datafusion::arrow::datatypes::UInt64Type>()
                .value(row);
            Ok(format_int_value(i128::from(value), false, decimals))
        }
        other => exec_err!("'format_number' does not support {other}"),
    }
}

fn render_with_pattern(values: &ArrayRef, row: usize, parsed: &NumberPattern) -> Result<String> {
    match values.data_type() {
        DataType::Float32 => {
            let floats = values
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs float values".to_owned(),
                    )
                })?;
            let value = f64::from(floats.value(row));
            Ok(render_double_pattern(value, parsed))
        }
        DataType::Float64 => {
            let floats = values
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs float values".to_owned(),
                    )
                })?;
            Ok(render_double_pattern(floats.value(row), parsed))
        }
        DataType::Decimal128(_, scale) => {
            let decimals_array = values
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs decimal values".to_owned(),
                    )
                })?;
            let unscaled = decimals_array.value(row);
            Ok(render_decimal_pattern(
                unscaled,
                i32::from(*scale),
                unscaled < 0,
                parsed,
            ))
        }
        _ => {
            let shaped = cast(values.as_ref(), &DataType::Int64)?;
            let ints = shaped
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "format_number needs integral values".to_owned(),
                    )
                })?;
            let value = ints.value(row);
            Ok(render_decimal_pattern(
                i128::from(value),
                0,
                value < 0,
                parsed,
            ))
        }
    }
}

fn pad_frac(frac: &str, min_frac: usize) -> String {
    let mut out = frac.to_owned();
    while out.len() < min_frac {
        out.push('0');
    }
    out
}

fn pad_int(whole: &str, min_int: usize) -> String {
    if whole.len() >= min_int {
        return whole.to_owned();
    }
    let mut out = String::new();
    for _ in 0..(min_int - whole.len()) {
        out.push('0');
    }
    out.push_str(whole);
    out
}

fn render_decimal_pattern(
    unscaled: i128,
    scale: i32,
    negative: bool,
    parsed: &NumberPattern,
) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let (whole, frac) = round_decimal_str(&digits, scale, parsed.max_frac);
    let frac = pad_frac(frac.trim_end_matches('0'), parsed.min_frac);
    let whole = pad_int(&whole, parsed.min_int.max(1));
    let grouped = if parsed.group == 0 {
        whole
    } else {
        insert_grouping(&whole, parsed.group)
    };
    let body = if frac.is_empty() && parsed.max_frac == 0 {
        grouped
    } else {
        format!("{grouped}.{frac}")
    };
    format!(
        "{}{}{}",
        parsed.prefix,
        if negative { format!("-{body}") } else { body },
        parsed.suffix
    )
}

fn render_double_pattern(value: f64, parsed: &NumberPattern) -> String {
    if value.is_nan() {
        return format!("{}NaN{}", parsed.prefix, parsed.suffix);
    }
    if value.is_infinite() {
        let symbol = if value.is_sign_positive() {
            "∞"
        } else {
            "-∞"
        };
        return format!("{}{}{}", parsed.prefix, symbol, parsed.suffix);
    }
    if parsed.max_frac > 1000 {
        return format!("{}{}{}", parsed.prefix, value, parsed.suffix);
    }
    let text = format!("{value:.max_frac$}", max_frac = parsed.max_frac);
    let (sign, rest) = text
        .strip_prefix('-')
        .map_or(("", text.as_str()), |tail| ("-", tail));
    let (whole, frac) = rest.split_once('.').unwrap_or((rest, ""));
    let frac = pad_frac(frac.trim_end_matches('0'), parsed.min_frac);
    let whole = pad_int(whole, parsed.min_int.max(1));
    let grouped = if parsed.group == 0 {
        whole
    } else {
        insert_grouping(&whole, parsed.group)
    };
    let body = if frac.is_empty() && parsed.max_frac == 0 {
        grouped
    } else {
        format!("{grouped}.{frac}")
    };
    format!("{}{sign}{body}{}", parsed.prefix, parsed.suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn strings_of(ctx: &SessionContext, sql: &str) -> Vec<Option<String>> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let mut out = Vec::new();
        for batch in &batches {
            out.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string output")
                    .iter()
                    .map(|cell| cell.map(str::to_owned)),
            );
        }
        out
    }

    #[tokio::test]
    async fn format_number_grouping_shapes() {
        let ctx = ctx();
        assert_eq!(
            strings_of(&ctx, "SELECT format_number(CAST(2.5 AS DOUBLE), 2)").await,
            vec![Some("2.50".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT format_number(CAST(0.125 AS DOUBLE), 2)").await,
            vec![Some("0.12".to_owned())]
        );
        assert_eq!(
            strings_of(
                &ctx,
                "SELECT format_number(CAST(12345.6789 AS DECIMAL(10,4)), 0)"
            )
            .await,
            vec![Some("12,346".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT format_number(CAST(-0.5 AS DECIMAL(10,4)), 0)").await,
            vec![Some("-0".to_owned())]
        );
        assert_eq!(
            strings_of(
                &ctx,
                "SELECT format_number(CAST(12345.6789 AS DECIMAL(10,4)), 3)"
            )
            .await,
            vec![Some("12,345.679".to_owned())]
        );
        assert_eq!(
            strings_of(
                &ctx,
                "SELECT format_number(CAST(1234567.891 AS DOUBLE), '#,###.##')"
            )
            .await,
            vec![Some("1,234,567.89".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT format_number(12345, 1)").await,
            vec![Some("12,345.0".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT format_number(1, -1)").await,
            vec![None]
        );
    }
}
