use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[derive(Debug)]
struct JavaFormatFloat {
    signature: Signature,
}

impl JavaFormatFloat {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for JavaFormatFloat {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JavaFormatFloat {}

impl Hash for JavaFormatFloat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for JavaFormatFloat {
    crate::shim_udf_boilerplate!("__repark_format_float__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [format, DataType::Float32 | DataType::Float64] if is_string_type(format) => {
                Ok(DataType::Utf8)
            }
            _ => Err(DataFusionError::Plan(format!(
                "'{}' expects (STRING, FLOAT or DOUBLE)",
                self.name()
            ))),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .get(1)
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(format_arg) = args.args.first() else {
            return exec_err!("'{}' expects 2 arguments", self.name());
        };
        let Some(values_arg) = args.args.get(1) else {
            return exec_err!("'{}' expects 2 arguments", self.name());
        };
        let format_text = match format_arg {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(text)
                | ScalarValue::LargeUtf8(text)
                | ScalarValue::Utf8View(text),
            ) => text.clone(),
            _ => {
                return exec_err!("'{}' expects a literal format string", self.name());
            }
        };
        let row_count = match values_arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let Some(format_text) = format_text else {
            return Ok(ColumnarValue::Array(Arc::new(StringArray::new_null(
                row_count,
            ))));
        };
        let Some(spec) = parse_float_format(&format_text) else {
            return exec_err!("'{}' got an unsupported format string", self.name());
        };
        let rendered = match values_arg {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Float32 => array
                    .as_primitive::<Float32Type>()
                    .iter()
                    .map(|value| Some(render_maybe_null(&spec, value.map(f64::from))))
                    .collect::<Vec<_>>(),
                DataType::Float64 => array
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|value| Some(render_maybe_null(&spec, value)))
                    .collect::<Vec<_>>(),
                other => {
                    return exec_err!(
                        "'{}' expects a FLOAT or DOUBLE argument, got {other}",
                        self.name()
                    );
                }
            },
            ColumnarValue::Scalar(scalar) => vec![Some(render_scalar(&spec, scalar)?)],
        };
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(rendered))))
    }
}

pub(crate) fn java_format_float_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(JavaFormatFloat::new()))
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn render_maybe_null(spec: &FloatFormat, value: Option<f64>) -> String {
    match value {
        Some(value) => format_float_value(spec, value),
        None => "null".to_owned(),
    }
}

fn render_scalar(spec: &FloatFormat, scalar: &ScalarValue) -> Result<String> {
    match scalar {
        ScalarValue::Float32(value) => Ok(render_maybe_null(spec, (*value).map(f64::from))),
        ScalarValue::Float64(value) => Ok(render_maybe_null(spec, *value)),
        ScalarValue::Null => Ok("null".to_owned()),
        other => exec_err!("'__repark_format_float__' got unsupported scalar {other}"),
    }
}

pub(crate) struct FloatFormat {
    flags: u8,
    width: usize,
    precision: usize,
    upper: bool,
}

impl FloatFormat {
    const LEFT: u8 = 0x01;
    const PLUS: u8 = 0x02;
    const SPACE: u8 = 0x04;
    const ZERO: u8 = 0x08;
    const GROUPING: u8 = 0x10;
    const PAREN: u8 = 0x20;

    fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

pub(crate) fn parse_float_format(format: &str) -> Option<FloatFormat> {
    let body = format.strip_prefix('%')?;
    let bytes = body.as_bytes();
    let mut index = 0usize;
    let mut spec = FloatFormat {
        flags: 0,
        width: 0,
        precision: 6,
        upper: false,
    };
    while index < bytes.len() {
        match bytes[index] {
            b'-' => spec.flags |= FloatFormat::LEFT,
            b'+' => spec.flags |= FloatFormat::PLUS,
            b' ' => spec.flags |= FloatFormat::SPACE,
            b'0' => spec.flags |= FloatFormat::ZERO,
            b',' => spec.flags |= FloatFormat::GROUPING,
            b'(' => spec.flags |= FloatFormat::PAREN,
            b'#' => {}
            _ => break,
        }
        index += 1;
    }
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        spec.width = spec
            .width
            .saturating_mul(10)
            .saturating_add(usize::from(bytes[index] - b'0'));
        index += 1;
    }
    if index < bytes.len() && bytes[index] == b'.' {
        index += 1;
        let precision_start = index;
        spec.precision = 0;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            spec.precision = spec
                .precision
                .saturating_mul(10)
                .saturating_add(usize::from(bytes[index] - b'0'));
            index += 1;
        }
        if index == precision_start {
            return None;
        }
    }
    if index + 1 != bytes.len() {
        return None;
    }
    match bytes[index] {
        b'f' => {}
        b'F' => spec.upper = true,
        _ => return None,
    }
    Some(spec)
}

pub(crate) fn format_float_value(spec: &FloatFormat, value: f64) -> String {
    let mut prefix = String::new();
    let mut suffix = String::new();
    if value.is_sign_negative() {
        if spec.has(FloatFormat::PAREN) {
            prefix.push('(');
            suffix.push(')');
        } else {
            prefix.push('-');
        }
    } else if spec.has(FloatFormat::PLUS) {
        prefix.push('+');
    } else if spec.has(FloatFormat::SPACE) {
        prefix.push(' ');
    }
    let number = if value.is_finite() {
        let mut fixed = half_up_fixed(value.abs(), spec.precision);
        if spec.has(FloatFormat::GROUPING) {
            insert_grouping(&mut fixed);
        }
        fixed
    } else if value.is_infinite() {
        if spec.upper {
            "INFINITY".to_owned()
        } else {
            "Infinity".to_owned()
        }
    } else if spec.upper {
        "NAN".to_owned()
    } else {
        "NaN".to_owned()
    };
    if spec.has(FloatFormat::LEFT) {
        let mut full = prefix + &number + &suffix;
        while full.len() < spec.width {
            full.push(' ');
        }
        return full;
    }
    if spec.has(FloatFormat::ZERO) && value.is_finite() {
        while prefix.len() + number.len() + suffix.len() < spec.width {
            prefix.push('0');
        }
        return prefix + &number + &suffix;
    }
    let mut full = prefix + &number + &suffix;
    while full.len() < spec.width {
        full = " ".to_owned() + &full;
    }
    full
}

fn insert_grouping(fixed: &mut String) {
    let point = fixed.find('.').unwrap_or(fixed.len());
    let mut grouped = String::with_capacity(fixed.len() + point / 3);
    for (position, byte) in fixed.bytes().take(point).enumerate() {
        if position > 0 && (point - position).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(byte as char);
    }
    grouped.push_str(&fixed[point..]);
    *fixed = grouped;
}

#[allow(clippy::cast_possible_truncation)]
fn half_up_fixed(abs: f64, precision: usize) -> String {
    debug_assert!(abs.is_finite() && !abs.is_sign_negative());
    let bits = abs.to_bits();
    let raw_exp = ((bits >> 52) & 0x7FF) as i32;
    let mantissa = bits & 0xF_FFFF_FFFF_FFFF;
    let (significand, exp2) = if raw_exp == 0 {
        (mantissa, -1074)
    } else {
        (mantissa | 0x10_0000_0000_0000, raw_exp - 1075)
    };
    if exp2 >= 0 {
        let mut limbs = vec![significand];
        shift_left(&mut limbs, exp2.unsigned_abs());
        let mut int_text = decimal_text(&mut limbs);
        if precision > 0 {
            int_text.push('.');
            int_text.extend(std::iter::repeat_n('0', precision));
        }
        return int_text;
    }
    let shift = exp2.unsigned_abs();
    let int_part = if shift >= 64 { 0 } else { significand >> shift };
    let mut limbs = vec![significand];
    mask_bits(&mut limbs, shift);
    let mut digits = Vec::with_capacity(precision + 1);
    for _ in 0..=precision {
        multiply_small(&mut limbs, 10);
        digits.push(shifted_value(&limbs, shift) as u8);
        mask_bits(&mut limbs, shift);
    }
    let guard = digits.pop().unwrap_or(0);
    let mut int_text = int_part.to_string();
    if guard >= 5 {
        let mut position = digits.len();
        loop {
            if position == 0 {
                int_text = add_one_decimal(&int_text);
                break;
            }
            position -= 1;
            if digits[position] < 9 {
                digits[position] += 1;
                break;
            }
            digits[position] = 0;
        }
    }
    let mut out = int_text;
    if !digits.is_empty() {
        out.push('.');
        for digit in digits {
            out.push((b'0' + digit) as char);
        }
    }
    out
}

fn add_one_decimal(text: &str) -> String {
    let mut bytes = text.as_bytes().to_vec();
    let mut index = bytes.len();
    loop {
        if index == 0 {
            let mut out = String::with_capacity(bytes.len() + 1);
            out.push('1');
            out.extend(bytes.into_iter().map(|byte| byte as char));
            return out;
        }
        index -= 1;
        if bytes[index] < b'9' {
            bytes[index] += 1;
            break;
        }
        bytes[index] = b'0';
    }
    bytes.into_iter().map(|byte| byte as char).collect()
}

fn shift_left(limbs: &mut Vec<u64>, shift: u32) {
    if shift == 0 {
        return;
    }
    let words = (shift / 64) as usize;
    let bits = shift % 64;
    if words > 0 {
        let mut grown = vec![0u64; words];
        grown.append(limbs);
        *limbs = grown;
    }
    if bits == 0 {
        return;
    }
    let mut carry = 0u64;
    for limb in limbs.iter_mut() {
        let next = *limb >> (64 - bits);
        *limb = (*limb << bits) | carry;
        carry = next;
    }
    if carry > 0 {
        limbs.push(carry);
    }
}

#[allow(clippy::cast_possible_truncation)]
fn low64(wide: u128) -> u64 {
    wide as u64
}

fn multiply_small(limbs: &mut Vec<u64>, factor: u64) {
    let mut carry = 0u64;
    for limb in limbs.iter_mut() {
        let wide = u128::from(*limb) * u128::from(factor) + u128::from(carry);
        *limb = low64(wide);
        carry = low64(wide >> 64);
    }
    if carry > 0 {
        limbs.push(carry);
    }
}

fn shifted_value(limbs: &[u64], shift: u32) -> u64 {
    let word = (shift / 64) as usize;
    let bits = shift % 64;
    let low = limbs.get(word).copied().unwrap_or(0) >> bits;
    if bits == 0 {
        return low;
    }
    let high = limbs.get(word + 1).copied().unwrap_or(0) << (64 - bits);
    low | high
}

fn mask_bits(limbs: &mut Vec<u64>, bits: u32) {
    let words = (bits.div_ceil(64) as usize).max(1);
    limbs.truncate(words);
    if !bits.is_multiple_of(64)
        && let Some(top) = limbs.get_mut(words - 1)
    {
        *top &= (1u64 << (bits % 64)) - 1;
    }
    while limbs.len() > 1 && limbs.last() == Some(&0) {
        limbs.pop();
    }
}

fn decimal_text(limbs: &mut Vec<u64>) -> String {
    while limbs.len() > 1 && limbs.last() == Some(&0) {
        limbs.pop();
    }
    if limbs.len() == 1 && limbs[0] == 0 {
        return "0".to_owned();
    }
    let mut groups = Vec::new();
    while !(limbs.len() == 1 && limbs[0] == 0) {
        groups.push(divide_small(limbs, 1_000_000_000));
    }
    let mut out = String::new();
    let mut first = true;
    for group in groups.iter().rev() {
        if first {
            out.push_str(&group.to_string());
            first = false;
        } else {
            let text = group.to_string();
            for _ in text.len()..9 {
                out.push('0');
            }
            out.push_str(&text);
        }
    }
    out
}

fn divide_small(limbs: &mut Vec<u64>, divisor: u64) -> u64 {
    let mut remainder = 0u128;
    for limb in limbs.iter_mut().rev() {
        let wide = (remainder << 64) | u128::from(*limb);
        *limb = low64(wide / u128::from(divisor));
        remainder = wide % u128::from(divisor);
    }
    while limbs.len() > 1 && limbs.last() == Some(&0) {
        limbs.pop();
    }
    low64(remainder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(format: &str, value: f64) -> String {
        let spec = parse_float_format(format).unwrap();
        format_float_value(&spec, value)
    }

    #[test]
    fn corpus_cells_answer_half_up() {
        assert_eq!(render("%f", 10_000_000.0), "10000000.000000");
        assert_eq!(render("%.2f", 0.125), "0.13");
    }

    #[test]
    fn exact_ties_round_up_while_near_ties_follow_value() {
        assert_eq!(render("%.2f", 2.675), "2.67");
        assert_eq!(render("%.2f", 0.375), "0.38");
        assert_eq!(render("%.0f", 0.5), "1");
        assert_eq!(render("%.0f", 0.4), "0");
        assert_eq!(render("%.2f", -0.125), "-0.13");
        assert_eq!(render("%f", -0.0), "-0.000000");
    }

    #[test]
    fn large_and_tiny_values_expand_exactly() {
        assert_eq!(render("%.1f", 1.0e21), "1000000000000000000000.0");
        assert_eq!(render("%.5f", f64::from_bits(1)), "0.00000");
        assert_eq!(render("%.0f", 9_007_199_254_740_992.0), "9007199254740992");
        assert_eq!(render("%.3f", 0.1), "0.100");
    }

    #[test]
    fn float32_widens_before_rounding() {
        assert_eq!(render("%.10f", f64::from(0.1f32)), "0.1000000015");
        assert_eq!(render("%f", f64::from(0.1f32)), "0.100000");
    }

    #[test]
    fn flags_width_grouping_and_case_match_java() {
        assert_eq!(render("%+.2f", 0.125), "+0.13");
        assert_eq!(render("% .2f", 0.125), " 0.13");
        assert_eq!(render("%10.2f", 0.125), "      0.13");
        assert_eq!(render("%-10.2f", 0.125), "0.13      ");
        assert_eq!(render("%08.2f", 1.5), "00001.50");
        assert_eq!(render("%,.2f", 1_234_567.891), "1,234,567.89");
        assert_eq!(render("%(.2f", -0.125), "(0.13)");
        assert_eq!(render("%f", f64::INFINITY), "Infinity");
        assert_eq!(render("%f", f64::NEG_INFINITY), "-Infinity");
        assert_eq!(render("%F", f64::NAN), "NAN");
        assert_eq!(render("%f", f64::NAN), "NaN");
        assert_eq!(render("%f", f64::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn parser_accepts_only_single_float_conversions() {
        for format in ["%f", "%.2f", "%-+, (.19F", "%100.50f", "%#f"] {
            assert!(parse_float_format(format).is_some(), "{format}");
        }
        for format in [
            "%d", "%s", "%e", "%g", "a%f", "%f ", "%%f", "%*f", "%<f", "%.f", "%", "f", "",
        ] {
            assert!(parse_float_format(format).is_none(), "{format}");
        }
    }

    #[test]
    fn udf_invokes_over_arrays_and_scalars() {
        let udf = java_format_float_udf();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%.2f".to_owned()))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(0.125))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, false)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        };
        let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
            panic!("expected array");
        };
        let texts = array.as_string::<i32>();
        assert_eq!(texts.value(0), "0.13");
    }
}
