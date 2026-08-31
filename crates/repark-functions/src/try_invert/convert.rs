//! `try_to_date` / `try_to_number` / `try_to_binary` / `try_to_time`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{Array, BinaryBuilder, Date32Array, Decimal128Builder, StringArray};
use datafusion::arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn try_to_date_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryToDate::new()))
}

#[must_use]
pub fn try_to_number_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryToNumber::new()))
}

#[must_use]
pub fn try_to_binary_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryToBinary::new()))
}

#[must_use]
pub fn try_to_time_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryToTime::new()))
}

fn utf8_signature() -> Signature {
    Signature::user_defined(Volatility::Immutable)
}

macro_rules! named_udf {
    ($type_name:ident, $name_literal:literal) => {
        #[derive(Debug)]
        struct $type_name {
            signature: Signature,
        }

        impl $type_name {
            fn new() -> Self {
                Self {
                    signature: utf8_signature(),
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
    };
}

named_udf!(SparkTryToDate, "try_to_date");
named_udf!(SparkTryToNumber, "try_to_number");
named_udf!(SparkTryToBinary, "try_to_binary");
named_udf!(SparkTryToTime, "try_to_time");

fn string_array(array: &dyn Array) -> Result<&StringArray> {
    array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("try_* expected Utf8, got {}", array.data_type()))
    })
}

impl ScalarUDFImpl for SparkTryToDate {
    crate::shim_udf_boilerplate!("try_to_date");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Date32, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            1 => Ok(vec![DataType::Utf8]),
            2 => Ok(vec![DataType::Utf8, DataType::Utf8]),
            n => exec_err!("'try_to_date' expects 1 or 2 arguments, got {n}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array(arrays[0].as_ref())?;
        let format = arrays
            .get(1)
            .map(|array| string_array(array.as_ref()))
            .transpose()?;
        let mut builder = Date32Array::builder(input.len());
        for row in 0..input.len() {
            if input.is_null(row) {
                builder.append_null();
                continue;
            }
            let text = input.value(row);
            if let Some(format_array) = format {
                if format_array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let pattern = format_array.value(row);
                refuse_illegal_datetime_pattern(pattern)?;
                match parse_with_java_pattern(text, pattern) {
                    Some(date) => builder.append_value(Date32Type::from_naive_date(date)),
                    None => builder.append_null(),
                }
            } else {
                match parse_default_date(text) {
                    Some(date) => builder.append_value(Date32Type::from_naive_date(date)),
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn parse_default_date(text: &str) -> Option<NaiveDate> {
    let trimmed = text.trim();
    let date_part = trimmed.split(['T', ' ']).next().unwrap_or(trimmed);
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(date_part, "%Y/%m/%d"))
        .ok()
}

const LEGAL_DATETIME_LETTERS: &[u8] = b"GyYuQqMLwdDFeEcahHkKmsSAVnNzZOXx";

fn refuse_illegal_datetime_pattern(pattern: &str) -> Result<()> {
    let mut in_quote = false;
    for byte in pattern.bytes() {
        if byte == b'\'' {
            in_quote = !in_quote;
            continue;
        }
        if in_quote {
            continue;
        }
        if byte.is_ascii_alphabetic() && !LEGAL_DATETIME_LETTERS.contains(&byte) {
            return Err(DataFusionError::Execution(format!(
                "[INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER] Unrecognized datetime pattern: \
                 {pattern}. Illegal pattern character found in datetime pattern: {}. Please \
                 provide legal character. SQLSTATE: 22007",
                byte as char
            )));
        }
    }
    Ok(())
}

fn parse_with_java_pattern(text: &str, pattern: &str) -> Option<NaiveDate> {
    let chrono_pattern = java_date_pattern_to_chrono(pattern)?;
    NaiveDate::parse_from_str(text, &chrono_pattern).ok()
}

fn java_date_pattern_to_chrono(pattern: &str) -> Option<String> {
    let mut out = String::new();
    let bytes = pattern.as_bytes();
    let mut index = 0;
    let mut in_quote = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\'' {
            in_quote = !in_quote;
            index += 1;
            continue;
        }
        if in_quote {
            out.push(byte as char);
            index += 1;
            continue;
        }
        if !byte.is_ascii_alphabetic() {
            out.push(byte as char);
            index += 1;
            continue;
        }
        let mut run = 1;
        while index + run < bytes.len() && bytes[index + run] == byte {
            run += 1;
        }
        match (byte, run) {
            (b'y' | b'u', 2) => out.push_str("%y"),
            (b'y' | b'u', _) => out.push_str("%Y"),
            (b'M', 2) => out.push_str("%m"),
            (b'M', 1) => out.push_str("%-m"),
            (b'd', 2) => out.push_str("%d"),
            (b'd', 1) => out.push_str("%-d"),
            (b'H', 2) => out.push_str("%H"),
            (b'H', 1) => out.push_str("%-H"),
            (b'm', 2) => out.push_str("%M"),
            (b'm', 1) => out.push_str("%-M"),
            (b's', 2) => out.push_str("%S"),
            (b's', 1) => out.push_str("%-S"),
            _ => return None,
        }
        index += run;
    }
    Some(out)
}

impl ScalarUDFImpl for SparkTryToNumber {
    crate::shim_udf_boilerplate!("try_to_number");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(DataType::Utf8) = arg_types.get(1) else {
            return Ok(DataType::Decimal128(38, 0));
        };
        Ok(DataType::Decimal128(38, 0))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let format = args.scalar_arguments.get(1).and_then(|value| *value);
        let (precision, scale) = match format {
            Some(
                ScalarValue::Utf8(Some(pattern))
                | ScalarValue::LargeUtf8(Some(pattern))
                | ScalarValue::Utf8View(Some(pattern)),
            ) => parse_number_format(pattern)?.decimal,
            _ => (38, 0),
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(precision, scale),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "'try_to_number' expects (expr, format), got {} argument(s)",
                arg_types.len()
            );
        }
        Ok(vec![DataType::Utf8, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array(arrays[0].as_ref())?;
        let format = string_array(arrays[1].as_ref())?;
        let DataType::Decimal128(precision, scale) = *args.return_field.data_type() else {
            return exec_err!("try_to_number promised Decimal128");
        };
        let mut builder = Decimal128Builder::with_capacity(input.len())
            .with_data_type(DataType::Decimal128(precision, scale));
        for row in 0..input.len() {
            if input.is_null(row) || format.is_null(row) {
                builder.append_null();
                continue;
            }
            let parsed = parse_number_format(format.value(row))?;
            match apply_number_format(input.value(row), &parsed) {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

struct NumberFormat {
    decimal: (u8, i8),
    has_dollar: bool,
    grouping: Option<char>,
    decimal_sep: Option<char>,
}

fn parse_number_format(pattern: &str) -> Result<NumberFormat> {
    let upper = pattern.to_ascii_uppercase();
    if !upper.bytes().any(|byte| byte == b'9' || byte == b'0') {
        return Err(DataFusionError::Plan(format!(
            "[INVALID_FORMAT.WRONG_NUM_DIGIT] The format is invalid: '{upper}'. The format \
             string requires at least one number digit. SQLSTATE: 42601"
        )));
    }
    let mut has_dollar = false;
    let mut grouping = None;
    let mut decimal_sep = None;
    let mut precision: u8 = 0;
    let mut scale: i8 = 0;
    let mut in_scale = false;
    let mut index = 0;
    let bytes = upper.as_bytes();
    while index < bytes.len() {
        match bytes[index] {
            b'M' if bytes.get(index + 1) == Some(&b'I') => index += 2,
            b'P' if bytes.get(index + 1) == Some(&b'R') => index += 2,
            b'S' => index += 1,
            b'$' => {
                has_dollar = true;
                index += 1;
            }
            b'9' | b'0' => {
                precision = precision.saturating_add(1);
                if in_scale {
                    scale = scale.saturating_add(1);
                }
                index += 1;
            }
            b',' | b'G' => {
                grouping = Some(if bytes[index] == b',' { ',' } else { 'G' });
                index += 1;
            }
            b'.' | b'D' => {
                if decimal_sep.is_some() {
                    return unexpected_format_token(pattern, bytes[index] as char);
                }
                decimal_sep = Some(if bytes[index] == b'.' { '.' } else { 'D' });
                in_scale = true;
                index += 1;
            }
            other => return unexpected_format_token(pattern, other as char),
        }
    }
    if precision == 0 {
        return Err(DataFusionError::Plan(format!(
            "[INVALID_FORMAT.WRONG_NUM_DIGIT] The format is invalid: '{upper}'. The format \
             string requires at least one number digit. SQLSTATE: 42601"
        )));
    }
    Ok(NumberFormat {
        decimal: (precision, scale),
        has_dollar,
        grouping,
        decimal_sep,
    })
}

fn unexpected_format_token(pattern: &str, token: char) -> Result<NumberFormat> {
    Err(DataFusionError::Plan(format!(
        "[INVALID_FORMAT.UNEXPECTED_TOKEN] The format is invalid: '{}'. Found the unexpected \
         character '{token}' in the format string; the structure of the format string must \
         match: `[MI|S]` `[$]` `[0|9|G|,]*` `[.|D]` `[0|9]*` `[$]` `[PR|MI|S]`. SQLSTATE: 42601",
        pattern.to_ascii_uppercase()
    )))
}

fn apply_number_format(input: &str, format: &NumberFormat) -> Option<i128> {
    let mut text = input.trim().to_string();
    if format.has_dollar {
        if let Some(stripped) = text.strip_prefix('$') {
            text = stripped.to_string();
        } else if let Some(stripped) = text.strip_suffix('$') {
            text = stripped.to_string();
        } else {
            return None;
        }
        text = text.trim().to_string();
    }
    if let Some(',') = format.grouping {
        text.retain(|ch| ch != ',');
    }
    if text.is_empty() {
        return None;
    }
    let negative = if let Some(stripped) = text.strip_prefix('-') {
        text = stripped.to_string();
        true
    } else if let Some(stripped) = text.strip_prefix('+') {
        text = stripped.to_string();
        false
    } else {
        false
    };
    let (whole, frac) = match format.decimal_sep {
        Some('.') => {
            let mut parts = text.split('.');
            let whole = parts.next().unwrap_or("");
            let frac = parts.next();
            if parts.next().is_some() {
                return None;
            }
            (whole, frac.unwrap_or(""))
        }
        Some('D') => {
            let mut parts = text.split('D');
            let whole = parts.next().unwrap_or("");
            let frac = parts.next();
            if parts.next().is_some() {
                return None;
            }
            (whole, frac.unwrap_or(""))
        }
        None | Some(_) => {
            if text.contains('.') {
                return None;
            }
            (text.as_str(), "")
        }
    };
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !frac.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let scale = usize::try_from(format.decimal.1).ok()?;
    if frac.len() > scale {
        return None;
    }
    let mut digits = String::new();
    if whole.is_empty() {
        digits.push('0');
    } else {
        digits.push_str(whole);
    }
    digits.push_str(frac);
    for _ in 0..(scale.saturating_sub(frac.len())) {
        digits.push('0');
    }
    let mut value: i128 = digits.parse().ok()?;
    if negative {
        value = -value;
    }
    Some(value)
}

impl ScalarUDFImpl for SparkTryToBinary {
    crate::shim_udf_boilerplate!("try_to_binary");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            1 => Ok(vec![DataType::Utf8]),
            2 => Ok(vec![DataType::Utf8, DataType::Utf8]),
            n => exec_err!("'try_to_binary' expects 1 or 2 arguments, got {n}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array(arrays[0].as_ref())?;
        let format = arrays
            .get(1)
            .map(|array| string_array(array.as_ref()))
            .transpose()?;
        let mut builder = BinaryBuilder::new();
        for row in 0..input.len() {
            if input.is_null(row) {
                builder.append_null();
                continue;
            }
            let fmt = match format {
                Some(array) if array.is_null(row) => {
                    builder.append_null();
                    continue;
                }
                Some(array) => array.value(row),
                None => "hex",
            };
            match decode_binary(input.value(row), fmt) {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn decode_binary(input: &str, fmt: &str) -> Option<Vec<u8>> {
    match fmt.to_ascii_lowercase().as_str() {
        "hex" => decode_hex(input),
        "utf-8" | "utf8" => Some(input.as_bytes().to_vec()),
        "base64" => decode_base64(input),
        _ => None,
    }
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    let mut hex = input.to_string();
    if hex.len() % 2 == 1 {
        hex.insert(0, '0');
    }
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).ok())
        .collect()
}

fn decode_base64(input: &str) -> Option<Vec<u8>> {
    fn value(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut filtered: Vec<u8> = Vec::new();
    let mut padding = 0usize;
    for byte in input.bytes() {
        if byte == b'=' {
            padding += 1;
            continue;
        }
        if byte.is_ascii_whitespace() {
            continue;
        }
        if padding > 0 {
            return None;
        }
        filtered.push(value(byte)?);
    }
    if filtered.is_empty() && padding == 0 && !input.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    let mut index = 0;
    while index < filtered.len() {
        if index + 1 >= filtered.len() {
            break;
        }
        let a = filtered[index];
        let b = filtered[index + 1];
        out.push((a << 2) | (b >> 4));
        if index + 2 < filtered.len() {
            let c = filtered[index + 2];
            out.push((b << 4) | (c >> 2));
            if index + 3 < filtered.len() {
                let d = filtered[index + 3];
                out.push((c << 6) | d);
            }
        }
        index += 4;
    }
    if padding > 2 {
        return None;
    }
    Some(out)
}

impl ScalarUDFImpl for SparkTryToTime {
    crate::shim_udf_boilerplate!("try_to_time");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(unsupported_time_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Err(unsupported_time_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            1 => Ok(vec![DataType::Utf8]),
            2 => Ok(vec![DataType::Utf8, DataType::Utf8]),
            n => exec_err!("'try_to_time' expects 1 or 2 arguments, got {n}"),
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(unsupported_time_type())
    }
}

fn unsupported_time_type() -> DataFusionError {
    DataFusionError::Plan(
        "[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported. SQLSTATE: 0A000".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::try_invert::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn try_to_date_malformed_is_null() {
        let batches = ctx()
            .sql("SELECT try_to_date('not-a-date') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(!array.is_valid(0));
    }

    #[tokio::test]
    async fn try_to_date_good_iso() {
        let batches = ctx()
            .sql("SELECT try_to_date('2024-01-15') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(
            Date32Type::to_naive_date_opt(array.value(0)).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );
    }

    #[tokio::test]
    async fn try_to_time_refuses_unsupported() {
        let error = match ctx().sql("SELECT try_to_time('12:34:56') AS v").await {
            Err(error) => error.to_string(),
            Ok(_) => "planned".to_string(),
        };
        assert!(
            error.contains("UNSUPPORTED_TIME_TYPE") || error.contains("TIME is not supported"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn try_to_binary_hex_default_pads_odd() {
        let batches = ctx()
            .sql("SELECT try_to_binary('61') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0].column(0);
        let binary = array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(binary.value(0), b"a");
    }
}
