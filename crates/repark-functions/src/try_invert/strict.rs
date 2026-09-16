use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryBuilder, Decimal128Builder, StringArray, StringBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::convert::{
    NumberFormat, apply_number_format, decode_base64, decode_hex, parse_number_format, string_array,
};
use crate::datetime::{
    JavaPatternToken, compile_java_pattern, datetime_from_micros, format_compiled_java_pattern,
    local_datetime_from_micros,
};
use crate::session_time_zone::session_time_zone_from_options;

#[must_use]
pub fn to_number_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToNumber::new()))
}

#[must_use]
pub fn to_binary_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToBinary::new()))
}

#[must_use]
pub fn to_char_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToChar::new("to_char")))
}

#[must_use]
pub fn to_varchar_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToChar::new("to_varchar")))
}

fn folded_format(args: &ReturnFieldArgs<'_>) -> Option<String> {
    let Some(Some(format)) = args.scalar_arguments.get(1) else {
        return None;
    };
    match format {
        ScalarValue::Utf8(Some(pattern))
        | ScalarValue::LargeUtf8(Some(pattern))
        | ScalarValue::Utf8View(Some(pattern)) => Some(pattern.clone()),
        _ => None,
    }
}

fn mismatch_text(format: &str, input: &str) -> String {
    format!(
        "[INVALID_FORMAT.MISMATCH_INPUT] The format is invalid: {format}. The input \"STRING\" \
         {input} does not match the format. SQLSTATE: 42601"
    )
}

fn conversion_text(input: &str, format: &str) -> String {
    format!(
        "[CONVERSION_INVALID_INPUT] The value '{input}' ('{}') cannot be converted to \"BINARY\" \
         because it is malformed. Correct the value as per the syntax, or change its format. Use \
         `try_to_binary` to tolerate malformed input and return NULL instead. SQLSTATE: 22018",
        format.to_ascii_uppercase()
    )
}

fn unknown_binary_format_text(name: &str, format: &str) -> String {
    format!(
        "[INVALID_FORMAT.UNEXPECTED_TOKEN] The format is invalid: '{format}'. {name} expects \
         'hex', 'base64', 'utf-8' or 'binary'. SQLSTATE: 42601"
    )
}

#[derive(Debug)]
struct SparkToNumber {
    signature: Signature,
}

impl SparkToNumber {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkToNumber {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToNumber {}

impl Hash for SparkToNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkToNumber {
    crate::shim_udf_boilerplate!("to_number");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(38, 18))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let decimal = match folded_format(&args) {
            Some(pattern) => parse_number_format(&pattern)?.decimal,
            None => (38, 18),
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(decimal.0, decimal.1),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "'to_number' expects (expr, format), got {} argument(s)",
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
            return exec_err!("to_number promised Decimal128");
        };
        let mut builder = Decimal128Builder::with_capacity(input.len())
            .with_data_type(DataType::Decimal128(precision, scale));
        let mut format_cache: Option<(String, NumberFormat)> = None;
        for row in 0..input.len() {
            if input.is_null(row) || format.is_null(row) {
                builder.append_null();
                continue;
            }
            let pattern = format.value(row);
            let format_miss = format_cache
                .as_ref()
                .is_none_or(|(saved, _)| saved.as_str() != pattern);
            if format_miss {
                let parsed = parse_number_format(pattern)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                format_cache = Some((pattern.to_string(), parsed));
            }
            let Some((_, parsed)) = format_cache.as_ref() else {
                return exec_err!("to_number lost its cached format");
            };
            let Some(value) = apply_number_format(input.value(row), parsed) else {
                return Err(DataFusionError::Execution(mismatch_text(
                    pattern,
                    input.value(row),
                )));
            };
            builder.append_value(rescale_number(value, parsed, scale)?);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn rescale_number(value: i128, parsed: &NumberFormat, scale: i8) -> Result<i128> {
    let mut result = value;
    let mut have = parsed.decimal.1;
    while have < scale {
        result = result
            .checked_mul(10)
            .ok_or_else(|| DataFusionError::Execution("to_number: decimal overflow".to_string()))?;
        have += 1;
    }
    while have > scale {
        result /= 10;
        have -= 1;
    }
    Ok(result)
}

#[derive(Debug)]
struct SparkToBinary {
    signature: Signature,
}

impl SparkToBinary {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkToBinary {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToBinary {}

impl Hash for SparkToBinary {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn check_binary_format(format: &str) -> Result<()> {
    match format.to_ascii_lowercase().as_str() {
        "hex" | "utf-8" | "utf8" | "base64" | "binary" => Ok(()),
        _ => Err(DataFusionError::Plan(unknown_binary_format_text(
            "to_binary",
            format,
        ))),
    }
}

impl ScalarUDFImpl for SparkToBinary {
    crate::shim_udf_boilerplate!("to_binary");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if let Some(format) = folded_format(&args) {
            check_binary_format(&format)?;
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            1 => Ok(vec![DataType::Utf8]),
            2 => Ok(vec![DataType::Utf8, DataType::Utf8]),
            n => exec_err!("'to_binary' expects 1 or 2 arguments, got {n}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array(arrays[0].as_ref())?;
        let format = arrays
            .get(1)
            .map(|array| string_array(array.as_ref()))
            .transpose()?;
        let bytes = input.get_array_memory_size();
        let mut builder = BinaryBuilder::with_capacity(input.len(), bytes / input.len().max(1));
        let mut format_cache: Option<(String, Option<BinaryKind>)> = None;
        for row in 0..input.len() {
            if input.is_null(row) {
                builder.append_null();
                continue;
            }
            let raw = match &format {
                Some(array) if array.is_null(row) => {
                    builder.append_null();
                    continue;
                }
                Some(array) => array.value(row),
                None => "hex",
            };
            let format_miss = format_cache
                .as_ref()
                .is_none_or(|(saved, _)| saved.as_str() != raw);
            if format_miss {
                let kind = match raw.to_ascii_lowercase().as_str() {
                    "hex" => Some(BinaryKind::Hex),
                    "utf-8" | "utf8" | "binary" => Some(BinaryKind::Utf8),
                    "base64" => Some(BinaryKind::Base64),
                    _ => None,
                };
                format_cache = Some((raw.to_string(), kind));
            }
            let Some((_, kind)) = format_cache.as_ref() else {
                return exec_err!("to_binary lost its cached format");
            };
            match kind {
                Some(BinaryKind::Utf8) => builder.append_value(input.value(row).as_bytes()),
                Some(BinaryKind::Hex) => match decode_hex(input.value(row)) {
                    Some(bytes) => builder.append_value(bytes),
                    None => {
                        return Err(DataFusionError::Execution(conversion_text(
                            input.value(row),
                            raw,
                        )));
                    }
                },
                Some(BinaryKind::Base64) => match decode_base64(input.value(row)) {
                    Some(bytes) => builder.append_value(bytes),
                    None => {
                        return Err(DataFusionError::Execution(conversion_text(
                            input.value(row),
                            raw,
                        )));
                    }
                },
                None => {
                    return Err(DataFusionError::Execution(unknown_binary_format_text(
                        "to_binary",
                        raw,
                    )));
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryKind {
    Hex,
    Utf8,
    Base64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaskToken {
    Nine,
    Zero,
    Comma,
    Dot,
    Dollar,
    Literal(char),
}

struct Mask {
    tokens: Vec<MaskToken>,
    int_slots: usize,
    frac_slots: usize,
}

fn parse_mask(pattern: &str) -> Result<Mask> {
    let upper = pattern.to_ascii_uppercase();
    if !upper.bytes().any(|byte| byte == b'9' || byte == b'0') {
        return Err(DataFusionError::Execution(format!(
            "[INVALID_FORMAT.WRONG_NUM_DIGIT] The format is invalid: '{upper}'. The format \
             string requires at least one number digit. SQLSTATE: 42601"
        )));
    }
    let mut tokens = Vec::new();
    let mut int_slots = 0usize;
    let mut frac_slots = 0usize;
    let mut seen_dot = false;
    for byte in upper.bytes() {
        let token = match byte {
            b'9' => MaskToken::Nine,
            b'0' => MaskToken::Zero,
            b',' | b'G' => MaskToken::Comma,
            b'.' | b'D' => {
                if seen_dot {
                    return Err(DataFusionError::Execution(format!(
                        "[INVALID_FORMAT.UNEXPECTED_TOKEN] The format is invalid: '{upper}'. \
                         Found a second decimal separator. SQLSTATE: 42601"
                    )));
                }
                seen_dot = true;
                MaskToken::Dot
            }
            b'$' => MaskToken::Dollar,
            other => MaskToken::Literal(other as char),
        };
        if matches!(token, MaskToken::Nine | MaskToken::Zero) {
            if seen_dot {
                frac_slots += 1;
            } else {
                int_slots += 1;
            }
        }
        tokens.push(token);
    }
    Ok(Mask {
        tokens,
        int_slots,
        frac_slots,
    })
}

fn render_mask_overflow(mask: &Mask) -> String {
    let mut out = String::new();
    for token in &mask.tokens {
        match token {
            MaskToken::Nine | MaskToken::Zero => out.push('#'),
            MaskToken::Comma => out.push(' '),
            MaskToken::Dot => out.push('.'),
            MaskToken::Dollar => out.push('$'),
            MaskToken::Literal(other) => out.push(*other),
        }
    }
    out
}

fn split_mask(mask: &Mask) -> (&[MaskToken], &[MaskToken]) {
    match mask
        .tokens
        .iter()
        .position(|token| *token == MaskToken::Dot)
    {
        Some(index) => (&mask.tokens[..index], &mask.tokens[index + 1..]),
        None => (mask.tokens.as_slice(), &[]),
    }
}

fn render_mask(mask: &Mask, int_digits: &str, frac_digits: &str) -> String {
    let int_fits = int_digits.len() <= mask.int_slots || (mask.int_slots == 0 && int_digits == "0");
    if !int_fits || frac_digits.len() > mask.frac_slots {
        return render_mask_overflow(mask);
    }
    let (int_tokens, frac_tokens) = split_mask(mask);
    let mut head = String::new();
    let mut rest = int_digits.as_bytes();
    if !(mask.int_slots == 0 && int_digits.bytes().all(|b| b == b'0')) {
        let mut reversed = Vec::new();
        for token in int_tokens.iter().rev() {
            match token {
                MaskToken::Nine => match rest.split_last() {
                    Some((digit, kept)) => {
                        rest = kept;
                        reversed.push(*digit as char);
                    }
                    None => reversed.push(' '),
                },
                MaskToken::Zero => match rest.split_last() {
                    Some((digit, kept)) => {
                        rest = kept;
                        reversed.push(*digit as char);
                    }
                    None => reversed.push('0'),
                },
                MaskToken::Comma => {
                    if rest.is_empty() {
                        reversed.push(' ');
                    } else {
                        reversed.push(',');
                    }
                }
                MaskToken::Dollar => reversed.push('$'),
                MaskToken::Dot => reversed.push('.'),
                MaskToken::Literal(other) => reversed.push(*other),
            }
        }
        head.extend(reversed.iter().rev());
    }
    let mut out = head;
    if mask.tokens.contains(&MaskToken::Dot) {
        out.push('.');
        let mut digits = frac_digits.as_bytes().iter();
        for token in frac_tokens {
            match token {
                MaskToken::Nine | MaskToken::Zero => {
                    out.push(digits.next().map_or('0', |digit| *digit as char));
                }
                MaskToken::Comma => out.push(','),
                MaskToken::Dollar => out.push('$'),
                MaskToken::Dot => out.push('.'),
                MaskToken::Literal(other) => out.push(*other),
            }
        }
    }
    out
}

fn split_decimal_text(digits: &str, scale: i32) -> Result<(String, String)> {
    let width = usize::try_from(scale.max(0))
        .map_err(|_| DataFusionError::Execution("to_char: scale out of range".to_string()))?;
    let padded = if digits.len() <= width {
        format!("{:0>width$}", digits, width = width + 1)
    } else {
        digits.to_string()
    };
    let cut = padded.len() - width;
    let mut whole = padded[..cut].trim_start_matches('0').to_string();
    if whole.is_empty() {
        whole.push('0');
    }
    let mut frac = padded[cut..].trim_end_matches('0').to_string();
    if scale <= 0 {
        frac.clear();
    }
    Ok((whole, frac))
}

fn encode_hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 15) as usize] as char);
    }
    out
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let mut word = 0u32;
        for byte in chunk {
            word = (word << 8) | u32::from(*byte);
        }
        word <<= 8 * (3 - chunk.len());
        out.push(ALPHABET[((word >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((word >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((word >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(word & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn render_binary(bytes: &[u8], format: &str, name: &str) -> Result<String> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => Ok(encode_hex_upper(bytes)),
        "base64" => Ok(encode_base64(bytes)),
        "utf-8" | "utf8" | "binary" => Ok(String::from_utf8_lossy(bytes).into_owned()),
        _ => Err(DataFusionError::Execution(unknown_binary_format_text(
            name, format,
        ))),
    }
}

fn render_temporal_row(
    stamps: &datafusion::arrow::array::TimestampMicrosecondArray,
    row: usize,
    instant: bool,
    pattern: &str,
    zone: Tz,
    cached: &mut Option<(String, Vec<JavaPatternToken>)>,
    out: &mut StringBuilder,
) -> Result<()> {
    if stamps.is_null(row) {
        out.append_null();
        return Ok(());
    }
    let micros = stamps.value(row);
    let rendered = if instant {
        local_datetime_from_micros(micros, zone)
    } else {
        datetime_from_micros(micros)
    };
    let Some(moment) = rendered else {
        out.append_null();
        return Ok(());
    };
    let stale = cached.as_ref().is_none_or(|(prior, _)| prior != pattern);
    if stale {
        *cached = Some((pattern.to_string(), compile_java_pattern(pattern)?));
    }
    let tokens = cached
        .as_ref()
        .map(|(_, tokens)| tokens.as_slice())
        .ok_or_else(|| {
            DataFusionError::Execution("to_char: internal pattern cache miss".to_string())
        })?;
    out.append_value(format_compiled_java_pattern(moment, tokens)?);
    Ok(())
}

fn render_decimal_value(value: i128, scale: i8, mask: &Mask) -> Result<String> {
    let digits = value.unsigned_abs().to_string();
    let scale = i32::from(scale);
    let (whole, frac) = if scale < 0 {
        let extra = usize::try_from(scale.checked_neg().unwrap_or(i32::MAX))
            .map_err(|_| DataFusionError::Execution("to_char: scale out of range".to_string()))?;
        let widened = format!("{digits}{}", "0".repeat(extra));
        (widened.trim_start_matches('0').to_string(), String::new())
    } else {
        split_decimal_text(&digits, scale)?
    };
    let whole = if whole.is_empty() {
        "0".to_string()
    } else {
        whole
    };
    Ok(render_mask(mask, &whole, &frac))
}

fn render_float_value(value: f64, mask: &Mask) -> String {
    if !value.is_finite() {
        return render_mask_overflow(mask);
    }
    let text = value.abs().to_string();
    let (whole_raw, frac_raw) = match text.split_once('.') {
        Some((whole, frac)) => (whole, frac),
        None => (text.as_str(), ""),
    };
    if !whole_raw.bytes().all(|byte| byte.is_ascii_digit())
        || !frac_raw.bytes().all(|byte| byte.is_ascii_digit())
    {
        return render_mask_overflow(mask);
    }
    let whole = whole_raw.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    render_mask(mask, whole, frac_raw.trim_end_matches('0'))
}

#[derive(Debug)]
struct SparkToChar {
    name: &'static str,
    signature: Signature,
}

enum CharInput {
    Stamped(ArrayRef),
    Bytes(ArrayRef),
    Text(ArrayRef),
    Decimal(ArrayRef, i8),
    Float(ArrayRef),
    Null,
}

impl CharInput {
    fn resolve(input: &ArrayRef, stamped: Option<&ArrayRef>, name: &str) -> Result<Self> {
        match input.data_type() {
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                let Some(stamped) = stamped else {
                    return exec_err!("to_char promised timestamps");
                };
                Ok(Self::Stamped(Arc::clone(stamped)))
            }
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Ok(Self::Bytes(cast(input.as_ref(), &DataType::Binary)?))
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(Self::Text(cast(input.as_ref(), &DataType::Utf8)?))
            }
            DataType::Decimal128(_, scale) => Ok(Self::Decimal(Arc::clone(input), *scale)),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                Ok(Self::Float(cast(input.as_ref(), &DataType::Float64)?))
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(Self::Decimal(
                cast(input.as_ref(), &DataType::Decimal128(38, 0))?,
                0,
            )),
            DataType::Null => Ok(Self::Null),
            other => exec_err!("'{name}' cannot format {other}"),
        }
    }
}

fn char_estimate(input: &ArrayRef, format: &StringArray) -> usize {
    if matches!(
        input.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
    ) {
        return input.get_array_memory_size() * 2 / input.len().max(1);
    }
    (0..format.len())
        .find_map(|row| {
            if format.is_null(row) {
                None
            } else {
                Some(format.value(row).len())
            }
        })
        .unwrap_or(16)
}

impl SparkToChar {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkToChar {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkToChar {}

impl Hash for SparkToChar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SparkToChar {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let input = args
            .arg_fields
            .first()
            .map(|field| field.data_type().clone());
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        let folded = folded_format(&args);
        if let Some(DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64) = input {
            return Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)));
        }
        if matches!(
            input,
            Some(DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_))
        ) {
            match folded.as_deref().map(str::to_ascii_lowercase) {
                Some(named) if named == "hex" || named == "base64" => {
                    return Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)));
                }
                Some(named) if named == "utf-8" || named == "utf8" || named == "binary" => {
                    return Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)));
                }
                Some(named) => {
                    return Err(DataFusionError::Plan(unknown_binary_format_text(
                        self.name, &named,
                    )));
                }
                None => {
                    return Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)));
                }
            }
        }
        if let (Some(_), Some(pattern)) = (input, folded) {
            parse_mask(&pattern).map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "'{}' expects (expr, format), got {} argument(s)",
                self.name(),
                arg_types.len()
            );
        }
        Ok(vec![arg_types[0].clone(), DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let format = string_array(arrays[1].as_ref())?;
        let zone_name = session_time_zone_from_options(args.config_options.as_ref());
        let zone = Tz::from_str(zone_name).map_err(|error| {
            DataFusionError::Execution(format!(
                "session timezone {zone_name:?} could not be resolved at query time ({error})"
            ))
        })?;
        let instant = matches!(arrays[0].data_type(), DataType::Timestamp(_, Some(_)));
        let stamped = match arrays[0].data_type() {
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => Some(cast(
                arrays[0].as_ref(),
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            )?),
            _ => None,
        };
        let mut cached: Option<(String, Vec<JavaPatternToken>)> = None;
        let mut prepared: Option<CharInput> = None;
        let mut mask_cache: Option<(String, Mask)> = None;
        let estimate = char_estimate(&arrays[0], format);
        let mut out = StringBuilder::with_capacity(arrays[0].len(), arrays[0].len() * estimate);
        for row in 0..arrays[0].len() {
            if arrays[0].is_null(row) || format.is_null(row) {
                out.append_null();
                continue;
            }
            let input = match prepared.as_ref() {
                Some(input) => input,
                None => {
                    prepared.insert(CharInput::resolve(&arrays[0], stamped.as_ref(), self.name)?)
                }
            };
            let pattern = format.value(row);
            match input {
                CharInput::Stamped(stamped) => {
                    let stamps = stamped.as_primitive::<TimestampMicrosecondType>();
                    render_temporal_row(
                        stamps,
                        row,
                        instant,
                        pattern,
                        zone,
                        &mut cached,
                        &mut out,
                    )?;
                }
                CharInput::Bytes(bytes) => {
                    let bytes = bytes.as_binary::<i32>();
                    out.append_value(render_binary(bytes.value(row), pattern, self.name)?);
                }
                CharInput::Text(text) => {
                    let text = string_array(text.as_ref())?;
                    out.append_value(text.value(row));
                }
                CharInput::Decimal(whole, scale) => {
                    let mask_miss = mask_cache
                        .as_ref()
                        .is_none_or(|(saved, _)| saved.as_str() != pattern);
                    if mask_miss {
                        mask_cache = Some((pattern.to_string(), parse_mask(pattern)?));
                    }
                    let Some((_, mask)) = mask_cache.as_ref() else {
                        return exec_err!("to_char lost its cached mask");
                    };
                    let whole =
                        whole.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
                    out.append_value(render_decimal_value(whole.value(row), *scale, mask)?);
                }
                CharInput::Float(numbers) => {
                    let mask_miss = mask_cache
                        .as_ref()
                        .is_none_or(|(saved, _)| saved.as_str() != pattern);
                    if mask_miss {
                        mask_cache = Some((pattern.to_string(), parse_mask(pattern)?));
                    }
                    let Some((_, mask)) = mask_cache.as_ref() else {
                        return exec_err!("to_char lost its cached mask");
                    };
                    let numbers =
                        numbers.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
                    out.append_value(render_float_value(numbers.value(row), mask));
                }
                CharInput::Null => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_overflow_spells_hash_with_blank_group() {
        let mask = parse_mask("99,999.99").unwrap();
        assert_eq!(render_mask(&mask, "12345", "6789"), "## ###.##");
    }

    #[test]
    fn mask_blank_group_with_zero_units() {
        let mask = parse_mask("99,999.99").unwrap();
        assert_eq!(render_mask(&mask, "0", "5"), "     0.50");
    }

    #[test]
    fn mask_zero_fill_and_fraction_pad() {
        let mask = parse_mask("000000.0000").unwrap();
        assert_eq!(render_mask(&mask, "12345", "6789"), "012345.6789");
        assert_eq!(render_mask(&mask, "0", "5"), "000000.5000");
    }

    #[test]
    fn mask_dollar_overflow_keeps_sign() {
        let mask = parse_mask("$9.99").unwrap();
        assert_eq!(render_float_value(2.5, &mask), "$2.50");
        assert_eq!(render_float_value(0.125, &mask), "$#.##");
    }

    #[test]
    fn mask_group_separator_prints_past_thousands() {
        let mask = parse_mask("99,999").unwrap();
        assert_eq!(render_mask(&mask, "12345", ""), "12,345");
    }

    #[test]
    fn decimal_render_drops_sign_and_strips_zeros() {
        let mask = parse_mask("99,999.99").unwrap();
        assert_eq!(render_decimal_value(-5000, 4, &mask).unwrap(), "     0.50");
        let zero = parse_mask("000000.0000").unwrap();
        assert_eq!(
            render_decimal_value(-5000, 4, &zero).unwrap(),
            "000000.5000"
        );
    }

    #[test]
    fn binary_renders_match_oracle_forms() {
        assert_eq!(render_binary(b"ABC", "hex", "to_char").unwrap(), "414243");
        assert_eq!(render_binary(b"ABC", "base64", "to_char").unwrap(), "QUJD");
        assert_eq!(render_binary(b"ABC", "utf-8", "to_char").unwrap(), "ABC");
    }

    #[test]
    fn base64_encodes_like_spark() {
        assert_eq!(encode_base64(b"ABC"), "QUJD");
        assert_eq!(encode_base64(b"a"), "YQ==");
    }
}
