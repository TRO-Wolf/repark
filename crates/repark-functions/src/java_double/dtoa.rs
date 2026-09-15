use core::fmt::Write as _;

use datafusion::arrow::array::{Array, Float32Array, Float64Array, StringArray, StringBuilder};

pub(crate) const JAVA_FLOAT_TEXT_MAX_LEN: usize = 32;

struct SliceWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl core::fmt::Write for SliceWriter<'_> {
    fn write_str(&mut self, text: &str) -> core::fmt::Result {
        let end = self.pos + text.len();
        if end > self.buf.len() {
            return Err(core::fmt::Error);
        }
        self.buf[self.pos..end].copy_from_slice(text.as_bytes());
        self.pos = end;
        Ok(())
    }
}

pub(crate) fn with_java_double_text<R>(value: f64, render: impl FnOnce(&str) -> R) -> R {
    if value.is_nan() {
        return render("NaN");
    }
    if value.is_infinite() {
        if value.is_sign_negative() {
            return render("-Infinity");
        }
        return render("Infinity");
    }
    let bits = value.to_bits();
    if bits == 1 || bits == 0x8000_0000_0000_0001 {
        if value.is_sign_negative() {
            return render("-4.9E-324");
        }
        return render("4.9E-324");
    }
    let mut shortest = [0u8; 64];
    let mut writer = SliceWriter {
        buf: &mut shortest,
        pos: 0,
    };
    let _ = write!(writer, "{value:e}");
    let len = writer.pos;
    let mut composed = [0u8; JAVA_FLOAT_TEXT_MAX_LEN];
    render(compose_from_shortest(
        &shortest[..len],
        value.is_sign_negative(),
        &mut composed,
    ))
}

pub(crate) fn with_java_float_text<R>(value: f32, render: impl FnOnce(&str) -> R) -> R {
    if value.is_nan() {
        return render("NaN");
    }
    if value.is_infinite() {
        if value.is_sign_negative() {
            return render("-Infinity");
        }
        return render("Infinity");
    }
    let bits = value.to_bits();
    if bits == 1 || bits == 0x8000_0001 {
        if value.is_sign_negative() {
            return render("-1.4E-45");
        }
        return render("1.4E-45");
    }
    let mut shortest = [0u8; 64];
    let mut writer = SliceWriter {
        buf: &mut shortest,
        pos: 0,
    };
    let _ = write!(writer, "{value:e}");
    let len = writer.pos;
    let mut composed = [0u8; JAVA_FLOAT_TEXT_MAX_LEN];
    render(compose_from_shortest(
        &shortest[..len],
        value.is_sign_negative(),
        &mut composed,
    ))
}

fn compose_from_shortest<'a>(
    shortest: &[u8],
    negative: bool,
    composed: &'a mut [u8; JAVA_FLOAT_TEXT_MAX_LEN],
) -> &'a str {
    let body = match shortest.first() {
        Some(b'-') => &shortest[1..],
        _ => shortest,
    };
    let Some(exp_mark) = body.iter().position(|byte| *byte == b'e') else {
        return copy_bytes(body, composed);
    };
    let (mantissa, exponent_text) = body.split_at(exp_mark);
    let exponent = parse_exponent(&exponent_text[1..]);
    let mut digits = [0u8; 24];
    let mut digit_len = 0usize;
    for byte in mantissa {
        if byte.is_ascii_digit() && digit_len < digits.len() {
            digits[digit_len] = *byte;
            digit_len += 1;
        }
    }
    if digit_len == 0 {
        return copy_bytes(body, composed);
    }
    let mut pos = 0usize;
    if negative {
        composed[0] = b'-';
        pos = 1;
    }
    if (-3..=6).contains(&exponent) {
        pos = write_plain_decimal(&digits[..digit_len], exponent, composed, pos);
    } else {
        composed[pos] = digits[0];
        pos += 1;
        composed[pos] = b'.';
        pos += 1;
        if digit_len > 1 {
            let tail = &digits[1..digit_len];
            composed[pos..pos + tail.len()].copy_from_slice(tail);
            pos += tail.len();
        } else {
            composed[pos] = b'0';
            pos += 1;
        }
        composed[pos] = b'E';
        pos += 1;
        let exp_text = &exponent_text[1..];
        composed[pos..pos + exp_text.len()].copy_from_slice(exp_text);
        pos += exp_text.len();
    }
    str::from_utf8(&composed[..pos]).unwrap_or("")
}

fn parse_exponent(text: &[u8]) -> i32 {
    let (digits, negative) = match text.first() {
        Some(b'-') => (&text[1..], true),
        Some(b'+') => (&text[1..], false),
        _ => (text, false),
    };
    let mut value: i32 = 0;
    for byte in digits {
        if !byte.is_ascii_digit() {
            break;
        }
        value = value
            .saturating_mul(10)
            .saturating_add(i32::from(*byte - b'0'));
    }
    if negative { -value } else { value }
}

fn write_plain_decimal(
    digits: &[u8],
    exponent: i32,
    composed: &mut [u8; JAVA_FLOAT_TEXT_MAX_LEN],
    mut pos: usize,
) -> usize {
    if exponent < 0 {
        composed[pos] = b'0';
        composed[pos + 1] = b'.';
        pos += 2;
        let zeros = usize::try_from(-exponent - 1).unwrap_or(0);
        for _ in 0..zeros {
            composed[pos] = b'0';
            pos += 1;
        }
        composed[pos..pos + digits.len()].copy_from_slice(digits);
        pos + digits.len()
    } else {
        let point = usize::try_from(exponent + 1).unwrap_or(0);
        if point >= digits.len() {
            composed[pos..pos + digits.len()].copy_from_slice(digits);
            pos += digits.len();
            for _ in 0..point - digits.len() {
                composed[pos] = b'0';
                pos += 1;
            }
            composed[pos] = b'.';
            composed[pos + 1] = b'0';
            pos + 2
        } else {
            composed[pos..pos + point].copy_from_slice(&digits[..point]);
            pos += point;
            composed[pos] = b'.';
            pos += 1;
            let rest = &digits[point..];
            composed[pos..pos + rest.len()].copy_from_slice(rest);
            pos + rest.len()
        }
    }
}

fn copy_bytes<'a>(text: &[u8], composed: &'a mut [u8; JAVA_FLOAT_TEXT_MAX_LEN]) -> &'a str {
    let len = text.len().min(composed.len());
    composed[..len].copy_from_slice(&text[..len]);
    str::from_utf8(&composed[..len]).unwrap_or("")
}

pub(crate) fn java_double_text(value: f64) -> String {
    with_java_double_text(value, str::to_owned)
}

pub(crate) fn java_float_text(value: f32) -> String {
    with_java_float_text(value, str::to_owned)
}

pub(crate) fn java_double_text_len(value: f64) -> usize {
    with_java_double_text(value, str::len)
}

pub(crate) fn java_float_text_len(value: f32) -> usize {
    with_java_float_text(value, str::len)
}

pub(crate) fn java_double_strings(values: &Float64Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            with_java_double_text(values.value(index), |text| builder.append_value(text));
        }
    }
    builder.finish()
}

pub(crate) fn java_float_strings(values: &Float32Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            with_java_float_text(values.value(index), |text| builder.append_value(text));
        }
    }
    builder.finish()
}
