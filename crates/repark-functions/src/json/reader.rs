use std::borrow::Cow;
use std::fmt::Write;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JsonValue<'a> {
    Null,
    Bool(bool),
    NonFinite(f64),
    Number(&'a str),
    Text(Cow<'a, str>),
    Array(Vec<JsonValue<'a>>),
    Object(Vec<(Cow<'a, str>, JsonValue<'a>)>),
}

pub(crate) fn parse_json(input: &str) -> Option<JsonValue<'_>> {
    let mut reader = JsonReader {
        input,
        position: 0,
        depth: 0,
    };
    reader.skip_whitespace();
    let value = reader.read_value()?;
    Some(value)
}

const MAX_DEPTH: usize = 200;

struct JsonReader<'a> {
    input: &'a str,
    position: usize,
    depth: usize,
}

impl<'a> JsonReader<'a> {
    fn rest(&self) -> &'a [u8] {
        &self.input.as_bytes()[self.position..]
    }

    fn peek(&self) -> Option<u8> {
        self.rest().first().copied()
    }

    fn skip_whitespace(&mut self) {
        while let Some(byte) = self.peek() {
            if matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
                self.position += 1;
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, byte: u8) -> Option<()> {
        if self.peek() == Some(byte) {
            self.position += 1;
            Some(())
        } else {
            None
        }
    }

    fn read_value(&mut self) -> Option<JsonValue<'a>> {
        if self.depth >= MAX_DEPTH {
            return None;
        }
        match self.peek()? {
            b'{' => self.read_object(),
            b'[' => self.read_array(),
            b'"' | b'\'' => self.read_text().map(JsonValue::Text),
            b't' => self.read_literal("true").map(|()| JsonValue::Bool(true)),
            b'f' => self.read_literal("false").map(|()| JsonValue::Bool(false)),
            b'n' => self.read_literal("null").map(|()| JsonValue::Null),
            b'N' => self
                .read_literal("NaN")
                .map(|()| JsonValue::NonFinite(f64::NAN)),
            b'I' => self
                .read_literal("Infinity")
                .map(|()| JsonValue::NonFinite(f64::INFINITY)),
            b'-' if self.input[self.position..].starts_with("-Infinity") => self
                .read_literal("-Infinity")
                .map(|()| JsonValue::NonFinite(f64::NEG_INFINITY)),
            _ => self.read_number(),
        }
    }

    fn read_literal(&mut self, word: &str) -> Option<()> {
        if self.input[self.position..].starts_with(word) {
            self.position += word.len();
            Some(())
        } else {
            None
        }
    }

    fn read_number(&mut self) -> Option<JsonValue<'a>> {
        let start = self.position;
        if self.peek() == Some(b'-') {
            self.position += 1;
        }
        let integral_start = self.position;
        let digits_before = self.read_digits();
        if digits_before == 0 {
            return None;
        }
        if digits_before > 1 && self.input.as_bytes()[integral_start] == b'0' {
            return None;
        }
        if self.peek() == Some(b'.') {
            self.position += 1;
            if self.read_digits() == 0 {
                return None;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            if self.read_digits() == 0 {
                return None;
            }
        }
        Some(JsonValue::Number(&self.input[start..self.position]))
    }

    fn read_digits(&mut self) -> usize {
        let start = self.position;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.position += 1;
        }
        self.position - start
    }

    fn read_text(&mut self) -> Option<Cow<'a, str>> {
        let quote = self.peek()?;
        if quote != b'"' && quote != b'\'' {
            return None;
        }
        self.position += 1;
        let start = self.position;
        loop {
            let byte = self.peek()?;
            match byte {
                found if found == quote => {
                    let plain = self.input.get(start..self.position)?;
                    self.position += 1;
                    return Some(Cow::Borrowed(plain));
                }
                b'\\' => {
                    let mut owned = String::from(self.input.get(start..self.position)?);
                    self.read_text_tail(&mut owned, quote)?;
                    return Some(Cow::Owned(owned));
                }
                _ => {
                    if byte < 0x20 {
                        return None;
                    }
                    self.position += utf8_width(byte);
                    if self.position > self.input.len() {
                        return None;
                    }
                }
            }
        }
    }

    fn read_text_tail(&mut self, owned: &mut String, quote: u8) -> Option<()> {
        loop {
            let byte = self.peek()?;
            match byte {
                found if found == quote => {
                    self.position += 1;
                    return Some(());
                }
                b'\\' => {
                    self.position += 1;
                    owned.push_str(&self.read_escape()?);
                }
                _ => {
                    if byte < 0x20 {
                        return None;
                    }
                    let width = utf8_width(byte);
                    let chunk = self.input.get(self.position..self.position + width)?;
                    owned.push_str(chunk);
                    self.position += width;
                }
            }
        }
    }

    fn read_escape(&mut self) -> Option<String> {
        let byte = self.peek()?;
        self.position += 1;
        let decoded = match byte {
            b'"' => '"'.to_string(),
            b'\\' => '\\'.to_string(),
            b'/' => '/'.to_string(),
            b'b' => '\u{8}'.to_string(),
            b'f' => '\u{c}'.to_string(),
            b'n' => '\n'.to_string(),
            b'r' => '\r'.to_string(),
            b't' => '\t'.to_string(),
            b'u' => self.read_unicode_escape()?,
            _ => return None,
        };
        Some(decoded)
    }

    fn read_unicode_escape(&mut self) -> Option<String> {
        let first = self.read_hex4()?;
        if (0xD800..0xDC00).contains(&first) {
            if self.peek() == Some(b'\\') {
                self.position += 1;
                self.expect(b'u')?;
                let second = self.read_hex4()?;
                if (0xDC00..0xE000).contains(&second) {
                    let combined = 0x1_0000 + ((first - 0xD800) << 10) + (second - 0xDC00);
                    return char::from_u32(combined).map(|found| found.to_string());
                }
            }
            return Some('\u{fffd}'.to_string());
        }
        char::from_u32(first)
            .map(|found| found.to_string())
            .or_else(|| Some('\u{fffd}'.to_string()))
    }

    fn read_hex4(&mut self) -> Option<u32> {
        let slice = self.input.get(self.position..self.position + 4)?;
        let value = u32::from_str_radix(slice, 16).ok()?;
        self.position += 4;
        Some(value)
    }

    fn read_array(&mut self) -> Option<JsonValue<'a>> {
        self.expect(b'[')?;
        self.depth += 1;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.position += 1;
            self.depth -= 1;
            return Some(JsonValue::Array(items));
        }
        loop {
            self.skip_whitespace();
            items.push(self.read_value()?);
            self.skip_whitespace();
            match self.peek()? {
                b',' => self.position += 1,
                b']' => {
                    self.position += 1;
                    self.depth -= 1;
                    return Some(JsonValue::Array(items));
                }
                _ => return None,
            }
        }
    }

    fn read_object(&mut self) -> Option<JsonValue<'a>> {
        self.expect(b'{')?;
        self.depth += 1;
        let mut entries = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.position += 1;
            self.depth -= 1;
            return Some(JsonValue::Object(entries));
        }
        loop {
            self.skip_whitespace();
            let key = self.read_text()?;
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            let value = self.read_value()?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.peek()? {
                b',' => self.position += 1,
                b'}' => {
                    self.position += 1;
                    self.depth -= 1;
                    return Some(JsonValue::Object(entries));
                }
                _ => return None,
            }
        }
    }
}

fn utf8_width(byte: u8) -> usize {
    match byte {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

pub(crate) fn json_number_text(raw: &str) -> String {
    if raw.contains(['.', 'e', 'E']) {
        return match raw.parse::<f64>() {
            Ok(value) if value.is_finite() => java_double_text(value),
            Ok(value) => {
                let mut quoted = String::new();
                write_escaped(&java_double_text(value), &mut quoted);
                quoted
            }
            Err(_) => raw.to_string(),
        };
    }
    if raw == "-0" {
        return "0".to_string();
    }
    raw.to_string()
}

pub(crate) fn java_double_text(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    java_decimal_text(&format!("{value:e}"))
}

pub(crate) fn java_float_text(value: f32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    java_decimal_text(&format!("{value:e}"))
}

fn java_decimal_text(shortest: &str) -> String {
    let (sign, body) = match shortest.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", shortest),
    };
    let Some((mantissa, exponent_text)) = body.split_once('e') else {
        return shortest.to_string();
    };
    let Ok(exponent) = exponent_text.parse::<i32>() else {
        return shortest.to_string();
    };
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if (-3..=6).contains(&exponent) {
        format!("{sign}{}", plain_decimal(&digits, exponent))
    } else {
        let head = &digits[..1];
        let tail = if digits.len() > 1 { &digits[1..] } else { "0" };
        format!("{sign}{head}.{tail}E{exponent}")
    }
}

fn plain_decimal(digits: &str, exponent: i32) -> String {
    if exponent < 0 {
        let zeros = "0".repeat(usize::try_from(-exponent - 1).unwrap_or(0));
        return format!("0.{zeros}{digits}");
    }
    let point = usize::try_from(exponent + 1).unwrap_or(0);
    if point >= digits.len() {
        let zeros = "0".repeat(point - digits.len());
        format!("{digits}{zeros}.0")
    } else {
        format!("{}.{}", &digits[..point], &digits[point..])
    }
}

pub(crate) fn write_compact(value: &JsonValue<'_>, out: &mut String) {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(true) => out.push_str("true"),
        JsonValue::Bool(false) => out.push_str("false"),
        JsonValue::Number(raw) => out.push_str(&json_number_text(raw)),
        JsonValue::NonFinite(value) => write_escaped(&java_double_text(*value), out),
        JsonValue::Text(text) => write_escaped(text, out),
        JsonValue::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_compact(item, out);
            }
            out.push(']');
        }
        JsonValue::Object(entries) => {
            out.push('{');
            for (index, (key, item)) in entries.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_escaped(key, out);
                out.push(':');
                write_compact(item, out);
            }
            out.push('}');
        }
    }
}

pub(crate) fn write_escaped(text: &str, out: &mut String) {
    out.push('"');
    for found in text.chars() {
        match found {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            control if control < ' ' => {
                let _ = write!(out, "\\u{:04X}", control as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
}
