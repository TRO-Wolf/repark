const UNREPRESENTABLE: char = '\u{003F}';

pub(crate) fn literal_value(raw: &str, quote: char, keep_verbatim: bool) -> String {
    if keep_verbatim {
        raw.to_owned()
    } else {
        unescape_spark_literal(raw, quote)
    }
}

pub(crate) fn raw_value(raw: &str, quote: char, keep_verbatim: bool) -> String {
    let (head, tail) = split_raw_head(raw, quote);
    if keep_verbatim {
        let mut out = String::with_capacity(raw.len() + 1);
        out.push(quote);
        out.push_str(head);
        out.push_str(tail);
        out
    } else {
        let mut out = String::with_capacity(raw.len());
        out.push_str(head);
        out.push_str(&unescape_spark_literal(tail, quote));
        out
    }
}

fn split_raw_head(raw: &str, quote: char) -> (&str, &str) {
    let doubled = if quote == '"' { "\"\"" } else { "''" };
    match raw.find(doubled) {
        Some(position) => (&raw[..position], &raw[position + 2..]),
        None => (raw, ""),
    }
}

pub(crate) fn unescape_spark_literal(raw: &str, quote: char) -> String {
    let characters: Vec<char> = raw.chars().collect();
    let mut out = String::with_capacity(raw.len());
    let mut index = 0;
    while index < characters.len() {
        let current = characters[index];
        if current == quote {
            out.push(quote);
            index += if characters.get(index + 1) == Some(&quote) {
                2
            } else {
                1
            };
            continue;
        }
        if current != '\\' {
            out.push(current);
            index += 1;
            continue;
        }
        let Some(&escaped) = characters.get(index + 1) else {
            out.push('\\');
            index += 1;
            continue;
        };
        index = apply_escape(escaped, &characters, index, &mut out);
    }
    out
}

fn apply_escape(escaped: char, characters: &[char], index: usize, out: &mut String) -> usize {
    match escaped {
        'n' => push_and_advance('\n', index, out),
        't' => push_and_advance('\t', index, out),
        'r' => push_and_advance('\r', index, out),
        'b' => push_and_advance('\u{0008}', index, out),
        'Z' => push_and_advance('\u{001A}', index, out),
        '%' => push_kept_backslash('%', index, out),
        '_' => push_kept_backslash('_', index, out),
        'u' => apply_unicode_16(characters, index, out),
        'U' => apply_unicode_32(characters, index, out),
        '0'..='7' => apply_octal(escaped, characters, index, out),
        other => push_and_advance(other, index, out),
    }
}

fn push_and_advance(character: char, index: usize, out: &mut String) -> usize {
    out.push(character);
    index + 2
}

fn push_kept_backslash(wildcard: char, index: usize, out: &mut String) -> usize {
    out.push('\\');
    out.push(wildcard);
    index + 2
}

fn apply_octal(first: char, characters: &[char], index: usize, out: &mut String) -> usize {
    let second = characters.get(index + 2).copied();
    let third = characters.get(index + 3).copied();
    if matches!(first, '0'..='1')
        && let Some(second) = second.filter(|c| c.is_digit(8))
        && let Some(third) = third.filter(|c| c.is_digit(8))
    {
        let value = ((octal_value(first)) << 6) | (octal_value(second) << 3) | octal_value(third);
        out.push(char::from(value));
        return index + 4;
    }
    if first == '0' {
        out.push('\0');
        return index + 2;
    }
    out.push(first);
    index + 2
}

fn octal_value(digit: char) -> u8 {
    (digit as u8).saturating_sub(b'0')
}

fn apply_unicode_16(characters: &[char], index: usize, out: &mut String) -> usize {
    let Some(high) = read_hex(characters, index + 2, 4) else {
        out.push('u');
        return index + 2;
    };
    if (0xD800..=0xDBFF).contains(&high)
        && characters.get(index + 6) == Some(&'\\')
        && characters.get(index + 7) == Some(&'u')
        && let Some(low) =
            read_hex(characters, index + 8, 4).filter(|v| (0xDC00..=0xDFFF).contains(v))
    {
        let combined = 0x1_0000 + ((high - 0xD800) << 10) + (low - 0xDC00);
        push_code_point(combined, out);
        return index + 12;
    }
    push_code_point(high, out);
    index + 6
}

fn apply_unicode_32(characters: &[char], index: usize, out: &mut String) -> usize {
    let Some(value) = read_hex(characters, index + 2, 8) else {
        out.push('U');
        return index + 2;
    };
    push_code_point(value, out);
    index + 10
}

fn read_hex(characters: &[char], start: usize, count: usize) -> Option<u32> {
    let end = start.checked_add(count)?;
    let slice = characters.get(start..end)?;
    let mut value = 0u32;
    for digit in slice {
        value = value * 16 + digit.to_digit(16)?;
    }
    Some(value)
}

fn push_code_point(code_point: u32, out: &mut String) {
    if let Some(character) = char::from_u32(code_point) {
        out.push(character);
        return;
    }
    if code_point <= 0xFFFF {
        out.push(UNREPRESENTABLE);
        return;
    }
    push_java_surrogate_artifact(code_point, out);
}

fn push_java_surrogate_artifact(code_point: u32, out: &mut String) {
    let shifted = code_point.wrapping_sub(0x1_0000);
    let high = 0xD800u32.wrapping_add((shifted.cast_signed() >> 10).cast_unsigned()) & 0xFFFF;
    let low = 0xDC00 + (shifted & 0x3FF);
    if (0xD800..=0xDBFF).contains(&high) && (0xDC00..=0xDFFF).contains(&low) {
        let combined = 0x1_0000 + ((high - 0xD800) << 10) + (low - 0xDC00);
        if let Some(character) = char::from_u32(combined) {
            out.push(character);
            return;
        }
    }
    push_java_unit(high, out);
    push_java_unit(low, out);
}

fn push_java_unit(unit: u32, out: &mut String) {
    if (0xD800..=0xDFFF).contains(&unit) {
        out.push(UNREPRESENTABLE);
    } else if let Some(character) = char::from_u32(unit) {
        out.push(character);
    } else {
        out.push(UNREPRESENTABLE);
    }
}
