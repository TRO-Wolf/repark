use super::dtoa::{java_double_text, java_float_text};
use super::tables_doubles::DOUBLE_ROWS;
use super::tables_floats::FLOAT_ROWS;

fn parse_corpus_rows(text: &str, key: &str) -> Vec<(String, String)> {
    let marker = ["\"", key, "\""].concat();
    let start = text.find(&marker).unwrap_or(text.len());
    let bytes = text.as_bytes();
    let mut index = start + marker.len();
    while index < bytes.len() && bytes[index] != b'[' {
        index += 1;
    }
    let mut rows = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut depth = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'"' {
            let mut cell = String::new();
            index += 1;
            while index < bytes.len() && bytes[index] != b'"' {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    index += 1;
                    cell.push(bytes[index] as char);
                } else {
                    cell.push(bytes[index] as char);
                }
                index += 1;
            }
            index += 1;
            if depth == 2 {
                current.push(cell);
                if current.len() == 3 {
                    rows.push((current[1].clone(), current[2].clone()));
                    current.clear();
                }
            }
        } else if byte == b'[' {
            depth += 1;
            index += 1;
        } else if byte == b']' {
            depth = depth.saturating_sub(1);
            index += 1;
            if depth == 0 && !rows.is_empty() {
                break;
            }
        } else {
            index += 1;
        }
    }
    rows
}

fn double_from_le_hex(hex: &str) -> f64 {
    let mut bytes = [0u8; 8];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * index..2 * index + 2], 16).unwrap_or(0);
    }
    f64::from_bits(u64::from_le_bytes(bytes))
}

fn float_from_bits_hex(hex: &str) -> f32 {
    f32::from_bits(u32::from_str_radix(hex, 16).unwrap_or(0))
}

#[test]
fn jdk17_corpus_byte_equal() {
    let Ok(path) = std::env::var("REPARK_JDK17_TOSTRING_CORPUS") else {
        return;
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let doubles = parse_corpus_rows(&text, "double");
    let floats = parse_corpus_rows(&text, "float");
    assert_eq!(doubles.len(), 29_451, "corpus double row count");
    assert_eq!(floats.len(), 9_976, "corpus float row count");
    let mut mismatches: Vec<String> = Vec::new();
    for (hex, expected) in &doubles {
        let rendered = java_double_text(double_from_le_hex(hex));
        if rendered != *expected {
            mismatches.push(["d ", hex, " got ", &rendered, " want ", expected].concat());
        }
    }
    for (hex, expected) in &floats {
        let rendered = java_float_text(float_from_bits_hex(hex));
        if rendered != *expected {
            mismatches.push(["f ", hex, " got ", &rendered, " want ", expected].concat());
        }
    }
    let total = mismatches.len();
    if mismatches.len() > 20 {
        mismatches.truncate(20);
    }
    assert!(
        mismatches.is_empty(),
        "byte mismatches against JDK 17 corpus: {total} rows, first {mismatches:?}"
    );
}

#[test]
fn in_tree_nonshortest_tables_hold() {
    for (hex, expected) in DOUBLE_ROWS {
        assert_eq!(
            java_double_text(double_from_le_hex(hex)),
            *expected,
            "d {hex}"
        );
    }
    for (hex, expected) in FLOAT_ROWS {
        assert_eq!(
            java_float_text(float_from_bits_hex(hex)),
            *expected,
            "f {hex}"
        );
    }
}
