use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{DataType, Expr, Statement, Visit, Visitor};
use datafusion::sql::sqlparser::parser::ParserError;

pub(crate) fn refuse_uuid_cast(sql: &str, statement: &Statement) -> Result<()> {
    if !has_uuid_cast(statement) {
        return Ok(());
    }
    Err(DataFusionError::SQL(
        Box::new(ParserError::ParserError(uuid_cast_message(sql))),
        None,
    ))
}

fn has_uuid_cast(statement: &Statement) -> bool {
    struct Probe;
    impl Visitor for Probe {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            match expr {
                Expr::Cast {
                    data_type: DataType::Uuid,
                    ..
                } => ControlFlow::Break(()),
                _ => ControlFlow::Continue(()),
            }
        }
    }
    statement.visit(&mut Probe).is_break()
}

fn uuid_cast_message(sql: &str) -> String {
    let offset = uuid_token_offset(sql).unwrap_or(0);
    let line_start = sql[..offset].rfind('\n').map_or(0, |index| index + 1);
    let line = sql[line_start..].split('\n').next().unwrap_or("");
    let line_number = sql[..offset].matches('\n').count() + 1;
    let column = sql[line_start..offset].chars().count() + 1;
    let window_start = column.saturating_sub(1).saturating_sub(32);
    let window_end = column.saturating_sub(1).saturating_add(36);
    let units = utf16_units(line);
    let shown = windowed_line(line, units, window_start, window_end.min(units));
    let caret_pad = if window_start > 0 { 35 } else { column - 1 };
    let caret = format!("{}^^^^", " ".repeat(caret_pad));
    format!(
        "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line {line_number}, position {column}) ==\n{shown}\n{caret}"
    )
}

fn utf16_units(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

fn windowed_line(line: &str, units: usize, start: usize, end: usize) -> String {
    let mut fragment = String::new();
    let mut unit = 0;
    for cell in line.chars() {
        let first = unit;
        unit += cell.len_utf16();
        if unit <= start || first >= end {
            continue;
        }
        if first < start || unit > end {
            fragment.push('?');
        } else {
            fragment.push(cell);
        }
    }
    match (start > 0, end < units) {
        (true, true) => format!("...{fragment}..."),
        (true, false) => format!("...{fragment}"),
        (false, true) => format!("{fragment}..."),
        (false, false) => fragment,
    }
}

fn uuid_token_offset(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut quote: Option<u8> = None;
    while index + 4 <= bytes.len() {
        let byte = bytes[index];
        if let Some(open) = quote {
            if byte == open {
                if bytes.get(index + 1) == Some(&open) {
                    index += 1;
                } else {
                    quote = None;
                }
            }
            index += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' || byte == b'`' {
            quote = Some(byte);
            index += 1;
            continue;
        }
        if bytes[index..index + 4].eq_ignore_ascii_case(b"uuid")
            && !is_word_byte(*bytes.get(index.wrapping_sub(1)).unwrap_or(&b' '))
            && !is_word_byte(*bytes.get(index + 4).unwrap_or(&b' '))
        {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_window_counts_characters() {
        let ascii = "SELECT CAST('a' AS UUID)";
        assert_eq!(
            uuid_cast_message(ascii),
            "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line 1, position 20) ==\nSELECT CAST('a' AS UUID)\n                   ^^^^",
            "pins: uuid-cast-window-1/C-001"
        );
        let wide = format!("SELECT '{}' AS x, CAST('a' AS UUID)", "é".repeat(40));
        assert_eq!(
            uuid_cast_message(&wide),
            format!(
                "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line 1, position 69) ==\n...{}' AS x, CAST('a' AS UUID)\n{}^^^^",
                "é".repeat(12),
                " ".repeat(35)
            ),
            "pins: uuid-cast-window-1/C-001"
        );
        let astral = format!("SELECT '{}' AS x, CAST('a' AS UUID)", "😀".repeat(34));
        assert_eq!(
            uuid_cast_message(&astral),
            format!(
                "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line 1, position 63) ==\n...{}' AS x, CAST('a' AS UU...\n{}^^^^",
                "😀".repeat(23),
                " ".repeat(35)
            ),
            "pins: uuid-cast-window-1/C-001"
        );
    }

    #[test]
    fn uuid_window_does_not_panic_on_non_ascii() {
        let split = format!("SELECT '{}' AS x,\nCAST('a' AS UUID)", "é".repeat(40));
        assert_eq!(
            uuid_cast_message(&split),
            "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line 2, position 13) ==\nCAST('a' AS UUID)\n            ^^^^",
            "pins: uuid-cast-window-1/C-001"
        );
        for head in ["SELECT '", "SELECT  '"] {
            for cell in ["é", "€", "😀"] {
                for count in 0..60 {
                    let sql = format!("{head}{}' AS x, CAST('a' AS UUID)", cell.repeat(count));
                    let token = sql.find("UUID").unwrap_or(0);
                    let position = sql[..token].chars().count() + 1;
                    let header = format!("== SQL (line 1, position {position}) ==");
                    assert!(
                        uuid_cast_message(&sql).contains(&header),
                        "pins: uuid-cast-window-1/C-001"
                    );
                }
            }
        }
    }
}
