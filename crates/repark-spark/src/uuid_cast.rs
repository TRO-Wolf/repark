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
    let column = offset - line_start + 1;
    let window_start = column.saturating_sub(1).saturating_sub(32);
    let shown = if window_start > 0 {
        format!("...{}", &line[window_start..])
    } else {
        line.to_string()
    };
    let caret_pad = if window_start > 0 { 35 } else { column - 1 };
    let caret = format!("{}^^^^", " ".repeat(caret_pad));
    format!(
        "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line {line_number}, position {column}) ==\n{shown}\n{caret}"
    )
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
