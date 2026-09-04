use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::Like;

pub(super) fn rewrite(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::Like(like) = &expr else {
        return Ok(Transformed::no(expr));
    };
    refuse_escape_at_end(like)?;
    Ok(Transformed::no(expr))
}

fn refuse_escape_at_end(like: &Like) -> Result<()> {
    let Some(pattern) = foldable_utf8(like.pattern.as_ref()) else {
        return Ok(());
    };
    let escape = like.escape_char.unwrap_or('\\');
    if !dangling_escape(pattern, escape) {
        return Ok(());
    }
    Err(datafusion::common::DataFusionError::Plan(format!(
        "[INVALID_FORMAT.ESC_AT_THE_END] The format is invalid: '{pattern}'. The escape \
         character is not allowed to end with. SQLSTATE: 42601"
    )))
}

fn foldable_utf8(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)),
            _,
        ) => Some(value.as_str()),
        _ => None,
    }
}

fn dangling_escape(pattern: &str, escape: char) -> bool {
    let mut pending = false;
    for character in pattern.chars() {
        if pending {
            pending = false;
            continue;
        }
        if character == escape {
            pending = true;
        }
    }
    pending
}

#[cfg(test)]
mod tests {
    use super::dangling_escape;

    #[test]
    fn trailing_backslash_is_dangling() {
        assert!(dangling_escape("ab\\", '\\'));
        assert!(!dangling_escape("a\\\\b", '\\'));
        assert!(!dangling_escape("ab", '\\'));
        assert!(dangling_escape("ab#", '#'));
        assert!(!dangling_escape("ab#c", '#'));
    }
}
