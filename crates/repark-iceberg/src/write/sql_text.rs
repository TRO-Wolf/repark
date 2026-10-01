use std::fmt::Display;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{Expr, Value, VisitMut, VisitorMut};

#[must_use]
pub fn render_for_reparse(node: &mut (impl VisitMut + Display + Clone)) -> String {
    let mut work = node.clone();
    let mut collect = CollectStrings::default();
    let _ = work.visit(&mut collect);
    if collect.values.is_empty() {
        return node.to_string();
    }
    let mut rendered = work.to_string();
    for (index, value) in collect.values.iter().enumerate() {
        let placeholder = format!("'__repark_string_{index:06}__'");
        if rendered.matches(&placeholder).count() != 1 {
            return node.to_string();
        }
        rendered = rendered.replacen(&placeholder, &requote(value), 1);
    }
    rendered
}

#[derive(Default)]
struct CollectStrings {
    values: Vec<String>,
}

impl VisitorMut for CollectStrings {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value) = expr
            && let Some(text) = mangled_text(&value.value)
        {
            let placeholder = format!("__repark_string_{:06}__", self.values.len());
            self.values.push(text);
            *expr = Expr::Value(Value::SingleQuotedString(placeholder).with_empty_span());
        }
        ControlFlow::Continue(())
    }
}

fn mangled_text(value: &Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(text) if text.contains('\'') => Some(text.clone()),
        Value::DoubleQuotedString(text) if text.contains('"') => Some(text.clone()),
        _ => None,
    }
}

fn requote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn round_trip(expr: &mut Expr) -> Expr {
        let rendered = render_for_reparse(expr);
        Parser::new(&GenericDialect {})
            .try_with_sql(&rendered)
            .unwrap_or_else(|error| panic!("re-parse of {rendered:?} failed: {error}"))
            .parse_expr()
            .unwrap_or_else(|error| panic!("re-parse of {rendered:?} failed: {error}"))
    }

    fn literal_text(expr: &Expr) -> &str {
        match expr {
            Expr::Value(value) => match &value.value {
                Value::SingleQuotedString(text)
                | Value::NationalStringLiteral(text)
                | Value::DoubleQuotedString(text) => text,
                other => panic!("expected string literal, got {other:?}"),
            },
            other => panic!("expected literal, got {other:?}"),
        }
    }

    fn string_expr(text: &str) -> Expr {
        Expr::Value(Value::SingleQuotedString(text.to_string()).with_empty_span())
    }

    #[test]
    fn quote_pairs_survive_a_display_round_trip() {
        for text in ["it''s", "a''b", "it's", "q'q'q", "x"] {
            let mut expr = string_expr(text);
            assert_eq!(literal_text(&round_trip(&mut expr)), text, "{text:?}");
        }
    }

    #[test]
    fn backslash_quote_survives_a_display_round_trip() {
        for text in ["k\\'m", "a\\b", "\\", "''\\''"] {
            let mut expr = string_expr(text);
            assert_eq!(literal_text(&round_trip(&mut expr)), text, "{text:?}");
        }
    }

    #[test]
    fn double_quoted_pairs_survive_a_display_round_trip() {
        let mut expr =
            Expr::Value(Value::DoubleQuotedString("q\"\"q".to_string()).with_empty_span());
        assert_eq!(literal_text(&round_trip(&mut expr)), "q\"\"q");
        assert_eq!(render_for_reparse(&mut expr), "'q\"\"q'");
    }

    #[test]
    fn quote_free_literals_render_unchanged() {
        let mut expr = string_expr("abc");
        assert_eq!(render_for_reparse(&mut expr), "'abc'");
    }

    #[test]
    fn substituted_text_doubles_every_quote() {
        for (text, rendered) in [
            ("it''s", "'it''''s'"),
            ("it's", "'it''s'"),
            ("k\\'m", "'k\\''m'"),
        ] {
            let mut expr = string_expr(text);
            assert_eq!(render_for_reparse(&mut expr), rendered, "{text:?}");
        }
    }

    #[test]
    fn placeholder_collision_falls_back_to_plain_display() {
        let mut expr = string_expr("x'__repark_string_000000__'y");
        let plain = expr.to_string();
        assert_eq!(render_for_reparse(&mut expr), plain);
    }

    #[test]
    fn nested_literals_survive_a_display_round_trip() {
        let mut expr = Parser::new(&GenericDialect {})
            .try_with_sql("s = 'it''''s' AND t = 'plain'")
            .expect("parse probe")
            .parse_expr()
            .expect("parse probe");
        let reparsed = round_trip(&mut expr);
        match reparsed {
            Expr::BinaryOp { left, right, .. } => {
                match left.as_ref() {
                    Expr::BinaryOp { right, .. } => {
                        assert_eq!(literal_text(right), "it''s");
                    }
                    other => panic!("expected comparison, got {other:?}"),
                }
                match right.as_ref() {
                    Expr::BinaryOp { right, .. } => {
                        assert_eq!(literal_text(right), "plain");
                    }
                    other => panic!("expected comparison, got {other:?}"),
                }
            }
            other => panic!("expected AND, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rendered_literals_plan_as_strings() {
        let mut expr = string_expr("it''s");
        let rendered = render_for_reparse(&mut expr);
        let context = datafusion::prelude::SessionContext::new();
        let batches = context
            .sql(&format!("SELECT {rendered} AS v"))
            .await
            .expect("substituted literal plans")
            .collect()
            .await
            .expect("collect");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("Utf8 column");
        assert_eq!(column.value(0), "it''s");
    }
}
