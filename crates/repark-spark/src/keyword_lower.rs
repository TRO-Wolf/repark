use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, TimezoneInfo, UnaryOperator, Value,
    ValueWithSpan, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Span;

fn null_expr() -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Null,
        span: Span::empty(),
    })
}

fn regexp_like_call(left: Box<Expr>, right: Box<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("regexp_like")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right)),
            ],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn lower_expression(node: &mut Expr) {
    if let Expr::RLike {
        negated,
        expr,
        pattern,
        ..
    } = node
    {
        let negated = *negated;
        let left = std::mem::replace(expr, Box::new(null_expr()));
        let right = std::mem::replace(pattern, Box::new(null_expr()));
        let call = regexp_like_call(left, right);
        *node = if negated {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(call),
            }
        } else {
            call
        };
        return;
    }
    if let Expr::Cast { data_type, .. } = node
        && data_type.to_string().eq_ignore_ascii_case("timestamp_ltz")
    {
        *data_type = DataType::Timestamp(None, TimezoneInfo::None);
    }
}

struct KeywordLower;

impl VisitorMut for KeywordLower {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        lower_expression(expr);
        ControlFlow::Continue(())
    }
}

pub(crate) fn lower_spark_keywords(statement: &mut Statement) {
    let _ = statement.visit(&mut KeywordLower);
}

#[must_use]
pub(crate) fn map_unsupported_timestamp_ntz(error: DataFusionError) -> DataFusionError {
    if error
        .to_string()
        .contains("Unsupported SQL type TIMESTAMP_NTZ")
    {
        DataFusionError::Plan(
            "[UNSUPPORTED_TIMESTAMP_NTZ] The data type TIMESTAMP_NTZ is not a SQL-door type: \
             declare TIMESTAMP instead. The naive/instant contract is TZ-6 \
             (docs/spark-sql-iceberg-parity.md). SQLSTATE: 0A000"
                .to_string(),
        )
    } else {
        error
    }
}

pub(crate) fn map_door_keyword_errors(error: DataFusionError) -> DataFusionError {
    map_unsupported_timestamp_ntz(crate::bare_nullary::map_bare_nullary_column_error(error))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn lowered(sql: &str) -> Statement {
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap()
            .remove(0);
        lower_spark_keywords(&mut statement);
        statement
    }

    #[test]
    fn rlike_lowers_to_regexp_like() {
        let text = lowered("SELECT 'abc' RLIKE '^a' AS v").to_string();
        assert!(text.contains("regexp_like('abc', '^a')"), "{text}");
    }

    #[test]
    fn not_rlike_lowers_to_not_regexp_like() {
        let text = lowered("SELECT 'abc' NOT RLIKE '^a' AS v").to_string();
        assert!(text.contains("NOT regexp_like('abc', '^a')"), "{text}");
    }

    #[test]
    fn ltz_cast_lowers_to_bare_timestamp() {
        let text = lowered("SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_LTZ) AS v").to_string();
        assert!(!text.contains("TIMESTAMP_LTZ"), "{text}");
        assert!(text.contains("TIMESTAMP"), "{text}");
    }

    #[test]
    fn other_casts_are_untouched() {
        let before = "SELECT CAST('1' AS INT) AS v";
        assert_eq!(lowered(before).to_string(), before);
    }

    #[test]
    fn ntz_refusal_names_the_registry_row() {
        let mapped = map_unsupported_timestamp_ntz(DataFusionError::NotImplemented(
            "This feature is not implemented: Unsupported SQL type TIMESTAMP_NTZ".to_string(),
        ));
        let text = mapped.to_string();
        assert!(
            text.contains("[UNSUPPORTED_TIMESTAMP_NTZ]") && text.contains("TZ-6"),
            "{text}"
        );
    }

    #[test]
    fn unrelated_errors_pass_through() {
        let error = DataFusionError::Plan("No field named 'revenue'.".to_string());
        let text = error.to_string();
        assert_eq!(map_door_keyword_errors(error).to_string(), text);
    }
}
