use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, Value, ValueWithSpan,
    VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Span;

const ADD_NAMES: &[&str] = &["timestampadd", "dateadd"];
const DIFF_NAMES: &[&str] = &["timestampdiff", "datediff"];

const UNITS: &[&str] = &[
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "DAYOFYEAR",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MILLISECOND",
    "MICROSECOND",
];

fn unresolved_routine(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}` on search path \
         [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883"
    ))
}

fn quoted_unit_refusal(name: &str, literal: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_PARAMETER_VALUE.DATETIME_UNIT] The value of parameter(s) `unit` in `{name}` \
         is invalid: expects one of the units without quotes YEAR, QUARTER, MONTH, WEEK, DAY, \
         DAYOFYEAR, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, but got the string literal \
         '{literal}'. SQLSTATE: 22023"
    ))
}

fn rewrite_call(name: &str, args: &mut Vec<FunctionArg>) -> Result<()> {
    if args.len() != 3 {
        return Ok(());
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(first)) = &mut args[0] else {
        return Ok(());
    };
    match first {
        Expr::Identifier(ident) if ident.quote_style.is_none() => {
            let upper = ident.value.to_ascii_uppercase();
            if UNITS.contains(&upper.as_str()) {
                *first = Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(upper),
                    span: Span::empty(),
                });
                Ok(())
            } else {
                Err(unresolved_routine(name))
            }
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(literal),
            ..
        }) => Err(quoted_unit_refusal(name, literal)),
        _ => Ok(()),
    }
}

struct BareUnitRewrite;

impl VisitorMut for BareUnitRewrite {
    type Break = DataFusionError;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        let written = function.name.to_string();
        let folded = written.to_ascii_lowercase();
        if !ADD_NAMES.contains(&folded.as_str()) && !DIFF_NAMES.contains(&folded.as_str()) {
            return ControlFlow::Continue(());
        }
        let FunctionArguments::List(list) = &mut function.args else {
            return ControlFlow::Continue(());
        };
        match rewrite_call(&written, &mut list.args) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

pub(crate) fn rewrite_bare_datetime_units(statement: &mut Statement) -> Result<()> {
    match statement.visit(&mut BareUnitRewrite) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn parsed(sql: &str) -> Statement {
        Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    fn rewritten(sql: &str) -> Result<Statement> {
        let mut statement = parsed(sql);
        rewrite_bare_datetime_units(&mut statement)?;
        Ok(statement)
    }

    #[test]
    fn bare_unit_becomes_string_literal() {
        let statement =
            rewritten("SELECT timestampadd(DAY, 1, TIMESTAMP'2024-01-01 00:00:00') AS v").unwrap();
        let text = statement.to_string();
        assert!(text.contains("timestampadd('DAY', 1,"), "{text}");
    }

    #[test]
    fn lowercase_unit_folds_to_upper() {
        let statement =
            rewritten("SELECT timestampadd(day, 1, TIMESTAMP'2024-01-01 00:00:00') AS v").unwrap();
        assert!(statement.to_string().contains("'DAY'"));
    }

    #[test]
    fn dateadd_keeps_name_and_rewrites_unit() {
        let statement =
            rewritten("SELECT dateadd(DAY, 1, TIMESTAMP'2024-01-01 00:00:00') AS v").unwrap();
        let text = statement.to_string();
        assert!(text.contains("dateadd('DAY', 1,"), "{text}");
    }

    #[test]
    fn quoted_unit_refuses_datetime_unit() {
        let error = rewritten("SELECT timestampadd('DAY', 1, TIMESTAMP'2024-01-01 00:00:00') AS v")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("[INVALID_PARAMETER_VALUE.DATETIME_UNIT]")
                && error.contains("got the string literal 'DAY'"),
            "{error}"
        );
    }

    #[test]
    fn unknown_unit_refuses_unresolved_routine() {
        let error =
            rewritten("SELECT timestampadd(FORTNIGHT, 1, TIMESTAMP'2024-01-01 00:00:00') AS v")
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("[UNRESOLVED_ROUTINE]")
                && error.contains("Cannot resolve routine `timestampadd`"),
            "{error}"
        );
    }

    #[test]
    fn two_arg_datediff_is_untouched() {
        let statement = rewritten("SELECT datediff(b, a) AS v").unwrap();
        assert!(!statement.to_string().contains("'B'"));
    }

    #[test]
    fn non_unit_calls_are_untouched() {
        let before = parsed("SELECT a + 1 AS b FROM t");
        let after = rewritten("SELECT a + 1 AS b FROM t").unwrap();
        assert_eq!(before.to_string(), after.to_string());
    }
}
