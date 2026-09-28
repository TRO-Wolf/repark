use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Ident, Select, SelectItem, Statement, VisitMut, VisitorMut,
};

pub(super) fn is_unique_name_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::Plan(message) => {
            message.starts_with("Projections require unique expression names")
        }
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            is_unique_name_error(inner)
        }
        DataFusionError::Shared(inner) => is_unique_name_error(inner),
        DataFusionError::Collection(errors) => errors.iter().any(is_unique_name_error),
        _ => false,
    }
}

pub(super) fn respell_case_twins(statement: &mut Statement) -> bool {
    let mut respell = TwinsRespell { changed: false };
    let _ = statement.visit(&mut respell);
    respell.changed
}

struct TwinsRespell {
    changed: bool,
}

impl VisitorMut for TwinsRespell {
    type Break = Infallible;

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        if respell_projection(&mut select.projection) {
            self.changed = true;
        }
        ControlFlow::Continue(())
    }
}

fn written_name(item: &SelectItem) -> Option<&str> {
    match item {
        SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => Some(ident.value.as_str()),
        SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
            parts.last().map(|ident| ident.value.as_str())
        }
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.as_str()),
        _ => None,
    }
}

fn respell_projection(projection: &mut [SelectItem]) -> bool {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut folded: HashMap<String, usize> = HashMap::new();
    for item in projection.iter() {
        let Some(name) = written_name(item) else {
            continue;
        };
        *counts.entry(name.to_string()).or_default() += 1;
        *folded.entry(name.to_ascii_lowercase()).or_default() += 1;
    }
    let wildcard = projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    });
    let mut changed = false;
    for item in projection.iter_mut() {
        let Some(name) = written_name(item).map(str::to_string) else {
            continue;
        };
        if counts.get(name.as_str()).is_some_and(|count| *count > 1) {
            continue;
        }
        let twin = wildcard
            || folded
                .get(&name.to_ascii_lowercase())
                .is_some_and(|size| *size > 1);
        if twin {
            changed = quote_item(item, &name) || changed;
        }
    }
    changed
}

fn quote_item(item: &mut SelectItem, name: &str) -> bool {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => {
            if alias.quote_style.is_some() {
                return false;
            }
            alias.quote_style = Some('"');
            true
        }
        SelectItem::UnnamedExpr(expr) => {
            let inner = expr.clone();
            *item = SelectItem::ExprWithAlias {
                expr: inner,
                alias: Ident::with_quote('"', name),
            };
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::{is_unique_name_error, respell_case_twins};

    fn parse(sql: &str) -> datafusion::sql::sqlparser::ast::Statement {
        let dialect = DatabricksDialect {};
        Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("parse statement")
            .parse_statement()
            .expect("one statement")
    }

    fn respelled(sql: &str) -> (String, bool) {
        let mut statement = parse(sql);
        let changed = respell_case_twins(&mut statement);
        (statement.to_string(), changed)
    }

    #[test]
    fn respells_case_twin_outputs() {
        let (text, changed) = respelled("SELECT ID, id FROM t");
        assert!(changed);
        assert_eq!(text, "SELECT ID AS \"ID\", id AS \"id\" FROM t");
        let (text, changed) = respelled("SELECT id AS Id, ID FROM t");
        assert!(changed);
        assert_eq!(text, "SELECT id AS \"Id\", ID AS \"ID\" FROM t");
        let (text, changed) = respelled("SELECT 1 AS a, 2 AS A");
        assert!(changed);
        assert_eq!(text, "SELECT 1 AS \"a\", 2 AS \"A\"");
        let (text, changed) = respelled("SELECT * FROM (SELECT ID, id FROM t)");
        assert!(changed);
        assert_eq!(
            text,
            "SELECT * FROM (SELECT ID AS \"ID\", id AS \"id\" FROM t)"
        );
        let (text, changed) = respelled("SELECT a FROM (SELECT 1 AS a, 2 AS A)");
        assert!(changed);
        assert_eq!(text, "SELECT a FROM (SELECT 1 AS \"a\", 2 AS \"A\")");
    }

    #[test]
    fn respells_a_twin_beside_a_star() {
        let (text, changed) = respelled("SELECT *, ID FROM t");
        assert!(changed);
        assert_eq!(text, "SELECT *, ID AS \"ID\" FROM t");
        let (text, changed) = respelled("SELECT *, data FROM t");
        assert!(changed);
        assert_eq!(text, "SELECT *, data AS \"data\" FROM t");
    }

    #[test]
    fn leaves_identical_written_names_alone() {
        for sql in [
            "SELECT id, id FROM t",
            "SELECT USERID, USERID FROM t",
            "SELECT 1 AS id, 2 AS id",
            "SELECT ID, ID FROM t",
            "SELECT id FROM t",
        ] {
            let (text, changed) = respelled(sql);
            assert!(!changed, "{sql}");
            assert_eq!(text, parse(sql).to_string(), "{sql}");
        }
    }

    #[test]
    fn unique_name_error_matches_the_planner_head() {
        use datafusion::error::DataFusionError;
        let head = "Projections require unique expression names but the expression \"t.id\" at \
                    position 0 and \"t.id\" at position 1 have the same name.";
        let error = DataFusionError::Plan(head.to_string());
        assert!(is_unique_name_error(&error));
        let wrapped = DataFusionError::Context("plan".to_string(), Box::new(error));
        assert!(is_unique_name_error(&wrapped));
        let error = DataFusionError::Plan(head.to_string());
        assert!(is_unique_name_error(&DataFusionError::Collection(vec![
            DataFusionError::Internal("other".to_string()),
            error,
        ])));
        assert!(!is_unique_name_error(&DataFusionError::Plan(
            "nope".to_string()
        )));
        assert!(!is_unique_name_error(&DataFusionError::Internal(
            head.to_string()
        )));
    }
}
