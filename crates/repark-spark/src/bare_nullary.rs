use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArguments, Ident, Statement, VisitMut, VisitorMut,
};

const REFUSING: &[&str] = &["localtimestamp", "current_timezone", "now"];

const MAPPED: &[&str] = &[
    "localtimestamp",
    "current_catalog",
    "current_database",
    "current_schema",
    "current_timezone",
    "now",
];

fn missing_field_name(text: &str) -> Option<String> {
    let rest = text.split("No field named ").nth(1)?;
    let end = rest.find(['.', ' ', '"', '\''])?;
    let name = rest[..end].trim();
    (!name.is_empty()).then(|| name.to_string())
}

fn suggestion_list(text: &str) -> Vec<String> {
    let Some(rest) = text.split("Valid fields are ").nth(1) else {
        return Vec::new();
    };
    rest.split(", ")
        .filter_map(|field| {
            let short = field.trim().rsplit('.').next()?.trim().trim_matches('"');
            (!short.is_empty()).then(|| format!("`{short}`"))
        })
        .collect()
}

pub(crate) fn demote_refusing_nullary_calls(statement: &mut Statement) {
    struct Demote;
    impl VisitorMut for Demote {
        type Break = Infallible;
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            if let Expr::Function(function) = expr
                && REFUSING.contains(&function.name.to_string().to_ascii_lowercase().as_str())
                && matches!(function.args, FunctionArguments::None)
            {
                *expr = Expr::Identifier(Ident::new(function.name.to_string()));
            }
            ControlFlow::Continue(())
        }
    }
    let _ = statement.visit(&mut Demote);
}

#[must_use]
pub(crate) fn map_bare_nullary_column_error(error: DataFusionError) -> DataFusionError {
    let text = error.to_string();
    let Some(name) = missing_field_name(&text) else {
        return error;
    };
    if !MAPPED.contains(&name.to_ascii_lowercase().as_str()) {
        return error;
    }
    let candidates = suggestion_list(&text);
    if candidates.is_empty() {
        DataFusionError::Plan(format!(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter \
             with name `{name}` cannot be resolved. SQLSTATE: 42703"
        ))
    } else {
        DataFusionError::Plan(format!(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
             name `{name}` cannot be resolved. Did you mean one of the following? [{}]. \
             SQLSTATE: 42703",
            candidates.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;

    use super::*;

    fn schema_error(name: &str, valid: &[&str]) -> DataFusionError {
        DataFusionError::SchemaError(
            Box::new(datafusion::common::SchemaError::FieldNotFound {
                field: Box::new(datafusion::common::Column::from_name(name)),
                valid_fields: valid
                    .iter()
                    .map(|field| datafusion::common::Column::from_name(*field))
                    .collect(),
            }),
            Box::new(None),
        )
    }

    #[test]
    fn refusing_name_maps_with_candidates() {
        let mapped = map_bare_nullary_column_error(schema_error("localtimestamp", &["t.a", "t.b"]));
        let text = mapped.to_string();
        assert!(
            text.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")
                && text.contains("with name `localtimestamp` cannot be resolved")
                && text.contains("Did you mean one of the following? [")
                && text.contains("SQLSTATE: 42703"),
            "{text}"
        );
    }

    #[test]
    fn frameless_query_refuses_without_suggestion() {
        let mapped = map_bare_nullary_column_error(schema_error("now", &[]));
        let text = mapped.to_string();
        assert!(
            text.contains("[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION]")
                && text.contains("with name `now` cannot be resolved")
                && !text.contains("Did you mean"),
            "{text}"
        );
    }

    #[test]
    fn resolving_name_passes_through() {
        let error = schema_error("current_user", &["t.a"]);
        let text = error.to_string();
        assert_eq!(map_bare_nullary_column_error(error).to_string(), text);
    }

    #[test]
    fn ordinary_column_passes_through() {
        let error = schema_error("revenue", &["t.a"]);
        let text = error.to_string();
        assert_eq!(map_bare_nullary_column_error(error).to_string(), text);
    }

    #[test]
    fn refusing_no_paren_call_demotes_to_identifier() {
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, "SELECT localtimestamp AS v")
            .unwrap()
            .remove(0);
        demote_refusing_nullary_calls(&mut statement);
        assert!(statement.to_string().contains("SELECT localtimestamp AS v"));
        assert!(!format!("{statement:?}").contains("Function(Function"));
    }

    #[test]
    fn parenthesised_call_survives_demotion() {
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        let mut statement =
            Parser::parse_sql(&DatabricksDialect {}, "SELECT localtimestamp() AS v")
                .unwrap()
                .remove(0);
        demote_refusing_nullary_calls(&mut statement);
        assert!(statement.to_string().contains("localtimestamp()"));
    }

    #[test]
    fn parenthesised_current_catalog_survives_demotion() {
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        let mut statement =
            Parser::parse_sql(&DatabricksDialect {}, "SELECT current_catalog() AS v")
                .unwrap()
                .remove(0);
        demote_refusing_nullary_calls(&mut statement);
        assert!(statement.to_string().contains("current_catalog()"));
    }

    #[test]
    fn bare_current_catalog_parses_as_identifier() {
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, "SELECT current_catalog AS v")
            .unwrap()
            .remove(0);
        demote_refusing_nullary_calls(&mut statement);
        assert!(!format!("{statement:?}").contains("Function"));
    }

    #[test]
    fn bare_current_catalog_column_error_stays_mapped() {
        let mapped =
            map_bare_nullary_column_error(schema_error("current_catalog", &["t.a", "t.b"]));
        let text = mapped.to_string();
        assert!(
            text.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")
                && text.contains("with name `current_catalog` cannot be resolved"),
            "{text}"
        );
    }

    #[test]
    fn resolving_no_paren_call_survives_demotion() {
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, "SELECT current_date AS v")
            .unwrap()
            .remove(0);
        demote_refusing_nullary_calls(&mut statement);
        assert!(format!("{statement:?}").contains("Function(Function"));
    }

    #[test]
    fn non_schema_error_passes_through() {
        let error = DataFusionError::Plan("No field named 'revenue'.".to_string());
        let text = error.to_string();
        assert_eq!(map_bare_nullary_column_error(error).to_string(), text);
    }
}
