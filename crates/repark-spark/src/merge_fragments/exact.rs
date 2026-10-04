use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr as SqlExpr, Ident, Value, VisitMut, VisitorMut,
};
use repark_common::names::{NameHit, NameRule};
use repark_common::spark_error;

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_fragment_exact(
    sql: &str,
    scopes: &[(&str, &[String])],
    unqualified_scope: Option<&str>,
) -> Result<String> {
    check_scoped_exact(sql, scopes, unqualified_scope, true)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_identity_exact(sql: &str, alias: &str, fields: &[String]) -> Result<String> {
    let scopes = [(alias, fields)];
    check_scoped_exact(sql, &scopes, Some(alias), false)
}

fn check_scoped_exact(
    sql: &str,
    scopes: &[(&str, &[String])],
    unqualified_scope: Option<&str>,
    qualified: bool,
) -> Result<String> {
    let dialect = datafusion::sql::sqlparser::dialect::DatabricksDialect {};
    let mut expr = datafusion::sql::sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|error| DataFusionError::SQL(Box::new(error), None))?
        .parse_expr()
        .map_err(|error| DataFusionError::SQL(Box::new(error), None))?;
    let mut check = ExactCheck {
        scopes,
        unqualified_scope,
        qualified,
        depth: 0,
        field_depth: 0,
        error: None,
    };
    let _ = expr.visit(&mut check);
    if let Some(error) = check.error {
        return Err(error);
    }
    Ok(repark_iceberg::write::sql_text::render_for_reparse(
        &mut expr,
    ))
}

struct ExactCheck<'a> {
    scopes: &'a [(&'a str, &'a [String])],
    unqualified_scope: Option<&'a str>,
    qualified: bool,
    depth: usize,
    field_depth: usize,
    error: Option<DataFusionError>,
}

impl ExactCheck<'_> {
    fn scope_fields(&self, alias: &str) -> Option<&[String]> {
        self.scopes.iter().find_map(|(name, fields)| {
            if NameRule::Exact.matches(trim_alias(name), alias) {
                Some(*fields)
            } else {
                None
            }
        })
    }

    fn scope_folded(&self, alias: &str) -> bool {
        self.scopes
            .iter()
            .any(|(name, _)| NameRule::IgnoreCase.matches(trim_alias(name), alias))
    }

    fn check_ident(&mut self, qualifier: Option<&str>, ident: &mut Ident) {
        if self.error.is_some() {
            return;
        }
        match qualifier {
            Some(scope) => self.check_qualified(scope, ident),
            None => self.check_unqualified(ident),
        }
    }

    fn check_qualified(&mut self, scope: &str, ident: &mut Ident) {
        let written = format!("`{scope}`.`{}`", ident.value);
        let Some(fields) = self.scope_fields(scope) else {
            if self.scope_folded(scope) {
                self.error = Some(self.refusal(&written));
            }
            return;
        };
        match NameRule::Exact.lookup(&ident.value, fields) {
            NameHit::One(_) => quote_ident(ident),
            NameHit::Many(_) | NameHit::None => {}
            NameHit::CaseOnly(_) => {
                self.error = Some(self.refusal(&written));
            }
        }
    }

    fn check_unqualified(&mut self, ident: &mut Ident) {
        let mut exact = 0;
        let mut folded = false;
        for (alias, fields) in self.scopes {
            if let Some(home) = self.unqualified_scope
                && !NameRule::Exact.matches(trim_alias(alias), home)
            {
                continue;
            }
            match NameRule::Exact.lookup(&ident.value, fields) {
                NameHit::One(_) | NameHit::Many(_) => exact += 1,
                NameHit::CaseOnly(_) => folded = true,
                NameHit::None => {}
            }
        }
        if exact == 1 {
            quote_ident(ident);
        } else if exact == 0 && folded {
            let written = format!("`{}`", ident.value);
            self.error = Some(self.refusal(&written));
        }
    }

    fn refusal(&self, written: &str) -> DataFusionError {
        let suggestions = if self.qualified {
            self.scopes
                .iter()
                .flat_map(|(alias, fields)| {
                    fields
                        .iter()
                        .map(|field| format!("`{}`.`{field}`", trim_alias(alias)))
                })
                .collect::<Vec<_>>()
        } else {
            self.scopes
                .iter()
                .flat_map(|(_, fields)| fields.iter())
                .map(|field| format!("`{field}`"))
                .collect::<Vec<_>>()
        }
        .join(", ");
        DataFusionError::Plan(spark_error::message(
            spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
            &[
                ("columnName", written),
                ("suggestions", suggestions.as_str()),
            ],
        ))
    }
}

fn trim_alias(alias: &str) -> &str {
    alias.trim_matches('"')
}

fn quote_ident(ident: &mut Ident) {
    ident.quote_style = Some('`');
}

impl VisitorMut for ExactCheck<'_> {
    type Break = ();

    fn pre_visit_query(
        &mut self,
        _query: &mut datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(
        &mut self,
        _query: &mut datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::CompoundFieldAccess { root, access_chain } = expr {
            self.field_depth += 1;
            if self.depth > 0 || self.error.is_some() {
                return ControlFlow::Continue(());
            }
            if let SqlExpr::Value(value) = root.as_mut()
                && let Value::DoubleQuotedString(qualifier) = &value.value
                && let [AccessExpr::Dot(field)] = access_chain.as_mut_slice()
                && let SqlExpr::Identifier(ident) = field
            {
                self.check_ident(Some(qualifier.as_str()), ident);
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if self.depth > 0 || self.error.is_some() {
            return ControlFlow::Continue(());
        }
        match expr {
            SqlExpr::Identifier(ident) => {
                if self.field_depth == 0 {
                    self.check_ident(None, ident);
                }
            }
            SqlExpr::CompoundIdentifier(parts) => {
                if self.field_depth == 0 && parts.len() == 2 {
                    let qualifier = parts[0].value.clone();
                    self.check_ident(Some(qualifier.as_str()), &mut parts[1]);
                }
            }
            SqlExpr::CompoundFieldAccess { .. } => {
                self.field_depth = self.field_depth.saturating_sub(1);
            }
            _ => {}
        }
        if self.error.is_some() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}
