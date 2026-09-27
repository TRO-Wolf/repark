use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Ident, Query, SelectItem, SetExpr, Statement, TableFactor, Visit, VisitMut,
    Visitor, VisitorMut,
};

pub(super) fn respell_inner_scopes(original: &Statement, folded: &mut Statement) -> bool {
    let mut written = Collector::default();
    let _ = original.visit(&mut written);
    if written.scopes.is_empty() {
        return false;
    }
    let mut probe = Collector::default();
    let _ = (*folded).visit(&mut probe);
    if probe.scopes.len() != written.scopes.len() {
        return false;
    }
    let mut apply = Applier {
        depth: 0,
        scopes: &written.scopes,
        index: 0,
        changed: false,
    };
    let _ = folded.visit(&mut apply);
    apply.changed && apply.index == written.scopes.len()
}

struct WrittenScope {
    items: Vec<Option<(String, bool)>>,
    counts: HashMap<String, usize>,
}

impl WrittenScope {
    fn of(projection: &[SelectItem]) -> Self {
        let mut items = Vec::with_capacity(projection.len());
        let mut counts: HashMap<String, usize> = HashMap::new();
        for item in projection {
            let spelling = match item {
                SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                    Some((ident.value.clone(), true))
                }
                SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts))
                    if !parts.is_empty() =>
                {
                    Some((parts[parts.len() - 1].value.clone(), true))
                }
                SelectItem::ExprWithAlias { alias, .. } => Some((alias.value.clone(), false)),
                _ => None,
            };
            if let Some((name, _)) = &spelling {
                *counts.entry(name.clone()).or_default() += 1;
            }
            items.push(spelling);
        }
        Self { items, counts }
    }
}

fn is_expression_subquery(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Subquery(_) | SqlExpr::InSubquery { .. } | SqlExpr::Exists { .. }
    )
}

fn has_upper_ascii(name: &str) -> bool {
    name.bytes().any(|byte| byte.is_ascii_uppercase())
}

#[derive(Default)]
struct Collector {
    depth: usize,
    scopes: Vec<WrittenScope>,
}

impl Collector {
    fn push_body(&mut self, body: &SetExpr) {
        let mut node = body;
        loop {
            match node {
                SetExpr::Select(select) => {
                    self.scopes.push(WrittenScope::of(&select.projection));
                    return;
                }
                SetExpr::SetOperation { left, .. } => {
                    node = left.as_ref();
                }
                _ => return,
            }
        }
    }

    fn push_ctes(&mut self, query: &Query) {
        if self.depth > 0 {
            return;
        }
        if let Some(with) = query.with.as_ref() {
            for cte in &with.cte_tables {
                self.push_body(&cte.query.body);
            }
        }
    }
}

impl Visitor for Collector {
    type Break = Infallible;

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if is_expression_subquery(expr) {
            self.depth += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if is_expression_subquery(expr) {
            self.depth = self.depth.saturating_sub(1);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.push_ctes(query);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        if self.depth == 0
            && let TableFactor::Derived { subquery, .. } = factor
        {
            self.push_body(&subquery.body);
        }
        ControlFlow::Continue(())
    }
}

struct Applier<'a> {
    depth: usize,
    scopes: &'a [WrittenScope],
    index: usize,
    changed: bool,
}

impl Applier<'_> {
    fn apply_body(&mut self, body: &mut SetExpr) {
        let Some(scope) = self.scopes.get(self.index) else {
            return;
        };
        self.index += 1;
        let mut node = body;
        loop {
            match node {
                SetExpr::Select(select) => {
                    if apply_projection(&mut select.projection, scope) {
                        self.changed = true;
                    }
                    return;
                }
                SetExpr::SetOperation { left, .. } => {
                    node = left.as_mut();
                }
                _ => return,
            }
        }
    }

    fn apply_ctes(&mut self, query: &mut Query) {
        if self.depth > 0 {
            return;
        }
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                self.apply_body(&mut cte.query.body);
            }
        }
    }
}

impl VisitorMut for Applier<'_> {
    type Break = Infallible;

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if is_expression_subquery(expr) {
            self.depth += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if is_expression_subquery(expr) {
            self.depth = self.depth.saturating_sub(1);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.apply_ctes(query);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        let TableFactor::Derived {
            subquery, alias, ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        self.apply_body(&mut subquery.body);
        if let Some(alias) = alias {
            for column in &mut alias.columns {
                if column.name.quote_style.is_none() && has_upper_ascii(&column.name.value) {
                    column.name.quote_style = Some('"');
                    self.changed = true;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn apply_projection(projection: &mut [SelectItem], scope: &WrittenScope) -> bool {
    if projection.len() != scope.items.len() {
        return false;
    }
    let mut changed = false;
    for (item, written) in projection.iter_mut().zip(scope.items.iter()) {
        let Some((spelling, plain)) = written else {
            continue;
        };
        if scope.counts.get(spelling).is_some_and(|count| *count > 1) {
            continue;
        }
        if *plain {
            changed = respell_plain_ref(item, spelling) || changed;
        } else if let SelectItem::ExprWithAlias { alias, .. } = item
            && alias.quote_style.is_none()
            && has_upper_ascii(&alias.value)
        {
            alias.quote_style = Some('"');
            changed = true;
        }
    }
    changed
}

fn respell_plain_ref(item: &mut SelectItem, spelling: &str) -> bool {
    let SelectItem::UnnamedExpr(expr) = item else {
        return false;
    };
    let last = match expr {
        SqlExpr::Identifier(ident) => Some(ident),
        SqlExpr::CompoundIdentifier(parts) if !parts.is_empty() => parts.last_mut(),
        _ => None,
    };
    let Some(last) = last else {
        return false;
    };
    let planned = planned_name(last);
    if spelling == planned {
        return false;
    }
    let inner = expr.clone();
    *item = SelectItem::ExprWithAlias {
        expr: inner,
        alias: Ident::with_quote('"', spelling),
    };
    true
}

fn planned_name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::respell_inner_scopes;

    fn parse(sql: &str) -> datafusion::sql::sqlparser::ast::Statement {
        let dialect = DatabricksDialect {};
        Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("parse statement")
            .parse_statement()
            .expect("one statement")
    }

    fn respelled(original_sql: &str, folded_sql: &str) -> (String, bool) {
        let original = parse(original_sql);
        let mut folded = parse(folded_sql);
        let changed = respell_inner_scopes(&original, &mut folded);
        (folded.to_string(), changed)
    }

    #[test]
    fn respells_derived_and_cte_items_written_in_another_case() {
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT ID, DATA FROM t)",
            "SELECT * FROM (SELECT `id`, `Data` FROM t)",
        );
        assert!(changed);
        assert_eq!(
            text,
            "SELECT * FROM (SELECT `id` AS \"ID\", `Data` AS \"DATA\" FROM t)"
        );
        let (text, changed) = respelled(
            "WITH c AS (SELECT ID, DATA FROM t) SELECT * FROM c",
            "WITH c AS (SELECT `id`, `Data` FROM t) SELECT * FROM c",
        );
        assert!(changed);
        assert_eq!(
            text,
            "WITH c AS (SELECT `id` AS \"ID\", `Data` AS \"DATA\" FROM t) SELECT * FROM c"
        );
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT * FROM (SELECT ID FROM t))",
            "SELECT * FROM (SELECT * FROM (SELECT `id` FROM t))",
        );
        assert!(changed);
        assert_eq!(
            text,
            "SELECT * FROM (SELECT * FROM (SELECT `id` AS \"ID\" FROM t))"
        );
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT 1 AS ID)",
            "SELECT * FROM (SELECT 1 AS ID)",
        );
        assert!(changed);
        assert_eq!(text, "SELECT * FROM (SELECT 1 AS \"ID\")");
    }

    #[test]
    fn leaves_expression_subqueries_alone() {
        let sql = "SELECT ID FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.ID = t.id)";
        let (text, changed) = respelled(sql, sql);
        assert!(!changed);
        assert_eq!(
            text,
            "SELECT ID FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.ID = t.id)"
        );
        let sql = "SELECT Data FROM t WHERE ID IN (SELECT ID FROM u)";
        let (text, changed) = respelled(sql, sql);
        assert!(!changed);
        assert_eq!(text, "SELECT Data FROM t WHERE ID IN (SELECT ID FROM u)");
        let sql = "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM (SELECT ID FROM u) dt)";
        let (text, changed) = respelled(sql, sql);
        assert!(!changed);
        assert_eq!(
            text,
            "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM (SELECT ID FROM u) dt)"
        );
        let sql = "SELECT ID FROM t WHERE ID = (SELECT ID FROM (SELECT ID FROM u) dt)";
        let (text, changed) = respelled(sql, sql);
        assert!(!changed);
        assert_eq!(
            text,
            "SELECT ID FROM t WHERE ID = (SELECT ID FROM (SELECT ID FROM u) dt)"
        );
    }

    #[test]
    fn quotes_an_upper_case_column_alias_list() {
        let sql = "SELECT * FROM (SELECT id FROM t) AS x(Kay)";
        let (text, changed) = respelled(sql, sql);
        assert!(changed);
        assert_eq!(text, "SELECT * FROM (SELECT id FROM t) AS x (\"Kay\")");
    }

    #[test]
    fn leaves_a_spelling_written_twice() {
        let sql = "SELECT * FROM (SELECT id, id FROM t)";
        let (text, changed) = respelled(sql, sql);
        assert!(!changed);
        assert_eq!(text, "SELECT * FROM (SELECT id, id FROM t)");
    }

    #[test]
    fn leaves_a_spelling_written_twice_in_another_case() {
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT ID, ID FROM t)",
            "SELECT * FROM (SELECT `id`, `id` FROM t)",
        );
        assert!(!changed);
        assert_eq!(text, "SELECT * FROM (SELECT `id`, `id` FROM t)");
    }

    #[test]
    fn refuses_unequal_scope_walks() {
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT ID FROM t) a",
            "SELECT * FROM (SELECT `id` FROM t) a, (SELECT `id` FROM u) b",
        );
        assert!(!changed);
        assert_eq!(
            text,
            "SELECT * FROM (SELECT `id` FROM t) a, (SELECT `id` FROM u) b"
        );
    }

    #[test]
    fn keeps_a_quoted_value() {
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT data FROM t)",
            "SELECT * FROM (SELECT `Data` FROM t)",
        );
        assert!(changed);
        assert_eq!(text, "SELECT * FROM (SELECT `Data` AS \"data\" FROM t)");
        let (text, changed) = respelled(
            "SELECT * FROM (SELECT Data FROM t)",
            "SELECT * FROM (SELECT `Data` FROM t)",
        );
        assert!(!changed);
        assert_eq!(text, "SELECT * FROM (SELECT `Data` FROM t)");
    }
}
