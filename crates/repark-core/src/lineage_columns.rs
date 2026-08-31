//! Rewrite queries that name v3 lineage columns onto a temp provider that serves them.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Statement, TableAlias, TableFactor, VisitMut,
    VisitorMut,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::catalog::{
    LineageColumnsTableProvider, table_serves_row_lineage, user_field_names,
};

use crate::catalog_state::CatalogRegistry;
use crate::time_travel::next_temp_view_name;

const ROW_ID: &str = "_row_id";
const LAST_UPDATED: &str = "_last_updated_sequence_number";
const TIME_TRAVEL_TEMP_PREFIX: &str = "__repark_tt_";
const ANSI_TIME_TRAVEL_TEMP_PREFIX: &str = "__repark_ansi_tt_";

/// Names registered for one statement so the door can drop them after planning.
#[derive(Debug, Default)]
pub struct LineagePins {
    names: Vec<String>,
}

impl LineagePins {
    /// Record a temp-view name for later release.
    pub fn push(&mut self, name: String) {
        self.names.push(name);
    }

    /// Deregister every name this statement minted.
    pub fn release(&self, ctx: &SessionContext) {
        for name in &self.names {
            let _ = ctx.deregister_table(name.as_str());
        }
    }
}

/// Whether `sql` names `_row_id` or `_last_updated_sequence_number` as an identifier.
#[must_use]
pub fn sql_mentions_lineage_columns(sql: &str, dialect: &dyn Dialect) -> bool {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize() else {
        return false;
    };
    tokens.iter().any(|token| match token {
        Token::Word(word) => {
            canonical_lineage_token(&word.value, word.quote_style.is_some()).is_some()
        }
        _ => false,
    })
}

/// Pin v3 Iceberg FROM tables to a lineage-serving temp view when the query names those columns.
///
/// The rewrite fires only for a single-table statement. JOIN, CTE, subquery, and time-travel
/// forms that name a lineage column refuse loud (`V3-ROWID-2`).
///
/// # Errors
/// Catalog load, composed-statement refusal, or temp-view registration fails.
pub async fn prepare_lineage_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    dialect: &dyn Dialect,
    pinned: &mut LineagePins,
) -> Result<Option<String>> {
    if !sql_mentions_lineage_columns(sql, dialect) {
        return Ok(None);
    }
    if sql_has_as_of_clause(sql, dialect) {
        return Err(refuse_composed("time-travel"));
    }
    let mut statements = Parser::parse_sql(dialect, sql).map_err(|error| {
        DataFusionError::Plan(format!("could not parse SQL for lineage rewrite: {error}"))
    })?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let statement = &mut statements[0];
    let Some(target) = classify_single_table(statement)? else {
        return Ok(None);
    };

    let Some((catalog_name, ident)) = resolve_table_ident(ctx, &target.name) else {
        return Ok(None);
    };
    let Some(catalog) = catalogs.get(&catalog_name) else {
        return Ok(None);
    };
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(None);
    };
    if !table_serves_row_lineage(&table) {
        return Ok(None);
    }
    let user_names = user_field_names(&table);
    let provider = LineageColumnsTableProvider::try_new(table)?;
    let temp_name = next_temp_view_name();
    let _ = ctx.deregister_table(temp_name.as_str());
    pinned.push(temp_name.clone());
    ctx.register_table(temp_name.as_str(), Arc::new(provider))
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register lineage temp view {temp_name}: {error}"
            ))
        })?;

    let alias = target.alias.unwrap_or_else(|| {
        last_ident(&target.name).unwrap_or_else(|| Ident::new(temp_name.clone()))
    });
    let mut visitor = RewriteLineage {
        original: target.name,
        replacement: ObjectName::from(vec![Ident::new(temp_name)]),
        alias,
        user_names,
    };
    let _ = statement.visit(&mut visitor);
    Ok(Some(statement.to_string()))
}

fn refuse_composed(kind: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[V3-ROWID-2] lineage projection over {kind} is not yet served; single-table reads are"
    ))
}

fn sql_has_as_of_clause(sql: &str, dialect: &dyn Dialect) -> bool {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize() else {
        return false;
    };
    let words: Vec<&str> = tokens
        .iter()
        .filter_map(|token| match token {
            Token::Word(word) => Some(word.value.as_str()),
            _ => None,
        })
        .collect();
    words
        .windows(2)
        .any(|pair| pair[0].eq_ignore_ascii_case("AS") && pair[1].eq_ignore_ascii_case("OF"))
}

struct SingleTable {
    name: ObjectName,
    alias: Option<Ident>,
}

fn classify_single_table(statement: &Statement) -> Result<Option<SingleTable>> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    classify_query(query)
}

fn classify_query(query: &Query) -> Result<Option<SingleTable>> {
    if query.with.is_some() {
        return Err(refuse_composed("CTEs"));
    }
    if query_wraps_lineage(query) {
        return Err(refuse_composed("subqueries"));
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => classify_select(select),
        SetExpr::Query(inner) => classify_query(inner),
        SetExpr::SetOperation { .. } => Err(refuse_composed("subqueries")),
        _ => Ok(None),
    }
}

fn classify_select(select: &Select) -> Result<Option<SingleTable>> {
    if select.from.len() > 1 {
        return Err(refuse_composed("joins"));
    }
    let Some(from) = select.from.first() else {
        return Ok(None);
    };
    if !from.joins.is_empty() {
        return Err(refuse_composed("joins"));
    }
    match &from.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            version,
            ..
        } => {
            if args.is_some() {
                return Err(refuse_composed("subqueries"));
            }
            if version.is_some() {
                return Err(refuse_composed("time-travel"));
            }
            if is_time_travel_temp_name(name) {
                return Err(refuse_composed("time-travel"));
            }
            Ok(Some(SingleTable {
                name: name.clone(),
                alias: alias.as_ref().map(|table_alias| table_alias.name.clone()),
            }))
        }
        TableFactor::NestedJoin { .. } => Err(refuse_composed("joins")),
        _ => Err(refuse_composed("subqueries")),
    }
}

fn query_wraps_lineage(query: &Query) -> bool {
    let mut finder = SubqueryFinder { found: false };
    let mut cloned = query.clone();
    let _ = cloned.visit(&mut finder);
    finder.found
}

struct SubqueryFinder {
    found: bool,
}

impl VisitorMut for SubqueryFinder {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if matches!(table_factor, TableFactor::Derived { .. }) {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::Subquery(_) | SqlExpr::Exists { .. } | SqlExpr::InSubquery { .. } => {
                self.found = true;
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

fn is_time_travel_temp_name(name: &ObjectName) -> bool {
    let Some(leaf) = last_ident(name) else {
        return false;
    };
    leaf.value.starts_with(TIME_TRAVEL_TEMP_PREFIX)
        || leaf.value.starts_with(ANSI_TIME_TRAVEL_TEMP_PREFIX)
}

fn last_ident(name: &ObjectName) -> Option<Ident> {
    name.0.last().and_then(ObjectNamePart::as_ident).cloned()
}

fn canonical_lineage_token(value: &str, quoted: bool) -> Option<&'static str> {
    if quoted {
        if value == ROW_ID {
            Some(ROW_ID)
        } else if value == LAST_UPDATED {
            Some(LAST_UPDATED)
        } else {
            None
        }
    } else if value.eq_ignore_ascii_case(ROW_ID) {
        Some(ROW_ID)
    } else if value.eq_ignore_ascii_case(LAST_UPDATED) {
        Some(LAST_UPDATED)
    } else {
        None
    }
}

fn canonical_lineage_ident(ident: &Ident) -> Option<&'static str> {
    canonical_lineage_token(&ident.value, ident.quote_style.is_some())
}

fn fold_lineage_ident(ident: &mut Ident) {
    if let Some(canonical) = canonical_lineage_ident(ident) {
        ident.value = canonical.to_string();
        ident.quote_style = None;
    }
}

struct RewriteLineage {
    original: ObjectName,
    replacement: ObjectName,
    alias: Ident,
    user_names: Vec<String>,
}

impl VisitorMut for RewriteLineage {
    type Break = std::convert::Infallible;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, alias, .. } = table_factor
            && name == &self.original
        {
            if alias.is_none() {
                *alias = Some(TableAlias {
                    explicit: false,
                    name: self.alias.clone(),
                    columns: Vec::new(),
                    at: None,
                });
            }
            *name = self.replacement.clone();
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        expand_wildcards(select, &self.user_names, &self.alias, &self.original);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::Identifier(ident) => fold_lineage_ident(ident),
            SqlExpr::CompoundIdentifier(idents) => {
                rewrite_compound(idents, &self.original, &self.alias);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn expand_wildcards(
    select: &mut Select,
    user_names: &[String],
    alias: &Ident,
    original: &ObjectName,
) {
    if user_names.is_empty() {
        return;
    }
    let table_parts = object_name_values(original);
    let mut expanded = Vec::new();
    for item in select.projection.drain(..) {
        match item {
            SelectItem::Wildcard(_) => {
                for column in user_names {
                    expanded.push(SelectItem::UnnamedExpr(SqlExpr::Identifier(Ident::new(
                        column,
                    ))));
                }
            }
            SelectItem::QualifiedWildcard(kind, options) => {
                let prefix = match &kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        name.0.last().and_then(ObjectNamePart::as_ident).cloned()
                    }
                    SelectItemQualifiedWildcardKind::Expr(_) => None,
                };
                let matches_table = prefix.as_ref().is_some_and(|ident| {
                    ident_eq(ident, &alias.value)
                        || table_parts.last().is_some_and(|leaf| ident_eq(ident, leaf))
                });
                if matches_table {
                    for column in user_names {
                        expanded.push(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                            alias.clone(),
                            Ident::new(column),
                        ])));
                    }
                } else {
                    expanded.push(SelectItem::QualifiedWildcard(kind, options));
                }
            }
            other => expanded.push(other),
        }
    }
    select.projection = expanded;
}

fn rewrite_compound(idents: &mut Vec<Ident>, original: &ObjectName, alias: &Ident) {
    if idents.is_empty() {
        return;
    }
    let Some(last) = idents.last_mut() else {
        return;
    };
    fold_lineage_ident(last);
    if idents.len() < 2 {
        return;
    }
    let prefix = &idents[..idents.len() - 1];
    if !prefix_matches_table(prefix, original, alias) {
        return;
    }
    let last = idents.last().cloned().unwrap_or_else(|| Ident::new(ROW_ID));
    idents.clear();
    idents.push(alias.clone());
    idents.push(last);
}

fn prefix_matches_table(prefix: &[Ident], original: &ObjectName, alias: &Ident) -> bool {
    if prefix.len() == 1 && ident_eq(&prefix[0], &alias.value) {
        return true;
    }
    let table_parts = object_name_values(original);
    for start in 0..table_parts.len() {
        let suffix = &table_parts[start..];
        if suffix.len() == prefix.len()
            && suffix
                .iter()
                .zip(prefix.iter())
                .all(|(part, ident)| ident_eq(ident, part))
        {
            return true;
        }
    }
    false
}

fn object_name_values(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

fn ident_eq(ident: &Ident, other: &str) -> bool {
    if ident.quote_style.is_some() {
        ident.value == other
    } else {
        ident.value.eq_ignore_ascii_case(other)
    }
}

fn resolve_table_ident(ctx: &SessionContext, name: &ObjectName) -> Option<(String, TableIdent)> {
    let mut parts: Vec<String> = object_name_values(name);
    if parts.is_empty() {
        return None;
    }
    if parts.len() < 3 {
        let options = ctx.copied_config().options().catalog.clone();
        if parts.len() == 1 {
            parts.insert(0, options.default_schema);
        }
        parts.insert(0, options.default_catalog);
    }
    let namespace = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()).ok()?;
    let table_leaf = parts[parts.len() - 1].clone();
    Some((parts[0].clone(), TableIdent::new(namespace, table_leaf)))
}
