//! Rewrite queries that name v3 lineage columns onto a temp provider that serves them.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, SelectItem, SelectItemQualifiedWildcardKind, Statement,
    TableFactor, VisitMut, VisitorMut, WildcardAdditionalOptions,
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
        Token::Word(word) => word.value == ROW_ID || word.value == LAST_UPDATED,
        _ => false,
    })
}

/// Pin v3 Iceberg FROM tables to a lineage-serving temp view when the query names those columns.
/// # Errors
/// Catalog load or temp-view registration fails.
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
    let mut statements = Parser::parse_sql(dialect, sql).map_err(|error| {
        DataFusionError::Plan(format!("could not parse SQL for lineage rewrite: {error}"))
    })?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let statement = &mut statements[0];
    let tables = collect_table_factors(statement);
    if tables.is_empty() {
        return Ok(None);
    }

    let mut replacements: HashMap<ObjectName, (ObjectName, Vec<String>)> = HashMap::new();
    for (name, _alias) in &tables {
        let Some((catalog_name, ident)) = resolve_table_ident(ctx, name) else {
            continue;
        };
        let Some(catalog) = catalogs.get(&catalog_name) else {
            continue;
        };
        let Ok(table) = catalog.load_table(&ident).await else {
            continue;
        };
        if !table_serves_row_lineage(&table) {
            continue;
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
        replacements.insert(
            name.clone(),
            (ObjectName::from(vec![Ident::new(temp_name)]), user_names),
        );
    }
    if replacements.is_empty() {
        return Ok(None);
    }

    let mut visitor = RewriteLineage {
        replacements: &replacements,
    };
    let _ = statement.visit(&mut visitor);
    Ok(Some(statement.to_string()))
}

fn collect_table_factors(statement: &Statement) -> Vec<(ObjectName, Option<Ident>)> {
    let mut visitor = CollectTables { tables: Vec::new() };
    let mut cloned = statement.clone();
    let _ = cloned.visit(&mut visitor);
    visitor.tables
}

struct CollectTables {
    tables: Vec<(ObjectName, Option<Ident>)>,
}

impl VisitorMut for CollectTables {
    type Break = std::convert::Infallible;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table {
            name, alias, args, ..
        } = table_factor
            && args.is_none()
        {
            let alias_name = alias.as_ref().map(|alias| alias.name.clone());
            self.tables.push((name.clone(), alias_name));
        }
        ControlFlow::Continue(())
    }
}

struct RewriteLineage<'a> {
    replacements: &'a HashMap<ObjectName, (ObjectName, Vec<String>)>,
}

impl VisitorMut for RewriteLineage<'_> {
    type Break = std::convert::Infallible;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, .. } = table_factor
            && let Some((replacement, _)) = self.replacements.get(name)
        {
            *name = replacement.clone();
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(
        &mut self,
        select: &mut datafusion::sql::sqlparser::ast::Select,
    ) -> ControlFlow<Self::Break> {
        expand_wildcards(select, self.replacements);
        ControlFlow::Continue(())
    }
}

fn expand_wildcards(
    select: &mut datafusion::sql::sqlparser::ast::Select,
    replacements: &HashMap<ObjectName, (ObjectName, Vec<String>)>,
) {
    let user_columns: Vec<String> = replacements
        .values()
        .flat_map(|(_, names)| names.iter().cloned())
        .collect();
    if user_columns.is_empty() {
        return;
    }
    let mut expanded = Vec::new();
    for item in select.projection.drain(..) {
        match item {
            SelectItem::Wildcard(_) => {
                for column in &user_columns {
                    expanded.push(SelectItem::UnnamedExpr(
                        datafusion::sql::sqlparser::ast::Expr::Identifier(Ident::new(column)),
                    ));
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let prefix = match &kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => name
                        .0
                        .last()
                        .and_then(ObjectNamePart::as_ident)
                        .map(|ident| ident.value.clone()),
                    SelectItemQualifiedWildcardKind::Expr(_) => None,
                };
                match prefix {
                    Some(alias) => {
                        for column in &user_columns {
                            expanded.push(SelectItem::UnnamedExpr(
                                datafusion::sql::sqlparser::ast::Expr::CompoundIdentifier(vec![
                                    Ident::new(alias.clone()),
                                    Ident::new(column),
                                ]),
                            ));
                        }
                    }
                    None => expanded.push(SelectItem::QualifiedWildcard(
                        kind,
                        WildcardAdditionalOptions::default(),
                    )),
                }
            }
            other => expanded.push(other),
        }
    }
    select.projection = expanded;
}

fn resolve_table_ident(ctx: &SessionContext, name: &ObjectName) -> Option<(String, TableIdent)> {
    let mut parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
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
