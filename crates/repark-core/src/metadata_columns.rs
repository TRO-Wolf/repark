use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Ident, ObjectName, ObjectNamePart, Select, SelectItem,
    SelectItemQualifiedWildcardKind, Statement, TableAlias, TableFactor, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::catalog::{
    METADATA_COLUMN_NAMES, MetadataColumnsTableProvider, UNSERVED_METADATA_COLUMN_NAMES,
    metadata_columns_user_field_names,
};

use crate::catalog_state::CatalogRegistry;

const TEMP_VIEW_PREFIX: &str = "__repark_mc_";
static TEMP_VIEW_SEQ: AtomicU64 = AtomicU64::new(0);

fn next_temp_view_name() -> String {
    format!(
        "{TEMP_VIEW_PREFIX}{}",
        TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Debug, Default)]
pub struct MetadataColumnPins {
    names: Vec<String>,
}

impl MetadataColumnPins {
    pub fn push(&mut self, name: String) {
        self.names.push(name);
    }

    pub fn release(&self, ctx: &SessionContext) {
        for name in &self.names {
            let _ = ctx.deregister_table(name.as_str());
        }
    }
}

#[must_use]
pub fn sql_mentions_metadata_columns(sql: &str, dialect: &dyn Dialect) -> bool {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize() else {
        return false;
    };
    tokens.iter().any(|token| match token {
        Token::Word(word) => {
            canonical_metadata_token(&word.value, word.quote_style.is_some()).is_some()
        }
        _ => false,
    })
}

fn canonical_metadata_token(value: &str, quoted: bool) -> Option<&'static str> {
    canonical_token_in(value, quoted, &METADATA_COLUMN_NAMES)
}

fn canonical_unserved_token(value: &str, quoted: bool) -> Option<&'static str> {
    canonical_token_in(value, quoted, &UNSERVED_METADATA_COLUMN_NAMES)
}

fn canonical_token_in(value: &str, quoted: bool, names: &[&'static str]) -> Option<&'static str> {
    names.iter().copied().find(|name| {
        if quoted {
            value == *name
        } else {
            value.eq_ignore_ascii_case(name)
        }
    })
}

fn first_unserved_metadata_column(sql: &str, dialect: &dyn Dialect) -> Option<&'static str> {
    let tokens = Tokenizer::new(dialect, sql).tokenize().ok()?;
    tokens.iter().find_map(|token| match token {
        Token::Word(word) => canonical_unserved_token(&word.value, word.quote_style.is_some()),
        _ => None,
    })
}

fn canonical_metadata_ident(ident: &Ident) -> Option<&'static str> {
    canonical_metadata_token(&ident.value, ident.quote_style.is_some())
}

fn fold_metadata_ident(ident: &mut Ident) {
    if let Some(canonical) = canonical_metadata_ident(ident) {
        ident.value = canonical.to_string();
        ident.quote_style = None;
    }
}

fn refuse(kind: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[ICE-MC-1] a metadata column (_file, _pos, _spec_id) over {kind} \
         is not served; name the columns explicitly on a table relation"
    ))
}

fn refuse_unserved(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[ICE-MC-1] metadata column {name} is not yet served; this layer serves (_file, _pos, _spec_id)"
    ))
}

#[allow(clippy::missing_errors_doc)]
pub async fn prepare_metadata_column_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    dialect: &dyn Dialect,
    pinned: &mut MetadataColumnPins,
) -> Result<Option<String>> {
    let mentions_served = sql_mentions_metadata_columns(sql, dialect);
    let unserved = first_unserved_metadata_column(sql, dialect);
    if !mentions_served && unserved.is_none() {
        return Ok(None);
    }
    let Ok(mut statements) = Parser::parse_sql(dialect, sql) else {
        return Ok(None);
    };
    if statements.len() != 1 {
        return Ok(None);
    }
    let statement = &mut statements[0];
    if !matches!(statement, Statement::Query(_)) {
        return Err(refuse("a non-query statement"));
    }

    let mut collector = CollectTables::default();
    let _ = statement.visit(&mut collector);
    if collector.names.is_empty() {
        return Ok(None);
    }

    let mut rewrites: Vec<Rewrite> = Vec::new();
    for name in collector.names {
        let Some((catalog_name, ident)) = resolve_table_ident(ctx, &name) else {
            continue;
        };
        let Some(catalog) = catalogs.get(&catalog_name) else {
            continue;
        };
        let Ok(table) = catalog.load_table(&ident).await else {
            continue;
        };
        let user_names = metadata_columns_user_field_names(&table);
        let provider = MetadataColumnsTableProvider::try_new(table)?;
        let temp_name = next_temp_view_name();
        let _ = ctx.deregister_table(temp_name.as_str());
        pinned.push(temp_name.clone());
        ctx.register_table(temp_name.as_str(), Arc::new(provider))
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to register metadata-column temp view {temp_name}: {error}"
                ))
            })?;
        let alias = last_ident(&name).unwrap_or_else(|| Ident::new(temp_name.clone()));
        rewrites.push(Rewrite {
            original: name,
            replacement: ObjectName::from(vec![Ident::new(temp_name)]),
            alias,
            user_names,
        });
    }
    if rewrites.is_empty() {
        return Ok(None);
    }
    if let Some(name) = unserved {
        return Err(refuse_unserved(name));
    }

    let mut visitor = RewriteMetadataColumns {
        rewrites,
        failure: None,
    };
    let _ = statement.visit(&mut visitor);
    if let Some(error) = visitor.failure {
        return Err(error);
    }
    Ok(Some(statement.to_string()))
}

#[derive(Default)]
struct CollectTables {
    names: Vec<ObjectName>,
}

impl VisitorMut for CollectTables {
    type Break = std::convert::Infallible;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table {
            name,
            args,
            version,
            ..
        } = table_factor
            && args.is_none()
            && version.is_none()
            && !self.names.iter().any(|seen| seen == name)
        {
            self.names.push(name.clone());
        }
        ControlFlow::Continue(())
    }
}

struct Rewrite {
    original: ObjectName,
    replacement: ObjectName,
    alias: Ident,
    user_names: Vec<String>,
}

struct RewriteMetadataColumns {
    rewrites: Vec<Rewrite>,
    failure: Option<DataFusionError>,
}

impl RewriteMetadataColumns {
    fn find(&self, name: &ObjectName) -> Option<&Rewrite> {
        self.rewrites.iter().find(|entry| &entry.original == name)
    }

    fn by_alias(&self, ident: &Ident) -> Option<&Rewrite> {
        self.rewrites
            .iter()
            .find(|entry| ident_eq(ident, &entry.alias.value))
    }
}

impl VisitorMut for RewriteMetadataColumns {
    type Break = std::convert::Infallible;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, alias, .. } = table_factor
            && let Some(entry) = self.find(name)
        {
            if alias.is_none() {
                *alias = Some(TableAlias {
                    explicit: false,
                    name: entry.alias.clone(),
                    columns: Vec::new(),
                    at: None,
                });
            }
            *name = entry.replacement.clone();
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        if let Err(error) = self.expand_wildcards(select)
            && self.failure.is_none()
        {
            self.failure = Some(error);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::Identifier(ident) => fold_metadata_ident(ident),
            SqlExpr::CompoundIdentifier(idents) => {
                if let Some(last) = idents.last_mut() {
                    fold_metadata_ident(last);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl RewriteMetadataColumns {
    fn expand_wildcards(&self, select: &mut Select) -> Result<()> {
        let sole = self.sole_rewritten_relation(select);
        let touches = self.select_touches_rewrite(select);
        let projection = std::mem::take(&mut select.projection);
        let mut expanded = Vec::with_capacity(projection.len());
        for item in projection {
            match item {
                SelectItem::Wildcard(options) => match sole {
                    Some(entry) => {
                        for column in &entry.user_names {
                            expanded.push(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(
                                vec![entry.alias.clone(), Ident::new(column.clone())],
                            )));
                        }
                    }
                    None if touches => {
                        return Err(refuse("a wildcard over more than one relation"));
                    }
                    None => expanded.push(SelectItem::Wildcard(options)),
                },
                SelectItem::QualifiedWildcard(kind, options) => {
                    let prefix = match &kind {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => last_ident(name),
                        SelectItemQualifiedWildcardKind::Expr(_) => None,
                    };
                    match prefix.as_ref().and_then(|ident| self.by_alias(ident)) {
                        Some(entry) => {
                            for column in &entry.user_names {
                                expanded.push(SelectItem::UnnamedExpr(
                                    SqlExpr::CompoundIdentifier(vec![
                                        entry.alias.clone(),
                                        Ident::new(column.clone()),
                                    ]),
                                ));
                            }
                        }
                        None => expanded.push(SelectItem::QualifiedWildcard(kind, options)),
                    }
                }
                other => expanded.push(other),
            }
        }
        select.projection = expanded;
        Ok(())
    }

    fn sole_rewritten_relation(&self, select: &Select) -> Option<&Rewrite> {
        if select.from.len() != 1 {
            return None;
        }
        let from = select.from.first()?;
        if !from.joins.is_empty() {
            return None;
        }
        match &from.relation {
            TableFactor::Table { name, alias, .. } => self.find(name).or_else(|| {
                alias
                    .as_ref()
                    .and_then(|table_alias| self.by_alias(&table_alias.name))
            }),
            _ => None,
        }
    }

    fn select_touches_rewrite(&self, select: &Select) -> bool {
        select.from.iter().any(|from| {
            let mut relations = vec![&from.relation];
            relations.extend(from.joins.iter().map(|join| &join.relation));
            relations.into_iter().any(|relation| match relation {
                TableFactor::Table { name, alias, .. } => {
                    self.find(name).is_some()
                        || alias
                            .as_ref()
                            .is_some_and(|table_alias| self.by_alias(&table_alias.name).is_some())
                }
                _ => false,
            })
        })
    }
}

fn last_ident(name: &ObjectName) -> Option<Ident> {
    name.0.last().and_then(ObjectNamePart::as_ident).cloned()
}

fn ident_eq(ident: &Ident, other: &str) -> bool {
    if ident.quote_style.is_some() {
        ident.value == other
    } else {
        ident.value.eq_ignore_ascii_case(other)
    }
}

fn object_name_values(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

fn resolve_table_ident(ctx: &SessionContext, name: &ObjectName) -> Option<(String, TableIdent)> {
    let mut parts: Vec<String> = object_name_values(name);
    if parts.is_empty() || parts.iter().any(|part| part.contains('$')) {
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
