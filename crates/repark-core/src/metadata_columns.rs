use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, Select,
    SelectItem, SelectItemQualifiedWildcardKind, Statement, TableAlias, TableFactor, VisitMut,
    VisitorMut,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::metadata_columns::RESERVED_COL_NAME_FILE;
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::catalog::{
    METADATA_COLUMN_NAMES, MetadataColumnsTableProvider, metadata_columns_user_field_names,
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

fn canonical_token_in(value: &str, quoted: bool, names: &[&'static str]) -> Option<&'static str> {
    names.iter().copied().find(|name| {
        if quoted {
            value == *name
        } else {
            value.eq_ignore_ascii_case(name)
        }
    })
}

const INPUT_FILE_NAME: &str = "input_file_name";

const INPUT_FILE_NAME_BLOCKING_AGGREGATES: [&str; 11] = [
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "collect_list",
    "collect_set",
    "first",
    "last",
    "any_value",
    "approx_count_distinct",
];

fn sql_mentions_input_file_name_call(sql: &str, dialect: &dyn Dialect) -> bool {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize() else {
        return false;
    };
    tokens.iter().enumerate().any(|(index, token)| {
        let Token::Word(word) = token else {
            return false;
        };
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case(INPUT_FILE_NAME) {
            return false;
        }
        tokens[index + 1..]
            .iter()
            .find(|next| !matches!(next, Token::Whitespace(_)))
            .is_some_and(|next| matches!(next, Token::LParen))
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
        "[ICE-MC-1] a metadata column (_file, _pos, _spec_id, _partition, _deleted) over {kind} \
         is not served; name the columns explicitly on a table relation"
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
    if !mentions_served && !sql_mentions_input_file_name_call(sql, dialect) {
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
        return if mentions_served {
            Err(refuse("a non-query statement"))
        } else {
            Ok(None)
        };
    }

    let mut cte_names = CollectCteNames::default();
    let _ = statement.visit(&mut cte_names);
    let mut collector = CollectTables {
        names: Vec::new(),
        cte_names: cte_names.names,
    };
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
struct CollectCteNames {
    names: Vec<String>,
}

impl VisitorMut for CollectCteNames {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.names.push(normalized_ident_value(&cte.alias.name));
            }
        }
        ControlFlow::Continue(())
    }
}

#[derive(Default)]
struct CollectTables {
    names: Vec<ObjectName>,
    cte_names: Vec<String>,
}

impl CollectTables {
    fn is_cte_reference(&self, name: &ObjectName) -> bool {
        single_ident(name).is_some_and(|ident| {
            self.cte_names
                .iter()
                .any(|cte| *cte == normalized_ident_value(ident))
        })
    }
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
            && !self.is_cte_reference(name)
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
        self.rewrite_input_file_names(select);
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

    fn rewrite_input_file_names(&self, select: &mut Select) {
        let Some(qualifier) = self.sole_relation_qualifier(select) else {
            return;
        };
        let mut visitor = InputFileNameCalls {
            qualifier: &qualifier,
            blocked: 0,
        };
        for item in &mut select.projection {
            if let SelectItem::UnnamedExpr(SqlExpr::Function(function)) = item
                && is_input_file_name_call(function)
            {
                *item = SelectItem::ExprWithAlias {
                    expr: input_file_name_expr(&qualifier),
                    alias: Ident::with_quote('`', "input_file_name()"),
                };
                continue;
            }
            let _ = item.visit(&mut visitor);
        }
        let _ = select.selection.visit(&mut visitor);
    }

    fn sole_relation_qualifier(&self, select: &Select) -> Option<Ident> {
        let entry = self.sole_input_file_name_relation(select)?;
        let qualifier = match &select.from.first()?.relation {
            TableFactor::Table {
                alias: Some(table_alias),
                ..
            } => table_alias.name.clone(),
            _ => entry.alias.clone(),
        };
        Some(qualifier)
    }

    fn sole_input_file_name_relation(&self, select: &Select) -> Option<&Rewrite> {
        if select.from.len() != 1 {
            return None;
        }
        let from = select.from.first()?;
        if !from.joins.is_empty() {
            return None;
        }
        match &from.relation {
            TableFactor::Table { name, .. } => self.find(name),
            _ => None,
        }
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

struct InputFileNameCalls<'a> {
    qualifier: &'a Ident,
    blocked: usize,
}

impl VisitorMut for InputFileNameCalls<'_> {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.blocked += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.blocked -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        let SqlExpr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        if self.blocked == 0 && is_input_file_name_call(function) {
            *expr = input_file_name_expr(self.qualifier);
        } else if is_blocking_aggregate(&function.name) {
            self.blocked += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::Function(function) = expr
            && is_blocking_aggregate(&function.name)
        {
            self.blocked -= 1;
        }
        ControlFlow::Continue(())
    }
}

fn single_ident(name: &ObjectName) -> Option<&Ident> {
    match name.0.as_slice() {
        [part] => part.as_ident(),
        _ => None,
    }
}

fn is_input_file_name_call(function: &Function) -> bool {
    single_ident(&function.name).is_some_and(|ident| {
        ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case(INPUT_FILE_NAME)
    }) && matches!(
        &function.args,
        FunctionArguments::List(list)
            if list.args.is_empty()
                && list.clauses.is_empty()
                && list.duplicate_treatment.is_none()
    ) && matches!(&function.parameters, FunctionArguments::None)
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
        && !function.uses_odbc_syntax
}

fn is_blocking_aggregate(name: &ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .is_some_and(|ident| {
            INPUT_FILE_NAME_BLOCKING_AGGREGATES
                .iter()
                .any(|aggregate| ident.value.eq_ignore_ascii_case(aggregate))
        })
}

fn input_file_name_expr(qualifier: &Ident) -> SqlExpr {
    SqlExpr::CompoundIdentifier(vec![qualifier.clone(), Ident::new(RESERVED_COL_NAME_FILE)])
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

fn normalized_ident_value(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
    use tempfile::TempDir;

    use super::{MetadataColumnPins, TEMP_VIEW_PREFIX, prepare_metadata_column_sql};
    use crate::catalog_state::{CatalogRegistry, LocationPolicy};

    async fn defaulted_iceberg_table() -> (SessionContext, CatalogRegistry, TempDir) {
        let warehouse = TempDir::new().unwrap();
        let root = warehouse.path().to_str().unwrap().to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), root.clone())]),
                )
                .await
                .unwrap(),
        );
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        for namespace in ["public", "ns"] {
            let ident = NamespaceIdent::new(namespace.to_string());
            catalog
                .create_namespace(&ident, HashMap::new())
                .await
                .unwrap();
            let creation = TableCreation::builder()
                .name("t".to_string())
                .location(format!("{root}/{namespace}/t"))
                .schema(schema.clone())
                .properties(HashMap::new())
                .build();
            catalog.create_table(&ident, creation).await.unwrap();
        }
        let mut catalogs = CatalogRegistry::new();
        catalogs.insert(
            "datafusion".to_string(),
            catalog,
            LocationPolicy::TempFallbackAllowed {
                root: warehouse.path().to_path_buf(),
            },
        );
        (SessionContext::new(), catalogs, warehouse)
    }

    async fn prepared(
        ctx: &SessionContext,
        catalogs: &CatalogRegistry,
        sql: &str,
    ) -> Option<String> {
        let mut pinned = MetadataColumnPins::default();
        prepare_metadata_column_sql(ctx, catalogs, sql, &GenericDialect, &mut pinned)
            .await
            .unwrap()
    }

    fn temp_view_count(rewritten: &str) -> usize {
        rewritten.matches(TEMP_VIEW_PREFIX).count()
    }

    #[tokio::test]
    async fn a_one_part_cte_name_is_never_collected_as_the_table() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH t AS (SELECT id, 'x' AS _file FROM datafusion.public.t) \
             SELECT input_file_name() FROM t",
        )
        .await
        .expect("the qualified reference inside the CTE still rewrites");
        assert!(
            rewritten.ends_with("FROM t"),
            "the outer one-part CTE reference must not become the physical table: {rewritten}"
        );
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "only the qualified reference collects the table: {rewritten}"
        );
    }

    #[tokio::test]
    async fn a_metadata_column_projection_over_a_cte_is_never_served_from_the_table() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH t AS (SELECT id, 'x' AS _file FROM datafusion.public.t) SELECT _file FROM t",
        )
        .await
        .expect("the qualified reference inside the CTE still rewrites");
        assert!(
            rewritten.ends_with("FROM t"),
            "the outer _file projection must resolve on the CTE: {rewritten}"
        );
        assert_eq!(temp_view_count(&rewritten), 1, "{rewritten}");
    }

    #[tokio::test]
    async fn a_one_part_name_matching_no_cte_is_still_collected() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH c AS (SELECT 1 AS one) SELECT input_file_name() FROM t",
        )
        .await
        .expect("a bare name without a matching CTE still resolves to the table");
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "the physical table stays rewritten: {rewritten}"
        );
        assert!(
            rewritten.contains("._file"),
            "input_file_name() rewrites onto the served table: {rewritten}"
        );
    }

    #[tokio::test]
    async fn a_qualified_name_matching_a_cte_is_still_collected() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH t AS (SELECT 1 AS one) SELECT input_file_name() FROM datafusion.public.t",
        )
        .await
        .expect("a qualified name is never a CTE reference");
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "the qualified table stays rewritten: {rewritten}"
        );
    }

    #[tokio::test]
    async fn a_two_part_name_matching_a_cte_is_still_collected() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH t AS (SELECT 1 AS one) SELECT input_file_name() FROM ns.t",
        )
        .await
        .expect("a two-part name is never a CTE reference");
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "the two-part table stays rewritten: {rewritten}"
        );
    }

    #[tokio::test]
    async fn a_nested_cte_reference_inside_another_cte_is_never_collected() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH t AS (SELECT id FROM datafusion.public.t), u AS (SELECT * FROM t) \
             SELECT input_file_name() FROM u",
        )
        .await
        .expect("the qualified reference inside the first CTE still rewrites");
        assert!(
            rewritten.ends_with("FROM u"),
            "the outer CTE reference must not become the physical table: {rewritten}"
        );
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "the nested one-part reference stays on the CTE: {rewritten}"
        );
    }

    #[tokio::test]
    async fn a_cte_reference_inside_a_subquery_is_never_collected() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH t AS (SELECT id FROM datafusion.public.t) \
             SELECT input_file_name() FROM (SELECT * FROM t) AS dt",
        )
        .await
        .expect("the qualified reference inside the CTE still rewrites");
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "the subquery's one-part reference stays on the CTE: {rewritten}"
        );
    }

    #[tokio::test]
    async fn a_quoted_reference_matches_the_unquoted_cte_after_folding() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        assert!(
            prepared(
                &ctx,
                &catalogs,
                "WITH T AS (SELECT 1 AS one) SELECT input_file_name() FROM `t`",
            )
            .await
            .is_none(),
            "a quoted one-part reference still resolves to the CTE"
        );
    }

    #[tokio::test]
    async fn a_quoted_cte_alias_does_not_shield_a_differently_folded_table() {
        let (ctx, catalogs, _warehouse) = defaulted_iceberg_table().await;
        let rewritten = prepared(
            &ctx,
            &catalogs,
            "WITH `T` AS (SELECT 1 AS one) SELECT input_file_name() FROM t",
        )
        .await
        .expect("an unquoted reference cannot reach a quoted CTE alias");
        assert_eq!(
            temp_view_count(&rewritten),
            1,
            "the physical table stays rewritten: {rewritten}"
        );
    }
}
