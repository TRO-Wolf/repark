use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::ViewTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Statement, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::catalog::iceberg_to_datafusion;
use repark_iceberg::view::{ViewReadSpec, view_read_spec};

use crate::catalog_ops::name_parts;
use crate::time_travel::{PinnedViews, prepare_time_travel_sql, sql_has_time_travel};

pub(crate) const MAX_VIEW_EXPANSION_DEPTH: usize = 100;
pub(crate) const VIEW_SUBQUERY_ALIAS: &str = "_repark_view";

pub(crate) struct ViewSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    catalog_name: String,
    catalog: Arc<dyn Catalog>,
    namespace: String,
    ctx: SessionContext,
    catalogs: CatalogRegistry,
}

impl std::fmt::Debug for ViewSchemaProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ViewSchemaProvider")
            .field("catalog_name", &self.catalog_name)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn ensure_view_wrappers(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Result<()> {
    for name in catalogs.registered_catalog_names() {
        let Some(provider) = ctx.catalog(&name) else {
            continue;
        };
        let Some(handle) = catalogs.get(&name) else {
            continue;
        };
        for schema_name in provider.schema_names() {
            let Some(schema) = provider.schema(&schema_name) else {
                continue;
            };
            if let Some(installed) = catalogs.view_wrapper_for(&name, &schema_name)
                && Arc::ptr_eq(&installed, &schema)
            {
                continue;
            }
            let wrapped: Arc<dyn SchemaProvider> = Arc::new(ViewSchemaProvider {
                inner: schema,
                catalog_name: name.clone(),
                catalog: handle.clone(),
                namespace: schema_name.clone(),
                ctx: ctx.clone(),
                catalogs: catalogs.clone(),
            });
            let _ = provider.register_schema(&schema_name, wrapped.clone())?;
            catalogs.note_view_wrapper(&name, &schema_name, wrapped);
        }
    }
    Ok(())
}

#[async_trait]
impl SchemaProvider for ViewSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(provider) = self.inner.table(name).await? {
            return Ok(Some(provider));
        }
        let ident = TableIdent::new(
            NamespaceIdent::new(self.namespace.clone()),
            name.to_string(),
        );
        match self.catalog.view_exists(&ident).await {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error)
                if error.kind() == ErrorKind::NamespaceNotFound
                    || error.kind() == ErrorKind::FeatureUnsupported =>
            {
                return Ok(None);
            }
            Err(error) => return Err(iceberg_to_datafusion(error)),
        }
        let view = self.catalog.load_view(&ident).await.map_err(|error| {
            if error.kind() == ErrorKind::ViewNotFound {
                return DataFusionError::Plan(format!(
                    "view `{}`.`{}`.`{}` was dropped while the query planned",
                    self.catalog_name, self.namespace, name
                ));
            }
            iceberg_to_datafusion(error)
        })?;
        let spec = view_read_spec(&view)?;
        let plan = expand_view_body(&self.ctx, &self.catalogs, &self.catalog_name, &spec).await?;
        Ok(Some(Arc::new(ViewTable::new(plan, Some(spec.sql)))))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
}

async fn expand_view_body(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    spec: &ViewReadSpec,
) -> Result<LogicalPlan> {
    let guard = catalogs.view_expansion_guard();
    if guard.level() > MAX_VIEW_EXPANSION_DEPTH {
        return Err(DataFusionError::Plan(format!(
            "[VIEW_NESTED_DEPTH_LIMIT] View resolution exceeded the maximum nested depth of \
             {MAX_VIEW_EXPANSION_DEPTH}"
        )));
    }
    let stored_catalog = spec
        .default_catalog
        .clone()
        .unwrap_or_else(|| catalog_name.to_string());
    let (prepared, pins) = prepare_view_body_sql(
        ctx,
        catalogs,
        &stored_catalog,
        &spec.default_namespace,
        &spec.sql,
    )
    .await?;
    let aliased = apply_view_aliases(&prepared, &spec.column_names);
    let frame = plan_prepared_body(ctx, catalogs, &aliased, &pins).await?;
    Ok(frame.logical_plan().clone())
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn prepare_view_body_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    stored_catalog: &str,
    stored_namespace: &NamespaceIdent,
    body_sql: &str,
) -> Result<(String, PinnedViews)> {
    let mut pins = PinnedViews::default();
    let mut sql = body_sql.to_string();
    if sql_has_time_travel(&sql)
        && let Some(rewritten) = prepare_time_travel_sql(ctx, catalogs, &sql, &mut pins).await?
    {
        sql = rewritten;
    }
    let qualified =
        qualify_view_body_refs(catalogs, stored_catalog, stored_namespace, &sql).await?;
    Ok((qualified, pins))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn plan_prepared_body(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    pins: &PinnedViews,
) -> Result<DataFrame> {
    let frame = crate::view_dispatch::execute_view_body_query(ctx, catalogs, sql).await;
    pins.release(ctx);
    frame
}

pub(crate) fn apply_view_aliases(sql: &str, column_names: &[String]) -> String {
    if column_names.is_empty() {
        return sql.to_string();
    }
    let aliases = column_names
        .iter()
        .map(|name| format!("\"{}\"", name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT * FROM ({sql}) AS {VIEW_SUBQUERY_ALIAS}({aliases})")
}

struct CteScopes {
    levels: Vec<HashSet<String>>,
}

impl CteScopes {
    fn shadowed(&self, name: &str) -> bool {
        self.levels
            .iter()
            .any(|level| level.contains(&name.to_lowercase()))
    }
}

async fn qualify_view_body_refs(
    catalogs: &CatalogRegistry,
    stored_catalog: &str,
    stored_namespace: &NamespaceIdent,
    sql: &str,
) -> Result<String> {
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).map_err(|error| {
        DataFusionError::Plan(format!("could not parse a stored view body: {error}"))
    })?;
    if statements.len() != 1 {
        return Err(DataFusionError::Plan(
            "a stored view body must hold exactly one statement".to_string(),
        ));
    }
    let mut statement = statements.into_iter().next().ok_or_else(|| {
        DataFusionError::Plan("a stored view body must hold exactly one statement".to_string())
    })?;
    if !matches!(statement, Statement::Query(_)) {
        return Err(DataFusionError::Plan(
            "a view body must hold a single query statement".to_string(),
        ));
    }
    let Some(handle) = catalogs.get(stored_catalog) else {
        return Ok(sql.to_string());
    };
    let mut candidates = Vec::new();
    let mut scopes = CteScopes { levels: Vec::new() };
    if let Statement::Query(query) = &mut statement {
        walk_query_refs(query, &mut scopes, &mut |name: &mut ObjectName| {
            if name_parts(name).len() <= 2 {
                candidates.push(name.clone());
            }
        });
    }
    let mut distinct = candidates.iter().map(name_parts).collect::<Vec<_>>();
    distinct.sort();
    distinct.dedup();
    let mut qualified: HashMap<Vec<String>, ObjectName> = HashMap::new();
    for parts in distinct {
        let (namespace, table) = match parts.as_slice() {
            [table] => (stored_namespace.clone(), table.clone()),
            [namespace, table] => (NamespaceIdent::new(namespace.clone()), table.clone()),
            _ => continue,
        };
        if !reference_is_catalog_object(handle.as_ref(), &namespace, &table).await? {
            continue;
        }
        let Some(original) = candidates
            .iter()
            .find(|candidate| name_parts(candidate) == parts)
        else {
            continue;
        };
        qualified.insert(
            parts,
            qualify_object_name(original, stored_catalog, stored_namespace),
        );
    }
    if qualified.is_empty() {
        return Ok(sql.to_string());
    }
    let mut scopes = CteScopes { levels: Vec::new() };
    if let Statement::Query(query) = &mut statement {
        walk_query_refs(query, &mut scopes, &mut |name: &mut ObjectName| {
            if let Some(replacement) = qualified.get(&name_parts(name)) {
                *name = replacement.clone();
            }
        });
    }
    Ok(statement.to_string())
}

struct ScopedRelations<'a, F: FnMut(&mut ObjectName)> {
    scopes: &'a mut CteScopes,
    sink: &'a mut F,
    depth: usize,
}

impl<F: FnMut(&mut ObjectName)> VisitorMut for ScopedRelations<'_, F> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.depth += 1;
        if self.depth == 2 {
            walk_query_refs(query, self.scopes, self.sink);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, name: &mut ObjectName) -> ControlFlow<()> {
        if self.depth == 1 {
            let parts = name_parts(name);
            if !(parts.len() == 1 && self.scopes.shadowed(&parts[0])) {
                (self.sink)(name);
            }
        }
        ControlFlow::Continue(())
    }
}

fn walk_query_refs<F: FnMut(&mut ObjectName)>(
    query: &mut Query,
    scopes: &mut CteScopes,
    sink: &mut F,
) {
    scopes.levels.push(HashSet::new());
    if let Some(with) = &mut query.with {
        let recursive = with.recursive;
        for table in &mut with.cte_tables {
            let alias = table.alias.name.value.to_lowercase();
            if recursive && let Some(level) = scopes.levels.last_mut() {
                level.insert(alias.clone());
            }
            walk_query_refs(&mut table.query, scopes, sink);
            if let Some(level) = scopes.levels.last_mut() {
                level.insert(alias);
            }
        }
    }
    let with = query.with.take();
    let mut visitor = ScopedRelations {
        scopes,
        sink,
        depth: 0,
    };
    let _ = query.visit(&mut visitor);
    query.with = with;
    visitor.scopes.levels.pop();
}

fn qualify_object_name(
    original: &ObjectName,
    stored_catalog: &str,
    stored_namespace: &NamespaceIdent,
) -> ObjectName {
    let mut parts = vec![ObjectNamePart::Identifier(Ident::new(stored_catalog))];
    if name_parts(original).len() == 1 {
        for level in stored_namespace.as_ref() {
            parts.push(ObjectNamePart::Identifier(Ident::new(level)));
        }
    }
    parts.extend(original.0.iter().cloned());
    ObjectName(parts)
}

async fn reference_is_catalog_object(
    handle: &dyn Catalog,
    namespace: &NamespaceIdent,
    table: &str,
) -> Result<bool> {
    let ident = TableIdent::new(namespace.clone(), table.to_string());
    match handle.table_exists(&ident).await {
        Ok(true) => return Ok(true),
        Ok(false) => {}
        Err(error)
            if error.kind() == ErrorKind::NamespaceNotFound
                || error.kind() == ErrorKind::FeatureUnsupported => {}
        Err(error) => return Err(iceberg_to_datafusion(error)),
    }
    match handle.view_exists(&ident).await {
        Ok(exists) => Ok(exists),
        Err(error)
            if error.kind() == ErrorKind::NamespaceNotFound
                || error.kind() == ErrorKind::FeatureUnsupported =>
        {
            Ok(false)
        }
        Err(error) => Err(iceberg_to_datafusion(error)),
    }
}
