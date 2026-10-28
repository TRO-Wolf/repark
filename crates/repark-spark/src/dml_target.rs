use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::ObjectName;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::write_to_branch::{RefSelectorKind, qualify_table_parts, split_write_ref_parts};

pub(crate) struct DmlTarget {
    pub(crate) catalog_name: String,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) ident: TableIdent,
    pub(crate) table: Table,
    pub(crate) on_branch: bool,
}

pub(crate) async fn resolve_dml_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Result<Option<DmlTarget>> {
    let Some(written) = written_parts(name) else {
        return Ok(None);
    };
    let values: Vec<String> = written.iter().map(|(value, _)| value.clone()).collect();
    let (kept, on_branch) = match split_write_ref_parts(&values) {
        Some((_, RefSelectorKind::Tag)) => return Ok(None),
        Some((stripped, RefSelectorKind::Branch(_))) => (stripped.len(), true),
        None => (values.len(), false),
    };
    let normalize = ctx
        .state()
        .config()
        .options()
        .sql_parser
        .enable_ident_normalization;
    let folded = written[..kept]
        .iter()
        .map(|(value, quoted)| fold_part(value, *quoted, normalize))
        .collect();
    let parts = qualify_table_parts(ctx, folded);
    let [catalog_name, namespace @ .., table_name] = parts.as_slice() else {
        return Ok(None);
    };
    let Some(catalog) = catalogs.get(catalog_name) else {
        return Ok(None);
    };
    let Ok(namespace) = NamespaceIdent::from_vec(namespace.to_vec()) else {
        return Ok(None);
    };
    let ident = TableIdent::new(namespace, table_name.clone());
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(Some(DmlTarget {
            catalog_name: catalog_name.clone(),
            catalog: Arc::clone(catalog),
            ident,
            table,
            on_branch,
        })),
        Err(error) => {
            if !planner_resolves(ctx, &parts) && !catalog_lists(catalog.as_ref(), &ident).await {
                return Ok(None);
            }
            Err(DataFusionError::Plan(format!(
                "cannot load the write target `{name}` (resolved as `{}`) to check it before the \
                 write: {error}; the statement is refused before any file is written",
                parts.join(".")
            )))
        }
    }
}

fn written_parts(name: &ObjectName) -> Option<Vec<(String, bool)>> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| (ident.value.clone(), ident.quote_style.is_some()))
        })
        .collect()
}

fn fold_part(value: &str, quoted: bool, normalize: bool) -> String {
    if normalize && !quoted {
        value.to_ascii_lowercase()
    } else {
        value.to_string()
    }
}

fn planner_resolves(ctx: &SessionContext, parts: &[String]) -> bool {
    let [catalog, schema, table] = parts else {
        return false;
    };
    let reference = TableReference::full(catalog.as_str(), schema.as_str(), table.as_str());
    ctx.table_exist(reference).unwrap_or(true)
}

async fn catalog_lists(catalog: &dyn Catalog, ident: &TableIdent) -> bool {
    catalog.table_exists(ident).await.unwrap_or(true)
}
