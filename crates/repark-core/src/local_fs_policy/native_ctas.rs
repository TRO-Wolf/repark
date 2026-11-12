use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::{CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;

use crate::catalog_state::CatalogRegistry;

pub(crate) const NATIVE_CTAS_CARD: &str = "NATIVE-CTAS-ICEBERG-REFUSE-1";

pub(crate) fn refusal_message(target: &str) -> String {
    format!(
        "repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table \
         {target}: create the table with its columns, CREATE TABLE {target} (<columns>), then \
         INSERT INTO {target} SELECT ...; or run the same statement through spark.sql \
         ({NATIVE_CTAS_CARD})"
    )
}

pub(crate) fn refuse_iceberg_ctas(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    let mut refused = None;
    plan.apply(|node| {
        if let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) = node
            && carries_query(create)
            && let Some(target) = refused_target(ctx, catalogs, create)
        {
            refused = Some(target);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    match refused {
        Some(target) => Err(repark_iceberg::write::unsupported_error(refusal_message(
            &target,
        ))),
        None => Ok(()),
    }
}

fn carries_query(create: &CreateMemoryTable) -> bool {
    !matches!(
        create.input.as_ref(),
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            ..
        })
    )
}

fn refused_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    create: &CreateMemoryTable,
) -> Option<String> {
    let config = ctx.copied_config();
    let options = &config.options().catalog;
    let resolved = create
        .name
        .clone()
        .resolve(&options.default_catalog, &options.default_schema);
    let schema = ctx
        .catalog(&resolved.catalog)
        .and_then(|catalog| catalog.schema(&resolved.schema));
    refused_schema(catalogs, &resolved.catalog, schema.as_deref()).then(|| resolved.to_string())
}

fn refused_schema(
    catalogs: &CatalogRegistry,
    catalog: &str,
    schema: Option<&dyn SchemaProvider>,
) -> bool {
    catalogs.get(catalog).is_some()
        && schema.is_some_and(|schema| schema.downcast_ref::<MemorySchemaProvider>().is_none())
}

#[cfg(test)]
mod tests;
