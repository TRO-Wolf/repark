use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, TableObject};
use iceberg::Catalog;
use iceberg::table::Table;
use repark_core::CatalogRegistry;

use crate::catalog_ops::{namespace_schema_name, reregister};
use crate::write_options::StatementWriteOptions;

pub(super) fn extra_source_columns<'a>(
    targets: &[String],
    sources: &'a [super::SourceName],
    case_sensitive: bool,
) -> Vec<&'a super::SourceName> {
    sources
        .iter()
        .filter(|name| {
            !targets
                .iter()
                .any(|target| super::same_name(target, &name.resolved, case_sensitive))
        })
        .collect()
}

pub(super) fn columns_to_add(
    ctx: &SessionContext,
    table: &Table,
    targets: &[String],
    sources: &[super::SourceName],
    case_sensitive: bool,
    write_options: &StatementWriteOptions,
) -> Result<Option<Vec<String>>> {
    if !repark_iceberg::write::accepts_any_schema(table) {
        return Ok(None);
    }
    let extras = extra_source_columns(targets, sources, case_sensitive);
    if write_options.merge_schema(ctx) {
        return Ok(Some(
            extras.iter().map(|name| name.resolved.clone()).collect(),
        ));
    }
    match extras.first() {
        Some(name) => Err(repark_core::illegal_argument_error(format!(
            "Field {} not found in source schema",
            name.display
        ))),
        None => Ok(None),
    }
}

pub(crate) async fn routes_positional_by_name(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    write_options: &StatementWriteOptions,
) -> bool {
    if insert.replace_into
        || insert.partitioned.is_some()
        || !insert.columns.is_empty()
        || insert.source.is_none()
        || !write_options.carries_only_merge_schema()
    {
        return false;
    }
    let TableObject::TableName(name) = &insert.table else {
        return false;
    };
    crate::insert_overwrite::try_resolve_iceberg_overwrite_target(ctx, catalogs, name)
        .await
        .ok()
        .flatten()
        .is_some_and(|(_, _, table, _)| repark_iceberg::write::accepts_any_schema(&table))
}

pub(super) async fn evolve_before_write(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    source_sql: &str,
    added: &[String],
) -> Result<Table> {
    let probe = format!("{source_sql} LIMIT 0");
    let frame = crate::spark_ast::execute_passthrough(ctx, catalogs, &probe).await?;
    let arrow = union_input_schema(frame.schema().as_arrow(), added)?;
    let incoming = repark_iceberg::write::incoming_schema(&arrow)?;
    let evolved = repark_iceberg::write::evolve_schema(catalog, table, incoming).await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, Arc::clone(catalog), catalog_name, &namespace).await?;
    Ok(evolved)
}

fn union_input_schema(arrow: &ArrowSchema, added: &[String]) -> Result<ArrowSchema> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(arrow.fields().len());
    for field in arrow.fields() {
        if field.data_type() == &DataType::Null {
            if added.iter().any(|name| name == field.name()) {
                return Err(DataFusionError::Plan(format!(
                    "cannot add column `{}` by schema merging: the source gives it no type \
                     (every value is NULL)",
                    field.name()
                )));
            }
            continue;
        }
        fields.push(Arc::clone(field));
    }
    Ok(ArrowSchema::new(fields))
}
