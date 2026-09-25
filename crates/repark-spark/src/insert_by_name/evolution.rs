use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, ObjectName, TableObject};
use iceberg::spec::NestedFieldRef;
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{namespace_schema_name, reregister};
use crate::write_options::StatementWriteOptions;

pub(super) struct SourceUnion {
    names: Vec<String>,
    added: Vec<String>,
}

pub(super) fn columns_to_add(
    ctx: &SessionContext,
    table: &Table,
    sources: &[super::SourceName],
    case_sensitive: bool,
    write_options: &StatementWriteOptions,
) -> Result<Option<SourceUnion>> {
    if !repark_iceberg::write::accepts_any_schema(table) {
        return Ok(None);
    }
    let schema = table.metadata().current_schema();
    let fields = schema.as_struct().fields();
    let matched: Vec<Option<&NestedFieldRef>> = sources
        .iter()
        .map(|name| {
            fields
                .iter()
                .find(|field| super::same_name(&field.name, &name.resolved, case_sensitive))
        })
        .collect();
    refuse_duplicate_sources(sources, &matched, case_sensitive)?;
    let unknown = || {
        sources
            .iter()
            .zip(&matched)
            .filter(|(_, field)| field.is_none())
    };
    if !write_options.merge_schema(ctx) {
        return match unknown().next() {
            Some((name, _)) => Err(repark_core::illegal_argument_error(format!(
                "Field {} not found in source schema",
                name.display
            ))),
            None => Ok(None),
        };
    }
    let added: Vec<&super::SourceName> = unknown().map(|(name, _)| name).collect();
    refuse_lower_case_collision(&added, case_sensitive)?;
    Ok(Some(SourceUnion {
        names: sources
            .iter()
            .zip(&matched)
            .map(|(name, field)| field.map_or_else(|| name.display.clone(), |f| f.name.clone()))
            .collect(),
        added: added.iter().map(|name| name.display.clone()).collect(),
    }))
}

fn refuse_duplicate_sources(
    sources: &[super::SourceName],
    matched: &[Option<&NestedFieldRef>],
    case_sensitive: bool,
) -> Result<()> {
    for (later, name) in sources.iter().enumerate() {
        if let Some(earlier) = sources[..later]
            .iter()
            .position(|other| other.display == name.display)
        {
            return Err(crate::catalog_ops::iceberg_err(iceberg::Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid schema: multiple fields for name {}: {earlier} and {later}",
                    name.display
                ),
            )));
        }
    }
    if case_sensitive {
        return Ok(());
    }
    for (later, (name, field)) in sources.iter().zip(matched).enumerate() {
        let Some(field) = field else {
            continue;
        };
        if let Some(earlier) = sources[..later]
            .iter()
            .find(|other| super::same_name(&other.resolved, &name.resolved, false))
        {
            return Err(repark_core::illegal_argument_error(format!(
                "Multiple entries with same key: {id}={} and {id}={}",
                name.display,
                earlier.display,
                id = field.id
            )));
        }
    }
    Ok(())
}

fn refuse_lower_case_collision(added: &[&super::SourceName], case_sensitive: bool) -> Result<()> {
    if case_sensitive {
        return Ok(());
    }
    for (later, name) in added.iter().enumerate() {
        if let Some(earlier) = added[..later]
            .iter()
            .find(|other| super::same_name(&other.resolved, &name.resolved, false))
        {
            return Err(repark_core::illegal_argument_error(format!(
                "Cannot build lower case index: {} and {} collide",
                earlier.display, name.display
            )));
        }
    }
    Ok(())
}

pub(crate) async fn routes_positional_by_name(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    write_options: &StatementWriteOptions,
) -> Result<bool> {
    if insert.replace_into
        || (insert.partitioned.is_some() && insert.overwrite)
        || !insert.columns.is_empty()
        || insert.source.is_none()
        || !write_options.carries_only_merge_schema()
    {
        return Ok(false);
    }
    let TableObject::TableName(name) = &insert.table else {
        return Ok(false);
    };
    let Some((catalog, ident)) = routing_target(ctx, catalogs, name) else {
        return Ok(false);
    };
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(repark_iceberg::write::accepts_any_schema(&table)),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(crate::catalog_ops::iceberg_err(error)),
    }
}

fn routing_target<'a>(
    ctx: &SessionContext,
    catalogs: &'a CatalogRegistry,
    name: &ObjectName,
) -> Option<(&'a Arc<dyn Catalog>, TableIdent)> {
    let mut parts = crate::catalog_ops::name_parts(name);
    match crate::write_to_branch::split_write_ref_parts(&parts) {
        Some((table_parts, crate::write_to_branch::RefSelectorKind::Branch(_))) => {
            parts = table_parts;
        }
        Some((_, crate::write_to_branch::RefSelectorKind::Tag)) => return None,
        None => {}
    }
    let parts = crate::write_to_branch::qualify_table_parts(ctx, parts);
    let (leaf, rest) = parts.split_last()?;
    let (catalog_name, namespace) = rest.split_first()?;
    if namespace.is_empty() {
        return None;
    }
    let namespace = NamespaceIdent::from_vec(namespace.to_vec()).ok()?;
    let catalog = catalogs.get(catalog_name)?;
    Some((catalog, TableIdent::new(namespace, leaf.clone())))
}

pub(super) async fn evolve_before_write(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    source_sql: &str,
    union: &SourceUnion,
) -> Result<Table> {
    let probe = format!("{source_sql} LIMIT 0");
    let frame = crate::spark_ast::execute_passthrough(ctx, catalogs, &probe).await?;
    let arrow = union_input_schema(frame.schema().as_arrow(), union)?;
    let incoming = repark_iceberg::write::incoming_schema(&arrow)?;
    let evolved = repark_iceberg::write::evolve_schema(catalog, table, incoming).await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, Arc::clone(catalog), catalog_name, &namespace).await?;
    Ok(evolved)
}

fn union_input_schema(arrow: &ArrowSchema, union: &SourceUnion) -> Result<ArrowSchema> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(arrow.fields().len());
    for (field, name) in arrow.fields().iter().zip(&union.names) {
        if field.data_type() == &DataType::Null {
            if union.added.contains(name) {
                return Err(DataFusionError::Plan(format!(
                    "cannot add column `{name}` by schema merging: the source gives it no type \
                     (every value is NULL)"
                )));
            }
            continue;
        }
        fields.push(Arc::new(field.as_ref().clone().with_name(name.clone())));
    }
    Ok(ArrowSchema::new(fields))
}
