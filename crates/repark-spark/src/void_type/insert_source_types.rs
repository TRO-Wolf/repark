use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, ObjectName, TableObject};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::insert_defaults::OverwriteSource;
use repark_iceberg::write::negated_null_store::{refuse_negated_null_writes, refuses_double};

use super::column_name;
use crate::catalog_ops::{name_parts, quoted_table_display};
use crate::write_to_branch::qualify_table_parts;

pub(crate) async fn refuse_insert_source_types(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    by_name: bool,
) -> Result<()> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let Some(source) = insert.source.as_ref() else {
        return Ok(());
    };
    let parts = qualify_table_parts(ctx, name_parts(name));
    if parts.len() < 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let Ok(presented) = repark_iceberg::catalog::uuid_presentation::presented_arrow_schema(
        table.metadata().current_schema(),
    ) else {
        return Ok(());
    };
    if !presented
        .fields()
        .iter()
        .any(|field| refuses_double(field.data_type()))
    {
        return Ok(());
    }
    let Ok(frame) = ctx.sql(&source.to_string()).await else {
        return Ok(());
    };
    let plan = frame.logical_plan();
    let planned = plan.schema().fields();
    if !planned
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Null))
    {
        return Ok(());
    }
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let targets: Option<Vec<&Field>> = if by_name {
        planned
            .iter()
            .map(|field| find_target(&presented, field.name(), case_insensitive))
            .collect()
    } else if insert.columns.is_empty() {
        (planned.len() == presented.fields().len())
            .then(|| presented.fields().iter().map(AsRef::as_ref).collect())
    } else {
        insert
            .columns
            .iter()
            .map(|column| find_target(&presented, &column_name(column), case_insensitive))
            .collect()
    };
    let Some(targets) = targets else {
        return Ok(());
    };
    refuse_negated_null_writes(
        ctx,
        &quoted_table_display(&parts),
        plan,
        targets
            .iter()
            .map(|field| (field.name().as_str(), field.data_type())),
    )
}

pub(crate) async fn refuse_partition_overwrite_sources(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
    table_name: &ObjectName,
    filled: &OverwriteSource,
    reserved: &[String],
) -> Result<()> {
    let Ok(presented) = repark_iceberg::catalog::uuid_presentation::presented_arrow_schema(
        table.metadata().current_schema(),
    ) else {
        return Ok(());
    };
    let targets: Option<Vec<(&str, &DataType)>> = if filled.columns.is_empty() {
        Some(
            presented
                .fields()
                .iter()
                .filter(|field| !is_reserved(field.name(), reserved))
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect(),
        )
    } else {
        filled
            .columns
            .iter()
            .map(|name| {
                if is_reserved(name, reserved) {
                    return None;
                }
                presented
                    .fields()
                    .iter()
                    .find(|field| field.name().eq_ignore_ascii_case(name))
                    .map(|field| (field.name().as_str(), field.data_type()))
            })
            .collect()
    };
    let Some(targets) = targets else {
        return Ok(());
    };
    if !targets
        .iter()
        .any(|(_, data_type)| refuses_double(data_type))
    {
        return Ok(());
    }
    let Ok(frame) = ctx.sql(&filled.sql).await else {
        return Ok(());
    };
    let plan = frame.logical_plan();
    if !plan
        .schema()
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Null))
    {
        return Ok(());
    }
    let parts = qualify_table_parts(ctx, name_parts(table_name));
    refuse_negated_null_writes(ctx, &quoted_table_display(&parts), plan, targets)
}

fn is_reserved(name: &str, reserved: &[String]) -> bool {
    reserved.iter().any(|item| item.eq_ignore_ascii_case(name))
}

fn find_target<'a>(
    presented: &'a ArrowSchema,
    wanted: &str,
    case_insensitive: bool,
) -> Option<&'a Field> {
    presented
        .fields()
        .iter()
        .find(|field| field.name() == wanted)
        .or_else(|| {
            presented
                .fields()
                .iter()
                .find(|field| case_insensitive && field.name().eq_ignore_ascii_case(wanted))
        })
        .map(AsRef::as_ref)
}
