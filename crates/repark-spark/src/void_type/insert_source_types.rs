use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, TableObject};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::negated_null_store::{refuse_negated_null_writes, refuses_double};
use repark_iceberg::write::update_cast::incompatible_store_message;

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
        .any(|field| refuses_double(field.data_type()) || is_float(field.data_type()))
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
        .any(|field| matches!(field.data_type(), DataType::Null) || is_string(field.data_type()))
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
    let display = quoted_table_display(&parts);
    refuse_negated_null_writes(
        ctx,
        &display,
        plan,
        targets
            .iter()
            .map(|field| (field.name().as_str(), field.data_type())),
    )?;
    for (source_field, target) in planned.iter().zip(&targets) {
        if !is_string(source_field.data_type()) || !is_float(target.data_type()) {
            continue;
        }
        if let Some(text) = incompatible_store_message(
            &display,
            &format!("`{}`", target.name()),
            source_field.data_type(),
            target.data_type(),
        ) {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}

fn is_float(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
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
