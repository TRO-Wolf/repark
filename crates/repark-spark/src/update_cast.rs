use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{AssignmentTarget, Update};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

pub(crate) async fn refuse_incompatible_update_cast(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    update: &Update,
) -> Result<()> {
    let Some(object_name) = crate::object_name_from_table_with_joins(&update.table) else {
        return Ok(());
    };
    let parts = crate::name_parts(object_name);
    if parts.is_empty() {
        return Ok(());
    }
    let qualified = crate::write_to_branch::qualify_table_parts(ctx, parts);
    if qualified.len() < 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&qualified[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(qualified[1..qualified.len() - 1].to_vec()) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, qualified[qualified.len() - 1].clone());
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let Ok(arrow_schema) =
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
    else {
        return Ok(());
    };
    let table_name = qualified
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".");
    let table_sql = object_name.to_string();
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            continue;
        };
        let target_parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
            .collect();
        if target_parts.len() != 1 {
            continue;
        }
        let target = &target_parts[0];
        let mut resolved = None;
        for field in arrow_schema.fields() {
            if field.name() == target {
                resolved = Some(field);
                break;
            }
            if resolved.is_none() && field.name().eq_ignore_ascii_case(target) {
                resolved = Some(field);
            }
        }
        let Some(field) = resolved else {
            continue;
        };
        let probe_sql = format!("SELECT ({}) FROM {table_sql}", assignment.value);
        let Ok(frame) = ctx.sql(&probe_sql).await else {
            return Ok(());
        };
        let Some(source_field) = frame.schema().fields().first() else {
            return Ok(());
        };
        if let Some(text) = repark_iceberg::write::update_cast::incompatible_update_message(
            &table_name,
            &format!("`{target}`"),
            source_field.data_type(),
            field.data_type(),
        ) {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}
