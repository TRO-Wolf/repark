use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{AssignmentTarget, Statement};
use repark_core::EngineContext;

pub(crate) async fn refuse_incompatible_update_cast(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Result<()> {
    let Statement::Update(update) = statement else {
        return Ok(());
    };
    let Some((_kind, table_sql, catalog_name, ident)) =
        crate::guards::dml_target_ident(cx, statement)
    else {
        return Ok(());
    };
    let Some(catalog) = cx.catalogs.get(&catalog_name) else {
        return Ok(());
    };
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let Ok(arrow_schema) =
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
    else {
        return Ok(());
    };
    let mut parts = vec![catalog_name.clone()];
    parts.extend(ident.namespace().iter().cloned());
    parts.push(ident.name().to_string());
    let table_name = parts
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".");
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
        let Ok(frame) = cx.ctx.sql(&probe_sql).await else {
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
