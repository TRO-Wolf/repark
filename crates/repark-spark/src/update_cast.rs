use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, ObjectName, Statement, TableFactor, Update,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::void_store::refuse_void_writes;

use crate::merge::nested_assign::{self, AssignmentScope};

struct UpdateTarget {
    arrow_schema: ArrowSchema,
}

async fn load_update_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    update: &Update,
) -> Option<UpdateTarget> {
    let object_name = crate::object_name_from_table_with_joins(&update.table)?;
    let parts = crate::name_parts(object_name);
    if parts.is_empty() {
        return None;
    }
    let qualified = crate::write_to_branch::qualify_table_parts(ctx, parts);
    if qualified.len() < 3 {
        return None;
    }
    let catalog = catalogs.get(&qualified[0])?;
    let namespace = NamespaceIdent::from_vec(qualified[1..qualified.len() - 1].to_vec()).ok()?;
    let ident = TableIdent::new(namespace, qualified[qualified.len() - 1].clone());
    let table = catalog.load_table(&ident).await.ok()?;
    let arrow_schema =
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).ok()?;
    Some(UpdateTarget { arrow_schema })
}

pub(crate) async fn refuse_cast_then_fold_nested(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    update: &Update,
) -> Result<Option<String>> {
    let Some(object_name) = crate::object_name_from_table_with_joins(&update.table) else {
        return Ok(None);
    };
    let Some(target) = load_update_target(ctx, catalogs, update).await else {
        return Ok(None);
    };
    let alias = match &update.table.relation {
        TableFactor::Table {
            alias: Some(alias), ..
        } => Some(alias.name.value.clone()),
        _ => None,
    };
    let written = crate::name_parts(object_name);
    let qualifiers = alias.clone().map_or_else(
        || nested_assign::name_suffixes(&written),
        |alias| vec![vec![alias]],
    );
    let scope = AssignmentScope {
        value_qualifiers: qualifiers.clone(),
        qualifiers,
        sql_qualifier: alias.unwrap_or_else(|| written.join(".")),
        column_prefix: None,
        probe_from: update.table.to_string(),
        case_sensitive: !crate::spark_door_case_insensitive(ctx.state().config().options()),
    };
    if !nested_assign::repeats_a_column(&target.arrow_schema, &scope, &update.assignments) {
        refuse_incompatible_update_cast(ctx, &target, object_name, update).await?;
    }
    let Some(assignments) = nested_assign::fold_nested_assignments(
        ctx,
        &target.arrow_schema,
        &scope,
        &update.assignments,
    )
    .await?
    else {
        return Ok(None);
    };
    let mut folded = update.clone();
    folded.assignments = assignments;
    Ok(Some(Statement::Update(folded).to_string()))
}

async fn refuse_incompatible_update_cast(
    ctx: &SessionContext,
    target: &UpdateTarget,
    object_name: &ObjectName,
    update: &Update,
) -> Result<()> {
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
        let column = &target_parts[0];
        let mut resolved = None;
        for field in target.arrow_schema.fields() {
            if field.name() == column {
                resolved = Some(field);
                break;
            }
            if resolved.is_none() && field.name().eq_ignore_ascii_case(column) {
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
        let pair = [(column.as_str(), field.data_type())];
        refuse_void_writes(ctx, "``", frame.logical_plan(), pair)?;
        let Some(source_field) = frame.schema().fields().first() else {
            return Ok(());
        };
        if let Some(text) = repark_iceberg::write::update_cast::incompatible_update_message(
            "``",
            &format!("`{column}`"),
            source_field.data_type(),
            field.data_type(),
        ) {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}
