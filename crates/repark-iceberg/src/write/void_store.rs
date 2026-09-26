use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Fields, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{PrimitiveType, Schema, Type};

use super::update_cast::incompatible_update_message;

#[allow(clippy::missing_errors_doc)]
pub fn refuse_void_writes<'a>(
    ctx: &SessionContext,
    table: &str,
    plan: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if !targets
        .iter()
        .any(|(_, data_type)| matches!(data_type, DataType::Null))
    {
        return Ok(());
    }
    let state = ctx.state();
    let analyzed =
        state
            .analyzer()
            .execute_and_check(plan.clone(), state.config_options(), |_, _| {})?;
    let planned = analyzed.schema().fields();
    if planned.len() != targets.len() {
        return Ok(());
    }
    for (field, (column, target)) in planned.iter().zip(targets) {
        if !matches!(target, DataType::Null) {
            continue;
        }
        if let Some(text) =
            incompatible_update_message(table, &format!("`{column}`"), field.data_type(), target)
        {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn arrow_schema_to_iceberg_with_unknown(arrow: &ArrowSchema) -> iceberg::Result<Schema> {
    let is_void = |field: &Arc<datafusion::arrow::datatypes::Field>| {
        matches!(field.data_type(), DataType::Null)
    };
    if !arrow.fields().iter().any(is_void) {
        return arrow_schema_to_schema_auto_assign_ids(arrow);
    }
    let stand_in = ArrowSchema::new_with_metadata(
        arrow
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Null => {
                    Arc::new(field.as_ref().clone().with_data_type(DataType::Boolean))
                }
                _ => Arc::clone(field),
            })
            .collect::<Fields>(),
        arrow.metadata().clone(),
    );
    let converted = arrow_schema_to_schema_auto_assign_ids(&stand_in)?;
    let fields = converted
        .as_struct()
        .fields()
        .iter()
        .zip(arrow.fields())
        .map(|(field, source)| {
            if !is_void(source) {
                return Arc::clone(field);
            }
            let mut unknown = field.as_ref().clone();
            unknown.field_type = Box::new(Type::Primitive(PrimitiveType::Unknown));
            unknown.required = false;
            Arc::new(unknown)
        });
    Schema::builder()
        .with_schema_id(converted.schema_id())
        .with_identifier_field_ids(converted.identifier_field_ids())
        .with_fields(fields)
        .build()
}
