use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::Column;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Cast, Expr, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{PrimitiveType, Type};
use iceberg::table::Table;

use super::update_cast::incompatible_update_message;

pub const NTZ_WALL_CAST_UDF_NAME: &str = "__repark_cast_timestamp_ntz__";

#[must_use]
pub fn is_ntz_wall_target(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

#[must_use]
pub fn is_ltz_instant_target(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

#[must_use]
pub fn ntz_wall_cast_sql(expr_sql: &str) -> String {
    format!("{NTZ_WALL_CAST_UDF_NAME}(({expr_sql}))")
}

#[must_use]
pub fn ltz_instant_cast_sql(expr_sql: &str) -> String {
    format!("CAST(({expr_sql}) AS TIMESTAMP)")
}

#[must_use]
pub fn needs_ltz_instant_cast(source: &DataType) -> bool {
    matches!(
        source,
        DataType::Timestamp(TimeUnit::Microsecond, None) | DataType::Date32 | DataType::Date64
    )
}

#[must_use]
pub fn analyzed_types(ctx: &SessionContext, plan: &LogicalPlan) -> Option<Vec<DataType>> {
    let state = ctx.state();
    let analyzed = state
        .analyzer()
        .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
        .ok()?;
    Some(
        analyzed
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect(),
    )
}

#[allow(clippy::missing_errors_doc)]
pub fn zone_stores(
    ctx: &SessionContext,
    frame: DataFrame,
    table: &Table,
    listed: &[String],
    reserved: &[String],
) -> Result<DataFrame> {
    let schema = table.metadata().current_schema();
    let fields = schema.as_struct().fields();
    let names: Vec<String> = if listed.is_empty() {
        fields
            .iter()
            .filter(|field| {
                !reserved
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&field.name))
            })
            .map(|field| field.name.clone())
            .collect()
    } else {
        listed.to_vec()
    };
    let targets: Vec<Option<DataType>> = names
        .iter()
        .map(|name| {
            let field = fields
                .iter()
                .find(|field| field.name == *name)
                .or_else(|| {
                    fields
                        .iter()
                        .find(|field| field.name.eq_ignore_ascii_case(name))
                })?;
            match field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Timestamp) => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                Type::Primitive(PrimitiveType::Timestamptz) => Some(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::from("+00:00")),
                )),
                _ => None,
            }
        })
        .collect();
    zone_store_frame(ctx, frame, &targets)
}

#[allow(clippy::missing_errors_doc)]
pub fn zone_stores_by_name(
    ctx: &SessionContext,
    frame: DataFrame,
    table: &Table,
) -> Result<DataFrame> {
    let names: Vec<String> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    zone_stores(ctx, frame, table, &names, &[])
}

fn zone_store_frame(
    ctx: &SessionContext,
    frame: DataFrame,
    targets: &[Option<DataType>],
) -> Result<DataFrame> {
    if targets.len() != frame.schema().fields().len() || targets.iter().all(Option::is_none) {
        return Ok(frame);
    }
    let Some(planned) = analyzed_types(ctx, frame.logical_plan()) else {
        return Ok(frame);
    };
    if planned.len() != targets.len() {
        return Ok(frame);
    }
    let wall = ctx.udf(NTZ_WALL_CAST_UDF_NAME).ok();
    let mut converted = false;
    let exprs: Vec<Expr> = frame
        .schema()
        .iter()
        .zip(planned.iter().zip(targets))
        .map(|((qualifier, field), (source, target))| {
            let column = Expr::Column(Column::from((qualifier, field.as_ref())));
            let store = match (target, &wall) {
                (Some(target), Some(udf))
                    if is_ntz_wall_target(target)
                        && matches!(source, DataType::Timestamp(_, Some(_))) =>
                {
                    Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(udf), vec![column]))
                }
                (Some(target), _)
                    if is_ltz_instant_target(target) && needs_ltz_instant_cast(source) =>
                {
                    let instant = Field::new(field.name(), target.clone(), field.is_nullable());
                    Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(instant)))
                }
                _ => return column,
            };
            converted = true;
            store.alias(field.name())
        })
        .collect();
    if !converted {
        return Ok(frame);
    }
    frame.select(exprs)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_ntz_writes<'a>(
    ctx: &SessionContext,
    table: &str,
    plan: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if !targets
        .iter()
        .any(|(_, data_type)| is_ntz_wall_target(data_type))
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
        if !is_ntz_wall_target(target) {
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
