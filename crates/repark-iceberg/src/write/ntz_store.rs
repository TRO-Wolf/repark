use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use super::update_cast::incompatible_update_message;

pub const NTZ_WALL_CAST_UDF_NAME: &str = "__repark_cast_timestamp_ntz__";

#[must_use]
pub fn is_ntz_wall_target(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

#[must_use]
pub fn ntz_wall_cast_sql(expr_sql: &str) -> String {
    format!("{NTZ_WALL_CAST_UDF_NAME}(({expr_sql}))")
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
