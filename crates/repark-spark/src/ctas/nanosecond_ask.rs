use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::timestamp_ns_cast::asked_nanosecond_columns;
use repark_iceberg::write::{NanosecondAsk, Remedies};

const REMEDIES: Remedies = Remedies {
    nanosecond_cast: true,
    naive_microseconds: "TIMESTAMP_NTZ",
    zoned_microseconds: "TIMESTAMP",
};

pub(crate) fn asked_columns(ctx: &SessionContext, plan: &LogicalPlan) -> Result<Option<Vec<bool>>> {
    let state = ctx.state();
    if repark_sql_settings_from_options(state.config_options()).allow_implicit_timestamp_nanos {
        return Ok(None);
    }
    let analyzed = repark_functions::analyze_eagerly(&state, plan.clone())?;
    asked_nanosecond_columns(&analyzed).map(Some)
}

pub(crate) fn ask_of(asked: Option<Vec<bool>>) -> NanosecondAsk {
    match asked {
        Some(flags) => NanosecondAsk::columns(flags, REMEDIES),
        None => NanosecondAsk::every(REMEDIES),
    }
}

pub(crate) fn nanosecond_ask(ctx: &SessionContext, plan: &LogicalPlan) -> Result<NanosecondAsk> {
    asked_columns(ctx, plan).map(ask_of)
}
