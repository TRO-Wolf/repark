use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::Schema;
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::timestamp_ns_cast::asked_nanosecond_columns;
use repark_iceberg::write::{NanosecondAsk, NewColumns, Remedies};

const REMEDIES: Remedies = Remedies {
    nanosecond_cast: false,
    naive_microseconds: "TIMESTAMP(6)",
    zoned_microseconds: "TIMESTAMP(6)",
};

pub(crate) fn nanosecond_ask(ctx: &SessionContext, plan: &LogicalPlan) -> Result<NanosecondAsk> {
    let state = ctx.state();
    if repark_sql_settings_from_options(state.config_options()).allow_implicit_timestamp_nanos {
        return Ok(NanosecondAsk::every(REMEDIES));
    }
    let analyzed = repark_functions::analyze_eagerly(&state, plan.clone())?;
    Ok(NanosecondAsk::columns(
        asked_nanosecond_columns(&analyzed)?,
        REMEDIES,
    ))
}

pub(super) struct NativeAsk(NanosecondAsk);

impl NativeAsk {
    pub(super) fn table_schema(
        &self,
        arrow: &ArrowSchema,
        existing: Option<&Schema>,
        or_replace: bool,
    ) -> Result<Schema> {
        repark_iceberg::write::new_table_schema(&NewColumns {
            arrow,
            ask: &self.0,
            existing,
            form: if or_replace { "RTAS" } else { "CTAS" },
            case_sensitive: true,
        })
    }
}

pub(super) fn ask_of(ctx: &SessionContext, query: Option<&DataFrame>) -> Result<NativeAsk> {
    match query {
        Some(frame) => nanosecond_ask(ctx, frame.logical_plan()).map(NativeAsk),
        None => Ok(NativeAsk(NanosecondAsk::every(REMEDIES))),
    }
}
