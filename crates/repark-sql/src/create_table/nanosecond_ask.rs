use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{FormatVersion, Schema};
use iceberg::table::Table;
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::timestamp_ns_cast::asked_nanosecond_columns_in;
use repark_iceberg::write::{NanosecondAsk, NewColumns, Remedies, Target};

const REMEDIES: Remedies = Remedies {
    nanosecond_cast: false,
    setting: "build the session with `repark.sql.allowImplicitTimestampNanos` = true in its \
              configuration (the key is read once, when the session is built)",
    naive_microseconds: Some("CAST(<expression> AS TIMESTAMP(6))"),
    zoned_microseconds: Some("CAST(<expression> AS TIMESTAMP(6))"),
    create_v3: "Create the table WITH (format_version = 3).",
    replace_v3: "Replace the table WITH (format_version = 3), or upgrade it first with \
                 ALTER TABLE … SET PROPERTIES (format_version = 3).",
    upgrade_v3: "Upgrade the table first with ALTER TABLE … SET PROPERTIES (format_version = 3).",
};

pub(crate) fn nanosecond_ask(ctx: &SessionContext, plan: &LogicalPlan) -> Result<NanosecondAsk> {
    let state = ctx.state();
    if repark_sql_settings_from_options(state.config_options()).allow_implicit_timestamp_nanos {
        return Ok(NanosecondAsk::every(REMEDIES));
    }
    Ok(NanosecondAsk::columns(
        asked_nanosecond_columns_in(&state, plan, &|_| None)?,
        REMEDIES,
    ))
}

pub(super) struct NativeAsk(NanosecondAsk);

impl NativeAsk {
    pub(super) fn table_schema(
        &self,
        arrow: &ArrowSchema,
        existing: Option<&Table>,
        requested_version: Option<&str>,
        or_replace: bool,
    ) -> Result<Schema> {
        repark_iceberg::write::new_table_schema(&NewColumns {
            arrow,
            ask: &self.0,
            existing: existing.map(|table| table.metadata().current_schema().as_ref()),
            form: if or_replace { "RTAS" } else { "CTAS" },
            case_sensitive: true,
            target: target(requested_version, existing),
        })
    }
}

pub(super) fn ask_of(ctx: &SessionContext, query: Option<&DataFrame>) -> Result<NativeAsk> {
    match query {
        Some(frame) => nanosecond_ask(ctx, frame.logical_plan()).map(NativeAsk),
        None => Ok(NativeAsk(NanosecondAsk::every(REMEDIES))),
    }
}

fn target(requested: Option<&str>, existing: Option<&Table>) -> Target {
    let holds_v3 = match requested.map(str::trim).filter(|value| !value.is_empty()) {
        Some(version) => version == "3",
        None => {
            existing.is_some_and(|table| table.metadata().format_version() >= FormatVersion::V3)
        }
    };
    let below = if existing.is_some() {
        Target::ReplacedBelowV3
    } else {
        Target::CreatedBelowV3
    };
    Target::of(holds_v3, below)
}
