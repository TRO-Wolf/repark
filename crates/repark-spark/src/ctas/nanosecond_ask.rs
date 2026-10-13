use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::prelude::SessionContext;
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::timestamp_ns_cast::asked_nanosecond_columns_in;
use repark_iceberg::write::{NanosecondAsk, Remedies, Target};

const REMEDIES: Remedies = Remedies {
    nanosecond_cast: true,
    setting: "build the session with `repark.sql.allowImplicitTimestampNanos` = true \
              (`ReparkSession.builder.config`; the key is read once, when the session is built)",
    naive_microseconds: Some("CAST(<expression> AS TIMESTAMP_NTZ)"),
    zoned_microseconds: Some("CAST(<expression> AS TIMESTAMP)"),
    create_v3: "Create the table with TBLPROPERTIES ('format-version'='3') in SQL or with \
                `writeTo(...).tableProperty('format-version', '3')`; `saveAsTable` cannot set \
                the format version.",
    replace_v3: "Replace the table with TBLPROPERTIES ('format-version'='3') in SQL or with \
                 `writeTo(...).tableProperty('format-version', '3')`, or upgrade it first with \
                 ALTER TABLE … SET TBLPROPERTIES ('format-version'='3').",
    upgrade_v3: "Upgrade the table first with ALTER TABLE … SET TBLPROPERTIES \
                 ('format-version'='3').",
};

const MERGE_REMEDIES: Remedies = Remedies {
    zoned_microseconds: Some("to_timestamp(<expression>)"),
    ..REMEDIES
};

pub(crate) fn asked_columns(ctx: &SessionContext, plan: &LogicalPlan) -> Result<Option<Vec<bool>>> {
    let state = ctx.state();
    if repark_sql_settings_from_options(state.config_options()).allow_implicit_timestamp_nanos {
        return Ok(None);
    }
    asked_nanosecond_columns_in(&state, plan, &temp_view_plan).map(Some)
}

fn temp_view_plan(scan: &TableScan) -> Option<LogicalPlan> {
    let provider = source_as_provider(&scan.source).ok()?;
    crate::view_ddl::temp_view::definition_plan(provider.as_ref())
}

fn ask_with(asked: Option<Vec<bool>>, remedies: Remedies) -> NanosecondAsk {
    match asked {
        Some(flags) => NanosecondAsk::columns(flags, remedies),
        None => NanosecondAsk::every(remedies),
    }
}

pub(crate) fn ask_of(asked: Option<Vec<bool>>) -> NanosecondAsk {
    ask_with(asked, REMEDIES)
}

pub(crate) fn nanosecond_ask(ctx: &SessionContext, plan: &LogicalPlan) -> Result<NanosecondAsk> {
    asked_columns(ctx, plan).map(ask_of)
}

pub(crate) fn merge_nanosecond_ask(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<NanosecondAsk> {
    Ok(ask_with(asked_columns(ctx, plan)?, MERGE_REMEDIES))
}

pub(crate) fn target(requested: Option<&str>, existing: Option<&Table>) -> Target {
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
