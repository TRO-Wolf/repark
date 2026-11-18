use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::TableReference;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlanBuilder;
use repark_core::ReparkSession;

use super::*;
use crate::view_ddl::temp_view::{definition_plan, renamed_temp_view, temp_view_column_comments};
use crate::{SparkDialect, SparkExtension};

const OVERFLOW: &str = "SELECT a + 1 AS m FROM (VALUES (CAST(2147483647 AS INT))) t(a)";

fn session(ansi: bool) -> ReparkSession {
    zoned(ansi, "UTC")
}

fn zoned(ansi: bool, zone: &str) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("spark.sql.ansi.enabled", ansi.to_string())
        .config("spark.sql.session.timeZone", zone)
        .build()
        .unwrap_or_else(|error| panic!("session: {error}"))
}

fn wrapped(frame: &DataFrame) -> Arc<dyn TableProvider> {
    frame_view("v", frame, frame.task_ctx().session_config().options())
}

fn held_on(reader: &ReparkSession) -> SessionState {
    let creation = session(true).context().state();
    let live = reader.context().state();
    TempViewSettings::capture(creation.config_options())
        .overlay(&live, live.config_options())
        .unwrap_or_else(|| panic!("an ANSI-on state over the reader"))
}

async fn unanalysed(reader: &ReparkSession, sql: &str) -> LogicalPlan {
    reader
        .context()
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .into_unoptimized_plan()
}

fn analysed(state: &SessionState, plan: LogicalPlan) -> LogicalPlan {
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
        .unwrap_or_else(|error| panic!("analyse: {error}"))
}

fn scanned(provider: Arc<dyn TableProvider>) -> LogicalPlan {
    LogicalPlanBuilder::scan("v", provider_as_source(provider), None)
        .and_then(LogicalPlanBuilder::build)
        .unwrap_or_else(|error| panic!("scan: {error}"))
}

fn closed_views(plan: &LogicalPlan) -> usize {
    let mut found = 0;
    plan.apply_with_subqueries(|node| {
        if closed_plan(node).is_some() {
            found += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap_or_else(|error| panic!("walk: {error}"));
    found
}

async fn rows(frame: DataFrame) -> std::result::Result<String, String> {
    match frame.collect().await {
        Ok(batches) => pretty_format_batches(&batches)
            .map(|table| table.to_string())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

#[test]
fn the_marker_rule_runs_first_and_no_function_rewrite_precedes_it() {
    let state = session(true).context().state();
    let analyzer = state.analyzer();
    assert!(analyzer.function_rewrites().is_empty());
    let names = analyzer
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names.first().map(String::as_str), Some(FRAME_SETTINGS_RULE));
    assert_eq!(
        names
            .iter()
            .filter(|name| *name == FRAME_SETTINGS_RULE)
            .count(),
        1
    );
    assert!(names.len() > 1, "{names:?}");
}

#[tokio::test]
async fn matching_settings_unwrap_the_marker_into_todays_plan() {
    let reader = session(true);
    let state = reader.context().state();
    let frame = DataFrame::new(state.clone(), unanalysed(&reader, OVERFLOW).await);
    let marked = scanned(wrapped(&frame));
    assert!(holds_marker(&marked).unwrap_or(false));
    let today = scanned(frame.clone().into_view());
    assert!(!holds_marker(&today).unwrap_or(true));
    let marked = analysed(&state, marked);
    assert!(!holds_marker(&marked).unwrap_or(true));
    assert_eq!(closed_views(&marked), 0);
    assert_eq!(
        marked.display_indent_schema().to_string(),
        analysed(&state, today).display_indent_schema().to_string()
    );
}

#[tokio::test]
async fn differing_settings_close_the_frame_under_its_own_settings() {
    let reader = session(false);
    let frame = DataFrame::new(held_on(&reader), unanalysed(&reader, OVERFLOW).await);
    reader
        .create_or_replace_temp_view_from("v", &frame)
        .unwrap_or_else(|error| panic!("register: {error}"));
    let read = reader
        .sql("SELECT m FROM v")
        .await
        .unwrap_or_else(|error| panic!("read: {error}"));
    assert!(!holds_marker(read.logical_plan()).unwrap_or(true));
    assert_eq!(closed_views(read.logical_plan()), 1);
    let error = rows(read).await.err().unwrap_or_default();
    assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
    reader
        .context()
        .register_table("today", frame.clone().into_view())
        .unwrap_or_else(|error| panic!("register today: {error}"));
    let wrapped = rows(
        reader
            .context()
            .sql("SELECT m FROM today")
            .await
            .unwrap_or_else(|error| panic!("today: {error}")),
    )
    .await
    .unwrap_or_else(|error| panic!("today's provider follows the reader: {error}"));
    assert!(wrapped.contains("-2147483648"), "{wrapped}");
}

#[tokio::test]
async fn the_marker_is_decided_under_the_planning_state_not_the_live_one() {
    let reader = session(false);
    let frame = DataFrame::new(held_on(&reader), unanalysed(&reader, OVERFLOW).await);
    let plan = scanned(wrapped(&frame));
    let on = held_on(&reader);
    let under_on = FrameSettingsRule
        .analyze(plan.clone(), on.config_options())
        .unwrap_or_else(|error| panic!("decide: {error}"));
    assert!(!holds_marker(&under_on).unwrap_or(true));
    assert_eq!(closed_views(&under_on), 0);
    let live = reader.context().state();
    let under_off = FrameSettingsRule
        .analyze(plan, live.config_options())
        .unwrap_or_else(|error| panic!("decide: {error}"));
    assert_eq!(closed_views(&under_off), 1);
    let LogicalPlan::SubqueryAlias(alias) = &under_off else {
        panic!(
            "the inlined alias stays on top: {}",
            under_off.display_indent()
        );
    };
    let LogicalPlan::Projection(projection) = alias.input.as_ref() else {
        panic!("a projection restores the frame's schema");
    };
    let wanted = frame.logical_plan().schema();
    let typed = analysed(&on, frame.logical_plan().clone());
    let restored = projection
        .schema
        .iter()
        .zip(wanted.iter())
        .zip(typed.schema().iter());
    for (((qualifier, field), (want_qualifier, want)), (_, typed)) in restored {
        assert_eq!(qualifier, want_qualifier);
        assert_eq!(field.name(), want.name());
        assert_eq!(field.metadata(), want.metadata());
        assert_eq!(field.data_type(), typed.data_type());
    }
    assert_eq!(projection.schema.fields().len(), wanted.fields().len());
}

#[tokio::test]
async fn walkers_see_through_a_closed_frame() {
    let reader = session(false);
    let base = TableReference::full("datafusion", "public", "base");
    reader
        .context()
        .register_table(
            base.clone(),
            reader
                .context()
                .sql(OVERFLOW)
                .await
                .unwrap_or_else(|error| panic!("base: {error}"))
                .into_view(),
        )
        .unwrap_or_else(|error| panic!("register base: {error}"));
    let inner = reader
        .context()
        .table(base.clone())
        .await
        .unwrap_or_else(|error| panic!("read base: {error}"))
        .into_unoptimized_plan();
    let frame = DataFrame::new(held_on(&reader), inner.clone());
    let live = reader.context().state();
    let closed = FrameSettingsRule
        .analyze(scanned(wrapped(&frame)), live.config_options())
        .unwrap_or_else(|error| panic!("decide: {error}"));
    assert_eq!(closed_views(&closed), 1);
    let opened = opened(&closed).unwrap_or_else(|error| panic!("open: {error}"));
    assert_eq!(closed_views(&opened), 0);
    let mut scans = Vec::new();
    opened
        .apply_with_subqueries(|node| {
            if let LogicalPlan::SubqueryAlias(alias) = node {
                scans.push(alias.alias.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap_or_else(|error| panic!("walk: {error}"));
    assert!(scans.contains(&base), "{scans:?}");
    let mut sources = Vec::new();
    closed
        .apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Ok(provider) = datafusion::datasource::source_as_provider(&scan.source)
            {
                sources.push(provider);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap_or_else(|error| panic!("walk: {error}"));
    let [source] = sources.as_slice() else {
        panic!("one closed source: {sources:?}");
    };
    let typed = analysed(&held_on(&reader), inner);
    assert_eq!(definition_plan(source.as_ref()).as_ref(), Some(&typed));
    assert_eq!(source.get_logical_plan().as_deref(), Some(&typed));
    assert!(
        source
            .schema()
            .fields()
            .iter()
            .all(|field| field.metadata().is_empty())
    );
}

#[tokio::test]
async fn a_frame_view_answers_the_view_walkers_as_todays_provider_did() {
    let reader = session(true);
    let frame = reader
        .context()
        .sql(OVERFLOW)
        .await
        .unwrap_or_else(|error| panic!("frame: {error}"));
    let marked = wrapped(&frame);
    let today = frame.clone().into_view();
    assert_eq!(marked.schema(), today.schema());
    assert_eq!(marked.table_type(), today.table_type());
    for provider in [&marked, &today] {
        assert!(matches!(
            temp_view_column_comments(provider.as_ref()),
            Ok(None)
        ));
        assert!(matches!(
            renamed_temp_view(provider, Vec::new(), String::new()),
            Ok(None)
        ));
    }
    assert!(matches!(
        marked.get_logical_plan().as_deref(),
        Some(LogicalPlan::Extension(_))
    ));
}

#[tokio::test]
async fn the_sql_door_registration_stays_unwrapped() {
    let reader = session(true);
    reader
        .sql("CREATE TEMPORARY VIEW sv AS SELECT 1 AS a")
        .await
        .unwrap_or_else(|error| panic!("create: {error}"));
    let home = reader
        .resolve_temp_view_home_ref("sv")
        .unwrap_or_else(|error| panic!("home: {error}"))
        .unwrap_or_else(|| panic!("sv is registered"));
    let [catalog, schema, table] = home.as_slice() else {
        panic!("a three-part home: {home:?}");
    };
    let provider = reader
        .context()
        .table_provider(TableReference::full(
            catalog.as_str(),
            schema.as_str(),
            table.as_str(),
        ))
        .await
        .unwrap_or_else(|error| panic!("provider: {error}"));
    assert!(frame_plan(provider.as_ref()).is_none());
    assert!(matches!(
        provider.get_logical_plan().as_deref(),
        Some(LogicalPlan::TableScan(_))
    ));
}

#[tokio::test]
async fn materialisation_takes_the_frames_options_and_its_own_start() {
    let reader = session(false);
    let past = Utc
        .with_ymd_and_hms(2001, 1, 1, 0, 0, 0)
        .single()
        .unwrap_or_else(|| panic!("a fixed instant"));
    let mut overflowing = held_on(&reader);
    overflowing.execution_props_mut().query_execution_start_time = Some(past);
    let frame = DataFrame::new(overflowing, unanalysed(&reader, OVERFLOW).await);
    let error = reader
        .materialize_dataframe_as_temp_view("c1", frame)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
    let mut stamped = held_on(&reader);
    stamped.execution_props_mut().query_execution_start_time = Some(past);
    let clock = unanalysed(&reader, "SELECT CAST(now() AS STRING) AS t").await;
    reader
        .materialize_dataframe_as_temp_view("c2", DataFrame::new(stamped, clock))
        .await
        .unwrap_or_else(|error| panic!("materialise: {error}"));
    let cached = rows(
        reader
            .sql("SELECT t FROM c2")
            .await
            .unwrap_or_else(|error| panic!("read c2: {error}")),
    )
    .await
    .unwrap_or_else(|error| panic!("collect c2: {error}"));
    assert!(!cached.contains("2001-01-01"), "{cached}");
}

#[tokio::test]
async fn a_writer_v2_scratch_view_takes_the_writers_zone_and_the_frames_ansi() {
    let creation = session(true);
    let frame = creation
        .context()
        .sql("SELECT hour(TIMESTAMP '2024-01-01 02:30:00') AS m")
        .await
        .unwrap_or_else(|error| panic!("frame: {error}"));
    let writer = zoned(true, "America/Los_Angeles").context().state();
    let decide = |name: &str| {
        let provider = frame_view(name, &frame, writer.config_options());
        let plan = FrameSettingsRule
            .analyze(scanned(provider), writer.config_options())
            .unwrap_or_else(|error| panic!("decide {name}: {error}"));
        closed_views(&plan)
    };
    assert_eq!(decide("__repark_selx_1"), 1);
    assert_eq!(decide("`datafusion`.`public`.`_repark_writer_v2_9`"), 0);
    let off = zoned(false, "America/Los_Angeles").context().state();
    let provider = frame_view("_repark_writer_v2_9", &frame, writer.config_options());
    let plan = FrameSettingsRule
        .analyze(scanned(provider), off.config_options())
        .unwrap_or_else(|error| panic!("decide off: {error}"));
    assert_eq!(closed_views(&plan), 1);
}

#[test]
fn an_absent_setting_matches_its_default_value() {
    use repark_functions::ansi::SparkAnsiConfig;
    use repark_functions::decimal_legacy::SparkDecimalTruncateLegacy;
    let mut options = ConfigOptions::new();
    let captured = TempViewSettings::capture(&options);
    options
        .extensions
        .insert(SparkDecimalTruncateLegacy { enabled: false });
    options.extensions.insert(SparkAnsiConfig::default());
    assert!(captured.same_as(&options));
    options
        .extensions
        .insert(SparkDecimalTruncateLegacy { enabled: true });
    assert!(!captured.same_as(&options));
}

#[tokio::test]
async fn a_frame_that_only_scans_keeps_todays_provider() {
    let reader = session(true);
    let base = TableReference::full("datafusion", "public", "scanned");
    reader
        .context()
        .register_table(
            base.clone(),
            reader
                .context()
                .sql(OVERFLOW)
                .await
                .unwrap_or_else(|error| panic!("base: {error}"))
                .into_view(),
        )
        .unwrap_or_else(|error| panic!("register: {error}"));
    let scan = DataFrame::new(
        reader.context().state(),
        LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(datafusion::datasource::empty::EmptyTable::new(
                Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                    datafusion::arrow::datatypes::Field::new(
                        "x",
                        datafusion::arrow::datatypes::DataType::Int32,
                        true,
                    ),
                ])),
            ))),
            None,
        )
        .and_then(|builder| builder.project(vec![datafusion::logical_expr::col("x").alias("y")]))
        .and_then(LogicalPlanBuilder::build)
        .unwrap_or_else(|error| panic!("scan: {error}")),
    );
    assert!(frame_plan(wrapped(&scan).as_ref()).is_none());
    let computing = scan
        .clone()
        .select(vec![
            datafusion::logical_expr::col("y") + datafusion::logical_expr::lit(1),
        ])
        .unwrap_or_else(|error| panic!("compute: {error}"));
    assert!(frame_plan(wrapped(&computing).as_ref()).is_some());
}
