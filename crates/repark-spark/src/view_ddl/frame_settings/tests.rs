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
    frame_view(
        "v",
        frame,
        frame.task_ctx().session_config().options(),
        LiveOptions::default(),
    )
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
async fn inside_a_sql_view_body_the_marker_is_decided_under_the_planning_state() {
    let reader = session(false);
    let frame = DataFrame::new(held_on(&reader), unanalysed(&reader, OVERFLOW).await);
    reader
        .create_or_replace_temp_view_from("v", &frame)
        .unwrap_or_else(|error| panic!("register: {error}"));
    let outside = unanalysed(&reader, "SELECT * FROM v").await;
    assert!(!holds_marker(&outside).unwrap_or(true));
    assert_eq!(closed_views(&outside), 1);
    let plan = view_body(unanalysed(&reader, "SELECT * FROM v")).await;
    assert!(holds_marker(&plan).unwrap_or(false));
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
    assert!(!holds_marker(&under_off).unwrap_or(true));
    assert_eq!(closed_views(&under_off), 1);
    let Some(LogicalPlan::Projection(projection)) = inlined_view(&under_off, "v") else {
        panic!(
            "a projection restores the frame's schema: {}",
            under_off.display_indent()
        );
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
        let provider = frame_view(
            name,
            &frame,
            writer.config_options(),
            LiveOptions::default(),
        );
        let plan = FrameSettingsRule
            .analyze(scanned(provider), writer.config_options())
            .unwrap_or_else(|error| panic!("decide {name}: {error}"));
        closed_views(&plan)
    };
    assert_eq!(decide("__repark_selx_1"), 1);
    assert_eq!(decide("`datafusion`.`public`.`_repark_writer_v2_9`"), 0);
    let off = zoned(false, "America/Los_Angeles").context().state();
    let provider = frame_view(
        "_repark_writer_v2_9",
        &frame,
        writer.config_options(),
        LiveOptions::default(),
    );
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

async fn json_frame(reader: &ReparkSession, root: &std::path::Path) -> DataFrame {
    let path = root.join("a.json");
    std::fs::write(&path, "{\"n\": 1, \"ts\": \"2024-01-01 03:00:00\"}\n")
        .unwrap_or_else(|error| panic!("write: {error}"));
    let path = path.to_str().unwrap_or_else(|| panic!("utf-8 path"));
    reader
        .read_json(path, &std::collections::HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("read: {error}"))
        .select(vec![
            datafusion::logical_expr::col("n"),
            datafusion::logical_expr::col("ts").alias("t"),
        ])
        .unwrap_or_else(|error| panic!("select: {error}"))
}

#[tokio::test]
async fn a_json_reader_lineage_registers_as_any_frame_does() {
    let reader = session(true);
    let root = tempfile::tempdir().unwrap_or_else(|error| panic!("dir: {error}"));
    let json = json_frame(&reader, root.path()).await;
    let computing = json
        .clone()
        .select(vec![
            datafusion::logical_expr::col("n") + datafusion::logical_expr::lit(1),
        ])
        .unwrap_or_else(|error| panic!("compute: {error}"));
    assert!(frame_plan(wrapped(&computing).as_ref()).is_some());
    reader
        .context()
        .register_table("jv", wrapped(&json))
        .unwrap_or_else(|error| panic!("register: {error}"));
    let over = reader
        .context()
        .table("jv")
        .await
        .unwrap_or_else(|error| panic!("table: {error}"))
        .select(vec![
            datafusion::logical_expr::col("n") * datafusion::logical_expr::lit(2),
        ])
        .unwrap_or_else(|error| panic!("over: {error}"));
    assert!(frame_plan(wrapped(&over).as_ref()).is_some());
}

const LA: &str = "America/Los_Angeles";
const PARSE: &str = "SELECT n, CAST(t AS TIMESTAMP) AS ts FROM raw";

async fn reader_parse_frames(
    reader: &ReparkSession,
    root: &std::path::Path,
) -> (DataFrame, DataFrame) {
    let raw = json_frame(reader, root).await;
    reader
        .context()
        .register_table("raw", raw.into_view())
        .unwrap_or_else(|error| panic!("register raw: {error}"));
    let bare = reader
        .context()
        .sql(PARSE)
        .await
        .unwrap_or_else(|error| panic!("parse: {error}"));
    let parse = repark_core::extension::reader_parse(&bare)
        .unwrap_or_else(|| panic!("the frame seat wraps a reader parse"));
    (bare, parse)
}

fn live_zone(reader: &ReparkSession, zone: &str) {
    let settings = TempViewSettings::capture(zoned(true, zone).context().state().config_options());
    settings.apply(
        reader
            .context()
            .state_ref()
            .write()
            .config_mut()
            .options_mut(),
    );
}

fn optimized(frame: &DataFrame) -> LogicalPlan {
    frame
        .clone()
        .into_optimized_plan()
        .unwrap_or_else(|error| panic!("optimize: {error}"))
}

async fn parsed_under(zone: &str, sql: &str) -> std::result::Result<String, String> {
    let fresh = zoned(true, zone);
    let root = tempfile::tempdir().unwrap_or_else(|error| panic!("dir: {error}"));
    let (bare, _) = reader_parse_frames(&fresh, root.path()).await;
    fresh
        .context()
        .register_table("jv", bare.into_view())
        .unwrap_or_else(|error| panic!("register: {error}"));
    let frame = fresh
        .context()
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    rows(frame).await
}

#[test]
fn the_reader_parse_rule_runs_first_in_the_optimizer() {
    let state = session(true).context().state();
    let names = state
        .optimizers()
        .iter()
        .map(|rule| rule.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names.first().map(String::as_str), Some(READER_PARSE_RULE));
    assert_eq!(
        names
            .iter()
            .filter(|name| *name == READER_PARSE_RULE)
            .count(),
        1
    );
}

#[tokio::test]
async fn under_matching_settings_a_reader_parse_becomes_its_analysed_input() {
    let reader = zoned(true, LA);
    let root = tempfile::tempdir().unwrap_or_else(|error| panic!("dir: {error}"));
    let (bare, parse) = reader_parse_frames(&reader, root.path()).await;
    assert!(holds_parse(parse.logical_plan()).unwrap_or(false));
    let decided = optimized(&parse);
    assert!(!holds_parse(&decided).unwrap_or(true));
    assert_eq!(closed_views(&decided), 0);
    assert_eq!(
        decided.display_indent_schema().to_string(),
        optimized(&bare).display_indent_schema().to_string()
    );
    assert_eq!(rows(parse).await, rows(bare).await);
}

#[tokio::test]
async fn under_differing_settings_a_reader_parse_reads_its_copy_under_the_live_settings() {
    let reader = zoned(true, LA);
    let root = tempfile::tempdir().unwrap_or_else(|error| panic!("dir: {error}"));
    let (bare, parse) = reader_parse_frames(&reader, root.path()).await;
    live_zone(&reader, "UTC");
    let decided = optimized(&parse);
    assert!(!holds_parse(&decided).unwrap_or(true));
    assert_eq!(closed_views(&decided), 1);
    let utc = parsed_under("UTC", "SELECT * FROM jv").await;
    assert!(utc.is_ok(), "{utc:?}");
    assert_eq!(rows(parse).await, utc);
    assert_ne!(rows(bare).await, utc);
}

#[tokio::test]
async fn a_reader_parse_inside_a_subquery_is_decided_under_both_settings() {
    let reader = zoned(true, LA);
    let root = tempfile::tempdir().unwrap_or_else(|error| panic!("dir: {error}"));
    let (_, parse) = reader_parse_frames(&reader, root.path()).await;
    reader
        .context()
        .register_table("jv", parse.into_view())
        .unwrap_or_else(|error| panic!("register: {error}"));
    let sql = "SELECT n, (SELECT max(ts) FROM jv) AS m FROM raw WHERE n IN (SELECT n FROM jv)";
    let frame = reader
        .context()
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert!(holds_parse(frame.logical_plan()).unwrap_or(false));
    let decided = optimized(&frame);
    assert!(!holds_parse(&decided).unwrap_or(true));
    assert_eq!(closed_views(&decided), 0);
    let la = parsed_under(LA, sql).await;
    assert!(la.is_ok(), "{la:?}");
    assert_eq!(rows(frame.clone()).await, la);
    live_zone(&reader, "UTC");
    let decided = optimized(&frame);
    assert!(!holds_parse(&decided).unwrap_or(true));
    assert_eq!(closed_views(&decided), 2);
    let utc = parsed_under("UTC", sql).await;
    assert_ne!(la, utc);
    assert_eq!(rows(frame).await, utc);
}

fn inlined_view(plan: &LogicalPlan, name: &str) -> Option<LogicalPlan> {
    let mut inlined = None;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias.alias.table() == name
        {
            inlined = Some(alias.input.as_ref().clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap_or_else(|error| panic!("walk: {error}"));
    inlined
}

async fn read_plan(reader: &ReparkSession, sql: &str) -> LogicalPlan {
    reader
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("read {sql}: {error}"))
        .logical_plan()
        .clone()
}

#[tokio::test]
async fn a_reader_under_the_frames_settings_inlines_the_frames_own_plan() {
    let reader = session(true);
    let frame = reader
        .context()
        .sql(OVERFLOW)
        .await
        .unwrap_or_else(|error| panic!("frame: {error}"));
    reader
        .create_or_replace_temp_view_from("v", &frame)
        .unwrap_or_else(|error| panic!("register: {error}"));
    let read = unanalysed(&reader, "SELECT m FROM v").await;
    assert!(!holds_marker(&read).unwrap_or(true));
    assert_eq!(closed_views(&read), 0);
    assert_eq!(
        inlined_view(&read, "v").as_ref(),
        Some(frame.logical_plan())
    );
    let body = view_body(unanalysed(&reader, "SELECT m FROM v")).await;
    assert!(holds_marker(&body).unwrap_or(false));
}

#[tokio::test]
async fn the_negated_null_store_check_reads_through_a_frame_view_on_both_paths() {
    use datafusion::arrow::datatypes::DataType;
    use repark_iceberg::write::negated_null_store::refuse_negated_null_writes;
    const SOURCE: &str = "SELECT 127 AS id, -NULL AS c";
    let reader = session(false);
    let matching = reader
        .context()
        .sql(SOURCE)
        .await
        .unwrap_or_else(|error| panic!("frame: {error}"));
    let differing = DataFrame::new(held_on(&reader), unanalysed(&reader, SOURCE).await);
    for (name, frame) in [("v", &matching), ("w", &differing)] {
        reader
            .create_or_replace_temp_view_from(name, frame)
            .unwrap_or_else(|error| panic!("register {name}: {error}"));
        let read = read_plan(&reader, &format!("SELECT * FROM {name}")).await;
        assert!(!holds_marker(&read).unwrap_or(true), "{name}");
        let refused = refuse_negated_null_writes(
            reader.context(),
            "`t`",
            &read,
            [("id", &DataType::Int32), ("c", &DataType::Date32)],
        )
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
        assert!(refused.contains("CANNOT_SAFELY_CAST"), "{name}: {refused}");
    }
    assert_eq!(
        closed_views(&read_plan(&reader, "SELECT * FROM w").await),
        1
    );
}

async fn registered_view(reader: &ReparkSession, name: &str) -> Arc<dyn TableProvider> {
    let home = reader
        .resolve_temp_view_home_ref(name)
        .unwrap_or_else(|error| panic!("home: {error}"))
        .unwrap_or_else(|| panic!("{name} is registered"));
    let [catalog, schema, table] = home.as_slice() else {
        panic!("a three-part home: {home:?}");
    };
    reader
        .context()
        .table_provider(TableReference::full(
            catalog.as_str(),
            schema.as_str(),
            table.as_str(),
        ))
        .await
        .unwrap_or_else(|error| panic!("provider: {error}"))
}

fn inlined_while_written<T>(
    reader: &ReparkSession,
    hold: impl FnOnce(&mut SessionState) + Send,
    inline: impl FnOnce() -> T,
    released: std::time::Duration,
) -> T {
    let shared = reader.context().state_ref();
    let (locked, holding) = std::sync::mpsc::channel();
    let (release, releasing) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut guard = shared.write();
            hold(&mut guard);
            let _ = locked.send(());
            let _ = releasing.recv_timeout(released);
        });
        holding
            .recv()
            .unwrap_or_else(|error| panic!("the writer holds the lock: {error}"));
        let found = inline();
        let _ = release.send(());
        found
    })
}

#[tokio::test]
async fn a_held_state_write_lock_never_inlines_the_marker_outside_a_view_body() {
    let reader = session(false);
    let matching = reader
        .context()
        .sql(OVERFLOW)
        .await
        .unwrap_or_else(|error| panic!("frame: {error}"));
    let differing = DataFrame::new(held_on(&reader), unanalysed(&reader, OVERFLOW).await);
    for (name, frame) in [("v", &matching), ("w", &differing)] {
        reader
            .create_or_replace_temp_view_from(name, frame)
            .unwrap_or_else(|error| panic!("register {name}: {error}"));
    }
    let same = registered_view(&reader, "v").await;
    let other = registered_view(&reader, "w").await;
    let shared = reader.context().state_ref();
    let (same, other) = inlined_while_written(
        &reader,
        |_| {},
        || {
            assert!(shared.try_read_recursive().is_none());
            (
                same.get_logical_plan().map(Cow::into_owned),
                other.get_logical_plan().map(Cow::into_owned),
            )
        },
        std::time::Duration::from_mins(1),
    );
    let same = same.unwrap_or_else(|| panic!("a frame view inlines a plan"));
    assert!(!holds_marker(&same).unwrap_or(true));
    assert_eq!(&same, matching.logical_plan());
    let other = other.unwrap_or_else(|| panic!("a frame view inlines a plan"));
    assert!(!holds_marker(&other).unwrap_or(true));
    assert_eq!(closed_views(&other), 1);
}

#[tokio::test]
async fn a_live_read_waits_for_a_writer_and_decides_under_its_setting() {
    let reader = session(true);
    let frame = reader
        .context()
        .sql(OVERFLOW)
        .await
        .unwrap_or_else(|error| panic!("frame: {error}"));
    reader
        .create_or_replace_temp_view_from("v", &frame)
        .unwrap_or_else(|error| panic!("register: {error}"));
    let view = registered_view(&reader, "v").await;
    let off = TempViewSettings::capture(session(false).context().state().config_options());
    let inlined = inlined_while_written(
        &reader,
        |state| off.apply(state.config_mut().options_mut()),
        || view.get_logical_plan().map(Cow::into_owned),
        std::time::Duration::from_millis(100),
    )
    .unwrap_or_else(|| panic!("a frame view inlines a plan"));
    assert!(!holds_marker(&inlined).unwrap_or(true));
    assert_eq!(closed_views(&inlined), 1);
}
