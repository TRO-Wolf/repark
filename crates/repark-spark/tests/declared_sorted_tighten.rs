//! SE-1 PR-D1: Spark-door execution-layer pin for `tightenNulls` on the `WindowSpec`
//! serving shape (nullable keys, Spark ASC → NULLS FIRST), plus the Iceberg-CREATE refuse.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .target_partitions(1)
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap()
}

fn nullable_sorted_rows(per_symbol: i64) -> RecordBatch {
    let mut symbols = Vec::new();
    let mut timestamps = Vec::new();
    let mut close = Vec::new();
    for symbol in ["AAA", "BBB"] {
        for tick in 0..per_symbol {
            symbols.push(symbol);
            timestamps.push(Some(tick));
            close.push(100.0 + f64::from(u32::try_from(tick).unwrap()));
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("close", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(symbols)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(close)),
        ],
    )
    .unwrap()
}

fn keys() -> Vec<String> {
    vec!["symbol".to_string(), "ts".to_string()]
}

/// Spark-default window: `ORDER BY ts` is NULLS FIRST. This is the cell hint-mode cannot elide
/// over nullable keys.
const SERVING_WINDOW: &str =
    "SELECT symbol, ts, sum(close) OVER (PARTITION BY symbol ORDER BY ts) AS s FROM {table}";

async fn physical_plan_text(session: &ReparkSession, sql: &str) -> String {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("must plan `{sql}`: {error}"));
    let plan = frame
        .create_physical_plan()
        .await
        .unwrap_or_else(|error| panic!("physical plan `{sql}`: {error}"));
    datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string()
}

fn sort_exec_count(plan: &str) -> usize {
    plan.matches("SortExec").count()
}

#[tokio::test]
async fn tighten_elides_spark_default_window_over_nullable_keys() {
    let session = spark_session();
    let rows = nullable_sorted_rows(20_000);
    session
        .register_record_batches_as_temp_view("hint", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("hint", &keys(), false)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();

    let hint_plan = physical_plan_text(&session, &SERVING_WINDOW.replace("{table}", "hint")).await;
    let tight_plan =
        physical_plan_text(&session, &SERVING_WINDOW.replace("{table}", "tight")).await;
    assert!(
        sort_exec_count(&hint_plan) >= 1,
        "hint mode must keep SortExec on Spark NULLS FIRST over nullable keys:\n{hint_plan}"
    );
    assert_eq!(
        sort_exec_count(&tight_plan),
        0,
        "tighten must elide SortExec on the serving shape:\n{tight_plan}"
    );
}

#[tokio::test]
async fn iceberg_create_of_tightened_frame_refuses_insert_into_existing_allowed() {
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();

    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();

    let refused = session
        .sql("CREATE TABLE ice.sales.tightened AS SELECT * FROM tight")
        .await
        .expect_err("tightened CTAS must refuse");
    let message = refused.to_string();
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
    assert!(message.contains("PR-D2"), "names the follow-up: {message}");

    session
        .sql("CREATE TABLE ice.sales.bars AS SELECT * FROM plain")
        .await
        .expect("untightened CTAS must succeed");
    session
        .sql("INSERT INTO ice.sales.bars SELECT * FROM tight")
        .await
        .expect("INSERT into an existing table stays allowed")
        .collect()
        .await
        .expect("collect insert");
}

#[tokio::test]
async fn iceberg_create_from_derived_expression_over_tightened_source_refuses() {
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    let refused = session
        .sql("CREATE TABLE ice.sales.derived AS SELECT ts + 1 AS ts2 FROM tight")
        .await
        .expect_err("derived-expression CTAS must refuse via the source walk");
    let message = refused.to_string();
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
}

#[tokio::test]
async fn iceberg_create_from_subquery_over_tightened_source_refuses() {
    // Kills: plan.apply (no subquery walk) letting a CTAS whose only tightened source sits in an
    // EXISTS subquery persist required (R-B). Y-5 (round 4): the prose said "scalar-subquery";
    // the SQL below is and always was `WHERE EXISTS (SELECT 1 FROM tight)`. The core twin
    // (`subquery_expression_source_is_visible_to_the_create_walk`) and the ANSI twin
    // (`ansi_ctas_from_subquery_over_tightened_source_refuses`) both said "expression-subquery"
    // and needed no change — checked, this door was the only drifted one. (The lane's octo
    // remediation round made the same correction independently.)
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    let refused = session
        .sql(
            "CREATE TABLE ice.sales.subq AS SELECT 1 AS n FROM plain \
             WHERE EXISTS (SELECT 1 FROM tight)",
        )
        .await
        .expect_err("subquery-expression CTAS must refuse via apply_with_subqueries");
    assert!(
        refused.to_string().contains("tightenNulls"),
        "names the flag: {refused}"
    );
}

#[tokio::test]
async fn iceberg_create_from_cached_derived_frame_refuses() {
    // Kills: cache remint dropping provenance so both doors go blind (R-A).
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    let derived = session
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .unwrap();
    session
        .materialize_dataframe_as_cache_view("cached", derived, None)
        .await
        .unwrap();
    let refused = session
        .sql("CREATE TABLE ice.sales.cached AS SELECT * FROM cached")
        .await
        .expect_err("cached derived CTAS must refuse");
    assert!(
        refused.to_string().contains("tightenNulls"),
        "names the flag: {refused}"
    );
}

#[tokio::test]
async fn iceberg_create_of_all_nullable_projection_is_allowed() {
    // Kills: hoisting refuse onto CREATEs that persist no required column (R-D allowed).
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    session
        .sql("CREATE TABLE ice.sales.nullable_only AS SELECT CAST(NULL AS BIGINT) AS n FROM tight")
        .await
        .expect("all-nullable projection must be allowed");
    session
        .sql("INSERT INTO ice.sales.nullable_only SELECT CAST(NULL AS BIGINT) FROM tight")
        .await
        .expect("INSERT into existing stays allowed")
        .collect()
        .await
        .expect("collect insert");
}

#[tokio::test]
async fn iceberg_create_from_lazy_view_of_derived_plan_refuses() {
    // Kills: into_view hop hiding the tightened MemTable on the Spark door (Q-001).
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    let derived = session
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .unwrap();
    session
        .create_or_replace_temp_view_from("d", &derived)
        .unwrap();
    let refused = session
        .sql("CREATE TABLE ice.sales.viewhop AS SELECT * FROM d")
        .await
        .expect_err("lazy view of derived plan must refuse");
    assert!(
        refused.to_string().contains("tightenNulls"),
        "names the flag: {refused}"
    );
}

/// Shared fixture for the round-4 DDL-sink pins (Y-3 / Y-4): an Iceberg catalog `ice` with
/// namespace `sales`, a tightened temp view `tight` and an untightened `plain`.
async fn ddl_sink_session() -> (TempDir, ReparkSession) {
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    (warehouse_dir, session)
}

#[tokio::test]
async fn create_view_in_iceberg_catalog_over_tightened_source_refuses() {
    // Y-3. Kills: the router `_ => execute_passthrough` catch-all letting `CREATE VIEW
    // ice.ns.v AS …` reach the fork's `register_table` sink, which persists a format-v2
    // Iceberg TABLE with required tightened keys. MEASURED on BASE (fe742a6): both statements
    // below returned Ok and `SELECT * FROM ice.sales.v` reported `symbol`/`ts` non-nullable
    // carrying `PARQUET:field_id` metadata. Deleting the
    // `refuse_iceberg_create_of_tightened_ddl` call in `spark_ast::execute_passthrough` turns
    // this red — the CTAS derivation never sees a `CREATE VIEW`.
    let (_dir, session) = ddl_sink_session().await;
    for sql in [
        "CREATE VIEW ice.sales.v_limit AS SELECT * FROM tight LIMIT 0",
        "CREATE VIEW ice.sales.v_false AS SELECT * FROM tight WHERE false",
    ] {
        let error = session
            .sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse — it persists an Iceberg table"));
        assert!(
            error.to_string().contains("tightenNulls"),
            "names the flag for `{sql}`: {error}"
        );
    }
}

#[tokio::test]
async fn select_into_iceberg_catalog_over_tightened_source_refuses() {
    // Y-4. Kills: `SELECT … INTO ice.ns.t` (planned as `CreateMemoryTable`) reaching the same
    // sink through the catch-all. Independent statement from Y-3 — a fix wired only to
    // `CreateView` leaves this green. MEASURED on BASE: Ok, and `SELECT * FROM ice.sales.t`
    // reported required `symbol`/`ts`.
    let (_dir, session) = ddl_sink_session().await;
    for sql in [
        "SELECT * INTO ice.sales.t_limit FROM tight LIMIT 0",
        "SELECT * INTO ice.sales.t_false FROM tight WHERE false",
    ] {
        let error = session
            .sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse — it persists an Iceberg table"));
        assert!(
            error.to_string().contains("tightenNulls"),
            "names the flag for `{sql}`: {error}"
        );
    }
}

#[tokio::test]
async fn session_scoped_create_view_and_select_into_over_tightened_source_stay_allowed() {
    // Y-3/Y-4 allowed side. Kills: a blanket DDL refuse. A one-part (session-scoped) name is
    // not a registered Iceberg catalog, persists nothing, and must keep working — the lazy-view
    // pins above depend on exactly this.
    let (_dir, session) = ddl_sink_session().await;
    session
        .sql("CREATE VIEW session_v AS SELECT * FROM tight")
        .await
        .expect("session-scoped CREATE VIEW must stay allowed")
        .collect()
        .await
        .expect("collect create view");
    session
        .sql("SELECT * INTO session_t FROM tight")
        .await
        .expect("session-scoped SELECT INTO must stay allowed")
        .collect()
        .await
        .expect("collect select into");
    let rows = session
        .sql("SELECT count(*) AS n FROM session_v")
        .await
        .expect("session view must read back")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        rows.iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn create_view_in_iceberg_catalog_over_untightened_source_stays_allowed() {
    // Y-3 allowed side + the PAYLOAD boundary: `CREATE VIEW` persisting an Iceberg table at all
    // predates this branch (MEASURED on BASE with `plain`). This round fixes only the tighten
    // leak, so the untightened statement must still behave exactly as it did on BASE.
    let (_dir, session) = ddl_sink_session().await;
    session
        .sql("CREATE VIEW ice.sales.v_plain AS SELECT * FROM plain LIMIT 0")
        .await
        .expect("untightened CREATE VIEW is unchanged by this round")
        .collect()
        .await
        .expect("collect");
}

// ===========================================================================================
// SQM round 5 — Spark door: resolved-catalog gating (Z-1) and the CTAS-wrapped DDL sink (Z-3).
// ===========================================================================================

#[tokio::test]
async fn default_catalog_bare_name_ddl_over_tightened_source_refuses() {
    // Z-1, Spark door. Kills: gating the DDL refuse on the three-part SPELLING — with
    // `SET datafusion.catalog.default_catalog = ice` (+ `default_schema = sales`) a one-part or
    // two-part name resolves into the Iceberg catalog and persists the same required columns.
    //
    // MEASURED on BASE (675a413), PER ROW — R6-3 (round 6) corrects the earlier blanket
    // "every statement below returned Ok", which was false for the Full row:
    //   - `CREATE VIEW v_bare …`      (Bare)    → **Ok** on BASE (the red this pin kills)
    //   - `SELECT * INTO t_bare …`    (Bare)    → **Ok** on BASE (the red this pin kills)
    //   - `CREATE VIEW sales.v_partial …` (Partial) → **Ok** on BASE (the red this pin kills)
    //   - `CREATE VIEW ice.sales.v_full …` (Full) → **already refused** on BASE (round 4 wired
    //     the three-part spelling on this door); it is the regression fence, not a round-5 red.
    let (_dir, session) = ddl_sink_session().await;
    session
        .sql("SET datafusion.catalog.default_catalog = 'ice'")
        .await
        .expect("SET default_catalog");
    session
        .sql("SET datafusion.catalog.default_schema = 'sales'")
        .await
        .expect("SET default_schema");
    //
    // R6-4 (round 6): each refusal also asserts the UNPUBLISHED half — `table_exists` FALSE for
    // the name the statement resolves to.
    for (sql, resolved) in [
        (
            "CREATE VIEW v_bare AS SELECT * FROM datafusion.public.tight LIMIT 0",
            "ice.sales.v_bare",
        ),
        (
            "SELECT * INTO t_bare FROM datafusion.public.tight LIMIT 0",
            "ice.sales.t_bare",
        ),
        (
            "CREATE VIEW sales.v_partial AS SELECT * FROM datafusion.public.tight LIMIT 0",
            "ice.sales.v_partial",
        ),
        // Three-part still refuses — round 4's behaviour is not traded away.
        (
            "CREATE VIEW ice.sales.v_full AS SELECT * FROM datafusion.public.tight LIMIT 0",
            "ice.sales.v_full",
        ),
    ] {
        let error = session
            .sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse — it resolves into `ice`"));
        assert!(
            error.to_string().contains("tightenNulls"),
            "names the flag for `{sql}`: {error}"
        );
        assert!(
            !session.table_exists(resolved).await.unwrap(),
            "`{sql}` refused but `{resolved}` was persisted anyway (R6-4 unpublished half)"
        );
    }
}

#[tokio::test]
async fn ctas_wrapping_a_ddl_sink_refuses_without_publishing_the_inner_table() {
    // Z-3, Spark door. This door plans the CTAS body through `execute_passthrough`, which now
    // runs the shared belt's guard on the planned statement — so the inner
    // `SELECT … INTO ice.sales.wrap_inner` refuses BEFORE it can publish. MEASURED on BASE
    // (675a413): already green on THIS door (round 4's passthrough refuse fires on the inner
    // statement's plan) — it is here as the regression fence for the outcome the ANSI door had
    // to be fixed to match, where BASE returned Ok and persisted BOTH tables.
    let (_dir, session) = ddl_sink_session().await;
    let sql =
        "CREATE TABLE ice.sales.wrap AS SELECT * INTO ice.sales.wrap_inner FROM tight LIMIT 0";
    let error = session
        .sql(sql)
        .await
        .expect_err("a CTAS wrapping a tightened DDL sink must refuse");
    assert!(
        error.to_string().contains("tightenNulls"),
        "names the flag: {error}"
    );
    assert!(
        !session
            .table_exists("ice.sales.wrap_inner")
            .await
            .expect("table_exists must answer"),
        "the inner DDL sink must NOT have been published"
    );
}
