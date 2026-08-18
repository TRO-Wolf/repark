//! SE-1 PR-D1: ANSI-door Iceberg-CREATE refuse of a `tightenNulls` frame.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

async fn ansi_session(warehouse: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("session must build");
    session
        .register_memory_catalog("ice", warehouse)
        .await
        .expect("catalog must register");
    session
}

fn nullable_sorted_rows() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["AAA", "AAA", "BBB"])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1)])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn ansi_ctas_of_tightened_frame_refuses_insert_into_existing_allowed() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");

    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
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
async fn ansi_ctas_from_derived_expression_over_tightened_source_refuses() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
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
async fn ansi_ctas_from_subquery_over_tightened_source_refuses() {
    // Kills: plan.apply missing expression-subquery scans on the ANSI door (R-B).
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();
    let refused = session
        .sql(
            "CREATE TABLE ice.sales.subq AS SELECT 1 AS n FROM plain \
             WHERE EXISTS (SELECT 1 FROM tight)",
        )
        .await
        .expect_err("ANSI subquery-expression CTAS must refuse");
    assert!(
        refused.to_string().contains("tightenNulls"),
        "names the flag: {refused}"
    );
}

#[tokio::test]
async fn ansi_ctas_from_cached_derived_frame_refuses() {
    // Kills: cache remint dropping provenance on the ANSI door (R-A).
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();
    let derived = session
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .expect("derived plan");
    session
        .materialize_dataframe_as_cache_view("cached", derived, None)
        .await
        .expect("cache remint");
    let refused = session
        .sql("CREATE TABLE ice.sales.cached AS SELECT * FROM cached")
        .await
        .expect_err("ANSI cached derived CTAS must refuse");
    assert!(
        refused.to_string().contains("tightenNulls"),
        "names the flag: {refused}"
    );
}

#[tokio::test]
async fn ansi_ctas_of_all_nullable_projection_is_allowed() {
    // Kills: hoisting refuse onto all-nullable CREATE / INSERT (R-D allowed).
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();
    session
        .sql("CREATE TABLE ice.sales.nullable_only AS SELECT CAST(NULL AS BIGINT) AS n FROM tight")
        .await
        .expect("ANSI all-nullable projection must be allowed");
    session
        .sql("INSERT INTO ice.sales.nullable_only SELECT CAST(NULL AS BIGINT) FROM tight")
        .await
        .expect("INSERT into existing stays allowed")
        .collect()
        .await
        .expect("collect insert");
}

#[tokio::test]
async fn ansi_ctas_from_lazy_view_of_derived_plan_refuses() {
    // Kills: into_view hop hiding the tightened MemTable on the ANSI door (Q-001).
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();
    let derived = session
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .expect("derived plan");
    session
        .create_or_replace_temp_view_from("d", &derived)
        .expect("lazy view");
    let refused = session
        .sql("CREATE TABLE ice.sales.viewhop AS SELECT * FROM d")
        .await
        .expect_err("ANSI lazy view of derived plan must refuse");
    assert!(
        refused.to_string().contains("tightenNulls"),
        "names the flag: {refused}"
    );
}

/// Shared fixture for the round-4 DDL-sink pins (Y-3 / Y-4, ANSI door).
async fn ansi_ddl_sink_session() -> (TempDir, ReparkSession) {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();
    (warehouse_dir, session)
}

#[tokio::test]
async fn ansi_create_view_in_iceberg_catalog_over_tightened_source_refuses() {
    // Y-3, ANSI twin. Kills: the ANSI router's `_ => delegate` arm letting `CREATE VIEW
    // ice.ns.v AS …` reach the Iceberg schema provider's `register_table` sink (which persists
    // a real table — MEASURED on BASE through the Spark door with the same provider).
    // Deleting the `refuse_iceberg_create_of_tightened_ddl` call in `router::delegate`
    // turns this red.
    let (_dir, session) = ansi_ddl_sink_session().await;
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
async fn ansi_select_into_iceberg_catalog_over_tightened_source_refuses() {
    // Y-4, ANSI twin. Independent statement (`CreateMemoryTable`, not `CreateView`): a fix
    // wired only to the `CreateView` DDL arm leaves this green — measured both ways.
    let (_dir, session) = ansi_ddl_sink_session().await;
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
async fn ansi_session_scoped_create_view_and_select_into_stay_allowed() {
    // Allowed side. Kills: a blanket DDL refuse — a one-part name is not a registered Iceberg
    // catalog and persists nothing.
    let (_dir, session) = ansi_ddl_sink_session().await;
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
}

#[tokio::test]
async fn ansi_create_view_in_iceberg_catalog_over_untightened_source_stays_allowed() {
    // The PAYLOAD boundary: `CREATE VIEW` persisting an Iceberg table at all predates this
    // branch. This round fixes only the tighten leak — the untightened statement is unchanged.
    let (_dir, session) = ansi_ddl_sink_session().await;
    session
        .sql("CREATE VIEW ice.sales.v_plain AS SELECT * FROM plain LIMIT 0")
        .await
        .expect("untightened CREATE VIEW is unchanged by this round")
        .collect()
        .await
        .expect("collect");
}

// ===========================================================================================
// SQM round 5 — ANSI door: resolved-catalog gating (Z-1) and plan-before-execute (Z-3).
// ===========================================================================================

#[tokio::test]
async fn ansi_default_catalog_bare_name_ddl_over_tightened_source_refuses() {
    // Z-1, ANSI door. Kills: gating the DDL refuse on the three-part SPELLING. With
    // `SET datafusion.catalog.default_catalog = ice` (+ `default_schema = sales`) a one-part
    // or two-part name resolves into the Iceberg catalog and persists the same required
    // columns. Deleting the resolve in `refuse_iceberg_create_of_tightened_ddl` (gating on
    // `TableReference::Full` again) turns this red.
    //
    // MEASURED on BASE (675a413) through this door's `delegate`, PER ROW (R6-3 discipline):
    // the Bare and Partial rows returned **Ok** (the reds this pin kills); the Full row
    // **already refused** on BASE (round 4 wired the three-part spelling on this door) and is
    // here as the regression fence.
    //
    // R6-4 (round 6): each refusal also asserts the UNPUBLISHED half — `table_exists` FALSE for
    // the name the statement resolves to.
    let (_dir, session) = ansi_ddl_sink_session().await;
    session
        .sql("SET datafusion.catalog.default_catalog = 'ice'")
        .await
        .expect("SET default_catalog");
    session
        .sql("SET datafusion.catalog.default_schema = 'sales'")
        .await
        .expect("SET default_schema");
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
async fn ansi_ctas_wrapping_a_ddl_sink_refuses_without_publishing_the_inner_table() {
    // Z-3, ANSI door. Kills: `create_table.rs` deriving the CTAS schema with
    // `cx.ctx.sql(&query.to_string())`, which EXECUTES the inner statement — so
    // `SELECT … INTO ice.sales.inner_x` inside a CTAS published `inner_x` (tightened, required
    // columns) BEFORE the next line's refuse ever saw the plan. MEASURED on BASE (675a413) —
    // and it is worse than "refuses too late": the statement returned **Ok**, with
    // `ice.sales.wrap_inner` persisted carrying required `symbol` / `ts`, and `ice.sales.wrap`
    // created too. The eager `ctx.sql` returns the DDL's own empty result, so the
    // next-line `refuse_iceberg_create_of_tightened_plan` walked an `EmptyRelation` and saw no
    // tightened source at all. The Spark door refuses the same wrap; this pins the ANSI door to
    // that outcome AND to not publishing the inner table.
    let (_dir, session) = ansi_ddl_sink_session().await;
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
        "the inner DDL sink must NOT have been published — the refuse runs before execution"
    );
}

#[tokio::test]
async fn ansi_plain_ctas_still_derives_and_writes() {
    // Z-3 allowed side. Kills: the plan-before-execute rewrite breaking ordinary CTAS schema
    // derivation (the derived schema must still come from the same plan that is executed).
    let (_dir, session) = ansi_ddl_sink_session().await;
    session
        .sql("CREATE TABLE ice.sales.plain_ctas AS SELECT symbol, ts FROM plain")
        .await
        .expect("plain CTAS must still derive")
        .collect()
        .await
        .expect("collect");
    let rows = session
        .sql("SELECT * FROM ice.sales.plain_ctas")
        .await
        .expect("read back")
        .collect()
        .await
        .expect("collect read back");
    assert_eq!(
        rows.iter()
            .map(datafusion::arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        3,
        "plain CTAS must write every source row"
    );
}
