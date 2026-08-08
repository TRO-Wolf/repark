//! **Cross-door equivalence** — the Q13 / graft-G5 TWO-SESSION protocol (design §2 Q13).
//!
//! The rule the design is emphatic about, because all three design attempts got it wrong: a
//! cross-door row runs **two sessions**, not one.
//!
//! - **Session A — native**: no `SessionExtension` at all, `AnsiDialect` as the session dialect.
//! - **Session B — Spark-extended**: `SparkExtension` at the build hooks, `SparkDialect` as the
//!   session dialect.
//!
//! Each door is driven through its OWN session, and the FINAL Arrow results are compared — value
//! AND type. Why not one session with `sql_with`: extensions are **session-scoped, not
//! dialect-scoped**. A Spark-extended session has Spark expression semantics through every door,
//! including this one, so a `sql_with(AnsiDialect)` call on it would be measuring the Spark
//! analyzer wearing an ANSI hat. `sql_with` single-session rows are legal only for surfaces the
//! analyzer/UDF layer cannot touch (pure DDL/catalog ops), and each row below states its profile.
//!
//! Each session gets its OWN in-memory catalog over its OWN warehouse: the two doors must produce
//! equal RESULTS from independent state, which is a stronger claim than two doors agreeing about
//! one shared table.
//!
//! `repark-spark` is a DEV-dependency of this crate for exactly this file. The crate-DAG guard
//! scopes layering to normal edges (dev-/build-deps excluded by design), so no door→door product
//! edge exists — verified with `make check-crate-dag`. Nothing in `src/` may name `repark_spark`.
//!
//! AWS-free by construction.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

/// One side of a cross-door row: a session, its door, and the warehouse it owns.
struct Door {
    session: ReparkSession,
    warehouse: String,
    // Held so the temp dir outlives the session.
    _dir: TempDir,
}

/// **Session A** — the native profile: NO extension, `AnsiDialect` as the session default.
async fn native_ansi_door() -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    Door {
        session,
        warehouse,
        _dir: dir,
    }
}

/// **Session B** — the Spark-extended profile: `SparkExtension` at both build hooks,
/// `SparkDialect` as the session default. Assembled exactly as v1 assembled a session.
async fn spark_extended_door() -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .expect("spark session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    Door {
        session,
        warehouse,
        _dir: dir,
    }
}

/// Create namespace `ice.sales` on a door's own catalog, the way that door spells it.
///
/// Pure catalog DDL — the two spellings differ (`CREATE SCHEMA … WITH (location = …)` vs Spark's
/// `CREATE NAMESPACE … LOCATION`), which is precisely why the SETUP is per-door and only the
/// RESULT is compared.
async fn make_namespace(door: &Door, spark_spelling: bool) {
    let warehouse = &door.warehouse;
    let sql = if spark_spelling {
        format!("CREATE NAMESPACE ice.sales LOCATION '{warehouse}/sales'")
    } else {
        format!("CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')")
    };
    door.session
        .sql(&sql)
        .await
        .unwrap_or_else(|error| panic!("namespace DDL failed ({sql}): {error}"));
}

/// The comparison primitive: run `sql` and return `(field name, Arrow type)` pairs plus the rows
/// rendered as `(i64, String)`. Both halves are compared across doors — value AND type.
async fn typed_rows(
    session: &ReparkSession,
    sql: &str,
) -> (Vec<(String, DataType)>, Vec<(i64, String)>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let fields: Vec<(String, DataType)> = schema
        .fields()
        .iter()
        .map(|field: &Arc<Field>| (field.name().clone(), field.data_type().clone()))
        .collect();
    let batches = frame.collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0 must be Int64");
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 1 must be Utf8");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), labels.value(row).to_string()));
        }
    }
    rows.sort();
    (fields, rows)
}

/// ROW 1 — **CTAS**, content AND schema. Profile: **`TwoSession`** (native ANSI vs Spark-extended
/// Spark). The two doors spell the create differently and route it through different lowerings
/// (`repark_sql::create_table` vs the Spark door's ported CTAS handler), but they must land the
/// same Iceberg table: same column names, same Arrow types, same rows.
///
/// This is the drift guard design §6 R3 asks for: the duplicated thin lowerings cannot diverge
/// without turning a test red.
///
/// Mutation: change either door's CTAS to widen `id` to a different integer width → the schema
/// half REDs even though the value half would still pass.
#[tokio::test]
async fn cross_door_ctas_produces_the_same_table_content_and_schema() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    let body = "SELECT 1 AS id, 'a' AS label UNION ALL SELECT 2 AS id, 'b' AS label";
    ansi.session
        .sql(&format!("CREATE TABLE ice.sales.orders AS {body}"))
        .await
        .expect("ANSI CTAS");
    spark
        .session
        .sql(&format!("CREATE TABLE ice.sales.orders AS {body}"))
        .await
        .expect("Spark CTAS");

    let read = "SELECT id, label FROM ice.sales.orders ORDER BY id";
    let (ansi_fields, ansi_rows) = typed_rows(&ansi.session, read).await;
    let (spark_fields, spark_rows) = typed_rows(&spark.session, read).await;

    assert_eq!(
        ansi_fields, spark_fields,
        "CTAS through the two doors must land the same schema (name AND Arrow type)"
    );
    assert_eq!(
        ansi_rows, spark_rows,
        "CTAS through the two doors must land the same rows"
    );
    assert_eq!(
        ansi_rows,
        vec![(1, "a".to_string()), (2, "b".to_string())],
        "…and that shared result must be the right one (an equal-but-wrong pair would otherwise \
         pass both assertions above)"
    );
}

/// ROW 2 — **INSERT round-trip**, content AND schema. Profile: **`TwoSession`**. The ANSI door
/// delegates `INSERT` to the fork's `TableProvider` (ADR-0003); the Spark door routes it through
/// its ported eager-DML path. Same table, same rows afterwards.
///
/// Included because it is the DML pair PR-6 can pin without waiting on a handler: it exercises
/// the same commit machinery MERGE does, one statement shape lower.
#[tokio::test]
async fn cross_door_insert_lands_the_same_rows() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
            .await
            .expect("CTAS");
        door.session
            .sql("INSERT INTO ice.sales.orders VALUES (2, 'b')")
            .await
            .expect("INSERT")
            .collect()
            .await
            .expect("INSERT must execute eagerly enough to commit");
    }

    let read = "SELECT id, label FROM ice.sales.orders ORDER BY id";
    let (ansi_fields, ansi_rows) = typed_rows(&ansi.session, read).await;
    let (spark_fields, spark_rows) = typed_rows(&spark.session, read).await;
    assert_eq!(ansi_fields, spark_fields, "post-INSERT schema");
    assert_eq!(ansi_rows, spark_rows, "post-INSERT rows");
    assert_eq!(ansi_rows, vec![(1, "a".to_string()), (2, "b".to_string())]);
}

/// ROW 3 — **the protocol's own guard rail**, and the reason this file exists in this shape. A
/// Spark-extended session driven through `sql_with(AnsiDialect)` is NOT a native ANSI session:
/// the extension is installed on the SESSION, so its function registry and analyzer rules are
/// live no matter which door the text enters by.
///
/// Concretely: `date_add` is a Spark-door function registered by `SparkExtension`. On the native
/// session it does not exist; through the ANSI dialect ON THE EXTENDED SESSION it resolves fine.
/// That asymmetry is what makes single-session cross-door rows invalid — and it is the exact
/// sentence PR-6 freezes into `docs/design/session-api.md`.
///
/// Mutation: if extensions ever became dialect-scoped, this test REDs, and every cross-door row
/// above would need re-reading.
#[tokio::test]
async fn extensions_are_session_scoped_not_dialect_scoped() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;

    // Native session, ANSI door: the Spark function is absent.
    ansi.session
        .sql("SELECT date_add(DATE '2024-01-01', CAST(1 AS INT)) AS d")
        .await
        .expect_err("a native session must not know the Spark function set");

    // Spark-EXTENDED session, driven through the ANSI door: the same function resolves, because
    // the extension is session-scoped.
    let ansi_dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let through_ansi = spark
        .session
        .sql_with(
            &ansi_dialect,
            "SELECT date_add(DATE '2024-01-01', CAST(1 AS INT)) AS d",
        )
        .await;
    assert!(
        through_ansi.is_ok(),
        "an extended session has Spark expression semantics through EVERY door — that is why \
         cross-door rows need two sessions: {:?}",
        through_ansi.err()
    );
}

/// ROW 4 — **pure catalog DDL**, the one shape where a single session is legal (design §2 Q13:
/// "`sql_with` single-session is legal only for surfaces the analyzer/UDF layer cannot touch").
/// Profile: **Native, single-session `sql_with`** — recorded as such, deliberately not claimed as
/// `TwoSession`.
///
/// Namespace creation reaches the catalog without planning an expression, so no extension can
/// change its outcome; both doors' spellings must create the same namespace on one session.
#[tokio::test]
async fn cross_door_namespace_ddl_is_single_session_legal() {
    let ansi = native_ansi_door().await;
    let warehouse = &ansi.warehouse;

    // ANSI spelling on the session default.
    ansi.session
        .sql(&format!(
            "CREATE SCHEMA ice.a WITH (location = '{warehouse}/a')"
        ))
        .await
        .expect("ANSI CREATE SCHEMA");
    // Spark spelling through an explicitly-passed dialect on the SAME session — legal here, and
    // only here, because nothing about it is analyzer-visible.
    let spark_dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ansi.session
        .sql_with(
            &spark_dialect,
            &format!("CREATE NAMESPACE ice.b LOCATION '{warehouse}/b'"),
        )
        .await
        .expect("Spark CREATE NAMESPACE through the same session");

    // Both namespaces exist on the ONE catalog and are equally usable — the doors' different
    // spellings produced the same catalog effect.
    for (namespace, table) in [("a", "t_a"), ("b", "t_b")] {
        ansi.session
            .sql(&format!(
                "CREATE TABLE ice.{namespace}.{table} AS SELECT 1 AS id, 'x' AS label"
            ))
            .await
            .unwrap_or_else(|error| panic!("namespace ice.{namespace} unusable: {error}"));
        let (fields, rows) = typed_rows(
            &ansi.session,
            &format!("SELECT id, label FROM ice.{namespace}.{table}"),
        )
        .await;
        assert_eq!(
            fields,
            vec![
                ("id".to_string(), DataType::Int64),
                ("label".to_string(), DataType::Utf8),
            ],
            "ice.{namespace}.{table} schema"
        );
        assert_eq!(
            rows,
            vec![(1, "x".to_string())],
            "ice.{namespace}.{table} rows"
        );
    }
}

/// The full read schema of `ice.sales.orders` as `(name, Arrow type)` pairs — the comparison
/// primitive for the ALTER row, where the SCHEMA is the result under test.
async fn table_schema(session: &ReparkSession) -> Vec<(String, DataType)> {
    let frame = session
        .sql("SELECT * FROM ice.sales.orders")
        .await
        .expect("read for schema");
    frame
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|field: &Arc<Field>| (field.name().clone(), field.data_type().clone()))
        .collect()
}

/// ROW 5 — **ALTER**, evolved schema equality. Profile: **`TwoSession`**.
///
/// Both doors reach the SAME fork `UpdateSchema` calls through tier-1 `repark_iceberg::write::
/// alter` — but through independently-written lowerings from different grammars (ANSI
/// `ADD COLUMN c TYPE` / `ALTER COLUMN c SET DATA TYPE t` vs Spark `ADD COLUMNS (c TYPE)` /
/// `ALTER COLUMN c TYPE t`). Equal evolved schemas is the only thing that proves the two
/// lowerings agree about what they asked the fork for.
///
/// Mutation: make either door's ADD COLUMN land a different Iceberg type (or a different
/// required/optional flag that changes the Arrow nullability projection) → this REDs.
#[tokio::test]
async fn cross_door_alter_lands_the_same_evolved_schema() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label, 2 AS drop_me")
            .await
            .expect("CTAS");
    }

    // Same three evolutions, each door's own spelling.
    for sql in [
        "ALTER TABLE ice.sales.orders ADD COLUMN qty INT",
        "ALTER TABLE ice.sales.orders DROP COLUMN drop_me",
        "ALTER TABLE ice.sales.orders RENAME COLUMN label TO name",
    ] {
        ansi.session
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("ANSI `{sql}`: {error}"));
    }
    for sql in [
        "ALTER TABLE ice.sales.orders ADD COLUMNS (qty INT)",
        "ALTER TABLE ice.sales.orders DROP COLUMN drop_me",
        "ALTER TABLE ice.sales.orders RENAME COLUMN label TO name",
    ] {
        spark
            .session
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("Spark `{sql}`: {error}"));
    }

    let ansi_schema = table_schema(&ansi.session).await;
    let spark_schema = table_schema(&spark.session).await;
    assert_eq!(
        ansi_schema, spark_schema,
        "the two doors' ALTER lowerings must evolve the schema identically"
    );
    let names: Vec<&str> = ansi_schema.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "name", "qty"],
        "…and the shared schema must be the RIGHT one (add + drop + rename all applied)"
    );

    // TABLE rename (`ALTER TABLE … RENAME TO`) — the same spelling in both doors, both riding
    // `repark_iceberg::write::alter::rename_table`. The old name must be gone and the new one
    // must carry the evolved schema, identically on both sides.
    for door in [&ansi, &spark] {
        door.session
            .sql("ALTER TABLE ice.sales.orders RENAME TO ice.sales.orders_v2")
            .await
            .expect("RENAME TO");
        door.session
            .sql("SELECT * FROM ice.sales.orders")
            .await
            .expect_err("the old name must be gone after RENAME TO");
    }
    let renamed = "SELECT id, name FROM ice.sales.orders_v2";
    let (ansi_fields, ansi_rows) = typed_rows(&ansi.session, renamed).await;
    let (spark_fields, spark_rows) = typed_rows(&spark.session, renamed).await;
    assert_eq!(ansi_fields, spark_fields, "post-RENAME schema");
    assert_eq!(ansi_rows, spark_rows, "post-RENAME rows");
    assert_eq!(ansi_rows, vec![(1, "a".to_string())]);
}

/// ROW 6 — **MERGE**, result table. Profile: **`TwoSession`**.
///
/// Both doors lower to `MergeSpec` and execute through the RePark-owned tier-1
/// `repark_iceberg::write::merge::execute_merge` (never the fork's `TableProvider` — design §2
/// Q4). The lowerings are separate ~150-LOC translations of two grammars that happen to be nearly
/// identical; this row is the drift guard (§6 R3) over exactly that duplication.
///
/// The fixture exercises all three clause outcomes at once: one matched-update, one
/// matched-delete, one not-matched-insert, and one untouched row.
#[tokio::test]
async fn cross_door_merge_produces_the_same_result_table() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    let merge = "MERGE INTO ice.sales.orders AS t \
                 USING (SELECT 2 AS id, 'B' AS label UNION ALL \
                        SELECT 3 AS id, 'C' AS label UNION ALL \
                        SELECT 9 AS id, 'N' AS label) AS s \
                 ON t.id = s.id \
                 WHEN MATCHED AND s.label = 'C' THEN DELETE \
                 WHEN MATCHED THEN UPDATE SET label = s.label \
                 WHEN NOT MATCHED THEN INSERT (id, label) VALUES (s.id, s.label)";

    for door in [&ansi, &spark] {
        door.session
            .sql(
                "CREATE TABLE ice.sales.orders AS \
                 SELECT 1 AS id, 'a' AS label UNION ALL \
                 SELECT 2 AS id, 'b' AS label UNION ALL \
                 SELECT 3 AS id, 'c' AS label",
            )
            .await
            .expect("CTAS");
        door.session
            .sql(merge)
            .await
            .unwrap_or_else(|error| panic!("MERGE failed: {error}"))
            .collect()
            .await
            .expect("MERGE must execute");
    }

    let read = "SELECT id, label FROM ice.sales.orders ORDER BY id";
    let (ansi_fields, ansi_rows) = typed_rows(&ansi.session, read).await;
    let (spark_fields, spark_rows) = typed_rows(&spark.session, read).await;
    assert_eq!(ansi_fields, spark_fields, "post-MERGE schema");
    assert_eq!(ansi_rows, spark_rows, "post-MERGE rows");
    assert_eq!(
        ansi_rows,
        vec![
            (1, "a".to_string()), // untouched
            (2, "B".to_string()), // matched → UPDATE
            (9, "N".to_string()), // not matched → INSERT
                                  // id 3 matched the DELETE clause and is gone
        ],
        "…and the shared MERGE result must be the right one"
    );
}

/// ROW 7 — **TIME TRAVEL**, snapshot pin. Profile: **`TwoSession`**.
///
/// Two independently-written scanners (ANSI's quote-parameterized `FOR VERSION AS OF` over
/// `GenericDialect` vs the Spark door's ported `VERSION AS OF` over `DatabricksDialect`) rewrite
/// to the SAME hoisted repark-core resolution half (`TimeTravelSpec` / `read_table_at` over the
/// fork's snapshot-pinned provider). Pinning the FIRST snapshot must return the pre-INSERT rows
/// through both doors, while the live read returns the post-INSERT rows.
///
/// Each door pins its own catalog's snapshot id (the ids differ — they are per-table), which is
/// the point: the comparison is of RESULTS, not of ids.
#[tokio::test]
async fn cross_door_time_travel_pins_the_same_snapshot_content() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    let mut first_snapshot = Vec::new();
    for door in [&ansi, &spark] {
        door.session
            .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
            .await
            .expect("CTAS");
        let history = door
            .session
            .testing_list_snapshots("ice.sales.orders")
            .await
            .expect("snapshot history");
        first_snapshot.push(history.first().expect("one snapshot after CTAS").0);
        door.session
            .sql("INSERT INTO ice.sales.orders VALUES (2, 'b')")
            .await
            .expect("INSERT")
            .collect()
            .await
            .expect("INSERT executes");
    }

    // Live reads agree (and show both rows).
    let live = "SELECT id, label FROM ice.sales.orders ORDER BY id";
    let (ansi_live_fields, ansi_live) = typed_rows(&ansi.session, live).await;
    let (spark_live_fields, spark_live) = typed_rows(&spark.session, live).await;
    assert_eq!(ansi_live_fields, spark_live_fields, "live schema");
    assert_eq!(ansi_live, spark_live, "live rows");
    assert_eq!(ansi_live.len(), 2, "the INSERT landed");

    // Pinned reads agree — each door in its OWN spelling, against its OWN snapshot id.
    let (ansi_pin_fields, ansi_pin) = typed_rows(
        &ansi.session,
        &format!(
            "SELECT id, label FROM ice.sales.orders FOR VERSION AS OF {} ORDER BY id",
            first_snapshot[0]
        ),
    )
    .await;
    let (spark_pin_fields, spark_pin) = typed_rows(
        &spark.session,
        &format!(
            "SELECT id, label FROM ice.sales.orders VERSION AS OF {} ORDER BY id",
            first_snapshot[1]
        ),
    )
    .await;
    assert_eq!(
        ansi_pin_fields, spark_pin_fields,
        "time-travelled schema must match across doors"
    );
    assert_eq!(
        ansi_pin, spark_pin,
        "time-travelled rows must match across doors"
    );
    assert_eq!(
        ansi_pin,
        vec![(1, "a".to_string())],
        "…and the pinned snapshot must be the PRE-insert one (a live read would give 2 rows)"
    );
}

/// ROW 8 — **identifier case folding**, the divergence DOC row (design §2 Q10 "case rules:
/// stock DF ANSI folding; divergence from Spark documented, one doc-test row per door").
/// Profile: **`TwoSession`**.
///
/// This row exists to make the folding claim a TESTED fact rather than a sentence in a doc, and
/// what it found is worth stating plainly:
///
/// * **Unquoted** identifiers behave the same through both doors — a mixed-case reference
///   resolves to the same column. This is the case that matters for portability, and it agrees.
/// * **Quoted** identifiers ALSO agree — and both doors diverge from real Spark here. Stock
///   DataFusion treats a quoted identifier as case-SENSITIVE, so neither ANSI `"ID"` nor Spark
///   `` `ID` `` resolves against a column stored as `id`; Apache Spark resolves the backticked
///   form case-insensitively (its default `spark.sql.caseSensitive = false` applies to quoted
///   names too). The divergence is therefore **engine-wide, not door-specific**: it is a
///   repark-vs-Spark difference the Spark door inherits, not a place the two doors disagree.
///
/// Recorded rather than fixed: changing it means changing the Spark door's resolution semantics
/// against stock DataFusion, which is a decision, not a bug fix. An untested claim about folding
/// would be worse than none — if either behavior changes, this REDs and the doc line moves with
/// it.
#[tokio::test]
async fn cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;
    for door in [&ansi, &spark] {
        door.session
            .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
            .await
            .expect("CTAS");
    }

    // Unquoted, mixed case: identical through both doors.
    let mixed = "SELECT Id AS id, LaBeL AS label FROM ice.sales.orders";
    let (ansi_fields, ansi_rows) = typed_rows(&ansi.session, mixed).await;
    let (spark_fields, spark_rows) = typed_rows(&spark.session, mixed).await;
    assert_eq!(
        ansi_fields, spark_fields,
        "unquoted identifiers must fold the same way through both doors"
    );
    assert_eq!(ansi_rows, spark_rows);
    assert_eq!(ansi_rows, vec![(1, "a".to_string())]);

    // Quoted, wrong case: both doors refuse. The divergence recorded here is repark-vs-SPARK,
    // not door-vs-door — Apache Spark would resolve the backticked form.
    let ansi_quoted = ansi
        .session
        .sql(r#"SELECT "ID" FROM ice.sales.orders"#)
        .await;
    assert!(
        ansi_quoted.is_err(),
        "ANSI door: a double-quoted identifier is case-SENSITIVE, so \"ID\" must not resolve to \
         a column stored as `id`"
    );
    let spark_quoted = spark.session.sql("SELECT `ID` FROM ice.sales.orders").await;
    assert!(
        spark_quoted.is_err(),
        "Spark door: the backticked form is ALSO case-sensitive today (stock DataFusion \
         resolution) — a divergence from Apache Spark, inherited engine-wide rather than \
         introduced by either door. If this ever starts resolving, update the doc line."
    );
}
