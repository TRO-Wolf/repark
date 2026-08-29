//! Cross-door equivalence uses two sessions so each door keeps its own session extensions.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
};
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

/// **Session B** uses `SparkExtension` at both build hooks and the Spark dialect.
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

/// Run `sql` and return `(field name, Arrow type)` pairs plus the rows.
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

/// ROW 1 — CTAS preserves content and schema across native and Spark-extended sessions.
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

/// ROW 2 — INSERT preserves content and schema across the two doors.
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

/// ROW 3 — session extensions are shared by a session, not scoped to a dialect.
/// Mutation: if extensions ever became dialect-scoped, this test REDs, and every cross-door row
/// above would need re-reading.
#[tokio::test]
async fn extensions_are_session_scoped_not_dialect_scoped() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;

    ansi.session
        .sql("SELECT date_add(DATE '2024-01-01', CAST(1 AS INT)) AS d")
        .await
        .expect_err("a native session must not know the Spark function set");

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

/// ROW 4 — **pure catalog DDL**, the one shape where a single session is legal (design §2 Q13).
#[tokio::test]
async fn cross_door_namespace_ddl_is_single_session_legal() {
    let ansi = native_ansi_door().await;
    let warehouse = &ansi.warehouse;

    ansi.session
        .sql(&format!(
            "CREATE SCHEMA ice.a WITH (location = '{warehouse}/a')"
        ))
        .await
        .expect("ANSI CREATE SCHEMA");
    let spark_dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ansi.session
        .sql_with(
            &spark_dialect,
            &format!("CREATE NAMESPACE ice.b LOCATION '{warehouse}/b'"),
        )
        .await
        .expect("Spark CREATE NAMESPACE through the same session");

    // Both namespaces use one catalog; the doors differ only in SQL spelling.
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

/// Return the full read schema of `ice.sales.orders` as `(name, Arrow type)` pairs.
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

/// ROW 5 — ALTER preserves the evolved schema across both sessions.
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

    // TABLE rename (`ALTER TABLE … RENAME TO`) uses the same spelling in both doors.
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
            (1, "a".to_string()),
            (2, "B".to_string()),
            (9, "N".to_string()),
        ],
        "…and the shared MERGE result must be the right one"
    );
}

/// ROW 7 — **TIME TRAVEL**, snapshot pin. Profile: **`TwoSession`**.
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

/// ROW 8 — **identifier case folding**, the divergence row for design §2 Q10.
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

    // Quoted, wrong case: both doors refuse; the registry records the repark-vs-Spark result.
    let ansi_quoted = match ansi
        .session
        .sql(r#"SELECT "ID" FROM ice.sales.orders"#)
        .await
    {
        Err(error) => error.to_string(),
        Ok(_) => panic!(
            "ANSI door: a double-quoted identifier is case-SENSITIVE, so \"ID\" must not resolve \
             to a column stored as `id`. If it now resolves, repark has CONVERGED on Apache \
             Spark and docs/spark-sql-iceberg-parity.md §3 row ID-1 must be retired, not this \
             assertion relaxed."
        ),
    };
    assert!(
        ansi_quoted.contains("No field named") && ansi_quoted.contains("\"ID\""),
        "the ANSI refusal must be a resolution failure naming the unresolved identifier as \
         `\"ID\"` (row ID-1): {ansi_quoted}"
    );
    let spark_quoted = match spark.session.sql("SELECT `ID` FROM ice.sales.orders").await {
        Err(error) => error.to_string(),
        Ok(_) => panic!(
            "Spark door: the backticked form is ALSO case-sensitive today (stock DataFusion \
             resolution) — a divergence from Apache Spark, inherited engine-wide rather than \
             introduced by either door. If this ever starts resolving, retire \
             docs/spark-sql-iceberg-parity.md §3 row ID-1 in the same change."
        ),
    };
    assert!(
        spark_quoted.contains("No field named") && spark_quoted.contains("\"ID\""),
        "the Spark-door refusal must be a resolution failure naming the unresolved identifier as \
         `\"ID\"` (row ID-1): {spark_quoted}"
    );
}

/// One-column Decimal128 result through a session: `(precision, scale, nullable, i128_or_null)`.
async fn decimal128_scalar(session: &ReparkSession, sql: &str) -> (u8, i8, bool, Option<i128>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let field = schema.field(0);
    let nullable = field.is_nullable();
    let (precision, scale) = match field.data_type() {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => panic!("expected Decimal128 for `{sql}`, got {other:?}"),
    };
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "`{sql}` must yield one row"
    );
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Decimal128Array");
    let value = if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    };
    (precision, scale, nullable, value)
}

/// G-7b cross-door row 1 — money add. Corpus row `add_same_precision_scale`.
#[tokio::test]
async fn cross_door_decimal_add_same_precision_scale_bit_exact() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2)) AS v";

    let ansi_pin = decimal128_scalar(&ansi.session, sql).await;
    let spark_pin = decimal128_scalar(&spark.session, sql).await;

    assert_eq!(
        ansi_pin, spark_pin,
        "decimal add must agree across doors (schema + nullability + i128)"
    );
    assert_eq!(
        ansi_pin,
        (11, 2, false, Some(579)),
        "shared result must match corpus row add_same_precision_scale"
    );
}

/// G-7b cross-door row 2 — money × quantity. Corpus row `mul_money_by_quantity`.
#[tokio::test]
async fn cross_door_decimal_mul_money_by_quantity_bit_exact() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CAST(19.99 AS DECIMAL(10,2)) * CAST(3 AS DECIMAL(10,0)) AS v";

    let ansi_pin = decimal128_scalar(&ansi.session, sql).await;
    let spark_pin = decimal128_scalar(&spark.session, sql).await;

    assert_eq!(
        ansi_pin, spark_pin,
        "decimal mul must agree across doors (schema + nullability + i128)"
    );
    assert_eq!(
        ansi_pin,
        (21, 2, false, Some(5997)),
        "shared result must match corpus row mul_money_by_quantity"
    );
}

/// ROW 9 — both doors render the G3-E8 residual refusal byte for byte.
/// Mutation: change one door's message (or one door's target derivation) → this row reds while
/// both doors' own message pins stay green.
#[tokio::test]
async fn cross_door_g3e8_refusals_render_identically() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
            .await
            .expect("target CTAS");
        door.session
            .sql("CREATE TABLE ice.sales.keys AS SELECT 1 AS id")
            .await
            .expect("keys CTAS");
    }

    for sql in [
        // ROW 9 — permanent v1 valve (IN / NOT IN / EXISTS / correlated IN / UPDATE IN execute).
        "DELETE FROM ice.sales.orders WHERE id IN (SELECT id FROM (SELECT id FROM ice.sales.keys) x)",
        "DELETE FROM ice.sales.orders WHERE id = (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.orders WHERE id > 1 AND id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.orders WHERE id = ANY (SELECT id FROM ice.sales.keys)",
        "UPDATE ice.sales.orders SET label = 'z' WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        // The target rendering, quoted — the half a template-only pin cannot see.
        "DELETE FROM \"ice\".\"sales\".\"orders\" WHERE id = (SELECT max(id) FROM ice.sales.keys)",
    ] {
        let ansi_refusal = match ansi.session.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("ANSI door must refuse: {sql}"),
        };
        let spark_refusal = match spark.session.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("Spark door must refuse: {sql}"),
        };
        assert_eq!(
            ansi_refusal, spark_refusal,
            "the two doors' G3-E8 refusals must be byte-identical, sql={sql:?}"
        );
        assert!(
            ansi_refusal.contains("subquery predicates are silently mis-executed")
                && ansi_refusal.contains("G3-E8"),
            "…and that shared string must be the G3-E8 refusal (an equal-but-wrong pair would \
             otherwise pass the assertion above): {ansi_refusal}"
        );
    }
}

/// ROW 9 executed column — uncorrelated `DELETE … NOT IN` on both doors, same remaining row-set.
#[tokio::test]
async fn cross_door_g3e8_not_in_delete_executes_identically() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql(
                "CREATE TABLE ice.sales.tgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
            )
            .await
            .expect("target CTAS");
        door.session
            .sql("CREATE TABLE ice.sales.keys AS SELECT 2 AS id")
            .await
            .expect("keys CTAS");
        door.session
            .sql("DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)")
            .await
            .unwrap_or_else(|error| panic!("NOT IN DELETE must execute: {error}"));
    }

    let mut remaining = Vec::new();
    for door in [&ansi, &spark] {
        let frame = door
            .session
            .sql("SELECT id FROM ice.sales.tgt ORDER BY id")
            .await
            .expect("read back");
        let batches = frame.collect().await.expect("collect");
        let mut ids = Vec::new();
        for batch in &batches {
            if let Some(column) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for row in 0..batch.num_rows() {
                    ids.push(column.value(row));
                }
            } else if let Some(column) = batch.column(0).as_any().downcast_ref::<Int32Array>() {
                for row in 0..batch.num_rows() {
                    ids.push(i64::from(column.value(row)));
                }
            } else {
                panic!("expected Int32/Int64 id column, got {:?}", batch.schema());
            }
        }
        remaining.push(ids);
    }
    assert_eq!(
        remaining[0], remaining[1],
        "both doors must keep the same ids"
    );
    assert_eq!(remaining[0], vec![2], "NOT IN keeps only the key row");
}

/// ROW 9 executed column — `[NOT] EXISTS` on both doors, same remaining row-set.
#[tokio::test]
async fn cross_door_g3e8_exists_delete_executes_identically() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql(
                "CREATE TABLE ice.sales.tgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
            )
            .await
            .expect("target CTAS");
        door.session
            .sql("CREATE TABLE ice.sales.keys AS SELECT 2 AS id")
            .await
            .expect("keys CTAS");
        door.session
            .sql(
                "DELETE FROM ice.sales.tgt WHERE EXISTS \
                 (SELECT 1 FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
            )
            .await
            .unwrap_or_else(|error| panic!("EXISTS DELETE must execute: {error}"));
    }

    let mut remaining = Vec::new();
    for door in [&ansi, &spark] {
        let frame = door
            .session
            .sql("SELECT id FROM ice.sales.tgt ORDER BY id")
            .await
            .expect("read back");
        let batches = frame.collect().await.expect("collect");
        let mut ids = Vec::new();
        for batch in &batches {
            if let Some(column) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for row in 0..batch.num_rows() {
                    ids.push(column.value(row));
                }
            } else if let Some(column) = batch.column(0).as_any().downcast_ref::<Int32Array>() {
                for row in 0..batch.num_rows() {
                    ids.push(i64::from(column.value(row)));
                }
            } else {
                panic!("expected Int32/Int64 id column, got {:?}", batch.schema());
            }
        }
        remaining.push(ids);
    }
    assert_eq!(
        remaining[0], remaining[1],
        "both doors must keep the same ids after correlated EXISTS"
    );
    assert_eq!(
        remaining[0],
        vec![1, 3],
        "correlated EXISTS deletes the key row"
    );
}

/// ROW 9 executed column — correlated `DELETE … IN` on both doors, same remaining row-set.
#[tokio::test]
async fn cross_door_g3e8_correlated_in_delete_executes_identically() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql(
                "CREATE TABLE ice.sales.tgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
            )
            .await
            .expect("target CTAS");
        door.session
            .sql("CREATE TABLE ice.sales.keys AS SELECT 2 AS id")
            .await
            .expect("keys CTAS");
        door.session
            .sql(
                "DELETE FROM ice.sales.tgt WHERE id IN \
                 (SELECT k.id FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
            )
            .await
            .unwrap_or_else(|error| panic!("correlated IN DELETE must execute: {error}"));
    }

    let mut remaining = Vec::new();
    for door in [&ansi, &spark] {
        let frame = door
            .session
            .sql("SELECT id FROM ice.sales.tgt ORDER BY id")
            .await
            .expect("read back");
        let batches = frame.collect().await.expect("collect");
        let mut ids = Vec::new();
        for batch in &batches {
            if let Some(column) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for row in 0..batch.num_rows() {
                    ids.push(column.value(row));
                }
            } else if let Some(column) = batch.column(0).as_any().downcast_ref::<Int32Array>() {
                for row in 0..batch.num_rows() {
                    ids.push(i64::from(column.value(row)));
                }
            } else {
                panic!("expected Int32/Int64 id column, got {:?}", batch.schema());
            }
        }
        remaining.push(ids);
    }
    assert_eq!(
        remaining[0], remaining[1],
        "both doors must keep the same ids after correlated IN"
    );
    assert_eq!(
        remaining[0],
        vec![1, 3],
        "correlated IN deletes the key row"
    );
}

/// ROW 9 executed column — identity `UPDATE … IN` on both doors, same remaining row-set.
#[tokio::test]
async fn cross_door_g3e8_update_in_executes_identically() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_namespace(&ansi, false).await;
    make_namespace(&spark, true).await;

    for door in [&ansi, &spark] {
        door.session
            .sql(
                "CREATE TABLE ice.sales.tgt AS SELECT 1 AS id, 'a' AS name UNION ALL \
                 SELECT 2, 'b' UNION ALL SELECT 3, 'c'",
            )
            .await
            .expect("target CTAS");
        door.session
            .sql("CREATE TABLE ice.sales.keys AS SELECT 2 AS id")
            .await
            .expect("keys CTAS");
        door.session
            .sql("UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM ice.sales.keys)")
            .await
            .unwrap_or_else(|error| panic!("UPDATE IN must execute: {error}"));
    }

    let mut remaining = Vec::new();
    for door in [&ansi, &spark] {
        let frame = door
            .session
            .sql("SELECT id, name FROM ice.sales.tgt ORDER BY id")
            .await
            .expect("read back");
        let batches = frame.collect().await.expect("collect");
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>();
            let ids32 = batch.column(0).as_any().downcast_ref::<Int32Array>();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("name Utf8");
            for row in 0..batch.num_rows() {
                let id = if let Some(column) = ids {
                    column.value(row)
                } else if let Some(column) = ids32 {
                    i64::from(column.value(row))
                } else {
                    panic!("expected Int32/Int64 id, got {:?}", batch.schema());
                };
                rows.push((id, names.value(row).to_string()));
            }
        }
        remaining.push(rows);
    }
    assert_eq!(
        remaining[0], remaining[1],
        "both doors must keep the same (id, name) after UPDATE IN"
    );
    assert_eq!(
        remaining[0],
        vec![(1, "a".into()), (2, "z".into()), (3, "c".into())],
        "UPDATE IN rewrites only the key row"
    );
}

/// One-column Boolean result: `(DataType, nullable, Option<bool>)`.
async fn boolean_scalar(session: &ReparkSession, sql: &str) -> (DataType, bool, Option<bool>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let field = schema.field(0);
    let data_type = field.data_type().clone();
    let nullable = field.is_nullable();
    assert_eq!(
        data_type,
        DataType::Boolean,
        "expected Boolean for `{sql}`, got {data_type:?}"
    );
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "`{sql}` must yield one row"
    );
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("BooleanArray");
    let value = if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    };
    (data_type, nullable, value)
}

/// One-column Int32 result: `(DataType, nullable, Option<i32>)`.
async fn int32_scalar(session: &ReparkSession, sql: &str) -> (DataType, bool, Option<i32>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let field = schema.field(0);
    let data_type = field.data_type().clone();
    let nullable = field.is_nullable();
    assert_eq!(
        data_type,
        DataType::Int32,
        "expected Int32 for `{sql}`, got {data_type:?}"
    );
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "`{sql}` must yield one row"
    );
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32Array");
    let value = if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    };
    (data_type, nullable, value)
}

/// G12 cross-door row 1 — TRUE AND NULL → NULL. Corpus row `and_true_null_is_null`.
#[tokio::test]
async fn cross_door_tvl_true_and_null_is_null() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT (TRUE AND CAST(NULL AS BOOLEAN)) AS v";

    let ansi_pin = boolean_scalar(&ansi.session, sql).await;
    let spark_pin = boolean_scalar(&spark.session, sql).await;

    assert_eq!(
        ansi_pin, spark_pin,
        "TRUE AND NULL must agree across doors (type + nullability + value)"
    );
    assert_eq!(
        ansi_pin,
        (DataType::Boolean, true, None),
        "shared result must match corpus row and_true_null_is_null"
    );
}

/// CASE WHEN falls through when its first predicate is null.
#[tokio::test]
async fn cross_door_tvl_case_when_null_predicate() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CASE \
                 WHEN CAST(NULL AS BOOLEAN) THEN CAST(1 AS INT) \
                 WHEN TRUE THEN CAST(2 AS INT) \
                 ELSE CAST(3 AS INT) \
               END AS v";

    let ansi_pin = int32_scalar(&ansi.session, sql).await;
    let spark_pin = int32_scalar(&spark.session, sql).await;

    assert_eq!(
        ansi_pin, spark_pin,
        "CASE WHEN null-predicate must agree across doors (type + nullability + value)"
    );
    assert_eq!(
        ansi_pin,
        (DataType::Int32, false, Some(2)),
        "shared result must match corpus row case_when_null_predicate"
    );
}

/// Plan- or collect-time error text. Panics if `{sql}` succeeds through `{session}`.
async fn collect_error(session: &ReparkSession, sql: &str) -> String {
    match session.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected `{sql}` to fail, but it produced rows"),
        },
    }
}

/// One-column Float64 result: `(DataType, nullable, Option<f64>)`.
async fn float64_scalar(session: &ReparkSession, sql: &str) -> (DataType, bool, Option<f64>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let field = schema.field(0);
    let data_type = field.data_type().clone();
    let nullable = field.is_nullable();
    assert_eq!(
        data_type,
        DataType::Float64,
        "expected Float64 for `{sql}`, got {data_type:?}"
    );
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "`{sql}` must yield one row"
    );
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    let value = if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    };
    (data_type, nullable, value)
}

/// Seed `ice.sales.nums(n INT)` with `{1, NULL, 2}` — the G11 ordering / aggregate fixture.
async fn make_nullable_ints(door: &Door, spark_spelling: bool) {
    make_namespace(door, spark_spelling).await;
    door.session
        .sql(
            "CREATE TABLE ice.sales.nums AS \
             SELECT CAST(1 AS INT) AS n UNION ALL \
             SELECT CAST(NULL AS INT) AS n UNION ALL \
             SELECT CAST(2 AS INT) AS n",
        )
        .await
        .expect("nums CTAS");
}

/// One-column Int32 result set in **statement order** (not sorted).
async fn ordered_int32(session: &ReparkSession, sql: &str) -> (DataType, bool, Vec<Option<i32>>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let field = schema.field(0);
    let data_type = field.data_type().clone();
    let nullable = field.is_nullable();
    assert_eq!(
        data_type,
        DataType::Int32,
        "expected Int32 for `{sql}`, got {data_type:?}"
    );
    let batches = frame.collect().await.expect("collect");
    let mut values = Vec::new();
    for batch in &batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        for row in 0..batch.num_rows() {
            if array.is_null(row) {
                values.push(None);
            } else {
                values.push(Some(array.value(row)));
            }
        }
    }
    (data_type, nullable, values)
}

/// G11 cross-door row 1 — integer `/`.
#[tokio::test]
async fn cross_door_integer_division_truncates_on_ansi_is_float_on_spark() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CAST(5 AS INT) / CAST(2 AS INT) AS v";

    let ansi_pin = int32_scalar(&ansi.session, sql).await;
    let spark_pin = float64_scalar(&spark.session, sql).await;

    assert_eq!(
        ansi_pin,
        (DataType::Int32, false, Some(2)),
        "ANSI door: integer `/` stays Int32 and truncates toward zero"
    );
    assert_eq!(
        spark_pin,
        (DataType::Float64, true, Some(2.5)),
        "Spark door: `/` promotes integers to nullable Float64"
    );
}

/// G11 cross-door row 2 — integer `/ 0`.
#[tokio::test]
async fn cross_door_integer_div_by_zero_raises_on_ansi_null_on_spark() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CAST(1 AS INT) / CAST(0 AS INT) AS v";

    let ansi_error = collect_error(&ansi.session, sql).await;
    assert!(
        ansi_error.contains("Divide by zero"),
        "ANSI door must raise on integer `/ 0`, got: {ansi_error}"
    );

    let spark_error = collect_error(&spark.session, sql).await;
    assert!(
        spark_error.contains("DIVIDE_BY_ZERO"),
        "Spark door (ANSI ON) must raise DIVIDE_BY_ZERO, got: {spark_error}"
    );
}

/// G11 cross-door row 3 — float `/ 0`.
#[tokio::test]
async fn cross_door_float_div_by_zero_is_infinity_on_ansi_null_on_spark() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CAST(1.0 AS DOUBLE) / CAST(0.0 AS DOUBLE) AS v";

    let (ansi_type, ansi_nullable, ansi_value) = float64_scalar(&ansi.session, sql).await;
    assert_eq!(ansi_type, DataType::Float64);
    assert!(
        !ansi_nullable,
        "ANSI IEEE `/ 0` is a non-null Infinity, not a SQL NULL"
    );
    let ansi_float = ansi_value.expect("ANSI float `/ 0` must not be SQL NULL");
    assert!(
        ansi_float.is_infinite() && ansi_float.is_sign_positive(),
        "ANSI door must yield +Infinity, got {ansi_float}"
    );

    let spark_error = collect_error(&spark.session, sql).await;
    assert!(
        spark_error.contains("DIVIDE_BY_ZERO"),
        "Spark door (ANSI ON) must raise DIVIDE_BY_ZERO on float /0, got: {spark_error}"
    );
}

/// G11 cross-door row 4 — decimal `/ 0`.
#[tokio::test]
async fn cross_door_decimal_div_by_zero_raises_on_ansi_null_on_spark() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    let sql = "SELECT CAST(1 AS DECIMAL(10,0)) / CAST(0 AS DECIMAL(10,0)) AS v";

    let ansi_error = collect_error(&ansi.session, sql).await;
    assert!(
        ansi_error.contains("Divide by zero"),
        "ANSI door must raise on decimal `/ 0`, got: {ansi_error}"
    );

    let spark_error = collect_error(&spark.session, sql).await;
    assert!(
        spark_error.contains("DIVIDE_BY_ZERO"),
        "Spark door (ANSI ON) must raise DIVIDE_BY_ZERO on decimal /0, got: {spark_error}"
    );
}

/// G11 cross-door row 5 — default `ORDER BY … ASC` null placement.
#[tokio::test]
async fn cross_door_order_by_asc_default_nulls_last_on_ansi_first_on_spark() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_nullable_ints(&ansi, false).await;
    make_nullable_ints(&spark, true).await;
    let sql = "SELECT n FROM ice.sales.nums ORDER BY n ASC";

    let ansi_pin = ordered_int32(&ansi.session, sql).await;
    let spark_pin = ordered_int32(&spark.session, sql).await;

    assert_eq!(
        ansi_pin,
        (DataType::Int32, true, vec![Some(1), Some(2), None]),
        "ANSI door: ASC defaults to NULLS LAST"
    );
    assert_eq!(
        spark_pin,
        (DataType::Int32, true, vec![None, Some(1), Some(2)]),
        "Spark door: ASC defaults to NULLS FIRST"
    );
}

/// G11 cross-door row 6 — default `ORDER BY … DESC` null placement.
#[tokio::test]
async fn cross_door_order_by_desc_default_nulls_first_on_ansi_last_on_spark() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door().await;
    make_nullable_ints(&ansi, false).await;
    make_nullable_ints(&spark, true).await;
    let sql = "SELECT n FROM ice.sales.nums ORDER BY n DESC";

    let ansi_pin = ordered_int32(&ansi.session, sql).await;
    let spark_pin = ordered_int32(&spark.session, sql).await;

    assert_eq!(
        ansi_pin,
        (DataType::Int32, true, vec![None, Some(2), Some(1)]),
        "ANSI door: DESC defaults to NULLS FIRST"
    );
    assert_eq!(
        spark_pin,
        (DataType::Int32, true, vec![Some(2), Some(1), None]),
        "Spark door: DESC defaults to NULLS LAST"
    );
}
