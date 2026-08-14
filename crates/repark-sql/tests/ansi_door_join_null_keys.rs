//! G8 / R-3 — ANSI-door JOIN NULL-key pins (G11: correctness, not Spark parity).
//!
//! SQL three-valued logic: `NULL = NULL` is unknown, so a NULL join key never
//! matches. This file pins that class on a **native** `AnsiDialect` session
//! (no `SessionExtension`) for INNER, LEFT, SEMI, and ANTI. Spark 4.1.2 agrees
//! on the same 3VL table (G4 corpus); that agreement is documented in the row
//! comments and is **not** a reason to retarget this door at Spark.
//!
//! SEMI / ANTI use DataFusion Generic `LEFT SEMI JOIN` / `LEFT ANTI JOIN`
//! (the door accepts those keywords). `EXISTS` / `NOT EXISTS` are the
//! standard-SQL spellings of the same class and were probed equivalent.
//!
//! Arrow path (`collect`), value AND type AND nullability. AWS-free.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;

/// Native ANSI session — no catalog, no extension.
fn native_ansi_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session")
}

/// One-column-pair projection `(k, a)` as `(Option<i64>, String)`.
async fn collect_key_label(
    session: &ReparkSession,
    sql: &str,
) -> (DataType, bool, DataType, bool, Vec<(Option<i64>, String)>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let key_type = schema.field(0).data_type().clone();
    let key_nullable = schema.field(0).is_nullable();
    let label_type = schema.field(1).data_type().clone();
    let label_nullable = schema.field(1).is_nullable();
    assert_eq!(key_type, DataType::Int64, "key type for `{sql}`");
    assert_string_type(&label_type, sql);
    let batches = frame.collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        append_key_label(batch, &mut rows);
    }
    (key_type, key_nullable, label_type, label_nullable, rows)
}

/// Four-column join projection `(lk, a, rk, b)`.
async fn collect_join_quad(
    session: &ReparkSession,
    sql: &str,
) -> Vec<(Option<i64>, String, Option<i64>, Option<String>)> {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "lk `{sql}`");
    assert_string_type(schema.field(1).data_type(), sql);
    assert_eq!(schema.field(2).data_type(), &DataType::Int64, "rk `{sql}`");
    assert_string_type(schema.field(3).data_type(), sql);
    let batches = frame.collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let left_keys = downcast_i64(batch, 0, sql);
        let labels = batch.column(1).as_ref();
        let right_keys = downcast_i64(batch, 2, sql);
        let payloads = batch.column(3).as_ref();
        for row in 0..batch.num_rows() {
            rows.push((
                optional_i64(left_keys, row),
                string_at(labels, row).expect("join label is a non-null literal"),
                optional_i64(right_keys, row),
                string_at(payloads, row),
            ));
        }
    }
    rows
}

fn append_key_label(batch: &RecordBatch, rows: &mut Vec<(Option<i64>, String)>) {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 key");
    let labels = batch.column(1).as_ref();
    for row in 0..batch.num_rows() {
        rows.push((
            optional_i64(keys, row),
            string_at(labels, row).expect("label is a non-null literal"),
        ));
    }
}

fn downcast_i64<'a>(batch: &'a RecordBatch, column: usize, sql: &str) -> &'a Int64Array {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column {column} of `{sql}` is not Int64"))
}

fn assert_string_type(data_type: &DataType, sql: &str) {
    assert!(
        matches!(
            data_type,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        ),
        "string column of `{sql}` must be Utf8/Utf8View/LargeUtf8, got {data_type:?}"
    );
}

fn optional_i64(array: &Int64Array, row: usize) -> Option<i64> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn string_at(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
        return Some(utf8.value(row).to_string());
    }
    if let Some(view) = array.as_any().downcast_ref::<StringViewArray>() {
        return Some(view.value(row).to_string());
    }
    panic!(
        "string cell is not Utf8/Utf8View (got {:?})",
        array.data_type()
    );
}

const LEFT_RIGHT_BOTH_NULL: &str = "\
FROM (SELECT CAST(1 AS BIGINT) AS k, CAST('a' AS VARCHAR) AS a \
      UNION ALL SELECT CAST(NULL AS BIGINT), CAST('n' AS VARCHAR)) l ";

const RIGHT_BOTH: &str = "\
(SELECT CAST(1 AS BIGINT) AS k, CAST('x' AS VARCHAR) AS b \
 UNION ALL SELECT CAST(NULL AS BIGINT), CAST('y' AS VARCHAR)) r ";

const RIGHT_NULL_ONLY: &str = "(SELECT CAST(NULL AS BIGINT) AS k) r ";

/// ===========================================================================================
/// NULL join keys never match on INNER / LEFT / SEMI / ANTI (native ANSI door).
///
/// Fixture: left `{(1,'a'), (NULL,'n')}`; right mixed `{(1,'x'), (NULL,'y')}` for
/// INNER/LEFT; right `{NULL}` for SEMI/ANTI so emptiness cannot be blamed on a
/// missing non-null partner. Spark 4.1.2 produces the same 3VL table (G4
/// `null_keys_inner_no_match` / `null_keys_left_outer_fate` /
/// `left_semi_null_keys_no_match` / `df_left_anti_null_keys_keeps_row`).
/// ===========================================================================================
#[tokio::test]
async fn ansi_door_null_keys_never_match_inner_left_semi_anti() {
    let session = native_ansi_session();

    let inner = collect_join_quad(
        &session,
        &format!(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b {LEFT_RIGHT_BOTH_NULL} \
             INNER JOIN {RIGHT_BOTH} ON l.k = r.k \
             ORDER BY lk NULLS LAST, a"
        ),
    )
    .await;
    assert_eq!(
        inner,
        vec![(Some(1), "a".into(), Some(1), Some("x".into()))],
        "INNER: NULL = NULL is unknown, so only the non-null key pair survives"
    );

    let left = collect_join_quad(
        &session,
        &format!(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b {LEFT_RIGHT_BOTH_NULL} \
             LEFT JOIN {RIGHT_BOTH} ON l.k = r.k \
             ORDER BY lk NULLS LAST, a"
        ),
    )
    .await;
    assert_eq!(
        left,
        vec![
            (Some(1), "a".into(), Some(1), Some("x".into())),
            (None, "n".into(), None, None),
        ],
        "LEFT: the NULL-key left row is an unmatched orphan (right payload NULL)"
    );

    let (_key_type, key_nullable, _label_type, label_nullable, semi) = collect_key_label(
        &session,
        &format!(
            "SELECT l.k, l.a {LEFT_RIGHT_BOTH_NULL} \
             LEFT SEMI JOIN {RIGHT_NULL_ONLY} ON l.k = r.k \
             ORDER BY k NULLS LAST, a"
        ),
    )
    .await;
    assert!(key_nullable, "SEMI key stays nullable");
    assert!(!label_nullable, "SEMI label is a non-null literal");
    assert_eq!(
        semi,
        Vec::<(Option<i64>, String)>::new(),
        "SEMI vs a NULL-only right: no match, empty result"
    );

    let (_key_type, _key_nullable, _label_type, _label_nullable, anti) = collect_key_label(
        &session,
        &format!(
            "SELECT l.k, l.a {LEFT_RIGHT_BOTH_NULL} \
             LEFT ANTI JOIN {RIGHT_NULL_ONLY} ON l.k = r.k \
             ORDER BY k NULLS LAST, a"
        ),
    )
    .await;
    assert_eq!(
        anti,
        vec![(Some(1), "a".into()), (None, "n".into())],
        "ANTI: because NULL never matches, BOTH left rows (including the NULL key) are kept"
    );
}
