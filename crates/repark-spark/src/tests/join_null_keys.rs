//! Pins Spark SQL three-valued JOIN behavior for NULL keys on the Arrow path.
//!
//! `NULL = NULL` is unknown, so NULL keys do not match. Values, types, and nullability are
//! checked against the Spark 4.1.2 corpus.

use super::super::*;
use super::common::*;

use datafusion::arrow::array::{Array, StringArray, StringViewArray};

/// Left relation used by every pin: `{(1,'a'), (NULL,'n')}`.
const LEFT_SQL: &str = "\
(SELECT CAST(1 AS BIGINT) AS k, 'a' AS a \
 UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l";

/// Right mixed: `{(1,'x'), (NULL,'y')}` — INNER / LEFT.
const RIGHT_BOTH_SQL: &str = "\
(SELECT CAST(1 AS BIGINT) AS k, 'x' AS b \
 UNION ALL SELECT CAST(NULL AS BIGINT), 'y') r";

/// Right NULL-only — SEMI / ANTI so emptiness is 3VL, not "no partner".
const RIGHT_NULL_SQL: &str = "(SELECT CAST(NULL AS BIGINT) AS k) r";

/// Four-column join projection `(lk, a, rk, b)`.
async fn collect_join_quad(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(Option<i64>, String, Option<i64>, Option<String>)> {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute failed for `{sql}`: {error}"));
    let schema = frame.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "lk `{sql}`");
    assert_string_type(schema.field(1).data_type(), sql);
    assert_eq!(schema.field(2).data_type(), &DataType::Int64, "rk `{sql}`");
    assert_string_type(schema.field(3).data_type(), sql);
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect failed for `{sql}`: {error}"));
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

/// Two-column projection `(k, a)` for SEMI / ANTI.
async fn collect_key_label(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (bool, Vec<(Option<i64>, String)>) {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute failed for `{sql}`: {error}"));
    let schema = frame.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "k `{sql}`");
    assert_string_type(schema.field(1).data_type(), sql);
    let key_nullable = schema.field(0).is_nullable();
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect failed for `{sql}`: {error}"));
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = downcast_i64(batch, 0, sql);
        let labels = batch.column(1).as_ref();
        for row in 0..batch.num_rows() {
            rows.push((
                optional_i64(keys, row),
                string_at(labels, row).expect("label is a non-null literal"),
            ));
        }
    }
    (key_nullable, rows)
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

/// ===========================================================================================
/// Spark-door NULL join keys never match on INNER / LEFT / SEMI / ANTI.
///
/// Goldens: live Spark 4.1.2 (G4 corpus + this unit's lock re-verify). `ORDER BY
/// … NULLS LAST` pins row order so the assertion is not a multiset compare.
/// ===========================================================================================
#[tokio::test]
async fn spark_door_null_keys_never_match_inner_left_semi_anti() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;

    let inner = collect_join_quad(
        &ctx,
        &catalogs,
        &format!(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b \
             FROM {LEFT_SQL} INNER JOIN {RIGHT_BOTH_SQL} ON l.k = r.k \
             ORDER BY lk NULLS LAST, a"
        ),
    )
    .await;
    assert_eq!(
        inner,
        vec![(Some(1), "a".into(), Some(1), Some("x".into()))],
        "INNER: NULL = NULL is unknown; only the non-null key pair survives (G4 \
         null_keys_inner_no_match)"
    );

    let left = collect_join_quad(
        &ctx,
        &catalogs,
        &format!(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b \
             FROM {LEFT_SQL} LEFT JOIN {RIGHT_BOTH_SQL} ON l.k = r.k \
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
        "LEFT: NULL-key left row is an unmatched orphan (G4 null_keys_left_outer_fate)"
    );

    let (semi_key_nullable, semi) = collect_key_label(
        &ctx,
        &catalogs,
        &format!(
            "SELECT l.k, l.a FROM {LEFT_SQL} \
             LEFT SEMI JOIN {RIGHT_NULL_SQL} ON l.k = r.k \
             ORDER BY k NULLS LAST, a"
        ),
    )
    .await;
    assert!(semi_key_nullable, "SEMI key stays nullable");
    assert_eq!(
        semi,
        Vec::<(Option<i64>, String)>::new(),
        "LEFT SEMI vs a NULL-only right is empty (G4 left_semi_null_keys_no_match)"
    );

    let (_anti_key_nullable, anti) = collect_key_label(
        &ctx,
        &catalogs,
        &format!(
            "SELECT l.k, l.a FROM {LEFT_SQL} \
             LEFT ANTI JOIN {RIGHT_NULL_SQL} ON l.k = r.k \
             ORDER BY k NULLS LAST, a"
        ),
    )
    .await;
    assert_eq!(
        anti,
        vec![(Some(1), "a".into()), (None, "n".into())],
        "LEFT ANTI keeps both left rows because NULL never matches \
         (G4 df_left_anti_null_keys_keeps_row)"
    );
}
