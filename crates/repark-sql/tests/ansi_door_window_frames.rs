//! Native-profile ROWS/RANGE frame-value pins.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
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

/// Seed the numeric window relation with `(id, k, v)` columns.
const WIN: &str = "\
FROM ( \
  SELECT CAST(1 AS INT) AS id, CAST(1 AS INT) AS k, CAST(10 AS INT) AS v UNION ALL \
  SELECT CAST(2 AS INT), CAST(1 AS INT), CAST(20 AS INT) UNION ALL \
  SELECT CAST(3 AS INT), CAST(2 AS INT), CAST(30 AS INT) UNION ALL \
  SELECT CAST(4 AS INT), CAST(1 AS INT), CAST(40 AS INT) UNION ALL \
  SELECT CAST(5 AS INT), CAST(3 AS INT), CAST(50 AS INT) \
) win";

/// DATE seed: 2024-01-01 / 01-02 / 01-04 with values 10 / 20 / 30.
const WIN_DATE: &str = "\
FROM ( \
  SELECT CAST(1 AS INT) AS id, DATE '2024-01-01' AS d, CAST(10 AS INT) AS v UNION ALL \
  SELECT CAST(2 AS INT), DATE '2024-01-02', CAST(20 AS INT) UNION ALL \
  SELECT CAST(3 AS INT), DATE '2024-01-04', CAST(30 AS INT) \
) wd";

/// Measured `sum` column (Int64, nullable) ordered by `id`.
async fn collect_sum(session: &ReparkSession, sql: &str) -> Vec<Option<i64>> {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int32,
        "id type for `{sql}`"
    );
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Int64,
        "sum type for `{sql}`"
    );
    assert!(schema.field(1).is_nullable(), "window sum is nullable");
    let batches = frame.collect().await.expect("collect");
    let mut values = Vec::new();
    for batch in &batches {
        let array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 sum");
        for row in 0..batch.num_rows() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row))
            });
        }
    }
    values
}

fn present(values: &[i64]) -> Vec<Option<i64>> {
    values.iter().copied().map(Some).collect()
}

/// Native-profile ROWS / RANGE frame values (numeric + DATE unit-less).
#[tokio::test]
async fn ansi_door_rows_and_range_frame_values() {
    let session = native_ansi_session();

    let default_frame = collect_sum(
        &session,
        &format!("SELECT id, sum(v) OVER (ORDER BY k) AS s {WIN} ORDER BY id"),
    )
    .await;
    assert_eq!(
        default_frame,
        present(&[70, 70, 100, 70, 150]),
        "default frame is RANGE UNBOUNDED PRECEDING AND CURRENT ROW (peers on k share 70)"
    );

    let rows_unbounded = collect_sum(
        &session,
        &format!(
            "SELECT id, sum(v) OVER (ORDER BY k, id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s {WIN} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        rows_unbounded,
        present(&[10, 30, 100, 70, 150]),
        "ROWS does not pull later peers at the same k"
    );

    let range_unbounded = collect_sum(
        &session,
        &format!(
            "SELECT id, sum(v) OVER (ORDER BY k \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s {WIN} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        range_unbounded, default_frame,
        "explicit RANGE unbounded is the written form of the default frame"
    );
    assert_ne!(
        rows_unbounded, range_unbounded,
        "ROWS vs RANGE must differ on the tied k=1 rows — otherwise the pin is vacuous"
    );

    let range_one = collect_sum(
        &session,
        &format!(
            "SELECT id, sum(v) OVER (ORDER BY k \
             RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s {WIN} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        range_one,
        present(&[70, 70, 100, 70, 80]),
        "numeric RANGE 1 PRECEDING includes peers within k-distance 1"
    );

    let rows_sliding = collect_sum(
        &session,
        &format!(
            "SELECT id, sum(v) OVER (ORDER BY id \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s {WIN} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        rows_sliding,
        present(&[30, 60, 90, 120, 90]),
        "sliding ROWS 1 PRECEDING … 1 FOLLOWING under a total order on id"
    );

    // DF-native: unit-less 1 over DATE is one MONTH, so Jan 1/2/4 all sit in one frame.
    let date_unitless = collect_sum(
        &session,
        &format!(
            "SELECT id, sum(v) OVER (ORDER BY d \
             RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s {WIN_DATE} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        date_unitless,
        present(&[10, 30, 60]),
        "unit-less RANGE 1 PRECEDING over DATE is DataFusion's one MONTH, not Spark's one DAY"
    );

    let date_interval = collect_sum(
        &session,
        &format!(
            "SELECT id, sum(v) OVER (ORDER BY d \
             RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS s \
             {WIN_DATE} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        date_interval,
        present(&[10, 30, 30]),
        "INTERVAL '1' DAY is the portable day window (id 3 is two days after id 2)"
    );
    assert_ne!(
        date_unitless, date_interval,
        "unit-less vs INTERVAL must disagree — that is the DF-native / Spark split on DATE"
    );
}
