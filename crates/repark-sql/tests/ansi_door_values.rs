//! **ANSI-door value pins** — G11, correctness not parity.
//! Spark is **not** this door's oracle. Expectations are standard SQL behavior.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

/// Native ANSI session plus one in-memory Iceberg catalog over a temp warehouse.
struct Door {
    session: ReparkSession,
    warehouse: String,
    _dir: TempDir,
}

async fn native_ansi_door() -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session");
    repark_spark::install_integer_overflow(session.context());
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

/// Create `ice.sales` and `ice.sales.nums(n INT) = {1, NULL, 2}`.
async fn make_nullable_ints(door: &Door) {
    let warehouse = &door.warehouse;
    door.session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");
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

/// Plan- or collect-time error text. Panics if `{sql}` succeeds.
async fn collect_error(session: &ReparkSession, sql: &str) -> String {
    match session.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected `{sql}` to fail, but it produced rows"),
        },
    }
}

/// One-column Int32 scalar: `(DataType, nullable, Option<i32>)`.
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

/// One-column Int64 scalar: `(DataType, nullable, Option<i64>)`.
async fn int64_scalar(session: &ReparkSession, sql: &str) -> (DataType, bool, Option<i64>) {
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
        DataType::Int64,
        "expected Int64 for `{sql}`, got {data_type:?}"
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
        .downcast_ref::<Int64Array>()
        .expect("Int64Array");
    let value = if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    };
    (data_type, nullable, value)
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

/// CAST overflow raises. `CAST(200 AS TINYINT)` is outside `Int8`; standard SQL requires an error.
#[tokio::test]
async fn ansi_door_cast_overflow_int_to_tinyint_raises() {
    let ansi = native_ansi_door().await;
    let error = collect_error(&ansi.session, "SELECT CAST(200 AS TINYINT) AS v").await;
    assert!(
        error.contains("Can't cast value 200 to type Int8"),
        "ANSI CAST overflow must name the out-of-range value and the target type, got: {error}"
    );
}

/// Integer `/` truncates toward zero. Standard SQL: `INT / INT` is integer division.
#[tokio::test]
async fn ansi_door_integer_division_truncates_toward_zero() {
    let ansi = native_ansi_door().await;
    let pin = int32_scalar(&ansi.session, "SELECT CAST(5 AS INT) / CAST(2 AS INT) AS v").await;
    assert_eq!(
        pin,
        (DataType::Int32, false, Some(2)),
        "ANSI integer `/` must stay Int32 and truncate toward zero (5/2 = 2, not 2.5)"
    );
}

/// Integer `/ 0` raises. Standard SQL: division by zero is an exception, not NULL.
#[tokio::test]
async fn ansi_door_integer_division_by_zero_raises() {
    let ansi = native_ansi_door().await;
    let error = collect_error(&ansi.session, "SELECT CAST(1 AS INT) / CAST(0 AS INT) AS v").await;
    assert!(
        error.contains("Divide by zero"),
        "ANSI integer `/ 0` must raise, got: {error}"
    );
}

/// `SUM` skips NULLs. `{1, NULL, 2}` sums to `3` at Int64.
#[tokio::test]
async fn ansi_door_sum_skips_nulls() {
    let ansi = native_ansi_door().await;
    make_nullable_ints(&ansi).await;
    let pin = int64_scalar(&ansi.session, "SELECT SUM(n) AS v FROM ice.sales.nums").await;
    assert_eq!(
        pin,
        (DataType::Int64, true, Some(3)),
        "SUM must ignore the NULL and add the two ints (1+2 = 3)"
    );
}

/// Default `ORDER BY … ASC` is `NULLS LAST` (nulls-sort-high; Trino / PostgreSQL).
#[tokio::test]
async fn ansi_door_order_by_asc_defaults_to_nulls_last() {
    let ansi = native_ansi_door().await;
    make_nullable_ints(&ansi).await;
    let pin = ordered_int32(&ansi.session, "SELECT n FROM ice.sales.nums ORDER BY n ASC").await;
    assert_eq!(
        pin,
        (DataType::Int32, true, vec![Some(1), Some(2), None]),
        "ANSI ASC must default to NULLS LAST"
    );
}

/// No implicit string→number coercion. `'1' + 1` is a type error; the value is not `2`.
#[tokio::test]
async fn ansi_door_implicit_string_plus_number_refuses() {
    let ansi = native_ansi_door().await;
    let error = collect_error(&ansi.session, "SELECT '1' + 1 AS v").await;
    assert!(
        error.contains("Cannot coerce arithmetic expression Utf8 + Int64"),
        "ANSI door must refuse implicit string→number coercion, got: {error}"
    );
}

/// pins: f-y10-1-int-overflow/C-001, C-002, C-003
#[tokio::test]
async fn ansi_door_untyped_one_plus_one_stays_int64() {
    let ansi = native_ansi_door().await;
    let pin = int64_scalar(&ansi.session, "SELECT 1 + 1 AS v").await;
    assert_eq!(
        pin,
        (DataType::Int64, false, Some(2)),
        "untyped 1+1 stays Int64 on the ANSI door"
    );
}

/// pins: f-y10-1-int-overflow/C-001, C-002, C-003
#[tokio::test]
async fn ansi_door_untyped_overflow_widens_to_int64() {
    let ansi = native_ansi_door().await;
    let pin = int64_scalar(&ansi.session, "SELECT 2147483647 + 1 AS v").await;
    assert_eq!(
        pin,
        (DataType::Int64, false, Some(2_147_483_648)),
        "untyped INT-boundary add stays the Int64 widen on the ANSI door"
    );
}

/// pins: f-y10-1-int-overflow/C-003
#[tokio::test]
async fn ansi_door_int32_add_overflow_raises() {
    let ansi = native_ansi_door().await;
    let error = collect_error(
        &ansi.session,
        "SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS v",
    )
    .await;
    assert!(
        error.contains("ARITHMETIC_OVERFLOW"),
        "ANSI door must raise on INT overflow (standard SQL), got: {error}"
    );
}

/// pins: f-y10-1-int-overflow/C-003
#[tokio::test]
async fn ansi_door_int32_add_literal_overflow_raises() {
    let ansi = native_ansi_door().await;
    let error = collect_error(&ansi.session, "SELECT CAST(2147483647 AS INT) + 1 AS v").await;
    assert!(
        error.contains("ARITHMETIC_OVERFLOW"),
        "ANSI door CAST(INT)+1 must raise, not widen, got: {error}"
    );
}
