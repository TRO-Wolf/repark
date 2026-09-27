use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{TimeUnit, TimestampMicrosecondType};
use datafusion::sql::sqlparser::parser::ParserError;

use super::super::*;
use super::common::*;

async fn setup_ntz(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    repark_functions::register_all(&ctx);
    (ctx, catalogs)
}

async fn setup_ntz_at(wh: &TempDir, zone: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup_at_zone(wh, zone).await;
    repark_functions::register_all(&ctx);
    (ctx, catalogs)
}

async fn batches(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

async fn failure(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(done) => panic!("{sql} answered instead of refusing: {done:?}"),
        },
    }
}

fn wall_ticks(batch: &RecordBatch, column: usize) -> Vec<Option<i64>> {
    let micros = batch
        .column(column)
        .as_primitive::<TimestampMicrosecondType>();
    (0..micros.len())
        .map(|row| micros.is_valid(row).then(|| micros.value(row)))
        .collect()
}

fn naive_micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

#[tokio::test]
async fn ntz_literal_is_a_naive_microsecond_wall() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT TIMESTAMP_NTZ'2024-01-01 12:34:56.123456'",
    )
    .await;
    let field = out[0].schema().field(0).clone();
    assert_eq!(field.data_type(), &naive_micros());
    assert_eq!(field.name(), "TIMESTAMP_NTZ '2024-01-01 12:34:56.123456'");
    assert_eq!(wall_ticks(&out[0], 0), vec![Some(1_704_112_496_123_456)]);
    let quoted = batches(
        &ctx,
        &catalogs,
        "SELECT TIMESTAMP_NTZ \"2024-01-01 00:00:00\" AS v",
    )
    .await;
    assert_eq!(wall_ticks(&quoted[0], 0), vec![Some(1_704_067_200_000_000)]);
}

#[tokio::test]
async fn ntz_literal_drops_a_zone_suffix_and_truncates_to_micros() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let zoned = batches(
        &ctx,
        &catalogs,
        "SELECT TIMESTAMP_NTZ'2024-01-01 12:00:00+05:00'",
    )
    .await;
    assert_eq!(
        zoned[0].schema().field(0).name(),
        "TIMESTAMP_NTZ '2024-01-01 12:00:00'"
    );
    assert_eq!(wall_ticks(&zoned[0], 0), vec![Some(1_704_110_400_000_000)]);
    let fract = batches(
        &ctx,
        &catalogs,
        "SELECT TIMESTAMP_NTZ'2024-01-01 12:34:56.1234567' AS v",
    )
    .await;
    assert_eq!(wall_ticks(&fract[0], 0), vec![Some(1_704_112_496_123_456)]);
    let dated = batches(&ctx, &catalogs, "SELECT timestamp_ntz'2024-01-01' AS v").await;
    assert_eq!(wall_ticks(&dated[0], 0), vec![Some(1_704_067_200_000_000)]);
    let zone_wh = TempDir::new().unwrap();
    let (ny, ny_catalogs) = setup_ntz_at(&zone_wh, "America/New_York").await;
    let ny_zoned = batches(
        &ny,
        &ny_catalogs,
        "SELECT TIMESTAMP_NTZ'2024-01-01 12:00:00+05:00' AS v",
    )
    .await;
    assert_eq!(
        wall_ticks(&ny_zoned[0], 0),
        vec![Some(1_704_110_400_000_000)]
    );
}

#[tokio::test]
async fn invalid_ntz_literal_is_a_parse_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let error = execute(&ctx, &catalogs, "SELECT TIMESTAMP_NTZ'x'")
        .await
        .expect_err("x refuses");
    let DataFusionError::SQL(inner, _) = error else {
        panic!("a parse error, got {error}");
    };
    let ParserError::ParserError(message) = *inner else {
        panic!("a parser message, got {inner}");
    };
    let mut lines = message.lines();
    assert_eq!(
        lines.next().unwrap_or_default(),
        "[INVALID_TYPED_LITERAL] The value of the typed literal \"TIMESTAMP_NTZ\" is invalid: \
         'x'. SQLSTATE: 42604"
    );
    assert_eq!(
        lines.next().unwrap_or_default(),
        "== SQL (line 1, position 8) =="
    );
}

#[tokio::test]
async fn ntz_cast_sources_answer_as_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT CAST('2024-01-01 12:34:56.123456' AS TIMESTAMP_NTZ) AS a, \
         '2024-01-01 12:34:56.123456'::timestamp_ntz AS b, \
         CAST(DATE'2024-01-02' AS TIMESTAMP_NTZ) AS c, CAST(NULL AS TIMESTAMP_NTZ) AS d, \
         CAST('2024-01-01 12:00:00+05:00' AS TIMESTAMP_NTZ) AS e, \
         CAST('2024-01-01' AS TIMESTAMP_NTZ) AS f, \
         typeof(CAST('2024-01-01' AS TIMESTAMP_NTZ)) AS g",
    )
    .await;
    let identity = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP_NTZ'2024-01-01 12:34:56.123456' AS TIMESTAMP_NTZ) AS v",
    )
    .await;
    assert_eq!(identity[0].schema().field(0).data_type(), &naive_micros());
    assert_eq!(
        wall_ticks(&identity[0], 0),
        vec![Some(1_704_112_496_123_456)]
    );
    for column in 0..6 {
        assert_eq!(
            out[0].schema().field(column).data_type(),
            &naive_micros(),
            "{column}"
        );
    }
    assert_eq!(wall_ticks(&out[0], 0), vec![Some(1_704_112_496_123_456)]);
    assert_eq!(wall_ticks(&out[0], 1), vec![Some(1_704_112_496_123_456)]);
    assert_eq!(wall_ticks(&out[0], 2), vec![Some(1_704_153_600_000_000)]);
    assert!(out[0].column(3).is_null(0));
    assert_eq!(wall_ticks(&out[0], 4), vec![Some(1_704_110_400_000_000)]);
    assert_eq!(wall_ticks(&out[0], 5), vec![Some(1_704_067_200_000_000)]);
    assert_eq!(
        out[0].column(6).as_string::<i32>().value(0),
        "timestamp_ntz"
    );
    for (zone, expected) in [
        ("UTC", 1_704_110_400_000_000),
        ("America/New_York", 1_704_092_400_000_000),
    ] {
        let zone_wh = TempDir::new().unwrap();
        let (zctx, zcatalogs) = setup_ntz_at(&zone_wh, zone).await;
        let inst = batches(
            &zctx,
            &zcatalogs,
            "SELECT CAST(TIMESTAMP'2024-01-01 12:00:00Z' AS TIMESTAMP_NTZ) AS v",
        )
        .await;
        assert_eq!(wall_ticks(&inst[0], 0), vec![Some(expected)], "{zone}");
        let zoned = batches(
            &zctx,
            &zcatalogs,
            "SELECT CAST('2024-01-01 12:00:00+05:00' AS TIMESTAMP_NTZ) AS v",
        )
        .await;
        assert_eq!(
            wall_ticks(&zoned[0], 0),
            vec![Some(1_704_110_400_000_000)],
            "{zone}"
        );
    }
}

#[tokio::test]
async fn try_cast_to_ntz_is_null_on_garbage() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT try_cast('x' AS TIMESTAMP_NTZ) AS v",
    )
    .await;
    assert_eq!(out[0].schema().field(0).data_type(), &naive_micros());
    assert!(out[0].column(0).is_null(0));
    let text = failure(&ctx, &catalogs, "SELECT CAST('x' AS TIMESTAMP_NTZ) AS v").await;
    assert!(text.contains("[CAST_INVALID_INPUT]"), "{text}");
    assert!(text.contains("\"TIMESTAMP_NTZ\""), "{text}");
}

#[tokio::test]
async fn ntz_cast_refuses_numeric_sources_and_targets() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let source = failure(&ctx, &catalogs, "SELECT CAST(1 AS TIMESTAMP_NTZ) AS v").await;
    assert!(
        source.contains(
            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"CAST(1 AS \
             TIMESTAMP_NTZ)\" due to data type mismatch: cannot cast \"INT\" to \"TIMESTAMP_NTZ\". \
             SQLSTATE: 42K09"
        ),
        "{source}"
    );
    for (target, name) in [
        ("BIGINT", "BIGINT"),
        ("INT", "INT"),
        ("DOUBLE", "DOUBLE"),
        ("DECIMAL(10,2)", "DECIMAL(10,2)"),
    ] {
        let refused = failure(
            &ctx,
            &catalogs,
            &format!("SELECT CAST(TIMESTAMP_NTZ'2024-01-01 00:00:00' AS {target}) AS v"),
        )
        .await;
        assert_eq!(
            refused,
            format!(
                "spark_expr_semantics\ncaused by\nError during planning: \
                 [DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \
                 \"CAST(TIMESTAMP_NTZ '2024-01-01 00:00:00' AS {name})\" due to data type \
                 mismatch: cannot cast \"TIMESTAMP_NTZ\" to \"{name}\". SQLSTATE: 42K09"
            ),
            "{target}"
        );
    }
}

#[tokio::test]
async fn nested_ntz_cast_renders_like_spark_in_the_numeric_refusal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    for (sql, rendered) in [
        (
            "SELECT CAST(CAST('2024-01-01' AS TIMESTAMP_NTZ) AS BIGINT) AS v",
            "CAST(CAST(2024-01-01 AS TIMESTAMP_NTZ) AS BIGINT)",
        ),
        (
            "SELECT TRY_CAST(TIMESTAMP_NTZ'2024-01-01 00:00:00' AS BIGINT) AS v",
            "TRY_CAST(TIMESTAMP_NTZ '2024-01-01 00:00:00' AS BIGINT)",
        ),
        (
            "SELECT CAST(c AS BIGINT) FROM (SELECT TIMESTAMP_NTZ'2024-01-01 00:00:00' AS c)",
            "CAST(c AS BIGINT)",
        ),
    ] {
        let refused = failure(&ctx, &catalogs, sql).await;
        assert_eq!(
            refused,
            format!(
                "spark_expr_semantics\ncaused by\nError during planning: \
                 [DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"{rendered}\" due \
                 to data type mismatch: cannot cast \"TIMESTAMP_NTZ\" to \"BIGINT\". SQLSTATE: 42K09"
            ),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn ltz_numeric_cast_stays_epoch_seconds() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' AS BIGINT) AS v",
    )
    .await;
    assert_eq!(out[0].schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(
        out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0),
        1_704_067_200
    );
}

#[tokio::test]
async fn nested_ntz_cast_target_keeps_the_r4_refusal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    for sql in [
        "SELECT CAST(array('2024-01-01 00:00:00') AS ARRAY<TIMESTAMP_NTZ>) AS v",
        "SELECT CAST(named_struct('a', '2024-01-01') AS STRUCT<a: TIMESTAMP_NTZ>) AS v",
    ] {
        let text = failure(&ctx, &catalogs, sql).await;
        assert!(
            text.contains("[UNSUPPORTED_TIMESTAMP_NTZ]")
                && text.contains("nested cast target")
                && text.contains("the scalar TIMESTAMP_NTZ literal and cast are"),
            "{sql}: {text}"
        );
    }
}

#[tokio::test]
async fn ntz_literal_reaches_every_dml_door() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:34:56.123456'), (1, \
         CAST('2024-01-02 00:00:00' AS TIMESTAMP_NTZ)), (2, NULL)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz SELECT 3, TIMESTAMP_NTZ'2024-01-03 00:00:00'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.ntz SET c = TIMESTAMP_NTZ'2031-01-01 00:00:00' WHERE id = 1",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.ntz WHERE c < TIMESTAMP_NTZ'2024-01-02 00:00:00'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.ntz t USING (SELECT 9 AS id, CAST('2030-01-01 00:00:00' AS \
         TIMESTAMP_NTZ) AS c) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ctas USING iceberg AS SELECT TIMESTAMP_NTZ'2024-01-01 12:00:00' \
         AS c",
    )
    .await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT id, CAST(c AS STRING) AS s FROM ice.sales.ntz ORDER BY id",
    )
    .await;
    let ids = out[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    let walls = out[0].column(1).as_string::<i32>();
    let rows: Vec<(i32, Option<String>)> = (0..out[0].num_rows())
        .map(|row| {
            let wall = if walls.is_null(row) {
                None
            } else {
                Some(walls.value(row).to_string())
            };
            (ids.value(row), wall)
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, Some("2031-01-01 00:00:00".to_string())),
            (2, None),
            (3, Some("2024-01-03 00:00:00".to_string())),
            (9, Some("2030-01-01 00:00:00".to_string())),
        ]
    );
    let stored = load_sales_table(&catalogs, "ctas").await;
    let fields: Vec<(String, String)> = stored
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect();
    assert_eq!(fields, vec![("c".to_string(), "timestamp".to_string())]);
}
