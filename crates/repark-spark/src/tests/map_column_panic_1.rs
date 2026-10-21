use datafusion::arrow::util::display::array_value_to_string;
use futures::FutureExt;

use super::super::*;
use super::common::*;

const SOURCE_DDL: &str = "CREATE TABLE ice.sales.s (id INT, big BIGINT, k STRING, ts TIMESTAMP, \
                          nk STRING, nv INT, mid INT) USING iceberg";
const SOURCE_ROWS: &str = "INSERT INTO ice.sales.s VALUES \
     (0, 0, 'k0', TIMESTAMP '2026-01-02 03:04:05.123456+00:00', NULL, NULL, 0), \
     (1, 1, 'k1', TIMESTAMP '2026-01-03 03:04:05.123456+00:00', NULL, NULL, NULL), \
     (2, 2, 'k2', TIMESTAMP '2026-01-04 03:04:05.123456+00:00', NULL, NULL, 2)";
const DUPLICATE_KEY: &str = "[DUPLICATED_MAP_KEY] Duplicate map key a was found, please check the \
     input data.\nIf you want to remove the duplicated keys, you can set \
     \"spark.sql.mapKeyDedupPolicy\" to \"LAST_WIN\" so that the key inserted at last takes \
     precedence. SQLSTATE: 23505";
const NULL_KEY: &str = "[NULL_MAP_KEY] Cannot use null as map key. SQLSTATE: 2200E";

async fn setup_source(warehouse: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(warehouse).await;
    repark_functions::register_all(&ctx);
    run(&ctx, &catalogs, SOURCE_DDL).await;
    run(&ctx, &catalogs, SOURCE_ROWS).await;
    (ctx, catalogs)
}

struct Answer {
    columns: Vec<(String, DataType, bool)>,
    rows: Vec<Vec<String>>,
}

async fn collect_answer(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<Answer, DataFusionError> {
    let batches = execute(ctx, catalogs, sql).await?.collect().await?;
    let schema = batches
        .first()
        .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for column in batch.columns() {
                cells.push(array_value_to_string(column, row)?);
            }
            rows.push(cells);
        }
    }
    Ok(Answer { columns, rows })
}

async fn answer(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Answer {
    collect_answer(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

async fn failure(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> DataFusionError {
    let outcome = std::panic::AssertUnwindSafe(collect_answer(ctx, catalogs, sql))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| panic!("`{sql}` panicked"));
    match outcome {
        Err(error) => error,
        Ok(_) => panic!("`{sql}` must refuse"),
    }
}

fn map_parts(data_type: &DataType) -> (DataType, DataType) {
    let DataType::Map(entries, _) = data_type else {
        panic!("not a map: {data_type:?}");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("not a struct entry: {entries:?}");
    };
    (fields[0].data_type().clone(), fields[1].data_type().clone())
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect()
}

async fn assert_map_column(sql: &str, key: &DataType, value: &DataType, expected: &[&[&str]]) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let out = answer(&ctx, &catalogs, sql).await;
    assert_eq!(out.rows, strings(expected), "{sql}");
    let (name, data_type, nullable) = &out.columns[1];
    assert_eq!(name, "m", "{sql}");
    assert_eq!(
        &map_parts(data_type),
        &(key.clone(), value.clone()),
        "{sql}"
    );
    assert!(!nullable, "a Spark map(...) is never NULL: {sql}");
}

#[tokio::test]
async fn a_column_value_builds_one_map_per_row() {
    assert_map_column(
        "SELECT id, map('a', id) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[&["0", "{a: 0}"], &["1", "{a: 1}"], &["2", "{a: 2}"]],
    )
    .await;
}

#[tokio::test]
async fn a_single_row_answers_as_many_rows_do() {
    assert_map_column(
        "SELECT id, map('a', id) AS m FROM ice.sales.s WHERE id = 1",
        &DataType::Utf8,
        &DataType::Int32,
        &[&["1", "{a: 1}"]],
    )
    .await;
}

#[tokio::test]
async fn a_function_value_builds_one_map_per_row() {
    assert_map_column(
        "SELECT id, map('a', year(ts)) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[
            &["0", "{a: 2026}"],
            &["1", "{a: 2026}"],
            &["2", "{a: 2026}"],
        ],
    )
    .await;
}

#[tokio::test]
async fn a_cast_value_builds_one_map_per_row() {
    assert_map_column(
        "SELECT id, map('a', CAST(id AS BIGINT)) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int64,
        &[&["0", "{a: 0}"], &["1", "{a: 1}"], &["2", "{a: 2}"]],
    )
    .await;
}

#[tokio::test]
async fn a_struct_value_builds_one_map_per_row() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let out = answer(
        &ctx,
        &catalogs,
        "SELECT id, map('a', named_struct('x', id, 'y', k)) AS m FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        out.rows,
        strings(&[
            &["0", "{a: {x: 0, y: k0}}"],
            &["1", "{a: {x: 1, y: k1}}"],
            &["2", "{a: {x: 2, y: k2}}"],
        ])
    );
    let (key, value) = map_parts(&out.columns[1].1);
    assert_eq!(key, DataType::Utf8);
    let DataType::Struct(fields) = value else {
        panic!("{value:?}");
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["x", "y"]
    );
}

#[tokio::test]
async fn an_array_value_builds_one_map_per_row() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let out = answer(
        &ctx,
        &catalogs,
        "SELECT id, map('a', array(id, id + 1)) AS m FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        out.rows,
        strings(&[
            &["0", "{a: [0, 1]}"],
            &["1", "{a: [1, 2]}"],
            &["2", "{a: [2, 3]}"],
        ])
    );
    let (_, value) = map_parts(&out.columns[1].1);
    assert!(matches!(value, DataType::List(_)), "{value:?}");
}

#[tokio::test]
async fn a_map_value_builds_one_map_per_row() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let out = answer(
        &ctx,
        &catalogs,
        "SELECT id, map('a', map('b', id)) AS m FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        out.rows,
        strings(&[
            &["0", "{a: {b: 0}}"],
            &["1", "{a: {b: 1}}"],
            &["2", "{a: {b: 2}}"],
        ])
    );
    let (_, value) = map_parts(&out.columns[1].1);
    assert!(matches!(value, DataType::Map(_, _)), "{value:?}");
}

#[tokio::test]
async fn two_pairs_keep_their_order_in_every_row() {
    assert_map_column(
        "SELECT id, map('a', id, 'b', id + 1) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[
            &["0", "{a: 0, b: 1}"],
            &["1", "{a: 1, b: 2}"],
            &["2", "{a: 2, b: 3}"],
        ],
    )
    .await;
}

#[tokio::test]
async fn a_column_key_and_an_integer_key_build_one_map_per_row() {
    assert_map_column(
        "SELECT id, map(k, id) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[&["0", "{k0: 0}"], &["1", "{k1: 1}"], &["2", "{k2: 2}"]],
    )
    .await;
    assert_map_column(
        "SELECT id, map(id, k) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Int32,
        &DataType::Utf8,
        &[&["0", "{0: k0}"], &["1", "{1: k1}"], &["2", "{2: k2}"]],
    )
    .await;
}

#[tokio::test]
async fn mixed_value_types_widen_to_spark_common_type() {
    assert_map_column(
        "SELECT id, map('a', id, 'b', big) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int64,
        &[
            &["0", "{a: 0, b: 0}"],
            &["1", "{a: 1, b: 1}"],
            &["2", "{a: 2, b: 2}"],
        ],
    )
    .await;
    assert_map_column(
        "SELECT id, map('a', id, 'b', 2.5) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Decimal128(11, 1),
        &[
            &["0", "{a: 0.0, b: 2.5}"],
            &["1", "{a: 1.0, b: 2.5}"],
            &["2", "{a: 2.0, b: 2.5}"],
        ],
    )
    .await;
}

#[tokio::test]
async fn null_values_stay_in_the_map() {
    assert_map_column(
        "SELECT id, map('a', nv) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[&["0", "{a: }"], &["1", "{a: }"], &["2", "{a: }"]],
    )
    .await;
    assert_map_column(
        "SELECT id, map('a', mid, 'b', id) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[
            &["0", "{a: 0, b: 0}"],
            &["1", "{a: , b: 1}"],
            &["2", "{a: 2, b: 2}"],
        ],
    )
    .await;
}

#[tokio::test]
async fn a_timestamp_value_and_a_wall_cast_value_build_one_map_per_row() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let instant = answer(
        &ctx,
        &catalogs,
        "SELECT id, map('a', ts) AS m FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        instant.rows[0][1],
        "{a: 2026-01-02T03:04:05.123456Z}".to_string()
    );
    let wall = answer(
        &ctx,
        &catalogs,
        "SELECT id, map('a', CAST(ts AS TIMESTAMP_NTZ)) AS m FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        wall.rows,
        strings(&[
            &["0", "{a: 2026-01-02T03:04:05.123456}"],
            &["1", "{a: 2026-01-03T03:04:05.123456}"],
            &["2", "{a: 2026-01-04T03:04:05.123456}"],
        ])
    );
}

#[tokio::test]
async fn a_map_feeds_lookup_size_and_predicate() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let lookup = answer(
        &ctx,
        &catalogs,
        "SELECT id, map('a', id)['a'] AS v FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        lookup.rows,
        strings(&[&["0", "0"], &["1", "1"], &["2", "2"]])
    );
    let size = answer(
        &ctx,
        &catalogs,
        "SELECT id, size(map('a', id, 'b', id + 1)) AS v FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(size.rows, strings(&[&["0", "2"], &["1", "2"], &["2", "2"]]));
    let filtered = answer(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.s WHERE map('a', id)['a'] = 1",
    )
    .await;
    assert_eq!(filtered.rows, strings(&[&["1"]]));
}

#[tokio::test]
async fn a_duplicate_key_raises_spark_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    for sql in [
        "SELECT map('a', id, 'a', id + 1) AS m FROM ice.sales.s",
        "SELECT map('a', 1, 'a', 2) AS m",
        "SELECT map(k, id, k, id + 1) AS m FROM ice.sales.s WHERE id = 0",
    ] {
        let expected = if sql.contains("map(k") {
            DUPLICATE_KEY.replace("key a", "key k0")
        } else {
            DUPLICATE_KEY.to_string()
        };
        match failure(&ctx, &catalogs, sql).await {
            DataFusionError::Execution(text) => assert_eq!(text, expected, "{sql}"),
            other => panic!("{sql}: {other}"),
        }
    }
}

#[tokio::test]
async fn a_null_key_raises_spark_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    for sql in [
        "SELECT map(nk, id) AS m FROM ice.sales.s",
        "SELECT map(CAST(NULL AS STRING), id) AS m FROM ice.sales.s",
        "SELECT map(NULL, 1) AS m",
    ] {
        match failure(&ctx, &catalogs, sql).await {
            DataFusionError::Execution(text) => assert_eq!(text, NULL_KEY, "{sql}"),
            other => panic!("{sql}: {other}"),
        }
    }
}

#[tokio::test]
async fn an_odd_argument_count_raises_spark_error_at_planning() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    for (sql, count) in [
        ("SELECT map('a', id, 'b') AS m FROM ice.sales.s", 3),
        ("SELECT map('a') AS m FROM ice.sales.s", 1),
        ("SELECT map('a', 1, 'b') AS m", 3),
    ] {
        let expected = format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `map` requires 2n (n > 0) parameters but \
             the actual number is {count}. Please, refer to \
             'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. \
             SQLSTATE: 42605"
        );
        match failure(&ctx, &catalogs, sql).await {
            DataFusionError::Plan(text) => assert_eq!(text, expected, "{sql}"),
            other => panic!("{sql}: {other}"),
        }
    }
}

#[tokio::test]
async fn literal_maps_keep_answering_as_before() {
    assert_map_column(
        "SELECT id, map('a', 1, 'b', 2) AS m FROM ice.sales.s ORDER BY id",
        &DataType::Utf8,
        &DataType::Int32,
        &[
            &["0", "{a: 1, b: 2}"],
            &["1", "{a: 1, b: 2}"],
            &["2", "{a: 1, b: 2}"],
        ],
    )
    .await;
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    let empty = answer(
        &ctx,
        &catalogs,
        "SELECT id, map() AS m FROM ice.sales.s ORDER BY id",
    )
    .await;
    assert_eq!(
        empty.rows,
        strings(&[&["0", "{}"], &["1", "{}"], &["2", "{}"]])
    );
}

#[tokio::test]
async fn an_insert_select_stores_a_column_map_into_a_map_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m (id INT, c MAP<STRING, INT>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.m SELECT id, map('a', id) FROM ice.sales.s",
    )
    .await;
    let stored = answer(
        &ctx,
        &catalogs,
        "SELECT id, c, c['a'] AS v FROM ice.sales.m ORDER BY id",
    )
    .await;
    assert_eq!(
        stored.rows,
        strings(&[
            &["0", "{a: 0}", "0"],
            &["1", "{a: 1}", "1"],
            &["2", "{a: 2}", "2"]
        ])
    );
}

#[tokio::test]
async fn an_insert_select_stores_a_two_pair_function_map_into_a_map_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m (id INT, c MAP<STRING, BIGINT>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.m SELECT id, map('year', year(ts), 'id', id) FROM ice.sales.s",
    )
    .await;
    let stored = answer(&ctx, &catalogs, "SELECT id, c FROM ice.sales.m ORDER BY id").await;
    assert_eq!(
        stored.rows,
        strings(&[
            &["0", "{year: 2026, id: 0}"],
            &["1", "{year: 2026, id: 1}"],
            &["2", "{year: 2026, id: 2}"],
        ])
    );
}

#[tokio::test]
async fn update_and_merge_assign_a_column_map() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_source(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mu (id INT, c MAP<STRING, INT>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.mu SELECT id, map('z', 9) FROM ice.sales.s",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.mu SET c = map('u', id + 10) WHERE id = 0",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.mu t USING ice.sales.s s ON t.id = s.id \
         WHEN MATCHED AND s.id = 1 THEN UPDATE SET c = map('m', s.id + 20)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.mu t USING (SELECT 7 AS id) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, c) VALUES (s.id, map('n', s.id))",
    )
    .await;
    let stored = answer(
        &ctx,
        &catalogs,
        "SELECT id, c FROM ice.sales.mu ORDER BY id",
    )
    .await;
    assert_eq!(
        stored.rows,
        strings(&[
            &["0", "{u: 10}"],
            &["1", "{m: 21}"],
            &["2", "{z: 9}"],
            &["7", "{n: 7}"],
        ])
    );
}
