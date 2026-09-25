use serde_json::Value as Json;

use super::super::*;
use super::accept_any_refusals::refusal;
use super::common::*;

const ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../python/repark/tests/u8_write_sql_spark_oracle.json"
));

const REPLAYED: [&str; 18] = [
    "r1/S1-",
    "r1/S2-",
    "r1/S3-",
    "r1/S4-",
    "r1/S5-",
    "r2/C-",
    "r2/I-",
    "r2/O-",
    "r2/A-",
    "r2b/O-",
    "r2fix/L-",
    "r2fix/IN-",
    "r2fix/SM-",
    "r2fix/IO-",
    "r2fix/QI-",
    "r2fix/Q-",
    "r2fix/CN-",
    "r2fix/P-",
];

const SPARK_BUG: [&str; 2] = ["r1/S1-notlike", "r2/C-not-like"];

const SPELLING: [&str; 6] = [
    "r2fix/Q-and-upper",
    "r2fix/Q-upper-and-arith",
    "r2fix/Q-upper-or",
    "r2fix/Q-not-upper-and",
    "r2fix/Q-and-or-arith",
    "r2fix/Q-like-suffix-and",
];

fn json_cell(column: &dyn Array, index: usize) -> Json {
    if column.is_null(index) {
        return Json::Null;
    }
    if let Ok(numbers) = datafusion::arrow::compute::cast(column, &DataType::Int64)
        && let Some(numbers) = numbers.as_any().downcast_ref::<Int64Array>()
        && !matches!(
            column.data_type(),
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        )
    {
        return Json::from(numbers.value(index));
    }
    let text = datafusion::arrow::compute::cast(column, &DataType::Utf8).unwrap();
    let text = text.as_any().downcast_ref::<StringArray>().unwrap();
    Json::from(text.value(index))
}

fn sorted(mut rows: Vec<Json>) -> Vec<Json> {
    rows.sort_by_key(ToString::to_string);
    rows
}

async fn table_rows(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<Json> {
    let batches = execute(ctx, catalogs, "SELECT * FROM ice.sales.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for index in 0..batch.num_rows() {
            rows.push(Json::Array(
                batch
                    .columns()
                    .iter()
                    .map(|column| json_cell(column.as_ref(), index))
                    .collect(),
            ));
        }
    }
    sorted(rows)
}

fn normalized(message: &str) -> String {
    message
        .strip_prefix("Error during planning: ")
        .unwrap_or(message)
        .replace("`ice`.`sales`.`t`", "`sc`.`ns`.`<t>`")
}

async fn replay(key: &str, case: &Json) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let columns = case["columns"].as_str().unwrap();
    let partitioned_by = case["partitioned_by"].as_str().unwrap_or("");
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.t ({columns}) USING iceberg {partitioned_by}"),
    )
    .await;
    if case["default_seed"].as_bool().unwrap() {
        run(&ctx, &catalogs, super::replace_where::SEED).await;
    }
    let statements: Vec<String> = case["statements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|statement| statement.as_str().unwrap().replace("{T}", "ice.sales.t"))
        .collect();
    let (last, setup_statements) = statements.split_last().unwrap();
    for statement in setup_statements {
        run(&ctx, &catalogs, statement).await;
    }
    let spark_step = case["steps"].as_array().unwrap().last().unwrap();
    if spark_step == "ok" {
        run(&ctx, &catalogs, last).await;
    } else {
        let mapped = refusal(&ctx, &catalogs, last).await;
        let actual = normalized(&mapped.to_string());
        let expected = spark_step["message"].as_str().unwrap();
        if expected.starts_with("Cannot convert Spark predicate") {
            assert!(
                matches!(mapped, repark_common::Error::IllegalArgument(_)),
                "{key}: {mapped:?}"
            );
            if !SPELLING.contains(&key) {
                assert_eq!(actual, expected, "{key}");
            }
        } else if let Some(condition) = spark_step["condition"].as_str() {
            assert!(
                actual.starts_with(&format!("[{condition}]")),
                "{key}: {actual}"
            );
            if condition.starts_with("INSERT_COLUMN_ARITY_MISMATCH") {
                assert_eq!(actual, expected, "{key}");
            }
        } else {
            assert!(
                actual.contains("Cannot delete file where some, but not all, rows match filter")
                    || actual.contains("Cannot find field"),
                "{key}: {actual}"
            );
        }
    }
    let expected_rows = sorted(case["rows"].as_array().unwrap().clone());
    assert_eq!(table_rows(&ctx, &catalogs).await, expected_rows, "{key}");
}

#[tokio::test]
async fn every_replayed_spark_measurement_answers_as_spark_did() {
    let oracle: Json = serde_json::from_str(ORACLE).unwrap();
    let mut replayed = 0;
    for (key, case) in oracle["cases"].as_object().unwrap() {
        if !REPLAYED.iter().any(|prefix| key.starts_with(prefix))
            || SPARK_BUG.contains(&key.as_str())
        {
            continue;
        }
        replay(key, case).await;
        replayed += 1;
    }
    assert!(replayed >= 200, "only {replayed} cases replayed");
}
