use serde_json::Value as Json;

use repark_common::ErrorClass;

use super::super::*;
use super::accept_any_refusals::refusal;
use super::common::*;

const ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../python/repark/tests/u8_write_sql_spark_oracle.json"
));

const REPLAYED: [&str; 25] = [
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
    "r2fix/A-",
    "r3/",
    "r3b/",
    "r3c/",
    "r3d/",
    "r3e/",
    "r3fix/",
];

fn json_cell(column: &dyn Array, index: usize) -> Json {
    if column.is_null(index) {
        return Json::Null;
    }
    if column.data_type().is_integer()
        && let Ok(numbers) = datafusion::arrow::compute::cast(column, &DataType::Int64)
        && let Some(numbers) = numbers.as_any().downcast_ref::<Int64Array>()
    {
        return Json::from(numbers.value(index));
    }
    let text = datafusion::arrow::compute::cast(column, &DataType::Utf8).unwrap();
    let text = text.as_any().downcast_ref::<StringArray>().unwrap();
    Json::from(text.value(index))
}

fn exception_class(spark_type: &str) -> ErrorClass {
    match spark_type {
        "IllegalArgumentException" => ErrorClass::IllegalArgument,
        "AnalysisException" => ErrorClass::Analysis,
        "ParseException" => ErrorClass::Parse,
        "NumberFormatException" => ErrorClass::NumberFormat,
        _ => ErrorClass::Base,
    }
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
    let residue = case.get("residue");
    let expected = residue.map_or_else(
        || serde_json::json!({"step": case["steps"].as_array().unwrap().last().unwrap(), "rows": case["rows"]}),
        |residue| residue["repark"].clone(),
    );
    let step = &expected["step"];
    if step == "ok" {
        run(&ctx, &catalogs, last).await;
    } else {
        let mapped = refusal(&ctx, &catalogs, last).await;
        let actual = normalized(&mapped.to_string());
        let spark_type = step["type"].as_str().unwrap();
        assert_eq!(
            mapped.exception_class(),
            exception_class(spark_type),
            "{key}: {mapped:?}"
        );
        if spark_type == "Py4JJavaError" {
            assert!(
                actual.contains("Cannot delete file where some, but not all, rows match filter"),
                "{key}: {actual}"
            );
        } else {
            assert_eq!(actual, step["message"].as_str().unwrap(), "{key}");
            if let Some(condition) = step["condition"].as_str() {
                assert!(
                    actual.starts_with(&format!("[{condition}]")),
                    "{key}: {actual}"
                );
            }
        }
    }
    let rows = table_rows(&ctx, &catalogs).await;
    assert_eq!(
        rows,
        sorted(expected["rows"].as_array().unwrap().clone()),
        "{key}"
    );
    if residue.is_some_and(|residue| residue["id"] == "R-2") {
        assert_eq!(
            rows,
            sorted(case["rows"].as_array().unwrap().clone()),
            "{key}"
        );
    }
}

#[tokio::test]
async fn every_replayed_spark_measurement_answers_as_spark_did() {
    let oracle: Json = serde_json::from_str(ORACLE).unwrap();
    let mut replayed = 0;
    for (key, case) in oracle["cases"].as_object().unwrap() {
        if !REPLAYED.iter().any(|prefix| key.starts_with(prefix)) {
            continue;
        }
        replay(key, case).await;
        replayed += 1;
    }
    assert_eq!(replayed, 438, "{replayed} cases replayed");
}
