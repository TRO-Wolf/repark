use datafusion::arrow::array::AsArray;
use serde_json::Value as Json;

use super::super::*;
use super::accept_any_refusals::refusal;
use super::common::*;

const ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../python/repark/tests/u8_write_sql_nested_spark_oracle.json"
));

const TABLE: &str = "ice.sales.t";

fn json_cell(column: &dyn Array, index: usize) -> Json {
    if column.is_null(index) {
        return Json::Null;
    }
    match column.data_type() {
        DataType::Struct(fields) => {
            let array = column.as_struct();
            Json::Object(
                fields
                    .iter()
                    .zip(array.columns())
                    .map(|(field, child)| (field.name().clone(), json_cell(child.as_ref(), index)))
                    .collect(),
            )
        }
        DataType::List(_) => {
            let items = column.as_list::<i32>().value(index);
            Json::Array(
                (0..items.len())
                    .map(|item| json_cell(items.as_ref(), item))
                    .collect(),
            )
        }
        DataType::Map(_, _) => {
            let entries = column.as_map().value(index);
            let keys =
                datafusion::arrow::compute::cast(entries.column(0), &DataType::Utf8).unwrap();
            let keys = keys.as_string::<i32>();
            Json::Object(
                (0..entries.len())
                    .map(|entry| {
                        (
                            keys.value(entry).to_string(),
                            json_cell(entries.column(1).as_ref(), entry),
                        )
                    })
                    .collect(),
            )
        }
        data_type if data_type.is_integer() => {
            let numbers = datafusion::arrow::compute::cast(column, &DataType::Int64).unwrap();
            Json::from(
                numbers
                    .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                    .value(index),
            )
        }
        _ => {
            let text = datafusion::arrow::compute::cast(column, &DataType::Utf8).unwrap();
            Json::from(text.as_string::<i32>().value(index))
        }
    }
}

fn sorted(mut rows: Vec<Json>) -> Vec<Json> {
    rows.sort_by_key(ToString::to_string);
    rows
}

async fn table_rows(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<Json> {
    let batches = execute(ctx, catalogs, &format!("SELECT * FROM {TABLE}"))
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
        .replace("`ice`.`sales`.`t`", "`<T>`")
        .replace(TABLE, "<T>")
        .replace("`t`", "`<t>`")
}

fn rust_door_replays(case: &Json) -> bool {
    let columns = case["columns"].as_str().unwrap();
    !columns.contains("ARRAY") && !columns.contains("MAP") && case.get("residue").is_none()
}

async fn replay(key: &str, case: &Json) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let columns = case["columns"].as_str().unwrap();
    let props = case["props"].as_str().unwrap();
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE {TABLE} ({columns}) USING iceberg {props}"),
    )
    .await;
    let seed = case["seed"].as_str().unwrap();
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO {TABLE} VALUES {seed}"),
    )
    .await;
    let statements: Vec<String> = case["statements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|statement| statement.as_str().unwrap().replace("{T}", TABLE))
        .collect();
    let (last, setup_statements) = statements.split_last().unwrap();
    for statement in setup_statements {
        run(&ctx, &catalogs, statement).await;
    }
    let step = case["steps"].as_array().unwrap().last().unwrap();
    if step == "ok" {
        run(&ctx, &catalogs, last).await;
    } else {
        let mapped = refusal(&ctx, &catalogs, last).await;
        assert_eq!(
            mapped.exception_class(),
            repark_common::ErrorClass::Analysis,
            "{key}: {mapped:?}"
        );
        assert_eq!(
            normalized(&mapped.to_string()),
            step["message"].as_str().unwrap(),
            "{key}"
        );
    }
    assert_eq!(
        table_rows(&ctx, &catalogs).await,
        sorted(case["rows"].as_array().unwrap().clone()),
        "{key}"
    );
}

#[tokio::test]
async fn every_nested_assignment_measurement_answers_as_spark_did() {
    let oracle: Json = serde_json::from_str(ORACLE).unwrap();
    let mut replayed = 0;
    for (key, case) in oracle["cases"].as_object().unwrap() {
        if !rust_door_replays(case) {
            continue;
        }
        replay(key, case).await;
        replayed += 1;
    }
    assert_eq!(replayed, 108, "{replayed} cases replayed");
}
