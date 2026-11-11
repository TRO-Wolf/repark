use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::common::ScalarValue;
use futures::FutureExt;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use serde_json::Value;
use tempfile::TempDir;

const CELLS: &str =
    include_str!("../../../python/repark/tests/sql/data/struct_compare_seat_family_cells.jsonl");
const PANICKED: &str = "PANIC";

fn json_of(array: &ArrayRef, row: usize) -> Value {
    ScalarValue::try_from_array(array, row).map_or(Value::Null, |scalar| json_scalar(&scalar))
}

fn json_rows(array: &ArrayRef) -> Value {
    Value::Array((0..array.len()).map(|row| json_of(array, row)).collect())
}

fn json_scalar(scalar: &ScalarValue) -> Value {
    if scalar.is_null() {
        return Value::Null;
    }
    match scalar {
        ScalarValue::Boolean(Some(value)) => Value::Bool(*value),
        ScalarValue::Int8(Some(value)) => Value::from(*value),
        ScalarValue::Int16(Some(value)) => Value::from(*value),
        ScalarValue::Int32(Some(value)) => Value::from(*value),
        ScalarValue::Int64(Some(value)) => Value::from(*value),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Value::String(value.clone()),
        ScalarValue::Struct(array) => Value::Array(
            array
                .columns()
                .iter()
                .map(|column| json_of(column, 0))
                .collect(),
        ),
        ScalarValue::List(array) => json_rows(&array.value(0)),
        ScalarValue::LargeList(array) => json_rows(&array.value(0)),
        ScalarValue::FixedSizeList(array) => json_rows(&array.value(0)),
        ScalarValue::Map(array) => {
            let entries = array.value(0);
            let mut pairs: Vec<Value> = (0..entries.len())
                .map(|row| {
                    Value::Array(vec![
                        json_of(entries.column(0), row),
                        json_of(entries.column(1), row),
                    ])
                })
                .collect();
            pairs.sort_by_key(ToString::to_string);
            Value::Array(pairs)
        }
        other => Value::String(other.to_string()),
    }
}

fn sorted(rows: Vec<Value>) -> Vec<Value> {
    let mut rows = rows;
    rows.sort_by_key(ToString::to_string);
    rows
}

fn projected(batches: &[RecordBatch], columns: &[String]) -> Option<Vec<Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        let names: Vec<&str> = batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let index = columns
            .iter()
            .map(|name| names.iter().position(|field| field == name))
            .collect::<Option<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            rows.push(Value::Array(
                index
                    .iter()
                    .map(|column| json_of(batch.column(*column), row))
                    .collect(),
            ));
        }
    }
    Some(sorted(rows))
}

struct Door {
    session: ReparkSession,
    _warehouse: TempDir,
}

impl Door {
    async fn new(views: &Value, case_sensitive: bool) -> Self {
        let session = ReparkSession::builder()
            .with_sql_dialect(Arc::new(SparkDialect))
            .with_extension(Arc::new(SparkExtension))
            .config("spark.sql.caseSensitive", case_sensitive.to_string())
            .config("spark.sql.session.timeZone", "UTC")
            .config("repark.sql.allowCreateFormatVersion3", "true")
            .build()
            .expect("session");
        let warehouse = TempDir::new().expect("warehouse");
        session
            .register_memory_catalog("hd", warehouse.path().to_str().expect("path"))
            .await
            .expect("catalog");
        let door = Self {
            session,
            _warehouse: warehouse,
        };
        door.batches("CREATE NAMESPACE IF NOT EXISTS hd.vp")
            .await
            .expect("namespace");
        for view in views.as_array().expect("views") {
            let name = view[0].as_str().expect("view name");
            let sql = view[1].as_str().expect("view sql");
            let (_, body) = sql.split_once(" AS ").expect("view body");
            let frame = door.session.sql(body).await.expect("view body plans");
            door.session
                .context()
                .register_table(name, frame.into_view())
                .expect("view");
        }
        door
    }

    async fn batches(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let run = async {
            let frame = self
                .session
                .sql(sql)
                .await
                .map_err(|error| error.to_string())?;
            frame.collect().await.map_err(|error| error.to_string())
        };
        AssertUnwindSafe(run)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(PANICKED.to_string()))
    }

    async fn query(&self, cell: &Value) -> Result<Vec<RecordBatch>, String> {
        let text = cell["op"]["text"].as_str().expect("text");
        if cell["prepare"] != true {
            return self.batches(text).await;
        }
        let name = format!("p_{}", cell["id"].as_str().expect("id").to_lowercase());
        self.batches(&format!("PREPARE {name} AS {text}")).await?;
        self.batches(&format!("EXECUTE {name}")).await
    }

    async fn write(&self, cell: &Value) -> Result<Vec<Value>, String> {
        for statement in cell["setup"].as_array().expect("setup") {
            self.batches(statement.as_str().expect("setup sql"))
                .await
                .expect("setup");
        }
        let stmt = cell["stmt"].as_str().expect("stmt");
        let stmt = if cell["prepare"] == true {
            format!("PREPARE w_{} AS {stmt}", cell["id"].as_str().expect("id"))
        } else {
            stmt.to_string()
        };
        self.batches(&stmt).await?;
        let table = cell["table"].as_str().expect("table");
        let batches = self.batches(&format!("SELECT * FROM {table}")).await?;
        let width = batches.first().map_or(0, RecordBatch::num_columns);
        let names: Vec<String> = batches.first().map_or_else(Vec::new, |batch| {
            (0..width)
                .map(|index| batch.schema_ref().field(index).name().clone())
                .collect()
        });
        projected(&batches, &names).ok_or_else(|| "readback columns".to_string())
    }
}

fn expected_rows(expect: &Value, key: &str) -> Vec<Value> {
    sorted(expect[key].as_array().cloned().unwrap_or_default())
}

fn query_verdict(cell: &Value, outcome: &Result<Vec<RecordBatch>, String>) -> bool {
    let expect = &cell["expect"];
    match (expect["kind"].as_str(), outcome) {
        (Some("rows"), Ok(batches)) => {
            let columns: Vec<String> = expect["columns"]
                .as_array()
                .expect("columns")
                .iter()
                .map(|name| name.as_str().expect("column").to_string())
                .collect();
            projected(batches, &columns) == Some(expected_rows(expect, "rows"))
        }
        (Some("error"), Err(_)) => true,
        _ => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_sql_door_family_cell_pairs_struct_fields_by_position_as_spark_does() {
    let mut lines = CELLS.lines();
    let header: Value = serde_json::from_str(lines.next().expect("header")).expect("header");
    let cells: Vec<Value> = lines
        .map(|line| serde_json::from_str(line).expect("cell"))
        .collect();
    assert_eq!(cells.len(), 2426);
    let mut failures = Vec::new();
    let mut seen = 0;
    for case_sensitive in [false, true] {
        let door = Door::new(&header["views"], case_sensitive).await;
        let case = if case_sensitive {
            "sensitive"
        } else {
            "insensitive"
        };
        for cell in &cells {
            let id = cell["id"].as_str().expect("id");
            let known = cell["known"].as_str().unwrap_or_default();
            if cell["kind"] == "dml" {
                if case_sensitive {
                    continue;
                }
                seen += 1;
                let outcome = door.write(cell).await;
                let want = expected_rows(&cell["expect"], "after");
                match outcome {
                    Err(error) if error == PANICKED => failures.push(format!("{id}: {PANICKED}")),
                    Ok(rows) if rows != want => {
                        failures.push(format!("{id}: wrote {rows:?}, want {want:?}"));
                    }
                    Err(error) if known.is_empty() => failures.push(format!("{id}: {error}")),
                    _ => {}
                }
                continue;
            }
            if cell["door"] != "sql" || cell["case"] != case {
                continue;
            }
            seen += 1;
            let outcome = door.query(cell).await;
            let both_rows = outcome.is_ok() && cell["expect"]["kind"] == "rows";
            if outcome
                .as_ref()
                .err()
                .is_some_and(|error| error == PANICKED)
            {
                failures.push(format!("{id}: {PANICKED}"));
            } else if (known.is_empty() || both_rows) && !query_verdict(cell, &outcome) {
                failures.push(format!(
                    "{id} [{} {} {}]: got {:?}, want {}",
                    cell["seat"],
                    cell["shape"],
                    cell["jt"],
                    outcome.map(|batches| projected(
                        &batches,
                        &batches
                            .first()
                            .map(|batch| batch
                                .schema_ref()
                                .fields()
                                .iter()
                                .map(|field| field.name().clone())
                                .collect::<Vec<_>>())
                            .unwrap_or_default()
                    )),
                    cell["expect"]
                ));
            }
        }
    }
    assert_eq!(seen, 1231);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
