use std::collections::{BTreeMap, HashMap};
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
    include_str!("../../../python/repark/tests/sql/data/struct_compare_by_position_cells.jsonl");
const PANICKED: &str = "PANIC";
const WRITES: [&str; 4] = ["DELETE", "UPDATE", "MERGE", "INSERT"];

const STRUCT_SETOP: &str = "unit C (#1283) refuses a reordered UNION ALL; not this unit";
const PARSE: &str = "PREPARE refuses without PARSE_SYNTAX_ERROR (SPARK-DOOR-PREPARE-PARSE-CLASS-1)";
const PLANNER_CLASS: &str =
    "a SELECT-list struct clash refuses without Spark's class (STRUCT-COMPARE-PLANNER-CLASS-1)";
const MAP_DML: &str = "a map subscript in a DML predicate cannot plan (STRUCT-MAP-SUBSCRIPT-DML-1)";

fn known(key: &str) -> Option<&'static str> {
    KNOWN
        .iter()
        .find(|(cell, _)| *cell == key)
        .map(|(_, reason)| *reason)
}

const KNOWN: &[(&str, &str)] = &[
    ("01147", PLANNER_CLASS),
    ("01150", PLANNER_CLASS),
    ("01166", PLANNER_CLASS),
    ("01169", PLANNER_CLASS),
    ("01185", PLANNER_CLASS),
    ("01188", PLANNER_CLASS),
    ("01204", PLANNER_CLASS),
    ("01207", PLANNER_CLASS),
    ("01223", PLANNER_CLASS),
    ("01226", PLANNER_CLASS),
    ("01242", PLANNER_CLASS),
    ("01245", PLANNER_CLASS),
    ("01261", PLANNER_CLASS),
    ("01264", PLANNER_CLASS),
    ("02491", MAP_DML),
    ("02492", MAP_DML),
    ("02510", MAP_DML),
    ("02511", MAP_DML),
    ("02529", MAP_DML),
    ("02530", MAP_DML),
    ("02548", MAP_DML),
    ("02549", MAP_DML),
    ("02567", MAP_DML),
    ("02568", MAP_DML),
    ("02586", MAP_DML),
    ("02587", MAP_DML),
    ("02605", MAP_DML),
    ("02606", MAP_DML),
    ("02624", MAP_DML),
    ("02625", MAP_DML),
    ("02643", MAP_DML),
    ("02644", MAP_DML),
    ("02662", MAP_DML),
    ("02663", MAP_DML),
    ("03969", PLANNER_CLASS),
    ("03972", PLANNER_CLASS),
    ("03988", PLANNER_CLASS),
    ("03991", PLANNER_CLASS),
    ("04007", PLANNER_CLASS),
    ("04010", PLANNER_CLASS),
    ("04045", PLANNER_CLASS),
    ("04048", PLANNER_CLASS),
    ("04064", PLANNER_CLASS),
    ("04067", PLANNER_CLASS),
    ("04083", PLANNER_CLASS),
    ("04086", PLANNER_CLASS),
    ("04121", PLANNER_CLASS),
    ("04124", PLANNER_CLASS),
    ("04140", PLANNER_CLASS),
    ("04143", PLANNER_CLASS),
    ("04159", PLANNER_CLASS),
    ("04162", PLANNER_CLASS),
    ("04376", PARSE),
    ("04377", STRUCT_SETOP),
    ("04378", STRUCT_SETOP),
    ("04387", STRUCT_SETOP),
];

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
        ScalarValue::Float64(Some(value)) => Value::from(*value),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Value::String(value.clone()),
        ScalarValue::Struct(array) => Value::Object(
            array
                .fields()
                .iter()
                .zip(array.columns())
                .map(|(field, column)| (field.name().clone(), json_of(column, 0)))
                .collect(),
        ),
        ScalarValue::List(array) => json_rows(&array.value(0)),
        ScalarValue::LargeList(array) => json_rows(&array.value(0)),
        ScalarValue::Map(array) => {
            let entries = array.value(0);
            Value::Object(
                (0..entries.len())
                    .map(|row| {
                        let key = match json_of(entries.column(0), row) {
                            Value::String(key) => key,
                            other => other.to_string(),
                        };
                        (key, json_of(entries.column(1), row))
                    })
                    .collect(),
            )
        }
        other => Value::String(other.to_string()),
    }
}

fn sorted(value: &Value) -> Value {
    let mut rows = value.as_array().cloned().unwrap_or_default();
    rows.sort_by_key(ToString::to_string);
    Value::Array(rows)
}

fn rows_of(batches: &[RecordBatch], width: usize) -> Value {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(Value::Array(
                batch
                    .columns()
                    .iter()
                    .take(width)
                    .map(|column| json_of(column, row))
                    .collect(),
            ));
        }
    }
    sorted(&Value::Array(rows))
}

struct Grid {
    session: ReparkSession,
    fixtures: BTreeMap<String, [String; 3]>,
    wide: String,
    count: usize,
    _warehouse: TempDir,
}

impl Grid {
    async fn new(fixtures: &Value, case_sensitive: bool, ansi: bool) -> Self {
        let session = ReparkSession::builder()
            .with_sql_dialect(Arc::new(SparkDialect))
            .with_extension(Arc::new(SparkExtension))
            .config("spark.sql.caseSensitive", case_sensitive.to_string())
            .config("spark.sql.ansi.enabled", ansi.to_string())
            .config("spark.sql.session.timeZone", "UTC")
            .build()
            .expect("session");
        let warehouse = TempDir::new().expect("warehouse");
        session
            .register_memory_catalog("hd", warehouse.path().to_str().expect("path"))
            .await
            .expect("catalog");
        let fixtures = fixtures
            .as_object()
            .expect("fixtures")
            .iter()
            .map(|(name, sql)| {
                let part = |index: usize| sql[index].as_str().expect("fixture sql").to_string();
                (name.clone(), [part(0), part(1), part(2)])
            })
            .collect();
        let mut grid = Self {
            session,
            fixtures,
            wide: String::new(),
            count: 0,
            _warehouse: warehouse,
        };
        grid.batches("CREATE NAMESPACE hd.sd")
            .await
            .expect("namespace");
        grid.wide = grid.fresh("W").await;
        grid
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

    async fn fresh(&mut self, name: &str) -> String {
        self.count += 1;
        let table = format!("hd.sd.t{}", self.count);
        let [create, seed, _] = self.fixtures[name].clone();
        self.batches(&create.replace("{t}", &table))
            .await
            .expect("create");
        self.batches(&seed.replace("{t}", &table))
            .await
            .expect("seed");
        table
    }

    async fn run(&mut self, seat: &str, stmt: &str) -> (Result<Value, String>, Option<Value>) {
        let mut stmt = stmt.replace("{W}", &self.wide);
        let mut target = None;
        for name in ["T", "T1", "L", "R", "U"] {
            let marker = format!("{{{name}}}");
            if stmt.contains(&marker) {
                let fixture = if name == "T" { "W" } else { name };
                let table = self.fresh(fixture).await;
                stmt = stmt.replace(&marker, &table);
                if name == "T" || name == "T1" {
                    target = Some((table, self.fixtures[fixture][2].clone()));
                }
            }
        }
        let result = match seat {
            "view" => match self.batches(&stmt).await {
                Ok(_) => self.batches("SELECT * FROM sdv").await,
                Err(error) => Err(error),
            },
            "df_temp_view" => self.temp_view(&stmt).await,
            _ => self.batches(&stmt).await,
        };
        let write = WRITES.iter().any(|head| stmt.starts_with(head));
        let answer = result.map(|batches| {
            if write {
                Value::String("ok".to_string())
            } else {
                rows_of(&batches, usize::MAX)
            }
        });
        let readback = match target {
            Some((table, sql)) => Some(
                self.batches(&sql.replace("{t}", &table))
                    .await
                    .map(|batches| rows_of(&batches, usize::MAX))
                    .expect("readback"),
            ),
            None => None,
        };
        (answer, readback)
    }

    async fn temp_view(&self, stmt: &str) -> Result<Vec<RecordBatch>, String> {
        let run = async {
            let frame = self
                .session
                .sql(stmt)
                .await
                .map_err(|error| error.to_string())?;
            let context = self.session.context();
            let _ = context.deregister_table("dv");
            context
                .register_table("dv", frame.into_view())
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(())
        };
        AssertUnwindSafe(run)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(PANICKED.to_string()))?;
        self.batches("SELECT * FROM dv").await
    }
}

fn verdict(expect: &Value, answer: &Result<Value, String>, readback: Option<&Value>) -> bool {
    let after = || readback.cloned().map(|rows| sorted(&rows)) == Some(sorted(&expect["after"]));
    match (expect["kind"].as_str().unwrap_or_default(), answer) {
        ("rows", Ok(rows)) => *rows == sorted(&expect["rows"]),
        ("error", Err(error)) => {
            let class = expect["class"].as_str().unwrap_or_default();
            !error.contains(PANICKED) && error.contains(&format!("[{class}]"))
        }
        ("ok" | "derived", Ok(_)) => after(),
        ("oracle-missing", outcome) => outcome.as_ref().err().is_none_or(|e| !e.contains(PANICKED)),
        _ => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_sql_door_cell_compares_structs_by_position_as_spark_does() {
    let mut lines = CELLS.lines();
    let header: Value = serde_json::from_str(lines.next().expect("header")).expect("header");
    let cells: Vec<Value> = lines
        .map(|line| serde_json::from_str(line).expect("cell"))
        .collect();
    assert_eq!(cells.len(), 4393);
    let mut groups: HashMap<(bool, bool), Vec<&Value>> = HashMap::new();
    for cell in cells.iter().filter(|cell| cell["door"] == "sql") {
        let key = (cell["case"] == "sensitive", cell["ansi"] != "off");
        groups.entry(key).or_default().push(cell);
    }
    assert_eq!(groups.len(), 3);
    let mut failures = Vec::new();
    let mut seen = 0;
    for ((case_sensitive, ansi), group) in groups {
        let mut grid = Grid::new(&header["fixtures"], case_sensitive, ansi).await;
        for cell in group {
            seen += 1;
            let key = cell["key"].as_str().expect("key");
            let seat = cell["seat"].as_str().expect("seat");
            let (answer, readback) = grid.run(seat, cell["stmt"].as_str().expect("stmt")).await;
            if answer.as_ref().err().is_some_and(|error| error == PANICKED) {
                failures.push(format!("{key}: {PANICKED}"));
                continue;
            }
            let same = verdict(&cell["expect"], &answer, readback.as_ref());
            match known(key) {
                None if !same => failures.push(format!(
                    "{key} [{seat} {}]: got {answer:?} readback {readback:?}, want {}",
                    cell["shape"], cell["expect"]
                )),
                Some(reason) if same => failures.push(format!("{key} now matches; drop {reason}")),
                Some(reason) if answer.is_ok() => {
                    failures.push(format!(
                        "{key} answers; KNOWN holds refusals only: {reason}"
                    ));
                }
                _ => {}
            }
        }
    }
    assert_eq!(seen, 2796);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
