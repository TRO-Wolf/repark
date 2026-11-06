use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::common::ScalarValue;
use futures::FutureExt;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use serde_json::Value;
use tempfile::TempDir;

const CELLS: &str = include_str!("../../../python/repark/tests/sql/data/struct_domain_cells.jsonl");
const FIXTURES: &[(&str, [&str; 3])] = &[
    (
        "W",
        [
            "CREATE TABLE {t} (id INT, v INT, i INT, l BIGINT, d DOUBLE, f FLOAT, c DECIMAL(12,2), c5 DECIMAL(5,2), c7 DECIMAL(7,2), dt DATE, ts TIMESTAMP, s STRING, b BOOLEAN, m MAP<STRING, INT>, st STRUCT<x: INT>, sl STRUCT<x: BIGINT>, ss STRUCT<x: STRING>, sy STRUCT<y: INT>, ai ARRAY<STRUCT<x: INT>>, mi MAP<STRING, STRUCT<x: INT>>) USING iceberg TBLPROPERTIES ('format-version'='2')",
            "INSERT INTO {t} VALUES (1, 1, 1, CAST(1 AS BIGINT), CAST(1 AS DOUBLE), CAST(1 AS FLOAT), CAST(1 AS DECIMAL(12,2)), CAST(1 AS DECIMAL(5,2)), CAST(1 AS DECIMAL(7,2)), DATE'2020-01-01', TIMESTAMP'2020-01-01 00:00:00', '1', true, map('x', 1), named_struct('x', 1), named_struct('x', CAST(1 AS BIGINT)), named_struct('x', '1'), named_struct('y', 1), array(named_struct('x', 1)), map('k', named_struct('x', 1))), (2, 2, 2, CAST(2 AS BIGINT), CAST(2 AS DOUBLE), CAST(2 AS FLOAT), CAST(2 AS DECIMAL(12,2)), CAST(2 AS DECIMAL(5,2)), CAST(2 AS DECIMAL(7,2)), DATE'2020-01-02', TIMESTAMP'2020-01-02 00:00:00', '2', false, map('x', 2), named_struct('x', 2), named_struct('x', CAST(2 AS BIGINT)), named_struct('x', '2'), named_struct('y', 2), array(named_struct('x', 2)), map('k', named_struct('x', 2)))",
            "SELECT id, v FROM {t}",
        ],
    ),
    (
        "VI",
        [
            "CREATE TABLE {t} (id INT, st STRUCT<x: INT>) USING iceberg TBLPROPERTIES ('format-version'='2')",
            "INSERT INTO {t} VALUES (1, named_struct('x', 1)), (2, named_struct('x', 2))",
            "SELECT id, st FROM {t}",
        ],
    ),
    (
        "VL",
        [
            "CREATE TABLE {t} (id INT, st STRUCT<x: BIGINT>) USING iceberg TBLPROPERTIES ('format-version'='2')",
            "INSERT INTO {t} VALUES (1, named_struct('x', CAST(1 AS BIGINT))), (2, named_struct('x', CAST(2 AS BIGINT)))",
            "SELECT id, st FROM {t}",
        ],
    ),
];
const PANICKED: &str = "the engine panicked";
const WRITES: [&str; 4] = ["DELETE", "UPDATE", "MERGE", "INSERT"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    Card(&'static str),
    OracleDefect,
    CannotPlan(&'static str),
}

const C1: Reason = Reason::Card("C1: a DELETE WHERE struct field chain refuses");
const UNION_STRING: Reason = Reason::Card("UNION widening of a STRING field to a number");
const ORACLE: Reason = Reason::OracleDefect;
const PLANNER_TYPES: Reason =
    Reason::CannotPlan("DataFusion's SQL planner types the comparison before the analyzer");
const UNION_ARITY: Reason =
    Reason::CannotPlan("DataFusion refuses a UNION of different struct arity while planning");
const UNION_INTO_WIDER: Reason =
    Reason::CannotPlan("the Iceberg provider cannot cast a UNION input into a wider struct");

const DIFFERS: &[(&str, Reason)] = &[
    ("a_select_bare_int_boolean_diff", PLANNER_TYPES),
    ("a_select_bare_int_boolean_same", PLANNER_TYPES),
    ("a_select_qualified_int_boolean_diff", PLANNER_TYPES),
    ("a_select_qualified_int_boolean_same", PLANNER_TYPES),
    ("a_select_nested_int_boolean_diff", PLANNER_TYPES),
    ("a_select_nested_int_boolean_same", PLANNER_TYPES),
    ("a_select_paren_int_boolean_diff", PLANNER_TYPES),
    ("a_select_paren_int_boolean_same", PLANNER_TYPES),
    ("a_delete_subscript_int_int_diff", C1),
    ("a_delete_subscript_int_int_same", C1),
    ("a_delete_subscript_int_bigint_same", C1),
    ("a_delete_subscript_int_double_same", C1),
    ("a_delete_subscript_int_dec12_same", C1),
    ("a_select_subscript_int_boolean_diff", PLANNER_TYPES),
    ("a_select_subscript_int_boolean_same", PLANNER_TYPES),
    ("a_select_dot_int_boolean_diff", PLANNER_TYPES),
    ("a_select_dot_int_boolean_same", PLANNER_TYPES),
    ("a_select_literal_int_boolean_diff", PLANNER_TYPES),
    ("a_select_literal_int_boolean_same", PLANNER_TYPES),
    ("a_select_expression_int_boolean_diff", PLANNER_TYPES),
    ("a_select_expression_int_boolean_same", PLANNER_TYPES),
    ("a_select_alias_int_boolean_diff", PLANNER_TYPES),
    ("a_select_alias_int_boolean_same", PLANNER_TYPES),
    ("b_union_all_k4_string", UNION_STRING),
    ("c_insert_union_all_vl_int_named", UNION_INTO_WIDER),
    ("c_insert_union_all_vl_int_alias_x", UNION_INTO_WIDER),
    ("c_insert_union_all_vi_string_x", UNION_STRING),
    ("c_insert_union_all_vl_string_x", UNION_STRING),
    ("c_insert_union_all_vi_arity", UNION_ARITY),
    ("c_insert_union_all_vl_arity", UNION_ARITY),
    ("c_insert_union_all_vl_literal", UNION_INTO_WIDER),
    ("c_insert_union_all_vl_int_y", UNION_INTO_WIDER),
    ("c_insert_union_all_vl_null_y", UNION_INTO_WIDER),
    ("c_insert_union_all_vl_column_x", UNION_INTO_WIDER),
    ("c_insert_union_all_vl_column_y", UNION_INTO_WIDER),
    ("d_union_all_struct_in_struct_3", UNION_STRING),
    ("d_delete_struct_in_array_10", ORACLE),
    ("d_delete_literal_vs_column_16", ORACLE),
    ("d_union_all_two_columns_22", UNION_STRING),
];

struct Outcome {
    kind: &'static str,
    answer: Value,
    readback: Value,
    seed: Value,
}

struct Grid {
    session: ReparkSession,
    wide: String,
    count: usize,
    _warehouse: TempDir,
}

fn fixture(name: &str) -> [&'static str; 3] {
    FIXTURES
        .iter()
        .find(|(fixture, _)| *fixture == name)
        .map(|(_, sql)| *sql)
        .expect("fixture")
}

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

fn sorted(mut rows: Vec<Value>) -> Value {
    rows.sort_by_key(ToString::to_string);
    Value::Array(rows)
}

fn sorted_value(value: &Value) -> Value {
    value
        .as_array()
        .map_or_else(|| value.clone(), |rows| sorted(rows.clone()))
}

impl Grid {
    async fn new() -> Self {
        let session = ReparkSession::builder()
            .with_sql_dialect(Arc::new(SparkDialect))
            .with_extension(Arc::new(SparkExtension))
            .build()
            .expect("session");
        let warehouse = TempDir::new().expect("warehouse");
        session
            .register_memory_catalog("hd", warehouse.path().to_str().expect("path"))
            .await
            .expect("catalog");
        let mut grid = Self {
            session,
            wide: String::new(),
            count: 0,
            _warehouse: warehouse,
        };
        grid.rows("CREATE NAMESPACE hd.sd")
            .await
            .expect("namespace");
        grid.wide = grid.fresh("W").await;
        grid
    }

    async fn rows(&self, sql: &str) -> Result<Value, String> {
        let run = async {
            let frame = self
                .session
                .sql(sql)
                .await
                .map_err(|error| error.to_string())?;
            let batches = frame.collect().await.map_err(|error| error.to_string())?;
            let mut rows = Vec::new();
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    rows.push(Value::Array(
                        batch
                            .columns()
                            .iter()
                            .map(|column| json_of(column, row))
                            .collect(),
                    ));
                }
            }
            Ok(sorted(rows))
        };
        AssertUnwindSafe(run)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(PANICKED.to_string()))
    }

    async fn fresh(&mut self, name: &str) -> String {
        self.count += 1;
        let table = format!("hd.sd.t{}", self.count);
        let [create, seed, _] = fixture(name);
        self.rows(&create.replace("{t}", &table))
            .await
            .expect("create");
        self.rows(&seed.replace("{t}", &table)).await.expect("seed");
        table
    }

    async fn run(&mut self, seat: &str, stmt: &str) -> Outcome {
        let mut stmt = stmt.replace("{W}", &self.wide);
        let mut target = None;
        if let Some(name) = ["T", "VI", "VL"]
            .into_iter()
            .find(|name| stmt.contains(&format!("{{{name}}}")))
        {
            let fixture_name = if name == "T" { "W" } else { name };
            let table = self.fresh(fixture_name).await;
            stmt = stmt.replace(&format!("{{{name}}}"), &table);
            target = Some((table, fixture(fixture_name)[2]));
        }
        let readback = |table: &str, sql: &str| sql.replace("{t}", table);
        let seed = match &target {
            Some((table, sql)) => self
                .rows(&readback(table, sql))
                .await
                .expect("seed readback"),
            None => Value::Null,
        };
        let result = if seat == "view" {
            match self.rows(&stmt).await {
                Ok(_) => self.rows("SELECT * FROM sdv").await,
                Err(error) => Err(error),
            }
        } else {
            self.rows(&stmt).await
        };
        let write = WRITES.iter().any(|head| stmt.starts_with(head));
        let (kind, answer) = match result {
            Ok(_) if write => ("ok", Value::String("ok".to_string())),
            Ok(rows) => ("rows", rows),
            Err(error) => ("error", Value::String(error)),
        };
        let readback = match &target {
            Some((table, sql)) => self.rows(&readback(table, sql)).await.expect("readback"),
            None => Value::Null,
        };
        Outcome {
            kind,
            answer,
            readback,
            seed,
        }
    }
}

fn spark_class(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_' || c == '.')
}

fn matches_spark(cell: &Value, outcome: &Outcome) -> bool {
    let spark = &cell["spark"];
    if cell["spark_kind"] != outcome.kind || sorted_value(&cell["readback"]) != outcome.readback {
        return false;
    }
    match outcome.kind {
        "rows" => sorted_value(spark) == outcome.answer,
        "error" => {
            let want = spark.as_str().unwrap_or_default();
            let got = outcome.answer.as_str().unwrap_or_default();
            spark_class(want) && got.contains(&format!("[{want}]"))
        }
        _ => true,
    }
}

#[tokio::test]
async fn every_struct_domain_cell_answers_and_writes_as_spark_does() {
    let differs: HashMap<&str, Reason> = DIFFERS.iter().copied().collect();
    let cells: Vec<Value> = CELLS
        .lines()
        .map(|line| serde_json::from_str(line).expect("cell"))
        .collect();
    assert_eq!(cells.len(), 1031);
    let keys: Vec<&str> = cells
        .iter()
        .filter_map(|cell| cell["key"].as_str())
        .collect();
    assert!(differs.keys().all(|key| keys.contains(key)));
    let mut grid = Grid::new().await;
    let mut mismatches = Vec::new();
    for cell in &cells {
        let key = cell["key"].as_str().expect("key");
        let seat = cell["seat"].as_str().expect("seat");
        if seat.starts_with("df_") {
            continue;
        }
        let outcome = grid.run(seat, cell["stmt"].as_str().expect("stmt")).await;
        if outcome.answer == Value::String(PANICKED.to_string()) {
            mismatches.push(format!("{key}: {PANICKED}"));
        }
        let same = matches_spark(cell, &outcome);
        match differs.get(key) {
            None if !same => mismatches.push(format!(
                "{key}: {} {} readback {}",
                outcome.kind, outcome.answer, outcome.readback
            )),
            Some(reason) if same => {
                mismatches.push(format!("{key} now matches Spark; drop {reason:?}"));
            }
            Some(Reason::CannotPlan(why))
                if outcome.kind != "error" || outcome.readback != outcome.seed =>
            {
                mismatches.push(format!("{key} must refuse and write nothing ({why})"));
            }
            _ => {}
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
