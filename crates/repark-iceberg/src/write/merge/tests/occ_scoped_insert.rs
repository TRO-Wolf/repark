use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use serde_json::{Map, Value};

use super::occ_scoped::{
    Layout, MODES, Row, VERSIONS, assert_validation_conflict, fixture, label, live_rows,
    merge_spec, racing_insert_row,
};
use crate::write::merge::{InsertAction, InsertClause, execute_merge};

const ON: &str = "t.k = 'a' AND t.k = s.k AND t.id = s.id";

type Keyed = (i64, &'static str);

const CELLS: [(&str, Keyed, Keyed); 4] = [
    ("same_key_in_on_partition", (1000, "a"), (1000, "a")),
    ("other_key_in_on_partition", (1000, "a"), (2000, "a")),
    ("other_partition", (1000, "a"), (1000, "b")),
    ("source_key_outside_on_partition", (1000, "b"), (1000, "b")),
];

fn spark_cells() -> Map<String, Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../python/repark-parity/fixtures/torture/data/ice_occ_scoped_1")
        .join("spark_occ_oracle3.json");
    let text = std::fs::read_to_string(&path).expect("Spark's NOT MATCHED INSERT recording");
    let mut root: Value = serde_json::from_str(&text).expect("the recording is JSON");
    match root["cells"].take() {
        Value::Object(cells) => cells,
        other => panic!("the recording's cells are not an object: {other}"),
    }
}

fn text<'a>(cell: &'a Value, key: &str) -> &'a str {
    cell[key]
        .as_str()
        .unwrap_or_else(|| panic!("`{key}` is text"))
}

fn spark_rows(cell: &Value) -> Vec<Row> {
    let rows = cell["rows_at_or_above_1000"]
        .as_array()
        .expect("recorded rows");
    rows.iter()
        .map(|row| {
            (
                row[0].as_i64().expect("id"),
                row[1].as_str().map(str::to_string),
                row[2].as_str().map(str::to_string),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_merge_that_inserts_answers_a_concurrent_append_as_spark_does() {
    let spark = spark_cells();
    for version in VERSIONS {
        for mode in MODES {
            for (name, (source_id, source_key), (insert_id, insert_key)) in CELLS {
                let cell = format!("{}/{name}", label(version, mode));
                let tag = format!(
                    "{}_{}_{name}",
                    format!("{version:?}").to_lowercase(),
                    mode.property().replace('-', "")
                );
                let recorded = &spark[&tag];
                let source = format!(
                    "(SELECT CAST({source_id} AS BIGINT) AS id, '{source_key}' AS k, 'merged' AS v)"
                );
                let statement = text(recorded, "merge");
                assert!(
                    statement.contains(&format!(
                        "CAST({source_id} AS BIGINT) AS id, '{source_key}' AS k"
                    )) && statement.contains(ON)
                        && statement.ends_with("WHEN NOT MATCHED THEN INSERT *"),
                    "{cell}: the pin drifted from Spark's statement {statement}"
                );
                assert!(
                    text(recorded, "concurrent_insert")
                        .ends_with(&format!("VALUES ({insert_id}, '{insert_key}', 'appended')")),
                    "{cell}: the pin drifted from Spark's concurrent INSERT"
                );
                let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
                fx.racing
                    .arm(racing_insert_row(&fx.ident, insert_id, insert_key));
                let mut spec = merge_spec(&fx.ident, &source, ON);
                spec.not_matched = vec![InsertClause {
                    predicate_sql: None,
                    action: InsertAction::All,
                }];
                let outcome = execute_merge(&SessionContext::new(), &fx.victim(), &spec).await;
                assert!(
                    fx.racing.fired(),
                    "{cell}: the race must land inside the commit"
                );
                let spark_outcome = text(recorded, "merge_outcome");
                match outcome {
                    Ok(()) => assert_eq!(
                        spark_outcome, "ok",
                        "{cell}: RePark committed, Spark did not"
                    ),
                    Err(error) => {
                        assert!(
                            spark_outcome.contains(
                                "Found conflicting files that can contain records matching"
                            ) && spark_outcome.contains("ref(name=\"k\") == \"a\""),
                            "{cell}: RePark aborted ({error}), Spark answered {spark_outcome}"
                        );
                        assert_validation_conflict(
                            &error,
                            "Found conflicting files that can contain records matching k = \"a\"",
                            &cell,
                        );
                    }
                }
                let rows = live_rows(&fx.inner, &fx.ident).await;
                let inserted: Vec<Row> = rows.iter().filter(|row| row.0 >= 1000).cloned().collect();
                assert_eq!(inserted, spark_rows(recorded), "{cell}");
                assert_eq!(
                    Some(rows.len() as u64),
                    recorded["count"].as_u64(),
                    "{cell}"
                );
            }
        }
    }
}
