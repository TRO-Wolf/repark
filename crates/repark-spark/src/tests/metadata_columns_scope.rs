use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::ReparkSession;
use tempfile::TempDir;

use super::metadata_columns_deleted::{
    MOR, batches, field_names, i64s, pairs_i64_bool, seed, session, strs, triples_i64,
};

const EXISTS_ICEBERG: &str = "EXISTS (SELECT 1 FROM ice.ns.t WHERE _spec_id = 0)";

async fn seed_scope_tables(session: &ReparkSession) {
    seed(session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
            Field::new("cat", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["b", "c"])),
            Arc::new(StringArray::from(vec!["y", "x"])),
            Arc::new(StringArray::from(vec!["e2", "e3"])),
        ],
    )
    .unwrap();
    session.context().register_batch("tv", batch).unwrap();
}

fn tv_rows() -> Vec<(i64, String, String)> {
    vec![
        (2, "b".to_string(), "y".to_string()),
        (3, "c".to_string(), "x".to_string()),
    ]
}

fn iceberg_rows() -> Vec<(i64, String, String)> {
    vec![
        (2, "b".to_string(), "y".to_string()),
        (3, "c".to_string(), "x".to_string()),
        (4, "d".to_string(), "x".to_string()),
    ]
}

#[tokio::test]
async fn wildcard_over_a_plain_relation_keeps_its_own_columns() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_scope_tables(&session).await;
    for (row, sql) in [
        (
            "S1",
            format!("SELECT * FROM tv t WHERE {EXISTS_ICEBERG} ORDER BY id"),
        ),
        (
            "S2",
            format!("SELECT t.* FROM tv t WHERE {EXISTS_ICEBERG} ORDER BY id"),
        ),
        (
            "S4",
            format!(
                "WITH t AS (SELECT id, data, cat, extra FROM tv) SELECT * FROM t \
                 WHERE {EXISTS_ICEBERG} ORDER BY id"
            ),
        ),
        (
            "S4B",
            format!(
                "WITH t AS (SELECT id, data, cat, extra FROM tv) SELECT t.* FROM t \
                 WHERE {EXISTS_ICEBERG} ORDER BY id"
            ),
        ),
    ] {
        let rows = batches(&session, &sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "data", "cat", "extra"],
            "{row}: {sql}"
        );
        assert_eq!(triples_i64(&rows), tv_rows(), "{row}: {sql}");
        assert_eq!(strs(&rows, 3), vec!["e2", "e3"], "{row}: {sql}");
    }
    for (row, sql) in [
        (
            "S3",
            "SELECT x.* FROM (SELECT id, data, 'e' AS extra FROM tv) x \
             WHERE EXISTS (SELECT 1 FROM ice.ns.t x WHERE x._spec_id = 0) ORDER BY id"
                .to_string(),
        ),
        (
            "S3B",
            format!(
                "SELECT t.* FROM (SELECT id, data, 'e' AS extra FROM tv) t \
                 WHERE {EXISTS_ICEBERG} ORDER BY id"
            ),
        ),
        (
            "S3C",
            format!(
                "SELECT * FROM (SELECT id, data, 'e' AS extra FROM tv) t \
                 WHERE {EXISTS_ICEBERG} ORDER BY id"
            ),
        ),
    ] {
        let rows = batches(&session, &sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "data", "extra"],
            "{row}: {sql}"
        );
        assert_eq!(
            triples_i64(&rows),
            vec![
                (2, "b".to_string(), "e".to_string()),
                (3, "c".to_string(), "e".to_string()),
            ],
            "{row}: {sql}"
        );
    }
    let sql =
        format!("SELECT * FROM tv t JOIN tv u ON t.id = u.id WHERE {EXISTS_ICEBERG} ORDER BY t.id");
    let rows = batches(&session, &sql).await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "data", "cat", "extra", "id", "data", "cat", "extra"],
        "S6"
    );
    assert_eq!(triples_i64(&rows), tv_rows(), "S6");
    assert_eq!(strs(&rows, 3), vec!["e2", "e3"], "S6");
    assert_eq!(i64s(&rows, 4), vec![2, 3], "S6");
    assert_eq!(strs(&rows, 5), vec!["b", "c"], "S6");
    assert_eq!(strs(&rows, 6), vec!["y", "x"], "S6");
    assert_eq!(strs(&rows, 7), vec!["e2", "e3"], "S6");
}

#[tokio::test]
async fn wildcard_over_the_iceberg_relation_ignores_a_plain_namesake() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_scope_tables(&session).await;
    for (row, sql) in [
        (
            "S5",
            "SELECT * FROM ice.ns.t t WHERE t._spec_id = 0 \
             AND EXISTS (SELECT * FROM tv t WHERE t.id = 2) ORDER BY id",
        ),
        (
            "S5B",
            "SELECT t.* FROM ice.ns.t t WHERE t._spec_id = 0 \
             AND EXISTS (SELECT t.* FROM tv t WHERE t.id = 2) ORDER BY id",
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "data", "cat"],
            "{row}: {sql}"
        );
        assert_eq!(triples_i64(&rows), iceberg_rows(), "{row}: {sql}");
    }
    let rows = batches(
        &session,
        "SELECT t.*, t._pos AS p FROM ice.ns.t t ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "data", "cat", "p"], "S7");
    assert_eq!(triples_i64(&rows), iceberg_rows(), "S7");
    assert_eq!(i64s(&rows, 3), vec![0, 0, 1], "S7");
    let rows = batches(&session, "SELECT id, _deleted FROM ice.ns.t ORDER BY id").await;
    assert_eq!(field_names(&rows), vec!["id", "_deleted"], "S8");
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(1, true), (2, false), (3, false), (4, false)],
        "S8"
    );
}
