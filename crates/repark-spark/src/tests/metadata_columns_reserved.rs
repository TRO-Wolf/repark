use tempfile::TempDir;

use super::metadata_columns_deleted::{
    MOR, batches, bools, field_names, i64s, pairs_i64_bool, pairs_i64_i32, pairs_i64_i64,
    pairs_i64_str, plan_error, run, session, strs,
};
use repark_core::ReparkSession;

async fn seed_user_columns(session: &ReparkSession, table: &str, columns: &[&str], props: &str) {
    let defs = columns
        .iter()
        .map(|column| format!(", {column} STRING"))
        .collect::<Vec<_>>()
        .concat();
    run(
        session,
        &format!(
            "CREATE TABLE {table} (id BIGINT{defs}) USING iceberg \
             TBLPROPERTIES ('format-version' = '2'{props})"
        ),
    )
    .await;
    let rows: Vec<String> = (1..=3)
        .map(|id| {
            let cells = vec![format!(", 'u{id}'"); columns.len()].concat();
            format!("({id}{cells})")
        })
        .collect();
    run(
        session,
        &format!("INSERT INTO {table} VALUES {}", rows.join(", ")),
    )
    .await;
}

fn reserved_name_collision(names: &str) -> String {
    format!(
        "Error during planning: Table column names conflict with names reserved for Iceberg \
         metadata columns: [{names}]. Please, use ALTER TABLE statements to rename the \
         conflicting table columns."
    )
}

fn pairs_str_bool(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(String, bool)> {
    strs(batches, 0)
        .into_iter()
        .zip(bools(batches, 1))
        .collect()
}

#[tokio::test]
async fn user_deleted_column_collision_refuses_like_spark() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    for (table, props) in [("ice.ns.uc", ""), ("ice.ns.um", MOR)] {
        seed_user_columns(&session, table, &["_deleted"], props).await;
        run(&session, &format!("DELETE FROM {table} WHERE id = 1")).await;
        for sql in [
            format!("SELECT id, _deleted FROM {table} ORDER BY id"),
            format!("SELECT id, _spec_id, _deleted FROM {table} ORDER BY id"),
            format!("SELECT t._deleted FROM {table} t ORDER BY 1"),
            format!("SELECT id FROM {table} WHERE _deleted = 'u2'"),
        ] {
            assert_eq!(
                plan_error(&session, &sql).await,
                reserved_name_collision("_deleted"),
                "R-MC-RESERVED-NAME: {sql}"
            );
        }
        let rows = batches(&session, &format!("SELECT * FROM {table} ORDER BY id")).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "_deleted"],
            "R-MC-RESERVED-NAME-SCAN"
        );
        assert_eq!(
            pairs_i64_str(&rows),
            vec![(2, "u2".to_string()), (3, "u3".to_string())],
            "R-MC-RESERVED-NAME-SCAN"
        );
    }
}

#[tokio::test]
async fn every_served_metadata_name_collision_refuses() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    for (index, column) in repark_iceberg::catalog::METADATA_COLUMN_NAMES
        .iter()
        .enumerate()
    {
        let table = format!("ice.ns.c{index}");
        seed_user_columns(&session, &table, &[column], "").await;
        assert_eq!(
            plan_error(&session, &format!("SELECT id, {column} FROM {table}")).await,
            reserved_name_collision(column),
            "R-MC-RESERVED-NAME: {column}"
        );
    }
    seed_user_columns(&session, "ice.ns.two", &["_pos", "_file"], "").await;
    seed_user_columns(&session, "ice.ns.rev", &["_file", "_pos"], "").await;
    seed_user_columns(
        &session,
        "ice.ns.tri",
        &["_spec_id", "_deleted", "_file"],
        "",
    )
    .await;
    for (sql, names) in [
        ("SELECT id, _file, _pos FROM ice.ns.two", "_pos, _file"),
        ("SELECT id, _pos, _file FROM ice.ns.two", "_pos, _file"),
        ("SELECT id, _file FROM ice.ns.two", "_file"),
        ("SELECT id, _pos, _file FROM ice.ns.rev", "_file, _pos"),
        ("SELECT id, _file, _pos FROM ice.ns.rev", "_file, _pos"),
        (
            "SELECT id, _file, _deleted, _spec_id FROM ice.ns.tri",
            "_spec_id, _deleted, _file",
        ),
        (
            "SELECT id, _deleted, _file FROM ice.ns.tri",
            "_deleted, _file",
        ),
    ] {
        assert_eq!(
            plan_error(&session, sql).await,
            reserved_name_collision(names),
            "R-MC-RESERVED-NAME-ORDER: {sql}"
        );
    }
}

#[tokio::test]
async fn reserved_name_collision_in_a_join() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.p (id BIGINT, data STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '2'{MOR})"
        ),
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.p VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    run(&session, "DELETE FROM ice.ns.p WHERE id = 1").await;
    seed_user_columns(&session, "ice.ns.uc", &["_deleted"], "").await;
    for sql in [
        "SELECT p.id, p._deleted FROM ice.ns.p p JOIN ice.ns.uc u ON p.id = u.id ORDER BY p.id",
        "SELECT p.id, p._deleted, u._deleted FROM ice.ns.p p JOIN ice.ns.uc u ON p.id = u.id \
         ORDER BY p.id",
        "SELECT u.id, u._deleted FROM ice.ns.uc u JOIN ice.ns.p p ON p.id = u.id ORDER BY u.id",
    ] {
        assert_eq!(
            plan_error(&session, sql).await,
            reserved_name_collision("_deleted"),
            "R-MC-RESERVED-NAME-JOIN: {sql}"
        );
    }
}

#[tokio::test]
async fn reserved_name_near_misses_still_answer() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_user_columns(&session, "ice.ns.near_deleted", &["deleted"], MOR).await;
    run(&session, "DELETE FROM ice.ns.near_deleted WHERE id = 1").await;
    let rows = batches(
        &session,
        "SELECT deleted, _deleted FROM ice.ns.near_deleted ORDER BY deleted",
    )
    .await;
    assert_eq!(
        pairs_str_bool(&rows),
        vec![
            ("u1".to_string(), true),
            ("u2".to_string(), false),
            ("u3".to_string(), false),
        ],
        "R-MC-RESERVED-NAME-NEAR"
    );
    for (index, column) in repark_iceberg::catalog::METADATA_COLUMN_NAMES
        .iter()
        .enumerate()
    {
        let table = format!("ice.ns.n{index}");
        run(
            &session,
            &format!(
                "CREATE TABLE {table} (id BIGINT, {column} STRING) USING iceberg \
                 TBLPROPERTIES ('format-version' = '2')"
            ),
        )
        .await;
        run(&session, &format!("INSERT INTO {table} VALUES (1, 'u1')")).await;
        if *column == "_spec_id" {
            let rows = batches(&session, &format!("SELECT id, _file FROM {table}")).await;
            assert_eq!(field_names(&rows), vec!["id", "_file"], "{column}");
            assert_eq!(i64s(&rows, 0), vec![1], "{column}");
            let files = strs(&rows, 1);
            assert_eq!(files.len(), 1, "{column}");
            assert!(files[0].ends_with(".parquet"), "{column}: {}", files[0]);
        } else {
            let rows = batches(&session, &format!("SELECT id, _spec_id FROM {table}")).await;
            assert_eq!(field_names(&rows), vec!["id", "_spec_id"], "{column}");
            assert_eq!(pairs_i64_i32(&rows), vec![(1, 0)], "{column}");
        }
    }
    seed_user_columns(&session, "ice.ns.near_two", &["_pos", "_file"], "").await;
    let rows = batches(
        &session,
        "SELECT id, _spec_id FROM ice.ns.near_two ORDER BY id",
    )
    .await;
    assert_eq!(
        pairs_i64_i32(&rows),
        vec![(1, 0), (2, 0), (3, 0)],
        "R-MC-RESERVED-NAME-NEAR"
    );
    let rows = batches(&session, "SELECT id FROM ice.ns.near_two ORDER BY id").await;
    assert_eq!(i64s(&rows, 0), vec![1, 2, 3], "R-MC-RESERVED-NAME-NEAR");
    seed_user_columns(&session, "ice.ns.near_file", &["_file"], "").await;
    let rows = batches(
        &session,
        "SELECT id, _deleted FROM ice.ns.near_file ORDER BY id",
    )
    .await;
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(1, false), (2, false), (3, false)],
        "R-MC-RESERVED-NAME-NEAR"
    );
    let rows = batches(
        &session,
        "SELECT id, _pos FROM ice.ns.near_file ORDER BY id",
    )
    .await;
    assert_eq!(
        pairs_i64_i64(&rows),
        vec![(1, 0), (2, 1), (3, 2)],
        "R-MC-RESERVED-NAME-NEAR"
    );
    let rows = batches(
        &session,
        "SELECT id FROM ice.ns.near_file WHERE _spec_id = 0 ORDER BY id",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![1, 2, 3], "R-MC-RESERVED-NAME-NEAR");
    let rows = batches(&session, "SELECT * FROM ice.ns.near_file ORDER BY id").await;
    assert_eq!(
        pairs_i64_str(&rows),
        vec![
            (1, "u1".to_string()),
            (2, "u2".to_string()),
            (3, "u3".to_string()),
        ],
        "R-MC-RESERVED-NAME-SCAN"
    );
}
