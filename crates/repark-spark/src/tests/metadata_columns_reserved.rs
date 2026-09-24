use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use super::metadata_columns_deleted::{
    MOR, batches, bools, field_names, i32s, i64s, pairs_i64_bool, pairs_i64_i32, pairs_i64_i64,
    pairs_i64_str, plan_error, refusal, run, seed, session, strs, triples_i64,
};
use repark_core::{ErrorClass, ReparkSession};

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

async fn seed_values(session: &ReparkSession, table: &str, ddl: &str, values: &str) {
    run(
        session,
        &format!(
            "CREATE TABLE {table} ({ddl}) USING iceberg TBLPROPERTIES ('format-version' = '2')"
        ),
    )
    .await;
    run(session, &format!("INSERT INTO {table} VALUES {values}")).await;
}

fn parquet_files_under(root: &Path) -> Vec<String> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path.to_str().unwrap().to_string());
            }
        }
    }
    files.sort();
    files
}

async fn seed_position_tables(session: &ReparkSession) {
    seed_values(
        session,
        "ice.ns.ndel",
        "id BIGINT, _deleted STRING",
        "(1, 'u1'), (2, 'u2')",
    )
    .await;
    seed_values(
        session,
        "ice.ns.ndel2",
        "id BIGINT, _deleted STRING",
        "(2, 'u2'), (3, 'u3')",
    )
    .await;
    seed_values(
        session,
        "ice.ns.pl",
        "id BIGINT, v STRING",
        "(1, 'a'), (2, 'b')",
    )
    .await;
}

async fn assert_unread_collision_answers(
    session: &ReparkSession,
    wh: &TempDir,
    index: usize,
    column: &str,
) {
    let table = format!("ice.ns.n{index}");
    run(
        session,
        &format!(
            "CREATE TABLE {table} (id BIGINT, {column} STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '2')"
        ),
    )
    .await;
    run(session, &format!("INSERT INTO {table} VALUES (1, 'u1')")).await;
    if column == "_spec_id" {
        let rows = batches(session, &format!("SELECT id, _file FROM {table}")).await;
        assert_eq!(field_names(&rows), vec!["id", "_file"], "{column}");
        assert_eq!(i64s(&rows, 0), vec![1], "{column}");
        assert_eq!(
            strs(&rows, 1),
            parquet_files_under(&wh.path().join("ns").join(format!("n{index}"))),
            "{column}"
        );
    } else {
        let rows = batches(session, &format!("SELECT id, _spec_id FROM {table}")).await;
        assert_eq!(field_names(&rows), vec!["id", "_spec_id"], "{column}");
        assert_eq!(pairs_i64_i32(&rows), vec![(1, 0)], "{column}");
    }
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
    let rows = batches(
        &session,
        "SELECT p.id, p._deleted FROM ice.ns.p p JOIN ice.ns.uc u ON p.id = u.id ORDER BY p.id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "_deleted"], "J1");
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(1, true), (2, false), (3, false)],
        "J1"
    );
    let schema = rows[0].schema();
    assert_eq!(schema.fields()[1].data_type(), &DataType::Boolean, "J1");
    assert!(!schema.fields()[1].is_nullable(), "J1");
    for sql in [
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
        field_names(&rows),
        vec!["deleted", "_deleted"],
        "field names"
    );
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
        assert_unread_collision_answers(&session, &wh, index, column).await;
    }
    seed_user_columns(&session, "ice.ns.near_two", &["_pos", "_file"], "").await;
    let rows = batches(
        &session,
        "SELECT id, _spec_id FROM ice.ns.near_two ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "_spec_id"], "field names");
    assert_eq!(
        pairs_i64_i32(&rows),
        vec![(1, 0), (2, 0), (3, 0)],
        "R-MC-RESERVED-NAME-NEAR"
    );
    let rows = batches(&session, "SELECT id FROM ice.ns.near_two ORDER BY id").await;
    assert_eq!(field_names(&rows), vec!["id"], "field names");
    assert_eq!(i64s(&rows, 0), vec![1, 2, 3], "R-MC-RESERVED-NAME-NEAR");
    seed_user_columns(&session, "ice.ns.near_file", &["_file"], "").await;
    let rows = batches(
        &session,
        "SELECT id, _deleted FROM ice.ns.near_file ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "_deleted"], "field names");
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
    assert_eq!(field_names(&rows), vec!["id", "_pos"], "field names");
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
    assert_eq!(field_names(&rows), vec!["id"], "field names");
    assert_eq!(i64s(&rows, 0), vec![1, 2, 3], "R-MC-RESERVED-NAME-NEAR");
    let rows = batches(&session, "SELECT * FROM ice.ns.near_file ORDER BY id").await;
    assert_eq!(field_names(&rows), vec!["id", "_file"], "field names");
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

#[tokio::test]
async fn reserved_word_outside_a_user_column_read_answers() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_values(
        &session,
        "ice.ns.ndel",
        "id BIGINT, _deleted STRING",
        "(1, 'u1'), (2, 'u2')",
    )
    .await;
    seed_values(
        &session,
        "ice.ns.nfile",
        "id BIGINT, _file STRING",
        "(1, 'f1'), (2, 'f2')",
    )
    .await;
    seed_values(
        &session,
        "ice.ns.pl",
        "id BIGINT, v STRING",
        "(1, 'a'), (2, 'b')",
    )
    .await;
    for (row, sql, field) in [
        (
            "A1",
            "SELECT id AS _deleted FROM ice.ns.ndel ORDER BY id",
            "_deleted",
        ),
        (
            "A2",
            "SELECT id AS _file FROM ice.ns.nfile ORDER BY id",
            "_file",
        ),
        (
            "A4",
            "SELECT id AS _deleted FROM ice.ns.ndel ORDER BY _deleted",
            "_deleted",
        ),
        (
            "A5",
            "SELECT _deleted.id FROM ice.ns.ndel AS _deleted ORDER BY 1",
            "id",
        ),
        (
            "A6",
            "WITH _deleted AS (SELECT id FROM ice.ns.ndel) SELECT id FROM _deleted ORDER BY id",
            "id",
        ),
        (
            "A7",
            "SELECT id AS _deleted FROM ice.ns.pl ORDER BY id",
            "_deleted",
        ),
        (
            "A10",
            "SELECT x AS _deleted FROM (SELECT id AS x FROM ice.ns.ndel) ORDER BY 1",
            "_deleted",
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(field_names(&rows), vec![field], "{row}: {sql}");
        assert_eq!(i64s(&rows, 0), vec![1, 2], "{row}: {sql}");
    }
    let rows = batches(
        &session,
        "SELECT id, 7 AS _pos FROM ice.ns.ndel ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "_pos"], "A3");
    assert_eq!(pairs_i64_i32(&rows), vec![(1, 7), (2, 7)], "A3");
    let rows = batches(&session, "SELECT id, v AS _file FROM ice.ns.pl ORDER BY id").await;
    assert_eq!(field_names(&rows), vec!["id", "_file"], "A8");
    assert_eq!(
        pairs_i64_str(&rows),
        vec![(1, "a".to_string()), (2, "b".to_string())],
        "A8"
    );
    assert_eq!(
        plan_error(
            &session,
            "SELECT id, _deleted AS d FROM ice.ns.ndel ORDER BY id"
        )
        .await,
        reserved_name_collision("_deleted"),
        "A9"
    );
}

#[tokio::test]
async fn reserved_word_positions_follow_spark() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_values(
        &session,
        "ice.ns.ndel",
        "id BIGINT, _deleted STRING",
        "(1, 'u1'), (2, 'u2')",
    )
    .await;
    let rows = batches(
        &session,
        "SELECT id AS _deleted, count(*) AS c FROM ice.ns.ndel GROUP BY 1 ORDER BY 1",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["_deleted", "c"], "B1");
    assert_eq!(pairs_i64_i64(&rows), vec![(1, 1), (2, 1)], "B1");
    for (row, sql, field) in [
        (
            "B3",
            "SELECT _deleted FROM (SELECT id FROM ice.ns.ndel) t(_deleted) ORDER BY 1",
            "_deleted",
        ),
        (
            "B4",
            "SELECT id AS `_deleted` FROM ice.ns.ndel ORDER BY id",
            "_deleted",
        ),
        (
            "B8",
            "SELECT id AS _file FROM ice.ns.ndel ORDER BY id",
            "_file",
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(field_names(&rows), vec![field], "{row}: {sql}");
        assert_eq!(i64s(&rows, 0), vec![1, 2], "{row}: {sql}");
    }
    let rows = batches(
        &session,
        "SELECT s._deleted FROM (SELECT named_struct('_deleted', id) AS s FROM ice.ns.ndel) \
         ORDER BY 1",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["s[_deleted]"], "B7");
    assert_eq!(i64s(&rows, 0), vec![1, 2], "B7");
    let rows = batches(
        &session,
        "SELECT id, '_deleted' AS s FROM ice.ns.ndel ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "s"], "B5");
    assert_eq!(
        pairs_i64_str(&rows),
        vec![(1, "_deleted".to_string()), (2, "_deleted".to_string())],
        "B5"
    );
    let rows = batches(
        &session,
        "SELECT count(*) FROM ice.ns.ndel WHERE id > 0 AND _spec_id = 0",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["count(*)"], "B10");
    assert_eq!(i64s(&rows, 0), vec![2], "B10");
    assert_eq!(
        plan_error(&session, "SELECT _deleted(id) FROM ice.ns.ndel").await,
        "[UNRESOLVED_ROUTINE] Cannot resolve routine `_deleted` on search path [`system`.`builtin`, \
         `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; line 1 pos 7",
        "B6"
    );
    for (row, sql) in [
        ("B9", "SELECT _deleted FROM ice.ns.ndel AS t ORDER BY 1"),
        ("R3", "SELECT *, _spec_id FROM ice.ns.ndel ORDER BY id"),
    ] {
        assert_eq!(
            plan_error(&session, sql).await,
            reserved_name_collision("_deleted"),
            "{row}: {sql}"
        );
    }
    assert_eq!(
        plan_error(
            &session,
            "SELECT id AS _deleted, count(*) AS c FROM ice.ns.ndel GROUP BY _deleted ORDER BY 1",
        )
        .await,
        "Error during planning: Column in SELECT must be in GROUP BY or an aggregate function: \
         While expanding wildcard, column \"ndel.id\" must appear in the GROUP BY clause or must \
         be part of an aggregate function, currently only \"ndel._deleted, count(Int64(1))\" \
         appears in the SELECT clause satisfies this requirement",
        "B2"
    );
}

#[tokio::test]
async fn reserved_name_join_positions() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_position_tables(&session).await;
    for (row, sql, expected) in [
        (
            "J4",
            "SELECT id FROM ice.ns.ndel JOIN ice.ns.ndel2 USING (_deleted) ORDER BY id",
            "Error during planning: [AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: \
             [`ndel`.`id`, `ndel2`.`id`]. SQLSTATE: 42704"
                .to_string(),
        ),
        (
            "J4B",
            "SELECT ndel.id FROM ice.ns.ndel JOIN ice.ns.ndel2 USING (_deleted) ORDER BY 1",
            reserved_name_collision("_deleted"),
        ),
        (
            "J5M",
            "SELECT id, _spec_id FROM ice.ns.ndel JOIN ice.ns.pl USING (id) ORDER BY id",
            "Error during planning: [AMBIGUOUS_REFERENCE] Reference `_spec_id` is ambiguous, \
             could be: [`ndel`.`_spec_id`, `pl`.`_spec_id`]. SQLSTATE: 42704"
                .to_string(),
        ),
        (
            "J6M",
            "SELECT *, ndel._spec_id FROM ice.ns.ndel JOIN ice.ns.pl USING (id) ORDER BY id",
            "Error during planning: [ICE-MC-1] a metadata column (_file, _pos, _spec_id, \
             _partition, _deleted) over a wildcard over more than one relation is not served; \
             name the columns explicitly on a table relation"
                .to_string(),
        ),
    ] {
        assert_eq!(plan_error(&session, sql).await, expected, "{row}: {sql}");
    }
    for (row, sql, ids) in [
        (
            "J5",
            "SELECT id FROM ice.ns.ndel JOIN ice.ns.pl USING (id) ORDER BY id",
            vec![1, 2],
        ),
        (
            "J7",
            "SELECT id FROM ice.ns.ndel NATURAL JOIN ice.ns.ndel2 ORDER BY id",
            vec![2],
        ),
        (
            "J7B",
            "SELECT ndel.id FROM ice.ns.ndel NATURAL JOIN ice.ns.ndel2 ORDER BY 1",
            vec![2],
        ),
        (
            "J8",
            "SELECT pl.id FROM ice.ns.pl JOIN ice.ns.ndel ON pl.id = ndel.id ORDER BY 1",
            vec![1, 2],
        ),
        (
            "J9",
            "SELECT pl.id FROM ice.ns.pl LEFT SEMI JOIN ice.ns.ndel ON pl.id = ndel.id ORDER BY 1",
            vec![1, 2],
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(field_names(&rows), vec!["id"], "{row}: {sql}");
        assert_eq!(i64s(&rows, 0), ids, "{row}: {sql}");
    }
    let rows = batches(
        &session,
        "SELECT * FROM ice.ns.ndel JOIN ice.ns.pl USING (id) ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "_deleted", "v"], "J6");
    assert_eq!(
        triples_i64(&rows),
        vec![
            (1, "u1".to_string(), "a".to_string()),
            (2, "u2".to_string(), "b".to_string()),
        ],
        "J6"
    );
    let rows = batches(
        &session,
        "SELECT pl.id, ndel._spec_id FROM ice.ns.pl JOIN ice.ns.ndel ON pl.id = ndel.id ORDER BY 1",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "_spec_id"], "J8M");
    assert_eq!(pairs_i64_i32(&rows), vec![(1, 0), (2, 0)], "J8M");
}

#[tokio::test]
async fn reserved_name_query_positions() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_position_tables(&session).await;
    for (row, sql) in [
        (
            "N1",
            "SELECT _deleted, count(*) FROM ice.ns.ndel GROUP BY _deleted ORDER BY 1",
        ),
        (
            "N2",
            "SELECT id FROM ice.ns.ndel GROUP BY id HAVING max(_deleted) = 'u2' ORDER BY id",
        ),
        ("N3", "SELECT id FROM ice.ns.ndel ORDER BY _deleted"),
        (
            "N4",
            "SELECT ndel.id FROM ice.ns.ndel JOIN ice.ns.pl ON ndel._deleted = pl.v ORDER BY 1",
        ),
        (
            "N5",
            "SELECT id, row_number() OVER (PARTITION BY _deleted ORDER BY id) FROM ice.ns.ndel \
             ORDER BY id",
        ),
        (
            "N6",
            "SELECT id, rank() OVER (ORDER BY _deleted) FROM ice.ns.ndel ORDER BY id",
        ),
        (
            "N7",
            "SELECT id FROM ice.ns.pl WHERE v IN (SELECT _deleted FROM ice.ns.ndel) ORDER BY id",
        ),
        (
            "N8",
            "SELECT id FROM ice.ns.pl WHERE EXISTS \
             (SELECT 1 FROM ice.ns.ndel WHERE ndel._deleted = 'u1') ORDER BY id",
        ),
        (
            "N9",
            "SELECT id, (SELECT max(_deleted) FROM ice.ns.ndel) FROM ice.ns.pl ORDER BY id",
        ),
        (
            "N10",
            "SELECT _deleted FROM ice.ns.ndel UNION ALL SELECT v FROM ice.ns.pl ORDER BY 1",
        ),
    ] {
        assert_eq!(
            plan_error(&session, sql).await,
            reserved_name_collision("_deleted"),
            "{row}: {sql}"
        );
    }
    assert_eq!(
        plan_error(
            &session,
            "SELECT id, e FROM ice.ns.ndel LATERAL VIEW explode(array(_deleted)) t AS e ORDER BY id",
        )
        .await,
        "This feature is not implemented: LATERAL VIEWS",
        "N11"
    );
    let n13 = refusal(
        &session,
        "SELECT t.*, t._spec_id FROM ice.ns.ndel t ORDER BY id",
    )
    .await;
    assert_eq!(
        (n13.exception_class(), n13.to_string()),
        (ErrorClass::Analysis, reserved_name_collision("_deleted")),
        "N13"
    );
}

#[tokio::test]
async fn reserved_name_query_positions_that_answer() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_position_tables(&session).await;
    let rows = batches(&session, "SELECT t.* FROM ice.ns.ndel t ORDER BY id").await;
    assert_eq!(field_names(&rows), vec!["id", "_deleted"], "N12");
    assert_eq!(
        pairs_i64_str(&rows),
        vec![(1, "u1".to_string()), (2, "u2".to_string())],
        "N12"
    );
    for (row, sql, ids) in [
        (
            "N17",
            "SELECT id FROM ice.ns.pl WHERE id IN (SELECT id FROM ice.ns.ndel) ORDER BY id",
            vec![1, 2],
        ),
        (
            "N18",
            "SELECT id FROM ice.ns.ndel UNION ALL SELECT id FROM ice.ns.pl ORDER BY 1",
            vec![1, 1, 2, 2],
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(field_names(&rows), vec!["id"], "{row}: {sql}");
        assert_eq!(i64s(&rows, 0), ids, "{row}: {sql}");
    }
    let rows = batches(
        &session,
        "SELECT id, count(*) OVER (PARTITION BY id) FROM ice.ns.ndel WHERE _spec_id = 0 \
         ORDER BY id",
    )
    .await;
    assert_eq!(
        field_names(&rows),
        vec![
            "id",
            "count(*) PARTITION BY [ndel.id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
        ],
        "N19"
    );
    assert_eq!(pairs_i64_i64(&rows), vec![(1, 1), (2, 1)], "N19");
}

async fn seed_qualified_wildcard_tables(session: &ReparkSession) {
    seed(session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    seed_values(
        session,
        "ice.ns.pl",
        "id BIGINT, v STRING",
        "(1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["b", "c"])),
        ],
    )
    .unwrap();
    session.context().register_batch("tv", batch).unwrap();
}

fn live_user_rows() -> Vec<(i64, String, String)> {
    vec![
        (2, "b".to_string(), "y".to_string()),
        (3, "c".to_string(), "x".to_string()),
        (4, "d".to_string(), "x".to_string()),
    ]
}

#[tokio::test]
async fn qualified_wildcard_under_another_alias_serves_user_columns() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_qualified_wildcard_tables(&session).await;
    for (row, sql) in [
        (
            "X3",
            "SELECT x.* FROM ice.ns.t x WHERE x._spec_id = 0 ORDER BY id",
        ),
        (
            "B1",
            "SELECT * FROM ice.ns.t x WHERE x._spec_id = 0 ORDER BY id",
        ),
        (
            "K1",
            "SELECT X.* FROM ice.ns.t x WHERE x._spec_id = 0 ORDER BY id",
        ),
        (
            "W2",
            "SELECT t.* FROM ice.ns.t t WHERE t._spec_id = 0 ORDER BY id",
        ),
        ("W4", "SELECT t.* FROM ice.ns.t t ORDER BY id"),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "data", "cat"],
            "{row}: {sql}"
        );
        assert_eq!(triples_i64(&rows), live_user_rows(), "{row}: {sql}");
    }
    for (row, sql) in [
        ("X1", "SELECT x.*, x._pos AS p FROM ice.ns.t x ORDER BY id"),
        ("B2", "SELECT *, x._pos AS p FROM ice.ns.t x ORDER BY id"),
        ("W1", "SELECT t.*, t._pos AS p FROM ice.ns.t t ORDER BY id"),
        ("W5", "SELECT t.*, t._pos AS p FROM ice.ns.t ORDER BY id"),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "data", "cat", "p"],
            "{row}: {sql}"
        );
        assert_eq!(triples_i64(&rows), live_user_rows(), "{row}: {sql}");
        assert_eq!(i64s(&rows, 3), vec![0, 0, 1], "{row}: {sql}");
    }
    for (row, sql) in [
        (
            "X2",
            "SELECT x.*, _spec_id AS s FROM ice.ns.t x ORDER BY id",
        ),
        (
            "W3",
            "SELECT t.*, _spec_id AS s FROM ice.ns.t t ORDER BY id",
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "data", "cat", "s"],
            "{row}: {sql}"
        );
        assert_eq!(triples_i64(&rows), live_user_rows(), "{row}: {sql}");
        assert_eq!(i32s(&rows, 3), vec![0, 0, 0], "{row}: {sql}");
    }
    for (row, sql) in [
        ("Q1", "SELECT t.*, t._spec_id FROM ice.ns.pl t ORDER BY id"),
        ("Q2", "SELECT t.*, _spec_id FROM ice.ns.pl t ORDER BY id"),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(
            field_names(&rows),
            vec!["id", "v", "_spec_id"],
            "{row}: {sql}"
        );
        assert_eq!(
            pairs_i64_str(&rows),
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string())
            ],
            "{row}: {sql}"
        );
        assert_eq!(i32s(&rows, 2), vec![0, 0, 0], "{row}: {sql}");
    }
}

#[tokio::test]
async fn qualified_wildcard_near_misses_keep_their_answers() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed_qualified_wildcard_tables(&session).await;
    for (row, sql, expected) in [
        (
            "M1",
            "SELECT y.* FROM ice.ns.t x WHERE x._spec_id = 0",
            "Error during planning: Invalid qualifier y",
        ),
        (
            "M2",
            "SELECT t.* FROM ice.ns.t x WHERE x._spec_id = 0 ORDER BY id",
            "Error during planning: Invalid qualifier t",
        ),
    ] {
        assert_eq!(plan_error(&session, sql).await, expected, "{row}: {sql}");
    }
    for (row, sql) in [
        (
            "D1",
            "SELECT x.* FROM (SELECT id FROM ice.ns.t WHERE _spec_id = 0) x ORDER BY id",
        ),
        (
            "C1",
            "WITH x AS (SELECT id FROM ice.ns.t WHERE _spec_id = 0) SELECT x.* FROM x ORDER BY id",
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(field_names(&rows), vec!["id"], "{row}: {sql}");
        assert_eq!(i64s(&rows, 0), vec![2, 3, 4], "{row}: {sql}");
    }
    let rows = batches(
        &session,
        "SELECT x.*, q.v FROM ice.ns.t x JOIN ice.ns.pl q ON x.id = q.id WHERE x._spec_id = 0 \
         ORDER BY x.id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id", "data", "cat", "v"], "P1");
    assert_eq!(triples_i64(&rows), live_user_rows()[..2].to_vec(), "P1");
    assert_eq!(strs(&rows, 3), vec!["b", "c"], "P1");
    for (row, sql) in [
        (
            "P2",
            "SELECT q.*, x.id AS xid FROM ice.ns.t x JOIN ice.ns.pl q ON x.id = q.id \
             WHERE x._spec_id = 0 ORDER BY q.id",
        ),
        (
            "P3",
            "SELECT q.*, x.id AS xid FROM ice.ns.t x JOIN tv q ON x.id = q.id \
             WHERE x._spec_id = 0 ORDER BY q.id",
        ),
    ] {
        let rows = batches(&session, sql).await;
        assert_eq!(field_names(&rows), vec!["id", "v", "xid"], "{row}: {sql}");
        assert_eq!(
            pairs_i64_str(&rows),
            vec![(2, "b".to_string()), (3, "c".to_string())],
            "{row}: {sql}"
        );
        assert_eq!(i64s(&rows, 2), vec![2, 3], "{row}: {sql}");
    }
}
