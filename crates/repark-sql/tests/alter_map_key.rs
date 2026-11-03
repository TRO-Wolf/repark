use std::sync::Arc;

use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const MAP_KEY_CREATE: &str =
    "CREATE TABLE ice.sales.t (id INT, mk MAP<STRUCT<a INT, b VARCHAR>, INT>)";

const ALTER_MK: &str = "Unsupported table change: Cannot alter map keys: \
     map<struct<#: a: optional int, #: b: optional string>, int>";

struct Door {
    session: ReparkSession,
    warehouse: String,
    _dir: TempDir,
}

async fn door_with(create: &str) -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    let door = Door {
        session,
        warehouse,
        _dir: dir,
    };
    outcome(&door, "CREATE SCHEMA IF NOT EXISTS ice.sales")
        .await
        .expect("schema");
    outcome(&door, create).await.expect(create);
    door
}

async fn outcome(door: &Door, sql: &str) -> Result<(), String> {
    match door.session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

fn metadata_file_count(warehouse: &str) -> usize {
    let mut count = 0;
    let mut stack = vec![std::path::PathBuf::from(warehouse)];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).expect("read warehouse") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "json")
                && path
                    .file_stem()
                    .is_some_and(|stem| stem.to_string_lossy().contains("metadata"))
            {
                count += 1;
            }
        }
    }
    count
}

async fn state(door: &Door) -> (usize, String) {
    let frame = door
        .session
        .sql("SELECT * FROM ice.sales.t")
        .await
        .expect("plan");
    (
        metadata_file_count(&door.warehouse),
        format!("{:?}", frame.schema().fields()),
    )
}

fn without_ids(message: &str) -> String {
    let mut masked = String::with_capacity(message.len());
    let mut rest = message;
    let mut after_word = false;
    while let Some(character) = rest.chars().next() {
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if digits > 0 && !after_word && rest[digits..].starts_with(": ") {
            masked.push('#');
            rest = &rest[digits..];
            after_word = false;
            continue;
        }
        masked.push(character);
        after_word = character.is_alphanumeric() || character == '_';
        rest = &rest[character.len_utf8()..];
    }
    masked
}

async fn refuses_unchanged(create: &str, alter: &str, expected: &str) {
    let door = door_with(create).await;
    let before = state(&door).await;
    let sql = format!("ALTER TABLE ice.sales.t {alter}");
    let error = outcome(&door, &sql).await.expect_err(alter);
    assert!(
        without_ids(&error).contains(expected),
        "{alter}: `{error}` lacks `{expected}`"
    );
    assert_eq!(state(&door).await, before, "{alter}");
}

async fn acts(create: &str, alter: &str) -> bool {
    let door = door_with(create).await;
    let before = state(&door).await;
    let sql = format!("ALTER TABLE ice.sales.t {alter}");
    outcome(&door, &sql).await.expect(alter);
    state(&door).await != before
}

#[tokio::test]
async fn native_door_refuses_a_drop_rename_or_add_under_a_map_key() {
    for alter in [
        "DROP COLUMN mk.key.b",
        "DROP COLUMN mk.key.a",
        "DROP COLUMN IF EXISTS mk.key.b",
        "RENAME COLUMN mk.key.a TO a2",
        "RENAME COLUMN MK.KEY.A TO a2",
    ] {
        refuses_unchanged(MAP_KEY_CREATE, alter, ALTER_MK).await;
    }
    refuses_unchanged(
        MAP_KEY_CREATE,
        "ADD COLUMN mk.key.c INT",
        "Unsupported table change: Cannot add fields to map keys: \
         map<struct<#: a: optional int, #: b: optional string>, int>",
    )
    .await;
}

#[tokio::test]
async fn native_door_moves_a_map_key_field_as_a_no_op() {
    for alter in [
        "ALTER COLUMN mk.key.b FIRST",
        "ALTER COLUMN mk.key.a AFTER b",
    ] {
        assert!(!acts(MAP_KEY_CREATE, alter).await, "{alter}");
    }
}

#[tokio::test]
async fn native_door_refuses_changes_deeper_under_a_map_key() {
    let create = "CREATE TABLE ice.sales.t (id INT, \
         mk MAP<STRUCT<s STRUCT<x INT, y INT>, a INT>, INT>)";
    let expected = "Unsupported table change: Cannot alter map keys: map<struct<#: s: optional \
         struct<#: x: optional int, #: y: optional int>, #: a: optional int>, int>";
    for alter in [
        "DROP COLUMN mk.key.s.x",
        "RENAME COLUMN mk.key.s.x TO x2",
        "ADD COLUMN mk.key.s.z INT",
        "ALTER COLUMN mk.key.s.y FIRST",
    ] {
        refuses_unchanged(create, alter, expected).await;
    }
    assert!(!acts(create, "ALTER COLUMN mk.key.a FIRST").await);
}

#[tokio::test]
async fn native_door_still_evolves_map_value_and_list_element_fields() {
    let create = "CREATE TABLE ice.sales.t (id INT, m MAP<VARCHAR, STRUCT<a INT, b VARCHAR>>, \
         ls ARRAY<STRUCT<a INT, b VARCHAR>>)";
    for alter in [
        "DROP COLUMN m.value.b",
        "RENAME COLUMN m.value.a TO a2",
        "ALTER COLUMN m.value.b FIRST",
        "ADD COLUMN m.value.c INT",
        "DROP COLUMN ls.element.b",
        "RENAME COLUMN ls.element.a TO a2",
        "ALTER COLUMN ls.element.b FIRST",
        "ADD COLUMN ls.element.c INT",
    ] {
        assert!(acts(create, alter).await, "{alter}");
    }
}
