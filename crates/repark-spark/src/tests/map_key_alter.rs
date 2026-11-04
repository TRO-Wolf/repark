use iceberg::spec::Type;
use serde_json::Value as Json;

use super::super::*;
use super::common::*;

pub(super) const CELLS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/corpus/nested-cells-2026-11-03.jsonl"
);

const NO_INSERT: &str =
    "no insert1 statement: the harness wrote none for a table left without columns";

const SKIPPED: &[(&str, &str, &str)] = &[("list_parquet_drop_last", "insert1", NO_INSERT)];

const MAP_KEY_CREATE: &str = "CREATE TABLE ice.sales.mk (id INT, mk MAP<STRUCT<a: INT, b: STRING>, INT>) \
     USING iceberg";

const ALTER_MK: &str = "Unsupported table change: Cannot alter map keys: \
     map<struct<#: a: optional int, #: b: optional string>, int>";

#[derive(Debug, Clone)]
pub(super) struct Cell {
    pub(super) spelling: String,
    pub(super) head: String,
    pub(super) stmt: String,
    pub(super) spark: String,
    pub(super) repark: String,
}

pub(super) fn text(row: &Json, key: &str) -> String {
    row[key].as_str().unwrap_or_default().to_string()
}

pub(super) fn ddl_cells() -> Option<Vec<Cell>> {
    let body = std::fs::read_to_string(CELLS).ok()?;
    Some(
        body.lines()
            .map(|line| serde_json::from_str::<Json>(line).unwrap())
            .filter(|row| row["cat"] == "A-ddl")
            .map(|row| Cell {
                spelling: text(&row, "spelling"),
                head: text(&row, "head"),
                stmt: text(&row, "stmt"),
                spark: text(&row, "spark"),
                repark: text(&row, "repark"),
            })
            .collect(),
    )
}

fn without_ids(message: &str) -> String {
    regex::Regex::new(r"\b\d+: ")
        .unwrap()
        .replace_all(message, "#: ")
        .into_owned()
}

fn spark_message(answer: &str) -> &str {
    answer.strip_prefix("ERR ").unwrap_or(answer)
}

pub(super) async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(), String> {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn table_state(catalogs: &CatalogRegistry, name: &str) -> Option<(String, String)> {
    let table = catalog_handle(catalogs, "ice")
        .unwrap()
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            name.into(),
        ))
        .await
        .ok()?;
    Some((
        table.metadata_location().unwrap_or_default().to_string(),
        format!("{:?}", table.metadata().current_schema().as_struct()),
    ))
}

fn local(stmt: &str) -> String {
    stmt.replace("hd.n.", "ice.sales.")
}

fn unit_fixture(spelling: &str) -> bool {
    spelling.starts_with("mapkey_")
}

fn substituted_insert(cells: &[Cell], cell: &Cell) -> String {
    let none = cells
        .iter()
        .find(|other| other.spelling == "mapkey_parquet_none" && other.head == "insert1")
        .unwrap();
    none.stmt.replace("mapkey_parquet_none", &cell.spelling)
}

pub(super) fn statement_for(cells: &[Cell], cell: &Cell) -> String {
    if !(unit_fixture(&cell.spelling) && cell.head == "insert1") {
        return local(&cell.stmt);
    }
    let substitute = substituted_insert(cells, cell);
    if substitute != cell.stmt {
        assert!(
            cells
                .iter()
                .filter(|other| other.spelling == cell.spelling && other.head.starts_with("ddl"))
                .all(|other| other.spark.starts_with("ERR")),
            "{}: the insert is rebuilt on Spark's unchanged schema only when Spark refused every DDL",
            cell.spelling
        );
    }
    local(&substitute)
}

fn expected_refusal(cell: &Cell) -> bool {
    let answer = if unit_fixture(&cell.spelling) {
        &cell.spark
    } else {
        &cell.repark
    };
    answer.starts_with("ERR")
}

async fn check_cell(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    cells: &[Cell],
    cell: &Cell,
) -> Option<String> {
    let before = table_state(catalogs, &cell.spelling).await;
    let result = outcome(ctx, catalogs, &statement_for(cells, cell)).await;
    let after = table_state(catalogs, &cell.spelling).await;
    let label = format!("{} {}", cell.spelling, cell.head);
    let refuse = expected_refusal(cell);
    match (&result, refuse) {
        (Ok(()), true) => return Some(format!("{label}: acted, expected a refusal")),
        (Err(error), false) => return Some(format!("{label}: refused `{error}`, expected to act")),
        _ => {}
    }
    if refuse && cell.head.starts_with("ddl") && before != after {
        return Some(format!("{label}: refused but the table metadata changed"));
    }
    if let (Err(error), true) = (&result, unit_fixture(&cell.spelling)) {
        let spark = without_ids(spark_message(&cell.spark));
        if cell.head.starts_with("ddl") && !without_ids(error).contains(&spark) {
            return Some(format!(
                "{label}: refused `{error}`, Spark refused `{spark}`"
            ));
        }
    }
    None
}

#[tokio::test]
async fn nested_ddl_cells_answer_spark_on_map_keys_and_keep_every_control() {
    let Some(cells) = ddl_cells() else {
        eprintln!("skipped: {CELLS} is absent (it lands with PR #1254)");
        return;
    };
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    let mut mismatches = Vec::new();
    let mut pinned = 0usize;
    let mut skipped = Vec::new();
    for cell in &cells {
        if let Some((_, _, reason)) = SKIPPED
            .iter()
            .find(|(spelling, head, _)| *spelling == cell.spelling && *head == cell.head)
        {
            skipped.push((cell.spelling.clone(), cell.head.clone(), *reason));
            continue;
        }
        pinned += 1;
        if let Some(mismatch) = check_cell(&ctx, &catalogs, &cells, cell).await {
            mismatches.push(mismatch);
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert_eq!(skipped.len(), SKIPPED.len(), "{skipped:?}");
    assert_eq!(pinned + skipped.len(), cells.len());
    assert_eq!(pinned, 172);
    let moved = load_sales_table(&catalogs, "mapkey_parquet_move").await;
    assert_eq!(
        key_names(moved.metadata().current_schema(), "mk"),
        ["a", "b"]
    );
}

fn key_names(schema: &iceberg::spec::Schema, column: &str) -> Vec<String> {
    let field = schema.field_by_name(column).unwrap();
    let Type::Map(map) = field.field_type.as_ref() else {
        panic!("{column} is not a map");
    };
    let Type::Struct(key) = map.key_field.field_type.as_ref() else {
        panic!("{column} has no struct key");
    };
    key.fields()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

async fn fresh(create: &str) -> (TempDir, SessionContext, CatalogRegistry) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    run(&ctx, &catalogs, create).await;
    (wh, ctx, catalogs)
}

async fn refuses_unchanged(create: &str, table: &str, alter: &str, expected: &str) {
    let (_wh, ctx, catalogs) = fresh(create).await;
    let before = table_state(&catalogs, table).await;
    let sql = format!("ALTER TABLE ice.sales.{table} {alter}");
    let error = outcome(&ctx, &catalogs, &sql).await.expect_err(alter);
    assert!(
        without_ids(&error).contains(expected),
        "{alter}: `{error}` lacks `{expected}`"
    );
    assert_eq!(table_state(&catalogs, table).await, before, "{alter}");
}

async fn acts_unchanged(create: &str, table: &str, alter: &str) {
    let (_wh, ctx, catalogs) = fresh(create).await;
    let before = table_state(&catalogs, table).await;
    let sql = format!("ALTER TABLE ice.sales.{table} {alter}");
    outcome(&ctx, &catalogs, &sql).await.expect(alter);
    assert_eq!(table_state(&catalogs, table).await, before, "{alter}");
}

async fn acts_changed(create: &str, table: &str, alter: &str) {
    let (_wh, ctx, catalogs) = fresh(create).await;
    let before = table_state(&catalogs, table).await;
    let sql = format!("ALTER TABLE ice.sales.{table} {alter}");
    outcome(&ctx, &catalogs, &sql).await.expect(alter);
    assert_ne!(table_state(&catalogs, table).await, before, "{alter}");
}

#[tokio::test]
async fn every_change_to_a_map_key_field_refuses_with_spark_text() {
    for alter in [
        "DROP COLUMN mk.key.b",
        "DROP COLUMN mk.key.a",
        "DROP COLUMN IF EXISTS mk.key.b",
        "DROP COLUMNS mk.key.b, id",
        "RENAME COLUMN mk.key.a TO a2",
        "RENAME COLUMN MK.KEY.A TO a2",
        "ALTER COLUMN mk.key.a TYPE BIGINT",
        "ALTER COLUMN mk.key.a COMMENT 'x'",
    ] {
        refuses_unchanged(MAP_KEY_CREATE, "mk", alter, ALTER_MK).await;
    }
}

#[tokio::test]
async fn adding_to_a_map_key_refuses_with_spark_text() {
    let expected = "Unsupported table change: Cannot add fields to map keys: \
         map<struct<#: a: optional int, #: b: optional string>, int>";
    for alter in [
        "ADD COLUMN mk.key.c INT",
        "ADD COLUMNS (mk.key.c INT, id2 INT)",
    ] {
        refuses_unchanged(MAP_KEY_CREATE, "mk", alter, expected).await;
    }
    refuses_unchanged(
        MAP_KEY_CREATE,
        "mk",
        "ADD COLUMN mk.key.a INT",
        "[FIELD_ALREADY_EXISTS] Cannot add column, because `mk`.`key`.`a` already exists",
    )
    .await;
}

#[tokio::test]
async fn moving_a_map_key_field_succeeds_and_keeps_the_key_struct() {
    for alter in [
        "ALTER COLUMN mk.key.b FIRST",
        "ALTER COLUMN mk.key.a AFTER b",
        "ALTER COLUMN mk.key.b AFTER a",
        "ALTER COLUMN mk.key.a TYPE INT",
        "DROP COLUMN IF EXISTS mk.key.zz",
    ] {
        acts_unchanged(MAP_KEY_CREATE, "mk", alter).await;
    }
    let (_wh, ctx, catalogs) = fresh(MAP_KEY_CREATE).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.mk ALTER COLUMN mk.key.b FIRST",
    )
    .await;
    let table = load_sales_table(&catalogs, "mk").await;
    assert_eq!(
        key_names(table.metadata().current_schema(), "mk"),
        ["a", "b"]
    );
}

#[tokio::test]
async fn a_struct_below_a_map_key_refuses_every_change_and_its_move() {
    let create = "CREATE TABLE ice.sales.deep (id INT, \
         mk MAP<STRUCT<s: STRUCT<x: INT, y: INT>, a: INT>, INT>) USING iceberg";
    let expected = "Unsupported table change: Cannot alter map keys: map<struct<#: s: optional \
         struct<#: x: optional int, #: y: optional int>, #: a: optional int>, int>";
    for alter in [
        "DROP COLUMN mk.key.s.x",
        "DROP COLUMN mk.key.s",
        "RENAME COLUMN mk.key.s.x TO x2",
        "ADD COLUMN mk.key.s.z INT",
        "ALTER COLUMN mk.key.s.x TYPE BIGINT",
        "ALTER COLUMN mk.key.s.y FIRST",
    ] {
        refuses_unchanged(create, "deep", alter, expected).await;
    }
    acts_unchanged(create, "deep", "ALTER COLUMN mk.key.a FIRST").await;
}

#[tokio::test]
async fn a_map_inside_a_map_key_refuses_with_the_outer_map() {
    let create = "CREATE TABLE ice.sales.inner (id INT, \
         mk MAP<STRUCT<m: MAP<STRING, STRUCT<x: INT, y: INT>>>, INT>) USING iceberg";
    let expected = "Unsupported table change: Cannot alter map keys: map<struct<#: m: optional \
         map<string, struct<#: x: optional int, #: y: optional int>>>, int>";
    for alter in [
        "DROP COLUMN mk.key.m.value.x",
        "ADD COLUMN mk.key.m.value.z INT",
        "ADD COLUMN mk.key.m.z INT",
        "RENAME COLUMN mk.key.m.value.x TO x2",
        "ALTER COLUMN mk.key.m.value.y FIRST",
        "ALTER COLUMN mk.key.m.value.x TYPE BIGINT",
    ] {
        refuses_unchanged(create, "inner", alter, expected).await;
    }
}

#[tokio::test]
async fn two_touched_map_keys_name_the_first_in_schema_order() {
    let create = "CREATE TABLE ice.sales.two (id INT, mk1 MAP<STRUCT<p: INT>, INT>, \
         mk2 MAP<STRUCT<q: INT>, INT>) USING iceberg";
    let expected =
        "Unsupported table change: Cannot alter map keys: map<struct<#: p: optional int>, int>";
    for alter in [
        "DROP COLUMNS mk2.key.q, mk1.key.p",
        "DROP COLUMNS mk1.key.p, mk2.key.q",
    ] {
        refuses_unchanged(create, "two", alter, expected).await;
    }
}

#[tokio::test]
async fn a_primitive_map_key_keeps_its_own_refusals() {
    let create = "CREATE TABLE ice.sales.prim (id INT, mk MAP<INT, INT>) USING iceberg";
    for (alter, expected) in [
        (
            "DROP COLUMN mk.key.x",
            "Cannot delete missing column: mk.key.x",
        ),
        (
            "ADD COLUMN mk.key.c INT",
            "Cannot add to non-struct column: mk.key: int",
        ),
        ("DROP COLUMN mk.key", "Cannot delete map keys"),
        ("RENAME COLUMN mk.key TO k2", "Cannot update map keys"),
        (
            "ALTER COLUMN mk.key TYPE BIGINT",
            "Unsupported table change: Cannot update map keys: map<int, int>",
        ),
    ] {
        refuses_unchanged(create, "prim", alter, expected).await;
    }
    acts_changed(create, "prim", "ALTER COLUMN mk.value TYPE BIGINT").await;
}

#[tokio::test]
async fn map_value_and_list_element_fields_still_evolve() {
    let create = "CREATE TABLE ice.sales.val (id INT, m MAP<STRING, STRUCT<a: INT, b: STRING>>, \
         ls ARRAY<STRUCT<a: INT, b: STRING>>) USING iceberg";
    for alter in [
        "DROP COLUMN m.value.b",
        "RENAME COLUMN m.value.a TO a2",
        "ALTER COLUMN m.value.b FIRST",
        "ADD COLUMN m.value.c INT",
        "ALTER COLUMN m.value.a TYPE BIGINT",
        "DROP COLUMN ls.element.b",
        "RENAME COLUMN ls.element.a TO a2",
        "ALTER COLUMN ls.element.b FIRST",
        "ADD COLUMN ls.element.c INT",
    ] {
        acts_changed(create, "val", alter).await;
    }
}

#[tokio::test]
async fn renaming_a_map_key_field_refuses_whatever_the_new_name() {
    for alter in [
        "RENAME COLUMN mk.key.a TO A",
        "RENAME COLUMN mk.key.a TO B",
        "RENAME COLUMN mk.key.A TO B",
        "RENAME COLUMN mk.key.b TO A",
        "RENAME COLUMN mk.key.a TO a",
        "RENAME COLUMN mk.key.a TO b",
    ] {
        refuses_unchanged(MAP_KEY_CREATE, "mk", alter, ALTER_MK).await;
    }
}

#[tokio::test]
async fn moving_a_map_key_field_after_itself_refuses_before_the_no_op() {
    for (alter, expected) in [
        (
            "ALTER COLUMN mk.key.b AFTER b",
            "Cannot move mk.key.b after itself",
        ),
        ("ALTER COLUMN mk.key.B AFTER b", "after itself"),
        ("ALTER COLUMN id AFTER id", "Cannot move id after itself"),
    ] {
        refuses_unchanged(MAP_KEY_CREATE, "mk", alter, expected).await;
    }
    for alter in [
        "ALTER COLUMN mk.key.b FIRST",
        "ALTER COLUMN mk.key.a AFTER b",
    ] {
        acts_unchanged(MAP_KEY_CREATE, "mk", alter).await;
    }
}
