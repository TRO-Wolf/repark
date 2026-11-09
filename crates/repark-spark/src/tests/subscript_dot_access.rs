use std::collections::HashSet;

use serde_json::Value as Json;

use super::super::*;
use super::common::*;
use super::map_key_alter::{CELLS, Cell, ddl_cells, outcome, statement_for, text};
use super::nested_assign_oracle::{json_cell, sorted};

const DOT_TEXT: &str = "Dot access not supported for non-string expr";

const NATIVE: &str = "native door: the respelling is not wired there (hand-back question Q1)";
const ORC: &str = "D3: an ORC data-file read of a nested column refuses before the access";
const SPARK_READER: &str = "D12: Spark's reader failed on its own table; no oracle answer";
const SPARK_COLLAPSE: &str =
    "D12: Spark collapses to null a list element whose projected children are absent";
const AVRO_NULL: &str = "fixture: RePark's Avro round trip reads a null map or list column back \
     as empty and a map's entries in another order, so `map_keys`, `size` and `IS NULL` differ";

const AVRO_EMPTY: &str = "fixture: RePark's Avro round trip reads a null map or list column back \
     as empty, so under ANSI a `[0]` into it raises INVALID_ARRAY_INDEX where Spark reads null";

const COLLAPSED: &[(&str, &str)] = &[
    ("structlist_parquet_add", "null_parent"),
    ("structlist_parquet_readd", "null_parent"),
];

const SAMPLE: &str = "(SELECT 1 AS id, map('k', named_struct('x', 1, 'arr', array(10, 20), \
     'g', named_struct('h', 7), 'weird name', 9)) AS m, array(named_struct('a', 1, 'b', 'p'), \
     named_struct('a', 2, 'b', 'q')) AS ls, array(10, 20) AS arr UNION ALL SELECT 2 AS id, \
     map('j', named_struct('x', 5, 'arr', array(30), 'g', named_struct('h', 8), 'weird name', 3)) \
     AS m, array(named_struct('a', 3, 'b', 'r')) AS ls, array(30) AS arr) t";

#[derive(Debug, Clone)]
struct Read {
    spelling: String,
    view: String,
    head: String,
    stmt: String,
    spark: String,
}

fn unit_reads() -> Option<Vec<Read>> {
    let body = std::fs::read_to_string(CELLS).ok()?;
    Some(
        body.lines()
            .map(|line| serde_json::from_str::<Json>(line).unwrap())
            .filter(|row| row["cat"] == "A-read" && text(row, "repark").contains(DOT_TEXT))
            .map(|row| Read {
                spelling: text(&row, "spelling"),
                view: text(&row, "view"),
                head: text(&row, "head"),
                stmt: text(&row, "stmt"),
                spark: text(&row, "spark"),
            })
            .collect(),
    )
}

fn format_of(spelling: &str) -> &str {
    spelling.split('_').nth(1).unwrap_or_default()
}

fn parquet_twin(spelling: &str) -> String {
    let format = format_of(spelling);
    spelling.replacen(&format!("_{format}_"), "_parquet_", 1)
}

fn skip_reason(read: &Read) -> Option<&'static str> {
    if read.view == "native-path-view" {
        return Some(NATIVE);
    }
    if read.spark.starts_with("ERR Job aborted") {
        return Some(SPARK_READER);
    }
    if read.spark.starts_with("ERR") {
        return None;
    }
    if format_of(&read.spelling) == "orc" {
        return Some(ORC);
    }
    if format_of(&read.spelling) == "avro" && matches!(read.head.as_str(), "alt" | "null_parent") {
        return Some(AVRO_NULL);
    }
    if format_of(&read.spelling) == "avro"
        && (read.spelling.starts_with("mapkey_") || read.spelling.starts_with("structlist_"))
        && read.stmt.contains("[0]")
    {
        return Some(AVRO_EMPTY);
    }
    if COLLAPSED
        .iter()
        .any(|(spelling, head)| read.spelling == *spelling && read.head == *head)
    {
        return Some(SPARK_COLLAPSE);
    }
    None
}

async fn build_fixture(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: &[Cell],
    spelling: &str,
) {
    let source = parquet_twin(spelling);
    let format = format!("'write.format.default'='{}'", format_of(spelling));
    for cell in ddl.iter().filter(|cell| cell.spelling == source) {
        let statement = statement_for(ddl, cell)
            .replace(&source, spelling)
            .replace("'write.format.default'='parquet'", &format);
        let _ = outcome(ctx, catalogs, &statement).await;
    }
}

async fn answer(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<Vec<Json>, String> {
    let frame = execute(ctx, catalogs, sql)
        .await
        .map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
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
    Ok(sorted(rows))
}

fn spark_rows(spark: &str) -> Option<Vec<Json>> {
    let at = spark.find(" rows ")?;
    let rows: Json = serde_json::from_str(&spark[at + 6..]).ok()?;
    Some(sorted(rows.as_array()?.clone()))
}

async fn check_read(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    read: &Read,
) -> Option<String> {
    let sql = read
        .stmt
        .replace(" FROM v", &format!(" FROM ice.sales.{}", read.spelling));
    let label = format!("{} {} `{}`", read.spelling, read.head, read.stmt);
    let got = answer(ctx, catalogs, &sql).await;
    match (spark_rows(&read.spark), got) {
        (None, Ok(rows)) => Some(format!(
            "{label}: answered {rows:?}, Spark refused `{}`",
            read.spark
        )),
        (Some(want), Err(error)) => Some(format!(
            "{label}: refused `{error}`, Spark answered {want:?}"
        )),
        (Some(want), Ok(rows)) if rows != want => Some(format!(
            "{label}: answered {rows:?}, Spark answered {want:?}"
        )),
        _ => None,
    }
}

#[tokio::test]
async fn dot_access_cells_answer_spark_on_the_spark_door() {
    let (Some(reads), Some(ddl)) = (unit_reads(), ddl_cells()) else {
        eprintln!("skipped: {CELLS} is absent");
        return;
    };
    assert_eq!(reads.len(), 816);
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    let mut built = HashSet::new();
    let mut mismatches = Vec::new();
    let mut pinned = 0usize;
    let mut skipped: HashMap<&'static str, usize> = HashMap::new();
    for read in &reads {
        if let Some(reason) = skip_reason(read) {
            *skipped.entry(reason).or_default() += 1;
            continue;
        }
        if built.insert(read.spelling.clone()) {
            build_fixture(&ctx, &catalogs, &ddl, &read.spelling).await;
        }
        pinned += 1;
        if let Some(mismatch) = check_read(&ctx, &catalogs, read).await {
            mismatches.push(mismatch);
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    eprintln!("pinned {pinned}, skipped {skipped:?}");
    assert_eq!(pinned, 224);
    for (reason, count) in [
        (NATIVE, 408),
        (ORC, 100),
        (AVRO_NULL, 26),
        (AVRO_EMPTY, 51),
        (SPARK_READER, 5),
        (SPARK_COLLAPSE, 2),
    ] {
        assert_eq!(skipped.get(reason), Some(&count), "{reason}");
    }
    assert_eq!(pinned + skipped.values().sum::<usize>(), reads.len());
}

async fn spark_door() -> (TempDir, SessionContext, CatalogRegistry) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    (wh, ctx, catalogs)
}

fn rows_of(literal: &str) -> Vec<Json> {
    sorted(
        serde_json::from_str::<Json>(literal)
            .unwrap()
            .as_array()
            .unwrap()
            .clone(),
    )
}

async fn assert_answers(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str, want: &str) {
    let got = answer(ctx, catalogs, sql).await;
    assert_eq!(got, Ok(rows_of(want)), "{sql}");
}

async fn assert_refuses(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let error = answer(ctx, catalogs, sql).await.expect_err(sql);
    assert!(!error.contains(DOT_TEXT), "{sql}: {error}");
    error
}

#[tokio::test]
async fn card_statements_answer_spark_on_a_struct_keyed_map() {
    let (_wh, ctx, catalogs) = spark_door().await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sv (id INT, m MAP<STRUCT<k: INT, j: INT>, \
         STRUCT<x: INT, arr: ARRAY<INT>>>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.sv VALUES \
         (1, map(named_struct('k', 1, 'j', 2), named_struct('x', 5, 'arr', array(10, 20)))), \
         (2, map(named_struct('k', 3, 'j', 4), named_struct('x', 6, 'arr', array(30))))",
    )
    .await;
    for (sql, want) in [
        (
            "SELECT id, m[named_struct('k', 1, 'j', 2)].x FROM ice.sales.sv ORDER BY id",
            "[[1, 5], [2, null]]",
        ),
        (
            "SELECT id, m[named_struct('k', 1, 'j', 2)].arr[0] FROM ice.sales.sv ORDER BY id",
            "[[1, 10], [2, null]]",
        ),
        (
            "SELECT id, (m[named_struct('k', 1, 'j', 2)]).x FROM ice.sales.sv ORDER BY id",
            "[[1, 5], [2, null]]",
        ),
        (
            "SELECT id, element_at(m, named_struct('k', 1, 'j', 2)).x FROM ice.sales.sv ORDER BY id",
            "[[1, 5], [2, null]]",
        ),
        ("SELECT map('a', named_struct('x', 1))['a'].x", "[[1]]"),
    ] {
        assert_answers(&ctx, &catalogs, sql, want).await;
    }
}

#[tokio::test]
async fn every_primary_takes_a_dereference_chain() {
    let (_wh, ctx, catalogs) = spark_door().await;
    for (select, want) in [
        ("m['k'].x", "[[1, 1], [2, null]]"),
        ("m['k'].arr[0]", "[[1, 10], [2, null]]"),
        ("m['k'].g.h", "[[1, 7], [2, null]]"),
        ("element_at(m, 'k').x", "[[1, 1], [2, null]]"),
        ("reverse(ls)[0].a", "[[1, 2], [2, 3]]"),
        ("(m['k']).x", "[[1, 1], [2, null]]"),
        ("(arr)[0]", "[[1, 10], [2, 30]]"),
        ("ls[0].b", "[[1, \"p\"], [2, \"r\"]]"),
        ("m['k'].`weird name`", "[[1, 9], [2, null]]"),
        (
            "transform(array(m), e -> e['k'].x)",
            "[[1, [1]], [2, [null]]]",
        ),
        ("max(ls[0].a) OVER (ORDER BY id)", "[[1, 1], [2, 3]]"),
    ] {
        let sql = format!("SELECT id, {select} FROM {SAMPLE} ORDER BY id");
        assert_answers(&ctx, &catalogs, &sql, want).await;
    }
    for (sql, want) in [
        ("SELECT map('a', named_struct('x', 1))['a'].x", "[[1]]"),
        (
            "SELECT array(named_struct('x', 1), named_struct('x', 2))[1].x",
            "[[2]]",
        ),
        ("SELECT array(array(named_struct('a', 4)))[0][0].a", "[[4]]"),
        (
            "SELECT CAST(named_struct('a', 1) AS STRUCT<a: BIGINT>).a",
            "[[1]]",
        ),
        (
            "SELECT (CASE WHEN true THEN named_struct('a', 1) END).a",
            "[[1]]",
        ),
        ("SELECT map('k', 1).k", "[[1]]"),
    ] {
        assert_answers(&ctx, &catalogs, sql, want).await;
    }
}

#[tokio::test]
async fn chains_answer_in_where_group_by_and_order_by() {
    let (_wh, ctx, catalogs) = spark_door().await;
    let filtered = format!("SELECT id FROM {SAMPLE} WHERE m['k'].x = 1");
    assert_answers(&ctx, &catalogs, &filtered, "[[1]]").await;
    let grouped = format!("SELECT ls[0].b, count(*) FROM {SAMPLE} GROUP BY ls[0].b");
    assert_answers(&ctx, &catalogs, &grouped, "[[\"p\", 1], [\"r\", 1]]").await;
    let ordered = format!("SELECT id FROM {SAMPLE} ORDER BY ls[0].a DESC LIMIT 1");
    assert_answers(&ctx, &catalogs, &ordered, "[[2]]").await;
}

#[tokio::test]
async fn a_missing_field_or_a_non_struct_base_still_refuses() {
    let (_wh, ctx, catalogs) = spark_door().await;
    let missing = format!("SELECT id, m['k'].zz FROM {SAMPLE}");
    let error = assert_refuses(&ctx, &catalogs, &missing).await;
    assert!(error.contains("zz"), "{error}");
    assert_refuses(&ctx, &catalogs, "SELECT abs(1).x").await;
    assert_refuses(&ctx, &catalogs, "SELECT array(1, 2)[0].x").await;
}

#[tokio::test]
async fn leading_column_paths_keep_their_resolution() {
    let (_wh, ctx, catalogs) = spark_door().await;
    let path = format!("SELECT id, m.k.x FROM {SAMPLE} ORDER BY id");
    assert_answers(&ctx, &catalogs, &path, "[[1, 1], [2, null]]").await;
    let qualified = format!("SELECT t.id, t.m['k'].x FROM {SAMPLE} ORDER BY id");
    assert_answers(&ctx, &catalogs, &qualified, "[[1, 1], [2, null]]").await;
}
