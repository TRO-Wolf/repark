use serde_json::Value as Json;

use super::super::*;
use super::common::*;
use super::map_key_alter::{CELLS as NESTED_CELLS, text};
use super::nested_assign_oracle::{json_cell, sorted};

const REAUDIT_CELLS: [&str; 2] = [
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/corpus/reaudit-cells-2026-11-05.jsonl"
    ),
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../reaudit-work/cells.jsonl"
    ),
];

const V2: &str = " USING iceberg TBLPROPERTIES ('format-version'='2')";

const ARRAY_ROWS: &str =
    "INSERT INTO {t} VALUES (1, array(10, 20, 30), map('k', 1)), (2, array(40), map('k', 2))";

const STRUCT_ROWS: &str = "INSERT INTO {t} VALUES (1, named_struct('a', 1, 'b', 'x')), \
     (2, named_struct('a', 7, 'b', 'y')), (3, NULL)";

const IN_UNIT: [&str; 4] = [
    "[INVALID_ARRAY_INDEX]",
    "[INVALID_ARRAY_INDEX_IN_ELEMENT_AT]",
    "[INVALID_INDEX_OF_ZERO]",
    "[AMBIGUOUS_REFERENCE_TO_FIELDS]",
];

const SPARK_WRAPPED_CAUSE: &[(&str, &str)] = &[
    (
        "D10_oob_subscript_in_delete",
        "[INVALID_ARRAY_INDEX] The index 10 is out of bounds. The array has 3 elements. Use the \
         SQL function `get()` to tolerate accessing element at invalid index and return NULL \
         instead. SQLSTATE: 22003",
    ),
    (
        "D10_oob_subscript_in_update",
        "[INVALID_ARRAY_INDEX] The index 5 is out of bounds. The array has 3 elements. Use the \
         SQL function `get()` to tolerate accessing element at invalid index and return NULL \
         instead. SQLSTATE: 22003",
    ),
];

const NESTED_IN_UNIT: [&str; 5] = [
    "arr_element_at_oob",
    "arr_empty_subscript",
    "arr_subscript_neg",
    "arr_subscript_oob",
    "ns_dup_field",
];

const NESTED_OUTSIDE_UNIT: [(&str, &str); 3] = [
    ("ns_struct_pos", "D13: struct() names its fields c0 and c1"),
    ("ns_case", "D13: a case-different single field refuses"),
    ("map_null_subscript", "D13: m[NULL] raises an Arrow error"),
];

fn reaudit_rows() -> Option<Vec<Json>> {
    let body = REAUDIT_CELLS
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok())?;
    Some(
        body.lines()
            .map(|line| serde_json::from_str::<Json>(line).unwrap())
            .filter(|row| row["cat"] == "f3-ruling1" && row["view"] == "facade")
            .collect(),
    )
}

fn retarget(stmt: &str, table: &str) -> String {
    let mut out = String::new();
    let mut rest = stmt;
    while let Some(at) = rest.find("hd.p") {
        out.push_str(&rest[..at]);
        let tail = &rest[at..];
        let end = tail.find(".t").map_or(tail.len(), |dot| dot + 2);
        out.push_str(table);
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

fn spark_message(text: &str) -> Option<&str> {
    let start = text.find('[')?;
    let stop = text[start..].find("SQLSTATE: ")? + start + "SQLSTATE: ".len() + 5;
    text.get(start..stop)
}

fn plain(value: &Json) -> Json {
    match value {
        Json::Array(items) => Json::Array(items.iter().map(plain).collect()),
        Json::Object(object) => {
            let pairs = object
                .get("map")
                .or_else(|| object.get("struct"))
                .and_then(Json::as_array);
            match pairs {
                Some(pairs) if object.len() == 1 => Json::Object(
                    pairs
                        .iter()
                        .filter_map(|pair| {
                            Some((pair.get(0)?.as_str()?.to_string(), plain(pair.get(1)?)))
                        })
                        .collect(),
                ),
                _ => value.clone(),
            }
        }
        _ => value.clone(),
    }
}

fn rows_after(text: &str) -> Vec<Json> {
    let at = text.find(" rows ").unwrap();
    let rows: Json = serde_json::from_str(&text[at + 6..]).unwrap();
    sorted(rows.as_array().unwrap().iter().map(plain).collect())
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

async fn fixture(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, structs: bool) {
    let (columns, rows) = if structs {
        ("(id INT, s STRUCT<a: INT, b: STRING>)", STRUCT_ROWS)
    } else {
        ("(id INT, l ARRAY<INT>, m MAP<STRING, INT>)", ARRAY_ROWS)
    };
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE {table} {columns}{V2}"),
    )
    .await;
    run(ctx, catalogs, &rows.replace("{t}", table)).await;
}

fn expected_refusal(head: &str, spark: &str) -> Option<String> {
    spark_message(spark).map(str::to_string).or_else(|| {
        SPARK_WRAPPED_CAUSE
            .iter()
            .find(|(name, _)| *name == head)
            .map(|(_, cause)| (*cause).to_string())
    })
}

#[tokio::test]
async fn reaudit_ruling_cells_answer_or_refuse_as_spark_and_write_nothing_spark_refuses() {
    let Some(rows) = reaudit_rows() else {
        eprintln!(
            "skipped: neither {} nor {} is present",
            REAUDIT_CELLS[0], REAUDIT_CELLS[1]
        );
        return;
    };
    assert_eq!(rows.len(), 8);
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    let mut text_checked = 0usize;
    for (index, row) in rows.iter().enumerate() {
        let head = text(row, "head");
        let spark = text(row, "spark");
        let (outcome, readback) = spark.split_once(" || readback ").unwrap();
        let table = format!("ice.sales.r{index}");
        fixture(&ctx, &catalogs, &table, readback.contains("\"s\"")).await;
        let got = answer(&ctx, &catalogs, &retarget(&text(row, "stmt"), &table)).await;
        let error = got.expect_err(&head);
        if let Some(message) = expected_refusal(&head, outcome)
            && IN_UNIT.iter().any(|class| message.starts_with(class))
        {
            assert!(error.contains(&message), "{head}: {error}");
            text_checked += 1;
        }
        let after = answer(&ctx, &catalogs, &format!("SELECT * FROM {table}")).await;
        assert_eq!(after, Ok(rows_after(readback)), "{head}");
    }
    assert_eq!(text_checked, 6);
}

#[tokio::test]
async fn nested_corpus_d10_and_d13_expressions_refuse_with_sparks_text() {
    let Ok(body) = std::fs::read_to_string(NESTED_CELLS) else {
        eprintln!("skipped: {NESTED_CELLS} is absent");
        return;
    };
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    let mut pinned = 0usize;
    let mut outside = 0usize;
    for line in body.lines() {
        let row: Json = serde_json::from_str(line).unwrap();
        let spelling = text(&row, "spelling");
        if row["cat"] != "B-expr" || row["view"] != "facade-sql" {
            continue;
        }
        if NESTED_OUTSIDE_UNIT
            .iter()
            .any(|(name, _)| *name == spelling)
        {
            outside += 1;
            continue;
        }
        if !NESTED_IN_UNIT.contains(&spelling.as_str()) {
            continue;
        }
        let message = spark_message(&text(&row, "spark")).unwrap().to_string();
        let error = answer(&ctx, &catalogs, &text(&row, "stmt"))
            .await
            .expect_err(&spelling);
        assert!(error.contains(&message), "{spelling}: {error}");
        pinned += 1;
    }
    assert_eq!((pinned, outside), (5, 3));
}

const NEIGHBOURS: &[(&str, &str)] = &[
    (
        "SELECT array(1, 2, 3)[3]",
        "[INVALID_ARRAY_INDEX] The index 3 is out of bounds. The array has 3 elements.",
    ),
    (
        "SELECT array(1, 2, 3)[-1]",
        "[INVALID_ARRAY_INDEX] The index -1 is out of bounds. The array has 3 elements.",
    ),
    (
        "SELECT array(named_struct('a', 1))[5].a",
        "[INVALID_ARRAY_INDEX] The index 5 is out of bounds. The array has 1 elements.",
    ),
    (
        "SELECT transform(array(array(1), array(1, 2)), a -> a[1])",
        "[INVALID_ARRAY_INDEX] The index 1 is out of bounds. The array has 1 elements.",
    ),
    (
        "SELECT element_at(array(1, 2, 3), -4)",
        "[INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index -4 is out of bounds. The array has 3 elements.",
    ),
    (
        "SELECT element_at(array(1, 2, 3), 0)",
        "[INVALID_INDEX_OF_ZERO] The index 0 is invalid.",
    ),
    (
        "SELECT try_element_at(array(1, 2, 3), 0)",
        "[INVALID_INDEX_OF_ZERO] The index 0 is invalid.",
    ),
    (
        "SELECT named_struct('a', 1, 'A', 2).a",
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `a`. It appears 2 times",
    ),
    (
        "SELECT named_struct('a', 1, 'A', 2, 'a', 3).a",
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `a`. It appears 3 times",
    ),
    (
        "SELECT named_struct('x', named_struct('a', 1, 'a', 2)).x.a",
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `a`. It appears 2 times",
    ),
    (
        "SELECT map('k', named_struct('a', 1, 'a', 2))['k'].a",
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `a`. It appears 2 times",
    ),
    (
        "SELECT named_struct('a', 1, 'a', 2)['a']",
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `a`. It appears 2 times",
    ),
];

const NEIGHBOUR_ANSWERS: &[(&str, &str)] = &[
    ("SELECT array(1, 2, 3)[CAST(NULL AS INT)]", "[[null]]"),
    ("SELECT CAST(NULL AS ARRAY<INT>)[5]", "[[null]]"),
    ("SELECT array(1, 2, 3)[1]", "[[2]]"),
    ("SELECT element_at(CAST(NULL AS ARRAY<INT>), 4)", "[[null]]"),
    ("SELECT try_element_at(array(1, 2, 3), 4)", "[[null]]"),
    ("SELECT map('a', 1)['z']", "[[null]]"),
    ("SELECT named_struct('a', 1, 'a', 2, 'b', 3).b", "[[3]]"),
    ("SELECT named_struct('a', 1, 'b', 2).a", "[[1]]"),
];

#[tokio::test]
async fn ansi_neighbours_raise_and_their_controls_answer_as_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    for (sql, message) in NEIGHBOURS {
        let error = answer(&ctx, &catalogs, sql).await.expect_err(sql);
        assert!(error.contains(message), "{sql}: {error}");
    }
    for (sql, want) in NEIGHBOUR_ANSWERS {
        let want: Json = serde_json::from_str(want).unwrap();
        let got = answer(&ctx, &catalogs, sql).await;
        assert_eq!(got, Ok(want.as_array().unwrap().clone()), "{sql}");
    }
}

#[tokio::test]
async fn legacy_mode_answers_null_and_case_sensitive_mode_resolves_a_case_pair() {
    let legacy_wh = TempDir::new().unwrap();
    let (legacy, legacy_catalogs) = setup_with_ansi(&legacy_wh, false).await;
    repark_functions::register_all(&legacy);
    for sql in [
        "SELECT array(1, 2, 3)[5]",
        "SELECT array(1, 2, 3)[-1]",
        "SELECT element_at(array(1, 2, 3), 4)",
        "SELECT element_at(array(1, 2, 3), -4)",
    ] {
        let got = answer(&legacy, &legacy_catalogs, sql).await;
        assert_eq!(got, Ok(vec![Json::Array(vec![Json::Null])]), "{sql}");
    }
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    repark_functions::register_all(&ctx);
    enable_case_sensitive(&ctx);
    let lower = answer(&ctx, &catalogs, "SELECT named_struct('a', 1, 'A', 2).a").await;
    assert_eq!(lower, Ok(vec![Json::Array(vec![Json::from(1)])]));
    let upper = answer(&ctx, &catalogs, "SELECT named_struct('a', 1, 'A', 2).A").await;
    assert_eq!(upper, Ok(vec![Json::Array(vec![Json::from(2)])]));
    let exact = answer(
        &ctx,
        &catalogs,
        "SELECT named_struct('a', 1, 'A', 2, 'a', 3).a",
    )
    .await
    .expect_err("exact duplicate");
    assert!(
        exact.contains("Ambiguous reference to the field `a`. It appears 2 times"),
        "{exact}"
    );
}
