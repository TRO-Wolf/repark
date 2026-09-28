use super::super::*;
use super::common::*;
use datafusion::arrow::array::ArrayRef;
use std::collections::HashSet;

type AnswerLeg<'a> = (&'a str, &'a [&'a str], &'a [&'a [&'a str]]);

async fn create_probe_tables(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t (id INT, Data STRING, s STRUCT<a: INT>) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', named_struct('a', 5)), (2, 'b', named_struct('a', 6))",
    )
    .await;
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.u (id INT, Data STRING) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.u VALUES (1, 'x'), (5, 'y')",
    )
    .await;
}

async fn register_probe_view(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    let frame = execute(ctx, catalogs, "SELECT id, Data FROM ice.sales.t")
        .await
        .unwrap();
    ctx.register_table("tv", frame.into_view()).unwrap();
}

fn cell(column: &ArrayRef, row: usize) -> String {
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values.value(row).to_string();
    }
    panic!("unexpected column type {}", column.data_type());
}

async fn names_and_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Vec<String>, Vec<Vec<String>>) {
    let frame = execute(ctx, catalogs, sql).await.unwrap();
    let names = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let batches = frame.collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::new();
            for column in 0..batch.num_columns() {
                rendered.push(cell(batch.column(column), row));
            }
            rows.push(rendered);
        }
    }
    rows.sort();
    (names, rows)
}

fn bare_set(entries: &[&str]) -> HashSet<String> {
    entries
        .iter()
        .map(|entry| match entry.split_once("`.`") {
            Some((_, qualified)) => format!("`{qualified}"),
            None => (*entry).to_string(),
        })
        .collect()
}

fn bare_candidates(message: &str) -> HashSet<String> {
    let mark = "Did you mean one of the following? [";
    let Some(tail) = message.split(mark).nth(1) else {
        return HashSet::new();
    };
    let head = tail.split(']').next().unwrap_or_default();
    let entries = head
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    bare_set(&entries)
}

fn strip_prefix(message: &str) -> &str {
    message
        .strip_prefix("Error during planning: ")
        .unwrap_or(message)
}

async fn assert_unresolved(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    name: &str,
    candidates: &[&str],
) {
    let error = execute(ctx, catalogs, sql).await.err().unwrap();
    let message = strip_prefix(&error.to_string()).to_string();
    assert!(
        message.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
        "{sql}: {message}"
    );
    assert!(message.contains("SQLSTATE: 42703"), "{sql}: {message}");
    let head = format!(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name {name} cannot be resolved. "
    );
    assert!(message.starts_with(&head), "{sql}: {message}");
    assert_eq!(
        bare_candidates(&message),
        bare_set(candidates),
        "{sql}: {message}"
    );
}

async fn assert_u_unchanged(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    let (_, rows) = names_and_rows(ctx, catalogs, "SELECT * FROM ice.sales.u").await;
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["5".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn wrong_case_refuses_in_every_scope_under_case_sensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    let join_candidates: &[&str] = &[
        "`a`.`s`",
        "`a`.`id`",
        "`a`.`Data`",
        "`b`.`id`",
        "`b`.`Data`",
    ];
    let legs: &[(&str, &str, &[&str])] = &[
        (
            "SELECT ID FROM ice.sales.t",
            "`ID`",
            &["`s`", "`id`", "`Data`"],
        ),
        (
            "SELECT a.ID FROM ice.sales.t a JOIN ice.sales.u b ON a.id = b.id",
            "`a`.`ID`",
            join_candidates,
        ),
        (
            "SELECT a.id FROM ice.sales.t a JOIN ice.sales.u b ON a.ID = b.id",
            "`a`.`ID`",
            join_candidates,
        ),
        (
            "SELECT ID FROM (SELECT id FROM ice.sales.t)",
            "`ID`",
            &["`id`"],
        ),
        (
            "WITH c AS (SELECT id FROM ice.sales.t) SELECT ID FROM c",
            "`ID`",
            &["`id`"],
        ),
        (
            "SELECT id FROM ice.sales.t WHERE ID = 1",
            "`ID`",
            &["`s`", "`id`", "`Data`"],
        ),
        (
            "SELECT id FROM ice.sales.t ORDER BY ID",
            "`ID`",
            &["`id`", "`Data`", "`s`"],
        ),
        (
            "SELECT count(*) FROM ice.sales.t GROUP BY ID",
            "`ID`",
            &["`s`", "`id`", "`Data`"],
        ),
        (
            "SELECT T.id FROM ice.sales.u t",
            "`T.id`",
            &["`id`", "`Data`"],
        ),
        (
            "SELECT id AS X FROM ice.sales.t ORDER BY x",
            "`x`",
            &["`X`", "`id`", "`Data`", "`s`"],
        ),
        ("SELECT `ID` FROM ice.sales.u", "`ID`", &["`id`", "`Data`"]),
    ];
    for (sql, name, candidates) in legs {
        assert_unresolved(&ctx, &catalogs, sql, name, candidates).await;
    }
    let error = execute(&ctx, &catalogs, "SELECT s.A FROM ice.sales.t")
        .await
        .err()
        .unwrap();
    assert_eq!(
        strip_prefix(&error.to_string()),
        "Field A not found in struct"
    );
}

#[tokio::test]
async fn exact_mixed_case_answers_under_case_sensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    register_probe_view(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    let legs: &[AnswerLeg<'_>] = &[
        ("SELECT Data FROM ice.sales.t", &["Data"], &[&["a"], &["b"]]),
        (
            "SELECT `Data` FROM ice.sales.t",
            &["Data"],
            &[&["a"], &["b"]],
        ),
        ("SELECT a FROM (SELECT 1 AS a, 2 AS A)", &["a"], &[&["1"]]),
        (
            "SELECT Id, id FROM (SELECT 1 AS Id, 2 AS id)",
            &["Id", "id"],
            &[&["1", "2"]],
        ),
        (
            "SELECT * FROM ice.sales.u",
            &["id", "Data"],
            &[&["1", "x"], &["5", "y"]],
        ),
        ("SELECT Data FROM tv", &["Data"], &[&["a"], &["b"]]),
    ];
    for (sql, names, rows) in legs {
        let (actual_names, actual_rows) = names_and_rows(&ctx, &catalogs, sql).await;
        let expected_names = names.iter().map(ToString::to_string).collect::<Vec<_>>();
        let expected_rows = rows
            .iter()
            .map(|row| row.iter().map(ToString::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(actual_names, expected_names, "{sql}");
        assert_eq!(actual_rows, expected_rows, "{sql}");
    }
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT UPPER(Data), COUNT(*) FROM ice.sales.t GROUP BY Data",
    )
    .await;
    assert_eq!(names[0], "upper(ice.sales.t.Data)".to_string());
    assert_eq!(
        rows,
        vec![
            vec!["A".to_string(), "1".to_string()],
            vec!["B".to_string(), "1".to_string()],
        ]
    );
}

#[tokio::test]
async fn relation_cte_and_view_names_match_exactly_under_case_sensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    register_probe_view(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    let legs: &[(&str, &str)] = &[
        (
            "SELECT id FROM ice.sales.T",
            "table 'ice.sales.T' not found",
        ),
        (
            "SELECT id FROM ice.SALES.t",
            "table 'ice.SALES.t' not found",
        ),
        (
            "WITH C AS (SELECT 1 AS a) SELECT * FROM c",
            "table 'datafusion.public.c' not found",
        ),
        ("SELECT * FROM TV", "table 'datafusion.public.TV' not found"),
    ];
    for (sql, expected) in legs {
        let error = execute(&ctx, &catalogs, sql).await.err().unwrap();
        assert_eq!(strip_prefix(&error.to_string()), *expected, "{sql}");
    }
}

#[tokio::test]
async fn dml_wrong_case_refuses_and_leaves_the_table_under_case_sensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    assert_unresolved(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.u (ID, DATA) VALUES (7, 'q')",
        "`ID`",
        &["`id`", "`Data`"],
    )
    .await;
    assert_u_unchanged(&ctx, &catalogs).await;
    assert_unresolved(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.u SET Data = 'w' WHERE ID = 1",
        "`ID`",
        &["`id`", "`Data`"],
    )
    .await;
    assert_u_unchanged(&ctx, &catalogs).await;
    assert_unresolved(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.u WHERE ID = 5",
        "`ID`",
        &["`id`", "`Data`"],
    )
    .await;
    assert_u_unchanged(&ctx, &catalogs).await;
    let error = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.u t USING (SELECT 1 AS id, 'm' AS Data) s ON t.ID = s.id \
         WHEN MATCHED THEN UPDATE SET Data = s.Data",
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        strip_prefix(&error.to_string()),
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
         name `t`.`ID` cannot be resolved. Did you mean one of the following? [`t`.`id`, \
         `t`.`Data`, `s`.`id`, `s`.`Data`]. SQLSTATE: 42703"
    );
    assert_u_unchanged(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.u (id, Data) VALUES (3, 'c')",
    )
    .await;
    let (_, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.u").await;
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["3".to_string(), "c".to_string()],
            vec!["5".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn default_session_keeps_folding() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    register_probe_view(&ctx, &catalogs).await;
    let legs: &[AnswerLeg<'_>] = &[
        ("SELECT T.id FROM ice.sales.t t", &["id"], &[&["1"], &["2"]]),
        (
            "WITH C AS (SELECT 1 AS a) SELECT * FROM c",
            &["a"],
            &[&["1"]],
        ),
        (
            "SELECT * FROM TV",
            &["id", "Data"],
            &[&["1", "a"], &["2", "b"]],
        ),
        (
            "SELECT count(*) AS n FROM ice.sales.t.SNAPSHOTS",
            &["n"],
            &[&["1"]],
        ),
        ("SELECT ID FROM ice.sales.t", &["ID"], &[&["1"], &["2"]]),
    ];
    for (sql, names, rows) in legs {
        let (actual_names, actual_rows) = names_and_rows(&ctx, &catalogs, sql).await;
        let expected_names = names.iter().map(ToString::to_string).collect::<Vec<_>>();
        let expected_rows = rows
            .iter()
            .map(|row| row.iter().map(ToString::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(actual_names, expected_names, "{sql}");
        assert_eq!(actual_rows, expected_rows, "{sql}");
    }
}

#[tokio::test]
async fn same_session_toggle_true_false_true_keeps_folding() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    assert_unresolved(
        &ctx,
        &catalogs,
        "SELECT ID FROM ice.sales.t",
        "`ID`",
        &["`s`", "`id`", "`Data`"],
    )
    .await;
    disable_case_sensitive(&ctx);
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT ID FROM ice.sales.t").await;
    assert_eq!(names, vec!["ID".to_string()]);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["2".to_string()]]);
    enable_case_sensitive(&ctx);
    assert_unresolved(
        &ctx,
        &catalogs,
        "SELECT ID FROM ice.sales.t",
        "`ID`",
        &["`s`", "`id`", "`Data`"],
    )
    .await;
}

#[tokio::test]
async fn exact_fragment_identifiers_quote_and_plan_under_case_sensitive() {
    use crate::merge_fragments::exact::{check_fragment_exact, check_identity_exact};
    let target = ["id".to_string(), "Data".to_string()];
    let source = ["id".to_string(), "Data".to_string()];
    let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
    let quoted = check_fragment_exact("t.Data = s.id", &scopes, None).unwrap();
    assert!(quoted.contains("`Data`"), "{quoted}");
    assert!(quoted.contains("`id`"), "{quoted}");
    let error = check_fragment_exact("t.ID = s.id", &scopes, None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
        "{error}"
    );
    let fields = ["id".to_string(), "Data".to_string()];
    let quoted = check_identity_exact("Data = 'w'", "u", &fields).unwrap();
    assert!(quoted.contains("`Data`"), "{quoted}");
    let error = check_identity_exact("ID = 1", "u", &fields).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
        "{error}"
    );
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.u t USING (SELECT 1 AS id, 'm' AS \"Data\") s \
         ON t.Data = s.Data WHEN MATCHED THEN UPDATE SET Data = s.Data",
    )
    .await;
    assert_u_unchanged(&ctx, &catalogs).await;
}

#[test]
fn missing_fragment_identifiers_pass_through_to_the_planner() {
    use crate::merge_fragments::exact::{check_fragment_exact, check_identity_exact};
    let target = ["id".to_string(), "Data".to_string()];
    let source = ["id".to_string(), "Data".to_string()];
    let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
    let passed = check_fragment_exact("nope = 1", &scopes, None).unwrap();
    assert!(!passed.contains('`'), "{passed}");
    assert!(passed.contains("nope"), "{passed}");
    let fields = ["id".to_string(), "Data".to_string()];
    let passed = check_identity_exact("nope = 1", "u", &fields).unwrap();
    assert!(!passed.contains('`'), "{passed}");
    assert!(passed.contains("nope"), "{passed}");
}

#[tokio::test]
async fn update_set_targets_match_exactly_under_case_sensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    assert_unresolved(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.u SET DATA = 'w' WHERE id = 1",
        "`DATA`",
        &["`id`", "`Data`"],
    )
    .await;
    assert_u_unchanged(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.u SET Data = 'w' WHERE id = 1",
    )
    .await;
    let (_, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.u").await;
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "w".to_string()],
            vec!["5".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn metadata_table_names_ignore_case_under_both_settings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (_, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT count(*) AS n FROM ice.sales.t.SNAPSHOTS",
    )
    .await;
    assert_eq!(rows, vec![vec!["1".to_string()]]);
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    enable_case_sensitive(&ctx);
    let (_, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT count(*) AS n FROM ice.sales.t.SNAPSHOTS",
    )
    .await;
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}
