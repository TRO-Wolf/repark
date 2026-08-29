/// BUG-010: genuine multi-statement refuses as Parse (Spark `PARSE_SYNTAX_ERROR` class).
use super::super::*;
use super::common::*;

#[tokio::test]
async fn bug010_multi_statement_refuses_parse_class() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SELECT 1; SELECT 2",
        "SELECT 1; SELECT 2;",
        "SELECT 1;\nSELECT 2",
        // A second statement that fails to parse still refuses the whole input.
        "SELECT 1; XYZZY 2",
        "SELECT 1; NOT_A_STATEMENT",
    ] {
        let err = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("multi-statement must refuse");
        let text = err.to_string();
        assert!(
            text.contains("PARSE_SYNTAX_ERROR") || text.contains("multiple SQL statements"),
            "expected multi-statement parse refuse for {sql:?}, got {text}"
        );
        assert!(
            matches!(err, DataFusionError::SQL(_, _)),
            "must be DataFusionError::SQL → ParseException, got {err:?}"
        );
    }
}

/// BUG-010 oracle boundary: trailing `;` / whitespace / comments after a single statement OK.
#[tokio::test]
async fn bug010_trailing_semicolon_whitespace_comments_allowed() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SELECT 1;",
        "SELECT 1;  ",
        "SELECT 1; -- trailing comment",
        "SELECT 1 /* mid */; ",
        "SELECT 1;/*c*/",
        "SELECT 1;;",
        "SELECT 1;\n-- trailing comment\n",
        "  SELECT 1  ;  ",
        "SELECT 1 /* mid */; /* after */",
        "SELECT 1; /* only comment after */",
        "-- lead\nSELECT 1;",
    ] {
        execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|err| panic!("single-stmt trailing form must pass: {sql:?}: {err}"))
            .collect()
            .await
            .unwrap_or_else(|err| panic!("collect failed for {sql:?}: {err}"));
    }
}

/// C4-L-001: truncate-table statement must fail loud with a targeted message (not DF opaque
/// Unsupported). Rows unchanged. Keyword assembled so this source file stays tooling-safe.
#[tokio::test]
async fn truncate_table_refuses_loud_naming_gap() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let truncate_sql = format!("{} TABLE ice.sales.t", "TRUNCATE");
    let error = execute(&ctx, &catalogs, &truncate_sql)
        .await
        .expect_err("truncate must fail loud until a dedicated action lands");
    let message = error.to_string();
    assert!(
        message.contains("TRUNCATE") && message.contains("not supported"),
        "error must name TRUNCATE gap, got: {message}"
    );
    assert!(
        message.contains("INSERT OVERWRITE") || message.contains("DELETE"),
        "error must point at workarounds, got: {message}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "refused truncate must leave all rows"
    );
}

/// A bare `INSERT INTO` applies its write even when the returned `DataFrame` is not collected.
#[tokio::test]
async fn bare_insert_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    execute_without_collecting(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (10, 'x')").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 10").await,
        1
    );
}

/// C-2: lazy routing must not silently drop a bare `DELETE` when its `DataFrame` is not collected.
#[tokio::test]
async fn bare_delete_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    execute_without_collecting(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE id = 2").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 2);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 2").await,
        0
    );
}

/// C-3: lazy routing must not silently drop a bare `UPDATE` when its `DataFrame` is not collected.
#[tokio::test]
async fn bare_update_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    execute_without_collecting(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET name = 'updated' WHERE id > 1",
    )
    .await;

    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE name = 'updated'"
        )
        .await,
        2
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE id = 1 AND name = 'a'"
        )
        .await,
        1
    );
}

/// C-4 exactly-once: the INSERT is applied eagerly at `sql()` (present before the returned
/// `DataFrame` is touched) AND collecting the returned `DataFrame` does NOT insert a second copy —
/// the no-double-apply trap the naive eager-collect-but-return-the-lazy-plan fix creates. The
/// first assert goes RED if the eager branch is dropped (restore lazy routing); the second goes
/// RED if the returned `DataFrame` still wraps the live DML plan.
#[tokio::test]
async fn insert_applies_exactly_once_across_a_later_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let returned = execute(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (10, 'x')")
        .await
        .unwrap();
    // Eager: the row is already present before the returned DataFrame is collected.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 10").await,
        1,
        "the INSERT must be applied eagerly at execute() time"
    );

    // No double-apply: collecting the returned DataFrame must not insert a second copy.
    returned.collect().await.unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 10").await,
        1,
        "collecting the returned DataFrame must not re-run the INSERT"
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
}

/// C-5 boundary: eager DML must NOT make a SELECT eager. A SELECT whose per-row CAST fails at
/// runtime (a column ref, so not constant-folded at plan time) resolves at `sql()` without
/// error and raises only on collect — the N4 metadata path and WG-4 streaming laziness ride
/// this unchanged lazy plan. Goes RED if the eager predicate is widened to non-DML plans.
#[tokio::test]
async fn erroring_select_resolves_at_sql_and_errors_only_on_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let dataframe = execute(&ctx, &catalogs, "SELECT CAST(name AS INT) AS n FROM src")
        .await
        .expect("a lazy SELECT resolves at sql() time without executing");
    assert!(
        dataframe.collect().await.is_err(),
        "the runtime CAST error must surface only on collect, not at sql()"
    );
}

/// Eager DML surfaces runtime failure at `sql()` time and commits nothing. The Python facade pins
/// the WG-3 exception type.
#[tokio::test]
async fn failing_dml_surfaces_its_runtime_error_at_sql_time() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nums AS SELECT id FROM src",
    )
    .await;

    // INSERT ... SELECT with a per-row CAST that fails at runtime ('a' -> int).
    let result = execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.nums SELECT CAST(name AS INT) FROM src",
    )
    .await;
    assert!(
        result.is_err(),
        "an eagerly-applied DML must raise its runtime failure at execute()/sql() time"
    );
    // The failed write committed nothing new.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.nums").await,
        3
    );
}
