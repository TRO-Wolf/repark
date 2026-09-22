use super::super::*;
use super::common::*;
use super::wap_id::ref_heads;

use crate::wap::{
    WapSessionConfig, wap_from_config_map, wap_from_options, with_wap_session_config,
};

const WAP_DDL: &str = "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
     TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')";
const PLAIN_DDL: &str = "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
     TBLPROPERTIES ('format-version'='2')";

fn set_wap(ctx: &SessionContext, branch: Option<&str>, id: Option<&str>) {
    let state_lock = ctx.state_ref();
    let mut state = state_lock.write();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(WapSessionConfig {
            branch: branch.map(str::to_string),
            id: id.map(str::to_string),
        });
}

async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry, ddl: &str) {
    run(ctx, catalogs, ddl).await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t SELECT 1 AS id, 'a' AS name",
    )
    .await;
}

async fn ids(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i32> {
    let mut found = time_travel_id_multiset(ctx, catalogs, sql).await;
    found.sort_unstable();
    found
}

#[tokio::test]
async fn wap_branch_redirects_the_write_and_the_session_read() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;

    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the session's plain read follows the wap branch"
    );

    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "main never moved"
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t VERSION AS OF 'audit'"
        )
        .await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn wap_branch_without_the_table_property_writes_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, PLAIN_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;

    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "write.wap.enabled is absent, so the conf is ignored entirely"
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t VERSION AS OF 'audit'"
        )
        .await,
        vec![1]
    );
}

#[tokio::test]
async fn an_explicit_branch_selector_wins_over_the_wap_conf() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH other",
    )
    .await;

    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_other SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t VERSION AS OF 'other'"
        )
        .await,
        vec![1, 2]
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t VERSION AS OF 'audit'"
        )
        .await,
        vec![1],
        "the conf's branch is untouched by an explicit selector"
    );
}

#[tokio::test]
async fn the_write_creates_a_wap_branch_that_does_not_exist() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t VERSION AS OF 'audit'"
        )
        .await,
        vec![1, 2],
        "the commit created the branch from main"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1]
    );
}

#[tokio::test]
async fn a_first_write_under_the_branch_conf_creates_only_the_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 1 AS id, 'a' AS name",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the session's plain read follows the branch the write created"
    );
    set_wap(&ctx, None, None);

    let table = load_sales_table(&catalogs, "t").await;
    assert_eq!(
        table.metadata().snapshots().count(),
        1,
        "the log holds only the branch commit"
    );
    let branch_head = table
        .metadata()
        .snapshots()
        .next()
        .expect("one snapshot in the log")
        .snapshot_id();
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("audit".to_string(), "BRANCH".to_string(), branch_head)],
        "only the branch exists, main was never created"
    );
    assert!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t")
            .await
            .is_empty(),
        "with no main head the read answers empty"
    );
}

#[tokio::test]
async fn explicit_version_as_of_main_reads_main_under_the_wap_conf() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;
    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t VERSION AS OF 'main'"
        )
        .await,
        vec![1],
        "an explicit selector outranks the conf on the read side too"
    );
    set_wap(&ctx, None, None);
}

#[tokio::test]
async fn wap_branch_with_wap_id_refuses_with_javas_text() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;

    set_wap(&ctx, Some("audit"), Some("w1"));
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await
    .expect_err("both WAP keys must refuse");
    assert!(
        error
            .to_string()
            .contains("Cannot set both WAP ID and branch, but got ID [w1] and branch [audit]"),
        "refusal must carry Java's text: {error}"
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the refused statement wrote nothing"
    );
}

#[tokio::test]
async fn the_metadata_tables_ignore_the_wap_conf() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;
    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    let snapshots = rows(
        &ctx,
        &catalogs,
        "SELECT operation FROM ice.sales.t.snapshots",
    )
    .await;
    set_wap(&ctx, None, None);
    assert_eq!(snapshots, 2, "both snapshots stay visible in the log");
}

#[test]
fn the_carrier_round_trips_through_the_config_map() {
    let mut map = HashMap::new();
    map.insert("spark.wap.branch".to_string(), " audit ".to_string());
    map.insert("spark.wap.id".to_string(), String::new());
    let wap = wap_from_config_map(&map);
    assert_eq!(wap.branch.as_deref(), Some("audit"), "the value is trimmed");
    assert_eq!(wap.id, None, "an empty value clears the key");

    let config = with_wap_session_config(datafusion::prelude::SessionConfig::new(), wap);
    let read_back = wap_from_options(config.options());
    assert_eq!(read_back.branch.as_deref(), Some("audit"));

    let missing = wap_from_options(&datafusion::common::config::ConfigOptions::new());
    assert_eq!(missing, WapSessionConfig::default());
}

#[test]
fn the_carrier_refuses_a_key_it_does_not_serve() {
    let mut wap = WapSessionConfig::default();
    let error = wap
        .set_value("spark.wap.other", Some("x".to_string()))
        .expect_err("only the two WAP keys are served");
    assert!(error.to_string().contains("spark.wap.branch"), "{error}");
}

async fn seed_two_rows_and_a_wap_write(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> &'static str {
    seed(ctx, catalogs, WAP_DDL).await;
    run(ctx, catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH audit").await;
    set_wap(ctx, Some("audit"), None);
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    "ice.sales.t"
}

#[tokio::test]
async fn a_comma_from_list_redirects_every_relation() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_two_rows_and_a_wap_write(&ctx, &catalogs).await;

    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT a.id FROM ice.sales.t a, ice.sales.t b"
        )
        .await,
        vec![1, 1, 2, 2],
        "the first comma relation reads the branch"
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT b.id FROM ice.sales.t a, ice.sales.t b"
        )
        .await,
        vec![1, 1, 2, 2],
        "the second comma relation reads the branch, not main"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT a.id, b.id, c.id FROM ice.sales.t a, ice.sales.t b, ice.sales.t c"
        )
        .await,
        8,
        "a three-way comma list is 2x2x2 on the branch"
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT s.id FROM (SELECT id FROM ice.sales.t) s, ice.sales.t b"
        )
        .await,
        vec![1, 1, 2, 2],
        "a comma relation after a subquery is redirected too"
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "WITH c AS (SELECT id FROM ice.sales.t) SELECT c.id FROM c, ice.sales.t b"
        )
        .await,
        vec![1, 1, 2, 2],
        "a CTE body and the comma relation beside it both read the branch"
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.t WHERE id = 2)"
        )
        .await,
        vec![2],
        "an IN subquery reads the branch"
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT a.id FROM ice.sales.t a, ice.sales.t b"
        )
        .await,
        1,
        "with the conf cleared every comma relation is back on main"
    );
}

#[tokio::test]
async fn a_comma_relation_keeps_the_explicit_selector_and_the_select_list() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_two_rows_and_a_wap_write(&ctx, &catalogs).await;

    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT b.id FROM ice.sales.t.branch_main a, ice.sales.t b"
        )
        .await,
        vec![1, 2],
        "an explicit branch_main selector is left on main, the plain name follows the conf"
    );
    assert_eq!(
        ids(
            &ctx,
            &catalogs,
            "SELECT a.id FROM ice.sales.t.branch_main a, ice.sales.t b"
        )
        .await,
        vec![1, 1],
        "the explicit selector's rows are main's single row"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT id, name FROM ice.sales.t GROUP BY id, name",
        )
        .await,
        2,
        "a GROUP BY comma list is not a relation list"
    );
    set_wap(&ctx, None, None);
}

#[tokio::test]
async fn a_delete_creates_the_wap_branch_that_does_not_exist() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;

    set_wap(&ctx, Some("audit"), None);
    run(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE id = 1").await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![2],
        "the delete landed on the branch the statement created"
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "main never moved"
    );
}

#[tokio::test]
async fn an_update_creates_the_wap_branch_that_does_not_exist() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;

    set_wap(&ctx, Some("audit"), None);
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET id = 7 WHERE id = 1",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![2, 7],
        "the update landed on the branch the statement created"
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "main never moved"
    );
}
