use super::super::*;
use super::common::*;

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
