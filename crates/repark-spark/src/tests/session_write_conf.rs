//! Session `spark.sql.iceberg.*` write confs on the Spark SQL doors.
//! pins: ice-session-write-conf-1/C-034

use super::super::*;
use super::common::*;

fn set_session_conf(ctx: &SessionContext, key: &str, value: &str) {
    let state = ctx.state_ref();
    let mut guard = state.write();
    assert!(repark_iceberg::write::apply_session_write_key(
        guard.config_mut().options_mut(),
        key,
        value
    ));
}

fn unset_session_conf(ctx: &SessionContext, key: &str) {
    let state = ctx.state_ref();
    let mut guard = state.write();
    assert!(repark_iceberg::write::unset_session_write_key(
        guard.config_mut().options_mut(),
        key
    ));
}

fn team_of(table: &iceberg::table::Table) -> Option<String> {
    table
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .summary()
        .additional_properties
        .get("team")
        .cloned()
}

#[tokio::test]
async fn session_team_stamps_plain_insert() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sess (id INT) USING iceberg",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    run(&ctx, &catalogs, "INSERT INTO ice.sales.sess VALUES (1)").await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    let table = load_sales_table(&catalogs, "sess").await;
    assert_eq!(team_of(&table).as_deref(), Some("a"));
}

#[tokio::test]
async fn session_team_stamps_cow_delete_overwrite() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sdel (id INT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.sdel VALUES (1)").await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    run(&ctx, &catalogs, "DELETE FROM ice.sales.sdel WHERE id = 1").await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    let table = load_sales_table(&catalogs, "sdel").await;
    assert_eq!(team_of(&table).as_deref(), Some("a"));
}

#[tokio::test]
async fn bogus_session_codec_refuses_naming_the_codec() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sbog (id INT) USING iceberg",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.compression-codec", "bogus");
    let error = execute(&ctx, &catalogs, "INSERT INTO ice.sales.sbog VALUES (1)")
        .await
        .expect_err("bogus session codec refuses");
    unset_session_conf(&ctx, "spark.sql.iceberg.compression-codec");
    assert!(error.to_string().contains("bogus"), "{error}");
    let table = load_sales_table(&catalogs, "sbog").await;
    assert!(table.metadata().current_snapshot().is_none());
}

#[tokio::test]
async fn unset_session_conf_restores_unstamped_writes() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.suns (id INT) USING iceberg",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    run(&ctx, &catalogs, "INSERT INTO ice.sales.suns VALUES (1)").await;
    let table = load_sales_table(&catalogs, "suns").await;
    assert_eq!(team_of(&table), None);
}
