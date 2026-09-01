//! REF — snapshot-ref READ selectors, the full retention grammar, and the WAP declaration.
//!
//! Oracle: live PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog, Java 17, 2026-09-01. Every
//! assertion here is a cell of that measurement, and the retention values are the oracle's own
//! `refs` rows rather than arithmetic done in the test.
//!
//! pins: ref-branch-tag-wap/C-001

use super::super::*;
use super::common::*;

/// Both `WITH SNAPSHOT RETENTION` halves in one clause, at the oracle's values.
/// pins: ref-branch-tag-wap/C-003
#[tokio::test]
async fn branch_snapshot_retention_takes_both_count_and_age_halves() {
    use datafusion::arrow::array::{Array, AsArray};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH both RETAIN 5 DAYS \
             WITH SNAPSHOT RETENTION 3 SNAPSHOTS 7 DAYS",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH hours WITH SNAPSHOT RETENTION 2 SNAPSHOTS 12 HOURS",
    )
    .await;

    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT name, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms \
             FROM ice.sales.t.refs WHERE name IN ('both', 'hours') ORDER BY name",
    )
    .await
    .expect("refs metadata")
    .collect()
    .await
    .unwrap();
    let batch = &batches[0];
    let names = batch.column(0).as_string::<i32>();
    let max_ref_age = batch
        .column(1)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
    let min_snaps = batch
        .column(2)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    let max_snap_age = batch
        .column(3)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
    assert_eq!(names.value(0), "both");
    assert_eq!(max_ref_age.value(0), 432_000_000);
    assert_eq!(min_snaps.value(0), 3);
    assert_eq!(max_snap_age.value(0), 604_800_000);
    assert_eq!(names.value(1), "hours");
    assert!(max_ref_age.is_null(1), "no RETAIN on the hours branch");
    assert_eq!(min_snaps.value(1), 2);
    assert_eq!(max_snap_age.value(1), 43_200_000);
}

/// The reversed retention order is a Spark parse error, so it refuses here too.
/// pins: ref-branch-tag-wap/C-003
#[tokio::test]
async fn branch_snapshot_retention_reversed_order_refuses_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH rev WITH SNAPSHOT RETENTION 7 DAYS 3 SNAPSHOTS",
    )
    .await
    .expect_err("reversed retention order must refuse");
    let message = error.to_string();
    assert!(
        message.contains("SNAPSHOT RETENTION"),
        "refusal must name the clause: {message}"
    );
    assert!(
        !message.contains("ParserError"),
        "refusal must not be a raw parser error: {message}"
    );
}

/// WAP is DECLARED: no publish procedure exists and the session confs are fail-closed.
/// pins: ref-branch-tag-wap/C-005
#[tokio::test]
async fn wap_publish_procedures_and_session_conf_refuse_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;

    for sql in [
        "CALL ice.system.fast_forward(table => 'sales.t', branch => 'main', to => 'audit')",
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
        "CALL ice.system.cherrypick_snapshot(table => 'sales.t', snapshot_id => 1)",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("no WAP publish procedure is implemented");
        let message = error.to_string();
        assert!(
            message.contains("is not supported"),
            "WAP procedure must refuse loud for {sql:?}, got: {message}"
        );
        assert!(
            message.contains("Supported procedures"),
            "the refusal must list what is supported for {sql:?}, got: {message}"
        );
    }

    for key in ["spark.wap.branch", "spark.wap.id"] {
        let error = execute(&ctx, &catalogs, &format!("SET {key} = 'audit'"))
            .await
            .expect_err("the WAP session confs must not be settable");
        assert!(
            error.to_string().contains("spark"),
            "the refusal must name the rejected key {key}, got: {error}"
        );
    }

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z' AS name",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
    let audit_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'audit' ORDER BY id",
    )
    .await;
    assert_eq!(
        audit_ids,
        vec![1, 2, 3],
        "a refused WAP conf must leave the branch where it was"
    );
}

/// Spark's dotted ref selectors read the ref, on the branch and the tag spelling.
/// pins: ref-branch-tag-wap/C-002
#[tokio::test]
async fn branch_and_tag_read_selectors_resolve_the_ref() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG v1").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z' AS name",
    )
    .await;

    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_audit").await,
        vec![1, 2, 3],
        "the branch selector reads the branch head, not main"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.tag_v1").await,
        vec![1, 2, 3],
        "the tag selector reads the tag snapshot"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 9],
        "the plain name still reads main"
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT b.id FROM ice.sales.t.branch_audit AS b \
                 JOIN ice.sales.t AS m ON b.id = m.id",
        )
        .await,
        vec![1, 2, 3],
        "a selector joins against the live table"
    );

    let error = execute(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_nope")
        .await
        .expect_err("a missing ref must refuse");
    let message = error.to_string();
    assert!(
        message.contains("nope"),
        "the refusal must name the missing ref: {message}"
    );
    assert!(
        !message.contains("compound identifier"),
        "the refusal must not be the opaque 4-part planning error: {message}"
    );
}

/// A metadata-table suffix stays a metadata table, and a real table can be named `branch_x`.
/// pins: ref-branch-tag-wap/C-002
#[tokio::test]
async fn ref_selector_does_not_claim_metadata_tables_or_real_table_names() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.branch_exp AS SELECT * FROM src",
    )
    .await;

    assert!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t.snapshots").await >= 1,
        "a metadata-table suffix is not a ref selector"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.branch_exp").await,
        vec![1, 2, 3],
        "a table whose own name starts with branch_ is not a selector"
    );
}
