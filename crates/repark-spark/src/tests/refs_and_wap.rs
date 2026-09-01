use super::super::*;
use super::common::*;

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
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 8 AS id, 'y' AS name",
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
        vec![1, 2, 3, 8],
        "the tag selector reads the tag snapshot, which is ahead of the branch"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 8, 9],
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

    for (sql, ref_name) in [
        ("SELECT id FROM ice.sales.t.branch_nope", "nope"),
        ("SELECT id FROM ice.sales.t.tag_missing", "missing"),
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a missing ref must refuse");
        let message = error.to_string();
        assert!(
            message.contains(ref_name),
            "the refusal must name the missing ref for {sql:?}: {message}"
        );
        assert!(
            !message.contains("compound identifier"),
            "the refusal must not be the opaque 4-part planning error for {sql:?}: {message}"
        );
    }
}

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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ref_selector_on_the_read_side_of_dml_is_a_read() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG v1").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 4 AS id, 'd' AS name",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 5 AS id, 'e' AS name",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.tag_v1").await,
        vec![1, 2, 3],
        "fixture: the tag sits behind the branch"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1, 2, 3, 4],
        "fixture: the branch sits behind main"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 4, 5],
        "fixture: main is ahead of both refs"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.into_dst AS SELECT * FROM src WHERE 1 = 0",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.into_dst SELECT id, name FROM ice.sales.t.branch_b",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.into_dst").await,
        vec![1, 2, 3, 4],
        "INSERT … SELECT reads the branch, not main"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.del_dst AS SELECT * FROM ice.sales.t",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.del_dst WHERE id IN (SELECT id FROM ice.sales.t.tag_v1)",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.del_dst").await,
        vec![4, 5],
        "the subquery reads the tag, so only the tag's ids are deleted"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.upd_dst AS SELECT * FROM ice.sales.t",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.upd_dst SET name = 'hit' \
             WHERE id IN (SELECT id FROM ice.sales.t.tag_v1)",
    )
    .await;
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.upd_dst WHERE name = 'hit'",
        )
        .await,
        3,
        "the subquery reads the tag, so only the tag's three rows are updated"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mrg_dst AS SELECT * FROM src WHERE 1 = 0",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.mrg_dst d USING ice.sales.t.branch_b s ON d.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.mrg_dst").await,
        vec![1, 2, 3, 4],
        "MERGE's USING operand is a read of the branch"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ctas_dst AS SELECT id, name FROM ice.sales.t.tag_v1",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.ctas_dst").await,
        vec![1, 2, 3],
        "CTAS reads the tag"
    );
}

#[tokio::test]
async fn write_to_branch_refusal_claims_the_target_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG v1").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dst AS SELECT * FROM src WHERE 1 = 0",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b SELECT id, name FROM ice.sales.t.tag_v1",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1, 1, 2, 2, 3, 3],
        "INSERT from a tag selector appends the tag's rows onto the branch"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3],
        "main stays at the pre-insert rows"
    );
}
