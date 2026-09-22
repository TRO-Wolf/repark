use super::super::*;
use super::common::*;
use super::wap_id::{
    WAP_DDL, ids, main_snapshot_id, ref_heads, seed, set_wap, snapshot_count, staged_snapshot_id,
};

#[tokio::test]
async fn publish_changes_publishes_the_named_id_not_the_latest_stage() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, Some("w2"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 3 AS id, 'c' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    let first = staged_snapshot_id(&catalogs, "w1").await;
    let second = staged_snapshot_id(&catalogs, "w2").await;
    assert_ne!(first, second, "the two stages are distinct snapshots");

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await
    .expect("publish answers")
    .collect()
    .await
    .unwrap();
    let source = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let current = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        source, first,
        "the named stage is the source, not the latest stage"
    );
    assert_eq!(
        current, first,
        "main had not moved, so the publish fast-forwards"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        first,
        "main moved to the named stage"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the later stage stays staged"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        3,
        "the log holds the seed and the two stages"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), first)],
        "main alone exists and points at the published stage"
    );
}

#[tokio::test]
async fn publish_changes_with_a_non_unique_wap_id_raises_the_bare_message() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 3 AS id, 'c' AS name",
    )
    .await;
    set_wap(&ctx, None, None);

    let table = load_sales_table(&catalogs, "t").await;
    let mut hits = 0;
    for snapshot in table.metadata().snapshots() {
        if snapshot
            .summary()
            .additional_properties
            .get("wap.id")
            .is_some_and(|value| value == "w1")
        {
            hits += 1;
        }
    }
    assert_eq!(hits, 2, "two staged snapshots carry the same wap id");

    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await
    .expect_err("a non-unique wap id must refuse");
    let expected = "Cannot apply non-unique WAP ID. Found multiple snapshots with WAP ID 'w1'";
    let DataFusionError::External(inner) = &error else {
        panic!("expected an External marker, got {error:?}");
    };
    let marker = inner
        .downcast_ref::<repark_core::IllegalArgumentMarker>()
        .expect("expected an IllegalArgumentMarker");
    assert_eq!(marker.0, expected);
    match repark_core::engine_err(error) {
        repark_core::Error::IllegalArgument(message) => assert_eq!(message, expected),
        other => panic!("expected IllegalArgument, got {other:?}"),
    }
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the refused publish wrote nothing"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        3,
        "the refused publish left the seed and the two stages"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), seed_id)],
        "main alone exists and still points at the seed"
    );
}

#[tokio::test]
async fn publish_changes_with_positional_arguments_publishes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    let staged = staged_snapshot_id(&catalogs, "w1").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes('sales.t', 'w1')",
    )
    .await
    .expect("publish answers")
    .collect()
    .await
    .unwrap();
    let source = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let current = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(source, staged, "the staged snapshot is the source");
    assert_eq!(
        current, staged,
        "a fast-forward publish lands on the staged snapshot itself"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        staged,
        "main moved to the staged snapshot"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn a_plain_read_with_the_wap_id_still_set_answers_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the id stages the write but never redirects the read"
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the staged row stays invisible on main"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the log holds the seed and the staged snapshot"
    );
}

#[tokio::test]
async fn a_plain_insert_stages_a_partitioned_write_without_options() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.p (id INT, name STRING) USING iceberg PARTITIONED BY (id) \
         TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.p SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let seed_id = load_sales_table(&catalogs, "p")
        .await
        .metadata()
        .snapshot_for_ref("main")
        .expect("main ref exists")
        .snapshot_id();

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.p SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);

    let mut found = time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.p").await;
    found.sort_unstable();
    assert_eq!(found, vec![1], "the staged row is invisible on main");
    let table = load_sales_table(&catalogs, "p").await;
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("main")
            .expect("main ref exists")
            .snapshot_id(),
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        table.metadata().snapshots().count(),
        2,
        "the log holds the seed and the staged snapshot"
    );
    let mut hits = 0;
    for snapshot in table.metadata().snapshots() {
        if snapshot
            .summary()
            .additional_properties
            .get("wap.id")
            .is_some_and(|value| value == "w1")
        {
            hits += 1;
            let staged_props = &snapshot.summary().additional_properties;
            assert_eq!(
                staged_props.get("added-records").map(String::as_str),
                Some("1"),
                "the staged snapshot added one record"
            );
            assert_eq!(
                staged_props.get("total-records").map(String::as_str),
                Some("2"),
                "the staged snapshot totals two records"
            );
        }
    }
    assert_eq!(hits, 1, "exactly one staged snapshot carries wap.id w1");
}

#[tokio::test]
async fn publishing_two_staged_ids_in_order_replays_the_second() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, Some("w2"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 3 AS id, 'c' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    let first = staged_snapshot_id(&catalogs, "w1").await;
    let second = staged_snapshot_id(&catalogs, "w2").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await
    .expect("the first publish answers")
    .collect()
    .await
    .unwrap();
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        first,
        "the first publish fast-forwards"
    );

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w2')",
    )
    .await
    .expect("the second publish answers")
    .collect()
    .await
    .unwrap();
    let source = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let current = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(source, second, "the second stage is the source");
    assert_ne!(
        current, second,
        "main moved under the second stage, so it replays"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        current,
        "main moved to the replayed snapshot"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3],
        "the replay carries both staged rows onto main"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        4,
        "the log holds seed, both stages and the replay"
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert_eq!(
        table
            .metadata()
            .snapshot_by_id(current)
            .expect("published snapshot in the log")
            .summary()
            .additional_properties
            .get("published-wap-id")
            .map(String::as_str),
        Some("w2"),
        "the replay stamps the published wap id"
    );
}

#[tokio::test]
async fn an_insert_with_an_explicit_column_list_stages_under_the_wap_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t (id, name) SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the staged row is invisible on main"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the log holds the seed and the staged snapshot"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    assert_ne!(staged, seed_id, "the staged snapshot is a new snapshot");
}

#[tokio::test]
async fn a_wap_id_write_to_a_missing_table_answers_the_normal_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    set_wap(&ctx, None, Some("w1"));
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.missing SELECT 1 AS id",
    )
    .await
    .expect_err("a missing table must refuse");
    set_wap(&ctx, None, None);
    assert!(
        error.to_string().contains("missing"),
        "the normal missing-table error surfaces, not a wap failure: {error}"
    );
}

#[tokio::test]
async fn publishing_the_same_wap_id_twice_refuses_the_second_publish() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    let staged = staged_snapshot_id(&catalogs, "w1").await;

    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await
    .expect("the first publish answers")
    .collect()
    .await
    .unwrap();
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await
    .expect_err("the second publish must refuse");
    assert!(
        error
            .to_string()
            .contains("Duplicate request to cherry pick wap id that was published already: w1"),
        "the second publish of one stage refuses: {error}"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        staged,
        "main still points at the published snapshot"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the refused publish changed no rows"
    );
}
