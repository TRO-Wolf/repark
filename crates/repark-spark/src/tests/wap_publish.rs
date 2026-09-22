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
