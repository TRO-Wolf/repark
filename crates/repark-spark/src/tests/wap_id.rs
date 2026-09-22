use std::collections::HashSet;

use super::super::*;
use super::common::*;
use super::session_write_conf::{set_session_conf, unset_session_conf};

use crate::wap::WapSessionConfig;

pub(crate) const WAP_DDL: &str = "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
     TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')";
const PLAIN_DDL: &str = "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
     TBLPROPERTIES ('format-version'='2')";

pub(crate) fn set_wap(ctx: &SessionContext, branch: Option<&str>, id: Option<&str>) {
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

pub(crate) async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry, ddl: &str) {
    run(ctx, catalogs, ddl).await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t SELECT 1 AS id, 'a' AS name",
    )
    .await;
}

pub(crate) async fn ids(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i32> {
    let mut found = time_travel_id_multiset(ctx, catalogs, sql).await;
    found.sort_unstable();
    found
}

pub(crate) async fn staged_snapshot_id(catalogs: &CatalogRegistry, wap_id: &str) -> i64 {
    let table = load_sales_table(catalogs, "t").await;
    let mut found = None;
    for snapshot in table.metadata().snapshots() {
        if snapshot
            .summary()
            .additional_properties
            .get("wap.id")
            .is_some_and(|value| value == wap_id)
        {
            assert!(
                found.is_none(),
                "one staged snapshot carries wap.id {wap_id}"
            );
            found = Some(snapshot.snapshot_id());
        }
    }
    found.expect("a staged snapshot carries the wap id")
}

pub(crate) async fn main_snapshot_id(catalogs: &CatalogRegistry) -> i64 {
    load_sales_table(catalogs, "t")
        .await
        .metadata()
        .snapshot_for_ref("main")
        .expect("main ref exists")
        .snapshot_id()
}

pub(crate) async fn snapshot_count(catalogs: &CatalogRegistry) -> usize {
    load_sales_table(catalogs, "t")
        .await
        .metadata()
        .snapshots()
        .count()
}

pub(crate) async fn ref_heads(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> Vec<(String, String, i64)> {
    use datafusion::arrow::array::AsArray;

    let batches = execute(
        ctx,
        catalogs,
        "SELECT name, type, snapshot_id FROM ice.sales.t.refs ORDER BY name",
    )
    .await
    .expect("refs read")
    .collect()
    .await
    .unwrap();
    let mut heads = Vec::new();
    for batch in &batches {
        let names = batch.column(0).as_string::<i32>();
        let kinds = batch.column(1).as_string::<i32>();
        let ids = batch
            .column(2)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            heads.push((
                names.value(row).to_string(),
                kinds.value(row).to_string(),
                ids.value(row),
            ));
        }
    }
    heads
}

#[tokio::test]
async fn wap_id_stages_the_insert_and_leaves_main_put() {
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
        load_sales_table(&catalogs, "t")
            .await
            .metadata()
            .snapshots()
            .count(),
        2,
        "the log holds the seed and the staged snapshot"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), seed_id)],
        "main alone exists and still points at the seed"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    assert_ne!(staged, seed_id, "the staged snapshot is a new snapshot");
    let table = load_sales_table(&catalogs, "t").await;
    assert_eq!(
        table
            .metadata()
            .snapshot_by_id(staged)
            .expect("staged snapshot in the log")
            .parent_snapshot_id(),
        Some(seed_id),
        "the staged snapshot hangs off the seed"
    );
    let props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn wap_id_on_a_first_write_stages_without_creating_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 1 AS id, 'a' AS name",
    )
    .await;
    set_wap(&ctx, None, None);

    assert!(
        ref_heads(&ctx, &catalogs).await.is_empty(),
        "no ref exists, main was never created"
    );
    assert!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t")
            .await
            .is_empty(),
        "with no main head the read answers empty"
    );
    assert_eq!(
        load_sales_table(&catalogs, "t")
            .await
            .metadata()
            .snapshots()
            .count(),
        1,
        "the log holds only the staged snapshot"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    let table = load_sales_table(&catalogs, "t").await;
    assert_eq!(
        table
            .metadata()
            .snapshot_by_id(staged)
            .expect("staged snapshot in the log")
            .parent_snapshot_id(),
        None,
        "the staged first write has no parent"
    );
    let staged_summary = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        staged_summary.get("added-records").map(String::as_str),
        Some("1"),
        "the staged first write added one record"
    );
    assert_eq!(
        staged_summary.get("total-records").map(String::as_str),
        Some("1"),
        "the staged first write totals one record"
    );
}

#[tokio::test]
async fn publish_changes_fast_forwards_main_to_the_staged_snapshot() {
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
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await
    .expect("publish answers")
    .collect()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1, "one output batch");
    assert_eq!(batches[0].num_rows(), 1, "one output row");
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "source_snapshot_id");
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int64,
        "source id reads back as Int64"
    );
    assert!(!schema.field(0).is_nullable(), "source id is non-null");
    assert_eq!(schema.field(1).name(), "current_snapshot_id");
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Int64,
        "current id reads back as Int64"
    );
    assert!(!schema.field(1).is_nullable(), "current id is non-null");
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
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the fast-forward moves main without adding a snapshot"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), staged)],
        "main alone exists and points at the published snapshot"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn publish_changes_replays_over_an_intervening_commit() {
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
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 3 AS id, 'c' AS name",
    )
    .await;

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
        source, staged,
        "the replay still reports the staged snapshot as its source"
    );
    assert_ne!(
        current, staged,
        "an intervening commit forces a replay, not a fast-forward"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3],
        "the replay carries the staged row onto main"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        current,
        "main moved to the replayed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        4,
        "the log holds seed, staged, intervening and replay"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), current)],
        "main alone exists and points at the replayed snapshot"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let published = table
        .metadata()
        .snapshot_by_id(current)
        .expect("published snapshot in the log");
    assert_eq!(
        published
            .summary()
            .additional_properties
            .get("published-wap-id")
            .map(String::as_str),
        Some("w1"),
        "the replay stamps the published wap id"
    );
    let staged_props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
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

#[tokio::test]
async fn publish_changes_with_an_unknown_wap_id_raises_the_bare_message() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'nope')",
    )
    .await
    .expect_err("an unknown wap id must refuse");
    let expected = "Cannot apply unknown WAP ID 'nope'";
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
        1,
        "the refused publish left only the seed snapshot"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), seed_id)],
        "main alone exists and still points at the seed"
    );
}

#[tokio::test]
async fn wap_enabled_without_an_id_stays_a_normal_commit() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the property alone stages nothing"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the normal commit appended one snapshot"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), head)],
        "main alone exists and points at the new head"
    );
}

#[tokio::test]
async fn wap_id_without_the_table_property_stays_a_normal_commit() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, PLAIN_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "without the property the id conf is ignored entirely"
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert!(
        table.metadata().snapshots().all(|snapshot| {
            !snapshot
                .summary()
                .additional_properties
                .contains_key("wap.id")
        }),
        "no snapshot carries a staged wap id"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the normal commit appended one snapshot"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), head)],
        "main alone exists and points at the new head"
    );
}

#[tokio::test]
async fn without_either_wap_key_the_write_sql_passes_through_untouched() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    let mut pinned = crate::time_travel::PinnedViews::default();
    let sql = "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name";
    let rewritten =
        crate::write_to_branch::apply_write_to_branch(&ctx, &catalogs, sql, &mut pinned, false)
            .await
            .expect("the fast path never fails");
    assert!(
        matches!(rewritten, std::borrow::Cow::Borrowed(_)),
        "with no wap key the router borrows the input"
    );
    assert_eq!(rewritten.as_ref(), sql, "the sql is byte-identical");
    pinned.release(&ctx);
}

#[tokio::test]
async fn a_wap_id_staged_snapshot_cherrypicks_onto_main() {
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
        &format!(
            "CALL ice.system.cherrypick_snapshot(table => 'sales.t', snapshot_id => {staged})"
        ),
    )
    .await
    .expect("cherry-pick answers")
    .collect()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1, "one output batch");
    assert_eq!(batches[0].num_rows(), 1, "one output row");
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "source_snapshot_id");
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int64,
        "source id reads back as Int64"
    );
    assert!(!schema.field(0).is_nullable(), "source id is non-null");
    assert_eq!(schema.field(1).name(), "current_snapshot_id");
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Int64,
        "current id reads back as Int64"
    );
    assert!(!schema.field(1).is_nullable(), "current id is non-null");
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
        "a fast-forward cherry-pick lands on the staged snapshot itself"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the staged snapshot publishes through the cherry-pick"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_eq!(
        head, staged,
        "the cherry-pick fast-forwards main to the staged snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the cherry-pick adds no snapshot of its own"
    );
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), head)],
        "main alone exists and points at the staged snapshot"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn wap_id_leaves_delete_and_update_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;

    set_wap(&ctx, None, Some("w1"));
    run(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE id = 1").await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![2],
        "the delete commits to main, not to a stage"
    );
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET id = 7 WHERE id = 2",
    )
    .await;
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![7],
        "the update commits to main, not to a stage"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        4,
        "seed, insert, delete and update each committed"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), head)],
        "main alone exists and points at the update"
    );
}

#[tokio::test]
async fn wap_id_with_a_session_snapshot_property_stages_both_stamps() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
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
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), seed_id)],
        "main alone exists and still points at the seed"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    assert_ne!(staged, seed_id, "the staged snapshot is a new snapshot");
    let table = load_sales_table(&catalogs, "t").await;
    let staged_snapshot = table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log");
    assert_eq!(
        staged_snapshot.parent_snapshot_id(),
        Some(seed_id),
        "the staged snapshot hangs off the seed"
    );
    let props = &staged_snapshot.summary().additional_properties;
    assert_eq!(
        props.get("wap.id").map(String::as_str),
        Some("w1"),
        "the staged snapshot carries the wap id"
    );
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the staged snapshot carries the session stamp"
    );
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn wap_id_with_a_session_codec_stages_off_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.compression-codec", "gzip");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.compression-codec");
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
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), seed_id)],
        "main alone exists and still points at the seed"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    assert_ne!(staged, seed_id, "the staged snapshot is a new snapshot");
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn wap_id_with_statement_and_session_options_stages_all_stamps() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
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
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("wap.id").map(String::as_str),
        Some("w1"),
        "the staged snapshot carries the wap id"
    );
    assert_eq!(
        props.get("team").map(String::as_str),
        Some("a"),
        "the staged snapshot carries the statement stamp"
    );
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the staged snapshot carries the session stamp"
    );
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn wap_id_leaves_insert_overwrite_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 9 AS id, 'z' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![9],
        "the overwrite replaces main instead of staging"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the overwrite committed one snapshot on main"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), head)],
        "main alone exists and points at the overwrite"
    );
}
