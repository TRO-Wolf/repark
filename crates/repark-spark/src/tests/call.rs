/// C3-L-001 residual: unknown / deferred `CALL system.*` procedures fail loud listing
/// the supported set (the three I3 procs succeed separately).
use super::super::*;
use super::common::*;

#[tokio::test]
async fn call_unknown_procedure_refuses_loud_listing_supported() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.not_a_real_proc(table => 'sales.t')",
    )
    .await
    .expect_err("unknown CALL must fail loud");
    let message = error.to_string();
    assert!(
        message.contains("not supported") || message.contains("not_a_real_proc"),
        "error must name the unknown proc, got: {message}"
    );
    assert!(
        message.contains("expire_snapshots")
            && message.contains("rewrite_data_files")
            && message.contains("rollback_to_snapshot"),
        "error must list supported procedures, got: {message}"
    );
}

/// `remove_orphan_files` is a deliberate loud-unsupported (fork-queue; do not hand-roll).
#[tokio::test]
async fn call_remove_orphan_files_refuses_loud_with_fork_queue() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.remove_orphan_files(table => 'sales.t')",
    )
    .await
    .expect_err("remove_orphan_files must refuse loud");
    let message = error.to_string();
    assert!(
        message.contains("remove_orphan_files") && message.contains("not supported"),
        "error must name remove_orphan_files, got: {message}"
    );
    assert!(
        message.contains("fork") || message.contains("orphan"),
        "error must point at fork-queue residual, got: {message}"
    );
}

/// I3: `rollback_to_snapshot` restores the prior multiset; result columns match Spark.
#[tokio::test]
async fn call_rollback_to_snapshot_restores_multiset() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.roll AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "roll".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.roll SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 head before rollback");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.roll").await,
        4
    );

    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.roll', snapshot_id => {s1})"
        ),
    )
    .await
    .expect("rollback CALL");
    let batches = result.collect().await.expect("collect rollback result");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["previous_snapshot_id", "current_snapshot_id"]
    );
    let previous = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("previous_snapshot_id i64")
        .value(0);
    let current = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("current_snapshot_id i64")
        .value(0);
    // C1-Q-003: both result columns are load-bearing (not only current).
    assert_eq!(
        previous, s2,
        "previous_snapshot_id must be pre-rollback head"
    );
    assert_eq!(current, s1);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.roll").await,
        3,
        "after rollback read must equal s1 multiset (3 rows)"
    );
}

/// I3 load-bearing safety: expire keeps tag/branch-reachable snapshots (R133).
#[tokio::test]
async fn call_expire_snapshots_keeps_tag_reachable() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.exp AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "exp".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.exp SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 intermediate");
    assert_ne!(
        s1, s2,
        "fixture must produce distinct intermediate snapshot"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.exp SELECT 5 AS id, 'e' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s3 = table.metadata().current_snapshot_id().expect("s3 head");
    let snap_count_before = table.metadata().snapshots().count();
    assert!(snap_count_before >= 3);
    assert_ne!(s2, s3);

    // Tag at s1 — expire must not remove a ref-reachable snapshot (R133).
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().create_tag("keep_s1", s1);
    let tx = action.apply(tx).expect("apply tag");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit tag");

    // older_than = far future so age would expire every snapshot; retain_last(1) keeps
    // only main head by count; the tag alone keeps s1 reachable (R133 safety).
    // C1-Q-001 dual probe: s2 has no ref and is not in retain_last head → must expire
    // (proves expire ran; no-op success would leave s2).
    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.exp', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire CALL");
    let batches = result.collect().await.expect("collect expire result");
    assert_eq!(
        batches[0].num_columns(),
        4,
        "expire result schema (divergence set)"
    );
    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "deleted_data_files_count",
            "deleted_manifest_files_count",
            "deleted_manifest_lists_count",
            "deleted_statistics_files_count",
        ]
    );

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(
        table.metadata().snapshot_by_id(s1).is_some(),
        "tag-reachable snapshot s1 must survive expire (R133 safety pin)"
    );
    assert!(
        table.metadata().snapshot_for_ref("keep_s1").is_some(),
        "tag keep_s1 must still resolve"
    );
    assert!(
        table.metadata().snapshot_by_id(s2).is_none(),
        "untagged intermediate s2 must be expired — proves expire ran (C1-Q-001); \
             no-op CALL would leave s2 and still keep s1"
    );
    assert!(
        table.metadata().snapshot_by_id(s3).is_some(),
        "main head s3 retained by retain_last=1"
    );
    // Current read still works.
    assert!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.exp").await >= 3);
}

/// C2-Q-001: branch-reachable snapshot survives expire (not only tags).
#[tokio::test]
async fn call_expire_snapshots_keeps_branch_reachable() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.expb AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "expb".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expb SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 intermediate");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expb SELECT 5 AS id, 'e' AS name",
    )
    .await;

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().create_branch("audit", s1);
    let tx = action.apply(tx).expect("apply branch");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit branch");

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.expb', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire CALL");

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(
        table.metadata().snapshot_by_id(s1).is_some(),
        "branch-reachable s1 must survive expire"
    );
    assert!(
        table.metadata().snapshot_for_ref("audit").is_some(),
        "branch audit must still resolve"
    );
    assert!(
        table.metadata().snapshot_by_id(s2).is_none(),
        "untagged intermediate s2 must expire (dual probe)"
    );
}

/// I3: `rewrite_data_files` preserves row multiset and reduces file count on multi-small files.
#[tokio::test]
async fn call_rewrite_data_files_preserves_rows_and_reduces_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Seed ≥5 tiny files (default min_input_files=5) so bin-pack qualifies.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rw AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    for index in 2..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.rw SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
    let before_ids =
        time_travel_id_multiset(&ctx, &catalogs, "SELECT CAST(id AS INT) FROM ice.sales.rw").await;
    assert_eq!(before_ids, vec![1, 2, 3, 4, 5, 6]);

    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "rw".into());
    let files_before_count = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert!(
        files_before_count >= 5,
        "fixture must have ≥5 small files, got {files_before_count}"
    );

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rw')",
    )
    .await
    .expect("rewrite CALL");
    let batches = result.collect().await.expect("collect rewrite result");
    let batch = &batches[0];
    let rewritten = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap()
        .value(0);
    assert!(
        rewritten >= 2,
        "expected some files rewritten, got {rewritten}"
    );

    let after_ids =
        time_travel_id_multiset(&ctx, &catalogs, "SELECT CAST(id AS INT) FROM ice.sales.rw").await;
    assert_eq!(
        after_ids, before_ids,
        "rewrite must preserve row multiset byte-exactly (ids)"
    );

    let files_after_count = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert!(
        files_after_count < files_before_count,
        "rewrite must reduce file count ({files_after_count} < {files_before_count})"
    );
}

/// rewrite `strategy` / `sort_order` other than binpack → loud unsupported (R135 deferred).
#[tokio::test]
async fn call_rewrite_sort_strategy_refuses_loud() {
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
        "CALL ice.system.rewrite_data_files(table => 'sales.t', strategy => 'sort')",
    )
    .await
    .expect_err("sort strategy must refuse");
    let message = error.to_string();
    assert!(
        message.contains("sort") && message.contains("not supported"),
        "got: {message}"
    );
    assert!(
        message.contains("R135") || message.contains("binpack") || message.contains("zOrder"),
        "must name R135 deferred list, got: {message}"
    );

    // C1-L-001: positional strategy must refuse the same way — never silent binpack.
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.t', 'sort')",
    )
    .await
    .expect_err("positional sort strategy must refuse (not binpack)");
    let message = error.to_string();
    assert!(
        message.contains("sort") && message.contains("not supported"),
        "positional sort must refuse loud, got: {message}"
    );

    // C2-Q-003: positional binpack is accepted (not a blanket positional refuse).
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.t', 'binpack')",
    )
    .await
    .expect("positional binpack must be accepted");

    // C2-Q-002: third positional exceeds supported arity (not silent ignore).
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.t', 'binpack', 'id ASC')",
    )
    .await
    .expect_err("third positional must refuse");
    let message = error.to_string();
    assert!(
        message.contains("at most") || message.contains("positional"),
        "excess positional must name arity, got: {message}"
    );
}

/// C4-Q-001: expire `older_than` accepts TIMESTAMP string form (Spark docs example shape).
#[tokio::test]
async fn call_expire_older_than_timestamp_string() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.texp AS SELECT * FROM src",
    )
    .await;
    // Far-future timestamp string — age would expire everything; retain_last=1 keeps head.
    // Pins TypedString / string parse path (not only epoch-ms integers).
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.expire_snapshots(\
             table => 'sales.texp', older_than => '2099-01-01 00:00:00', retain_last => 1)",
    )
    .await
    .expect("TIMESTAMP string older_than must parse and run");
    assert!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.texp").await >= 1);
}

/// C4-Q-002: three-part table identity on CALL (`catalog.ns.table`) resolves.
#[tokio::test]
async fn call_table_three_part_ident() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t3 AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "t3".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t3 SELECT 4 AS id, 'd' AS name",
    )
    .await;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(\
                 table => 'ice.sales.t3', snapshot_id => {s1})"
        ),
    )
    .await
    .expect("three-part table ident on CALL");
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t3").await, 3);
}

/// C3-Q-001: `retain_last` must be `>= 1` or CALL fails at plan time.
#[tokio::test]
async fn call_expire_retain_last_zero_refuses_loud() {
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
        "CALL ice.system.expire_snapshots(table => 'sales.t', retain_last => 0)",
    )
    .await
    .expect_err("retain_last=0 must refuse");
    let message = error.to_string();
    assert!(
        message.contains("retain_last") && (message.contains(">= 1") || message.contains('1')),
        "got: {message}"
    );
}

/// C3-Q-002: mixing named and positional CALL args refuses (Spark procedures).
#[tokio::test]
async fn call_mixed_named_and_positional_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rollback_to_snapshot('sales.t', snapshot_id => 1)",
    )
    .await
    .expect_err("mixed args must refuse");
    let message = error.to_string();
    assert!(
        message.contains("mixing") || message.contains("named and positional"),
        "got: {message}"
    );
}

/// C3-Q-003: expire accepts full positional form (`table`, `older_than`, `retain_last`).
#[tokio::test]
async fn call_expire_positional_args() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.expp AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "expp".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expp SELECT 4 AS id, 'd' AS name",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expp SELECT 5 AS id, 'e' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().create_tag("keep", s1);
    let tx = action.apply(tx).expect("tag");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit tag");

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.expire_snapshots('sales.expp', {older_than_ms}, 1)"),
    )
    .await
    .expect("positional expire");
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(table.metadata().snapshot_by_id(s1).is_some());
}

/// C2-Q-004: rollback to a non-ancestor snapshot fails loud (fork R98).
#[tokio::test]
async fn call_rollback_non_ancestor_refuses_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.roll2 AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "roll2".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.roll2 SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table.metadata().current_snapshot_id().expect("s2");

    // Roll main to s1 — s2 remains in history as a *descendant*, not an ancestor.
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.roll2', snapshot_id => {s1})"
        ),
    )
    .await
    .expect("rollback to s1");

    let error = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.roll2', snapshot_id => {s2})"
        ),
    )
    .await
    .expect_err("non-ancestor s2 must refuse");
    let message = error.to_string();
    assert!(
        !message.is_empty(),
        "must fail loud on non-ancestor snapshot_id, got empty"
    );
    // Table still at s1 multiset (failed CALL must not move main).
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.roll2").await,
        3
    );
}

/// LOCAL-only: Glue-policy catalog refuses CALL expire/rewrite/rollback.
#[tokio::test]
async fn call_refuses_non_local_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Re-tag the same memory catalog as RequireExplicitLocation (Glue posture).
    let mut remote = CatalogRegistry::new();
    remote.insert(
        "ice".to_string(),
        Arc::clone(&catalogs["ice"]),
        LocationPolicy::RequireExplicitLocation,
    );
    // Need the provider still registered under ice — setup already did.
    let error = execute(
        &ctx,
        &remote,
        "CALL ice.system.expire_snapshots(table => 'sales.t')",
    )
    .await
    .expect_err("Glue-policy catalog must refuse CALL");
    let message = error.to_string();
    assert!(
        message.contains("LOCAL-only") || message.contains("RequireExplicitLocation"),
        "must name LOCAL gate, got: {message}"
    );

    // C1-Q-002: ServiceManagedLocation (S3 Tables) is a distinct arm — pin both policies.
    let mut s3_tables = CatalogRegistry::new();
    s3_tables.insert(
        "ice".to_string(),
        Arc::clone(&catalogs["ice"]),
        LocationPolicy::ServiceManagedLocation,
    );
    let error = execute(
        &ctx,
        &s3_tables,
        "CALL ice.system.expire_snapshots(table => 'sales.t')",
    )
    .await
    .expect_err("S3 Tables policy catalog must refuse CALL");
    let message = error.to_string();
    assert!(
        message.contains("LOCAL-only")
            || message.contains("ServiceManagedLocation")
            || message.contains("S3 Tables"),
        "must name S3/LOCAL gate, got: {message}"
    );
}
