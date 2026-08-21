/// C3-L-001 residual: unknown / deferred `CALL system.*` procedures fail loud listing
/// the supported set (the three I3 procs succeed separately).
use super::super::*;
use super::common::*;

/// MW-1: Spark's full six-column `expire_snapshots` result, in Spark's order and nullability.
///
/// Measured on a live Spark 4.0.1 + Iceberg 1.10.0 oracle — the 4.1.2 oracle cannot execute this
/// procedure (a Spark 4.0-to-4.1 `DataSourceV2Relation.create` signature break), which is why the
/// campaign runs two. Nullability comes from the shipping jar's `OUTPUT_TYPE` constant:
/// `iconst_1` on each `StructField`. This engine had pinned all six non-nullable while agreeing
/// with Spark on the two rewrite procedures — one procedure out of step, not a blanket policy.
fn assert_expire_schema_is_sparks(batch: &datafusion::arrow::array::RecordBatch) {
    assert_eq!(batch.num_columns(), 6, "expire result schema is Spark's");
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "deleted_data_files_count",
            "deleted_position_delete_files_count",
            "deleted_equality_delete_files_count",
            "deleted_manifest_files_count",
            "deleted_manifest_lists_count",
            "deleted_statistics_files_count",
        ]
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable()),
        "Spark declares all six expire columns nullable"
    );
}

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
    assert_expire_schema_is_sparks(&batches[0]);

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

/// MW-1: the expire result splits content files the way Spark does, with real numbers behind it.
///
/// **Measured on a live Spark 4.0.1 + Iceberg 1.10.0 oracle.** Three merge-on-read MERGEs plus a
/// compaction expire as `deleted_data_files_count=4` and
/// `deleted_position_delete_files_count=2`. The fork hands back ONE funnel
/// (`CleanupReport.deleted_content_files`) holding every path, so reporting it under Spark's
/// data-file name over-counts by exactly the delete files — on the very workload this campaign
/// exists for.
///
/// The classification is not lost, only discarded: every manifest entry carries `content_type()`.
///
/// **Why this reaches the position-delete column by rollback rather than by compaction:** this
/// engine's `rewrite_data_files` rewrites data files and KEEPS the position deletes (verified —
/// the compacted table still reads correctly, with the deletes still applied). Orphaning a
/// delete file through compaction is what `rewrite_position_delete_files` is for, and that is
/// MW-2. Rolling back past the MERGEs strands their delete files now, without waiting.
#[tokio::test]
async fn call_expire_splits_content_files_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mor (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    for id in 1..=3 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.mor VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "mor".into());
    let pre_merge = catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("pre-merge snapshot");

    // Two merge-on-read MERGEs, each rewriting the same row: position deletes accumulate.
    for value in ["x", "y"] {
        run(
            &ctx,
            &catalogs,
            &format!(
                "MERGE INTO ice.sales.mor AS t USING (SELECT 1 AS id, '{value}' AS v) AS s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v"
            ),
        )
        .await;
    }
    let delete_files = i64::try_from(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.mor.files WHERE content != 0",
        )
        .await,
    )
    .expect("delete-file count fits i64");
    assert!(
        delete_files > 0,
        "the MERGEs must actually write position deletes, else the split below proves nothing"
    );

    // Roll back past both MERGEs: their delete files are now unreachable from any live snapshot.
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.mor', snapshot_id => {pre_merge})"
        ),
    )
    .await
    .expect("rollback CALL");

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.mor', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire CALL");
    let batches = result.collect().await.expect("collect expire result");
    let column = |name: &str| -> i64 {
        let index = batches[0].schema().index_of(name).expect("column present");
        batches[0]
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count column")
            .value(0)
    };
    let data = column("deleted_data_files_count");
    let position = column("deleted_position_delete_files_count");
    let equality = column("deleted_equality_delete_files_count");

    assert_eq!(
        position, delete_files,
        "every stranded position delete must be reported under Spark's position-delete column, \
         not funnelled into the data-file count"
    );
    assert_eq!(
        equality, 0,
        "nothing here writes equality deletes — a measured control, not a placeholder"
    );
    // The control that makes this a SPLIT rather than a relabel: the rollback strands the
    // MERGEs' new data files too, so both columns must carry independent non-zero counts. Before
    // MW-1 the single funnel reported `data + position` under the data-file name alone.
    assert!(
        data > 0,
        "the rolled-back MERGEs strand data files as well; got {data}"
    );
}

/// MW-1: the maintenance fence is lifted for BOTH remote catalog policies.
///
/// The refusal was a v1 blast-radius decision, not a capability gap — nothing downstream of the
/// gate assumes a local filesystem. The owner ruled on 2026-08-21 to lift for both, so a
/// Glue-policy and an S3-Tables-policy catalog each execute the three procedures. What the fence
/// guarded against is a commit conflict the fork's own `validate_data_files_exist` already
/// catches loudly (fork `ENGINE_CONTRACT` §8), not corruption.
#[tokio::test]
async fn call_runs_against_both_remote_catalog_policies() {
    for policy in [
        LocationPolicy::RequireExplicitLocation,
        LocationPolicy::ServiceManagedLocation,
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        run(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.fence (id INT) USING iceberg",
        )
        .await;
        run(&ctx, &catalogs, "INSERT INTO ice.sales.fence VALUES (1)").await;

        let mut remote = CatalogRegistry::new();
        remote.insert(
            "ice".to_string(),
            Arc::clone(&catalogs["ice"]),
            policy.clone(),
        );

        for procedure in [
            "expire_snapshots(table => 'sales.fence')",
            "rewrite_data_files(table => 'sales.fence')",
        ] {
            execute(&ctx, &remote, &format!("CALL ice.system.{procedure}"))
                .await
                .unwrap_or_else(|error| {
                    panic!("{policy:?} must execute {procedure} after MW-1, got: {error}")
                });
        }
    }
}

/// MW-1 refusal preservation: an unknown catalog still refuses, on every policy. Lifting the
/// fence must not turn a typo into a silent no-op.
#[tokio::test]
async fn call_still_refuses_an_unknown_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL nosuchcatalog.system.expire_snapshots(table => 'sales.t')",
    )
    .await
    .expect_err("unknown catalog must refuse");
    let message = error.to_string();
    assert!(
        message.contains("nosuchcatalog"),
        "refusal must name the unknown catalog, got: {message}"
    );
}
