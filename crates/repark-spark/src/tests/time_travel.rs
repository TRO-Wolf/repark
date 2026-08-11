/// I1 / R-TIME-TRAVEL: multi-snapshot table + VERSION AS OF / TIMESTAMP AS OF / branch / tag,
/// plus unknown-id loud error and current-read unaffected after time-travel reads.
use super::super::*;
use super::common::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // multi-snapshot matrix + error pins in one oracle
async fn time_travel_version_timestamp_branch_tag_and_errors() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    // Snapshot 1: CTAS three rows.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tt AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "tt".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table
        .metadata()
        .current_snapshot_id()
        .expect("s1 current snapshot");
    let s1_ts = table.metadata().snapshot_by_id(s1).unwrap().timestamp_ms();

    // Snapshot 2: append one row.
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tt SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 current snapshot");

    // Snapshot 3: overwrite to a single row (distinct multiset).
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.tt SELECT 9 AS id, 'z' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s3 = table
        .metadata()
        .current_snapshot_id()
        .expect("s3 current snapshot");
    assert_ne!(s1, s2);
    assert_ne!(s2, s3);

    // Tag at s1, branch at s2 (test-support ManageSnapshots seam).
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx
        .manage_snapshots()
        .create_tag("tag_s1", s1)
        .create_branch("branch_s2", s2);
    let tx = action.apply(tx).expect("apply create ref");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit refs");

    // VERSION AS OF snapshot id.
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt VERSION AS OF {s1}")
        )
        .await,
        vec![1, 2, 3]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt FOR SYSTEM_VERSION AS OF {s2}")
        )
        .await,
        vec![1, 2, 3, 4]
    );

    // TIMESTAMP AS OF — pin at s1's timestamp (latest with ts <= s1_ts is s1).
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {s1_ts}")
        )
        .await,
        vec![1, 2, 3]
    );
    // Latest-match pin (octo C1-Q-001 / C1-L-001): as-of s2_ts must be s2 multiset,
    // not s1 — distinguishes first-history-match from last-matching `<=` walk.
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2_ts = table.metadata().snapshot_by_id(s2).unwrap().timestamp_ms();
    let s3_ts = table.metadata().snapshot_by_id(s3).unwrap().timestamp_ms();
    assert!(s1_ts < s2_ts && s2_ts <= s3_ts);
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {s2_ts}")
        )
        .await,
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {s3_ts}")
        )
        .await,
        vec![9]
    );
    // Mid-interval (s1_ts, s2_ts) → still s1 (C1-L-002).
    let mid = s1_ts + ((s2_ts - s1_ts) / 2).max(1);
    if mid < s2_ts {
        assert_eq!(
            time_travel_id_multiset(
                &ctx,
                &catalogs,
                &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {mid}")
            )
            .await,
            vec![1, 2, 3]
        );
    }
    // Earlier than first snapshot → loud error.
    let early_err = execute(
        &ctx,
        &catalogs,
        &format!("SELECT * FROM ice.sales.tt TIMESTAMP AS OF {}", s1_ts - 1),
    )
    .await
    .expect_err("ts earlier than first snapshot must fail");
    assert!(
        early_err.to_string().contains("earlier")
            || early_err.to_string().contains("no Iceberg snapshot"),
        "got: {early_err}"
    );

    // Tag + branch refs via VERSION AS OF.
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.tt VERSION AS OF 'tag_s1'"
        )
        .await,
        vec![1, 2, 3]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.tt VERSION AS OF 'branch_s2'"
        )
        .await,
        vec![1, 2, 3, 4]
    );

    // Unknown snapshot id / ref → loud, naming it.
    let unknown_id = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.tt VERSION AS OF 999999999",
    )
    .await
    .expect_err("unknown snapshot id");
    assert!(
        unknown_id.to_string().contains("999999999"),
        "must name snapshot id, got: {unknown_id}"
    );
    let unknown_ref = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.tt VERSION AS OF 'no_such_ref'",
    )
    .await
    .expect_err("unknown ref");
    assert!(
        unknown_ref.to_string().contains("no_such_ref"),
        "must name ref, got: {unknown_ref}"
    );

    // Filter/projection composition on a pinned snapshot.
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt VERSION AS OF {s1} WHERE id >= 2")
        )
        .await,
        vec![2, 3]
    );

    // Current read still sees s3 (overwrite) after time-travel reads.
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.tt").await,
        vec![9]
    );
}
