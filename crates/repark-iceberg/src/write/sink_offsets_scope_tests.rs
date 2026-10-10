use std::collections::BTreeSet;

use datafusion::common::config::ConfigOptions;

use super::*;
use crate::write::session_write_conf::{
    SESSION_SNAPSHOT_PREFIX, apply_session_write_key, resolve_write_for_session,
    session_write_conf_from_options,
};
use crate::write::write_options::WriterStagingOverrides;

fn head_summary_keys(table: &Table) -> BTreeSet<String> {
    table
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties
        .keys()
        .cloned()
        .collect()
}

fn head_carries_token(table: &Table) -> bool {
    table
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties
        .contains_key(SCOPE_TOKEN_KEY)
}

#[tokio::test]
async fn a_foreign_writer_inside_the_scope_commits_as_on_main_and_leaves_the_claim() {
    let (_warehouse, catalog, ident) = fixture("foreign_writer").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let plain_keys = head_summary_keys(&table);
    let stamp = stamp_for(5, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let other_session = catalog
        .load_table(&ident)
        .await
        .expect("other session load");
    let properties = other_session.metadata().properties().clone();
    let foreign_files = stage(&other_session, &[777]).await;
    let foreign = commit_append_with_summary(&catalog, &other_session, foreign_files, &[], None)
        .await
        .expect("an unrelated INSERT INTO from another session");
    assert_unstamped(&foreign);
    assert_eq!(head_summary_keys(&foreign), plain_keys);
    assert_eq!(foreign.metadata().properties(), &properties);
    assert_eq!(stamped_snapshots(&foreign), 0);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    let batch_files = stage(&table, &[2, 3]).await;
    let batch = commit_append_with_summary(&catalog, &table, batch_files, &scoped(&guard), None)
        .await
        .expect("the batch commit lands after the foreign one");
    assert_stamped_head(&batch, &stamp);
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
    drop(guard);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(
        read_resume_point(&reloaded, query()).expect("resume"),
        Some(stamp.record)
    );
    assert_eq!(stamped_snapshots(&reloaded), 1);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2, 3, 777]);
}

#[tokio::test]
async fn a_writer_with_a_wrong_token_does_not_claim() {
    let (_warehouse, catalog, ident) = fixture("wrong_token").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(2, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    assert_eq!(format!("{:?}", guard.token()), "ScopeToken(..)");
    assert_eq!(
        ScopeToken::parse(&guard.token().to_string()).as_ref(),
        Some(guard.token())
    );
    let forged = ScopeToken::parse("dddddddd-0000-4000-8000-0000000000d4").expect("token");
    let mut table = table;
    for wrong in [
        vec![forged.summary_entry()],
        vec![(SCOPE_TOKEN_KEY.to_string(), String::from("not-a-token"))],
    ] {
        let files = stage(&table, &[2]).await;
        table = commit_append_with_summary(&catalog, &table, files, &wrong, None)
            .await
            .expect("a commit carrying a wrong token");
        assert_unstamped(&table);
        assert!(!head_carries_token(&table));
        assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    }
    let files = stage(&table, &[3]).await;
    let table = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("the batch's own commit still claims");
    assert_stamped_head(&table, &stamp);
    assert_eq!(stamped_snapshots(&table), 1);
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
}

#[tokio::test]
async fn the_session_snapshot_property_carries_the_token_to_the_arm() {
    let (_warehouse, catalog, ident) = fixture("session_token").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let mut options = ConfigOptions::new();
    assert!(apply_session_write_key(
        &mut options,
        &format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}"),
        &guard.token().to_string(),
    ));
    let (extra, _) = resolve_write_for_session(
        &[],
        &WriterStagingOverrides::none(),
        &session_write_conf_from_options(&options),
    )
    .expect("resolve");
    let files = stage(&table, &[2]).await;
    let committed = commit_append_with_summary(&catalog, &table, files, &extra, None)
        .await
        .expect("stamped through the session config");
    assert_stamped_head(&committed, &stamp);
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
}

#[tokio::test]
async fn a_mismatched_stamp_only_commit_leaves_the_claim() {
    let (_warehouse, catalog, ident) = fixture("stamp_only_mismatch").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(3, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let other = stamp_for(4, SinkDoor::ForeachBatch);
    let error = commit_stamp_only(&catalog, &table, &other, Some(guard.token()))
        .await
        .expect_err("a stamp that differs from the scope's");
    assert!(
        matches!(error, MicroBatchError::Catalog(ref message) if message.contains("does not match")),
        "{error:?}"
    );
    let snapshot = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect("the scope's own stamp-only commit");
    assert_eq!(guard.outcome(), ScopeOutcome::Committed { snapshot });
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_stamped_head(&reloaded, &stamp);
    assert_eq!(stamped_snapshots(&reloaded), 1);
}

#[tokio::test]
async fn caller_extras_cannot_displace_the_stamp() {
    let (_warehouse, catalog, ident) = fixture("caller_stamp_keys").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let base_snapshots = table.metadata().snapshots().count();
    let stamp = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    for (key, value) in [
        ("repark.cdc.epoch", "99"),
        ("spark.sql.streaming.epochId", "99"),
        ("Repark.CDC.Epoch", "99"),
        ("repark.cdc.offsets.other", "{}"),
    ] {
        let mut extra = scoped(&guard);
        extra.push((key.to_string(), value.to_string()));
        let files = stage(&table, &[2]).await;
        let error = commit_append_with_summary(&catalog, &table, files, &extra, None)
            .await
            .expect_err("a caller stamp key on a stamped commit");
        assert!(
            matches!(microbatch_cause(&error), MicroBatchError::Catalog(message) if message.contains(key)),
            "{key}: {error:?}"
        );
        assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    }
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), base_snapshots);
    assert_eq!(stamped_snapshots(&reloaded), 0);
    let files = stage(&table, &[2]).await;
    let committed = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("the claim survives the refusals");
    assert_stamped_head(&committed, &stamp);
    drop(guard);
    let note = [(String::from("repark.cdc.note"), String::from("caller"))];
    let files = stage(&committed, &[3]).await;
    let refused = commit_append_with_summary(&catalog, &committed, files, &note, None)
        .await
        .expect_err("an unscoped commit cannot bring a reserved key either");
    assert!(
        refused.to_string().contains(
            "snapshot property repark.cdc.note is reserved for a streaming query's commit stamp"
        ),
        "{refused}"
    );
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(
        reloaded.metadata().current_snapshot_id(),
        committed.metadata().current_snapshot_id()
    );
}

#[test]
fn a_claimed_site_adds_the_stamp_and_the_starting_head_and_drops_the_token() {
    let stamp = stamp_for(7, SinkDoor::Table);
    let site = SiteStamp {
        claimed: Some(ClaimedStamp {
            stamp: stamp.clone(),
            base: None,
            started: Some(StartingMark {
                head: Some(SnapshotId::new(41)),
            }),
        }),
        sink: None,
        attempted: AtomicBool::new(false),
    };
    let token = ScopeToken::parse("eeeeeeee-0000-4000-8000-0000000000e5").expect("token");
    let extra = [
        token.summary_entry(),
        (String::from("run_id"), String::from("caller")),
    ];
    let engine = EngineSummary::default();
    let (_, folded) = site.summary(&extra, &engine).expect("summary");
    assert!(!folded.contains_key(SCOPE_TOKEN_KEY));
    assert_eq!(folded.get("run_id").map(String::as_str), Some("caller"));
    assert_eq!(
        SinkRecord::from_summary(&folded).expect("stamp"),
        Some(stamp.record)
    );
    assert_eq!(
        folded.get(SPARK_EPOCH_ID_KEY).map(String::as_str),
        Some("7")
    );
    assert_eq!(
        StartingMark::from_summary(&folded),
        Some(StartingMark {
            head: Some(SnapshotId::new(41))
        })
    );
    let forged = [(String::from("repark.cdc.epoch"), String::from("99"))];
    assert!(site.summary(&forged, &engine).is_err());
}

fn recovery_parts(error: MicroBatchError) -> (Epoch, Option<SinkRecord>, RecoveryReason, String) {
    let text = error.to_string();
    match error {
        MicroBatchError::RecoveryRequired {
            epoch,
            durable,
            reason,
            ..
        } => (epoch, durable.map(|record| *record), reason, text),
        other => panic!("expected RecoveryRequired, got {other:?}"),
    }
}

async fn rolled_back_to(catalog: &Arc<dyn Catalog>, table: &Table, snapshot: i64) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .manage_snapshots()
        .rollback_to(snapshot)
        .apply(tx)
        .expect("apply rollback");
    tx.commit(catalog.as_ref()).await.expect("rollback")
}

#[tokio::test]
async fn resume_refuses_a_rollback_before_every_stamp_as_not_in_lineage() {
    let (_warehouse, catalog, ident) = fixture("rollback_all").await;
    let plain = append_plain(&catalog, &ident, &[1]).await;
    let plain_id = plain.metadata().current_snapshot_id().expect("plain id");
    let stamp = stamp_for(0, SinkDoor::Table);
    let stamped = stamped_append(&catalog, &ident, &stamp, &[2]).await;
    let stamped_id = stamped
        .metadata()
        .current_snapshot_id()
        .expect("stamped id");
    let table = rolled_back_to(&catalog, &stamped, plain_id).await;
    assert!(table.metadata().snapshot_by_id(stamped_id).is_some());
    let (epoch, durable, reason, text) =
        recovery_parts(read_resume_point(&table, query()).expect_err("rolled back"));
    assert_eq!(epoch, Epoch::new(0));
    assert_eq!(durable, None);
    assert_eq!(
        reason,
        RecoveryReason::StampNotInLineage {
            snapshot: SnapshotId::new(stamped_id)
        }
    );
    for phrase in ["rolled back", "new queryName", "restore the sink"] {
        assert!(text.contains(phrase), "{phrase}: {text}");
    }
    assert!(!text.contains("expired"), "{text}");
}

#[tokio::test]
async fn resume_refuses_a_rollback_between_stamps_with_the_summary_as_authority() {
    let (_warehouse, catalog, ident) = fixture("rollback_mid").await;
    let first = stamp_for(0, SinkDoor::Table);
    let second = stamp_for(1, SinkDoor::Table);
    let after_first = stamped_append(&catalog, &ident, &first, &[1]).await;
    let first_id = after_first
        .metadata()
        .current_snapshot_id()
        .expect("first id");
    let table = stamped_append(&catalog, &ident, &second, &[2]).await;
    let table = rolled_back_to(&catalog, &table, first_id).await;
    let (epoch, durable, reason, _) =
        recovery_parts(read_resume_point(&table, query()).expect_err("rolled back"));
    assert_eq!(epoch, Epoch::new(0));
    assert_eq!(durable, Some(first.record));
    assert_eq!(
        reason,
        RecoveryReason::OffsetMismatch {
            summary_epoch: Some(Epoch::new(0)),
            property_epoch: Some(Epoch::new(1)),
        }
    );
}

#[tokio::test]
async fn resume_refuses_a_malformed_property_version_as_corrupt() {
    let (_warehouse, catalog, ident) = fixture("malformed_version").await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let table = stamped_append(&catalog, &ident, &stamp, &[1]).await;
    let (key, value) = stamp.record.property().expect("property");
    let canonical = "\"format-version\":1";
    for (label, text, newer) in [
        (
            "string",
            value.replace(canonical, "\"format-version\":\"1\""),
            false,
        ),
        (
            "float",
            value.replace(canonical, "\"format-version\":1.0"),
            false,
        ),
        ("not-json", String::from("{not json"), false),
        (
            "newer",
            value.replace(canonical, "\"format-version\":2"),
            true,
        ),
    ] {
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(key.clone(), text)
            .apply(tx)
            .expect("apply");
        let drifted = tx.commit(catalog.as_ref()).await.expect("drift");
        match read_resume_point(&drifted, query()) {
            Err(MicroBatchError::UnsupportedOffsetFormat { found, supported }) if newer => {
                assert_eq!((found.as_str(), supported), ("2", 1));
            }
            Err(MicroBatchError::Catalog(_)) if !newer => {}
            other => panic!("{label} gave {other:?}"),
        }
    }
}

fn warehouse_files(root: &std::path::Path) -> BTreeSet<std::path::PathBuf> {
    let mut files = BTreeSet::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path);
            }
        }
    }
    files
}

#[tokio::test]
async fn a_refused_merge_on_read_claim_stages_no_delete_file() {
    let (warehouse, catalog, ident) = fixture("mor_claim_first").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let seeded = stage(&table, &[1, 2]).await;
    let target: Arc<str> = Arc::from(seeded[0].file_path());
    let table = commit_append_with_summary(&catalog, &table, seeded, &[], None)
        .await
        .expect("seed");
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[3]).await;
    let table = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("the batch's one stamped commit");
    let added = stage(&table, &[10]).await;
    let before = warehouse_files(warehouse.path());
    let error = crate::write::merge::commit_row_delta_on_ref_with_partitions(
        &catalog,
        &table,
        table.metadata().current_snapshot_id(),
        vec![(target, 0)],
        added,
        WriteConcurrency::new(1).expect("K=1"),
        &Predicate::AlwaysTrue,
        None,
        crate::write::merge::KnownPartitions::new(),
        &scoped(&guard),
        &WriterStagingOverrides::none(),
    )
    .await
    .expect_err("a second stamped commit in one batch");
    assert_eq!(
        microbatch_cause(&error),
        &MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(0)
        }
    );
    assert_eq!(warehouse_files(warehouse.path()), before);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(live_ids(&reloaded).await, vec![1, 2, 3]);
    assert_eq!(stamped_snapshots(&reloaded), 1);
}

#[tokio::test]
async fn resume_reads_a_gap_in_the_ancestry_as_expiry_not_rollback() {
    let (_warehouse, catalog, ident) = fixture("ancestry_gap").await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let stamped = stamped_append(&catalog, &ident, &stamp, &[1]).await;
    let stamped_id = stamped
        .metadata()
        .current_snapshot_id()
        .expect("stamped id");
    let middle = append_plain(&catalog, &ident, &[2]).await;
    let middle_id = middle.metadata().current_snapshot_id().expect("middle id");
    let table = append_plain(&catalog, &ident, &[3]).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshots()
        .expire_snapshot_id(middle_id)
        .apply(tx)
        .expect("apply");
    let table = tx
        .commit(catalog.as_ref())
        .await
        .expect("expire the middle");
    assert!(table.metadata().snapshot_by_id(stamped_id).is_some());
    assert!(table.metadata().snapshot_by_id(middle_id).is_none());
    let (epoch, durable, reason, _) =
        recovery_parts(read_resume_point(&table, query()).expect_err("gap"));
    assert_eq!(epoch, Epoch::new(0));
    assert_eq!(durable, Some(stamp.record));
    assert_eq!(reason, RecoveryReason::StampedSnapshotExpired);
}

async fn expired(catalog: &Arc<dyn Catalog>, table: &Table, snapshot: i64) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .expire_snapshots()
        .expire_snapshot_id(snapshot)
        .apply(tx)
        .expect("apply expiry");
    tx.commit(catalog.as_ref()).await.expect("expire")
}

async fn v1_fixture(name: &str) -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("silver".to_string()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::new(NamespaceIdent::new("silver".to_string()), name.to_string());
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(name.to_string())
                .schema(id_schema())
                .format_version(FormatVersion::V1)
                .build(),
        )
        .await
        .expect("create v1 table");
    (warehouse, catalog, ident)
}

async fn rollback_past_the_stamp_after_expiry(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> (Table, i64) {
    let pause = std::time::Duration::from_millis(3);
    let oldest = append_plain(catalog, ident, &[1]).await;
    let oldest_id = oldest.metadata().current_snapshot_id().expect("oldest id");
    tokio::time::sleep(pause).await;
    let kept = append_plain(catalog, ident, &[2]).await;
    let kept_id = kept.metadata().current_snapshot_id().expect("kept id");
    tokio::time::sleep(pause).await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let stamped = stamped_append(catalog, ident, &stamp, &[3]).await;
    let stamped_id = stamped
        .metadata()
        .current_snapshot_id()
        .expect("stamped id");
    let table = rolled_back_to(catalog, &stamped, kept_id).await;
    let table = expired(catalog, &table, oldest_id).await;
    assert!(table.metadata().snapshot_by_id(oldest_id).is_none());
    assert!(table.metadata().snapshot_by_id(stamped_id).is_some());
    let (epoch, durable, reason, text) =
        recovery_parts(read_resume_point(&table, query()).expect_err("rolled back"));
    assert_eq!(epoch, Epoch::new(0));
    assert_eq!(durable, None);
    assert_eq!(
        reason,
        RecoveryReason::StampNotInLineage {
            snapshot: SnapshotId::new(stamped_id)
        }
    );
    assert!(!text.contains("expired"), "{text}");
    assert_eq!(live_ids(&table).await, vec![1, 2]);
    (table, stamped_id)
}

#[tokio::test]
async fn resume_refuses_a_rollback_past_the_stamp_after_routine_expiry_as_not_in_lineage() {
    let (_warehouse, catalog, ident) = fixture("rollback_after_expiry").await;
    let (table, stamped_id) = rollback_past_the_stamp_after_expiry(&catalog, &ident).await;
    let metadata = table.metadata();
    let head = metadata.current_snapshot().expect("head");
    let stamped = metadata.snapshot_by_id(stamped_id).expect("stamped");
    assert!(stamped.sequence_number() > head.sequence_number());
}

#[tokio::test]
async fn resume_reads_a_v1_rollback_after_expiry_by_timestamp() {
    let (_warehouse, catalog, ident) = v1_fixture("v1_rollback_after_expiry").await;
    let (table, stamped_id) = rollback_past_the_stamp_after_expiry(&catalog, &ident).await;
    let metadata = table.metadata();
    assert_eq!(metadata.format_version(), FormatVersion::V1);
    let head = metadata.current_snapshot().expect("head");
    let stamped = metadata.snapshot_by_id(stamped_id).expect("stamped");
    assert_eq!(stamped.sequence_number(), head.sequence_number());
    assert!(stamped.timestamp_ms() > head.timestamp_ms());
}

fn head_leaks_token(table: &Table, token: &str) -> bool {
    table
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties
        .iter()
        .any(|(key, value)| key.eq_ignore_ascii_case(SCOPE_TOKEN_KEY) || value == token)
}

fn session_extra(guard: &BatchScopeGuard) -> Vec<(String, String)> {
    let mut options = ConfigOptions::new();
    assert!(apply_session_write_key(
        &mut options,
        &format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}"),
        &guard.token().to_string(),
    ));
    let (extra, _) = resolve_write_for_session(
        &[(String::from("caller-key"), String::from("kept"))],
        &WriterStagingOverrides::none(),
        &session_write_conf_from_options(&options),
    )
    .expect("resolve");
    assert!(extra.iter().any(|(key, _)| key == SCOPE_TOKEN_KEY));
    extra
}

#[tokio::test]
async fn the_batch_session_token_never_lands_on_a_replace_or_overwrite_filter_commit() {
    let (_warehouse, catalog, ident) = fixture("token_choke_point").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(2, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let token = guard.token().to_string();
    let extra = session_extra(&guard);
    let files = stage(&table, &[5]).await;
    let replaced = crate::write::write_options::commit_replace_write_with_summary(
        &catalog, &table, files, &extra,
    )
    .await
    .expect("replace inside the batch");
    assert!(!head_leaks_token(&replaced, &token));
    assert!(head_summary_keys(&replaced).contains("caller-key"));
    assert_unstamped(&replaced);
    let files = stage(&replaced, &[6]).await;
    let filtered = crate::write::overwrite_filter::commit_overwrite_by_filter_with_summary(
        &catalog,
        &replaced,
        files,
        Predicate::AlwaysTrue,
        None,
        &extra,
        crate::write::FilterValidation::default(),
    )
    .await
    .expect("overwrite-filter inside the batch");
    assert!(!head_leaks_token(&filtered, &token));
    assert!(head_summary_keys(&filtered).contains("caller-key"));
    assert_unstamped(&filtered);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    let files = stage(&filtered, &[7]).await;
    let committed = commit_append_with_summary(&catalog, &filtered, files, &extra, None)
        .await
        .expect("the batch's own stamped commit");
    assert_stamped_head(&committed, &stamp);
    assert!(!head_leaks_token(&committed, &token));
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
}

#[tokio::test]
async fn a_replayed_or_case_variant_token_key_never_claims() {
    let (_warehouse, catalog, ident) = fixture("token_replay").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(3, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let token = guard.token().to_string();
    let files = stage(&table, &[2]).await;
    let replaced = crate::write::write_options::commit_replace_write_with_summary(
        &catalog,
        &table,
        files,
        &session_extra(&guard),
    )
    .await
    .expect("replace inside the batch");
    let foreign = catalog.load_table(&ident).await.expect("foreign load");
    let replayable: Vec<(String, String)> = foreign
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties
        .iter()
        .filter(|(key, _)| key.to_ascii_lowercase().starts_with("repark.cdc."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert!(replayable.is_empty(), "{replayable:?}");
    let mut current = replaced;
    for key in [
        SCOPE_TOKEN_KEY.to_ascii_uppercase(),
        String::from("Repark.Cdc.Scope-Token"),
    ] {
        let files = stage(&current, &[3]).await;
        current =
            commit_append_with_summary(&catalog, &current, files, &[(key, token.clone())], None)
                .await
                .expect("a case-variant key commits as on main");
        assert!(!head_leaks_token(&current, &token));
        assert_unstamped(&current);
        assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    }
    let files = stage(&current, &[4]).await;
    let committed = commit_append_with_summary(&catalog, &current, files, &scoped(&guard), None)
        .await
        .expect("the batch's own stamped commit");
    assert_stamped_head(&committed, &stamp);
    assert_eq!(stamped_snapshots(&committed), 1);
}

#[tokio::test]
async fn a_second_sink_write_in_one_batch_names_the_loss_and_the_fix() {
    let (_warehouse, catalog, ident) = fixture("two_writes_one_batch").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(4, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let token = guard.token().to_string();
    let extra = session_extra(&guard);
    let files = stage(&table, &[2]).await;
    let table = commit_append_with_summary(&catalog, &table, files, &extra, None)
        .await
        .expect("the batch's first sink write");
    let files = stage(&table, &[3]).await;
    let error = commit_append_with_summary(&catalog, &table, files, &extra, None)
        .await
        .expect_err("a second sink write in one batch");
    let cause = microbatch_cause(&error);
    assert_eq!(
        cause,
        &MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(4)
        }
    );
    let text = format!("{error} {cause:?}");
    assert!(!text.contains(&token), "{text}");
    for phrase in [
        "a restart resumes after epoch 4",
        "rows would never land",
        "once per batch body",
        "a single write",
    ] {
        assert!(text.contains(phrase), "{phrase}: {text}");
    }
    drop(guard);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(
        read_resume_point(&reloaded, query()).expect("resume"),
        Some(stamp.record)
    );
    assert_eq!(live_ids(&reloaded).await, vec![1, 2]);
}
