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
    let unscoped = commit_append_with_summary(&catalog, &committed, files, &note, None)
        .await
        .expect("an unscoped commit keeps the caller's key as on main");
    let head = unscoped.metadata().current_snapshot().expect("head");
    assert_eq!(
        head.summary()
            .additional_properties
            .get("repark.cdc.note")
            .map(String::as_str),
        Some("caller")
    );
}

#[test]
fn site_stamp_extras_put_the_stamp_last_and_drop_the_token() {
    let stamp = stamp_for(7, SinkDoor::Table);
    let site = SiteStamp {
        claimed: Some(ClaimedStamp {
            stamp: stamp.clone(),
            base: None,
        }),
    };
    let token = ScopeToken::parse("eeeeeeee-0000-4000-8000-0000000000e5").expect("token");
    let extra = [
        token.summary_entry(),
        (String::from("repark.cdc.epoch"), String::from("99")),
        (
            String::from("spark.sql.streaming.epochId"),
            String::from("99"),
        ),
        (String::from("run_id"), String::from("caller")),
    ];
    let folded: HashMap<String, String> = site
        .extras(&extra)
        .expect("extras")
        .iter()
        .cloned()
        .collect();
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
}
