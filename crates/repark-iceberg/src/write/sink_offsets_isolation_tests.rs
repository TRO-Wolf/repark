use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::*;
use crate::write::merge::{InsertAction, InsertClause, MergeSpec, execute_merge};
use crate::write::predicate_dml::{PredicateDmlSpec, execute_predicate_dml};
use crate::write::session_write_conf::{SESSION_SNAPSHOT_PREFIX, apply_session_write_key};

const MERGE_ISOLATION: &str = "write.merge.isolation-level";
const UPDATE_ISOLATION: &str = "write.update.isolation-level";
const DELETE_ISOLATION: &str = "write.delete.isolation-level";

fn session(guard: Option<&BatchScopeGuard>) -> SessionContext {
    let mut config = SessionConfig::new();
    if let Some(guard) = guard {
        assert!(apply_session_write_key(
            config.options_mut(),
            &format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}"),
            &guard.token().to_string(),
        ));
    }
    SessionContext::new_with_config(config)
}

fn register_ids(ctx: &SessionContext, name: &str, ids: &[i32]) {
    let batch = id_batch(ids);
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("mem table");
    ctx.register_table(name, Arc::new(table)).expect("register");
}

fn merge_window_into(ident: &TableIdent) -> MergeSpec {
    MergeSpec {
        target: ident.clone(),
        target_alias: String::from("t"),
        source_from_sql: String::from("window"),
        source_alias: String::from("s"),
        on_sql: String::from("t.id = s.id"),
        matched: Vec::new(),
        not_matched: vec![InsertClause {
            predicate_sql: None,
            action: InsertAction::All,
        }],
        not_matched_by_source: Vec::new(),
        commit_branch: None,
        case_insensitive: false,
        schema_evolution: false,
    }
}

fn keyed_dml(ident: &TableIdent, update: bool) -> PredicateDmlSpec {
    PredicateDmlSpec {
        target: ident.clone(),
        target_alias: ident.name().to_string(),
        selection_sql: String::from("id IN (SELECT id FROM keys)"),
        assignments: update.then(|| vec![(String::from("id"), String::from("50"))]),
        case_insensitive: true,
        branch: None,
    }
}

fn refusal(error: &DataFusionError) -> MicroBatchError {
    match error.find_root() {
        DataFusionError::External(inner) => inner
            .downcast_ref::<MicroBatchError>()
            .unwrap_or_else(|| panic!("expected a MicroBatchError cause, got {error:?}"))
            .clone(),
        _ => panic!("expected an external MicroBatchError, got {error:?}"),
    }
}

fn isolation_refused(ident: &TableIdent, property: &str) -> MicroBatchError {
    MicroBatchError::MergeIsolationRefused {
        sink: ident.to_string(),
        property: property.to_string(),
    }
}

fn stamps_at(table: &Table, epoch: &str) -> usize {
    table
        .metadata()
        .snapshots()
        .filter(|snapshot| {
            snapshot.summary().additional_properties.get(STAMP_KEY) == Some(&epoch.to_string())
        })
        .count()
}

#[tokio::test]
async fn a_stamped_merge_under_snapshot_isolation_refuses_before_commit_and_epoch_one_lands_once() {
    let (_warehouse, memory, ident) =
        fixture_with("i_merge_snapshot", &[(MERGE_ISOLATION, "snapshot")]).await;
    stamped_append(&memory, &ident, &stamp_for(0, SinkDoor::Table), &[1, 2]).await;
    let probe = Arc::new(ProbeCatalog::new(Arc::clone(&memory), ProbeMode::Forward));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    let sink_b = catalog.load_table(&ident).await.expect("run B loads");
    let mut stamp_a = stamp_for(1, SinkDoor::Table);
    stamp_a.record.run = RunId::fresh();
    let mut stamp_b = stamp_for(1, SinkDoor::Table);
    stamp_b.record.run = RunId::fresh();
    *probe.stamped_racer.lock().expect("stamped") =
        Some((stamp_a.clone(), stage(&sink_b, &[3]).await));

    let guard = BatchScope::enter(TableUuid::of(&sink_b), stamp_b).expect("enter");
    let ctx = session(Some(&guard));
    register_ids(&ctx, "window", &[3]);
    let error = execute_merge(&ctx, &catalog, &merge_window_into(&ident))
        .await
        .expect_err("a stamped MERGE under snapshot isolation must refuse");
    assert_eq!(refusal(&error), isolation_refused(&ident, MERGE_ISOLATION));
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    assert!(probe.seen().is_empty(), "the refusal comes before commit");

    probe.race_stamped(&ident).await.expect("run A commits");
    let sink = memory.load_table(&ident).await.expect("reload");
    assert_eq!(stamps_at(&sink, "1"), 1);
    assert_eq!(live_ids(&sink).await, vec![1, 2, 3]);
    let (key, _) = stamp_a.record.property().expect("property");
    let property = sink.metadata().properties().get(&key).expect("offsets");
    assert_eq!(
        SinkRecord::from_property(query(), property)
            .expect("property record")
            .run,
        stamp_a.record.run
    );
}

#[tokio::test]
async fn a_stamped_update_or_delete_under_snapshot_isolation_refuses_on_both_arms() {
    let cases = [
        ("i_cow_update", "copy-on-write", true),
        ("i_cow_delete", "copy-on-write", false),
        ("i_mor_update", "merge-on-read", true),
        ("i_mor_delete", "merge-on-read", false),
    ];
    for (name, mode, update) in cases {
        let (property, mode_key) = if update {
            (UPDATE_ISOLATION, "write.update.mode")
        } else {
            (DELETE_ISOLATION, "write.delete.mode")
        };
        let (_warehouse, catalog, ident) =
            fixture_with(name, &[(property, "snapshot"), (mode_key, mode)]).await;
        let seeded =
            stamped_append(&catalog, &ident, &stamp_for(0, SinkDoor::Table), &[1, 2]).await;
        let snapshots = seeded.metadata().snapshots().count();
        let guard = BatchScope::enter(TableUuid::of(&seeded), stamp_for(1, SinkDoor::ForeachBatch))
            .expect("enter");
        let ctx = session(Some(&guard));
        register_ids(&ctx, "keys", &[1]);
        let error = execute_predicate_dml(&ctx, &catalog, &keyed_dml(&ident, update))
            .await
            .expect_err(name);
        assert_eq!(
            refusal(&error),
            isolation_refused(&ident, property),
            "{name}"
        );
        assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted, "{name}");
        drop(guard);
        let reloaded = catalog.load_table(&ident).await.expect("reload");
        assert_eq!(reloaded.metadata().snapshots().count(), snapshots, "{name}");
        assert_eq!(live_ids(&reloaded).await, vec![1, 2], "{name}");
    }
}

#[tokio::test]
async fn a_stamped_merge_under_serializable_isolation_commits_its_stamp() {
    let (_warehouse, catalog, ident) =
        fixture_with("i_merge_serializable", &[(MERGE_ISOLATION, "serializable")]).await;
    stamped_append(&catalog, &ident, &stamp_for(0, SinkDoor::Table), &[1, 2]).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let stamp = stamp_for(1, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let ctx = session(Some(&guard));
    register_ids(&ctx, "window", &[3]);
    execute_merge(&ctx, &catalog, &merge_window_into(&ident))
        .await
        .expect("a stamped MERGE under serializable isolation commits");
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
    drop(guard);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_stamped_head(&reloaded, &stamp);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn an_unstamped_merge_under_snapshot_isolation_commits_as_before() {
    let (_warehouse, catalog, ident) =
        fixture_with("i_merge_unstamped", &[(MERGE_ISOLATION, "snapshot")]).await;
    append_plain(&catalog, &ident, &[1, 2]).await;
    let plain = session(None);
    register_ids(&plain, "window", &[3]);
    execute_merge(&plain, &catalog, &merge_window_into(&ident))
        .await
        .expect("an unscoped MERGE commits");
    let foreign = TableUuid::new(Uuid::parse_str(BRONZE_UUID).expect("uuid"));
    let guard = BatchScope::enter(foreign, stamp_for(1, SinkDoor::ForeachBatch)).expect("enter");
    let scoped_elsewhere = session(Some(&guard));
    register_ids(&scoped_elsewhere, "window", &[4]);
    execute_merge(&scoped_elsewhere, &catalog, &merge_window_into(&ident))
        .await
        .expect("a MERGE into a table outside the scope commits");
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_unstamped(&reloaded);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2, 3, 4]);
}
