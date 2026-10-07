use iceberg::expr::Reference;
use iceberg::spec::Datum;

use super::*;
use crate::write::write_options::WriterStagingOverrides;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeMode {
    Forward,
    FailBeforeLanding,
    UnknownAfterLanding,
    UnknownWithoutLanding,
    LandedThenReconcileFails,
    UnknownThenLoadsFail,
}

struct ProbeCatalog {
    inner: Arc<dyn Catalog>,
    mode: ProbeMode,
    seen: Mutex<Vec<String>>,
    racers: Mutex<Vec<Vec<DataFile>>>,
    stamped_racer: Mutex<Option<(CommitStamp, Vec<DataFile>)>>,
    failing_loads: AtomicUsize,
}

impl std::fmt::Debug for ProbeCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProbeCatalog")
    }
}

impl ProbeCatalog {
    fn new(inner: Arc<dyn Catalog>, mode: ProbeMode) -> Self {
        Self {
            inner,
            mode,
            seen: Mutex::new(Vec::new()),
            racers: Mutex::new(Vec::new()),
            stamped_racer: Mutex::new(None),
            failing_loads: AtomicUsize::new(0),
        }
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("seen").clone()
    }

    async fn race_plain(&self, ident: &TableIdent) -> iceberg::Result<()> {
        let racer = self.racers.lock().expect("racers").pop();
        let Some(files) = racer else {
            return Ok(());
        };
        let current = self.inner.load_table(ident).await?;
        let tx = Transaction::new(&current);
        let tx = tx.fast_append().add_data_files(files).apply(tx)?;
        tx.commit(self.inner.as_ref()).await.map(|_| ())
    }

    async fn race_stamped(&self, ident: &TableIdent) -> iceberg::Result<()> {
        let stamped = self.stamped_racer.lock().expect("stamped").take();
        let Some((stamp, files)) = stamped else {
            return Ok(());
        };
        let current = self.inner.load_table(ident).await?;
        let claimed = ClaimedStamp { stamp, base: None };
        let summary: HashMap<String, String> = claimed
            .summary_entries()
            .expect("racer entries")
            .into_iter()
            .collect();
        let tx = Transaction::new(&current);
        let tx = tx
            .fast_append()
            .add_data_files(files)
            .set_snapshot_properties(summary)
            .apply(tx)?;
        let tx = claimed.stamp_transaction(tx).expect("racer property");
        tx.commit(self.inner.as_ref()).await.map(|_| ())
    }
}

#[async_trait]
impl Catalog for ProbeCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        if self
            .failing_loads
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "injected: the catalog refused the load",
            ));
        }
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.seen.lock().expect("seen").push(format!("{commit:?}"));
        self.race_plain(commit.identifier()).await?;
        self.race_stamped(commit.identifier()).await?;
        match self.mode {
            ProbeMode::Forward => self.inner.update_table(commit).await,
            ProbeMode::FailBeforeLanding => Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "injected: the catalog refused the commit",
            )),
            ProbeMode::UnknownAfterLanding => {
                self.inner.update_table(commit).await?;
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::CommitStateUnknown,
                    "injected: landed, outcome unknown",
                ))
            }
            ProbeMode::UnknownWithoutLanding => Err(iceberg::Error::new(
                iceberg::ErrorKind::CommitStateUnknown,
                "injected: not landed, outcome unknown",
            )),
            ProbeMode::LandedThenReconcileFails => {
                self.inner.update_table(commit).await?;
                self.failing_loads.store(1, Ordering::SeqCst);
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::CommitStateUnknown,
                    "injected: landed, outcome unknown, the reconcile reload fails",
                ))
            }
            ProbeMode::UnknownThenLoadsFail => {
                self.failing_loads.store(usize::MAX, Ordering::SeqCst);
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::CommitStateUnknown,
                    "injected: not landed, outcome unknown, the catalog then refuses loads",
                ))
            }
        }
    }
}

async fn fixture_with(
    name: &str,
    extra: &[(&str, &str)],
) -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let (warehouse, catalog, ident) = fixture(name).await;
    if extra.is_empty() {
        return (warehouse, catalog, ident);
    }
    let table = catalog.load_table(&ident).await.expect("load");
    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for (key, value) in extra {
        action = action.set((*key).to_string(), (*value).to_string());
    }
    let tx = action.apply(tx).expect("apply properties");
    tx.commit(catalog.as_ref()).await.expect("set properties");
    (warehouse, catalog, ident)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arm {
    Append,
    CowInsert,
    CowRewrite,
    Mor,
    StampOnly,
}

const ARMS: [Arm; 5] = [
    Arm::Append,
    Arm::CowInsert,
    Arm::CowRewrite,
    Arm::Mor,
    Arm::StampOnly,
];

fn arm_name(arm: Arm) -> &'static str {
    match arm {
        Arm::Append => "p_append",
        Arm::CowInsert => "p_cow_insert",
        Arm::CowRewrite => "p_cow_rewrite",
        Arm::Mor => "p_mor",
        Arm::StampOnly => "p_stamp_only",
    }
}

async fn seed(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> (Table, Vec<DataFile>) {
    let table = catalog.load_table(ident).await.expect("load");
    let files = stage(&table, &[1, 2]).await;
    let table = commit_append_with_summary(catalog, &table, files.clone(), &[], None)
        .await
        .expect("seed");
    (table, files)
}

fn below_the_racers() -> Predicate {
    Reference::new("id").less_than(Datum::int(50))
}

async fn run_arm(
    arm: Arm,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    seeded: &[DataFile],
    guard: &BatchScopeGuard,
    stamp: &CommitStamp,
    pinned: bool,
) -> Result<(), String> {
    let pin = table.metadata().current_snapshot_id().filter(|_| pinned);
    let extra = scoped(guard);
    let outcome = match arm {
        Arm::Append => {
            let files = stage(table, &[3]).await;
            commit_append_with_summary(catalog, table, files, &extra, None)
                .await
                .map(|_| ())
        }
        Arm::CowInsert | Arm::CowRewrite => {
            let (affected, files) = if arm == Arm::CowInsert {
                (Vec::new(), stage(table, &[3]).await)
            } else {
                (seeded.to_vec(), stage(table, &[2]).await)
            };
            crate::write::merge::commit_on_ref(
                catalog,
                table,
                pin,
                affected,
                files,
                &below_the_racers(),
                None,
                &extra,
            )
            .await
        }
        Arm::Mor => {
            let target: Arc<str> = Arc::from(seeded[0].file_path());
            let files = stage(table, &[10]).await;
            crate::write::merge::commit_row_delta_on_ref_with_partitions(
                catalog,
                table,
                pin,
                vec![(target, 0)],
                files,
                WriteConcurrency::new(1).expect("K=1"),
                &below_the_racers(),
                None,
                crate::write::merge::KnownPartitions::new(),
                &extra,
                &WriterStagingOverrides::none(),
            )
            .await
        }
        Arm::StampOnly => {
            return commit_stamp_only(catalog, table, stamp, Some(guard.token()))
                .await
                .map(|_| ())
                .map_err(|error| format!("{error:?}"));
        }
    };
    outcome.map_err(|error| format!("{error:?}"))
}

fn assert_no_half(table: &Table) {
    assert_eq!(stamped_snapshots(table), 0, "a summary half landed alone");
    assert!(
        table
            .metadata()
            .properties()
            .keys()
            .all(|key| !key.starts_with(OFFSETS_PROPERTY_PREFIX)),
        "a property half landed alone"
    );
}

fn other_query() -> QueryId {
    QueryId::new(Uuid::parse_str("dddddddd-0000-4000-8000-0000000000d4").expect("uuid"))
}

fn recovery_reason(error: MicroBatchError) -> (Epoch, Option<SinkRecord>, RecoveryReason) {
    match error {
        MicroBatchError::RecoveryRequired {
            epoch,
            durable,
            reason,
            ..
        } => (epoch, durable.map(|record| *record), reason),
        other => panic!("expected RecoveryRequired, got {other:?}"),
    }
}

#[tokio::test]
async fn one_catalog_request_carries_both_halves_on_every_arm() {
    for arm in ARMS {
        let (_warehouse, memory, ident) = fixture(arm_name(arm)).await;
        let (table, seeded) = seed(&memory, &ident).await;
        let probe = Arc::new(ProbeCatalog::new(Arc::clone(&memory), ProbeMode::Forward));
        let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
        let stamp = stamp_for(0, SinkDoor::Table);
        let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
        run_arm(arm, &catalog, &table, &seeded, &guard, &stamp, true)
            .await
            .unwrap_or_else(|error| panic!("{arm:?}: {error}"));
        let seen = probe.seen();
        assert_eq!(seen.len(), 1, "{arm:?}: one catalog request");
        let (property_key, _) = stamp.record.property().expect("property");
        for needle in [
            "AddSnapshot",
            "repark.cdc.epoch",
            "SetProperties",
            &property_key,
        ] {
            assert!(seen[0].contains(needle), "{arm:?}: {needle}");
        }
        assert!(
            !seen[0].contains(SCOPE_TOKEN_KEY),
            "{arm:?}: the token leaked"
        );
        let reloaded = memory.load_table(&ident).await.expect("reload");
        assert_stamped_head(&reloaded, &stamp);
        assert_eq!(stamped_snapshots(&reloaded), 1, "{arm:?}");
        assert!(
            matches!(guard.outcome(), ScopeOutcome::Committed { .. }),
            "{arm:?}"
        );
    }
}

#[tokio::test]
async fn an_injected_failure_lands_neither_half_on_every_arm() {
    for arm in ARMS {
        for mode in [
            ProbeMode::FailBeforeLanding,
            ProbeMode::UnknownWithoutLanding,
        ] {
            let name = format!("{}_{mode:?}", arm_name(arm)).to_lowercase();
            let (_warehouse, memory, ident) = fixture(&name).await;
            let (table, seeded) = seed(&memory, &ident).await;
            let probe = Arc::new(ProbeCatalog::new(Arc::clone(&memory), mode));
            let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
            let stamp = stamp_for(0, SinkDoor::ForeachBatch);
            let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
            run_arm(arm, &catalog, &table, &seeded, &guard, &stamp, true)
                .await
                .expect_err("injected failure");
            let reloaded = memory.load_table(&ident).await.expect("reload");
            assert_no_half(&reloaded);
            assert_eq!(read_resume_point(&reloaded, query()).expect("resume"), None);
            assert_eq!(
                guard.outcome(),
                ScopeOutcome::NotCommitted,
                "{arm:?} {mode:?}"
            );
        }
    }
}

#[tokio::test]
async fn an_unknown_outcome_that_landed_holds_both_halves_on_every_arm() {
    for arm in ARMS {
        let name = format!("{}_landed", arm_name(arm));
        let (_warehouse, memory, ident) = fixture(&name).await;
        let (table, seeded) = seed(&memory, &ident).await;
        let probe = Arc::new(ProbeCatalog::new(
            Arc::clone(&memory),
            ProbeMode::UnknownAfterLanding,
        ));
        let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
        let stamp = stamp_for(0, SinkDoor::Table);
        let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
        let _ = run_arm(arm, &catalog, &table, &seeded, &guard, &stamp, true).await;
        let reloaded = memory.load_table(&ident).await.expect("reload");
        assert_eq!(stamped_snapshots(&reloaded), 1, "{arm:?}");
        assert_stamped_head(&reloaded, &stamp);
        assert_eq!(
            read_resume_point(&reloaded, query()).expect("resume"),
            Some(stamp.record.clone())
        );
    }
}

#[tokio::test]
async fn three_racing_appends_still_stamp_exactly_once_on_every_arm() {
    for arm in ARMS {
        let name = format!("{}_race3", arm_name(arm));
        let (_warehouse, memory, ident) = fixture_with(
            &name,
            &[
                ("write.merge.isolation-level", "serializable"),
                ("write.delete.isolation-level", "serializable"),
            ],
        )
        .await;
        let (table, seeded) = seed(&memory, &ident).await;
        let probe = Arc::new(ProbeCatalog::new(Arc::clone(&memory), ProbeMode::Forward));
        for racer_id in [97, 98, 99] {
            let files = stage(&table, &[racer_id]).await;
            probe.racers.lock().expect("racers").push(files);
        }
        let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
        let base_snapshots = table.metadata().snapshots().count();
        let stamp = stamp_for(0, SinkDoor::Table);
        let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
        run_arm(arm, &catalog, &table, &seeded, &guard, &stamp, false)
            .await
            .unwrap_or_else(|error| panic!("{arm:?}: {error}"));
        assert_eq!(probe.seen().len(), 4, "{arm:?}: three retries");
        let reloaded = memory.load_table(&ident).await.expect("reload");
        assert_eq!(
            reloaded.metadata().snapshots().count(),
            base_snapshots + 4,
            "{arm:?}"
        );
        assert_eq!(stamped_snapshots(&reloaded), 1, "{arm:?}");
        assert_stamped_head(&reloaded, &stamp);
        let ids = live_ids(&reloaded).await;
        for racer_id in [97, 98, 99] {
            assert!(
                ids.contains(&racer_id),
                "{arm:?}: racer {racer_id} lost: {ids:?}"
            );
        }
        assert_eq!(
            read_resume_point(&reloaded, query()).expect("resume"),
            Some(stamp.record.clone())
        );
        assert_eq!(
            guard.outcome(),
            ScopeOutcome::Committed {
                snapshot: SnapshotId::new(reloaded.metadata().current_snapshot_id().expect("head"))
            }
        );
    }
}

#[tokio::test]
async fn a_racing_stamp_of_another_query_keeps_both_records() {
    let (_warehouse, memory, ident) = fixture("p_two_query_race").await;
    let table = append_plain(&memory, &ident, &[1]).await;
    let probe = Arc::new(ProbeCatalog::new(Arc::clone(&memory), ProbeMode::Forward));
    let foreign = CommitStamp {
        record: record_for(other_query(), 7, 700),
        door: SinkDoor::Table,
    };
    let racer_files = stage(&table, &[70]).await;
    *probe.stamped_racer.lock().expect("stamped") = Some((foreign.clone(), racer_files));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    let stamp = stamp_for(3, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("stamped append over a stamped racer");
    drop(guard);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(stamped_snapshots(&reloaded), 2);
    assert_eq!(
        read_resume_point(&reloaded, query()).expect("own"),
        Some(stamp.record)
    );
    assert_eq!(
        read_resume_point(&reloaded, other_query()).expect("other"),
        Some(foreign.record)
    );
    assert_eq!(live_ids(&reloaded).await, vec![1, 2, 70]);
}

#[tokio::test]
async fn interleaved_queries_each_resume_their_own() {
    let (_warehouse, catalog, ident) = fixture("p_interleave").await;
    let mut last_own = None;
    let mut last_other = None;
    for epoch in 0..3u64 {
        let id = i32::try_from(epoch).expect("id");
        let own = stamp_for(epoch, SinkDoor::ForeachBatch);
        stamped_append(&catalog, &ident, &own, &[id]).await;
        last_own = Some(own.record);
        let other = CommitStamp {
            record: record_for(other_query(), epoch, 500 + i64::from(id)),
            door: SinkDoor::Table,
        };
        stamped_append(&catalog, &ident, &other, &[100 + id]).await;
        last_other = Some(other.record);
    }
    let table = catalog.load_table(&ident).await.expect("load");
    assert_eq!(read_resume_point(&table, query()).expect("own"), last_own);
    assert_eq!(
        read_resume_point(&table, other_query()).expect("other"),
        last_other
    );
}

#[tokio::test]
async fn resume_reads_a_stamp_only_chain_and_a_root_stamp() {
    let (_warehouse, catalog, ident) = fixture("p_stamp_only_chain").await;
    let mut table = catalog.load_table(&ident).await.expect("load");
    for epoch in 0..3u64 {
        commit_stamp_only(
            &catalog,
            &table,
            &stamp_for(epoch, SinkDoor::ForeachBatch),
            None,
        )
        .await
        .expect("stamp-only");
        table = catalog.load_table(&ident).await.expect("reload");
    }
    assert_eq!(stamped_snapshots(&table), 3);
    assert!(live_ids(&table).await.is_empty());
    assert_eq!(
        read_resume_point(&table, query()).expect("resume"),
        Some(stamp_for(2, SinkDoor::ForeachBatch).record)
    );
    let (_root_warehouse, root_catalog, root_ident) = fixture("p_root_stamp").await;
    let root = stamp_for(0, SinkDoor::Table);
    stamped_append(&root_catalog, &root_ident, &root, &[1]).await;
    for id in [2, 3, 4] {
        append_plain(&root_catalog, &root_ident, &[id]).await;
    }
    let root_table = root_catalog.load_table(&root_ident).await.expect("load");
    assert_eq!(root_table.metadata().snapshots().count(), 4);
    assert_eq!(
        read_resume_point(&root_table, query()).expect("root stamp"),
        Some(root.record)
    );
}

#[tokio::test]
async fn resume_refuses_a_property_behind_the_summary() {
    let (_warehouse, catalog, ident) = fixture("p_behind").await;
    let first = stamp_for(0, SinkDoor::Table);
    let second = stamp_for(1, SinkDoor::Table);
    stamped_append(&catalog, &ident, &first, &[1]).await;
    let table = stamped_append(&catalog, &ident, &second, &[2]).await;
    let (key, behind) = first.record.property().expect("property");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(key, behind)
        .apply(tx)
        .expect("apply");
    let table = tx.commit(catalog.as_ref()).await.expect("drift back");
    let (epoch, durable, reason) =
        recovery_reason(read_resume_point(&table, query()).expect_err("behind"));
    assert_eq!(epoch, Epoch::new(1));
    assert_eq!(durable, Some(second.record));
    assert_eq!(
        reason,
        RecoveryReason::OffsetMismatch {
            summary_epoch: Some(Epoch::new(1)),
            property_epoch: Some(Epoch::new(0)),
        }
    );
}

#[tokio::test]
async fn resume_never_falls_back_to_an_older_stamp_when_the_newest_expired() {
    let (_warehouse, catalog, ident) = fixture("p_expire_newest").await;
    let first = stamp_for(0, SinkDoor::Table);
    let second = stamp_for(1, SinkDoor::Table);
    stamped_append(&catalog, &ident, &first, &[1]).await;
    let stamped = stamped_append(&catalog, &ident, &second, &[2]).await;
    let second_id = stamped.metadata().current_snapshot_id().expect("id");
    let table = append_plain(&catalog, &ident, &[3]).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshots()
        .expire_snapshot_id(second_id)
        .apply(tx)
        .expect("apply");
    let table = tx.commit(catalog.as_ref()).await.expect("expire");
    let (epoch, durable, reason) =
        recovery_reason(read_resume_point(&table, query()).expect_err("expired"));
    assert_eq!(epoch, Epoch::new(1));
    assert_eq!(durable, Some(second.record));
    assert_eq!(reason, RecoveryReason::StampedSnapshotExpired);
}

#[tokio::test]
async fn resume_refuses_a_newer_format_on_both_halves() {
    let (_warehouse, catalog, ident) = fixture("p_v2_both").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let mut summary: HashMap<String, String> = stamp
        .record
        .summary_entries(SinkDoor::Table)
        .expect("entries")
        .into_iter()
        .collect();
    summary.insert(String::from("repark.cdc.format-version"), String::from("2"));
    let (key, value) = stamp.record.property().expect("property");
    let tx = Transaction::new(&table);
    let tx = tx
        .merge_append()
        .set_snapshot_properties(summary)
        .apply(tx)
        .expect("apply");
    let tx = tx
        .update_table_properties()
        .set(
            key,
            value.replace("\"format-version\":1", "\"format-version\":2"),
        )
        .apply(tx)
        .expect("apply");
    let table = tx.commit(catalog.as_ref()).await.expect("v2 stamp");
    assert!(matches!(
        read_resume_point(&table, query()),
        Err(MicroBatchError::UnsupportedOffsetFormat { ref found, supported: 1 }) if found == "2"
    ));
}

#[tokio::test]
async fn a_bare_empty_merge_append_refuses_on_an_empty_and_a_seeded_sink() {
    let (_warehouse, catalog, ident) = fixture("p_dm5_bare").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let tx = Transaction::new(&table);
    let bare = tx.merge_append().apply(tx).expect("apply bare");
    assert!(bare.commit(catalog.as_ref()).await.is_err());
    let table = append_plain(&catalog, &ident, &[1]).await;
    let tx = Transaction::new(&table);
    let bare = tx.merge_append().apply(tx).expect("apply bare");
    assert!(bare.commit(catalog.as_ref()).await.is_err());
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(live_ids(&table).await, vec![1]);
}

#[tokio::test]
async fn an_unlanded_unknown_outcome_walks_to_the_durable_record_without_a_resubmit() {
    let (_warehouse, memory, ident) = fixture("p_walk_unlanded").await;
    let durable = stamp_for(0, SinkDoor::Table);
    stamped_append(&memory, &ident, &durable, &[1]).await;
    let table = memory.load_table(&ident).await.expect("load");
    let snapshots = table.metadata().snapshots().count();
    let probe = Arc::new(ProbeCatalog::new(
        Arc::clone(&memory),
        ProbeMode::UnknownWithoutLanding,
    ));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    let stamp = stamp_for(1, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let refused = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect_err("an unlanded stamp never resolves");
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let (epoch, found, reason) = recovery_reason(refused);
    assert_eq!(epoch, Epoch::new(1));
    assert_eq!(found, Some(durable.record));
    assert!(matches!(
        reason,
        RecoveryReason::CommitOutcomeUnknown {
            operation_id: Some(_)
        }
    ));
    assert_eq!(probe.seen().len(), 1);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), snapshots);
}

#[tokio::test]
async fn a_same_epoch_stamp_of_another_run_is_not_the_landed_attempt() {
    let (_warehouse, memory, ident) = fixture("p_walk_other_run").await;
    stamped_append(&memory, &ident, &stamp_for(0, SinkDoor::Table), &[1]).await;
    let table = memory.load_table(&ident).await.expect("load");
    let probe = Arc::new(ProbeCatalog::new(
        Arc::clone(&memory),
        ProbeMode::UnknownWithoutLanding,
    ));
    let mut racer = stamp_for(1, SinkDoor::Table);
    racer.record.run =
        RunId::new(Uuid::parse_str("ffffffff-0000-4000-8000-0000000000f6").expect("uuid"));
    let racer_files = stage(&table, &[70]).await;
    *probe.stamped_racer.lock().expect("stamped") = Some((racer.clone(), racer_files));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    let stamp = stamp_for(1, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let refused = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect_err("the racer's stamp is not this attempt");
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let (epoch, found, reason) = recovery_reason(refused);
    assert_eq!(epoch, Epoch::new(1));
    assert_eq!(found, Some(racer.record));
    assert!(matches!(
        reason,
        RecoveryReason::CommitOutcomeUnknown { .. }
    ));
}

#[tokio::test]
async fn the_walk_finds_a_landed_stamp_above_the_base_by_operation_id_then_by_record() {
    let (_warehouse, catalog, ident) = fixture("p_walk_direct").await;
    let base = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    let head = commit_stamp_only(&catalog, &base, &stamp, None)
        .await
        .expect("stamp-only");
    let landed = catalog.load_table(&ident).await.expect("reload");
    let operation = landed
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties
        .get(OPERATION_ID_PROP)
        .cloned()
        .expect("operation id");
    for operation_id in [
        Some(operation.as_str()),
        Some("00000000-0000-4000-8000-000000000000"),
        None,
    ] {
        assert_eq!(
            resolve_unknown_outcome(&catalog, &base, &stamp, operation_id).await,
            Ok(head),
            "{operation_id:?}"
        );
    }
    let below = resolve_unknown_outcome(&catalog, &landed, &stamp, Some(&operation))
        .await
        .expect_err("a stamp at or below the base is not this attempt");
    let (epoch, found, reason) = recovery_reason(below);
    assert_eq!(epoch, Epoch::new(0));
    assert_eq!(found, Some(stamp.record));
    assert_eq!(
        reason,
        RecoveryReason::CommitOutcomeUnknown {
            operation_id: Some(operation)
        }
    );
    assert_eq!(
        catalog
            .load_table(&ident)
            .await
            .expect("reload")
            .metadata()
            .snapshots()
            .count(),
        landed.metadata().snapshots().count()
    );
}

#[path = "sink_offsets_walk_tests.rs"]
mod walk;

#[path = "sink_offsets_isolation_tests.rs"]
mod isolation;
