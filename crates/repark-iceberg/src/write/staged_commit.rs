use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use iceberg::spec::{DataFile, PartitionKey};
use iceberg::table::Table;
use iceberg::transaction::{
    ApplyTransactionAction, FastAppendAction, MergeAppendAction, OverwriteFilesAction,
    ReplacePartitionsAction, RowDeltaAction, Transaction,
};
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::{Catalog, ErrorKind};

use crate::write::encryption::{EncryptedTableRefusal, carries_encryption_key};
use crate::write::merge::OPERATION_ID_PROP;
use crate::write::sink_offsets::SiteStamp;

const UNCLAIMED_CAPACITY: usize = 1 << 20;

#[derive(Default)]
struct Unclaimed {
    order: VecDeque<String>,
    paths: HashSet<String>,
}

impl Unclaimed {
    fn record(&mut self, path: &str) {
        self.record_within(path, UNCLAIMED_CAPACITY);
    }

    fn record_within(&mut self, path: &str, capacity: usize) {
        if !self.paths.insert(path.to_string()) {
            return;
        }
        self.order.push_back(path.to_string());
        while self.order.len() > capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.paths.remove(&evicted);
            }
        }
    }
}

fn unclaimed() -> MutexGuard<'static, Unclaimed> {
    static LEDGER: OnceLock<Mutex<Unclaimed>> = OnceLock::new();
    LEDGER
        .get_or_init(|| Mutex::new(Unclaimed::default()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

fn claim<'a>(paths: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut ledger = unclaimed();
    let mut claimed = Vec::new();
    for path in paths {
        if ledger.paths.remove(path) {
            claimed.push(path.to_string());
        }
    }
    claimed
}

fn release(paths: &[String]) {
    let mut ledger = unclaimed();
    for path in paths {
        ledger.record(path);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OwnLocations<L>(L);

impl<L: LocationGenerator> OwnLocations<L> {
    pub(crate) fn new(inner: L) -> Self {
        Self(inner)
    }
}

impl<L: LocationGenerator> LocationGenerator for OwnLocations<L> {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let location = self.0.generate_location(partition_key, file_name);
        unclaimed().record(&location);
        location
    }
}

pub(crate) struct Sealed(());

pub(crate) trait StagesFiles: ApplyTransactionAction + Sized {
    fn with_files(self, data: Vec<DataFile>, deletes: Vec<DataFile>, sealed: Sealed) -> Self;
}

impl StagesFiles for FastAppendAction {
    #[expect(
        clippy::disallowed_methods,
        reason = "the staged-commit module is the one place a staged file joins an action"
    )]
    fn with_files(self, data: Vec<DataFile>, _deletes: Vec<DataFile>, _: Sealed) -> Self {
        self.add_data_files(data)
    }
}

impl StagesFiles for MergeAppendAction {
    #[expect(
        clippy::disallowed_methods,
        reason = "the staged-commit module is the one place a staged file joins an action"
    )]
    fn with_files(self, data: Vec<DataFile>, _deletes: Vec<DataFile>, _: Sealed) -> Self {
        self.add_data_files(data)
    }
}

impl StagesFiles for OverwriteFilesAction {
    #[expect(
        clippy::disallowed_methods,
        reason = "the staged-commit module is the one place a staged file joins an action"
    )]
    fn with_files(self, data: Vec<DataFile>, _deletes: Vec<DataFile>, _: Sealed) -> Self {
        self.add_files(data)
    }
}

impl StagesFiles for ReplacePartitionsAction {
    #[expect(
        clippy::disallowed_methods,
        reason = "the staged-commit module is the one place a staged file joins an action"
    )]
    fn with_files(self, data: Vec<DataFile>, _deletes: Vec<DataFile>, _: Sealed) -> Self {
        self.add_files(data)
    }
}

impl StagesFiles for RowDeltaAction {
    #[expect(
        clippy::disallowed_methods,
        reason = "the staged-commit module is the one place a staged file joins an action"
    )]
    fn with_files(self, data: Vec<DataFile>, deletes: Vec<DataFile>, _: Sealed) -> Self {
        self.add_data_files(data).add_deletes(deletes)
    }
}

#[must_use]
pub(crate) struct StagedFiles {
    data: Vec<DataFile>,
    deletes: Vec<DataFile>,
    own: Vec<String>,
}

impl StagedFiles {
    pub(crate) fn claimed(data: Vec<DataFile>) -> Self {
        let own = claim(data.iter().map(DataFile::file_path));
        Self {
            data,
            deletes: Vec::new(),
            own,
        }
    }

    pub(crate) fn with_written_deletes(mut self, deletes: Vec<DataFile>) -> Self {
        let paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        claim(paths.iter().map(String::as_str));
        self.own.extend(paths);
        self.deletes = deletes;
        self
    }

    pub(crate) fn stage<A: StagesFiles>(
        self,
        action: A,
        tx: Transaction,
    ) -> iceberg::Result<StagedCommit> {
        let Self { data, deletes, own } = self;
        match action.with_files(data, deletes, Sealed(())).apply(tx) {
            Ok(tx) => Ok(StagedCommit { tx, own }),
            Err(error) => {
                release(&own);
                Err(error)
            }
        }
    }
}

#[must_use]
pub(crate) struct StagedCommit {
    tx: Transaction,
    own: Vec<String>,
}

impl StagedCommit {
    pub(crate) fn stamped(self, stamp: &SiteStamp) -> datafusion::error::Result<Self> {
        let Self { tx, own } = self;
        match stamp.transaction(tx) {
            Ok(tx) => Ok(Self { tx, own }),
            Err(error) => {
                release(&own);
                Err(error)
            }
        }
    }

    pub(crate) async fn commit(
        self,
        table: &Table,
        catalog: &dyn Catalog,
        operation_id: &str,
    ) -> iceberg::Result<Table> {
        let Self { tx, own } = self;
        match commit_transaction(tx, catalog).await {
            Ok(committed) => Ok(committed),
            Err(error) if error.kind() == ErrorKind::CommitStateUnknown => {
                settle_unknown_outcome(table, catalog, operation_id, &own).await;
                Err(error)
            }
            Err(error) if EncryptedTableRefusal::find(&error).is_some() => {
                Err(sweep_refused(table, &own, error).await)
            }
            Err(error) => {
                release(&own);
                Err(error)
            }
        }
    }

    pub(crate) async fn commit_or_abort(
        self,
        table: &Table,
        catalog: &dyn Catalog,
        operation_id: &str,
    ) -> iceberg::Result<Table> {
        let Self { tx, own } = self;
        match commit_transaction(tx, catalog).await {
            Ok(committed) => Ok(committed),
            Err(error) if error.kind() == ErrorKind::CommitStateUnknown => {
                settle_unknown_outcome(table, catalog, operation_id, &own).await;
                Err(error)
            }
            Err(error) if EncryptedTableRefusal::find(&error).is_some() => {
                Err(sweep_refused(table, &own, error).await)
            }
            Err(error) => {
                delete_best_effort(table, &own).await;
                Err(error)
            }
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "every Iceberg commit runs here, so a staged write commits only through StagedCommit"
)]
async fn commit_transaction(tx: Transaction, catalog: &dyn Catalog) -> iceberg::Result<Table> {
    tx.commit(catalog).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_unstaged(tx: Transaction, catalog: &dyn Catalog) -> iceberg::Result<Table> {
    commit_transaction(tx, catalog).await
}

async fn delete_best_effort(table: &Table, paths: &[String]) -> Vec<(String, iceberg::Error)> {
    let mut failures = Vec::new();
    let file_io = table.file_io();
    for path in paths {
        if let Err(error) = file_io.delete(path).await {
            tracing::warn!(
                path = %path,
                error = %error,
                "failed to delete a staged file after a refused commit"
            );
            failures.push((path.clone(), error));
        }
    }
    failures
}

async fn sweep_refused(table: &Table, own: &[String], error: iceberg::Error) -> iceberg::Error {
    let failures = delete_best_effort(table, own).await;
    if failures.is_empty() {
        return error;
    }
    let mut message = error.message().to_string();
    for (path, failure) in &failures {
        message.push_str(" failed to delete staged file '");
        message.push_str(path);
        message.push_str("': ");
        message.push_str(&failure.to_string());
        message.push(';');
    }
    iceberg::Error::new(error.kind(), message)
        .with_retryable(error.retryable())
        .with_source(error)
}

fn canonical(path: &str) -> &str {
    path.strip_prefix("file://")
        .or_else(|| path.strip_prefix("file:"))
        .unwrap_or(path)
}

fn carries_snapshot_of(table: &Table, operation_id: &str) -> bool {
    table.metadata().snapshots().any(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(OPERATION_ID_PROP)
            .is_some_and(|stamped| stamped == operation_id)
    })
}

async fn referenced_among(table: &Table, own: &[String]) -> iceberg::Result<HashSet<String>> {
    let mut pending: HashSet<&str> = own.iter().map(|path| canonical(path)).collect();
    let mut referenced = HashSet::new();
    let mut loaded = HashSet::new();
    let metadata = table.metadata();
    for snapshot in metadata.snapshots() {
        if pending.is_empty() {
            break;
        }
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await?;
        for manifest_file in manifest_list.entries() {
            if !loaded.insert(manifest_file.manifest_path.clone()) {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                let path = canonical(entry.data_file().file_path());
                if pending.remove(path) {
                    referenced.insert(path.to_string());
                }
            }
        }
    }
    Ok(referenced)
}

async fn settle_unknown_outcome(
    table: &Table,
    catalog: &dyn Catalog,
    operation_id: &str,
    own: &[String],
) {
    let fresh = match catalog.load_table(table.identifier()).await {
        Ok(fresh) => fresh,
        Err(error) => {
            tracing::warn!(
                files = own.len(),
                error = %error,
                "commit state unknown and the table could not be read back; leaving staged files in place"
            );
            return;
        }
    };
    if !carries_encryption_key(fresh.metadata().properties())
        || carries_snapshot_of(&fresh, operation_id)
    {
        tracing::warn!(
            files = own.len(),
            "commit state unknown; leaving staged files in place"
        );
        return;
    }
    let referenced = match referenced_among(&fresh, own).await {
        Ok(referenced) => referenced,
        Err(error) => {
            tracing::warn!(
                files = own.len(),
                error = %error,
                "commit state unknown and the snapshots could not be read; leaving staged files in place"
            );
            return;
        }
    };
    let unreferenced: Vec<String> = own
        .iter()
        .filter(|path| !referenced.contains(canonical(path)))
        .cloned()
        .collect();
    delete_best_effort(table, &unreferenced).await;
}

#[cfg(test)]
#[path = "staged_commit_tests.rs"]
mod tests;
