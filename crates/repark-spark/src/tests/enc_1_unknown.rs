use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{ErrorKind, Namespace, TableCommit};

use super::super::*;
use super::common::*;
use super::enc_1_fold::{VERSIONS, freeze};
use super::enc_1_own_set::{
    FILE_DOORS, answered_rows, file_door, local, partitioned_table, stage_own, url_spelling,
};

const LOST_KEY: &str = "lost-reply-probe-key";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reply {
    LandsLate,
    NeverLands,
    NeverLandsUnkeyed,
}

#[derive(Debug)]
struct LostReply {
    inner: Arc<dyn Catalog>,
    reply: Reply,
    lost: AtomicBool,
    loads_after_loss: AtomicUsize,
    held: Mutex<Option<TableCommit>>,
    other_attempt: Mutex<Option<DataFile>>,
}

impl LostReply {
    fn install(
        inner: Arc<dyn Catalog>,
        reply: Reply,
        other_attempt: Option<DataFile>,
    ) -> Arc<dyn Catalog> {
        Arc::new(Self {
            inner,
            reply,
            lost: AtomicBool::new(false),
            loads_after_loss: AtomicUsize::new(0),
            held: Mutex::new(None),
            other_attempt: Mutex::new(other_attempt),
        })
    }

    fn take<T>(slot: &Mutex<Option<T>>) -> Option<T> {
        slot.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    async fn set_key(&self, ident: &TableIdent) -> iceberg::Result<()> {
        let table = self.inner.load_table(ident).await?;
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(
                repark_iceberg::write::ENCRYPTION_KEY_ID_PROPERTY.to_string(),
                LOST_KEY.to_string(),
            )
            .apply(tx)?;
        tx.commit(self.inner.as_ref()).await.map(|_| ())
    }

    async fn commit_other_attempt(
        &self,
        ident: &TableIdent,
        file: DataFile,
    ) -> iceberg::Result<()> {
        let table = self.inner.load_table(ident).await?;
        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(vec![file]).apply(tx)?;
        tx.commit(self.inner.as_ref()).await.map(|_| ())
    }
}

#[async_trait]
impl Catalog for LostReply {
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

    async fn update_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .update_namespace_properties(namespace, removals, updates)
            .await
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
        let lands_now = self.reply == Reply::LandsLate
            && self.lost.load(Ordering::SeqCst)
            && self.loads_after_loss.fetch_add(1, Ordering::SeqCst) == 1;
        if lands_now && let Some(held) = Self::take(&self.held) {
            self.inner.update_table(held).await?;
            self.set_key(table).await?;
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
        if self.lost.load(Ordering::SeqCst) {
            return self.inner.update_table(commit).await;
        }
        let ident = commit.identifier().clone();
        match self.reply {
            Reply::LandsLate => {
                *self
                    .held
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(commit);
            }
            Reply::NeverLands | Reply::NeverLandsUnkeyed => {
                if let Some(file) = Self::take(&self.other_attempt) {
                    self.commit_other_attempt(&ident, file).await?;
                }
                if self.reply == Reply::NeverLands {
                    self.set_key(&ident).await?;
                }
            }
        }
        self.lost.store(true, Ordering::SeqCst);
        Err(iceberg::Error::new(
            ErrorKind::CommitStateUnknown,
            "the reply to UpdateTable was lost on a closed connection",
        ))
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }
}

fn unknown_outcome(
    label: &str,
    result: &datafusion::error::Result<Table>,
    faults: &mut Vec<String>,
) {
    match result {
        Ok(_) => faults.push(format!("{label}: the lost reply reported success")),
        Err(error) if repark_iceberg::write::is_commit_state_unknown(error) => {}
        Err(error) => faults.push(format!("{label}: not the unknown outcome: {error}")),
    }
}

fn exists(path: &str) -> bool {
    std::path::Path::new(path).exists()
}

async fn lost_reply_file_door(version: &str, index: usize, reply: Reply) -> Vec<String> {
    let (ctx, catalogs, _warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let table = load_sales_table(&catalogs, "t").await;
    let guarded = catalogs.get("ice").expect("ice catalog").clone();
    let own = stage_own(&ctx, &table).await;
    let own_paths: Vec<String> = own.iter().map(|file| local(file.file_path())).collect();
    let other = (reply != Reply::LandsLate).then(|| url_spelling(&own[0]));
    let injector = LostReply::install(guarded, reply, other);
    let (door, result) = file_door(index, &injector, &table, own).await;
    let label = format!("v{version} {door} {reply:?}");
    let mut faults = Vec::new();
    unknown_outcome(&label, &result, &mut faults);
    match reply {
        Reply::LandsLate | Reply::NeverLandsUnkeyed => {
            for path in &own_paths {
                if !exists(path) {
                    faults.push(format!("{label}: {path} was swept"));
                }
            }
        }
        Reply::NeverLands => {
            if !exists(&own_paths[0]) {
                faults.push(format!(
                    "{label}: {} is referenced by another snapshot and was swept",
                    own_paths[0]
                ));
            }
            for path in &own_paths[1..] {
                if exists(path) {
                    faults.push(format!("{label}: unreferenced {path} was left behind"));
                }
            }
        }
    }
    if let Err(error) = answered_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await {
        faults.push(format!("{label}: the table does not read: {error}"));
    }
    faults
}

async fn lost_reply_streaming_door(version: &str, reply: Reply) -> Vec<String> {
    let (ctx, catalogs, warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let table = load_sales_table(&catalogs, "t").await;
    let guarded = catalogs.get("ice").expect("ice catalog").clone();
    let injector = LostReply::install(guarded, reply, None);
    let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
    let batches = ctx
        .sql("SELECT 9 AS id, 'z' AS name, 'x' AS cat")
        .await
        .expect("source frames")
        .collect()
        .await
        .expect("source collects");
    let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>));
    let result = repark_iceberg::write::append_staged_with_options(
        &injector,
        &table,
        stream,
        &[],
        &repark_iceberg::write::WriterStagingOverrides::none(),
        repark_iceberg::write::WriteConcurrency::default(),
        None,
    )
    .await;
    let label = format!("v{version} append_staged_with_options {reply:?}");
    let mut faults = Vec::new();
    unknown_outcome(&label, &result, &mut faults);
    let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
    let parquet = |listing: &std::collections::BTreeSet<String>| -> Vec<String> {
        listing
            .iter()
            .filter(|path| path.ends_with(".parquet"))
            .cloned()
            .collect()
    };
    let grew = parquet(&after.listing).len() > parquet(&before.listing).len();
    match reply {
        Reply::LandsLate | Reply::NeverLandsUnkeyed if !grew => {
            faults.push(format!("{label}: the staged file was swept"));
        }
        Reply::NeverLands if grew => {
            faults.push(format!(
                "{label}: the unreferenced staged file was left behind"
            ));
        }
        _ => {}
    }
    if let Err(error) = answered_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await {
        faults.push(format!("{label}: the table does not read: {error}"));
    }
    faults
}

async fn lost_reply_every_door(reply: Reply) -> Vec<String> {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for index in 0..FILE_DOORS {
            faults.extend(lost_reply_file_door(version, index, reply).await);
        }
        faults.extend(lost_reply_streaming_door(version, reply).await);
    }
    faults
}

#[tokio::test]
async fn a_lost_reply_whose_snapshot_lands_sweeps_nothing() {
    let faults = lost_reply_every_door(Reply::LandsLate).await;
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn a_lost_reply_whose_snapshot_never_lands_sweeps_only_unreferenced_own_files() {
    let faults = lost_reply_every_door(Reply::NeverLands).await;
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn a_lost_reply_on_an_unkeyed_table_sweeps_nothing() {
    let faults = lost_reply_every_door(Reply::NeverLandsUnkeyed).await;
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}
