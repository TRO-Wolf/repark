use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{EPOCH_KEY, Epoch, QUERY_ID_KEY};
use repark_iceberg::write::{
    commit_append_with_summary, concurrency_from_ctx, resolve_empty_session_write,
    stage_overwrite_files_with,
};

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryHandle, SinkSpec, StreamSpec, StreamingQueryManager, Trigger,
};
use crate::microbatch::run::engine_error;
use crate::time_travel::microbatch_source::SourceOptions;

pub(crate) const SOURCE: &str = "ice.sales.orders";
pub(crate) const SINK: &str = "ice.sales.silver";

pub(crate) struct Fixture {
    pub(crate) warehouse: TempDir,
    pub(crate) session: Session,
}

impl Fixture {
    pub(crate) async fn new() -> Fixture {
        let warehouse = TempDir::new().expect("a scratch warehouse");
        let root = warehouse
            .path()
            .to_str()
            .expect("the warehouse path is text")
            .to_string();
        let session = Session::builder().build().expect("a session");
        session
            .register_memory_catalog("ice", &root)
            .await
            .expect("the memory catalog registers");
        let catalog = session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the catalog is visible");
        let sales = NamespaceIdent::new("sales".to_string());
        catalog
            .create_namespace(&sales, HashMap::new())
            .await
            .expect("the namespace creates");
        for name in ["orders", "silver", "other"] {
            catalog
                .create_table(&sales, creation(name, &format!("{root}/sales/{name}")))
                .await
                .expect("the table creates");
        }
        session
            .refresh_catalog_provider("ice")
            .await
            .expect("the provider refreshes");
        Fixture { warehouse, session }
    }

    pub(crate) fn root(&self) -> String {
        self.warehouse.path().display().to_string()
    }

    pub(crate) async fn insert(&self, table: &str, values: &str) {
        self.session
            .sql(&format!("INSERT INTO {table} VALUES {values}"))
            .await
            .expect("the insert plans")
            .collect()
            .await
            .expect("the insert commits");
    }

    pub(crate) async fn table(&self, name: &str) -> Table {
        let catalog = self
            .session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the catalog is visible");
        catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                name.to_string(),
            ))
            .await
            .expect("the table loads")
    }

    pub(crate) async fn ids(&self, table: &str) -> Vec<i64> {
        let frames = self
            .session
            .sql(&format!("SELECT id FROM {table} ORDER BY id"))
            .await
            .expect("the read plans")
            .collect()
            .await
            .expect("the read runs");
        ids_of(&frames)
    }
}

pub(crate) fn stamped_epochs(table: &Table) -> Vec<u64> {
    let metadata = table.metadata();
    let mut epochs = Vec::new();
    let mut cursor = metadata.current_snapshot();
    while let Some(snapshot) = cursor {
        let summary = &snapshot.summary().additional_properties;
        if summary.contains_key(QUERY_ID_KEY)
            && let Some(epoch) = summary.get(EPOCH_KEY)
        {
            epochs.push(epoch.parse().expect("an epoch is a number"));
        }
        cursor = snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent));
    }
    epochs.reverse();
    epochs
}

pub(crate) async fn wait_for_epoch(handle: &QueryHandle, epoch: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if handle
            .durable()
            .is_some_and(|record| record.epoch.get() >= epoch)
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "epoch {epoch} never became durable: {:?} {:?}",
            handle.state(),
            handle.exception()
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

pub(crate) fn table_spec(trigger: Trigger, source: &SourceOptions) -> StreamSpec {
    let mut spec = StreamSpec::new(
        SOURCE,
        source.clone(),
        SinkSpec::Table {
            sink: SINK.to_string(),
        },
    );
    spec.trigger = trigger;
    spec
}

pub(crate) async fn started(fixture: &Fixture, spec: StreamSpec) -> QueryHandle {
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(&fixture.session, spec)
        .await
        .expect("register");
    handle.start_below_catalog_check().expect("start");
    handle
}

pub(crate) fn ids_of(frames: &[RecordBatch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for frame in frames {
        let column = frame
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id reads as Int64");
        ids.extend((0..column.len()).map(|index| column.value(index)));
    }
    ids.sort_unstable();
    ids
}

pub(crate) fn options(pairs: &[(&str, &str)]) -> SourceOptions {
    let map: BTreeMap<String, String> = pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    SourceOptions::from_options(&map).expect("the options parse")
}

pub(crate) fn creation(name: &str, location: &str) -> TableCreation {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("the schema builds");
    TableCreation::builder()
        .name(name.to_string())
        .location(location.to_string())
        .schema(schema)
        .properties(HashMap::new())
        .build()
}

pub(crate) async fn append_frame(
    session: &Session,
    table: &str,
    frame: DataFrame,
) -> Result<(), MicroBatchError> {
    let [catalog_name, namespace, name] = table.split('.').collect::<Vec<_>>()[..] else {
        return Err(MicroBatchError::Catalog(format!("{table} is not a table")));
    };
    let catalog = session
        .catalogs_snapshot()
        .guarded_in_batch_body()
        .get(catalog_name)
        .cloned()
        .ok_or_else(|| MicroBatchError::Catalog(format!("no catalog {catalog_name}")))?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.to_string()), name.to_string());
    let target = catalog
        .load_table(&ident)
        .await
        .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
    let context = session.context();
    let (extra, staging) =
        resolve_empty_session_write(context).map_err(|error| engine_error(&error))?;
    let stream = frame
        .execute_stream()
        .await
        .map_err(|error| engine_error(&error))?;
    let files = stage_overwrite_files_with(
        &target,
        stream,
        Vec::new(),
        concurrency_from_ctx(context),
        &staging,
    )
    .await
    .map_err(|error| engine_error(&error))?;
    commit_append_with_summary(&catalog, &target, files, &extra, None)
        .await
        .map_err(|error| engine_error(&error))?;
    Ok(())
}

pub(crate) struct SinkWriter {
    session: Session,
    sink: String,
    writes: usize,
    calls: AtomicUsize,
}

impl SinkWriter {
    pub(crate) fn new(session: &Session, sink: &str, writes: usize) -> std::sync::Arc<SinkWriter> {
        std::sync::Arc::new(SinkWriter {
            session: session.clone(),
            sink: sink.to_string(),
            writes,
            calls: AtomicUsize::new(0),
        })
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub(crate) fn spec(self: &std::sync::Arc<Self>, source: &SourceOptions) -> StreamSpec {
        let mut spec = StreamSpec::new(
            SOURCE,
            source.clone(),
            SinkSpec::ForeachBatch {
                sink: self.sink.clone(),
                body: std::sync::Arc::clone(self) as std::sync::Arc<dyn BatchBody>,
            },
        );
        spec.trigger = Trigger::AvailableNow;
        spec
    }
}

impl BatchBody for SinkWriter {
    fn run(&self, frame: DataFrame, _epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            for _ in 0..self.writes {
                append_frame(&self.session, &self.sink, frame.clone()).await?;
            }
            Ok(())
        })
    }
}

pub(crate) async fn mark_the_start(fixture: &Fixture, handle: &QueryHandle) {
    let catalog = fixture
        .session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the catalog is visible");
    let sink = fixture.table("silver").await;
    repark_iceberg::write::sink_offsets::commit_starting_mark(&catalog, &sink, handle.id())
        .await
        .expect("the starting mark commits");
}
