use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{NamespaceIdent, TableCreation};
use repark_common::Generation;
use repark_iceberg::microbatch::offset::{
    Epoch, OffsetFormatVersion, OffsetVector, QueryId, RunId,
};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;

enum Change {
    Rename(&'static str, &'static str),
    Drop(&'static str),
    Add(&'static str, PrimitiveType),
    Promote(&'static str, PrimitiveType),
}

struct Evolving {
    _warehouse: TempDir,
    session: Session,
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    name: String,
}

impl Evolving {
    async fn create(table: &str, fields: Vec<NestedField>) -> Self {
        let warehouse = TempDir::new().expect("a scratch warehouse must build");
        let root = warehouse
            .path()
            .to_str()
            .expect("the warehouse path must be text")
            .to_string();
        let session = Session::builder().build().expect("a session must build");
        session
            .register_memory_catalog("ice", &root)
            .await
            .expect("the memory catalog must register");
        let catalog = session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the catalog must be visible");
        let sales = NamespaceIdent::new("sales".to_string());
        catalog
            .create_namespace(&sales, HashMap::new())
            .await
            .expect("the namespace must create");
        let schema = Schema::builder()
            .with_fields(fields.into_iter().map(Arc::new).collect::<Vec<_>>())
            .build()
            .expect("the schema must build");
        catalog
            .create_table(
                &sales,
                TableCreation::builder()
                    .name(table.to_string())
                    .location(format!("{root}/sales/{table}"))
                    .schema(schema)
                    .properties(HashMap::new())
                    .build(),
            )
            .await
            .expect("the table must create");
        session
            .refresh_catalog_provider("ice")
            .await
            .expect("the provider must refresh");
        Self {
            _warehouse: warehouse,
            session,
            catalog,
            ident: TableIdent::new(sales, table.to_string()),
            name: format!("ice.sales.{table}"),
        }
    }

    async fn insert(&self, values: &str) {
        self.session
            .sql(&format!("INSERT INTO {} VALUES {values}", self.name))
            .await
            .expect("the insert must plan")
            .collect()
            .await
            .expect("the insert must commit");
    }

    async fn evolve(&self, change: Change) {
        let table = self
            .catalog
            .load_table(&self.ident)
            .await
            .expect("the table must load");
        let tx = Transaction::new(&table);
        let action = tx.update_schema();
        let action = match change {
            Change::Rename(from, to) => action.rename_column(from, to),
            Change::Drop(name) => action.delete_column(name),
            Change::Add(name, kind) => action.add_column(name, Type::Primitive(kind)),
            Change::Promote(name, kind) => action.update_column(name, kind),
        };
        let tx = action.apply(tx).expect("the schema change must apply");
        tx.commit(self.catalog.as_ref())
            .await
            .expect("the schema change must commit");
        self.session
            .refresh_catalog_provider("ice")
            .await
            .expect("the provider must refresh");
    }

    async fn open_capped(&self) -> MicroBatchSource {
        let options = SourceOptions::from_options(&BTreeMap::from([(
            "streaming-max-files-per-micro-batch".to_string(),
            "1".to_string(),
        )]))
        .expect("a file cap must parse");
        MicroBatchSource::open(&self.session, &self.name, options)
            .await
            .expect("the source must open")
    }
}

async fn drain_rendered(source: &MicroBatchSource) -> Vec<String> {
    let mut cursor = source
        .initial_offset()
        .await
        .expect("the initial offset must resolve")
        .expect("a nonempty table must have a start");
    let mut rendered = Vec::new();
    while let Some(batch) = source
        .next_batch(&cursor, WindowLimit::Capped)
        .await
        .expect("the window must plan")
    {
        rendered.push(render(&batch).await);
        cursor = batch.end;
    }
    rendered
}

async fn render(batch: &SourceBatch) -> String {
    let fields: Vec<String> = batch
        .frame
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}:{}", field.name(), field.data_type()))
        .collect();
    let frames = batch
        .frame
        .clone()
        .collect()
        .await
        .expect("the batch must read");
    let rows = pretty_format_batches(&frames).expect("the batch must render");
    format!("{}\n{rows}", fields.join(","))
}

fn id_note() -> Vec<NestedField> {
    vec![
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        NestedField::optional(2, "note", Type::Primitive(PrimitiveType::String)),
    ]
}

#[tokio::test]
async fn a_rename_before_start_reads_every_batch_under_the_new_name() {
    let table = Evolving::create("renamed", id_note()).await;
    table.insert("(1, 'a')").await;
    table.evolve(Change::Rename("note", "memo")).await;
    table.insert("(2, 'b')").await;
    let source = table.open_capped().await;
    assert_eq!(
        drain_rendered(&source).await,
        vec![
            "id:Int64,memo:Utf8\n+----+------+\n| id | memo |\n+----+------+\n| 1  | a    |\n+----+------+",
            "id:Int64,memo:Utf8\n+----+------+\n| id | memo |\n+----+------+\n| 2  | b    |\n+----+------+",
        ]
    );
}

#[tokio::test]
async fn a_drop_and_re_add_never_shows_the_dropped_field() {
    let table = Evolving::create("readded", id_note()).await;
    table.insert("(1, 'old')").await;
    table.evolve(Change::Drop("note")).await;
    table.insert("(2)").await;
    table
        .evolve(Change::Add("note", PrimitiveType::String))
        .await;
    table.insert("(3, 'new')").await;
    let source = table.open_capped().await;
    assert_eq!(
        drain_rendered(&source).await,
        vec![
            "id:Int64,note:Utf8\n+----+------+\n| id | note |\n+----+------+\n| 1  |      |\n+----+------+",
            "id:Int64,note:Utf8\n+----+------+\n| id | note |\n+----+------+\n| 2  |      |\n+----+------+",
            "id:Int64,note:Utf8\n+----+------+\n| id | note |\n+----+------+\n| 3  | new  |\n+----+------+",
        ]
    );
}

#[tokio::test]
async fn a_type_promotion_before_start_reads_every_batch_promoted() {
    let table = Evolving::create(
        "promoted",
        vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(3, "f", Type::Primitive(PrimitiveType::Float)),
        ],
    )
    .await;
    table.insert("(1, 10, CAST(1.5 AS FLOAT))").await;
    table
        .evolve(Change::Promote("v", PrimitiveType::Long))
        .await;
    table
        .evolve(Change::Promote("f", PrimitiveType::Double))
        .await;
    table.insert("(2, 20000000000, 2.25)").await;
    let source = table.open_capped().await;
    assert_eq!(
        drain_rendered(&source).await,
        vec![
            "id:Int64,v:Int64,f:Float64\n+----+----+-----+\n| id | v  | f   |\n+----+----+-----+\n| 1  | 10 | 1.5 |\n+----+----+-----+",
            "id:Int64,v:Int64,f:Float64\n+----+-------------+------+\n| id | v           | f    |\n+----+-------------+------+\n| 2  | 20000000000 | 2.25 |\n+----+-------------+------+",
        ]
    );
}

#[tokio::test]
async fn a_schema_change_after_open_leaves_the_batch_schema_alone() {
    let table = Evolving::create("held", id_note()).await;
    table.insert("(1, 'a')").await;
    let source = table.open_capped().await;
    table.evolve(Change::Rename("note", "memo")).await;
    table
        .evolve(Change::Add("extra", PrimitiveType::String))
        .await;
    table.insert("(2, 'b', 'x')").await;
    assert_eq!(
        drain_rendered(&source).await,
        vec![
            "id:Int64,note:Utf8\n+----+------+\n| id | note |\n+----+------+\n| 1  | a    |\n+----+------+",
            "id:Int64,note:Utf8\n+----+------+\n| id | note |\n+----+------+\n| 2  | b    |\n+----+------+",
        ]
    );
}

fn parse(pairs: &[(&str, &str)]) -> Result<SourceOptions, MicroBatchError> {
    SourceOptions::from_options(
        &pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    )
}

#[test]
fn a_key_that_folds_to_a_streaming_prefix_only_under_unicode_refuses() {
    for key in [
        "repar\u{212A}.cdc.start-after-snapshot-id",
        "streaming-s\u{212A}ip-overwrite-snapshots",
        "STREAMING-MAX-FILES-PER-MICRO-BATCH\u{212A}",
        "Stream-From-Timestamp-\u{00E9}",
    ] {
        let error = parse(&[(key, "5")]).expect_err("a Unicode-folded streaming key must refuse");
        assert_eq!(
            error.to_string(),
            format!(
                "streaming option {key:?} names a streaming key only under Unicode case folding; option keys match ASCII case-insensitively, so spell it in ASCII"
            )
        );
    }
    for key in [
        "\u{0130}STREAMING-MAX-FILES-PER-MICRO-BATCH",
        "note-\u{212A}",
    ] {
        let options = parse(&[(key, "1")]).expect("a non-ASCII key outside the prefixes passes");
        assert_eq!(options.caps, ReadCaps::default());
        assert_eq!(options.start, StartPosition::Earliest);
    }
}

#[test]
fn boolean_twins_compare_by_meaning() {
    for (first, second) in [("false", "FALSE"), ("False", "false")] {
        let options = parse(&[
            ("Streaming-Skip-Overwrite-Snapshots", first),
            ("streaming-skip-overwrite-snapshots", second),
        ])
        .expect("twins that both mean false must parse");
        assert_eq!(options.caps, ReadCaps::default());
    }
    let error = parse(&[
        ("STREAMING-SKIP-DELETE-SNAPSHOTS", "TRUE"),
        ("streaming-skip-delete-snapshots", "true"),
    ])
    .expect_err("twins that both mean true refuse the skip");
    assert!(matches!(error, MicroBatchError::SkipOptionRefused { .. }));
    for (first, second) in [("false", "TRUE"), ("false", "0")] {
        let error = parse(&[
            ("Streaming-Skip-Overwrite-Snapshots", first),
            ("streaming-skip-overwrite-snapshots", second),
        ])
        .expect_err("twins that differ in meaning must refuse");
        assert_eq!(
            error.to_string(),
            "streaming options Streaming-Skip-Overwrite-Snapshots and streaming-skip-overwrite-snapshots are the same key in different case with different values; pass it once"
        );
    }
    let error = parse(&[
        ("Stream-From-Timestamp", "AbC"),
        ("stream-from-timestamp", "abc"),
    ])
    .expect_err("a non-boolean key keeps the exact comparison");
    assert!(error.to_string().contains("different values"), "{error}");
}

#[tokio::test]
async fn planner_errors_name_the_table_as_the_source_was_opened() {
    let (warehouse, session) = session_with_two_appends().await;
    let root = warehouse
        .path()
        .to_str()
        .expect("the warehouse path must be text");
    let (source, end) = drained_source(&session).await;
    assert_eq!(end.table_name, "sales.orders");
    let handle = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the catalog must be visible");
    let sales = NamespaceIdent::new("sales".to_string());
    handle
        .drop_table(&TableIdent::new(sales.clone(), "orders".to_string()))
        .await
        .expect("the table must drop");
    let replacement = handle
        .create_table(&sales, orders_creation(&format!("{root}/sales/orders-v2")))
        .await
        .expect("the replacement must create");
    session
        .refresh_catalog_provider("ice")
        .await
        .expect("the provider must refresh");
    let error = source
        .next_batch(&end, WindowLimit::Capped)
        .await
        .err()
        .expect("a replaced table must refuse");
    assert_eq!(
        error.to_string(),
        format!(
            "source table ice.sales.orders was replaced (recorded uuid {}, current uuid {}); start a new query (new queryName)",
            end.table,
            TableUuid::of(&replacement)
        )
    );
}

#[test]
fn a_state_snapshot_carries_the_settings_and_the_start_time_of_its_moment() {
    let context = SessionContext::new();
    let weak = WeakSessionState::of(&context);
    let first = weak.snapshot().expect("the context is alive");
    let started = first
        .execution_props()
        .query_execution_start_time
        .expect("a snapshot marks its start");
    context
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .set("datafusion.execution.batch_size", "17")
        .expect("the setting must apply");
    let second = weak.snapshot().expect("the context is alive");
    let props = second.execution_props();
    let bound = props
        .config_options()
        .expect("a snapshot binds the settings its plan runs under");
    assert_eq!(bound.execution.batch_size, 17);
    assert_eq!(second.config().options().execution.batch_size, 17);
    assert!(props.query_execution_start_time.expect("a start time") >= started);
    drop(context);
    assert!(weak.snapshot().is_none());
}

fn record_ending_at(offset: InputOffset) -> SinkRecord {
    SinkRecord {
        format: OffsetFormatVersion::CURRENT,
        query: QueryId::new(Uuid::nil()),
        run: RunId::new(Uuid::nil()),
        epoch: Epoch::new(0),
        generation: Generation::new(1).expect("generation 1 is valid"),
        offsets: OffsetVector::single(offset),
    }
}

#[tokio::test]
async fn only_an_offset_that_ends_its_snapshot_is_a_start_after_position() {
    let (_warehouse, session) = session_with_two_appends().await;
    let options = SourceOptions::from_options(&options_of(&[])).expect("no option must parse");
    let source = MicroBatchSource::open(&session, "ice.sales.orders", options)
        .await
        .expect("the source must open");
    let from = source
        .initial_offset()
        .await
        .expect("the initial offset must resolve")
        .expect("a nonempty table must have a start");
    let end = source
        .next_batch(&from, WindowLimit::Unbounded)
        .await
        .expect("the window must plan")
        .expect("two appends are one unbounded batch")
        .end;
    assert!(end.position.get() > 0);
    assert_eq!(
        source
            .whole_snapshot_end(&record_ending_at(end.clone()))
            .await,
        Some(end.snapshot)
    );
    let inside = InputOffset {
        position: FilePosition::new(end.position.get() - 1),
        ..end.clone()
    };
    assert_eq!(
        source.whole_snapshot_end(&record_ending_at(inside)).await,
        None
    );
    let elsewhere = InputOffset {
        table: TableUuid::new(Uuid::nil()),
        table_name: String::from("ice.sales.other"),
        ..end
    };
    assert_eq!(
        source
            .whole_snapshot_end(&record_ending_at(elsewhere))
            .await,
        None
    );
}
