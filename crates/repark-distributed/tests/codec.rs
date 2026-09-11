#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_methods)]

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ballista_core::JobId;
use ballista_core::execution_plans::sort_shuffle::SortShuffleConfig;
use ballista_core::execution_plans::{
    ChaosExec, ShuffleReaderExec, ShuffleWriterExec, SortShuffleWriterExec, UnresolvedShuffleExec,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::col;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, displayable,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_core::{CatalogKind, CatalogSpec, ReparkSession};
use repark_distributed::{
    DistributedExecutor, IcebergScanSpec, LocalDataFusionExecutor, ReparkClusterExecutor,
    ReparkSessionProvider, repark_ballista_codec,
};

const CATALOG_NAME: &str = "ice";
const NAMESPACE: &str = "sales";
const TABLE: &str = "orders";
const INSERT_COUNT: usize = 8;

fn unique_warehouse() -> PathBuf {
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    let path = std::env::temp_dir().join(format!("repark-m2a-ice-{}-{nanos}", std::process::id()));
    if let Err(error) = std::fs::create_dir_all(&path) {
        panic!("create warehouse {}: {error}", path.display());
    }
    path
}

fn scan_spec(warehouse: &str) -> IcebergScanSpec {
    let mut props = HashMap::new();
    props.insert("warehouse".to_owned(), warehouse.to_owned());
    IcebergScanSpec::new(
        CatalogSpec {
            name: CATALOG_NAME.to_owned(),
            kind: CatalogKind::Memory,
            props,
        },
        vec![
            CATALOG_NAME.to_owned(),
            NAMESPACE.to_owned(),
            TABLE.to_owned(),
        ],
        None,
        Some(vec!["id".to_owned()]),
        vec!["id >= 4".to_owned()],
    )
}

async fn iceberg_session(warehouse: &str) -> (ReparkSession, SessionContext) {
    let session = match ReparkSession::builder().target_partitions(2).build() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::build: {error}"),
    };
    if let Err(error) = session
        .register_memory_catalog(CATALOG_NAME, warehouse)
        .await
    {
        panic!("register_memory_catalog: {error}");
    }
    if let Err(error) = session
        .create_namespace(CATALOG_NAME, NAMESPACE, HashMap::new())
        .await
    {
        panic!("create_namespace: {error}");
    }
    if let Err(error) = session
        .testing_oob_create_table(CATALOG_NAME, NAMESPACE, TABLE, warehouse)
        .await
    {
        panic!("testing_oob_create_table: {error}");
    }
    if let Err(error) = session.refresh_catalog_provider(CATALOG_NAME).await {
        panic!("refresh_catalog_provider: {error}");
    }
    for value in 0..INSERT_COUNT {
        let sql = format!("INSERT INTO {CATALOG_NAME}.{NAMESPACE}.{TABLE} VALUES ({value})");
        let frame = match session.context().sql(&sql).await {
            Ok(frame) => frame,
            Err(error) => panic!("insert {sql}: {error}"),
        };
        if let Err(error) = frame.collect().await {
            panic!("insert collect {sql}: {error}");
        }
    }
    let context = session.context().clone();
    (session, context)
}

fn plan_verbose(node: &Arc<dyn ExecutionPlan>) -> String {
    displayable(node.as_ref()).indent(true).to_string()
}

fn verbose_snapshot_id(node: &Arc<dyn ExecutionPlan>) -> i64 {
    let text = plan_verbose(node);
    let Some(start) = text.find("] snapshot_id=") else {
        panic!("scan verbose text has no snapshot_id field: {text}");
    };
    let rest = &text[start + "] snapshot_id=".len()..];
    let end = rest
        .find(|character: char| !character.is_ascii_digit() && character != '-')
        .unwrap_or(rest.len());
    match rest[..end].parse::<i64>() {
        Ok(id) => id,
        Err(error) => panic!("snapshot id {:?} is not an i64: {error}", &rest[..end]),
    }
}

type ShuffleCase = (String, Arc<dyn ExecutionPlan>, Vec<Arc<dyn ExecutionPlan>>);

fn shuffle_nodes() -> Vec<ShuffleCase> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let key = match col("id", schema.as_ref()) {
        Ok(column) => column,
        Err(error) => panic!("col id: {error}"),
    };
    let partitioning = Partitioning::Hash(vec![key], 4);
    let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let writer = match ShuffleWriterExec::try_new(
        JobId::new("job-1"),
        1,
        Arc::clone(&input),
        "/tmp/repark-m2a-shuffle".to_owned(),
        Some(partitioning.clone()),
    ) {
        Ok(exec) => exec,
        Err(error) => panic!("ShuffleWriterExec: {error}"),
    };
    let sort_writer = match SortShuffleWriterExec::try_new(
        JobId::new("job-1"),
        2,
        Arc::clone(&input),
        "/tmp/repark-m2a-shuffle".to_owned(),
        partitioning.clone(),
        SortShuffleConfig::default(),
    ) {
        Ok(exec) => exec,
        Err(error) => panic!("SortShuffleWriterExec: {error}"),
    };
    let reader = match ShuffleReaderExec::try_new(
        3,
        Vec::new(),
        Arc::clone(&schema),
        partitioning.clone(),
    ) {
        Ok(exec) => exec,
        Err(error) => panic!("ShuffleReaderExec: {error}"),
    };
    let chaos = match ChaosExec::new(Arc::clone(&input), 0.0, "transient", Some(7)) {
        Ok(exec) => exec,
        Err(error) => panic!("ChaosExec: {error}"),
    };
    vec![
        (
            "ShuffleWriterExec".to_owned(),
            Arc::new(writer) as Arc<dyn ExecutionPlan>,
            vec![Arc::clone(&input)],
        ),
        (
            "SortShuffleWriterExec".to_owned(),
            Arc::new(sort_writer) as Arc<dyn ExecutionPlan>,
            vec![Arc::clone(&input)],
        ),
        (
            "ShuffleReaderExec".to_owned(),
            Arc::new(reader) as Arc<dyn ExecutionPlan>,
            Vec::new(),
        ),
        (
            "UnresolvedShuffleExec".to_owned(),
            Arc::new(UnresolvedShuffleExec::new(4, schema, partitioning)) as Arc<dyn ExecutionPlan>,
            Vec::new(),
        ),
        (
            "ChaosExec".to_owned(),
            Arc::new(chaos) as Arc<dyn ExecutionPlan>,
            vec![input],
        ),
    ]
}

#[derive(Debug)]
struct UnownedScanExec {
    properties: Arc<PlanProperties>,
}

impl UnownedScanExec {
    fn new(schema: SchemaRef) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for UnownedScanExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(formatter, "UnownedScanExec")
    }
}

impl ExecutionPlan for UnownedScanExec {
    fn name(&self) -> &'static str {
        "UnownedScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Plan(
                "UnownedScanExec takes no children".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "UnownedScanExec cannot execute".to_owned(),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repark_ballista_codec_installs_the_repark_physical_wrapper() {
    let session = match ReparkSession::new() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::new: {error}"),
    };
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let installed = format!("{:?}", codec.physical_extension_codec());
    assert!(
        installed.contains("ReparkPhysicalExtensionCodec"),
        "installed physical codec must be the RePark wrapper, got {installed}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ballista_shuffle_nodes_round_trip_through_the_wrapper() {
    let session = match ReparkSession::new() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::new: {error}"),
    };
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    let task = SessionContext::new().task_ctx();
    for (label, node, inputs) in shuffle_nodes() {
        let mut buffer = Vec::new();
        if let Err(error) = physical.try_encode(Arc::clone(&node), &mut buffer) {
            panic!("{label} encode through the wrapper failed: {error}");
        }
        assert!(
            !buffer.is_empty(),
            "{label} encode produced no bytes through the wrapper"
        );
        let decoded = match physical.try_decode(&buffer, &inputs, &task) {
            Ok(plan) => plan,
            Err(error) => panic!("{label} decode through the wrapper failed: {error}"),
        };
        assert!(
            decoded.name() == node.name(),
            "{label} decoded as {}",
            decoded.name()
        );
        assert!(
            decoded.schema() == node.schema(),
            "{label} schema changed through the codec"
        );
        let before = format!("{:?}", node.properties().partitioning);
        let after = format!("{:?}", decoded.properties().partitioning);
        assert!(
            after == before,
            "{label} partitioning changed through the codec: {after} != {before}"
        );
        assert!(
            decoded.output_partitioning().partition_count()
                == node.output_partitioning().partition_count(),
            "{label} partition count changed through the codec"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iceberg_table_scan_round_trips_through_the_wrapper() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = iceberg_session(&warehouse_text).await;
    let spec = scan_spec(&warehouse_text);
    let scan = match spec.scan(&context).await {
        Ok(plan) => plan,
        Err(error) => panic!("spec.scan: {error}"),
    };
    assert!(
        scan.name() == "IcebergTableScan",
        "spec.scan must produce IcebergTableScan, got {}",
        scan.name()
    );
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();

    let mut buffer = Vec::new();
    if let Err(error) = physical.try_encode(Arc::clone(&scan), &mut buffer) {
        panic!("wrapper refused the IcebergTableScan on encode: {error}");
    }
    let wire = match IcebergScanSpec::decode(&buffer) {
        Ok(decoded) => decoded,
        Err(error) => panic!("encoded payload is not an IcebergScanSpec: {error}"),
    };
    assert!(
        wire.catalog.name == CATALOG_NAME,
        "wire catalog name {:?} != {CATALOG_NAME}",
        wire.catalog.name
    );
    assert!(
        wire.catalog.kind == CatalogKind::Memory,
        "wire catalog kind {:?} != Memory",
        wire.catalog.kind
    );
    assert!(
        wire.table_identifier == spec.table_identifier,
        "wire table identifier {:?} != {:?}",
        wire.table_identifier,
        spec.table_identifier
    );
    let frozen_snapshot = verbose_snapshot_id(&scan);
    assert!(
        wire.snapshot_id == Some(frozen_snapshot),
        "wire snapshot id {:?} != the scan's frozen {frozen_snapshot}",
        wire.snapshot_id
    );
    assert!(
        wire.projection == spec.projection,
        "wire projection {:?} != {:?}",
        wire.projection,
        spec.projection
    );
    assert!(
        wire.filters == spec.filters,
        "wire filters {:?} != {:?}",
        wire.filters,
        spec.filters
    );

    let task = SessionContext::new().task_ctx();
    let decoded = match physical.try_decode(&buffer, &[], &task) {
        Ok(plan) => plan,
        Err(error) => panic!("wrapper refused the IcebergTableScan on decode: {error}"),
    };
    assert!(
        decoded.name() == "IcebergTableScan",
        "decoded node {} is not IcebergTableScan",
        decoded.name()
    );
    assert!(
        decoded.schema() == scan.schema(),
        "decoded scan schema changed through the codec"
    );
    assert!(
        decoded.output_partitioning().partition_count()
            == scan.output_partitioning().partition_count(),
        "decoded scan partition count changed through the codec"
    );
    assert!(
        plan_verbose(&decoded) == plan_verbose(&scan),
        "decoded scan display {} != original {}",
        plan_verbose(&decoded),
        plan_verbose(&scan)
    );
    let mut again = Vec::new();
    if let Err(error) = physical.try_encode(Arc::clone(&decoded), &mut again) {
        panic!("decoded scan did not re-encode: {error}");
    }
    assert!(
        again == buffer,
        "decoded scan re-encoded to different bytes than the original"
    );
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrapper_refuses_the_scan_without_the_session_catalog() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = iceberg_session(&warehouse_text).await;
    let spec = scan_spec(&warehouse_text);
    let scan = match spec.scan(&context).await {
        Ok(plan) => plan,
        Err(error) => panic!("spec.scan: {error}"),
    };
    let payload = match spec.encode() {
        Ok(bytes) => bytes,
        Err(error) => panic!("spec encode: {error}"),
    };
    let vanilla_provider = ReparkSessionProvider::from_context(&SessionContext::new());
    let vanilla_codec = repark_ballista_codec(&vanilla_provider);
    let physical = vanilla_codec.physical_extension_codec();
    let task = SessionContext::new().task_ctx();

    let decode_error = match physical.try_decode(&payload, &[], &task) {
        Ok(_) => panic!("vanilla codec decoded an IcebergTableScan from ambient authority"),
        Err(error) => error.to_string(),
    };
    assert!(
        decode_error.contains("not registered") || decode_error.contains("ReparkSessionProvider"),
        "vanilla decode refusal should name the missing session catalog, got {decode_error}"
    );
    let mut buffer = Vec::new();
    let encode_error = match physical.try_encode(scan, &mut buffer) {
        Ok(()) => panic!("vanilla codec encoded an IcebergTableScan it cannot locate"),
        Err(error) => error.to_string(),
    };
    assert!(
        !encode_error.is_empty(),
        "vanilla encode must refuse loud, got {encode_error}"
    );
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unowned_node_refuses_encode_and_passes_the_rewrite_untouched() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = iceberg_session(&warehouse_text).await;
    let spec = scan_spec(&warehouse_text);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(UnownedScanExec::new(schema));

    let rewritten = match spec
        .rewrite_iceberg_table_scans_as_file_groups(Arc::clone(&plan), &context)
        .await
    {
        Ok(plan) => plan,
        Err(error) => panic!("rewrite refused an unowned node: {error}"),
    };
    assert!(
        Arc::ptr_eq(&rewritten, &plan),
        "the file-group rewrite must leave a plan with no IcebergTableScan untouched"
    );

    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    let mut buffer = Vec::new();
    let encode_error = match physical.try_encode(plan, &mut buffer) {
        Ok(()) => panic!("unowned node encoded through the wrapper"),
        Err(error) => error.to_string(),
    };
    assert!(
        encode_error.contains("Unsupported plan node") && encode_error.contains("UnownedScanExec"),
        "unowned node must refuse encode loud, got {encode_error}"
    );
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

fn named_spec(
    warehouse: &str,
    table: &str,
    projection: Option<Vec<String>>,
    filters: Vec<String>,
) -> IcebergScanSpec {
    let mut props = HashMap::new();
    props.insert("warehouse".to_owned(), warehouse.to_owned());
    IcebergScanSpec::new(
        CatalogSpec {
            name: CATALOG_NAME.to_owned(),
            kind: CatalogKind::Memory,
            props,
        },
        vec![
            CATALOG_NAME.to_owned(),
            NAMESPACE.to_owned(),
            table.to_owned(),
        ],
        None,
        projection,
        filters,
    )
}

async fn adversarial_session(warehouse: &str) -> (ReparkSession, SessionContext) {
    let session = match ReparkSession::builder().target_partitions(2).build() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::build: {error}"),
    };
    if let Err(error) = session
        .register_memory_catalog(CATALOG_NAME, warehouse)
        .await
    {
        panic!("register_memory_catalog: {error}");
    }
    if let Err(error) = session
        .create_namespace(CATALOG_NAME, NAMESPACE, HashMap::new())
        .await
    {
        panic!("create_namespace: {error}");
    }
    let statements = [
        "CREATE TABLE ice.sales.strs (id INT NOT NULL, name STRING NOT NULL)",
        "CREATE TABLE ice.sales.dates (id INT NOT NULL, d DATE, ts TIMESTAMP)",
        "CREATE TABLE ice.sales.t2 (\"we]col\" INT NOT NULL)",
        "CREATE TABLE ice.sales.\"we]t\" (id INT NOT NULL)",
        "INSERT INTO ice.sales.strs VALUES (1, 'alpha'), (2, 'x] snapshot_id=1'), (3, 'beta')",
        "INSERT INTO ice.sales.dates VALUES (1, DATE '2024-01-01', TIMESTAMP '2024-01-02 03:04:05'), (2, DATE '2024-06-01', TIMESTAMP '2024-06-02 03:04:05')",
        "INSERT INTO ice.sales.t2 VALUES (5)",
        "INSERT INTO ice.sales.\"we]t\" VALUES (7)",
    ];
    for sql in statements {
        let frame = match session.sql(sql).await {
            Ok(frame) => frame,
            Err(error) => panic!("sql {sql}: {error}"),
        };
        if let Err(error) = frame.collect().await {
            panic!("collect {sql}: {error}");
        }
    }
    let context = session.context().clone();
    (session, context)
}

async fn built_scan(context: &SessionContext, spec: &IcebergScanSpec) -> Arc<dyn ExecutionPlan> {
    match spec.scan(context).await {
        Ok(plan) => plan,
        Err(error) => panic!("spec.scan for {:?}: {error}", spec.table_identifier),
    }
}

fn encode_or_refusal(
    physical: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec,
    scan: &Arc<dyn ExecutionPlan>,
    label: &str,
) -> Result<Vec<u8>, String> {
    let mut buffer = Vec::new();
    match physical.try_encode(Arc::clone(scan), &mut buffer) {
        Ok(()) => {
            assert!(!buffer.is_empty(), "{label} encode produced no bytes");
            Ok(buffer)
        }
        Err(error) => {
            assert!(
                buffer.is_empty(),
                "{label} refusal must not emit bytes, got {}",
                buffer.len()
            );
            Err(error.to_string())
        }
    }
}

fn assert_refusal_names_field(label: &str, message: &str, field: &str) {
    assert!(
        message.contains(field),
        "{label} refusal must name the {field} field, got {message}"
    );
    assert!(
        message.contains("IcebergTableScan"),
        "{label} refusal must name IcebergTableScan, got {message}"
    );
}

fn decoded_scan(
    physical: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec,
    buffer: &[u8],
    label: &str,
) -> Arc<dyn ExecutionPlan> {
    let task = SessionContext::new().task_ctx();
    match physical.try_decode(buffer, &[], &task) {
        Ok(plan) => plan,
        Err(error) => panic!(
            "{label} encoded bytes did not decode — the codec emitted a spec it cannot \
             rebuild: {error}"
        ),
    }
}

fn assert_same_scan(
    label: &str,
    decoded: &Arc<dyn ExecutionPlan>,
    original: &Arc<dyn ExecutionPlan>,
) {
    assert!(
        decoded.name() == original.name(),
        "{label} decoded as {}",
        decoded.name()
    );
    assert!(
        decoded.schema() == original.schema(),
        "{label} schema changed through the codec"
    );
    assert!(
        decoded.output_partitioning().partition_count()
            == original.output_partitioning().partition_count(),
        "{label} partition count changed through the codec"
    );
    assert!(
        plan_verbose(decoded) == plan_verbose(original),
        "{label} decoded scan display {} != original {}",
        plan_verbose(decoded),
        plan_verbose(original)
    );
}

fn sorted_rows(batches: &[RecordBatch], label: &str) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for column in batch.columns() {
                match array_value_to_string(column, row) {
                    Ok(cell) => cells.push(cell),
                    Err(error) => panic!("{label} cell format: {error}"),
                }
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

async fn drain(stream: SendableRecordBatchStream, label: &str) -> Vec<RecordBatch> {
    let mut stream = stream;
    let mut batches = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(batch) => batches.push(batch),
            Err(error) => panic!("{label} stream: {error}"),
        }
    }
    batches
}

async fn cluster_batches(
    label: &str,
    session: &ReparkSession,
    context: &SessionContext,
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<RecordBatch> {
    let local = LocalDataFusionExecutor::new(context.clone());
    let local_handle = match local.execute(Arc::clone(plan)).await {
        Ok(handle) => handle,
        Err(error) => panic!("{label} local execute: {error}"),
    };
    let expected = drain(local_handle.stream(), label).await;

    let provider = ReparkSessionProvider::from_session(session);
    let cluster = match ReparkClusterExecutor::new(2, bind_address(), provider).await {
        Ok(cluster) => cluster,
        Err(error) => panic!("{label} ReparkClusterExecutor::new: {error}"),
    };
    let handle = match cluster.execute(Arc::clone(plan)).await {
        Ok(handle) => handle,
        Err(error) => panic!("{label} cluster execute: {error}"),
    };
    let Ok(got) =
        tokio::time::timeout(Duration::from_secs(30), drain(handle.stream(), label)).await
    else {
        panic!("{label} cluster drain timed out");
    };
    assert!(
        sorted_rows(&got, label) == sorted_rows(&expected, label),
        "{label} cluster rows != LocalDataFusionExecutor rows"
    );
    got
}

fn bind_address() -> SocketAddr {
    match "127.0.0.1:0".parse() {
        Ok(address) => address,
        Err(error) => panic!("bind address: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn string_literal_injection_predicate_refuses_loud() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = adversarial_session(&warehouse_text).await;
    let spec = named_spec(
        &warehouse_text,
        "strs",
        Some(vec!["id".to_owned()]),
        vec!["name = 'x] snapshot_id=1'".to_owned()],
    );
    let scan = built_scan(&context, &spec).await;
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    match encode_or_refusal(physical, &scan, "literal-injection") {
        Err(message) => assert_refusal_names_field("literal-injection", &message, "predicate"),
        Ok(buffer) => {
            let decoded = decoded_scan(physical, &buffer, "literal-injection");
            assert_same_scan("literal-injection", &decoded, &scan);
        }
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn string_literal_predicate_refuses_loud() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = adversarial_session(&warehouse_text).await;
    let spec = named_spec(
        &warehouse_text,
        "strs",
        None,
        vec!["name = 'alpha'".to_owned()],
    );
    let scan = built_scan(&context, &spec).await;
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    match encode_or_refusal(physical, &scan, "string-literal") {
        Err(message) => assert_refusal_names_field("string-literal", &message, "predicate"),
        Ok(buffer) => {
            let decoded = decoded_scan(physical, &buffer, "string-literal");
            assert_same_scan("string-literal", &decoded, &scan);
        }
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn date_and_timestamp_predicates_measure_the_pushdown_surface() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = adversarial_session(&warehouse_text).await;
    let timestamp_spec = named_spec(
        &warehouse_text,
        "dates",
        None,
        vec!["ts = TIMESTAMP '2024-01-02 03:04:05'".to_owned()],
    );
    match timestamp_spec.scan(&context).await {
        Ok(_) => panic!("the fork bound a string-typed timestamp literal as a scan predicate"),
        Err(error) => assert!(
            error.to_string().contains("timestamp"),
            "timestamp literal refusal should name its type, got {error}"
        ),
    }
    let spec = named_spec(
        &warehouse_text,
        "dates",
        None,
        vec!["d = DATE '2024-01-01'".to_owned()],
    );
    let scan = built_scan(&context, &spec).await;
    assert!(
        plan_verbose(&scan).contains("predicate:[]"),
        "measured: the fork drops the DATE predicate out of the scan node, got {}",
        plan_verbose(&scan)
    );
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    match encode_or_refusal(physical, &scan, "date-predicate") {
        Err(message) => assert_refusal_names_field("date-predicate", &message, "predicate"),
        Ok(buffer) => {
            let decoded = decoded_scan(physical, &buffer, "date-predicate");
            assert_same_scan("date-predicate", &decoded, &scan);
            cluster_batches("date-predicate", &session, &context, &scan).await;
        }
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_list_predicate_travels_exactly_or_refuses() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = adversarial_session(&warehouse_text).await;
    let spec = named_spec(
        &warehouse_text,
        "strs",
        None,
        vec!["id IN (1, 2, 3)".to_owned()],
    );
    let scan = built_scan(&context, &spec).await;
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    match encode_or_refusal(physical, &scan, "in-list") {
        Err(message) => assert_refusal_names_field("in-list", &message, "predicate"),
        Ok(buffer) => {
            let decoded = decoded_scan(physical, &buffer, "in-list");
            assert_same_scan("in-list", &decoded, &scan);
            cluster_batches("in-list", &session, &context, &scan).await;
        }
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bracket_column_projection_refuses_loud() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = adversarial_session(&warehouse_text).await;
    let spec = named_spec(
        &warehouse_text,
        "t2",
        Some(vec!["we]col".to_owned()]),
        Vec::new(),
    );
    let scan = built_scan(&context, &spec).await;
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    match encode_or_refusal(physical, &scan, "bracket-column") {
        Err(message) => assert_refusal_names_field("bracket-column", &message, "projection"),
        Ok(buffer) => {
            let decoded = decoded_scan(physical, &buffer, "bracket-column");
            assert_same_scan("bracket-column", &decoded, &scan);
        }
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bracket_table_identifier_refuses_loud() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = adversarial_session(&warehouse_text).await;
    let spec = named_spec(&warehouse_text, "we]t", None, Vec::new());
    let scan = built_scan(&context, &spec).await;
    let provider = ReparkSessionProvider::from_session(&session);
    let codec = repark_ballista_codec(&provider);
    let physical = codec.physical_extension_codec();
    match encode_or_refusal(physical, &scan, "bracket-table") {
        Err(message) => assert_refusal_names_field("bracket-table", &message, "identifier"),
        Ok(buffer) => {
            let decoded = decoded_scan(physical, &buffer, "bracket-table");
            assert_same_scan("bracket-table", &decoded, &scan);
        }
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}
