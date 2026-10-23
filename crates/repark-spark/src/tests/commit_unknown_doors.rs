use super::super::*;
use super::commit_unknown_gate::{Attempt, GateCatalog};
use super::common::*;
use super::session_write_conf::set_session_conf;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::compute::cast;
use std::collections::BTreeSet;
use std::path::Path;

const SESSION_KEY: &str = "spark.sql.iceberg.snapshot-property.k";
const INSERT: &str = "INSERT INTO fault.sales.t SELECT id + 10, name FROM src";
const MERGE: &str = "MERGE INTO fault.sales.t AS t USING src AS s ON t.id = s.id \
                     WHEN MATCHED AND s.id = 2 THEN UPDATE SET name = 'm' \
                     WHEN NOT MATCHED THEN INSERT *";
const STATUS_CHECK: [(&str, &str); 4] = [
    ("commit.status-check.num-retries", "1"),
    ("commit.status-check.min-wait-ms", "100"),
    ("commit.status-check.max-wait-ms", "200"),
    ("commit.status-check.total-timeout-ms", "5000"),
];
const STATUS_CHECK_LOADS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Door {
    Plain,
    SessionKey,
    Merge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Script {
    LandedReadsOpen,
    LandedReadsCut,
    AbsentReadsCut,
    AbsentReadsOpen,
}

impl Script {
    fn id(self) -> &'static str {
        match self {
            Self::LandedReadsOpen => "U-1",
            Self::LandedReadsCut => "U-2",
            Self::AbsentReadsCut => "U-3",
            Self::AbsentReadsOpen => "U-4",
        }
    }

    fn landed(self) -> bool {
        matches!(self, Self::LandedReadsOpen | Self::LandedReadsCut)
    }

    fn reads_cut(self) -> bool {
        matches!(self, Self::LandedReadsCut | Self::AbsentReadsCut)
    }
}

impl Door {
    fn statement(self) -> &'static str {
        match self {
            Self::Plain | Self::SessionKey => INSERT,
            Self::Merge => MERGE,
        }
    }

    fn mints_the_error_id(self) -> bool {
        !matches!(self, Self::Plain)
    }

    fn committed_rows(self) -> Vec<(i64, String)> {
        let rows: &[(i64, &str)] = match self {
            Self::Plain | Self::SessionKey => &[
                (1, "a"),
                (2, "b"),
                (3, "c"),
                (11, "a"),
                (12, "b"),
                (13, "c"),
            ],
            Self::Merge => &[(1, "a"), (2, "m"), (3, "c")],
        };
        rows.iter()
            .map(|(id, name)| (*id, (*name).to_string()))
            .collect()
    }
}

async fn statement_result(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(), DataFusionError> {
    execute(ctx, catalogs, sql).await?.collect().await?;
    Ok(())
}

async fn batches_of(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect {sql}: {error}"))
}

async fn id_name_rows(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches_of(
        ctx,
        catalogs,
        "SELECT id, name FROM ice.sales.t ORDER BY id",
    )
    .await
    {
        let ids = cast(batch.column(0), &DataType::Int64).expect("ids as Int64");
        let ids = ids
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an Int64 column");
        let names = cast(batch.column(1), &DataType::Utf8).expect("names as Utf8");
        let names = names
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a Utf8 column");
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), names.value(index).to_string()));
        }
    }
    rows
}

async fn count_of(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> i64 {
    let batches = batches_of(ctx, catalogs, sql).await;
    let column = cast(batches[0].column(0), &DataType::Int64).expect("a count as Int64");
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 count")
        .value(0)
}

async fn snapshot_set(catalogs: &CatalogRegistry) -> BTreeSet<i64> {
    catalogs["ice"]
        .load_table(&TableIdent::from_strs(["sales", "t"]).expect("table ident"))
        .await
        .expect("load sales.t")
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

fn newest_metadata_file(table_dir: &Path) -> String {
    std::fs::read_dir(table_dir.join("metadata"))
        .expect("list the metadata directory")
        .map(|entry| entry.expect("a metadata entry").path())
        .filter(|path| path.to_string_lossy().ends_with(".metadata.json"))
        .max_by_key(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.split_once('-'))
                .and_then(|(version, _)| version.parse::<u64>().ok())
                .expect("a versioned metadata file name")
        })
        .expect("a metadata file")
        .display()
        .to_string()
}

async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry, format: u8) {
    let status_check = STATUS_CHECK
        .iter()
        .map(|(key, value)| format!("'{key}' = '{value}'"))
        .collect::<Vec<_>>()
        .join(", ");
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '{format}', {status_check})"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    let table = catalogs["ice"]
        .load_table(&TableIdent::from_strs(["sales", "t"]).expect("table ident"))
        .await
        .expect("load the drill table");
    for (key, value) in STATUS_CHECK {
        assert_eq!(
            table.metadata().properties().get(key).map(String::as_str),
            Some(value),
            "the drill table carries {key} before the statement"
        );
    }
}

async fn register_gate(
    ctx: &SessionContext,
    catalogs: &mut CatalogRegistry,
    gate: Arc<GateCatalog>,
    warehouse: &Path,
) {
    repark_iceberg::catalog::register_iceberg_catalog(ctx, "fault", gate.clone())
        .await
        .unwrap_or_else(|error| panic!("register the gate catalog: {error}"));
    catalogs.insert(
        "fault".to_string(),
        gate,
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.to_path_buf(),
        },
    );
}

fn assert_statement(
    label: &str,
    door: Door,
    script: Script,
    result: std::result::Result<(), DataFusionError>,
    attempt: &Attempt,
) {
    if script == Script::LandedReadsOpen {
        assert!(
            result.is_ok(),
            "{label}: reconciliation finds the landed snapshot, got {result:?}"
        );
        return;
    }
    let error = result.expect_err("an unresolved commit outcome must surface");
    match repark_core::engine_err(error) {
        repark_core::Error::CommitStateUnknown { operation_id, .. } => {
            let expected = if door.mints_the_error_id() {
                attempt.operation_id.clone()
            } else {
                None
            };
            assert_eq!(operation_id, expected, "{label}: the error's operation id");
        }
        other => panic!("{label}: expected CommitStateUnknown, got {other:?}"),
    }
}

async fn assert_resolution(
    label: &str,
    door: Door,
    script: Script,
    warehouse: &Path,
    attempt: &Attempt,
    pre: (&[(i64, String)], &BTreeSet<i64>),
) {
    let (ctx, catalogs) = setup_v3_at(warehouse).await;
    let newest = newest_metadata_file(&warehouse.join("sales").join("t"));
    run(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.register_table(table => 'sales.t', metadata_file => '{newest}')"),
    )
    .await;
    let expected = i64::from(script.landed());
    let operation_id = attempt
        .operation_id
        .as_deref()
        .expect("every door stamps the attempted snapshot");
    assert_eq!(
        count_of(
            &ctx,
            &catalogs,
            &format!(
                "SELECT count(*) FROM ice.sales.t.snapshots WHERE summary['engine.operation-id'] = '{operation_id}'"
            ),
        )
        .await,
        expected,
        "{label}: snapshots carrying the attempt's operation id"
    );
    assert_eq!(
        count_of(
            &ctx,
            &catalogs,
            &format!(
                "SELECT count(*) FROM ice.sales.t.snapshots WHERE snapshot_id = {}",
                attempt.snapshot_id
            ),
        )
        .await,
        expected,
        "{label}: the attempted snapshot id"
    );
    let mut snapshots = pre.1.clone();
    if script.landed() {
        snapshots.insert(attempt.snapshot_id);
    }
    assert_eq!(
        snapshot_set(&catalogs).await,
        snapshots,
        "{label}: snapshot set"
    );
    let rows = if script.landed() {
        door.committed_rows()
    } else {
        pre.0.to_vec()
    };
    assert_eq!(
        id_name_rows(&ctx, &catalogs).await,
        rows,
        "{label}: rows, once or not at all"
    );
}

async fn drill(door: Door, script: Script, format: u8) {
    let label = format!("{} {door:?} v{format}", script.id());
    let root = TempDir::new().expect("the cell's directory");
    let warehouse = root.path().join("warehouse");
    std::fs::create_dir(&warehouse).expect("the warehouse directory");
    let (ctx, mut catalogs) = setup_v3_at(&warehouse).await;
    seed(&ctx, &catalogs, format).await;
    let pre_rows = id_name_rows(&ctx, &catalogs).await;
    let pre_snapshots = snapshot_set(&catalogs).await;
    let gate = Arc::new(GateCatalog::new(
        catalogs["ice"].clone(),
        script.landed(),
        script.reads_cut(),
    ));
    register_gate(&ctx, &mut catalogs, gate.clone(), &warehouse).await;
    if door == Door::SessionKey {
        set_session_conf(&ctx, SESSION_KEY, "v");
    }
    let result = statement_result(&ctx, &catalogs, door.statement()).await;
    gate.open_reads();
    assert_eq!(
        gate.commits(),
        1,
        "{label}: one commit call, never re-applied"
    );
    assert_eq!(
        gate.forwarded(),
        usize::from(script.landed()),
        "{label}: commits that reached the catalog"
    );
    let failed_loads = if script.reads_cut() {
        STATUS_CHECK_LOADS
    } else {
        0
    };
    assert_eq!(
        gate.failed_loads(),
        failed_loads,
        "{label}: refreshes cut inside the status-check budget"
    );
    let attempts = gate.attempts();
    assert_eq!(attempts.len(), 1, "{label}: one attempted snapshot");
    let attempt = &attempts[0];
    assert_eq!(
        attempt.session_property,
        door == Door::SessionKey,
        "{label}: the session snapshot property marks the routed append"
    );
    assert_statement(&label, door, script, result, attempt);
    assert_resolution(
        &label,
        door,
        script,
        &warehouse,
        attempt,
        (&pre_rows, &pre_snapshots),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u1_plain_insert_landed_reads_open_commits_v2() {
    drill(Door::Plain, Script::LandedReadsOpen, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u1_plain_insert_landed_reads_open_commits_v3() {
    drill(Door::Plain, Script::LandedReadsOpen, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u1_session_key_insert_landed_reads_open_commits_v2() {
    drill(Door::SessionKey, Script::LandedReadsOpen, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u1_session_key_insert_landed_reads_open_commits_v3() {
    drill(Door::SessionKey, Script::LandedReadsOpen, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u1_merge_landed_reads_open_commits_v2() {
    drill(Door::Merge, Script::LandedReadsOpen, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u1_merge_landed_reads_open_commits_v3() {
    drill(Door::Merge, Script::LandedReadsOpen, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u2_plain_insert_landed_reads_cut_raises_and_resolves_committed_v2() {
    drill(Door::Plain, Script::LandedReadsCut, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u2_plain_insert_landed_reads_cut_raises_and_resolves_committed_v3() {
    drill(Door::Plain, Script::LandedReadsCut, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u2_session_key_insert_landed_reads_cut_raises_and_resolves_committed_v2() {
    drill(Door::SessionKey, Script::LandedReadsCut, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u2_session_key_insert_landed_reads_cut_raises_and_resolves_committed_v3() {
    drill(Door::SessionKey, Script::LandedReadsCut, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u2_merge_landed_reads_cut_raises_and_resolves_committed_v2() {
    drill(Door::Merge, Script::LandedReadsCut, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u2_merge_landed_reads_cut_raises_and_resolves_committed_v3() {
    drill(Door::Merge, Script::LandedReadsCut, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u3_plain_insert_absent_reads_cut_raises_and_resolves_not_committed_v2() {
    drill(Door::Plain, Script::AbsentReadsCut, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u3_plain_insert_absent_reads_cut_raises_and_resolves_not_committed_v3() {
    drill(Door::Plain, Script::AbsentReadsCut, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u3_session_key_insert_absent_reads_cut_raises_and_resolves_not_committed_v2() {
    drill(Door::SessionKey, Script::AbsentReadsCut, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u3_session_key_insert_absent_reads_cut_raises_and_resolves_not_committed_v3() {
    drill(Door::SessionKey, Script::AbsentReadsCut, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u3_merge_absent_reads_cut_raises_and_resolves_not_committed_v2() {
    drill(Door::Merge, Script::AbsentReadsCut, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u3_merge_absent_reads_cut_raises_and_resolves_not_committed_v3() {
    drill(Door::Merge, Script::AbsentReadsCut, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u4_plain_insert_absent_reads_open_raises_and_resolves_not_committed_v2() {
    drill(Door::Plain, Script::AbsentReadsOpen, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u4_plain_insert_absent_reads_open_raises_and_resolves_not_committed_v3() {
    drill(Door::Plain, Script::AbsentReadsOpen, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u4_session_key_insert_absent_reads_open_raises_and_resolves_not_committed_v2() {
    drill(Door::SessionKey, Script::AbsentReadsOpen, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u4_session_key_insert_absent_reads_open_raises_and_resolves_not_committed_v3() {
    drill(Door::SessionKey, Script::AbsentReadsOpen, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u4_merge_absent_reads_open_raises_and_resolves_not_committed_v2() {
    drill(Door::Merge, Script::AbsentReadsOpen, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn u4_merge_absent_reads_open_raises_and_resolves_not_committed_v3() {
    drill(Door::Merge, Script::AbsentReadsOpen, 3).await;
}
