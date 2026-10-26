use super::super::*;
use super::common::*;
use super::kill_mid_write_hold::{local_path, reachable};
use super::session_write_conf::set_session_conf;
use datafusion::arrow::array::{BooleanArray, Float64Array};
use datafusion::arrow::compute::{concat_batches, sort_to_indices, take_record_batch};
use datafusion::datasource::MemTable;
use datafusion::prelude::ParquetReadOptions;
use std::collections::BTreeSet;
use std::path::Path;

pub(super) const MIB: usize = 1024 * 1024;
pub(super) const ORDERED_KEY: &str = "repark.write.ordered.max-input-fraction";
pub(super) const MERGE_KEY: &str = "repark.merge.mor.max-updated-fraction";
pub(super) const CHANGELOG_KEY: &str = "repark.changelog.max-source-fraction";
pub(super) const WRITERS_KEY: &str = "repark.write.max-concurrent-files";
pub(super) const ORDERED_KIND: &str = "ordered write";
pub(super) const MERGE_KIND: &str = "merge-on-read MERGE";
pub(super) const CHANGELOG_KIND: &str = "changelog read";
const DENIED: [&str; 6] = ["delete", "drop", "truncate", "expire", "remove", "purge"];
const COLUMNS: &str =
    "id BIGINT, k BIGINT, p INT, a DOUBLE, b DOUBLE, s STRING, c INT, flag BOOLEAN";
pub(super) const D2: &str = "INSERT INTO ice.sales.t SELECT * FROM bsrc";
const D3: &str = "INSERT INTO ice.sales.t BY NAME SELECT * FROM bsrc";
const D4: &str = "INSERT OVERWRITE ice.sales.t SELECT * FROM bsrc";
const D5: &str = "INSERT INTO ice.sales.t REPLACE WHERE id >= 0 SELECT * FROM bsrc";
const D8: &str = "MERGE INTO ice.sales.t t USING bsrc s ON t.id = s.id \
                  WHEN MATCHED THEN UPDATE SET t.a = s.a + 1.0";
const D10: &str = "UPDATE ice.sales.t SET a = a + 1.0";
pub(super) const MOR_MERGE: &str =
    "MERGE INTO ice.sales.t t USING msrc s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.a = s.a";
const NOT_MATCHED_BY_SOURCE: &str = " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.c = -1";
const SOURCE: &str = "SELECT id, k, p, a, b, s, c, flag FROM bsrc";
const PLUS_ONE: &str = "SELECT id, k, p, a + 1.0 AS a, b, s, c, flag FROM bsrc";
const MATCHED_TEN: &str = "SELECT id, k, p, \
                           CASE WHEN (id * 7) % 100 < 10 THEN a + 1.0 ELSE a END AS a, b, s, \
                           CASE WHEN (id * 7) % 100 < 10 THEN c ELSE CAST(-1 AS INT) END AS c, flag \
                           FROM bsrc";
pub(super) const SESSION_KEY: &str = "spark.sql.iceberg.snapshot-property.k";

#[derive(Debug, Clone, Copy)]
pub(super) struct Shape {
    pub(super) partitioned: bool,
    pub(super) writers: usize,
}

const SHAPES: [Shape; 4] = [
    Shape {
        partitioned: false,
        writers: 1,
    },
    Shape {
        partitioned: false,
        writers: 4,
    },
    Shape {
        partitioned: true,
        writers: 1,
    },
    Shape {
        partitioned: true,
        writers: 4,
    },
];

pub(super) struct Lane {
    pub(super) root: TempDir,
    pub(super) ctx: SessionContext,
    pub(super) catalogs: CatalogRegistry,
}

pub(super) struct Observed {
    pub(super) current: Option<i64>,
    pub(super) snapshots: BTreeSet<i64>,
    pub(super) batch: RecordBatch,
    pub(super) files: BTreeSet<String>,
    pub(super) orphans: BTreeSet<String>,
}

fn source_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("k", DataType::Int64, true),
        Field::new("p", DataType::Int32, true),
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("c", DataType::Int32, true),
        Field::new("flag", DataType::Boolean, true),
    ]))
}

fn source_batch(start: i64, stop: i64, rows: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..stop).collect();
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(ids.clone())),
        Arc::new(Int64Array::from(
            ids.iter().map(|id| (id * 7919) % rows).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            ids.iter()
                .map(|id| i32::try_from(id % 32).unwrap())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            ids.iter().map(|id| *id as f64 * 0.5).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            ids.iter()
                .map(|id| ((id * 13) % 1000) as f64)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            ids.iter()
                .map(|id| format!("s{:012}", (id * 31) % 100_000_000))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            ids.iter()
                .map(|id| i32::try_from(id % 1000).unwrap())
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            ids.iter().map(|id| id % 2 == 0).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(source_schema(), columns).unwrap()
}

pub(super) fn register_rows(ctx: &SessionContext, rows: usize) {
    let total = i64::try_from(rows).unwrap();
    let mut batches = Vec::new();
    let mut start = 0;
    while start < total {
        let stop = (start + 8192).min(total);
        batches.push(source_batch(start, stop, total));
        start = stop;
    }
    let table = MemTable::try_new(source_schema(), vec![batches]).unwrap();
    ctx.register_table("bsrc", Arc::new(table)).unwrap();
}

pub(super) async fn lane(pool_mib: usize, keys: &[(&str, &str)], writers: usize) -> Lane {
    let root = TempDir::new().unwrap();
    let (ctx, catalogs) = bounded_at(root.path(), pool_mib, keys, writers).await;
    Lane {
        root,
        ctx,
        catalogs,
    }
}

pub(super) async fn bounded_at(
    warehouse: &Path,
    pool_mib: usize,
    keys: &[(&str, &str)],
    writers: usize,
) -> (SessionContext, CatalogRegistry) {
    let writers = writers.to_string();
    let mut all: Vec<(&str, &str)> = keys.to_vec();
    all.push((WRITERS_KEY, writers.as_str()));
    setup_v3_bounded(warehouse, pool_mib * MIB, &all).await
}

pub(super) async fn attempt(lane: &Lane, sql: &str) -> std::result::Result<(), DataFusionError> {
    execute(&lane.ctx, &lane.catalogs, sql)
        .await?
        .collect()
        .await
        .map(|_| ())
}

pub(super) async fn batches_of(lane: &Lane, sql: &str) -> Vec<RecordBatch> {
    execute(&lane.ctx, &lane.catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect {sql}: {error}"))
}

fn sorted_by_id(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches
        .first()
        .map_or_else(source_schema, RecordBatch::schema);
    let all = concat_batches(&schema, batches).unwrap();
    let indices = sort_to_indices(all.column(0), None, None).unwrap();
    take_record_batch(&all, &indices).unwrap()
}

pub(super) async fn create_target(lane: &Lane, format: u8, partitioned: bool, merge_on_read: bool) {
    let partition = if partitioned {
        " PARTITIONED BY (p)"
    } else {
        ""
    };
    let modes = if merge_on_read {
        ", 'write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', \
         'write.delete.mode' = 'merge-on-read'"
    } else {
        ""
    };
    run(
        &lane.ctx,
        &lane.catalogs,
        &format!(
            "CREATE TABLE ice.sales.t ({COLUMNS}) USING iceberg{partition} \
             TBLPROPERTIES ('format-version' = '{format}'{modes})"
        ),
    )
    .await;
}

pub(super) async fn order_target(lane: &Lane) {
    run(
        &lane.ctx,
        &lane.catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY k",
    )
    .await;
}

pub(super) async fn seed(lane: &Lane, rows: usize, files: usize) {
    let step = rows / files;
    for index in 0..files {
        let start = index * step;
        let stop = if index == files - 1 {
            rows
        } else {
            start + step
        };
        run(
            &lane.ctx,
            &lane.catalogs,
            &format!(
                "INSERT INTO ice.sales.t SELECT * FROM bsrc WHERE id >= {start} AND id < {stop}"
            ),
        )
        .await;
    }
}

fn files_under(dir: &Path) -> BTreeSet<String> {
    let mut files = BTreeSet::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path.display().to_string());
            }
        }
    }
    files
}

pub(super) async fn table_of(lane: &Lane) -> iceberg::table::Table {
    lane.catalogs["ice"]
        .load_table(&TableIdent::from_strs(["sales", "t"]).unwrap())
        .await
        .unwrap()
}

pub(super) async fn observe(lane: &Lane) -> Observed {
    let table = table_of(lane).await;
    let location = local_path(table.metadata().location());
    let files = files_under(Path::new(&location));
    let reached = reachable(&table).await;
    let orphans = files
        .iter()
        .filter(|path| !reached.contains_key(*path))
        .cloned()
        .collect();
    Observed {
        current: table.metadata().current_snapshot_id(),
        snapshots: table
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect(),
        batch: sorted_by_id(&batches_of(lane, "SELECT * FROM ice.sales.t").await),
        files,
        orphans,
    }
}

pub(super) fn assert_refusal_text(error: &DataFusionError, kind: &str, key: &str, label: &str) {
    let text = error.to_string();
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{label}: the refusal is a ResourcesExhausted error, got {error:?}"
    );
    assert!(
        text.contains(&format!("REPARK: the {kind} holds ")),
        "{label}: the text names the {kind}: {text}"
    );
    assert!(
        text.contains(&format!(
            "Raise `{key}` (0 = no bound) or raise the memory pool."
        )),
        "{label}: the text names the key {key}: {text}"
    );
    assert!(
        text.contains(" x pool ") && text.contains(" bytes"),
        "{label}: the text names the fraction times the pool: {text}"
    );
    let lowered = text.to_lowercase();
    for word in DENIED {
        assert!(
            !lowered.contains(word),
            "{label}: the text names no data-deleting action ({word}): {text}"
        );
    }
    println!("{label} refusal: {text}");
}

pub(super) fn assert_unchanged(before: &Observed, after: &Observed, label: &str) {
    assert_eq!(after.current, before.current, "{label}: current snapshot");
    assert_eq!(after.snapshots, before.snapshots, "{label}: snapshot set");
    assert_eq!(after.batch, before.batch, "{label}: rows and types");
    assert_eq!(after.files, before.files, "{label}: zero staged files");
    assert!(
        after.orphans.is_empty(),
        "{label}: zero orphans: {:?}",
        after.orphans
    );
}

pub(super) async fn assert_refused(lane: &Lane, sql: &str, kind: &str, key: &str, label: &str) {
    let before = observe(lane).await;
    let error = attempt(lane, sql)
        .await
        .expect_err(&format!("{label}: the statement refuses above the bound"));
    assert_refusal_text(&error, kind, key, label);
    assert_unchanged(&before, &observe(lane).await, label);
}

async fn assert_file_order(lane: &Lane, label: &str) {
    let paths = batches_of(
        lane,
        "SELECT file_path FROM ice.sales.t.files WHERE content = 0",
    )
    .await;
    let mut checked = 0;
    for batch in &paths {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..column.len() {
            let path = local_path(column.value(index));
            let keys = lane
                .ctx
                .read_parquet(path.as_str(), ParquetReadOptions::default())
                .await
                .unwrap()
                .select_columns(&["k"])
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut previous = i64::MIN;
            for keys in &keys {
                let values = keys
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for value in values.iter().flatten() {
                    assert!(value >= previous, "{label}: {path} is ordered by k");
                    previous = value;
                }
            }
            checked += 1;
        }
    }
    assert!(checked > 0, "{label}: at least one data file");
}

async fn assert_committed(lane: &Lane, sql: &str, expected: &str, ordered: bool, label: &str) {
    let before = observe(lane).await;
    attempt(lane, sql)
        .await
        .unwrap_or_else(|error| panic!("{label}: the statement commits below the bound: {error}"));
    let after = observe(lane).await;
    assert_ne!(after.current, before.current, "{label}: a new snapshot");
    let expected = sorted_by_id(&batches_of(lane, expected).await);
    let types = |batch: &RecordBatch| -> Vec<DataType> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    };
    assert_eq!(
        types(&after.batch),
        types(&expected),
        "{label}: Arrow types"
    );
    assert_eq!(after.batch.columns(), expected.columns(), "{label}: values");
    assert!(
        after.orphans.is_empty(),
        "{label}: zero orphans: {:?}",
        after.orphans
    );
    if ordered {
        assert_file_order(lane, label).await;
    }
}

async fn ordered_lane(
    format: u8,
    shape: Shape,
    rows: usize,
    pool_mib: usize,
    keys: &[(&str, &str)],
) -> Lane {
    let lane = lane(pool_mib, keys, shape.writers).await;
    register_rows(&lane.ctx, rows);
    create_target(&lane, format, shape.partitioned, false).await;
    order_target(&lane).await;
    lane
}

fn shape_label(cell: &str, format: u8, shape: Shape) -> String {
    format!(
        "{cell} v{format} partitioned={} writers={}",
        shape.partitioned, shape.writers
    )
}

async fn ordered_refuses(cell: &str, format: u8, rows: usize, pool_mib: usize, shapes: &[Shape]) {
    for shape in shapes {
        let lane = ordered_lane(format, *shape, rows, pool_mib, &[]).await;
        let label = shape_label(cell, format, *shape);
        assert_refused(&lane, D4, ORDERED_KIND, ORDERED_KEY, &label).await;
    }
}

async fn ordered_commits(
    cell: &str,
    format: u8,
    rows: usize,
    pool_mib: usize,
    keys: &[(&str, &str)],
    shapes: &[Shape],
) {
    for shape in shapes {
        let lane = ordered_lane(format, *shape, rows, pool_mib, keys).await;
        let label = shape_label(cell, format, *shape);
        assert_committed(&lane, D4, SOURCE, true, &label).await;
    }
}

async fn pp01_r1(format: u8) {
    ordered_refuses("PP-01-R1", format, 100_000, 32, &SHAPES).await;
}

async fn pp01_c1(format: u8) {
    ordered_commits("PP-01-C1", format, 100_000, 128, &[], &SHAPES).await;
}

async fn pp01_r2(format: u8) {
    ordered_refuses("PP-01-R2", format, 1_000_000, 128, &[SHAPES[0], SHAPES[3]]).await;
}

async fn pp01_c2(format: u8) {
    ordered_commits(
        "PP-01-C2",
        format,
        1_000_000,
        2048,
        &[],
        &[SHAPES[0], SHAPES[3]],
    )
    .await;
}

async fn pp01_k0(format: u8) {
    ordered_commits(
        "PP-01-K0",
        format,
        100_000,
        32,
        &[(ORDERED_KEY, "0")],
        &[SHAPES[0], SHAPES[3]],
    )
    .await;
}

async fn door_lane(format: u8, door: &str, pool_mib: usize, keys: &[(&str, &str)]) -> Lane {
    let lane = lane(pool_mib, keys, 1).await;
    register_rows(&lane.ctx, 100_000);
    create_target(&lane, format, false, false).await;
    if matches!(door, "D8" | "D10") {
        seed(&lane, 100_000, 1).await;
    }
    order_target(&lane).await;
    if door == "D2" {
        set_session_conf(&lane.ctx, SESSION_KEY, "v");
    }
    lane
}

const DOORS: [(&str, &str, &str); 4] = [
    ("D2", D2, SOURCE),
    ("D3", D3, SOURCE),
    ("D5", D5, SOURCE),
    ("D10", D10, PLUS_ONE),
];

async fn pp01_doors(format: u8) {
    for (door, sql, expected) in DOORS {
        let label = format!("PP-01-D {door} v{format}");
        let refused = door_lane(format, door, 32, &[]).await;
        assert_refused(&refused, sql, ORDERED_KIND, ORDERED_KEY, &label).await;
        let committed = door_lane(format, door, 128, &[]).await;
        assert_committed(&committed, sql, expected, true, &label).await;
    }
}

async fn pp01_m(format: u8) {
    for partitioned in [false, true] {
        let label = format!("PP-01-M D8 v{format} partitioned={partitioned}");
        let make = |keys: &'static [(&'static str, &'static str)]| async move {
            let lane = lane(128, keys, 1).await;
            register_rows(&lane.ctx, 100_000);
            create_target(&lane, format, partitioned, false).await;
            seed(&lane, 100_000, 1).await;
            order_target(&lane).await;
            lane
        };
        let refused = make(&[(ORDERED_KEY, "0.01")]).await;
        assert_refused(&refused, D8, ORDERED_KIND, ORDERED_KEY, &label).await;
        let committed = make(&[]).await;
        assert_committed(&committed, D8, PLUS_ONE, true, &label).await;
    }
}

async fn pp01_u(format: u8) {
    for shape in [SHAPES[0], SHAPES[3]] {
        let lane = lane(32, &[], shape.writers).await;
        register_rows(&lane.ctx, 100_000);
        create_target(&lane, format, shape.partitioned, false).await;
        let label = shape_label("PP-01-U", format, shape);
        assert_committed(&lane, D4, SOURCE, false, &label).await;
    }
}

pub(super) async fn merge_lane(
    format: u8,
    rows: usize,
    match_percent: u32,
    pool_mib: usize,
    keys: &[(&str, &str)],
    writers: usize,
    ordered: bool,
) -> Lane {
    let lane = lane(pool_mib, keys, writers).await;
    register_rows(&lane.ctx, rows);
    create_target(&lane, format, false, true).await;
    seed(&lane, rows, 32).await;
    if ordered {
        order_target(&lane).await;
    }
    register_matched(&lane.ctx, match_percent).await;
    lane
}

pub(super) async fn register_matched(ctx: &SessionContext, match_percent: u32) {
    let matched = ctx
        .sql(&format!(
            "SELECT id, a + 1.0 AS a FROM bsrc WHERE (id * 7) % 100 < {match_percent}"
        ))
        .await
        .unwrap();
    ctx.register_table("msrc", matched.into_view()).unwrap();
}

fn merge_with_source_leg() -> String {
    format!("{MOR_MERGE}{NOT_MATCHED_BY_SOURCE}")
}

async fn pp02_r1(format: u8) {
    for writers in [1, 4] {
        let label = format!("PP-02-R1 v{format} writers={writers}");
        let lane = merge_lane(
            format,
            200_000,
            100,
            512,
            &[(MERGE_KEY, "0.01")],
            writers,
            false,
        )
        .await;
        assert_refused(&lane, MOR_MERGE, MERGE_KIND, MERGE_KEY, &label).await;
    }
}

async fn pp02_r2(format: u8) {
    let label = format!("PP-02-R2 v{format}");
    let lane = merge_lane(format, 200_000, 10, 512, &[(MERGE_KEY, "0.01")], 1, false).await;
    assert_refused(
        &lane,
        &merge_with_source_leg(),
        MERGE_KIND,
        MERGE_KEY,
        &label,
    )
    .await;
    let matched_only = format!("PP-02-R2 matched leg alone v{format}");
    assert_committed(
        &lane,
        MOR_MERGE,
        "SELECT id, k, p, CASE WHEN (id * 7) % 100 < 10 THEN a + 1.0 ELSE a END AS a, \
         b, s, c, flag FROM bsrc",
        false,
        &matched_only,
    )
    .await;
}

async fn pp02_c1(format: u8) {
    for writers in [1, 4] {
        let label = format!("PP-02-C1 v{format} writers={writers}");
        let lane = merge_lane(format, 200_000, 100, 512, &[], writers, false).await;
        assert_committed(&lane, MOR_MERGE, PLUS_ONE, false, &label).await;
    }
}

async fn pp02_c2(format: u8) {
    let label = format!("PP-02-C2 v{format}");
    let lane = merge_lane(format, 200_000, 10, 2048, &[(MERGE_KEY, "0.01")], 1, false).await;
    assert_committed(&lane, &merge_with_source_leg(), MATCHED_TEN, false, &label).await;
}

async fn pp02_o8(format: u8) {
    let label = format!("PP-02-O8 ordered target v{format}");
    let keys = [(MERGE_KEY, "0.01"), (ORDERED_KEY, "0.01")];
    let lane = merge_lane(format, 200_000, 100, 512, &keys, 1, true).await;
    assert_refused(&lane, MOR_MERGE, MERGE_KIND, MERGE_KEY, &label).await;
}

async fn pp02_p(format: u8) {
    let label = format!("PP-02-P v{format}");
    let lane = merge_lane(format, 200_000, 100, 32, &[], 1, false).await;
    let before = observe(&lane).await;
    let error = attempt(&lane, MOR_MERGE)
        .await
        .expect_err("PP-02-P: the 32 MiB pool refuses the statement");
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{label}: the refusal is a ResourcesExhausted error, got {error:?}"
    );
    println!("{label} refusal: {error}");
    assert_unchanged(&before, &observe(&lane).await, &label);
}

async fn changelog_lane(format: u8, pool_mib: usize) -> Lane {
    let lane = lane(pool_mib, &[], 1).await;
    register_rows(&lane.ctx, 100_000);
    create_target(&lane, format, false, false).await;
    seed(&lane, 100_000, 10).await;
    run(
        &lane.ctx,
        &lane.catalogs,
        "CALL ice.system.create_changelog_view(table => 'sales.t')",
    )
    .await;
    lane
}

const CHANGELOG_QUERIES: [&str; 2] = [
    "SELECT id FROM t_changes LIMIT 10",
    "SELECT count(id) FROM t_changes",
];

async fn pa11_r1(format: u8) {
    let lane = changelog_lane(format, 32).await;
    for query in CHANGELOG_QUERIES {
        let label = format!("PA-11-R1 v{format} {query}");
        let before = observe(&lane).await;
        let error = attempt(&lane, query)
            .await
            .expect_err(&format!("{label}: the read refuses above the bound"));
        assert_refusal_text(&error, CHANGELOG_KIND, CHANGELOG_KEY, &label);
        assert_unchanged(&before, &observe(&lane).await, &label);
    }
}

async fn pa11_c1(format: u8) {
    let lane = changelog_lane(format, 2048).await;
    let limited = batches_of(&lane, CHANGELOG_QUERIES[0]).await;
    assert_eq!(
        limited.iter().map(RecordBatch::num_rows).sum::<usize>(),
        10,
        "PA-11-C1 v{format}: LIMIT 10 answers ten rows"
    );
    let counted = batches_of(&lane, CHANGELOG_QUERIES[1]).await;
    let count = counted[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 100_000, "PA-11-C1 v{format}: every inserted row");
}

macro_rules! on_both_formats {
    ($($name:ident),* $(,)?) => {
        $(
            mod $name {
                #[tokio::test(flavor = "multi_thread")]
                async fn v2() {
                    super::$name(2).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                async fn v3() {
                    super::$name(3).await;
                }
            }
        )*
    };
}

on_both_formats!(
    pp01_r1, pp01_c1, pp01_r2, pp01_c2, pp01_k0, pp01_doors, pp01_m, pp01_u, pp02_r1, pp02_r2,
    pp02_c1, pp02_c2, pp02_o8, pp02_p, pa11_r1, pa11_c1,
);
