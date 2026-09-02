use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use super::super::*;
use super::common::*;

use iceberg::spec::ManifestContentType;

const MOR_V3: &str = "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'";

async fn seed_one_file_per_id(ctx: &SessionContext, catalogs: &CatalogRegistry, files: i32) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.wide (id INT, tag STRING) USING iceberg \
             TBLPROPERTIES ({MOR_V3})"
        ),
    )
    .await;
    for id in 1..=files {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.wide VALUES ({id}, 'r')"),
        )
        .await;
    }
}

async fn seed_key_source(ctx: &SessionContext, catalogs: &CatalogRegistry, key: i32) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.keys (id INT) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.keys VALUES ({key})"),
    )
    .await;
}

async fn live_data_files(catalogs: &CatalogRegistry, table: &str) -> Vec<(String, String)> {
    let ident = TableIdent::from_strs(["sales", table]).expect("ident");
    let loaded = catalogs["ice"].load_table(&ident).await.expect("load");
    let metadata = loaded.metadata();
    let snapshot = metadata.current_snapshot().expect("snapshot");
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .expect("manifest list");
    let mut paths = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(loaded.file_io())
            .await
            .expect("data manifest");
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            let bound = file
                .lower_bounds()
                .get(&1)
                .map(std::string::ToString::to_string)
                .expect("the id column carries a lower bound");
            paths.push((file.file_path().to_string(), bound));
        }
    }
    paths.sort();
    paths
}

struct HiddenFiles {
    moved: Vec<(PathBuf, PathBuf)>,
}

impl HiddenFiles {
    fn hide(paths: &[String]) -> Self {
        let mut moved = Vec::new();
        for path in paths {
            let from = PathBuf::from(path);
            let to = from.with_extension("parquet.hidden");
            fs::rename(&from, &to).expect("hide data file");
            moved.push((from, to));
        }
        Self { moved }
    }
}

impl Drop for HiddenFiles {
    fn drop(&mut self) {
        for (from, to) in &self.moved {
            let _ = fs::rename(to, from);
        }
    }
}

async fn live_ids(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<i32> {
    let batches = execute(ctx, catalogs, "SELECT id FROM ice.sales.wide ORDER BY id")
        .await
        .expect("select ids")
        .collect()
        .await
        .expect("collect ids");
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("id is Int32");
        for row in 0..batch.num_rows() {
            ids.push(column.value(row));
        }
    }
    ids
}

#[tokio::test]
async fn subquery_delete_opens_only_the_files_the_key_bounds_admit() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_key_source(&ctx, &catalogs, 5).await;
    seed_one_file_per_id(&ctx, &catalogs, 8).await;
    let files = live_data_files(&catalogs, "wide").await;
    assert_eq!(files.len(), 8, "one data file per single-row INSERT");
    let unreachable_files: Vec<String> = files
        .iter()
        .filter(|(_, bound)| bound != "5")
        .map(|(path, _)| path.clone())
        .collect();
    assert_eq!(unreachable_files.len(), 7);
    let hidden = HiddenFiles::hide(&unreachable_files);
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.wide WHERE id IN (SELECT id FROM ice.sales.keys)",
    )
    .await;
    drop(hidden);
    assert_eq!(live_ids(&ctx, &catalogs).await, vec![1, 2, 3, 4, 6, 7, 8]);
}

const WIDE_ROWS: i32 = 200;

async fn seed_wide_files(ctx: &SessionContext, catalogs: &CatalogRegistry, files: i32) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.wide (id INT, tag STRING) USING iceberg \
             TBLPROPERTIES ({MOR_V3})"
        ),
    )
    .await;
    for key in 1..=files {
        let values: Vec<String> = (0..WIDE_ROWS)
            .map(|row| format!("({key}, 'padpadpadpadpadpadpadpadpadpad{row}')"))
            .collect();
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.wide VALUES {}", values.join(", ")),
        )
        .await;
    }
}

async fn measure_subquery_delete(files: i32) -> u128 {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_key_source(&ctx, &catalogs, files / 2).await;
    seed_wide_files(&ctx, &catalogs, files).await;
    let started = Instant::now();
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.wide WHERE id IN (SELECT id FROM ice.sales.keys)",
    )
    .await;
    started.elapsed().as_millis()
}

async fn measure_partitioned_fresh_delete(files: i32) -> u128 {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_key_source(&ctx, &catalogs, files / 2).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.wide (id INT, tag STRING) USING iceberg \
             PARTITIONED BY (id) TBLPROPERTIES ({MOR_V3})"
        ),
    )
    .await;
    for id in 1..=files {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.wide VALUES ({id}, 'r')"),
        )
        .await;
    }
    let started = Instant::now();
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.wide WHERE id IN (SELECT id FROM ice.sales.keys)",
    )
    .await;
    started.elapsed().as_millis()
}

#[tokio::test]
#[ignore = "measurement: 64 and 192 sequential single-row appends"]
async fn measure_v3_mor_subquery_delete_statement_wall() {
    for files in [64, 192] {
        let flat = measure_subquery_delete(files).await;
        let partitioned = measure_partitioned_fresh_delete(files).await;
        println!("files={files} flat_ms={flat} partitioned_fresh_ms={partitioned}");
    }
}
