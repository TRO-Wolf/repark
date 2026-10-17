use datafusion::prelude::ParquetReadOptions;
use iceberg::spec::DataFile;

use super::super::*;
use super::common::*;
use super::enc_1_fold::{VERSIONS, add_key, refusal_faults};

#[derive(Clone, Copy, Debug)]
enum Door {
    Append,
    AppendTo,
    AppendWithSummary,
    ReplaceWriteWithSummary,
    ReplaceAllWithSummary,
    RowFilterWithSummary,
    ReplacePartitionsWithSummary,
    ReplaceAllTo,
    ReplaceWrite,
    RowFilterTo,
    ReplacePartitionsTo,
    OverwriteByFilter,
}

struct Fixture {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    _warehouse: TempDir,
    stale: iceberg::table::Table,
    historical: DataFile,
}

fn local(path: &str) -> String {
    path.strip_prefix("file://").unwrap_or(path).to_string()
}

async fn snapshot_data_files(
    table: &iceberg::table::Table,
    snapshot: &iceberg::spec::Snapshot,
) -> Vec<DataFile> {
    let list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("manifest list loads");
    let mut files = Vec::new();
    for manifest_file in list.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("manifest loads");
        files.extend(
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
                .map(|entry| entry.data_file().clone()),
        );
    }
    files
}

async fn fixture(version: &str) -> Fixture {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg TBLPROPERTIES \
             ('format-version' = '{version}')"
        ),
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (1, 'a')").await;
    let first = load_sales_table(&catalogs, "t").await;
    let first_snapshot = first.metadata().current_snapshot().expect("first snapshot");
    let mut historical = snapshot_data_files(&first, first_snapshot).await;
    assert_eq!(historical.len(), 1, "the first insert wrote one file");
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (2, 'b')",
    )
    .await;
    let stale = load_sales_table(&catalogs, "t").await;
    let current = stale
        .metadata()
        .current_snapshot()
        .expect("current snapshot");
    let historical = historical.remove(0);
    assert!(
        snapshot_data_files(&stale, current)
            .await
            .iter()
            .all(|file| file.file_path() != historical.file_path()),
        "the overwrite removed the first file from the current snapshot"
    );
    add_key(&ctx, &catalogs, "t").await;
    Fixture {
        ctx,
        catalogs,
        _warehouse: warehouse,
        stale,
        historical,
    }
}

async fn stage_fresh(fixture: &Fixture) -> Vec<DataFile> {
    let batches = fixture
        .ctx
        .sql("SELECT 9 AS id, 'z' AS name")
        .await
        .expect("staging source frames")
        .collect()
        .await
        .expect("staging source collects");
    let staged = repark_iceberg::write::write_data_files(&fixture.stale, batches)
        .await
        .expect("staging writes");
    assert!(!staged.is_empty(), "staging wrote no file");
    staged
}

async fn commit(
    door: Door,
    catalog: &std::sync::Arc<dyn iceberg::Catalog>,
    table: &iceberg::table::Table,
    files: Vec<DataFile>,
) -> datafusion::error::Result<iceberg::table::Table> {
    use repark_iceberg::write as w;
    let always = iceberg::expr::Predicate::AlwaysTrue;
    match door {
        Door::Append => w::commit_append(catalog, table, files).await,
        Door::AppendTo => w::commit_append_to(catalog, table, files, None).await,
        Door::AppendWithSummary => {
            w::commit_append_with_summary(catalog, table, files, &[], None).await
        }
        Door::ReplaceWriteWithSummary => {
            w::commit_replace_write_with_summary(catalog, table, files, &[]).await
        }
        Door::ReplaceAllWithSummary => {
            w::commit_overwrite_replace_all_with_summary(catalog, table, files, None, &[], None)
                .await
        }
        Door::RowFilterWithSummary => {
            let overwrite = w::StaticPartitionOverwrite {
                predicate: always,
                equalities: Vec::new(),
            };
            w::commit_overwrite_by_row_filter_with_summary(
                catalog,
                table,
                files,
                overwrite,
                None,
                &[],
                None,
            )
            .await
        }
        Door::ReplacePartitionsWithSummary => {
            w::commit_replace_partitions_with_summary(catalog, table, files, None, &[], None).await
        }
        Door::ReplaceAllTo => w::commit_overwrite_replace_all_to(catalog, table, files, None).await,
        Door::ReplaceWrite => w::commit_replace_write(catalog, table, files).await,
        Door::RowFilterTo => {
            w::commit_overwrite_by_row_filter_to(catalog, table, files, always, None).await
        }
        Door::ReplacePartitionsTo => {
            w::commit_replace_partitions_to(catalog, table, files, None).await
        }
        Door::OverwriteByFilter => {
            w::commit_overwrite_by_filter_with_summary(
                catalog,
                table,
                files,
                always,
                None,
                &[],
                w::FilterValidation::default(),
            )
            .await
        }
    }
}

async fn parquet_rows(path: &str) -> datafusion::error::Result<usize> {
    SessionContext::new()
        .read_parquet(path, ParquetReadOptions::default())
        .await?
        .count()
        .await
}

async fn door_faults(version: &str, door: Door) -> Vec<String> {
    let fixture = fixture(version).await;
    let catalog = fixture
        .catalogs
        .get("ice")
        .expect("ice catalog is registered");
    let fresh = stage_fresh(&fixture).await;
    let fresh_paths: Vec<String> = fresh.iter().map(|file| local(file.file_path())).collect();
    let referenced = local(fixture.historical.file_path());
    let mut files = vec![fixture.historical.clone()];
    files.extend(fresh);
    let result = commit(door, catalog, &fixture.stale, files).await;
    let mut faults = Vec::new();
    match result {
        Ok(_) => faults.push("the keyed write stored".to_string()),
        Err(error) => faults.extend(refusal_faults(error, "sales.t")),
    }
    match parquet_rows(&referenced).await {
        Ok(1) => {}
        Ok(rows) => faults.push(format!("referenced {referenced} reads {rows} rows")),
        Err(error) => faults.push(format!("referenced {referenced} is unreadable: {error}")),
    }
    for path in fresh_paths {
        if std::path::Path::new(&path).exists() {
            faults.push(format!("fresh {path} was left behind"));
        }
    }
    let read = execute(
        &fixture.ctx,
        &fixture.catalogs,
        "SELECT id FROM ice.sales.t",
    )
    .await;
    match read {
        Ok(frame) => match frame.count().await {
            Ok(1) => {}
            Ok(rows) => faults.push(format!("the table reads {rows} rows")),
            Err(error) => faults.push(format!("the table is unreadable: {error}")),
        },
        Err(error) => faults.push(format!("the table does not plan: {error}")),
    }
    faults
        .into_iter()
        .map(|fault| format!("v{version} {door:?}: {fault}"))
        .collect()
}

async fn assert_referenced_survives(doors: &[Door]) {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for door in doors {
            faults.extend(door_faults(version, *door).await);
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_refused_append_keeps_a_snapshot_referenced_file() {
    assert_referenced_survives(&[Door::Append]).await;
}

#[tokio::test]
async fn keyed_refused_branch_append_keeps_a_snapshot_referenced_file() {
    assert_referenced_survives(&[Door::AppendTo]).await;
}

#[tokio::test]
async fn keyed_refused_write_options_doors_keep_a_snapshot_referenced_file() {
    assert_referenced_survives(&[
        Door::AppendWithSummary,
        Door::ReplaceWriteWithSummary,
        Door::ReplaceAllWithSummary,
        Door::RowFilterWithSummary,
        Door::ReplacePartitionsWithSummary,
    ])
    .await;
}

#[tokio::test]
async fn keyed_refused_overwrite_keeps_a_snapshot_referenced_file() {
    assert_referenced_survives(&[Door::ReplaceAllTo, Door::ReplaceWrite]).await;
}

#[tokio::test]
async fn keyed_refused_partition_overwrite_keeps_a_snapshot_referenced_file() {
    assert_referenced_survives(&[Door::RowFilterTo, Door::ReplacePartitionsTo]).await;
}

#[tokio::test]
async fn keyed_refused_overwrite_by_filter_keeps_a_snapshot_referenced_file() {
    assert_referenced_survives(&[Door::OverwriteByFilter]).await;
}
