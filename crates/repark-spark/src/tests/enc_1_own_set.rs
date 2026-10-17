use datafusion::prelude::ParquetReadOptions;
use iceberg::spec::DataFile;

use super::super::*;
use super::common::*;
use super::enc_1_fold::{VERSIONS, add_key, freeze, refusal_faults};

pub(super) const FILE_DOORS: usize = 12;

pub(super) async fn file_door(
    index: usize,
    catalog: &Arc<dyn Catalog>,
    table: &iceberg::table::Table,
    files: Vec<DataFile>,
) -> (
    &'static str,
    datafusion::error::Result<iceberg::table::Table>,
) {
    use repark_iceberg::write as w;
    let always = iceberg::expr::Predicate::AlwaysTrue;
    match index {
        0 => (
            "commit_append",
            w::commit_append(catalog, table, files).await,
        ),
        1 => (
            "commit_append_to",
            w::commit_append_to(catalog, table, files, None).await,
        ),
        2 => (
            "commit_append_with_summary",
            w::commit_append_with_summary(catalog, table, files, &[], None).await,
        ),
        3 => (
            "commit_replace_write_with_summary",
            w::commit_replace_write_with_summary(catalog, table, files, &[]).await,
        ),
        4 => (
            "commit_overwrite_replace_all_with_summary",
            w::commit_overwrite_replace_all_with_summary(catalog, table, files, None, &[], None)
                .await,
        ),
        5 => (
            "commit_overwrite_by_row_filter_with_summary",
            w::commit_overwrite_by_row_filter_with_summary(
                catalog,
                table,
                files,
                w::StaticPartitionOverwrite {
                    predicate: always,
                    equalities: Vec::new(),
                },
                None,
                &[],
                None,
            )
            .await,
        ),
        6 => (
            "commit_replace_partitions_with_summary",
            w::commit_replace_partitions_with_summary(catalog, table, files, None, &[], None).await,
        ),
        7 => (
            "commit_overwrite_replace_all_to",
            w::commit_overwrite_replace_all_to(catalog, table, files, None).await,
        ),
        8 => (
            "commit_replace_write",
            w::commit_replace_write(catalog, table, files).await,
        ),
        9 => (
            "commit_overwrite_by_row_filter_to",
            w::commit_overwrite_by_row_filter_to(catalog, table, files, always, None).await,
        ),
        10 => (
            "commit_replace_partitions_to",
            w::commit_replace_partitions_to(catalog, table, files, None).await,
        ),
        _ => (
            "commit_overwrite_by_filter_with_summary",
            w::commit_overwrite_by_filter_with_summary(
                catalog,
                table,
                files,
                always,
                None,
                &[],
                w::FilterValidation::default(),
            )
            .await,
        ),
    }
}

pub(super) async fn partitioned_table(version: &str) -> (SessionContext, CatalogRegistry, TempDir) {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.t (id INT, name STRING, cat STRING) USING iceberg \
             PARTITIONED BY (cat) TBLPROPERTIES ('format-version' = '{version}')"
        ),
    )
    .await;
    (ctx, catalogs, warehouse)
}

pub(super) async fn alive_files(
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

pub(super) async fn stage_own(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
) -> Vec<DataFile> {
    let batches = ctx
        .sql(
            "SELECT * FROM (VALUES (7, 'p', 'x'), (8, 'q', 'y'), (9, 'r', 'z'), (10, 's', 'w')) \
             AS v(id, name, cat)",
        )
        .await
        .expect("staging source frames")
        .collect()
        .await
        .expect("staging source collects");
    let staged = repark_iceberg::write::write_partitioned_data_files(table, batches)
        .await
        .expect("staging writes");
    assert!(staged.len() >= 4, "staging wrote {} files", staged.len());
    staged
}

pub(super) fn local(path: &str) -> String {
    path.strip_prefix("file://").unwrap_or(path).to_string()
}

pub(super) fn respelled(file: &DataFile, path: String) -> DataFile {
    iceberg::spec::DataFileBuilder::default()
        .content(file.content_type())
        .file_path(path)
        .file_format(file.file_format())
        .partition(file.partition().clone())
        .partition_spec_id(file.partition_spec_id())
        .record_count(file.record_count())
        .file_size_in_bytes(file.file_size_in_bytes())
        .build()
        .expect("respelled data file builds")
}

pub(super) fn url_spelling(file: &DataFile) -> DataFile {
    let path = file.file_path();
    let spelled = match path.strip_prefix("file://") {
        Some(rest) => rest.to_string(),
        None => format!("file://{path}"),
    };
    respelled(file, spelled)
}

async fn parquet_rows(path: &str) -> std::result::Result<usize, String> {
    match SessionContext::new()
        .read_parquet(path, ParquetReadOptions::default())
        .await
    {
        Ok(frame) => frame.count().await.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

pub(super) async fn answered_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<usize, String> {
    let frame = execute(ctx, catalogs, sql)
        .await
        .map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

fn is_keyed_refusal(result: &datafusion::error::Result<iceberg::table::Table>) -> bool {
    result
        .as_ref()
        .err()
        .is_some_and(|error| repark_iceberg::write::EncryptedTableRefusal::find(error).is_some())
}

fn refusal_outcome(
    label: &str,
    index: usize,
    result: datafusion::error::Result<iceberg::table::Table>,
    faults: &mut Vec<String>,
) -> bool {
    let keyed = is_keyed_refusal(&result);
    match result {
        Ok(_) => faults.push(format!("{label}: the keyed write stored")),
        Err(error) if keyed => faults.extend(
            refusal_faults(error, "sales.t")
                .into_iter()
                .map(|fault| format!("{label}: {fault}")),
        ),
        Err(error) if index < 3 && error.to_string().contains("already referenced") => {}
        Err(error) => faults.push(format!("{label}: not the keyed refusal: {error}")),
    }
    keyed
}

async fn foreign_survives(label: &str, files: &[DataFile], faults: &mut Vec<String>) {
    for file in files {
        let path = local(file.file_path());
        match parquet_rows(&path).await {
            Ok(rows) if rows as u64 == file.record_count() => {}
            Ok(rows) => faults.push(format!("{label}: {path} reads {rows} rows")),
            Err(error) => faults.push(format!("{label}: {path} is gone: {error}")),
        }
    }
}

fn own_swept(label: &str, keyed: bool, own: &[String], faults: &mut Vec<String>) {
    if !keyed {
        return;
    }
    for path in own {
        if std::path::Path::new(path).exists() {
            faults.push(format!("{label}: own staged {path} was left behind"));
        }
    }
}

async fn reads(
    label: &str,
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    expected: usize,
    faults: &mut Vec<String>,
) {
    match answered_rows(ctx, catalogs, sql).await {
        Ok(rows) if rows == expected => {}
        Ok(rows) => faults.push(format!("{label}: `{sql}` answers {rows} rows")),
        Err(error) => faults.push(format!("{label}: `{sql}` fails: {error}")),
    }
}

async fn stale_view_with_a_concurrent_commit(version: &str, index: usize) -> Vec<String> {
    let (ctx, catalogs, _warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let older = load_sales_table(&catalogs, "t").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (2, 'b', 'y')",
    )
    .await;
    let newer = load_sales_table(&catalogs, "t").await;
    let seen = alive_files(&older, older.metadata().current_snapshot().expect("s1")).await;
    let current: Vec<DataFile> =
        alive_files(&newer, newer.metadata().current_snapshot().expect("s2"))
            .await
            .into_iter()
            .filter(|file| seen.iter().all(|old| old.file_path() != file.file_path()))
            .collect();
    assert_eq!(current.len(), 1, "the concurrent commit added one file");
    add_key(&ctx, &catalogs, "t").await;
    let catalog = catalogs.get("ice").expect("ice catalog").clone();
    let own = stage_own(&ctx, &older).await;
    let own_paths: Vec<String> = own.iter().map(|file| local(file.file_path())).collect();
    let mut handed = current.clone();
    handed.extend(own);
    let (door, result) = file_door(index, &catalog, &older, handed).await;
    let label = format!("v{version} {door}");
    let mut faults = Vec::new();
    let keyed = refusal_outcome(&label, index, result, &mut faults);
    foreign_survives(&label, &current, &mut faults).await;
    own_swept(&label, keyed, &own_paths, &mut faults);
    reads(
        &label,
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.t",
        2,
        &mut faults,
    )
    .await;
    faults
}

async fn url_spelling_of_a_referenced_file(version: &str, index: usize) -> Vec<String> {
    let (ctx, catalogs, _warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let first = load_sales_table(&catalogs, "t").await;
    let s1 = first.metadata().current_snapshot().expect("s1").clone();
    let historical = alive_files(&first, &s1).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (3, 'c', 'x')",
    )
    .await;
    let stale = load_sales_table(&catalogs, "t").await;
    add_key(&ctx, &catalogs, "t").await;
    let catalog = catalogs.get("ice").expect("ice catalog").clone();
    let own = stage_own(&ctx, &stale).await;
    let own_paths: Vec<String> = own.iter().map(|file| local(file.file_path())).collect();
    let mut handed: Vec<DataFile> = historical.iter().map(url_spelling).collect();
    handed.extend(own);
    let (door, result) = file_door(index, &catalog, &stale, handed).await;
    let label = format!("v{version} {door}");
    let mut faults = Vec::new();
    let keyed = refusal_outcome(&label, index, result, &mut faults);
    foreign_survives(&label, &historical, &mut faults).await;
    own_swept(&label, keyed, &own_paths, &mut faults);
    let travel = format!(
        "SELECT * FROM ice.sales.t VERSION AS OF {}",
        s1.snapshot_id()
    );
    reads(&label, &ctx, &catalogs, &travel, 1, &mut faults).await;
    reads(
        &label,
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.t",
        1,
        &mut faults,
    )
    .await;
    faults
}

fn added_since(files: Vec<DataFile>, before: &[DataFile]) -> Vec<DataFile> {
    files
        .into_iter()
        .filter(|file| before.iter().all(|old| old.file_path() != file.file_path()))
        .collect()
}

async fn branch_only_file(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<DataFile> {
    run(ctx, catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b").await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t.branch_b SELECT 5 AS id, 'e' AS name, 'q' AS cat",
    )
    .await;
    let current = load_sales_table(catalogs, "t").await;
    let main = current.metadata().current_snapshot().expect("main");
    let main_files = alive_files(&current, main).await;
    let branch = current
        .metadata()
        .snapshot_for_ref("b")
        .expect("branch b")
        .clone();
    let branch_only = added_since(alive_files(&current, &branch).await, &main_files);
    assert_eq!(branch_only.len(), 1, "one file only branch b references");
    branch_only
}

async fn older_snapshot_and_branch_files(version: &str, index: usize) -> Vec<String> {
    let (ctx, catalogs, _warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t SET TBLPROPERTIES ('write.overwrite.isolation-level' = 'none')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let older = load_sales_table(&catalogs, "t").await;
    let s1_files = alive_files(&older, older.metadata().current_snapshot().expect("s1")).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (2, 'b', 'y')",
    )
    .await;
    let second = load_sales_table(&catalogs, "t").await;
    let s2 = second.metadata().current_snapshot().expect("s2").clone();
    let historical = added_since(alive_files(&second, &s2).await, &s1_files);
    assert_eq!(historical.len(), 1, "the second insert added one file");
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (3, 'c', 'x')",
    )
    .await;
    let branch_only = branch_only_file(&ctx, &catalogs).await;
    add_key(&ctx, &catalogs, "t").await;
    let catalog = catalogs.get("ice").expect("ice catalog").clone();
    let own = stage_own(&ctx, &older).await;
    let own_paths: Vec<String> = own.iter().map(|file| local(file.file_path())).collect();
    let mut foreign = historical.clone();
    foreign.extend(branch_only.iter().cloned());
    let mut handed = foreign.clone();
    handed.extend(own);
    let (door, result) = file_door(index, &catalog, &older, handed).await;
    let label = format!("v{version} {door}");
    let mut faults = Vec::new();
    let keyed = refusal_outcome(&label, index, result, &mut faults);
    foreign_survives(&label, &foreign, &mut faults).await;
    own_swept(&label, keyed, &own_paths, &mut faults);
    let travel = format!(
        "SELECT * FROM ice.sales.t VERSION AS OF {}",
        s2.snapshot_id()
    );
    reads(&label, &ctx, &catalogs, &travel, 2, &mut faults).await;
    reads(
        &label,
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.t.branch_b",
        2,
        &mut faults,
    )
    .await;
    reads(
        &label,
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.t",
        1,
        &mut faults,
    )
    .await;
    faults
}

async fn staged_create_file(version: &str, index: usize) -> Vec<String> {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (cat) TBLPROPERTIES \
             ('format-version' = '{version}') AS SELECT 1 AS id, 'a' AS name, 'x' AS cat"
        ),
    )
    .await;
    let stale = load_sales_table(&catalogs, "t").await;
    let created = stale.metadata().current_snapshot().expect("ctas snapshot");
    let published = alive_files(&stale, created).await;
    assert_eq!(published.len(), 1, "the create published one file");
    add_key(&ctx, &catalogs, "t").await;
    let catalog = catalogs.get("ice").expect("ice catalog").clone();
    let own = stage_own(&ctx, &stale).await;
    let own_paths: Vec<String> = own.iter().map(|file| local(file.file_path())).collect();
    let mut handed = published.clone();
    handed.extend(own);
    let (door, result) = file_door(index, &catalog, &stale, handed).await;
    let label = format!("v{version} {door}");
    let mut faults = Vec::new();
    let keyed = refusal_outcome(&label, index, result, &mut faults);
    foreign_survives(&label, &published, &mut faults).await;
    own_swept(&label, keyed, &own_paths, &mut faults);
    reads(
        &label,
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.t",
        1,
        &mut faults,
    )
    .await;
    faults
}

fn concurrent_copy(file: &DataFile) -> DataFile {
    let source = local(file.file_path());
    let source_path = std::path::Path::new(&source);
    let directory = source_path.parent().expect("a staged file has a directory");
    let stem = source_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .expect("a staged file has a name");
    let target = directory.join(format!("{stem}-concurrent.parquet"));
    std::fs::copy(&source, &target).expect("the concurrent writer stages its file");
    let target = target.to_str().expect("utf8 path").to_string();
    let spelled = if file.file_path().starts_with("file://") {
        format!("file://{target}")
    } else {
        target
    };
    respelled(file, spelled)
}

async fn concurrent_writer_files(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
) -> (Vec<DataFile>, Vec<DataFile>) {
    let theirs: Vec<DataFile> = stage_own(ctx, table)
        .await
        .iter()
        .map(concurrent_copy)
        .collect();
    let (handed, kept) = theirs.split_at(theirs.len() / 2);
    (handed.to_vec(), kept.to_vec())
}

async fn concurrent_writer_door(version: &str, index: usize) -> Vec<String> {
    let (ctx, catalogs, warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let stale = load_sales_table(&catalogs, "t").await;
    add_key(&ctx, &catalogs, "t").await;
    let catalog = catalogs.get("ice").expect("ice catalog").clone();
    let (handed_theirs, kept_theirs) = concurrent_writer_files(&ctx, &stale).await;
    let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
    let own = stage_own(&ctx, &stale).await;
    let mut handed = own;
    handed.extend(handed_theirs.iter().cloned());
    let (door, result) = file_door(index, &catalog, &stale, handed).await;
    let label = format!("v{version} {door}");
    let mut faults = Vec::new();
    let keyed = refusal_outcome(&label, index, result, &mut faults);
    if !keyed {
        faults.push(format!("{label}: the refusal is not the keyed refusal"));
    }
    foreign_survives(&label, &handed_theirs, &mut faults).await;
    foreign_survives(&label, &kept_theirs, &mut faults).await;
    let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
    if after.listing != before.listing {
        let gone: Vec<&String> = before.listing.difference(&after.listing).collect();
        let left: Vec<&String> = after.listing.difference(&before.listing).collect();
        faults.push(format!("{label}: deleted {gone:?}, left behind {left:?}"));
    }
    faults
}

async fn concurrent_writer_streaming_door(version: &str) -> Vec<String> {
    let (ctx, catalogs, warehouse) = partitioned_table(version).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    let stale = load_sales_table(&catalogs, "t").await;
    add_key(&ctx, &catalogs, "t").await;
    let catalog = catalogs.get("ice").expect("ice catalog").clone();
    let (first, second) = concurrent_writer_files(&ctx, &stale).await;
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
        &catalog,
        &stale,
        stream,
        &[],
        &repark_iceberg::write::WriterStagingOverrides::none(),
        repark_iceberg::write::WriteConcurrency::default(),
        None,
    )
    .await;
    let label = format!("v{version} append_staged_with_options");
    let mut faults = Vec::new();
    if !refusal_outcome(&label, FILE_DOORS, result, &mut faults) {
        faults.push(format!("{label}: the refusal is not the keyed refusal"));
    }
    foreign_survives(&label, &first, &mut faults).await;
    foreign_survives(&label, &second, &mut faults).await;
    let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
    if after.listing != before.listing {
        let gone: Vec<&String> = before.listing.difference(&after.listing).collect();
        let left: Vec<&String> = after.listing.difference(&before.listing).collect();
        faults.push(format!("{label}: deleted {gone:?}, left behind {left:?}"));
    }
    faults
}

#[tokio::test]
async fn a_stale_view_keeps_the_file_a_concurrent_commit_made_current() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for index in 0..FILE_DOORS {
            faults.extend(stale_view_with_a_concurrent_commit(version, index).await);
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn a_referenced_file_handed_as_a_file_url_survives() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for index in 0..FILE_DOORS {
            faults.extend(url_spelling_of_a_referenced_file(version, index).await);
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn files_only_an_older_snapshot_or_a_branch_references_survive() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for index in 0..FILE_DOORS {
            faults.extend(older_snapshot_and_branch_files(version, index).await);
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn a_concurrent_writers_staged_files_survive_our_refused_commit() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for index in 0..FILE_DOORS {
            faults.extend(concurrent_writer_door(version, index).await);
        }
        faults.extend(concurrent_writer_streaming_door(version).await);
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn a_file_a_staged_create_published_survives_a_refused_commit() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for index in 0..FILE_DOORS {
            faults.extend(staged_create_file(version, index).await);
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}
