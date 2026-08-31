//! pins: maint-rewrite-data-files-options/C-002, C-003, C-004, C-005, C-006, C-007, C-008
//! `rewrite_data_files` `where` / `strategy` / `sort_order` on v2 tables.

use std::collections::HashMap;
use std::path::PathBuf;

use super::super::*;
use super::call::call_count;
use super::common::*;
use iceberg::scan::FileScanTask;

async fn planned_data_files(catalog: &dyn Catalog, ident: &TableIdent) -> Vec<FileScanTask> {
    use futures::TryStreamExt;
    let table = catalog.load_table(ident).await.expect("load");
    let scan = table.scan().build().expect("scan");
    scan.plan_files()
        .await
        .expect("plan_files")
        .try_collect()
        .await
        .expect("collect tasks")
}

fn local_file_path(uri: &str) -> PathBuf {
    let stripped = uri
        .strip_prefix("file://")
        .or_else(|| uri.strip_prefix("file:"))
        .unwrap_or(uri);
    PathBuf::from(stripped)
}

fn file_bytes(uri: &str) -> Vec<u8> {
    std::fs::read(local_file_path(uri)).expect("read data file")
}

async fn seed_two_partition_groups(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, part INT) USING iceberg PARTITIONED BY (part)"
        ),
    )
    .await;
    for index in 1..=5 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({index}, 0)"),
        )
        .await;
    }
    for index in 101..=105 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({index}, 1)"),
        )
        .await;
    }
}

#[tokio::test]
async fn call_rewrite_where_keeps_out_of_scope_files_byte_identical() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_two_partition_groups(&ctx, &catalogs, "filt").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "filt".into());
    let before = planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(before.len(), 10, "fixture is 5 files per partition");
    let before_bytes: HashMap<String, Vec<u8>> = before
        .iter()
        .map(|task| {
            let path = task.data_file_path.to_string();
            (path.clone(), file_bytes(&path))
        })
        .collect();

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.filt', where => 'part = 0')",
    )
    .await
    .expect("filtered rewrite");
    let batches = result.collect().await.expect("collect");
    let batch = &batches[0];
    assert_eq!(call_count(batch, "rewritten_data_files_count"), 5);
    assert_eq!(call_count(batch, "added_data_files_count"), 1);
    assert_eq!(call_count(batch, "failed_data_files_count"), 0);
    assert!(
        !batch.schema().field(0).is_nullable()
            && !batch.schema().field(2).is_nullable()
            && batch.schema().field(2).data_type()
                == &datafusion::arrow::datatypes::DataType::Int64,
        "Spark's rewrite_data_files columns stay non-nullable with bigint bytes"
    );

    let after = planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(after.len(), 6, "5 rewritten into 1, plus 5 untouched");
    let after_paths: Vec<String> = after
        .iter()
        .map(|task| task.data_file_path.to_string())
        .collect();
    let mut identical = 0usize;
    for (path, bytes) in &before_bytes {
        if after_paths.iter().any(|after_path| after_path == path) {
            assert_eq!(
                file_bytes(path),
                *bytes,
                "out-of-scope file {path} must stay byte-identical"
            );
            identical += 1;
        }
    }
    assert_eq!(
        identical, 5,
        "all five part=1 files must keep their paths and bytes"
    );
    let after_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT CAST(id AS INT) FROM ice.sales.filt",
    )
    .await;
    assert_eq!(after_ids, vec![1, 2, 3, 4, 5, 101, 102, 103, 104, 105]);
}

#[tokio::test]
async fn call_rewrite_unknown_strategy_matches_spark_message() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    for name in ["nope", "zorder"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_data_files(table => 'sales.t', strategy => '{name}')"
            ),
        )
        .await
        .expect_err("unknown strategy must refuse");
        let message = error.to_string();
        assert!(
            message.contains(&format!(
                "unsupported strategy: {name}. Only binpack or sort is supported"
            )),
            "got: {message}"
        );
    }
}

#[tokio::test]
async fn call_rewrite_sort_order_refuses_and_does_not_compact() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_two_partition_groups(&ctx, &catalogs, "so").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "so".into());
    let files_before = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.so', sort_order => 'id ASC')",
    )
    .await
    .expect_err("sort_order must refuse");
    let message = error.to_string();
    assert!(
        message.contains("sort_order") && message.contains("not supported"),
        "got: {message}"
    );
    let files_after = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(files_after, files_before, "a refused CALL must not compact");
}

#[tokio::test]
async fn call_rewrite_bad_where_matches_spark_message() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    for expr in ["id === 1", "no_such_col = 1", "id", ""] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.rewrite_data_files(table => 'sales.t', where => '{expr}')"),
        )
        .await
        .expect_err("bad where must refuse");
        let message = error.to_string();
        assert!(
            message.contains(&format!("Cannot parse predicates in where option: {expr}")),
            "got: {message}"
        );
    }
}

#[tokio::test]
async fn call_rewrite_named_binpack_still_compacts_v2() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bin AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    for index in 2..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.bin SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.bin', strategy => 'BINPACK')",
    )
    .await
    .expect("named binpack");
    let batches = result.collect().await.expect("collect");
    assert!(call_count(&batches[0], "rewritten_data_files_count") >= 2);
}
