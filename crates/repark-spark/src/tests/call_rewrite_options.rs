//! pins: maint-rewrite-data-files-options/C-002, C-003, C-004, C-005, C-006, C-007, C-008
//! `rewrite_data_files` `where` / `strategy` / `sort_order` on v2 tables.

use std::collections::HashMap;
use std::path::PathBuf;

use super::super::*;
use super::call::call_count;
use super::common::*;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, PrimitiveLiteral};

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

fn identity_partition_int(task: &FileScanTask) -> i32 {
    let partition = task
        .partition
        .as_ref()
        .expect("partitioned rewrite fixture must stamp a partition struct");
    match partition.fields().first().and_then(|field| field.as_ref()) {
        Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => *value,
        other => panic!("expected Int partition field, got {other:?}"),
    }
}

fn assert_kept_paths_are_exactly_part(
    before: &[FileScanTask],
    after: &[FileScanTask],
    before_bytes: &HashMap<String, Vec<u8>>,
    kept_part: i32,
) {
    let kept_before: Vec<String> = before
        .iter()
        .filter(|task| identity_partition_int(task) == kept_part)
        .map(|task| task.data_file_path.to_string())
        .collect();
    assert_eq!(
        kept_before.len(),
        5,
        "fixture must have five files in part={kept_part}"
    );
    let rewritten_before: Vec<String> = before
        .iter()
        .filter(|task| identity_partition_int(task) != kept_part)
        .map(|task| task.data_file_path.to_string())
        .collect();
    assert_eq!(rewritten_before.len(), 5);
    let after_paths: std::collections::HashSet<String> = after
        .iter()
        .map(|task| task.data_file_path.to_string())
        .collect();
    for path in &kept_before {
        assert!(
            after_paths.contains(path),
            "part={kept_part} file must keep its path: {path}"
        );
        assert_eq!(
            file_bytes(path),
            before_bytes[path],
            "part={kept_part} file {path} must stay byte-identical"
        );
    }
    for path in &rewritten_before {
        assert!(
            !after_paths.contains(path),
            "in-scope file must be rewritten away: {path}"
        );
    }
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
    assert_kept_paths_are_exactly_part(&before, &after, &before_bytes, 1);
    let after_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT CAST(id AS INT) FROM ice.sales.filt",
    )
    .await;
    assert_eq!(after_ids, vec![1, 2, 3, 4, 5, 101, 102, 103, 104, 105]);
}

#[tokio::test]
async fn call_rewrite_where_in_keeps_out_of_scope_files_byte_identical() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_two_partition_groups(&ctx, &catalogs, "filt_in").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "filt_in".into());
    let before = planned_data_files(catalogs["ice"].as_ref(), &ident).await;
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
        "CALL ice.system.rewrite_data_files(table => 'sales.filt_in', where => 'part IN (0)')",
    )
    .await
    .expect("IN rewrite");
    let batches = result.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 5);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 1);
    let after = planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(after.len(), 6);
    assert_kept_paths_are_exactly_part(&before, &after, &before_bytes, 1);
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
