use std::path::Path;

use futures::future::FutureExt;
use iceberg::maintenance::DeleteReachableFiles;
use iceberg::{Error, ErrorKind};

use super::super::*;
use super::common::*;
use crate::namespace_ddl::purge::{GC_DISABLED_REFUSAL, plan_purge};

const QUALIFIED: [&str; 3] = ["ice", "sales", "purged"];

async fn seeded(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, props: &str) {
    let tail = if props.is_empty() {
        String::new()
    } else {
        format!(" TBLPROPERTIES ({props})")
    };
    execute(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id BIGINT, data STRING) USING iceberg{tail}"),
    )
    .await
    .unwrap();
    execute(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b')"),
    )
    .await
    .unwrap();
}

async fn reachable_paths(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<String> {
    let frame = execute(
        ctx,
        catalogs,
        &format!("SELECT file_path FROM ice.sales.{table}.all_files"),
    )
    .await
    .unwrap();
    let batches = frame.collect().await.unwrap();
    let mut out = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..column.len() {
            out.push(column.value(index).to_string());
        }
    }
    out
}

fn on_disk(paths: &[String]) -> Vec<bool> {
    paths
        .iter()
        .map(|path| Path::new(path.trim_start_matches("file://")).exists())
        .collect()
}

#[tokio::test]
async fn drop_table_purge_deletes_reachable_files_and_plain_drop_keeps_them() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    seeded(&ctx, &catalogs, "purged", "").await;
    let purged_paths = reachable_paths(&ctx, &catalogs, "purged").await;
    assert!(!purged_paths.is_empty());
    assert!(on_disk(&purged_paths).iter().all(|exists| *exists));

    seeded(&ctx, &catalogs, "kept", "").await;
    let kept_paths = reachable_paths(&ctx, &catalogs, "kept").await;
    assert!(!kept_paths.is_empty());

    execute(&ctx, &catalogs, "DROP TABLE ice.sales.purged PURGE")
        .await
        .unwrap();
    execute(&ctx, &catalogs, "DROP TABLE ice.sales.kept")
        .await
        .unwrap();

    assert!(
        on_disk(&purged_paths).iter().all(|exists| !exists),
        "PURGE must delete every data file: {purged_paths:?}"
    );
    assert!(
        on_disk(&kept_paths).iter().all(|exists| *exists),
        "plain DROP TABLE must keep every data file: {kept_paths:?}"
    );
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "purged".to_string(),
    );
    assert!(!catalogs["ice"].table_exists(&ident).await.unwrap());
}

#[tokio::test]
async fn purge_refuses_when_gc_is_disabled_and_sweeps_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seeded(&ctx, &catalogs, "purged", "'gc.enabled'='false'").await;
    let paths = reachable_paths(&ctx, &catalogs, "purged").await;

    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "purged".to_string(),
    );
    let planned = plan_purge(catalogs["ice"].as_ref(), &ident, QUALIFIED).await;
    assert!(
        planned
            .as_ref()
            .err()
            .is_some_and(|error| error.to_string().contains(GC_DISABLED_REFUSAL)),
        "plan must refuse: {planned:?}"
    );

    let refused = execute(&ctx, &catalogs, "DROP TABLE ice.sales.purged PURGE")
        .await
        .expect_err("gc.enabled=false must refuse the purge")
        .to_string();
    assert!(refused.contains(GC_DISABLED_REFUSAL), "got: {refused}");
    assert!(on_disk(&paths).iter().all(|exists| *exists));
    assert!(
        catalogs["ice"].table_exists(&ident).await.unwrap(),
        "a refused purge must not drop the table"
    );
}

#[tokio::test]
async fn purge_collects_delete_failures_and_the_drop_still_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seeded(&ctx, &catalogs, "purged", "").await;
    let paths = reachable_paths(&ctx, &catalogs, "purged").await;

    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "purged".to_string(),
    );
    let (location, file_io) = plan_purge(catalogs["ice"].as_ref(), &ident, QUALIFIED)
        .await
        .unwrap();
    let result = DeleteReachableFiles::new(location)
        .io(file_io)
        .delete_with(|path| {
            async move { Err(Error::new(ErrorKind::Unexpected, format!("refused {path}"))) }.boxed()
        })
        .execute()
        .await
        .expect("a per-file delete failure is collected, never returned as Err");

    assert!(!result.delete_failures.is_empty());
    assert_eq!(
        result.delete_failures.len() as u64,
        result.total_deleted_files_count(),
        "every planned file failed, so the failure list is the whole reachable set"
    );
    assert!(on_disk(&paths).iter().all(|exists| *exists));

    execute(&ctx, &catalogs, "DROP TABLE ice.sales.purged PURGE")
        .await
        .unwrap();
    assert!(!catalogs["ice"].table_exists(&ident).await.unwrap());
    assert!(on_disk(&paths).iter().all(|exists| !exists));
}

#[tokio::test]
async fn purge_composes_with_if_exists_and_names_a_missing_table_the_spark_way() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "DROP TABLE IF EXISTS ice.sales.never_there PURGE",
    )
    .await
    .unwrap();

    let error = execute(&ctx, &catalogs, "DROP TABLE ice.sales.never_there PURGE")
        .await
        .expect_err("a missing PURGE target refuses")
        .to_string();
    assert!(error.contains("[TABLE_OR_VIEW_NOT_FOUND]"), "got: {error}");
    assert!(error.contains("SQLSTATE: 42P01"), "got: {error}");
}
