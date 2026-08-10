/// CT-1 (F-BR-2 residual, COPY TO): a bare `COPY … TO …` whose returned `DataFrame` is never
/// collected still writes the files. `LogicalPlan::Copy` is DataFusion-lazy (the file sink
/// commits only on collect) exactly like DML — PySpark applies commands eagerly. Mutation: drop
/// `Copy` from the eager-command predicate → the write never happens → no files → RED.
/// r24 SB1: conf `repark.sql.allowLocalFilesystemDDL=true` (destination is outside warehouse).
use super::super::*;
use super::common::*;

#[tokio::test]
async fn bare_copy_to_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_local_fs_ddl(&wh).await;
    let out = TempDir::new().unwrap();
    let dest = out.path().join("exported");
    let dest_str = dest.to_str().unwrap();

    execute_without_collecting(
        &ctx,
        &catalogs,
        &format!("COPY (SELECT * FROM src) TO '{dest_str}' STORED AS PARQUET"),
    )
    .await;

    assert!(
        count_parquet_files(&dest) > 0,
        "a bare COPY TO must write files eagerly at execute()"
    );
}

/// CT-2 (F-BR-2 residual, COPY TO): the COPY is applied eagerly AND collecting the returned
/// `DataFrame` does NOT re-run it — the no-double-apply trap the naive return-the-live-plan fix
/// creates. Files are deleted after the eager write; a `.collect()` that re-ran the sink would
/// recreate them. Mutation: return the live `Copy` plan → the deleted files reappear → RED.
/// r24 SB1: conf `repark.sql.allowLocalFilesystemDDL=true` (destination is outside warehouse).
#[tokio::test]
async fn copy_to_applies_exactly_once_across_a_later_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_local_fs_ddl(&wh).await;
    let out = TempDir::new().unwrap();
    let dest = out.path().join("exported");
    let dest_str = dest.to_str().unwrap();

    let returned = execute(
        &ctx,
        &catalogs,
        &format!("COPY (SELECT * FROM src) TO '{dest_str}' STORED AS PARQUET"),
    )
    .await
    .unwrap();
    // Eager: the files are present before the returned DataFrame is touched.
    assert!(
        count_parquet_files(&dest) > 0,
        "the COPY must be applied eagerly at execute() time"
    );

    // Remove the written files; collecting the returned DataFrame must NOT re-run the COPY.
    std::fs::remove_dir_all(&dest).unwrap();
    returned.collect().await.unwrap();
    assert_eq!(
        count_parquet_files(&dest),
        0,
        "collecting the returned DataFrame must not re-run the COPY"
    );
}

/// r24 SB1 / SEC-02: default conf refuses COPY TO outside the warehouse and names the conf.
#[tokio::test]
async fn copy_to_local_outside_warehouse_refuses_by_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let out = TempDir::new().unwrap();
    let dest = out.path().join("blocked");
    let dest_str = dest.to_str().unwrap();
    let err = execute(
        &ctx,
        &catalogs,
        &format!("COPY (SELECT * FROM src) TO '{dest_str}' STORED AS PARQUET"),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(repark_functions::cardinality::ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
        "must name conf: {err}"
    );
    assert!(!dest.exists(), "blocked COPY must not write files");
}

/// r24 SB1 / SEC-02: warehouse-root grandfather still allows COPY under the registered root.
#[tokio::test]
async fn copy_to_under_warehouse_root_grandfathers() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let dest = wh.path().join("exported_under_wh");
    let dest_str = dest.to_str().unwrap();
    execute(
        &ctx,
        &catalogs,
        &format!("COPY (SELECT * FROM src) TO '{dest_str}' STORED AS PARQUET"),
    )
    .await
    .unwrap();
    assert!(
        count_parquet_files(&dest) > 0,
        "COPY under warehouse root must be grandfathered"
    );
}

/// r24 SB1 / SEC-01 free-SQL path: `array_repeat` over the ceiling refuses naming conf.
#[tokio::test]
async fn free_sql_array_repeat_over_ceiling_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Default ceiling is 10_000_000 — use a higher literal.
    let err = execute(
        &ctx,
        &catalogs,
        "SELECT cardinality(array_repeat(1, 10000001)) AS n",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(repark_functions::cardinality::MAX_ARRAY_ELEMENTS_KEY),
        "free-SQL ceiling must name conf: {err}"
    );
}
