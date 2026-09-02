//! pins: rp-2-fork-repin/C-006
//! RP-2 / C-006: the composed `remove-dangling-deletes` sub-action through the CALL.

use super::super::*;
use super::call::call_count;
use super::common::*;

#[tokio::test]
async fn call_rewrite_data_files_remove_dangling_deletes_reports_a_true_count() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rwd (id INT, name STRING, part INT) \
         PARTITIONED BY (part) \
         TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')",
    )
    .await;
    for index in 1..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.rwd VALUES ({index}, 'n{index}', 0)"),
        )
        .await;
    }
    run(&ctx, &catalogs, "DELETE FROM ice.sales.rwd WHERE id = 2").await;

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rwd', \
         'remove-dangling-deletes' => true)",
    )
    .await
    .expect("rewrite CALL with the option");
    let batches = result.collect().await.expect("collect rewrite result");
    let removed = call_count(&batches[0], "removed_delete_files_count");
    assert!(
        removed >= 1,
        "the compacted data file strands the position delete; the sub-action must remove it, \
         got removed_delete_files_count = {removed}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rwd WHERE id = 2").await,
        0,
        "the deleted row stays deleted after the rewrite + GC"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rwd").await,
        5,
        "live rows are untouched by the dangling-delete GC"
    );
}

#[tokio::test]
async fn call_rewrite_data_files_drops_the_merge_delete_that_names_one_data_file() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rfs (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    for id in 1..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.rfs VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.rfs AS t USING (SELECT 2 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.v = 'merged'",
    )
    .await;
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.rfs.files WHERE content = 1"
        )
        .await,
        1,
        "the MERGE must write one position-delete file over one data file"
    );

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rfs')",
    )
    .await
    .expect("rewrite CALL");
    let batches = result.collect().await.expect("collect rewrite result");
    assert_eq!(
        call_count(&batches[0], "removed_delete_files_count"),
        1,
        "the delete file carries exact, equal `file_path` bounds, so it is file-scoped and dies \
         with the data file it named — no `remove-dangling-deletes` needed"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.rfs.files WHERE content = 1"
        )
        .await,
        0,
        "no delete file outlives the rewrite"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.rfs WHERE v = 'merged'"
        )
        .await,
        1,
        "the merged row does not resurrect its pre-merge twin"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rfs").await,
        6
    );
}

#[tokio::test]
async fn call_rewrite_data_files_keeps_a_partition_delete_that_names_two_data_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rps (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', \
         'write.delete.granularity' = 'partition')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.rps VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.rps VALUES (3, 'c'), (4, 'd')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.rps AS t USING (SELECT 1 AS id UNION ALL SELECT 3) AS s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = 'merged'",
    )
    .await;
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.rps.files WHERE content = 1"
        )
        .await,
        1,
        "partition granularity folds both data files' deletes into one delete file"
    );

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rps')",
    )
    .await
    .expect("rewrite CALL");
    let batches = result.collect().await.expect("collect rewrite result");
    assert_eq!(
        call_count(&batches[0], "removed_delete_files_count"),
        0,
        "a delete file naming two data files has UNEQUAL `file_path` bounds, so it is not \
         file-scoped and no rewrite can attribute it to either file"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.rps.files WHERE content = 1"
        )
        .await,
        1,
        "the partition-scoped delete file outlives the rewrite, still covering live data files"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.rps WHERE v = 'merged'"
        )
        .await,
        2,
        "the rows it shadows do not resurrect"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rps").await,
        4
    );
}
