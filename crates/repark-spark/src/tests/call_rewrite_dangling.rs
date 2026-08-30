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
