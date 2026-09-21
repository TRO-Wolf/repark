use super::super::*;
use super::common::*;

#[tokio::test]
async fn merge_cardinality_violation_merge_on_read_errors() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "p"), (2, "q")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t \
             TBLPROPERTIES('write.merge.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await
    .unwrap_err();
    let mapped = repark_core::engine_err(err);
    assert!(matches!(mapped, repark_common::Error::Analysis(_)));
    assert!(mapped.to_string().contains("[MERGE_CARDINALITY_VIOLATION]"));
    assert!(mapped.to_string().contains("SQLSTATE: 23K01"));
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.t").await, before);
}
