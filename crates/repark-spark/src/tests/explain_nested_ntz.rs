use super::super::*;
use super::common::*;

const STORE_KERNEL: &str = "__repark_cast_nested_ntz__";
const LOCALIZE_KERNEL: &str = "__repark_localize_nested_ntz__";

async fn explain(ctx: &SessionContext, catalogs: &CatalogRegistry, statement: &str) -> String {
    let batches = execute(ctx, catalogs, &format!("EXPLAIN {statement}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn explain_names_the_store_kernel_on_the_converted_iceberg_doors_only() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_at_zone(&warehouse, "America/New_York").await;
    repark_functions::register_all(&ctx);
    for ddl in [
        "CREATE TABLE ice.sales.zoned (id INT, st STRUCT<v: TIMESTAMP, n: INT>, c TIMESTAMP, \
         ns STRUCT<v: TIMESTAMP_NTZ, n: INT>) USING iceberg",
        "CREATE TABLE ice.sales.naive (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, \
         c TIMESTAMP, ns STRUCT<v: TIMESTAMP_NTZ, n: INT>) USING iceberg",
    ] {
        run(&ctx, &catalogs, ddl).await;
    }
    let select = "SELECT id, st, c, ns FROM ice.sales.zoned";
    let insert = format!("INSERT INTO ice.sales.naive {select}");
    let append = explain(&ctx, &catalogs, &insert).await;
    assert!(append.contains(STORE_KERNEL), "{append}");
    let overwrite = explain(
        &ctx,
        &catalogs,
        &format!("INSERT OVERWRITE ice.sales.naive {select}"),
    )
    .await;
    assert!(!overwrite.contains(STORE_KERNEL), "{overwrite}");
    let instant = explain(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.naive SET st = named_struct('v', c, 'n', 1) WHERE id >= 0",
    )
    .await;
    assert!(instant.contains(STORE_KERNEL), "{instant}");
    let wall = explain(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.naive SET st = ns WHERE id >= 0",
    )
    .await;
    assert!(!wall.contains(STORE_KERNEL), "{wall}");
    let field = "EXPLAIN UPDATE ice.sales.naive SET st.v = c WHERE id >= 0";
    let refused = match execute(&ctx, &catalogs, field).await {
        Ok(frame) => frame.collect().await.is_err(),
        Err(_) => true,
    };
    assert!(refused, "a field UPDATE keeps main's refusal");
    let mirror = explain(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.zoned SET st = ns WHERE id >= 0",
    )
    .await;
    assert!(!mirror.contains(LOCALIZE_KERNEL), "{mirror}");
    let cast = "CAST(ice.sales.zoned.ns AS Struct(";
    assert!(mirror.contains(cast), "{mirror}");
}
