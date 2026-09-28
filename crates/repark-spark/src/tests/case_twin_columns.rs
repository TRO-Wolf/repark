use super::super::*;
use super::common::*;
use datafusion::arrow::array::Int32Array;

async fn setup_case_sensitive(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_functions::case_sensitive::SparkCaseSensitiveConfig { enabled: true });
    (ctx, catalogs)
}

fn set_case_sensitive(ctx: &SessionContext, enabled: bool) {
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_functions::case_sensitive::SparkCaseSensitiveConfig { enabled });
}

async fn select_star_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Vec<String>, Vec<RecordBatch>) {
    let frame = execute(ctx, catalogs, sql).await.unwrap();
    let names = frame
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let batches = frame.collect().await.unwrap();
    (names, batches)
}

async fn describe_table_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(String, String, Option<String>)> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let comments = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                names.value(index).to_string(),
                types.value(index).to_string(),
                (!comments.is_null(index)).then(|| comments.value(index).to_string()),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn case_twin_create_reads_both_columns_under_case_sensitive_true() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_case_sensitive(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tw (a INT, A INT) USING iceberg",
    )
    .await
    .unwrap();
    let (names, batches) = select_star_names(&ctx, &catalogs, "SELECT * FROM ice.sales.tw").await;
    assert_eq!(names, vec!["a".to_string(), "A".to_string()]);
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 0);
    assert_eq!(
        describe_table_rows(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.tw").await,
        vec![
            ("a".to_string(), "int".to_string(), None),
            ("A".to_string(), "int".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn case_twin_ctas_reads_both_values_under_case_sensitive_true() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_case_sensitive(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.twctas USING iceberg AS SELECT 1 AS a, 2 AS \"A\"",
    )
    .await
    .unwrap();
    let (names, batches) =
        select_star_names(&ctx, &catalogs, "SELECT * FROM ice.sales.twctas").await;
    assert_eq!(names, vec!["a".to_string(), "A".to_string()]);
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1);
    let mut values: Vec<Vec<i32>> = Vec::new();
    for batch in &batches {
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push(vec![left.value(row), right.value(row)]);
        }
    }
    assert_eq!(values, vec![vec![1, 2]]);
}

#[tokio::test]
async fn case_twin_bare_name_select_refuses_under_case_sensitive_false() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_case_sensitive(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.twf (a INT, A INT) USING iceberg",
    )
    .await
    .unwrap();
    set_case_sensitive(&ctx, false);
    let result = execute(&ctx, &catalogs, "SELECT a FROM ice.sales.twf").await;
    assert!(result.is_err());
}
