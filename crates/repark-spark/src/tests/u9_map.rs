use datafusion::arrow::util::pretty::pretty_format_batches;

use super::super::*;
use super::common::*;

async fn batches(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

async fn rendered(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    pretty_format_batches(&batches(ctx, catalogs, sql).await)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn an_empty_map_call_is_an_empty_map_of_void() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let out = batches(&ctx, &catalogs, "SELECT map() AS m, MAP() AS n").await;
    let DataType::Map(entries, _) = out[0].schema().field(0).data_type().clone() else {
        panic!("{:?}", out[0].schema());
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("{entries:?}");
    };
    assert_eq!(fields[0].data_type(), &DataType::Null);
    assert_eq!(fields[1].data_type(), &DataType::Null);
    assert_eq!(
        out[0].schema().field(1).data_type(),
        out[0].schema().field(0).data_type()
    );
    assert_eq!(
        pretty_format_batches(&out).unwrap().to_string(),
        "+----+----+\n| m  | n  |\n+----+----+\n| {} | {} |\n+----+----+"
    );
}

#[tokio::test]
async fn a_map_column_takes_literals_the_empty_map_and_null() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m (id INT, c MAP<STRING, INT>, n MAP<STRING, MAP<STRING, INT>>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.m VALUES (0, map('k', 1), map('k', map('j', 1))), \
         (1, map(), map('e', map())), (2, NULL, map()), (3, map('a', 2, 'b', NULL), NULL)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.m SELECT 4, map(), map()",
    )
    .await;
    let table = load_sales_table(&catalogs, "m").await;
    let schema = table.metadata().current_schema();
    assert_eq!(
        serde_json::to_string(&schema.as_struct().fields()[1].field_type).unwrap(),
        r#"{"type":"map","key-id":4,"key":"string","value-id":5,"value-required":false,"value":"int"}"#
    );
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, c, n, c['k'] AS k, n['k']['j'] AS j FROM ice.sales.m ORDER BY id"
        )
        .await,
        "+----+-------------+-------------+---+---+\n\
         | id | c           | n           | k | j |\n\
         +----+-------------+-------------+---+---+\n\
         | 0  | {k: 1}      | {k: {j: 1}} | 1 | 1 |\n\
         | 1  | {}          | {e: {}}     |   |   |\n\
         | 2  |             | {}          |   |   |\n\
         | 3  | {a: 2, b: } |             |   |   |\n\
         | 4  | {}          | {}          |   |   |\n\
         +----+-------------+-------------+---+---+"
    );
}
