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

async fn field_types(catalogs: &CatalogRegistry, table: &str) -> Vec<(String, String)> {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect()
}

fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, kind)| ((*name).to_string(), (*kind).to_string()))
        .collect()
}

#[tokio::test]
async fn a_timestamp_ltz_column_is_an_iceberg_timestamptz_on_v2_and_v3() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    for version in [2, 3] {
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.l{version} (id INT, c TIMESTAMP_LTZ, s STRUCT<t: timestamp_ltz>) \
                 USING iceberg TBLPROPERTIES ('format-version'='{version}')"
            ),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE ice.sales.l{version} ADD COLUMN c2 TIMESTAMP_LTZ"),
        )
        .await;
        assert_eq!(
            field_types(&catalogs, &format!("l{version}")).await,
            owned(&[
                ("id", "int"),
                ("c", "timestamptz"),
                ("s", "struct<timestamptz>"),
                ("c2", "timestamptz"),
            ])
        );
    }
}

#[tokio::test]
async fn a_timestamp_ltz_column_writes_reads_and_partitions_by_day() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lp (id INT, c TIMESTAMP_LTZ) USING iceberg PARTITIONED BY (days(c))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lp VALUES (0, TIMESTAMP'2024-01-01 23:00:00'), \
         (1, TIMESTAMP_LTZ '2024-01-02 01:00:00'), (2, NULL)",
    )
    .await;
    let table = load_sales_table(&catalogs, "lp").await;
    let spec: Vec<(String, String)> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.transform.to_string()))
        .collect();
    assert_eq!(spec, owned(&[("c_day", "day")]));
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, CAST(c AS STRING) AS s FROM ice.sales.lp ORDER BY id"
        )
        .await,
        "+----+---------------------+\n\
         | id | s                   |\n\
         +----+---------------------+\n\
         | 0  | 2024-01-01 23:00:00 |\n\
         | 1  | 2024-01-02 01:00:00 |\n\
         | 2  |                     |\n\
         +----+---------------------+"
    );
    let filtered = rendered(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.lp WHERE c > TIMESTAMP_LTZ '2024-01-02 00:00:00'",
    )
    .await;
    assert_eq!(filtered, "+----+\n| id |\n+----+\n| 1  |\n+----+");
}

#[tokio::test]
async fn a_timestamp_ltz_typed_literal_is_a_session_zone_instant() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT TIMESTAMP_LTZ '2024-01-01 00:00:00' AS a, timestamp_ltz'2024-06-01 12:30:00.123456' AS b",
    )
    .await;
    let schema = out[0].schema();
    for index in 0..2 {
        assert!(
            matches!(
                schema.field(index).data_type(),
                DataType::Timestamp(_, Some(_))
            ),
            "{:?}",
            schema.field(index)
        );
    }
    let text = rendered(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP_LTZ '2024-06-01 12:30:00.123456' AS STRING) AS s",
    )
    .await;
    assert!(text.contains("| 2024-06-01 12:30:00.123456 |"), "{text}");
}

#[tokio::test]
async fn a_timestamp_ltz_word_that_is_not_a_typed_literal_keeps_its_meaning() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.w (timestamp_ltz STRING) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.w VALUES ('x')").await;
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT timestamp_ltz FROM ice.sales.w WHERE timestamp_ltz = 'x'"
        )
        .await,
        "+---------------+\n| timestamp_ltz |\n+---------------+\n| x             |\n+---------------+"
    );
}

#[tokio::test]
async fn a_double_quoted_timestamp_ltz_literal_is_the_same_instant() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let text = rendered(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP_LTZ \"2024-01-01 00:00:00\" AS STRING) AS x, \
         TIMESTAMP_LTZ \"2024-01-01 00:00:00\" = TIMESTAMP_LTZ '2024-01-01 00:00:00' AS same",
    )
    .await;
    assert_eq!(
        text,
        "+---------------------+------+\n\
         | x                   | same |\n\
         +---------------------+------+\n\
         | 2024-01-01 00:00:00 | true |\n\
         +---------------------+------+"
    );
}
