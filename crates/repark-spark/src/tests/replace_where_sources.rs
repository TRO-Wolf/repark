use super::super::*;
use super::common::*;
use super::replace_where::{PARTITIONED, SEED};

type Row = (Option<i64>, &'static str, Option<&'static str>);

async fn typed_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> Vec<(Option<i64>, String, Option<String>)> {
    let batches = execute(ctx, catalogs, "SELECT id, data, cat FROM ice.sales.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let data = datafusion::arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let data = data.as_any().downcast_ref::<StringArray>().unwrap();
        let cats = datafusion::arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
        let cats = cats.as_any().downcast_ref::<StringArray>().unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                ids.is_valid(index).then(|| ids.value(index)),
                data.value(index).to_string(),
                cats.is_valid(index).then(|| cats.value(index).to_string()),
            ));
        }
    }
    rows.sort();
    rows
}

fn owned(rows: &[Row]) -> Vec<(Option<i64>, String, Option<String>)> {
    let mut rows: Vec<_> = rows
        .iter()
        .map(|(id, data, cat)| (*id, (*data).to_string(), cat.map(str::to_string)))
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn a_replace_where_source_with_repeated_names_writes_as_spark_does() {
    let cases: [(&str, &[Row]); 3] = [
        (
            "SELECT id, CAST(id AS STRING), 'x' FROM ice.sales.t WHERE cat = 'x'",
            &[
                (Some(1), "1", Some("x")),
                (Some(2), "b", Some("y")),
                (Some(3), "3", Some("x")),
            ],
        ),
        (
            "SELECT 9 AS a, 'z' AS a, 'x'",
            &[(Some(2), "b", Some("y")), (Some(9), "z", Some("x"))],
        ),
        (
            "SELECT 9 AS a, 'z' AS a, 'x' UNION ALL SELECT 10, 'w', 'x'",
            &[
                (Some(10), "w", Some("x")),
                (Some(2), "b", Some("y")),
                (Some(9), "z", Some("x")),
            ],
        ),
    ];
    for (source, expected) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        run(&ctx, &catalogs, PARTITIONED).await;
        run(&ctx, &catalogs, SEED).await;
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' {source}"),
        )
        .await;
        assert_eq!(
            typed_rows(&ctx, &catalogs).await,
            owned(expected),
            "{source}"
        );
    }
}

#[tokio::test]
async fn a_replace_where_into_a_missing_namespace_answers_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = match execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.nope.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
    )
    .await
    {
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("a missing namespace refuses"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`nope`.`t` \
         cannot be found. Verify the spelling and correctness of the schema and catalog. If you \
         did not qualify the name with a schema, verify the current_schema() output, or qualify \
         the name with the correct schema and catalog. To tolerate the error on drop use DROP \
         VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    );
}

#[tokio::test]
async fn a_replace_where_into_a_session_table_answers_the_iceberg_only_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "plain", &[(1, "a")]);
    let error = match execute(
        &ctx,
        &catalogs,
        "INSERT INTO plain REPLACE WHERE id = 1 SELECT 2, 'b'",
    )
    .await
    {
        Ok(frame) => frame.collect().await.expect_err("a session table refuses"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "Error during planning: INSERT INTO … REPLACE WHERE requires an Iceberg table, got `plain`"
    );
}

async fn refusal(sql: &str) -> String {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, PARTITIONED).await;
    run(&ctx, &catalogs, SEED).await;
    match execute(&ctx, &catalogs, sql).await {
        Ok(frame) => frame.collect().await.expect_err(sql),
        Err(error) => error,
    }
    .to_string()
}

#[tokio::test]
async fn an_arity_refusal_names_every_data_column_as_spark_does() {
    let cases = [
        (
            "SELECT id, CAST(id AS STRING) FROM ice.sales.t",
            "not enough data columns",
            "`id`, `id`",
        ),
        (
            "SELECT *, CAST(id AS STRING) FROM (SELECT id FROM ice.sales.t)",
            "not enough data columns",
            "`id`, `id`",
        ),
        (
            "SELECT substr(data, 1, 1), id * 2 FROM ice.sales.t",
            "not enough data columns",
            "`substr(data, 1, 1)`, `(id * 2)`",
        ),
        (
            "SELECT data, CAST(id AS STRING) AS data FROM ice.sales.t",
            "not enough data columns",
            "`data`, `data`",
        ),
        (
            "SELECT id, CAST(id AS STRING), 'x', 1 FROM ice.sales.t",
            "too many data columns",
            "`id`, `id`, `x`, `1`",
        ),
    ];
    for (source, reason, columns) in cases {
        let message = refusal(&format!(
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' {source}"
        ))
        .await;
        assert!(
            message.contains(&format!(
                "the reason is {reason}:\nTable columns: `id`, `data`, `cat`.\nData columns: {columns}. SQLSTATE: 21S01"
            )),
            "{source}: {message}"
        );
        assert!(!message.contains("__repark_"), "{source}: {message}");
    }
}

#[tokio::test]
async fn a_suffix_typed_literal_renders_as_its_cast_in_the_refusal() {
    let message =
        refusal("INSERT INTO ice.sales.t REPLACE WHERE id = 1BD SELECT 9, 'z', 'x'").await;
    assert_eq!(
        message,
        "External error: Cannot convert Spark predicate to Iceberg expression: \
         id = CAST(1 AS DECIMAL(1,0))"
    );
}
