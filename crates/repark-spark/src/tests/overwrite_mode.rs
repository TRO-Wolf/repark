use std::collections::HashSet;

use iceberg::spec::Operation;

use super::super::*;
use super::common::*;
use super::dyn_partition_overwrite::{seed, setup_dynamic};

type OptionCase = (&'static [(&'static str, &'static str)], Vec<(i32, String)>);

async fn joined_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> Vec<String> {
    joined_rows_of(
        ctx,
        catalogs,
        &format!("SELECT concat_ws('|', CAST(id AS STRING), name, sub) AS row FROM {table}"),
    )
    .await
}

async fn joined_rows_of(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<String> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let column = datafusion::arrow::compute::cast(
            batch.column(0),
            &datafusion::arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        let values = column.as_any().downcast_ref::<StringArray>().unwrap();
        for index in 0..batch.num_rows() {
            rows.push(values.value(index).to_string());
        }
    }
    rows.sort();
    rows
}

async fn seed_two_level(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.two (id INT, name STRING, sub STRING) USING iceberg \
         PARTITIONED BY (name, sub)",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.two VALUES (1, 'x', 'p'), (2, 'y', 'p'), (3, 'x', 'q'), \
         (4, 'y', 'q')",
    )
    .await;
}

async fn last_operation(catalogs: &CatalogRegistry, table: &str) -> (Operation, bool) {
    let table = load_sales_table(catalogs, table).await;
    let snapshot = table.metadata().current_snapshot().expect("snapshot");
    let replace = snapshot
        .summary()
        .additional_properties
        .get("replace-partitions")
        .is_some_and(|value| value == "true");
    (snapshot.summary().operation.clone(), replace)
}

async fn run_with_options(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    options: &crate::write_options::StatementWriteOptions,
) {
    crate::execute_with_statement_options(ctx, catalogs, sql, &HashSet::<String>::new(), options)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

fn options(
    pairs: &[(&str, &str)],
    intent: repark_iceberg::write::OverwriteIntent,
) -> crate::write_options::StatementWriteOptions {
    let mut options = crate::write_options::StatementWriteOptions::validate(
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    )
    .unwrap();
    options.overwrite_intent = intent;
    options
}

#[tokio::test]
async fn static_mode_partition_clause_without_values_replaces_the_whole_table() {
    for sql in [
        "INSERT OVERWRITE ice.sales.t PARTITION (name) SELECT 20, 'b'",
        "INSERT OVERWRITE TABLE ice.sales.t PARTITION (name) SELECT 20, 'b'",
    ] {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&warehouse).await;
        seed(&ctx, &catalogs).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.t").await,
            vec![(20, "b".into())],
            "{sql}"
        );
        assert_eq!(
            last_operation(&catalogs, "t").await,
            (Operation::Overwrite, false)
        );
    }
}

#[tokio::test]
async fn static_mode_empty_partition_clause_without_values_wipes_the_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (name) SELECT 20, 'b' WHERE false",
    )
    .await;
    assert!(table_rows(&ctx, &catalogs, "ice.sales.t").await.is_empty());
    assert_eq!(
        last_operation(&catalogs, "t").await,
        (Operation::Delete, false)
    );
}

#[tokio::test]
async fn mixed_static_and_dynamic_keys_follow_the_session_mode() {
    for sql in [
        "INSERT OVERWRITE ice.sales.two PARTITION (name = 'x', sub) SELECT 9, 'p'",
        "INSERT OVERWRITE ice.sales.two PARTITION (name = 'x', sub) BY NAME \
         SELECT 'p' AS sub, 9 AS id",
    ] {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&warehouse).await;
        seed_two_level(&ctx, &catalogs).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            joined_rows(&ctx, &catalogs, "ice.sales.two").await,
            vec!["2|y|p", "4|y|q", "9|x|p"],
            "{sql}"
        );
        assert_eq!(
            last_operation(&catalogs, "two").await,
            (Operation::Overwrite, false)
        );

        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_dynamic(&warehouse).await;
        seed_two_level(&ctx, &catalogs).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            joined_rows(&ctx, &catalogs, "ice.sales.two").await,
            vec!["2|y|p", "3|x|q", "4|y|q", "9|x|p"],
            "{sql}"
        );
        assert_eq!(
            last_operation(&catalogs, "two").await,
            (Operation::Overwrite, true)
        );
    }
}

#[tokio::test]
async fn empty_mixed_source_commits_nothing_in_dynamic_mode_and_deletes_in_static_mode() {
    let sql =
        "INSERT OVERWRITE ice.sales.two PARTITION (name = 'x', sub) SELECT 9, 'p' WHERE false";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed_two_level(&ctx, &catalogs).await;
    let before = load_sales_table(&catalogs, "two")
        .await
        .metadata()
        .snapshots()
        .count();
    run(&ctx, &catalogs, sql).await;
    assert_eq!(
        load_sales_table(&catalogs, "two")
            .await
            .metadata()
            .snapshots()
            .count(),
        before
    );
    assert_eq!(
        joined_rows(&ctx, &catalogs, "ice.sales.two").await,
        vec!["1|x|p", "2|y|p", "3|x|q", "4|y|q"]
    );

    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_two_level(&ctx, &catalogs).await;
    run(&ctx, &catalogs, sql).await;
    assert_eq!(
        joined_rows(&ctx, &catalogs, "ice.sales.two").await,
        vec!["2|y|p", "4|y|q"]
    );
    assert_eq!(
        last_operation(&catalogs, "two").await,
        (Operation::Delete, false)
    );
}

#[tokio::test]
async fn static_value_is_cast_to_a_date_partition_and_an_invalid_value_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.d (id INT, name STRING, d DATE) USING iceberg PARTITIONED BY (d)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.d VALUES (1, 'a', DATE'2024-01-01'), (2, 'b', DATE'2024-01-02')",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.d PARTITION (d = '2024-13-45') SELECT 9, 'z'",
    )
    .await
    .expect_err("the DATE cast rejects the value");
    assert!(
        error.to_string().contains(
            "[CAST_INVALID_INPUT] The value '2024-13-45' of the type \"STRING\" cannot be \
                 cast to \"DATE\" because it is malformed."
        ),
        "{error}"
    );
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.d PARTITION (d = '2024-01-01') SELECT 9, 'z'",
    )
    .await;
    assert_eq!(
        joined_rows_of(
            &ctx,
            &catalogs,
            "SELECT concat_ws('|', CAST(id AS STRING), name, CAST(d AS STRING)) AS row \
             FROM ice.sales.d",
        )
        .await,
        vec!["2|b|2024-01-02", "9|z|2024-01-01"]
    );
    assert_eq!(
        last_operation(&catalogs, "d").await,
        (Operation::Overwrite, false)
    );
}

#[tokio::test]
async fn partition_clause_on_an_unpartitioned_table_refuses_non_partition_column() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    for sql in [
        "INSERT OVERWRITE ice.sales.t PARTITION (name) SELECT 20, 'b'",
        "INSERT OVERWRITE ice.sales.t PARTITION (name = 'b') SELECT 20",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("PARTITION on an unpartitioned table must refuse");
        assert_eq!(
            error.strip_backtrace(),
            "Error during planning: [NON_PARTITION_COLUMN] PARTITION clause cannot contain the \
             non-partition column: `name`. SQLSTATE: 42000",
            "{sql}"
        );
    }
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into())]
    );
}

#[tokio::test]
async fn overwrite_mode_writer_option_turns_a_whole_table_overwrite_dynamic() {
    use repark_iceberg::write::OverwriteIntent::Session;
    let cases: [OptionCase; 5] = [
        (
            &[("overwrite-mode", "dynamic")],
            vec![(1, "a".into()), (3, "c".into()), (20, "b".into())],
        ),
        (
            &[("OVERWRITE-MODE", "DYNAMIC")],
            vec![(1, "a".into()), (3, "c".into()), (20, "b".into())],
        ),
        (&[("overwrite-mode", "static")], vec![(20, "b".into())]),
        (&[("overwrite-mode", "bogus")], vec![(20, "b".into())]),
        (
            &[("partitionOverwriteMode", "dynamic")],
            vec![(20, "b".into())],
        ),
    ];
    for (pairs, want) in cases {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&warehouse).await;
        seed(&ctx, &catalogs).await;
        run_with_options(
            &ctx,
            &catalogs,
            "INSERT OVERWRITE ice.sales.t SELECT 20, 'b'",
            &options(pairs, Session),
        )
        .await;
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.t").await,
            want,
            "{pairs:?}"
        );
    }
}

#[tokio::test]
async fn overwrite_mode_option_keeps_static_values_and_the_static_intent() {
    use repark_iceberg::write::OverwriteIntent::{Session, Static};
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_two_level(&ctx, &catalogs).await;
    run_with_options(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.two PARTITION (name = 'x', sub) SELECT 9, 'p'",
        &options(&[("overwrite-mode", "dynamic")], Session),
    )
    .await;
    assert_eq!(
        joined_rows(&ctx, &catalogs, "ice.sales.two").await,
        vec!["2|y|p", "4|y|q", "9|x|p"]
    );

    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run_with_options(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (id, name) SELECT 20, 'b'",
        &options(&[("overwrite-mode", "dynamic")], Static),
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(20, "b".into())]
    );
}

#[tokio::test]
async fn dynamic_intent_replaces_partitions_in_static_session_mode() {
    use repark_iceberg::write::OverwriteIntent::Dynamic;
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run_with_options(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (id, name) PARTITION (name) SELECT 20, 'b'",
        &options(&[("overwrite-mode", "static")], Dynamic),
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (3, "c".into()), (20, "b".into())]
    );
    assert_eq!(
        last_operation(&catalogs, "t").await,
        (Operation::Overwrite, true)
    );
}

#[tokio::test]
async fn string_static_value_on_a_timestamp_partition_refuses_until_the_cast_follows_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ts (id INT, name STRING, ts TIMESTAMP) USING iceberg \
         PARTITIONED BY (ts)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ts VALUES (1, 'a', TIMESTAMP'2024-01-01 00:00:00')",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.ts PARTITION (ts = '2024-01-01 00:00:00') SELECT 9, 'z'",
    )
    .await
    .expect_err("a string static value on a TIMESTAMP partition refuses");
    assert!(error.to_string().contains("CAST-TS-STRING-1"), "{error}");
    assert_eq!(
        joined_rows_of(
            &ctx,
            &catalogs,
            "SELECT concat_ws('|', CAST(id AS STRING), name) AS row FROM ice.sales.ts",
        )
        .await,
        vec!["1|a"]
    );
}
