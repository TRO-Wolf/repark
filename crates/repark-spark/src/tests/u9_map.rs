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

#[tokio::test]
async fn a_back_quoted_empty_map_call_is_an_empty_map_of_void() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT `map`() AS m, `MAP`() AS n").await,
        "+----+----+\n| m  | n  |\n+----+----+\n| {} | {} |\n+----+----+"
    );
}

#[tokio::test]
async fn update_and_every_merge_clause_assign_the_empty_map() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for sql in [
        "CREATE TABLE ice.sales.mu (id INT, c MAP<STRING, INT>) USING iceberg",
        "INSERT INTO ice.sales.mu VALUES (0, map('a', 1)), (1, map('b', 2)), (2, map('c', 3))",
        "UPDATE ice.sales.mu SET c = map() WHERE id = 0",
        "MERGE INTO ice.sales.mu t USING (SELECT 1 AS id) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET c = map()",
        "MERGE INTO ice.sales.mu t USING (SELECT 5 AS id) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, c) VALUES (s.id, map())",
        "MERGE INTO ice.sales.mu t USING (SELECT 0 AS id) s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE AND t.id = 2 THEN UPDATE SET c = map()",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, c FROM ice.sales.mu ORDER BY id"
        )
        .await,
        "+----+----+\n| id | c  |\n+----+----+\n| 0  | {} |\n| 1  | {} |\n| 2  | {} |\n| 5  | {} |\n+----+----+"
    );
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let outcome = match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame.collect().await.map(|_| ()),
        Err(error) => Err(error),
    };
    match outcome {
        Ok(()) => panic!("`{sql}` must refuse"),
        Err(DataFusionError::Plan(text)) => text,
        Err(error) => panic!("`{sql}` must refuse at planning: {error}"),
    }
}

fn cannot_safely_cast(path: &str, from: &str, to: &str) -> String {
    format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the \
         table ``: Cannot safely cast {path} \"{from}\" to \"{to}\". SQLSTATE: KD000"
    )
}

#[tokio::test]
async fn update_and_merge_refuse_a_map_key_or_value_that_cannot_store_assign() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for sql in [
        "CREATE TABLE ice.sales.mk (id INT, c MAP<INT, STRING>) USING iceberg",
        "CREATE TABLE ice.sales.mv (id INT, c MAP<STRING, INT>) USING iceberg",
        "CREATE TABLE ice.sales.mst (id INT, c MAP<STRING, STRUCT<a: INT, b: INT>>) USING iceberg",
        "INSERT INTO ice.sales.mk VALUES (0, NULL)",
        "INSERT INTO ice.sales.mv VALUES (0, NULL)",
        "INSERT INTO ice.sales.mst VALUES (0, NULL)",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let key = cannot_safely_cast("`c`.`key`", "STRING", "INT");
    let value = cannot_safely_cast("`c`.`value`", "STRING", "INT");
    let date = cannot_safely_cast("`c`.`value`", "DATE", "INT");
    let missing = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data \
                   for the table ``: Cannot find data for the output column `c`.`value`.`b`. \
                   SQLSTATE: KD000"
        .to_string();
    for (table, source, expected) in [
        ("mk", "map('1', 'x')", &key),
        ("mv", "map('a', 'x')", &value),
        ("mv", "map('a', DATE'2024-01-01')", &date),
        ("mst", "map('a', named_struct('a', 1))", &missing),
    ] {
        let update = format!("UPDATE ice.sales.{table} SET c = {source} WHERE id = 0");
        assert_eq!(
            &refusal(&ctx, &catalogs, &update).await,
            expected,
            "{update}"
        );
        let merge = format!(
            "MERGE INTO ice.sales.{table} x USING (SELECT 0 AS id, {source} AS m) y \
             ON x.id = y.id WHEN MATCHED THEN UPDATE SET c = y.m"
        );
        assert_eq!(&refusal(&ctx, &catalogs, &merge).await, expected, "{merge}");
        let insert = format!(
            "MERGE INTO ice.sales.{table} x USING (SELECT 7 AS id, {source} AS m) y \
             ON x.id = y.id WHEN NOT MATCHED THEN INSERT (id, c) VALUES (y.id, y.m)"
        );
        assert_eq!(
            &refusal(&ctx, &catalogs, &insert).await,
            expected,
            "{insert}"
        );
    }
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT id, c FROM ice.sales.mk").await,
        "+----+---+\n| id | c |\n+----+---+\n| 0  |   |\n+----+---+"
    );
}

#[tokio::test]
async fn a_map_operand_refuses_comparison_ordering_and_distinct_as_spark_does() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for sql in [
        "CREATE TABLE ice.sales.q (id INT, c MAP<STRING, INT>, d MAP<STRING, INT>, note STRING) \
         USING iceberg",
        "INSERT INTO ice.sales.q VALUES (0, map('a', 1), map('a', 1), 'n'), (1, map(), map(), 'n')",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let ordering = |resolved: &str, symbol: &str| {
        format!(
            "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve \"{resolved}\" due to data \
             type mismatch: The `{symbol}` does not support ordering on type \
             \"MAP<STRING, INT>\". SQLSTATE: 42K09"
        )
    };
    for (sql, expected) in [
        (
            "SELECT id FROM ice.sales.q WHERE c = map('a', 1)",
            ordering("(c = map(a, 1))", "="),
        ),
        (
            "SELECT id FROM ice.sales.q WHERE c = map()",
            ordering("(c = map())", "="),
        ),
        (
            "SELECT id FROM ice.sales.q WHERE c <> d",
            ordering("(c = d)", "="),
        ),
        (
            "SELECT id FROM ice.sales.q WHERE c < d",
            ordering("(c < d)", "<"),
        ),
        (
            "SELECT id FROM ice.sales.q WHERE c <=> d",
            ordering("(c <=> d)", "<=>"),
        ),
        (
            "SELECT id FROM ice.sales.q WHERE c IN (map('a', 1))",
            ordering("(c IN (map(a, 1)))", "in"),
        ),
        (
            "SELECT id FROM ice.sales.q ORDER BY c",
            ordering("c ASC NULLS FIRST", "sortorder"),
        ),
        (
            "SELECT id FROM ice.sales.q ORDER BY c DESC",
            ordering("c DESC NULLS LAST", "sortorder"),
        ),
        (
            "UPDATE ice.sales.q SET note = 'hit' WHERE c = map()",
            ordering("(c = map())", "="),
        ),
        (
            "DELETE FROM ice.sales.q WHERE c = d",
            ordering("(c = d)", "="),
        ),
        (
            "SELECT DISTINCT c FROM ice.sales.q",
            "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot \
             have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, \
             etc.), but the type of column `c` is \"MAP<STRING, INT>\". SQLSTATE: 0A000"
                .to_string(),
        ),
    ] {
        assert_eq!(refusal(&ctx, &catalogs, sql).await, expected, "{sql}");
    }
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, note FROM ice.sales.q ORDER BY id"
        )
        .await,
        "+----+------+\n| id | note |\n+----+------+\n| 0  | n    |\n| 1  | n    |\n+----+------+"
    );
}
