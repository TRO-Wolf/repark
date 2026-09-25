use super::super::*;
use super::accept_any_refusals::{refusal, sorted_rows};
use super::common::*;
use super::replace_where::{PARTITIONED, SEED, row, seeded};

const TWO_KEYS: &str = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) \
                        USING iceberg PARTITIONED BY (cat, data)";
const BY_ID: &str = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) \
                     USING iceberg PARTITIONED BY (id)";
const BUCKETED: &str = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) \
                        USING iceberg PARTITIONED BY (bucket(4, id))";

async fn latest(catalogs: &CatalogRegistry) -> (String, HashMap<String, String>) {
    let table = load_sales_table(catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("a current snapshot");
    (
        snapshot.summary().operation.as_str().to_string(),
        snapshot.summary().additional_properties.clone(),
    )
}

async fn snapshots(catalogs: &CatalogRegistry) -> usize {
    load_sales_table(catalogs, "t")
        .await
        .metadata()
        .snapshots()
        .count()
}

#[tokio::test]
async fn a_static_partition_value_fills_the_missing_column() {
    let statements = [
        "INSERT INTO ice.sales.t PARTITION (cat = 'q') SELECT 9, 'z'",
        "INSERT INTO ice.sales.t PARTITION (cat = 'q') VALUES (9, 'z')",
        "INSERT INTO TABLE ice.sales.t PARTITION (cat = 'q') SELECT 9, 'z'",
        "INSERT INTO ice.sales.t PARTITION (CAT = 'q') SELECT 9, 'z'",
        "INSERT INTO ice.sales.t PARTITION (cat) SELECT 9, 'z', 'q'",
        "INSERT INTO ice.sales.t PARTITION (cat) VALUES (9, 'z', 'q')",
        "INSERT INTO ice.sales.t PARTITION (cat = 'q') (id, data) SELECT 9, 'z'",
        "INSERT INTO ice.sales.t PARTITION (cat = 'q') (data, id) SELECT 'z', 9",
        "INSERT INTO ice.sales.t PARTITION (cat = 'q') (data, id) VALUES ('z', 9)",
    ];
    for sql in statements {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            sorted_rows(&ctx, &catalogs).await,
            vec![
                row(1, "a", "x"),
                row(2, "b", "y"),
                row(3, "c", "x"),
                row(9, "z", "q")
            ],
            "{sql}"
        );
        let (operation, summary) = latest(&catalogs).await;
        assert_eq!(operation, "append", "{sql}");
        assert_eq!(
            summary.get("added-records").map(String::as_str),
            Some("1"),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn two_key_specs_take_static_and_dynamic_values_in_any_order() {
    let statements = [
        "INSERT INTO ice.sales.t PARTITION (cat = 'q', data = 'w') SELECT 9",
        "INSERT INTO ice.sales.t PARTITION (data = 'w', cat = 'q') SELECT 9",
        "INSERT INTO ice.sales.t PARTITION (cat = 'q', data) SELECT 9, 'w'",
        "INSERT INTO ice.sales.t PARTITION (cat, data = 'w') SELECT 9, 'q'",
        "INSERT INTO ice.sales.t PARTITION (data = 'w', cat) SELECT 9, 'q'",
    ];
    for sql in statements {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, TWO_KEYS).await;
        run(&ctx, &catalogs, sql).await;
        assert!(
            sorted_rows(&ctx, &catalogs)
                .await
                .contains(&row(9, "w", "q")),
            "{sql}"
        );
        assert_eq!(snapshots(&catalogs).await, 2, "{sql}");
    }
}

#[tokio::test]
async fn static_values_follow_spark_string_casts() {
    let cases = [
        (
            BY_ID,
            "INSERT INTO ice.sales.t PARTITION (id = '7') SELECT 'z', 'q'",
            row(7, "z", "q"),
        ),
        (
            BY_ID,
            "INSERT INTO ice.sales.t PARTITION (id = 7) SELECT 'z', 'q'",
            row(7, "z", "q"),
        ),
        (
            BY_ID,
            "INSERT INTO ice.sales.t PARTITION (id = '7') VALUES ('z', 'q')",
            row(7, "z", "q"),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (cat = 5) SELECT 9, 'z'",
            row(9, "z", "5"),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (cat = true) SELECT 9, 'z'",
            row(9, "z", "true"),
        ),
    ];
    for (create, sql, expected) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, create).await;
        run(&ctx, &catalogs, sql).await;
        assert!(
            sorted_rows(&ctx, &catalogs).await.contains(&expected),
            "{sql}"
        );
    }
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t PARTITION (cat = NULL) SELECT 9, 'z'",
    )
    .await;
    let nulls = execute(
        &ctx,
        &catalogs,
        "SELECT count(*) FROM ice.sales.t WHERE cat IS NULL AND id = 9",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let count = nulls[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn a_partition_insert_writes_to_a_named_branch_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b1").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b1 PARTITION (cat = 'q') SELECT 9, 'z'",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![row(1, "a", "x"), row(2, "b", "y"), row(3, "c", "x")]
    );
    let table = load_sales_table(&catalogs, "t").await;
    let branch = table
        .metadata()
        .snapshot_for_ref("b1")
        .expect("the branch head");
    assert_eq!(
        branch
            .summary()
            .additional_properties
            .get("total-records")
            .map(String::as_str),
        Some("4")
    );
}

#[tokio::test]
async fn partition_clause_refusals_carry_spark_text_and_commit_nothing() {
    let not_partition = |name: &str| {
        format!(
            "Error during planning: [NON_PARTITION_COLUMN] PARTITION clause cannot contain the \
             non-partition column: `{name}`. SQLSTATE: 42000"
        )
    };
    let cast = |value: &str, to: &str| {
        format!(
            "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
             \"{to}\" because it is malformed. Correct the value as per the syntax, or change its \
             target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
             SQLSTATE: 22018"
        )
    };
    let cases = [
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (data = 'w') SELECT 9, 'q'",
            not_partition("data"),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (nope = 'w') SELECT 9, 'z'",
            not_partition("nope"),
        ),
        (
            "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) USING iceberg",
            "INSERT INTO ice.sales.t PARTITION (cat = 'q') SELECT 9, 'z'",
            not_partition("cat"),
        ),
        (
            BUCKETED,
            "INSERT INTO ice.sales.t PARTITION (id = 5) SELECT 'z', 'q'",
            not_partition("id"),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (cat = 'q', cat = 'r') SELECT 9, 'z'",
            "[DUPLICATE_KEY] Found duplicate keys `cat`. SQLSTATE: 23505".to_string(),
        ),
        (
            PARTITIONED,
            "INSERT OVERWRITE ice.sales.t PARTITION (cat = 'q', cat = 'r') SELECT 9, 'z'",
            "[DUPLICATE_KEY] Found duplicate keys `cat`. SQLSTATE: 23505".to_string(),
        ),
        (
            BY_ID,
            "INSERT INTO ice.sales.t PARTITION (id = 'abc') SELECT 'z', 'q'",
            cast("abc", "BIGINT"),
        ),
        (
            BY_ID,
            "INSERT INTO ice.sales.t PARTITION (id = 7.5) SELECT 'z', 'q'",
            cast("7.5", "BIGINT"),
        ),
        (
            BY_ID,
            "INSERT OVERWRITE ice.sales.t PARTITION (id = 'abc') SELECT 'z', 'q'",
            cast("abc", "BIGINT"),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (cat = 'q') SELECT 9",
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot \
             write to `ice`.`sales`.`t`, the reason is not enough data columns:\nTable columns: \
             `id`, `data`, `cat`.\nData columns: `9`, `cat`. SQLSTATE: 21S01"
                .to_string(),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (cat = 'q') SELECT 9, 'z', 'q'",
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot \
             write to `ice`.`sales`.`t`, the reason is too many data columns:\nTable columns: \
             `id`, `data`, `cat`.\nData columns: `9`, `z`, `cat`, `q`. SQLSTATE: 21S01"
                .to_string(),
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t PARTITION (cat = 'q') (id, cat) SELECT 9, 'z'",
            "Error during planning: [STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST] Static \
             partition column cat is also specified in the column list. SQLSTATE: 42713"
                .to_string(),
        ),
    ];
    for (create, sql, text) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, create).await;
        let mapped = refusal(&ctx, &catalogs, sql).await;
        assert_eq!(mapped.to_string(), text, "{sql}");
        assert_eq!(snapshots(&catalogs).await, 1, "{sql} must not commit");
    }
}

#[tokio::test]
async fn a_bad_static_value_refuses_as_an_illegal_argument() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, BY_ID).await;
    let mapped = refusal(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t PARTITION (id = 'abc') SELECT 'z', 'q'",
    )
    .await;
    assert!(
        matches!(mapped, repark_common::Error::IllegalArgument(_)),
        "{mapped:?}"
    );
}

async fn bucketed_with_source(wh: &TempDir, create: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    run(&ctx, &catalogs, create).await;
    run(&ctx, &catalogs, SEED).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.src (id BIGINT) USING iceberg",
    )
    .await;
    let values = (0..20)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(", ");
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.src VALUES {values}"),
    )
    .await;
    (ctx, catalogs)
}

#[tokio::test]
async fn a_cast_that_keeps_its_input_name_no_longer_collides_in_a_positional_source() {
    let cases = [
        (
            BUCKETED,
            "INSERT INTO ice.sales.t SELECT id, CAST(id AS STRING), 'k' FROM ice.sales.src",
            "append",
            23,
        ),
        (
            BUCKETED,
            "INSERT OVERWRITE ice.sales.t SELECT id, CAST(id AS STRING), 'k' FROM ice.sales.src",
            "overwrite",
            20,
        ),
        (
            PARTITIONED,
            "INSERT INTO ice.sales.t SELECT id, CAST(id AS STRING), 'k' FROM ice.sales.src",
            "append",
            23,
        ),
        (
            BUCKETED,
            "INSERT INTO ice.sales.t SELECT id, CAST(id AS STRING), 'k' FROM ice.sales.src \
             UNION ALL SELECT id, CAST(id AS STRING), 'k' FROM ice.sales.src WHERE false",
            "append",
            23,
        ),
        (
            BUCKETED,
            "INSERT INTO ice.sales.t SELECT id, id || '', 'k' AS id FROM ice.sales.src",
            "append",
            23,
        ),
    ];
    for (create, sql, operation, total) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = bucketed_with_source(&wh, create).await;
        run(&ctx, &catalogs, sql).await;
        let (actual, summary) = latest(&catalogs).await;
        assert_eq!(actual, operation, "{sql}");
        assert_eq!(
            summary.get("total-records").map(String::as_str),
            Some(total.to_string().as_str()),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn a_bucketed_insert_writes_one_file_per_bucket() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = bucketed_with_source(&wh, BUCKETED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT id, CAST(id AS STRING), 'k' FROM ice.sales.src",
    )
    .await;
    let (_, summary) = latest(&catalogs).await;
    assert_eq!(
        summary.get("added-data-files").map(String::as_str),
        Some("4")
    );
    assert_eq!(
        summary.get("changed-partition-count").map(String::as_str),
        Some("4")
    );
}

#[tokio::test]
async fn the_source_rename_leaves_by_name_inserts_alone() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    let mapped = refusal(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME SELECT 9 AS id, 'z' AS data, 'q' AS data",
    )
    .await;
    assert!(
        mapped.to_string().contains("AMBIGUOUS_COLUMN_NAME"),
        "{mapped}"
    );
}

#[tokio::test]
async fn a_dynamic_only_clause_keeps_the_spark_arity_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    let mapped = refusal(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t PARTITION (cat) SELECT 9, 'z'",
    )
    .await;
    assert_eq!(
        mapped.to_string(),
        "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot \
         write to `ice`.`sales`.`t`, the reason is not enough data columns:\nTable columns: \
         `id`, `data`, `cat`.\nData columns: `9`, `z`. SQLSTATE: 21S01"
    );
    assert_eq!(snapshots(&catalogs).await, 1);
}

#[tokio::test]
async fn a_partition_insert_on_an_accept_any_table_resolves_by_name() {
    let accept_any = "ALTER TABLE ice.sales.t SET TBLPROPERTIES \
                      ('write.spark.accept-any-schema'='true')";
    let refusals = [
        (
            "INSERT INTO ice.sales.t PARTITION (cat = 'x') SELECT 9, 'z'",
            "Field 9 not found in source schema",
        ),
        (
            "INSERT INTO ice.sales.t PARTITION (cat = 'x') VALUES (9, 'z')",
            "Field col1 not found in source schema",
        ),
        (
            "INSERT INTO ice.sales.t PARTITION (cat = 'x') SELECT 9 AS id, 'z' AS data, 1 AS extra",
            "Field extra not found in source schema",
        ),
    ];
    for (sql, text) in refusals {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
        run(&ctx, &catalogs, accept_any).await;
        let mapped = refusal(&ctx, &catalogs, sql).await;
        assert!(
            matches!(mapped, repark_common::Error::IllegalArgument(ref message) if message == text),
            "{sql}: {mapped:?}"
        );
        assert_eq!(snapshots(&catalogs).await, 1, "{sql} must not commit");
    }
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(&ctx, &catalogs, accept_any).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t PARTITION (cat = 'x') SELECT 9 AS id, 'z' AS data",
    )
    .await;
    assert!(
        sorted_rows(&ctx, &catalogs)
            .await
            .contains(&row(9, "z", "x"))
    );
}
