//! Partition-scoped INSERT OVERWRITE pins (DML-B).
use iceberg::spec::Operation;

use super::super::*;
use super::common::*;

/// pins: dml-b-insert-overwrite/C-002, C-004
#[tokio::test]
async fn empty_dynamic_partition_overwrite_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id) SELECT * FROM src WHERE false",
    )
    .await
    .expect_err("empty dynamic overwrite must refuse");
    assert!(
        error
            .to_string()
            .contains(repark_iceberg::write::EMPTY_DYNAMIC_OVERWRITE_NEEDLE),
        "got {error}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
        "empty dynamic overwrite must leave every row"
    );
}

/// pins: dml-b-insert-overwrite/C-002
#[tokio::test]
async fn dynamic_partition_overwrite_replaces_source_partitions_only() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id) SELECT 1 AS id, 'z' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "z".into()), (2, "b".into()), (3, "c".into())],
        "dynamic overwrite must keep partitions absent from the source"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("overwrite snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Overwrite);
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get("replace-partitions")
            .map(String::as_str),
        Some("true"),
        "dynamic overwrite stamps replace-partitions=true"
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_partition_overwrite_stamps_overwrite_operation() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let sibling_before: std::collections::HashSet<String> = live_data_file_paths(&catalogs, "t")
        .await
        .into_iter()
        .filter(|path| !path.contains("/id=2/"))
        .collect();
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 2) SELECT 'y' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "y".into()), (3, "c".into())],
    );
    let sibling_after: std::collections::HashSet<String> = live_data_file_paths(&catalogs, "t")
        .await
        .into_iter()
        .filter(|path| !path.contains("/id=2/"))
        .collect();
    assert_eq!(
        sibling_before, sibling_after,
        "static overwrite must leave sibling data-file paths unchanged"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("overwrite snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Overwrite);
    assert!(
        !snapshot
            .summary()
            .additional_properties
            .contains_key("replace-partitions"),
        "static overwrite must not stamp replace-partitions"
    );
}

/// pins: dml-b-insert-overwrite/C-001, C-004, C-005
#[tokio::test]
async fn empty_static_partition_overwrite_stamps_delete_operation() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(2, "b".into()), (3, "c".into())],
        "empty static overwrite must drop only the named partition"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("empty static snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    assert!(
        !snapshot
            .summary()
            .additional_properties
            .contains_key("replace-partitions"),
        "empty static overwrite must not stamp replace-partitions"
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_partition_overwrite_rejects_too_many_source_columns() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT 1 AS id, 'z' AS name",
    )
    .await
    .expect_err("Hive static PARTITION injects id; SELECT must not also supply it");
    assert!(
        error.to_string().contains("TOO_MANY_DATA_COLUMNS"),
        "got {error}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
        "arity mismatch must not mutate the table"
    );
}

fn register_two_key(ctx: &SessionContext, name: &str, rows: &[(i32, &str, &str)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("cat", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

async fn two_key_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, String, String)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, cat, payload FROM {table} ORDER BY id, cat"),
    )
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
            .downcast_ref::<Int32Array>()
            .unwrap();
        let cats = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let payloads = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                ids.value(index),
                cats.value(index).to_string(),
                payloads.value(index).to_string(),
            ));
        }
    }
    rows
}

fn register_nullable_id(ctx: &SessionContext, name: &str, rows: &[(Option<i32>, &str)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

async fn nullable_id_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(Option<i32>, String)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, name FROM {table} ORDER BY id NULLS FIRST"),
    )
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
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            let id = if ids.is_null(index) {
                None
            } else {
                Some(ids.value(index))
            };
            rows.push((id, names.value(index).to_string()));
        }
    }
    rows
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_two_key_partition_overwrite_replaces_only_the_tuple() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_two_key(
        &ctx,
        "two_key",
        &[(1, "west", "a"), (1, "east", "b"), (2, "west", "c")],
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.two USING iceberg PARTITIONED BY (id, cat) AS SELECT * FROM two_key",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.two PARTITION (id = 1, cat = 'west') SELECT 'z'",
    )
    .await;
    assert_eq!(
        two_key_rows(&ctx, &catalogs, "ice.sales.two").await,
        vec![
            (1, "east".into(), "b".into()),
            (1, "west".into(), "z".into()),
            (2, "west".into(), "c".into()),
        ],
        "two-key static overwrite must replace only (id=1, cat=west)"
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_incomplete_two_key_partition_replaces_all_k2_under_k1() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_two_key(
        &ctx,
        "two_key",
        &[(1, "west", "a"), (1, "east", "b"), (2, "west", "c")],
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.two USING iceberg PARTITIONED BY (id, cat) AS SELECT * FROM two_key",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.two PARTITION (id = 1) SELECT 'north' AS cat, 'z' AS payload",
    )
    .await;
    assert_eq!(
        two_key_rows(&ctx, &catalogs, "ice.sales.two").await,
        vec![
            (1, "north".into(), "z".into()),
            (2, "west".into(), "c".into()),
        ],
        "PARTITION (id=1) on a two-key spec replaces every cat under id=1"
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_string_partition_overwrite_keeps_siblings() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.s USING iceberg PARTITIONED BY (name) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.s PARTITION (name = 'a') SELECT CAST(9 AS INT) AS id",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.s").await,
        vec![(2, "b".into()), (3, "c".into()), (9, "a".into())],
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_null_partition_overwrite_keeps_siblings() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_nullable_id(
        &ctx,
        "nullable_id",
        &[(None, "n"), (Some(1), "a"), (Some(2), "b")],
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.n USING iceberg PARTITIONED BY (id) AS SELECT * FROM nullable_id",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.n PARTITION (id = NULL) SELECT 'z' AS name",
    )
    .await;
    assert_eq!(
        nullable_id_rows(&ctx, &catalogs, "ice.sales.n").await,
        vec![
            (None, "z".into()),
            (Some(1), "a".into()),
            (Some(2), "b".into()),
        ],
    );
}
