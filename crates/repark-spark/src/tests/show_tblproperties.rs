use super::super::*;
use super::common::*;

use datafusion::prelude::DataFrame;

async fn property_rows(frame: DataFrame) -> Vec<(String, String)> {
    let batches = frame.collect().await.expect("collect properties");
    assert_eq!(batches.len(), 1);
    let batch = batches.first().expect("one batch");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("key", &DataType::Utf8, false),
            ("value", &DataType::Utf8, false)
        ]
    );
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keys");
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("values");
    (0..batch.num_rows())
        .map(|index| {
            (
                keys.value(index).to_string(),
                values.value(index).to_string(),
            )
        })
        .collect()
}

async fn create_fresh_table(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
}

fn fresh_rows(snapshot: &str) -> Vec<(String, String)> {
    vec![
        ("current-snapshot-id".to_string(), snapshot.to_string()),
        ("format".to_string(), "iceberg/parquet".to_string()),
        ("format-version".to_string(), "2".to_string()),
        ("k".to_string(), "v".to_string()),
        (
            "write.parquet.compression-codec".to_string(),
            "zstd".to_string(),
        ),
    ]
}

async fn snapshot_id(catalogs: &CatalogRegistry, table: &str) -> String {
    catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            table.to_string(),
        ))
        .await
        .expect("load table")
        .metadata()
        .current_snapshot_id()
        .expect("snapshot id")
        .to_string()
}

#[tokio::test]
async fn fresh_table_answers_spark_rows() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        fresh_rows("none"),
        "pins: tblprops-1/C-001"
    );
}

#[tokio::test]
async fn owner_is_stored_but_never_a_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    let stored = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .expect("load table");
    assert!(
        stored.metadata().properties().contains_key("owner"),
        "pins: tblprops-1/C-005"
    );
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect("table must answer");
    let rows = property_rows(frame).await;
    assert!(
        rows.iter().all(|(key, _)| key != "owner"),
        "pins: tblprops-1/C-005"
    );
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t ('owner')")
        .await
        .expect("keyed table must answer");
    assert_eq!(
        property_rows(frame).await,
        vec![(
            "owner".to_string(),
            "Table ice.sales.t does not have property: owner".to_string()
        )],
        "pins: tblprops-1/C-005"
    );
}

#[tokio::test]
async fn comment_is_never_a_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.p (id INT, c STRING) USING iceberg PARTITIONED BY (c) \
         TBLPROPERTIES ('write.parquet.compression-codec'='zstd', 'comment'='hello')",
    )
    .await;
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.p")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        vec![
            ("current-snapshot-id".to_string(), "none".to_string()),
            ("format".to_string(), "iceberg/parquet".to_string()),
            ("format-version".to_string(), "2".to_string()),
            (
                "write.parquet.compression-codec".to_string(),
                "zstd".to_string()
            ),
        ],
        "pins: tblprops-1/C-005"
    );
    let frame = execute(
        &ctx,
        &catalogs,
        "SHOW TBLPROPERTIES ice.sales.p ('comment')",
    )
    .await
    .expect("keyed table must answer");
    assert_eq!(
        property_rows(frame).await,
        vec![(
            "comment".to_string(),
            "Table ice.sales.p does not have property: comment".to_string()
        )],
        "pins: tblprops-1/C-005"
    );
}

#[tokio::test]
async fn insert_moves_current_snapshot_id_to_the_decimal_id() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (1)").await;
    let id = snapshot_id(&catalogs, "t").await;
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        fresh_rows(&id),
        "pins: tblprops-1/C-002"
    );
}

#[tokio::test]
async fn set_tblproperties_updates_format_and_adds_rows() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (1)").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t SET TBLPROPERTIES ('k2'='v2', 'write.format.default'='orc')",
    )
    .await;
    let id = snapshot_id(&catalogs, "t").await;
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        vec![
            ("current-snapshot-id".to_string(), id),
            ("format".to_string(), "iceberg/orc".to_string()),
            ("format-version".to_string(), "2".to_string()),
            ("k".to_string(), "v".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("write.format.default".to_string(), "orc".to_string()),
            (
                "write.parquet.compression-codec".to_string(),
                "zstd".to_string()
            ),
        ],
        "pins: tblprops-1/C-003"
    );
}

#[tokio::test]
async fn v1_table_reports_format_version_once() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v1 (id INT) USING iceberg TBLPROPERTIES ('format-version'='1')",
    )
    .await;
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.v1")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        vec![
            ("current-snapshot-id".to_string(), "none".to_string()),
            ("format".to_string(), "iceberg/parquet".to_string()),
            ("format-version".to_string(), "1".to_string()),
            (
                "write.parquet.compression-codec".to_string(),
                "zstd".to_string()
            ),
        ],
        "pins: tblprops-1/C-004"
    );
}

#[tokio::test]
async fn stored_compression_codec_wins_over_the_default() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t SET TBLPROPERTIES ('write.parquet.compression-codec'='snappy')",
    )
    .await;
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        vec![
            ("current-snapshot-id".to_string(), "none".to_string()),
            ("format".to_string(), "iceberg/parquet".to_string()),
            ("format-version".to_string(), "2".to_string()),
            ("k".to_string(), "v".to_string()),
            (
                "write.parquet.compression-codec".to_string(),
                "snappy".to_string()
            ),
        ],
        "pins: tblprops-1/C-011"
    );
}

#[tokio::test]
async fn unset_compression_codec_falls_back_to_the_default() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t UNSET TBLPROPERTIES ('write.parquet.compression-codec')",
    )
    .await;
    let stored = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .expect("load table");
    assert!(
        !stored
            .metadata()
            .properties()
            .contains_key("write.parquet.compression-codec"),
        "pins: tblprops-1/C-011"
    );
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect("table must answer");
    assert_eq!(
        property_rows(frame).await,
        fresh_rows("none"),
        "pins: tblprops-1/C-011"
    );
}

#[tokio::test]
async fn keyed_lookup_hits_builtins_and_misses_loudly() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    for (key, value) in [
        ("k", "v"),
        ("format-version", "2"),
        ("current-snapshot-id", "none"),
        ("write.parquet.compression-codec", "zstd"),
    ] {
        let frame = execute(
            &ctx,
            &catalogs,
            &format!("SHOW TBLPROPERTIES ice.sales.t ('{key}')"),
        )
        .await
        .expect("keyed table must answer");
        assert_eq!(
            property_rows(frame).await,
            vec![(key.to_string(), value.to_string())],
            "pins: tblprops-1/C-006"
        );
    }
    for key in ["nope", "K"] {
        let frame = execute(
            &ctx,
            &catalogs,
            &format!("SHOW TBLPROPERTIES ice.sales.t ('{key}')"),
        )
        .await
        .expect("keyed table must answer");
        assert_eq!(
            property_rows(frame).await,
            vec![(
                key.to_string(),
                format!("Table ice.sales.t does not have property: {key}")
            )],
            "pins: tblprops-1/C-006"
        );
    }
}

#[tokio::test]
async fn two_part_and_one_part_names_answer_after_use() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    for sql in [
        "SHOW TBLPROPERTIES sales.t",
        "SHOW TBLPROPERTIES t",
        "SHOW TBLPROPERTIES ice.sales.t",
    ] {
        let frame = execute(&ctx, &catalogs, sql)
            .await
            .expect("table must answer");
        assert_eq!(
            property_rows(frame).await,
            fresh_rows("none"),
            "pins: tblprops-1/C-007"
        );
    }
}

#[tokio::test]
async fn temp_view_answers_the_empty_frame() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let frame = execute(&ctx, &catalogs, "SELECT 1 AS id")
        .await
        .expect("plan temp view");
    ctx.register_table("tv", frame.into_view())
        .expect("register temp view");
    let frame = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES tv")
        .await
        .expect("temp view must answer");
    assert!(
        property_rows(frame).await.is_empty(),
        "pins: tblprops-1/C-008"
    );
}

#[tokio::test]
async fn missing_table_keeps_the_not_found_refusal() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.nope")
        .await
        .expect_err("missing table must refuse");
    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`nope` cannot be found. Verify the spelling and correctness of the schema and catalog. If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01",
        "pins: tblprops-1/C-009"
    );
    assert!(
        !error.to_string().contains("information_schema"),
        "pins: tblprops-1/C-010"
    );
}

#[tokio::test]
async fn fallthrough_text_is_unreachable_on_every_arm() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    create_fresh_table(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    let frame = execute(&ctx, &catalogs, "SELECT 1 AS id")
        .await
        .expect("plan temp view");
    ctx.register_table("tv", frame.into_view())
        .expect("register temp view");
    for sql in [
        "SHOW TBLPROPERTIES ice.sales.t",
        "SHOW TBLPROPERTIES ice.sales.t ('k')",
        "SHOW TBLPROPERTIES ice.sales.v",
        "SHOW TBLPROPERTIES tv",
    ] {
        execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql} must answer: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("{sql} must collect: {error}"));
    }
    let error = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.nope")
        .await
        .expect_err("missing table must refuse");
    assert!(
        !error.to_string().contains("information_schema"),
        "pins: tblprops-1/C-010"
    );
}
