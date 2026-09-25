use super::super::*;
use super::accept_any_refusals::assert_illegal_argument;
use super::common::*;
use tempfile::TempDir;

fn metadata_json(table: &iceberg::table::Table) -> serde_json::Value {
    let location = table.metadata_location().unwrap();
    let path = location.strip_prefix("file://").unwrap_or(location);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[tokio::test]
async fn create_format_version_one_writes_v1_metadata_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v1 (id BIGINT, data STRING, cat STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='1')",
    )
    .await;
    let empty = metadata_json(&load_sales_table(&catalogs, "v1").await);
    assert_eq!(empty["format-version"], 1);
    assert!(empty.get("last-sequence-number").is_none());
    assert!(empty.get("schema").is_some() && empty.get("partition-spec").is_some());
    assert_eq!(empty["current-snapshot-id"], serde_json::json!(null));

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.v1 VALUES (0,'d0','a'),(1,'d1','b'),(2,'d2','a')",
    )
    .await;
    let table = load_sales_table(&catalogs, "v1").await;
    let seeded = metadata_json(&table);
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();
    assert!(seeded.get("last-sequence-number").is_none());
    assert!(seeded["snapshots"][0].get("sequence-number").is_none());
    assert_eq!(seeded["snapshots"][0]["summary"]["operation"], "append");
    assert_eq!(seeded["snapshot-log"][0]["snapshot-id"], snapshot_id);
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("main")
            .map(|snapshot| snapshot.snapshot_id()),
        Some(snapshot_id)
    );
    assert!(!table.metadata().properties().contains_key("format-version"));
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.v1").await, 3);

    run(&ctx, &catalogs, "DELETE FROM ice.sales.v1 WHERE id = 1").await;
    let deleted = metadata_json(&load_sales_table(&catalogs, "v1").await);
    let summary = &deleted["snapshots"][1]["summary"];
    assert_eq!(summary["operation"], "overwrite");
    assert_eq!(summary["total-delete-files"], "0");
    assert_eq!(summary["total-records"], "2");
    assert!(deleted.get("last-sequence-number").is_none());
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.v1").await, 2);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.v1 WHERE id = 1").await,
        0
    );
}

#[tokio::test]
async fn create_format_version_one_ctas_and_zero_padded_value_write_v1() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.c1 USING iceberg TBLPROPERTIES ('format-version'='1') \
         AS SELECT 1 AS id",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.z1 (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='01')",
    )
    .await;
    for name in ["c1", "z1"] {
        let table = load_sales_table(&catalogs, name).await;
        assert_eq!(
            table.metadata().format_version(),
            iceberg::spec::FormatVersion::V1
        );
    }
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.c1").await, 1);
}

#[tokio::test]
async fn create_format_version_refusals_match_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let create = |version: &str| {
        format!(
            "CREATE TABLE ice.sales.bad (id BIGINT) USING iceberg \
             TBLPROPERTIES ('format-version'='{version}')"
        )
    };
    assert_illegal_argument(
        &ctx,
        &catalogs,
        &create("5"),
        "Unsupported format version: v5 (supported: v4)",
    )
    .await;
    assert_illegal_argument(&ctx, &catalogs, &create("abc"), "For input string: \"abc\"").await;
    for unwritable in ["0", "-1", "4"] {
        let error = execute(&ctx, &catalogs, &create(unwritable))
            .await
            .expect_err(unwritable);
        assert_eq!(
            error.to_string(),
            format!(
                "This feature is not implemented: TBLPROPERTIES 'format-version' = \
                 '{unwritable}' is not supported (tables are created as Iceberg format v1, v2 or \
                 v3)"
            )
        );
    }
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v2 (id BIGINT) USING iceberg",
    )
    .await;
    let downgrade = execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.v2 (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version'='1')",
    )
    .await
    .expect_err("downgrade");
    assert_eq!(
        downgrade.to_string(),
        "External error: DataInvalid => Cannot downgrade FormatVersion from v2 to v1"
    );
    let catalog = catalog_handle(&catalogs, "ice").unwrap();
    assert!(
        !catalog
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".into()),
                "bad".into()
            ))
            .await
            .unwrap()
    );
}
