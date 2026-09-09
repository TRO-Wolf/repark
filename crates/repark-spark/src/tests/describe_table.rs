use super::super::*;
use super::common::*;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};

async fn create_step_one_table(catalogs: &CatalogRegistry, warehouse: &str) {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            std::sync::Arc::new(
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long))
                    .with_doc("the row identifier"),
            ),
            std::sync::Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            std::sync::Arc::new(NestedField::optional(
                3,
                "ts",
                Type::Primitive(PrimitiveType::Timestamptz),
            )),
        ])
        .build()
        .unwrap();
    let partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(3, "ts_day", Transform::Day)
        .unwrap()
        .build();
    let location = format!("{warehouse}/sales/t1");
    std::fs::create_dir_all(&location).unwrap();
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            iceberg::TableCreation::builder()
                .name("t1".to_string())
                .location(location)
                .schema(schema)
                .partition_spec(partition_spec)
                .properties(HashMap::from([("k".to_string(), "v".to_string())]))
                .build(),
        )
        .await
        .unwrap();
}

async fn describe_rows(
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
async fn describe_table_parser_accepts_plain_and_extended_forms() {
    for sql in [
        "DESCRIBE ice.sales.t1",
        "DESCRIBE TABLE ice.sales.t1",
        "DESC ice.sales.t1",
        "DESC TABLE ice.sales.t1",
    ] {
        let parsed = crate::describe_show::try_parse_describe_table(sql)
            .expect("a plain describe must parse")
            .expect("a plain describe must not error");
        assert_eq!(parsed.catalog, "ice");
        assert_eq!(parsed.namespace, "sales");
        assert_eq!(parsed.table, "t1");
        assert!(!parsed.extended);
    }
    for sql in [
        "DESCRIBE EXTENDED ice.sales.t1",
        "DESCRIBE TABLE EXTENDED ice.sales.t1",
        "DESC EXTENDED ice.sales.t1",
        "DESCRIBE FORMATTED ice.sales.t1",
        "DESCRIBE TABLE FORMATTED ice.sales.t1",
        "DESC TABLE FORMATTED ice.sales.t1",
    ] {
        let parsed = crate::describe_show::try_parse_describe_table(sql)
            .expect("an extended describe must parse")
            .expect("an extended describe must not error");
        assert_eq!(parsed.catalog, "ice");
        assert_eq!(parsed.namespace, "sales");
        assert_eq!(parsed.table, "t1");
        assert!(parsed.extended, "{sql} must set the extended flag");
    }
}

#[tokio::test]
async fn describe_table_parser_leaves_non_table_forms_alone() {
    for sql in [
        "DESCRIBE EXTENDED",
        "DESCRIBE TABLE EXTENDED",
        "DESCRIBE NAMESPACE ice.sales",
        "DESCRIBE DATABASE ice.sales",
        "DESC SCHEMA ice.sales",
        "DESCRIBE src",
        "DESCRIBE ice.sales.t1.snapshots",
        "DESCRIBE ice.sales.t1 extra",
        "SELECT 1",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_none(),
            "{sql} must not take the describe-table path"
        );
    }
}

#[tokio::test]
async fn describe_table_plain_matches_step_one_capture_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_step_one_table(&catalogs, wh.path().to_str().unwrap()).await;

    let frame = execute(&ctx, &catalogs, "DESCRIBE ice.sales.t1")
        .await
        .unwrap();
    let schema = frame.schema();
    let fields: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(fields, vec!["col_name", "data_type", "comment"]);
    assert!(!schema.field(0).is_nullable());
    assert!(!schema.field(1).is_nullable());
    assert!(schema.field(2).is_nullable());

    let plain = describe_rows(&ctx, &catalogs, "DESCRIBE ice.sales.t1").await;
    assert_eq!(
        plain,
        vec![
            (
                "id".to_string(),
                "bigint".to_string(),
                Some("the row identifier".to_string())
            ),
            ("name".to_string(), "string".to_string(), None),
            ("ts".to_string(), "timestamp".to_string(), None),
            (String::new(), String::new(), Some(String::new())),
            (
                "# Partitioning".to_string(),
                String::new(),
                Some(String::new())
            ),
            (
                "Part 0".to_string(),
                "days(ts)".to_string(),
                Some(String::new())
            ),
        ]
    );
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.t1").await,
        plain
    );
}

#[tokio::test]
async fn describe_table_extended_emits_metadata_and_detail_sections() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let warehouse = wh.path().to_str().unwrap().to_string();
    create_step_one_table(&catalogs, &warehouse).await;

    let rows = describe_rows(&ctx, &catalogs, "DESC EXTENDED ice.sales.t1").await;
    assert_eq!(rows.len(), 22);
    assert_eq!(
        rows[6..13],
        vec![
            (String::new(), String::new(), Some(String::new())),
            (
                "# Metadata Columns".to_string(),
                String::new(),
                Some(String::new())
            ),
            (
                "_spec_id".to_string(),
                "int".to_string(),
                Some(String::new())
            ),
            (
                "_partition".to_string(),
                "struct<ts_day:date>".to_string(),
                Some(String::new())
            ),
            (
                "_file".to_string(),
                "string".to_string(),
                Some(String::new())
            ),
            (
                "_pos".to_string(),
                "bigint".to_string(),
                Some(String::new())
            ),
            (
                "_deleted".to_string(),
                "boolean".to_string(),
                Some(String::new())
            ),
        ]
    );
    assert_eq!(
        rows[13..18],
        vec![
            (String::new(), String::new(), Some(String::new())),
            (
                "# Detailed Table Information".to_string(),
                String::new(),
                Some(String::new())
            ),
            (
                "Name".to_string(),
                "ice.sales.t1".to_string(),
                Some(String::new())
            ),
            (
                "Type".to_string(),
                "MANAGED".to_string(),
                Some(String::new())
            ),
            (
                "Location".to_string(),
                format!("{warehouse}/sales/t1"),
                Some(String::new())
            ),
        ]
    );
    assert_eq!(
        rows[18],
        (
            "Provider".to_string(),
            "iceberg".to_string(),
            Some(String::new())
        )
    );
    let (owner_name, owner_value, owner_comment) = rows[19].clone();
    assert_eq!(owner_name, "Owner");
    assert!(!owner_value.is_empty());
    assert_eq!(owner_comment, Some(String::new()));
    let (props_name, props_value, props_comment) = rows[20].clone();
    assert_eq!(props_name, "Table Properties");
    assert!(props_value.starts_with('[') && props_value.ends_with(']'));
    assert!(props_value.contains("k=v"));
    assert!(props_value.contains("current-snapshot-id=none"));
    assert_eq!(props_comment, Some(String::new()));
    assert_eq!(
        rows[21],
        (
            "Statistics".to_string(),
            "0 bytes, 0 rows".to_string(),
            None
        )
    );
}

#[tokio::test]
async fn describe_table_formatted_is_byte_identical_to_extended() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_step_one_table(&catalogs, wh.path().to_str().unwrap()).await;

    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE FORMATTED ice.sales.t1").await,
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.t1").await
    );
}

#[tokio::test]
async fn describe_table_missing_table_raises_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.no_such_table")
        .await
        .expect_err("a missing table must fail loud");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "Plan is the variant repark-core classifies Analysis, got: {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("[TABLE_OR_VIEW_NOT_FOUND]")
            && message.contains("`ice`.`sales`.`no_such_table`"),
        "the message must carry Spark's condition and the qualified name, got: {message}"
    );
}

#[tokio::test]
async fn describe_table_temp_view_falls_through_to_datafusion() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let frame = execute(&ctx, &catalogs, "DESCRIBE src")
        .await
        .unwrap_or_else(|error| panic!("DESCRIBE src must still describe the view: {error}"));
    assert_ne!(frame.schema().field(0).name(), "col_name");
    assert_ne!(frame.schema().field(0).name(), "info_name");
    let rows: usize = frame
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn describe_table_unregistered_catalog_falls_through_unchanged() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(&ctx, &catalogs, "DESCRIBE nosuch.sales.t1")
        .await
        .expect_err("an unregistered catalog must fail outside the describe path");
    assert!(
        !error.to_string().contains("[TABLE_OR_VIEW_NOT_FOUND]"),
        "a non-catalog name must not take the describe-table path, got: {error}"
    );
}

#[tokio::test]
async fn describe_table_properties_redact_secrets() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let warehouse = wh.path().to_str().unwrap().to_string();
    let location = format!("{warehouse}/sales/creds");
    std::fs::create_dir_all(&location).unwrap();
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![std::sync::Arc::new(NestedField::optional(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            iceberg::TableCreation::builder()
                .name("creds".to_string())
                .location(location)
                .schema(schema)
                .properties(HashMap::from([(
                    "secret_token".to_string(),
                    "hunter2".to_string(),
                )]))
                .build(),
        )
        .await
        .unwrap();

    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.creds").await;
    let properties = rows
        .iter()
        .find(|(name, _, _)| name == "Table Properties")
        .expect("extended output carries Table Properties");
    assert!(
        properties.1.contains("*********(redacted)"),
        "the secret value must be redacted, got: {}",
        properties.1
    );
    assert!(
        !properties.1.contains("hunter2"),
        "the plaintext secret must never reach output, got: {}",
        properties.1
    );
}
