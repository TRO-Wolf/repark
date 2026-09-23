use super::super::*;
use super::common::*;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

type DescribeRow = (String, String, Option<String>);

const RESERVED_OWNER_PROPERTY_ERROR: &str = "[UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY] The feature is not supported: owner is a reserved table property, it will be set to the current user. SQLSTATE: 0A000";

async fn execute_statement(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} must plan: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} must execute: {error}"));
}

async fn describe_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<DescribeRow> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} must plan: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} must execute: {error}"));
    let mut rows = Vec::new();
    for batch in batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("DESCRIBE names must be strings");
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("DESCRIBE types must be strings");
        let comments = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("DESCRIBE comments must be strings");
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

async fn table_properties(catalogs: &CatalogRegistry, table_name: &str) -> HashMap<String, String> {
    let table_ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        table_name.to_string(),
    );
    catalogs["ice"]
        .load_table(&table_ident)
        .await
        .expect("table must load")
        .metadata()
        .properties()
        .clone()
}

async fn assert_table_owner(catalogs: &CatalogRegistry, table_name: &str, owner: &str) {
    let properties = table_properties(catalogs, table_name).await;
    assert_eq!(
        properties.get("owner").map(String::as_str),
        Some(owner),
        "{table_name} must retain the session owner in metadata"
    );
}

async fn create_unowned_table(catalogs: &CatalogRegistry, warehouse: &str, table_name: &str) {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("test schema must build");
    let location = format!("{warehouse}/sales/{table_name}");
    std::fs::create_dir_all(&location).expect("test table location must exist");
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            TableCreation::builder()
                .name(table_name.to_string())
                .location(location)
                .schema(schema)
                .build(),
        )
        .await
        .expect("direct catalog table must create");
}

fn identity_partition_rows() -> Vec<DescribeRow> {
    vec![
        ("id".to_string(), "bigint".to_string(), None),
        ("data".to_string(), "string".to_string(), None),
        (
            "# Partition Information".to_string(),
            String::new(),
            Some(String::new()),
        ),
        (
            "# col_name".to_string(),
            "data_type".to_string(),
            Some("comment".to_string()),
        ),
        ("data".to_string(), "string".to_string(), None),
    ]
}

#[tokio::test]
async fn identity_partition_describe_rows_match_spark_plain_and_extended() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let (ctx, catalogs) = setup_with_owner(&warehouse, "identity_owner").await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.identity (id BIGINT, data STRING) USING iceberg PARTITIONED BY (data)",
    )
    .await;

    let expected = identity_partition_rows();
    let plain = describe_rows(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.identity").await;
    assert_eq!(plain, expected);

    let extended = describe_rows(
        &ctx,
        &catalogs,
        "DESCRIBE TABLE EXTENDED ice.sales.identity",
    )
    .await;
    assert_eq!(&extended[..expected.len()], expected.as_slice());
    assert_eq!(
        &extended[expected.len()..expected.len() + 9],
        &[
            (String::new(), String::new(), Some(String::new())),
            (
                "# Metadata Columns".to_string(),
                String::new(),
                Some(String::new()),
            ),
            (
                "_spec_id".to_string(),
                "int".to_string(),
                Some(String::new())
            ),
            (
                "_partition".to_string(),
                "struct<data:string>".to_string(),
                Some(String::new()),
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
                Some(String::new()),
            ),
            (String::new(), String::new(), Some(String::new())),
            (
                "# Detailed Table Information".to_string(),
                String::new(),
                Some(String::new()),
            ),
        ]
    );
    let detail_names: Vec<&str> = extended[expected.len() + 9..]
        .iter()
        .map(|(name, _, _)| name.as_str())
        .collect();
    assert_eq!(
        detail_names,
        vec![
            "Name",
            "Type",
            "Location",
            "Provider",
            "Owner",
            "Table Properties",
            "Statistics",
        ]
    );
}

#[tokio::test]
async fn identity_partition_describe_rows_keep_spec_order() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let (ctx, catalogs) = setup(&warehouse).await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.two_identity (id BIGINT, data STRING, category STRING) USING iceberg PARTITIONED BY (data, category)",
    )
    .await;

    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE ice.sales.two_identity").await,
        vec![
            ("id".to_string(), "bigint".to_string(), None),
            ("data".to_string(), "string".to_string(), None),
            ("category".to_string(), "string".to_string(), None),
            (
                "# Partition Information".to_string(),
                String::new(),
                Some(String::new()),
            ),
            (
                "# col_name".to_string(),
                "data_type".to_string(),
                Some("comment".to_string()),
            ),
            ("data".to_string(), "string".to_string(), None),
            ("category".to_string(), "string".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn non_identity_and_unpartitioned_describe_sections_keep_existing_shapes() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let (ctx, catalogs) = setup(&warehouse).await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mixed (id BIGINT, data STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY (data, days(ts))",
    )
    .await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bucketed (id BIGINT) USING iceberg PARTITIONED BY (bucket(4, id))",
    )
    .await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.unpartitioned (id BIGINT, data STRING) USING iceberg",
    )
    .await;

    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE ice.sales.mixed").await,
        vec![
            ("id".to_string(), "bigint".to_string(), None),
            ("data".to_string(), "string".to_string(), None),
            ("ts".to_string(), "timestamp".to_string(), None),
            (String::new(), String::new(), Some(String::new())),
            (
                "# Partitioning".to_string(),
                String::new(),
                Some(String::new()),
            ),
            (
                "Part 0".to_string(),
                "data".to_string(),
                Some(String::new())
            ),
            (
                "Part 1".to_string(),
                "days(ts)".to_string(),
                Some(String::new()),
            ),
        ]
    );
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE ice.sales.bucketed").await,
        vec![
            ("id".to_string(), "bigint".to_string(), None),
            (String::new(), String::new(), Some(String::new())),
            (
                "# Partitioning".to_string(),
                String::new(),
                Some(String::new()),
            ),
            (
                "Part 0".to_string(),
                "bucket(4, id)".to_string(),
                Some(String::new()),
            ),
        ]
    );
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE ice.sales.unpartitioned").await,
        vec![
            ("id".to_string(), "bigint".to_string(), None),
            ("data".to_string(), "string".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn table_creation_paths_stamp_the_session_owner() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let owner = "table_creation_owner";
    let (ctx, catalogs) = setup_with_owner(&warehouse, owner).await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.created (id BIGINT) USING iceberg",
    )
    .await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ctas_created USING iceberg AS SELECT 1 AS id",
    )
    .await;
    create_unowned_table(
        &catalogs,
        warehouse
            .path()
            .to_str()
            .expect("warehouse path must be UTF-8"),
        "replace_schema",
    )
    .await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.replace_schema (id BIGINT) USING iceberg",
    )
    .await;
    create_unowned_table(
        &catalogs,
        warehouse
            .path()
            .to_str()
            .expect("warehouse path must be UTF-8"),
        "replace_ctas",
    )
    .await;
    execute_statement(
        &ctx,
        &catalogs,
        "REPLACE TABLE ice.sales.replace_ctas USING iceberg AS SELECT 2 AS id",
    )
    .await;

    for table_name in ["created", "ctas_created", "replace_schema", "replace_ctas"] {
        assert_table_owner(&catalogs, table_name, owner).await;
    }

    let extended =
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.created").await;
    let owner_row = extended
        .iter()
        .find(|(name, _, _)| name == "Owner")
        .expect("stamped owner must render as an extended row");
    assert_eq!(owner_row.1, owner);
    let properties_row = extended
        .iter()
        .find(|(name, _, _)| name == "Table Properties")
        .expect("extended output must render properties");
    assert!(!properties_row.1.contains("owner="));
}

#[tokio::test]
async fn create_and_ctas_refuse_the_reserved_owner_property_before_catalog_access() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let (ctx, catalogs) = setup(&warehouse).await;

    for sql in [
        "CREATE TABLE absent.sales.owner_create (id BIGINT) USING iceberg TBLPROPERTIES ('owner'='alice')",
        "CREATE TABLE absent.sales.owner_ctas USING iceberg TBLPROPERTIES ('owner'='alice') AS SELECT 1 AS id",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("reserved owner must refuse before catalog access");
        assert_eq!(error.to_string(), RESERVED_OWNER_PROPERTY_ERROR, "{sql}");
    }
}

#[tokio::test]
async fn create_keeps_case_variant_and_prefixed_owner_properties() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let owner = "near_miss_owner";
    let (ctx, catalogs) = setup_with_owner(&warehouse, owner).await;
    execute_statement(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.owner_near_miss (id BIGINT) USING iceberg TBLPROPERTIES ('Owner'='x', 'owner.x'='y')",
    )
    .await;

    let properties = table_properties(&catalogs, "owner_near_miss").await;
    assert_eq!(properties.get("Owner").map(String::as_str), Some("x"));
    assert_eq!(properties.get("owner.x").map(String::as_str), Some("y"));
    assert_eq!(properties.get("owner").map(String::as_str), Some(owner));
}

#[tokio::test]
async fn describe_extended_omits_owner_for_unstamped_catalog_tables() {
    let warehouse = TempDir::new().expect("warehouse must create");
    let (ctx, catalogs) = setup_with_owner(&warehouse, "describe_session_owner").await;
    create_unowned_table(
        &catalogs,
        warehouse
            .path()
            .to_str()
            .expect("warehouse path must be UTF-8"),
        "unowned",
    )
    .await;

    let properties = table_properties(&catalogs, "unowned").await;
    assert!(!properties.contains_key("owner"));
    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.unowned").await;
    assert!(rows.iter().all(|(name, _, _)| name != "Owner"));
}
