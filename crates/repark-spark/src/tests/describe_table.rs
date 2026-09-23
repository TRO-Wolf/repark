use super::super::*;
use super::common::*;

use iceberg::spec::{
    NestedField, PrimitiveType, Schema, StructType, Transform, Type, UnboundPartitionSpec,
};

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

async fn create_describe_column_table(catalogs: &CatalogRegistry, warehouse: &str) {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            std::sync::Arc::new(
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).with_doc("c"),
            ),
            std::sync::Arc::new(NestedField::optional(
                2,
                "data",
                Type::Primitive(PrimitiveType::String),
            )),
            std::sync::Arc::new(NestedField::optional(
                3,
                "st",
                Type::Struct(StructType::new(vec![
                    std::sync::Arc::new(NestedField::optional(
                        4,
                        "a",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    std::sync::Arc::new(NestedField::optional(
                        5,
                        "b",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])),
            )),
        ])
        .build()
        .unwrap();
    let location = format!("{warehouse}/sales/dc");
    std::fs::create_dir_all(&location).unwrap();
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            iceberg::TableCreation::builder()
                .name("dc".to_string())
                .location(location)
                .schema(schema)
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

async fn describe_column_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(String, String)> {
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                names.value(index).to_string(),
                values.value(index).to_string(),
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
async fn describe_table_parser_accepts_short_names_for_session_completion() {
    let one = crate::describe_show::try_parse_describe_table("DESCRIBE TABLE desc_demo")
        .expect("a one-part describe must parse")
        .expect("a one-part describe must not error");
    assert_eq!(one.table, "desc_demo");
    assert!(one.catalog.is_empty());
    assert!(one.namespace.is_empty());
    assert!(!one.extended);
    let two =
        crate::describe_show::try_parse_describe_table("DESCRIBE TABLE EXTENDED sales.desc_demo")
            .expect("a two-part describe must parse")
            .expect("a two-part describe must not error");
    assert!(two.catalog.is_empty());
    assert_eq!(two.namespace, "sales");
    assert_eq!(two.table, "desc_demo");
    assert!(two.extended);
}

#[test]
fn describe_table_parser_accepts_one_column_tail() {
    for sql in [
        "DESCRIBE TABLE ice.sales.dc id",
        "DESCRIBE ice.sales.dc data",
        "DESCRIBE TABLE EXTENDED ice.sales.dc id",
        "DESCRIBE FORMATTED ice.sales.dc id",
        "DESCRIBE ice.sales.dc ID",
        "DESCRIBE ice.sales.dc `id`;",
        "DESCRIBE ice.sales.dc st.a",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_some(),
            "{sql} must take the describe-table path"
        );
    }
}

#[test]
fn describe_table_parser_refuses_time_travel_tails() {
    for (sql, near) in [
        ("DESCRIBE ice.sales.dc VERSION AS OF 1", "OF"),
        ("DESCRIBE TABLE ice.sales.dc VERSION AS OF 1", "OF"),
        (
            "DESCRIBE ice.sales.dc TIMESTAMP AS OF '2030-01-01 00:00:00'",
            "OF",
        ),
        ("DESCRIBE ice.sales.dc FOR VERSION AS OF 1", "VERSION"),
    ] {
        let parsed = crate::describe_show::try_parse_describe_table(sql)
            .expect("the time-travel tail must take the describe-table path");
        let Err(error) = parsed else {
            panic!("the time-travel tail must refuse");
        };
        assert!(
            error.to_string().ends_with(&format!(
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"
            )),
            "{error}"
        );
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
        "DESCRIBE ice.sales.t1.snapshots",
        "DESCRIBE ice.sales.t1 col extra",
        "DESCRIBE ice.sales.t1 AS JSON",
        "DESCRIBE FUNCTION upper",
        "DESCRIBE QUERY SELECT 1",
        "SELECT 1",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_none(),
            "{sql} must not take the describe-table path"
        );
    }
    assert!(
        crate::describe_show::try_parse_describe_as_json("DESCRIBE ice.sales.t1 AS JSON").is_some()
    );
}

#[tokio::test]
async fn describe_table_column_matches_spark_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_describe_column_table(&catalogs, wh.path().to_str().unwrap()).await;

    let frame = execute(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.dc id")
        .await
        .unwrap();
    let schema = frame.schema();
    let fields: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(fields, vec!["info_name", "info_value"]);
    assert!(schema.fields().iter().all(|field| !field.is_nullable()));

    let id_rows = vec![
        ("col_name".to_string(), "id".to_string()),
        ("data_type".to_string(), "bigint".to_string()),
        ("comment".to_string(), "c".to_string()),
    ];
    for sql in [
        "DESCRIBE TABLE ice.sales.dc id",
        "DESCRIBE ice.sales.dc id",
        "DESCRIBE TABLE EXTENDED ice.sales.dc id",
        "DESCRIBE FORMATTED ice.sales.dc id",
        "DESCRIBE ice.sales.dc `id`",
    ] {
        assert_eq!(
            describe_column_rows(&ctx, &catalogs, sql).await,
            id_rows,
            "{sql}"
        );
    }
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.dc ID").await,
        vec![
            ("col_name".to_string(), "ID".to_string()),
            ("data_type".to_string(), "bigint".to_string()),
            ("comment".to_string(), "c".to_string()),
        ]
    );
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.dc data").await,
        vec![
            ("col_name".to_string(), "data".to_string()),
            ("data_type".to_string(), "string".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.dc st").await,
        vec![
            ("col_name".to_string(), "st".to_string()),
            (
                "data_type".to_string(),
                "struct<a:int,b:string>".to_string(),
            ),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn describe_table_column_refusals_match_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_describe_column_table(&catalogs, wh.path().to_str().unwrap()).await;

    let nested = execute(&ctx, &catalogs, "DESCRIBE ice.sales.dc st.a")
        .await
        .expect_err("a nested describe column must refuse");
    assert_eq!(
        nested.to_string(),
        "[_LEGACY_ERROR_TEMP_1060] DESC TABLE COLUMN does not support nested column: st.a."
    );
    let missing = execute(&ctx, &catalogs, "DESCRIBE ice.sales.dc nope")
        .await
        .expect_err("a missing describe column must refuse");
    assert_eq!(
        missing.to_string(),
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
         name `nope` cannot be resolved. Did you mean one of the following? [`id`, `st`, `data`]. \
         SQLSTATE: 42703"
    );
    for (sql, near) in [
        ("DESCRIBE ice.sales.dc VERSION AS OF 1", "OF"),
        ("DESCRIBE TABLE ice.sales.dc VERSION AS OF 1", "OF"),
        (
            "DESCRIBE ice.sales.dc TIMESTAMP AS OF '2030-01-01 00:00:00'",
            "OF",
        ),
        ("DESCRIBE ice.sales.dc FOR VERSION AS OF 1", "VERSION"),
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a describe time-travel tail must refuse");
        assert_eq!(
            error.to_string(),
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601")
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
async fn describe_table_extended_describes_a_real_table_named_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let warehouse = wh.path().to_str().unwrap().to_string();
    let location = format!("{warehouse}/sales/files");
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
                .name("files".to_string())
                .location(location)
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();

    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.files").await;
    assert_eq!(
        rows[0],
        ("id".to_string(), "bigint".to_string(), None),
        "a real table named files takes the describe-table path"
    );
    assert!(
        rows.iter()
            .any(|(name, value, _)| name == "Name" && value == "ice.sales.files"),
        "extended output names the three-part table, got: {rows:?}"
    );
    let (owner_name, owner_value, _) = rows
        .iter()
        .find(|(name, _, _)| name == "Owner")
        .expect("extended output carries Owner");
    assert_eq!(owner_name, "Owner");
    assert!(!owner_value.is_empty());
}

#[tokio::test]
async fn describe_table_owner_is_the_session_resolved_user() {
    let first = TempDir::new().unwrap();
    let (first_ctx, first_catalogs) =
        setup_with_owner(&first, "review_fix_5_first_session_user").await;
    create_step_one_table(&first_catalogs, first.path().to_str().unwrap()).await;
    let second = TempDir::new().unwrap();
    let (second_ctx, second_catalogs) =
        setup_with_owner(&second, "review_fix_5_second_session_user").await;
    create_step_one_table(&second_catalogs, second.path().to_str().unwrap()).await;

    let first_rows = describe_rows(
        &first_ctx,
        &first_catalogs,
        "DESCRIBE TABLE EXTENDED ice.sales.t1",
    )
    .await;
    let second_rows = describe_rows(
        &second_ctx,
        &second_catalogs,
        "DESCRIBE TABLE EXTENDED ice.sales.t1",
    )
    .await;
    let first_owner = first_rows
        .iter()
        .find(|(name, _, _)| name == "Owner")
        .expect("extended output carries Owner");
    let second_owner = second_rows
        .iter()
        .find(|(name, _, _)| name == "Owner")
        .expect("extended output carries Owner");
    assert_eq!(
        first_owner.1, "review_fix_5_first_session_user",
        "Owner is fixed at session build, not read at query time"
    );
    assert_eq!(
        second_owner.1, "review_fix_5_second_session_user",
        "a second session in the same process resolves its own user"
    );
}

#[tokio::test]
async fn describe_table_owner_resolves_in_a_production_built_session() {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap().to_string();
    let session = repark_core::ReparkSession::builder()
        .build()
        .expect("a production session builds");
    session
        .register_memory_catalog("ice", &warehouse_path)
        .await
        .expect("the production session registers ice");
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse_path}/sales"))]),
        )
        .await
        .expect("the production session creates sales");
    let catalogs = session.catalogs_snapshot();
    let location = format!("{warehouse_path}/sales/prod_owner");
    std::fs::create_dir_all(&location).expect("the fixture directory builds");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![std::sync::Arc::new(NestedField::optional(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("the fixture schema builds");
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            iceberg::TableCreation::builder()
                .name("prod_owner".to_string())
                .location(location)
                .schema(schema)
                .build(),
        )
        .await
        .expect("the production catalog creates the table");

    let rows = describe_rows(
        session.context(),
        &catalogs,
        "DESCRIBE TABLE EXTENDED ice.sales.prod_owner",
    )
    .await;
    let owner = rows
        .iter()
        .find(|(name, _, _)| name == "Owner")
        .expect("extended output carries Owner");
    assert_eq!(
        owner.1,
        repark_core::session_owner_snapshot(),
        "a production-built session resolves the owner at build, never unknown"
    );
}

#[tokio::test]
async fn describe_table_short_names_complete_from_session_defaults() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let warehouse = wh.path().to_str().unwrap().to_string();
    let location = format!("{warehouse}/sales/desc_demo");
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
                .name("desc_demo".to_string())
                .location(location)
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    for set in [
        "SET datafusion.catalog.default_catalog = 'ice'",
        "SET datafusion.catalog.default_schema = 'sales'",
    ] {
        ctx.sql(set).await.unwrap().collect().await.unwrap();
    }

    let full = describe_rows(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.desc_demo").await;
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE desc_demo").await,
        full,
        "a one-part name resolves through the session current catalog and namespace"
    );
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE sales.desc_demo").await,
        full,
        "a two-part name resolves through the session current catalog"
    );
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED desc_demo").await,
        describe_rows(
            &ctx,
            &catalogs,
            "DESCRIBE TABLE EXTENDED ice.sales.desc_demo"
        )
        .await,
        "short names work in the extended spelling too"
    );
}

#[tokio::test]
async fn describe_table_extended_redacts_through_prop_key_is_secret() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let warehouse = wh.path().to_str().unwrap().to_string();
    let location = format!("{warehouse}/sales/keyed");
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
                .name("keyed".to_string())
                .location(location)
                .schema(schema)
                .properties(HashMap::from([
                    ("s3.access-key-id".to_string(), "AKIAEXAMPLE".to_string()),
                    ("k".to_string(), "v".to_string()),
                ]))
                .build(),
        )
        .await
        .unwrap();

    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.keyed").await;
    let properties = rows
        .iter()
        .find(|(name, _, _)| name == "Table Properties")
        .expect("extended output carries Table Properties");
    assert!(
        properties.1.contains("*********(redacted)"),
        "the secret-shaped key must be redacted, got: {}",
        properties.1
    );
    assert!(
        !properties.1.contains("AKIAEXAMPLE"),
        "the plaintext access key must never reach output, got: {}",
        properties.1
    );
    assert!(
        properties.1.contains("k=v"),
        "a non-secret property renders in the clear, got: {}",
        properties.1
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
