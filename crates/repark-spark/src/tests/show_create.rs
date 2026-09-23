use super::super::*;
use super::common::*;

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(Vec<String>, Vec<String>), String> {
    let frame = execute(ctx, catalogs, sql)
        .await
        .map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    let columns = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let mut cells = Vec::new();
    for batch in &batches {
        if let Some(texts) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for index in 0..batch.num_rows() {
                cells.push(texts.value(index).to_string());
            }
        }
    }
    Ok((columns, cells))
}

async fn show_create(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> String {
    let (columns, cells) = outcome(ctx, catalogs, &format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    assert_eq!(columns, vec!["createtab_stmt".to_string()]);
    assert_eq!(cells.len(), 1);
    cells.into_iter().next().unwrap()
}

async fn location(catalogs: &CatalogRegistry, table: &str) -> String {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .location()
        .to_string()
}

async fn snapshot_id(catalogs: &CatalogRegistry, table: &str) -> i64 {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_snapshot_id()
        .unwrap()
}

#[tokio::test]
async fn show_create_partitioned_table_with_column_comment_matches_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sc1 (id BIGINT NOT NULL COMMENT 'c', data STRING) USING iceberg \
         PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('k'='v')",
    )
    .await;
    let expected = format!(
        "CREATE TABLE ice.sales.sc1 (\n  id BIGINT NOT NULL COMMENT 'c',\n  data STRING)\n\
         USING iceberg\nPARTITIONED BY (bucket(4, id))\nLOCATION '{}'\nTBLPROPERTIES (\n  \
         'current-snapshot-id' = 'none',\n  'format' = 'iceberg/parquet',\n  \
         'format-version' = '2',\n  'k' = 'v',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "sc1").await
    );
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.sc1").await,
        expected
    );
}

#[tokio::test]
async fn show_create_plain_table_then_insert_then_write_order_matches_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sc2 (id BIGINT, data STRING) USING iceberg",
    )
    .await;
    let at = location(&catalogs, "sc2").await;
    let head = format!(
        "CREATE TABLE ice.sales.sc2 (\n  id BIGINT,\n  data STRING)\nUSING iceberg\n\
         LOCATION '{at}'\nTBLPROPERTIES (\n"
    );
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.sc2").await,
        format!(
            "{head}  'current-snapshot-id' = 'none',\n  'format' = 'iceberg/parquet',\n  \
             'format-version' = '2',\n  'write.parquet.compression-codec' = 'zstd')\n"
        )
    );
    run(&ctx, &catalogs, "INSERT INTO ice.sales.sc2 VALUES (1, 'a')").await;
    let snapshot = snapshot_id(&catalogs, "sc2").await;
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.sc2").await,
        format!(
            "{head}  'current-snapshot-id' = '{snapshot}',\n  'format' = 'iceberg/parquet',\n  \
             'format-version' = '2',\n  'write.parquet.compression-codec' = 'zstd')\n"
        )
    );
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sc2 WRITE ORDERED BY id",
    )
    .await;
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.sc2").await,
        format!(
            "{head}  'current-snapshot-id' = '{snapshot}',\n  'format' = 'iceberg/parquet',\n  \
             'format-version' = '2',\n  'sort-order' = 'id ASC NULLS FIRST',\n  \
             'write.distribution-mode' = 'range',\n  \
             'write.parquet.compression-codec' = 'zstd')\n"
        )
    );
}

#[tokio::test]
async fn show_create_rich_types_partitions_and_comment_matches_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sc3 (id BIGINT, data STRING, ts TIMESTAMP, d DATE, \
         dec DECIMAL(6,2), m MAP<STRING, INT>, a ARRAY<STRING>, \
         st STRUCT<x: INT, y: STRING>) USING iceberg \
         PARTITIONED BY (data, days(ts), truncate(3, data), years(d), bucket(8, id)) \
         COMMENT 'table doc' TBLPROPERTIES ('k'='v', 'a.b'='it''s')",
    )
    .await;
    repark_iceberg::write::alter::apply_schema_changes(
        catalog_handle(&catalogs, "ice").unwrap().as_ref(),
        &TableIdent::new(NamespaceIdent::new("sales".into()), "sc3".into()),
        &[
            repark_iceberg::write::alter::SchemaChange::UpdateColumnDoc {
                name: "st.y".to_string(),
                doc: Some("yy".to_string()),
            },
        ],
    )
    .await
    .unwrap();
    let expected = format!(
        "CREATE TABLE ice.sales.sc3 (\n  id BIGINT,\n  data STRING,\n  ts TIMESTAMP,\n  d DATE,\n  \
         dec DECIMAL(6,2),\n  m MAP<STRING, INT>,\n  a ARRAY<STRING>,\n  \
         st STRUCT<x: INT, y: STRING COMMENT 'yy'>)\nUSING iceberg\n\
         PARTITIONED BY (data, days(ts), truncate(3, data), years(d), bucket(8, id))\n\
         COMMENT 'table doc'\nLOCATION '{}'\nTBLPROPERTIES (\n  'a.b' = 'it\\'s',\n  \
         'current-snapshot-id' = 'none',\n  'format' = 'iceberg/parquet',\n  \
         'format-version' = '2',\n  'k' = 'v',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "sc3").await
    );
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.sc3").await,
        expected
    );
}

#[tokio::test]
async fn show_create_identifier_fields_matches_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sc4 (id BIGINT NOT NULL, data STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sc4 SET IDENTIFIER FIELDS id",
    )
    .await;
    let expected = format!(
        "CREATE TABLE ice.sales.sc4 (\n  id BIGINT NOT NULL,\n  data STRING)\nUSING iceberg\n\
         LOCATION '{}'\nTBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  \
         'format' = 'iceberg/parquet',\n  'format-version' = '2',\n  \
         'identifier-fields' = '[id]',\n  'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "sc4").await
    );
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.sc4").await,
        expected
    );
}

#[tokio::test]
async fn show_create_identifier_fields_follow_java_hash_set_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.h1 (zz BIGINT NOT NULL, id BIGINT NOT NULL, a STRING NOT NULL) \
         USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.h1 SET IDENTIFIER FIELDS id, zz, a",
    )
    .await;
    let text = show_create(&ctx, &catalogs, "ice.sales.h1").await;
    assert!(
        text.contains("  'identifier-fields' = '[zz,a,id]',\n"),
        "got: {text}"
    );
}

#[tokio::test]
async fn show_create_multi_term_sort_order_matches_spark() {
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, Schema as IcebergSchema, SortDirection, SortField,
        SortOrder, Transform, Type,
    };
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "data",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "ts",
                Type::Primitive(PrimitiveType::Timestamptz),
            )),
        ])
        .build()
        .unwrap();
    let term = |source_id, transform, direction, null_order| SortField {
        source_id,
        transform,
        direction,
        null_order,
    };
    let fields = vec![
        term(
            1,
            Transform::Identity,
            SortDirection::Descending,
            NullOrder::Last,
        ),
        term(
            2,
            Transform::Bucket(4),
            SortDirection::Ascending,
            NullOrder::First,
        ),
        term(3, Transform::Day, SortDirection::Ascending, NullOrder::Last),
        term(
            2,
            Transform::Truncate(3),
            SortDirection::Ascending,
            NullOrder::First,
        ),
    ];
    let mut builder = SortOrder::builder();
    builder.with_order_id(1);
    for field in fields {
        builder.with_sort_field(field);
    }
    let sort_order = builder.build_unbound().unwrap();
    let at = format!("{}/sales/m2", wh.path().to_str().unwrap());
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            TableCreation::builder()
                .name("m2".to_string())
                .location(at)
                .schema(schema)
                .sort_order(sort_order)
                .build(),
        )
        .await
        .unwrap();
    let text = show_create(&ctx, &catalogs, "ice.sales.m2").await;
    assert!(
        text.contains(
            "  'sort-order' = 'id DESC NULLS LAST, bucket(4, data) ASC NULLS FIRST, \
             days(ts) ASC NULLS LAST, truncate(data, 3) ASC NULLS FIRST',\n"
        ),
        "got: {text}"
    );
}

#[tokio::test]
async fn show_create_escapes_quotes_but_not_backslashes_and_renders_options() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        r"CREATE TABLE ice.sales.m1 (id BIGINT NOT NULL COMMENT 'it''s a\\b', ts TIMESTAMP_NTZ, b BINARY, f FLOAT, dd DOUBLE, bo BOOLEAN, i INT, st STRUCT<x: INT NOT NULL, y: ARRAY<STRUCT<z: STRING NOT NULL>>>, `we-ird` STRING, `123` INT) USING iceberg COMMENT 'tab''le \\ doc' TBLPROPERTIES ('bs'='a\\b', 'q'='x''y', 'option.foo'='bar', 'foo'='1', 'write.format.default'='orc')",
    )
    .await;
    let expected = format!(
        "CREATE TABLE ice.sales.m1 (\n  id BIGINT NOT NULL COMMENT 'it\\'s a\\b',\n  \
         ts TIMESTAMP_NTZ,\n  b BINARY,\n  f FLOAT,\n  dd DOUBLE,\n  bo BOOLEAN,\n  i INT,\n  \
         st STRUCT<x: INT NOT NULL, y: ARRAY<STRUCT<z: STRING NOT NULL>>>,\n  \
         `we-ird` STRING,\n  `123` INT)\nUSING iceberg\nOPTIONS (\n  'foo' = 'bar')\n\
         COMMENT 'tab\\'le \\ doc'\nLOCATION '{}'\nTBLPROPERTIES (\n  'bs' = 'a\\b',\n  \
         'current-snapshot-id' = 'none',\n  'format' = 'iceberg/orc',\n  \
         'format-version' = '2',\n  'q' = 'x\\'y',\n  'write.format.default' = 'orc',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "m1").await
    );
    assert_eq!(show_create(&ctx, &catalogs, "ice.sales.m1").await, expected);
}

#[tokio::test]
async fn show_create_quotes_a_table_name_that_needs_backticks() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.`we-ird` (id BIGINT) USING iceberg",
    )
    .await;
    let text = show_create(&ctx, &catalogs, "ice.sales.`we-ird`").await;
    assert!(
        text.starts_with("CREATE TABLE ice.sales.`we-ird` (\n  id BIGINT)\nUSING iceberg\n"),
        "got: {text}"
    );
}

#[tokio::test]
async fn show_create_redacts_a_secret_looking_property() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.creds (id BIGINT) USING iceberg \
         TBLPROPERTIES ('my.secret'='hunter2')",
    )
    .await;
    let text = show_create(&ctx, &catalogs, "ice.sales.creds").await;
    assert!(
        text.contains("  'my.secret' = '*********(redacted)',\n"),
        "got: {text}"
    );
    assert!(!text.contains("hunter2"), "got: {text}");
}

#[tokio::test]
async fn show_create_resolves_a_session_qualified_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sc5 (id BIGINT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let text = show_create(&ctx, &catalogs, "sc5").await;
    assert!(
        text.starts_with("CREATE TABLE ice.sales.sc5 (\n"),
        "got: {text}"
    );
}

#[tokio::test]
async fn show_create_as_serde_refuses_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sc2 (id BIGINT, data STRING) USING iceberg",
    )
    .await;
    let error = outcome(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.sc2 AS SERDE")
        .await
        .unwrap_err();
    assert!(
        error.contains(
            "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] SHOW CREATE TABLE AS SERDE is not supported \
             for v2 tables. SQLSTATE: 0A000"
        ),
        "got: {error}"
    );
}

#[tokio::test]
async fn show_create_missing_table_refuses_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = outcome(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.nope")
        .await
        .unwrap_err();
    assert!(
        error.contains(
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`nope` cannot be found."
        ),
        "got: {error}"
    );
    assert!(error.contains("SQLSTATE: 42P01"), "got: {error}");
}

#[tokio::test]
async fn show_create_table_without_a_name_is_a_loud_parse_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = outcome(&ctx, &catalogs, "SHOW CREATE TABLE")
        .await
        .unwrap_err();
    assert!(
        error.contains(
            "[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not \
             valid. SQLSTATE: 42601"
        ),
        "got: {error}"
    );
    assert!(!error.contains("information_schema"), "got: {error}");
}

#[tokio::test]
async fn show_create_near_misses_keep_their_current_behaviour() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT id FROM ice.sales.t",
    )
    .await;
    for sql in [
        "SHOW CREATE TABLE ice.sales.v",
        "SHOW CREATE VIEW ice.sales.v",
    ] {
        if let Ok((_, cells)) = outcome(&ctx, &catalogs, sql).await {
            assert!(
                cells.iter().all(|cell| !cell.starts_with("CREATE TABLE")),
                "{sql} must not be answered as a table: {cells:?}"
            );
        }
    }
    let no_object = outcome(&ctx, &catalogs, "SHOW CREATE").await.unwrap_err();
    assert!(!no_object.contains("createtab_stmt"), "got: {no_object}");
    for sql in [
        "SHOW TABLES IN ice.sales",
        "SHOW COLUMNS IN ice.sales.t",
        "SHOW TBLPROPERTIES ice.sales.t",
    ] {
        if let Ok((columns, _)) = outcome(&ctx, &catalogs, sql).await {
            assert!(
                !columns.contains(&"createtab_stmt".to_string()),
                "{sql} must not be claimed by SHOW CREATE: {columns:?}"
            );
        }
    }
}

#[tokio::test]
async fn describe_extended_table_properties_row_matches_spark_for_a_fresh_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.de (id BIGINT, data STRING) USING iceberg PARTITIONED BY (data)",
    )
    .await;
    let batches = execute(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.de")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut properties = None;
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
            if names.value(index) == "Table Properties" {
                properties = Some(values.value(index).to_string());
            }
        }
    }
    assert_eq!(
        properties.as_deref(),
        Some(
            "[current-snapshot-id=none,format=iceberg/parquet,format-version=2,\
             write.parquet.compression-codec=zstd]"
        )
    );
}

#[tokio::test]
async fn describe_extended_carries_the_table_comment_as_its_own_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.doc (Comment STRING) USING iceberg COMMENT 'tab''le doc'",
    )
    .await;
    let batches = execute(&ctx, &catalogs, "DESCRIBE TABLE EXTENDED ice.sales.doc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut names = Vec::new();
    let mut values = Vec::new();
    for batch in &batches {
        let name_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_column = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            names.push(name_column.value(index).to_string());
            values.push(value_column.value(index).to_string());
        }
    }
    let detail = names
        .iter()
        .position(|name| name == "# Detailed Table Information")
        .unwrap();
    assert_eq!(
        names[detail + 1..detail + 5],
        ["Name", "Type", "Comment", "Location"]
    );
    assert_eq!(values[detail + 3], "tab'le doc");
    let properties = names
        .iter()
        .position(|name| name == "Table Properties")
        .unwrap();
    assert!(
        !values[properties].contains("comment="),
        "{}",
        values[properties]
    );
}
