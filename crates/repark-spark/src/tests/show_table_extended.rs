use datafusion::arrow::array::BooleanArray;

use super::super::*;
use super::common::*;

type ExtendedRow = (String, String, bool, String);

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(Vec<String>, Vec<ExtendedRow>), String> {
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
    let mut rows = Vec::new();
    for batch in &batches {
        if batch.num_columns() != 4 {
            continue;
        }
        let Some(namespaces) = batch.column(0).as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        let Some(tables) = batch.column(1).as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        let Some(temporary) = batch.column(2).as_any().downcast_ref::<BooleanArray>() else {
            continue;
        };
        let Some(information) = batch.column(3).as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for index in 0..batch.num_rows() {
            rows.push((
                namespaces.value(index).to_string(),
                tables.value(index).to_string(),
                temporary.value(index),
                information.value(index).to_string(),
            ));
        }
    }
    Ok((columns, rows))
}

async fn show_table_extended(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Vec<String>, Vec<ExtendedRow>) {
    outcome(ctx, catalogs, sql).await.unwrap()
}

async fn table_location(catalogs: &CatalogRegistry, table: &str) -> String {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .location()
        .to_string()
}

async fn table_snapshot_id(catalogs: &CatalogRegistry, table: &str) -> String {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_snapshot_id()
        .unwrap()
        .to_string()
}

fn character_properties(pairs: &[(&str, &str)]) -> String {
    let properties = format!(
        "[{}]",
        pairs
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    format!(
        "[{}]",
        properties
            .chars()
            .map(|character| character.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn information(
    catalog: &str,
    namespace: &str,
    table: &str,
    location: &str,
    properties: &str,
    comment: Option<&str>,
    owner: Option<&str>,
    tree: &str,
) -> String {
    let mut lines = vec![
        format!("Catalog: {catalog}"),
        format!("Namespace: {namespace}"),
        format!("Table: {table}"),
        "Type: MANAGED".to_string(),
    ];
    if let Some(comment) = comment {
        lines.push(format!("Comment: {comment}"));
    }
    lines.push(format!("Location: {location}"));
    lines.push("Provider: iceberg".to_string());
    if let Some(owner) = owner {
        lines.push(format!("Owner: {owner}"));
    }
    lines.push(format!("Table Properties: {properties}"));
    lines.push(format!("Schema: {tree}"));
    format!("{}\n", lines.join("\n"))
}

#[tokio::test]
async fn show_table_extended_answers_exact_partitioned_information() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pc (id BIGINT NOT NULL COMMENT 'the id', ts TIMESTAMP, cat STRING) \
         USING iceberg PARTITIONED BY (cat, days(ts), bucket(4, id)) COMMENT 'tbl comment' \
         TBLPROPERTIES ('db.secret'='hunter2', 'k'='v')",
    )
    .await;
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("db.secret", "*********(redacted)"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("k", "v"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let expected = information(
        "ice",
        "sales",
        "pc",
        &table_location(&catalogs, "pc").await,
        &properties,
        Some("tbl comment"),
        None,
        "root\n |-- id: long (nullable = false)\n |-- ts: timestamp (nullable = true)\n |-- cat: string (nullable = true)\n",
    );
    let (columns, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc'",
    )
    .await;
    assert_eq!(
        columns,
        vec![
            "namespace".to_string(),
            "tableName".to_string(),
            "isTemporary".to_string(),
            "information".to_string(),
        ]
    );
    assert_eq!(
        rows,
        vec![("sales".to_string(), "pc".to_string(), false, expected)]
    );
}

#[tokio::test]
async fn show_table_extended_tracks_snapshot_and_plain_information() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pl (id BIGINT) USING iceberg",
    )
    .await;
    let location = table_location(&catalogs, "pl").await;
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let expected = information(
        "ice",
        "sales",
        "pl",
        &location,
        &properties,
        None,
        None,
        "root\n |-- id: long (nullable = true)\n",
    );
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED FROM ice.sales LIKE 'pl'",
    )
    .await;
    assert_eq!(
        rows,
        vec![("sales".to_string(), "pl".to_string(), false, expected)]
    );
    run(&ctx, &catalogs, "INSERT INTO ice.sales.pl VALUES (1)").await;
    let snapshot = table_snapshot_id(&catalogs, "pl").await;
    let snapshot_properties = character_properties(&[
        ("current-snapshot-id", &snapshot),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pl'",
    )
    .await;
    assert_eq!(
        rows[0].3,
        information(
            "ice",
            "sales",
            "pl",
            &location,
            &snapshot_properties,
            None,
            None,
            "root\n |-- id: long (nullable = true)\n",
        )
    );
}

#[tokio::test]
async fn show_table_extended_keeps_v3_and_unicode_property_scalars() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3 (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version'='3', 'κ'='💥')",
    )
    .await;
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "3"),
        ("write.parquet.compression-codec", "zstd"),
        ("κ", "💥"),
    ]);
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'v3'",
    )
    .await;
    assert!(
        rows[0]
            .3
            .contains(&format!("Table Properties: {properties}\n")),
        "{}",
        rows[0].3
    );
}

#[tokio::test]
async fn show_table_extended_lists_sorted_tables_and_excludes_views() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for table in ["pl", "lo", "v3"] {
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.{table} (id BIGINT) USING iceberg"),
        )
        .await;
    }
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.vw AS SELECT id FROM ice.sales.pl",
    )
    .await;
    let (_, rows) =
        show_table_extended(&ctx, &catalogs, "SHOW TABLE EXTENDED IN ice.sales LIKE '*'").await;
    let names: Vec<String> = rows.into_iter().map(|row| row.1).collect();
    assert_eq!(names, vec!["lo", "pl", "v3"]);
    let (_, view_rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'vw'",
    )
    .await;
    assert!(view_rows.is_empty());
}

#[tokio::test]
async fn show_table_extended_filters_alternation_case_and_ambient_scope() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for table in ["lo", "pl"] {
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.{table} (id BIGINT) USING iceberg"),
        )
        .await;
    }
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pl\\|lo'",
    )
    .await;
    assert_eq!(
        rows.into_iter().map(|row| row.1).collect::<Vec<_>>(),
        vec!["lo", "pl"]
    );
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'PL'",
    )
    .await;
    assert_eq!(rows[0].1, "pl");
    run(&ctx, &catalogs, "USE ice.sales").await;
    let (_, rows) = show_table_extended(&ctx, &catalogs, "SHOW TABLE EXTENDED LIKE 'pl'").await;
    assert_eq!(rows[0].0, "sales");
    assert_eq!(rows[0].1, "pl");
}

#[tokio::test]
async fn show_table_extended_reports_location_management_owner_and_tree() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let location = warehouse.path().join("custom_lo");
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.lo (id BIGINT) USING iceberg LOCATION '{}'",
            location.display()
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ownered (id BIGINT) USING iceberg",
    )
    .await;
    repark_iceberg::write::alter::set_table_properties(
        catalog_handle(&catalogs, "ice").unwrap().as_ref(),
        &TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ownered".to_string(),
        ),
        &HashMap::from([("owner".to_string(), "john".to_string())]),
    )
    .await
    .unwrap();
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nested (s STRUCT<x: INT, y: ARRAY<STRING>>) USING iceberg",
    )
    .await;
    let (_, locations) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'lo'",
    )
    .await;
    assert!(locations[0].3.contains("Type: MANAGED\n"));
    assert!(locations[0].3.contains(&format!(
        "Location: {}\n",
        table_location(&catalogs, "lo").await
    )));
    let (_, owners) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'ownered'",
    )
    .await;
    assert!(owners[0].3.contains("Provider: iceberg\nOwner: john\n"));
    let (_, nested) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'nested'",
    )
    .await;
    assert!(nested[0].3.contains(" |-- s: struct (nullable = true)\n"));
    assert!(
        nested[0]
            .3
            .contains(" |    |-- x: integer (nullable = true)\n")
    );
    assert!(
        nested[0]
            .3
            .contains(" |    |-- y: array (nullable = true)\n")
    );
    assert!(
        nested[0]
            .3
            .contains(" |    |    |-- element: string (containsNull = true)\n")
    );
}

#[tokio::test]
async fn show_table_extended_refuses_required_error_shapes() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pc (id BIGINT, cat STRING) USING iceberg PARTITIONED BY (cat)",
    )
    .await;
    let missing_like = outcome(&ctx, &catalogs, "SHOW TABLE EXTENDED IN ice.sales")
        .await
        .unwrap_err();
    assert!(
        missing_like
            .contains("[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601"),
        "{missing_like}"
    );
    let partition = outcome(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='a')",
    )
    .await
    .unwrap_err();
    assert!(
        partition.contains(
            "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] The partition \
             command is invalid. Table `ice`.`sales`.`pc` does not support partition management. \
             SQLSTATE: 42601"
        ),
        "{partition}"
    );
    let absent_partition = outcome(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'absent' PARTITION (cat='a')",
    )
    .await
    .unwrap_err();
    assert!(
        absent_partition.contains("[TABLE_OR_VIEW_NOT_FOUND]"),
        "{absent_partition}"
    );
    let namespace = outcome(&ctx, &catalogs, "SHOW TABLE EXTENDED IN ice.nope LIKE '*'")
        .await
        .unwrap_err();
    assert!(namespace.contains("[SCHEMA_NOT_FOUND]"), "{namespace}");
}

#[tokio::test]
async fn show_table_extended_near_misses_keep_their_existing_paths() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pl (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let (columns, _) = outcome(&ctx, &catalogs, "SHOW TABLES IN sales")
        .await
        .unwrap();
    assert_eq!(columns, vec!["namespace", "tableName", "isTemporary"]);
    let (columns, _) = outcome(&ctx, &catalogs, "SHOW TABLES IN sales LIKE 'p*'")
        .await
        .unwrap();
    assert_eq!(columns, vec!["namespace", "tableName", "isTemporary"]);
    let bare = outcome(&ctx, &catalogs, "SHOW TABLE EXTENDED")
        .await
        .unwrap_err();
    assert!(bare.contains("[PARSE_SYNTAX_ERROR]"), "{bare}");
    let tables_extended = outcome(&ctx, &catalogs, "SHOW TABLES EXTENDED IN sales LIKE '*'").await;
    if let Ok((columns, _)) = tables_extended {
        assert_ne!(
            columns,
            vec!["namespace", "tableName", "isTemporary", "information"]
        );
    }
    let properties = outcome(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.pl")
        .await
        .unwrap_err();
    assert!(properties.contains("SHOW [VARIABLE]"), "{properties}");
    let (columns, _) = outcome(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.pl")
        .await
        .unwrap();
    assert_eq!(columns, vec!["createtab_stmt"]);
}
