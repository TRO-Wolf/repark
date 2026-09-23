use datafusion::arrow::array::BooleanArray;
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;

use super::super::*;
use super::common::*;

pub(super) type ExtendedRow = (String, String, bool, String);

#[derive(Clone, Copy)]
struct Information<'a> {
    catalog: &'a str,
    namespace: &'a str,
    table: &'a str,
    location: &'a str,
    properties: &'a str,
    comment: Option<&'a str>,
    owner: Option<&'a str>,
    tree: &'a str,
}

pub(super) async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(Schema, Vec<ExtendedRow>), DataFusionError> {
    let frame = execute(ctx, catalogs, sql).await?;
    let schema = frame.schema().as_arrow().clone();
    let batches = frame.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().as_ref(), &schema, "{sql}");
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
    Ok((schema, rows))
}

pub(super) fn extended_schema() -> Schema {
    Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("tableName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
        Field::new("information", DataType::Utf8, false),
    ])
}

pub(super) fn assert_parse_refusal(sql: &str, error: DataFusionError, expected: &str) {
    assert_eq!(
        error.to_string(),
        format!("SQL error: ParserError({expected:?})"),
        "{sql}"
    );
    let DataFusionError::SQL(parser_error, _) = error else {
        panic!("{sql}: expected a SQL parser error, got {error:?}");
    };
    let ParserError::ParserError(message) = parser_error.as_ref() else {
        panic!("{sql}: expected a parser message");
    };
    assert_eq!(message, expected, "{sql}");
}

pub(super) fn assert_analysis_refusal(sql: &str, error: DataFusionError, expected: &str) {
    let DataFusionError::Plan(message) = error else {
        panic!("{sql}: expected a planning error");
    };
    assert_eq!(message, expected, "{sql}");
}

async fn show_table_extended(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Schema, Vec<ExtendedRow>) {
    let (schema, rows) = outcome(ctx, catalogs, sql).await.unwrap();
    assert_eq!(schema, extended_schema(), "{sql}");
    (schema, rows)
}

pub(super) async fn table_location(catalogs: &CatalogRegistry, table: &str) -> String {
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

pub(super) fn character_properties(pairs: &[(&str, &str)]) -> String {
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

fn information(expectation: Information<'_>) -> String {
    let Information {
        catalog,
        namespace,
        table,
        location,
        properties,
        comment,
        owner,
        tree,
    } = expectation;
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

pub(super) fn managed_row(
    table: &str,
    location: &str,
    properties: &str,
    owner: Option<&str>,
    tree: &str,
) -> ExtendedRow {
    (
        "sales".to_string(),
        table.to_string(),
        false,
        information(Information {
            catalog: "ice",
            namespace: "sales",
            table,
            location,
            properties,
            comment: None,
            owner,
            tree,
        }),
    )
}

#[tokio::test]
async fn show_table_extended_answers_exact_partitioned_information() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
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
    let expected = information(Information {
        catalog: "ice",
        namespace: "sales",
        table: "pc",
        location: &table_location(&catalogs, "pc").await,
        properties: &properties,
        comment: Some("tbl comment"),
        owner: Some(&owner),
        tree: "root\n |-- id: long (nullable = false)\n |-- ts: timestamp (nullable = true)\n |-- cat: string (nullable = true)\n",
    });
    let (columns, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc'",
    )
    .await;
    assert_eq!(columns, extended_schema());
    assert_eq!(
        rows,
        vec![("sales".to_string(), "pc".to_string(), false, expected)]
    );
}

#[tokio::test]
async fn show_table_extended_tracks_snapshot_and_plain_information() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
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
    let expected = information(Information {
        catalog: "ice",
        namespace: "sales",
        table: "pl",
        location: &location,
        properties: &properties,
        comment: None,
        owner: Some(&owner),
        tree: "root\n |-- id: long (nullable = true)\n",
    });
    let (columns, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED FROM ice.sales LIKE 'pl'",
    )
    .await;
    assert_eq!(columns, extended_schema());
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
        rows,
        vec![managed_row(
            "pl",
            &location,
            &snapshot_properties,
            Some(&owner),
            "root\n |-- id: long (nullable = true)\n",
        )]
    );
}

#[tokio::test]
async fn show_table_extended_keeps_v3_and_unicode_property_scalars() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
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
    let location = table_location(&catalogs, "v3").await;
    let expected = information(Information {
        catalog: "ice",
        namespace: "sales",
        table: "v3",
        location: &location,
        properties: &properties,
        comment: None,
        owner: Some(&owner),
        tree: "root\n |-- id: long (nullable = true)\n",
    });
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'v3'",
    )
    .await;
    assert_eq!(
        rows,
        vec![("sales".to_string(), "v3".to_string(), false, expected)]
    );
}

#[tokio::test]
async fn show_table_extended_lists_sorted_tables_and_excludes_views() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
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
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let lo_location = table_location(&catalogs, "lo").await;
    let pl_location = table_location(&catalogs, "pl").await;
    let v3_location = table_location(&catalogs, "v3").await;
    let expected_rows = vec![
        (
            "sales".to_string(),
            "lo".to_string(),
            false,
            information(Information {
                catalog: "ice",
                namespace: "sales",
                table: "lo",
                location: &lo_location,
                properties: &properties,
                comment: None,
                owner: Some(&owner),
                tree: "root\n |-- id: long (nullable = true)\n",
            }),
        ),
        (
            "sales".to_string(),
            "pl".to_string(),
            false,
            information(Information {
                catalog: "ice",
                namespace: "sales",
                table: "pl",
                location: &pl_location,
                properties: &properties,
                comment: None,
                owner: Some(&owner),
                tree: "root\n |-- id: long (nullable = true)\n",
            }),
        ),
        (
            "sales".to_string(),
            "v3".to_string(),
            false,
            information(Information {
                catalog: "ice",
                namespace: "sales",
                table: "v3",
                location: &v3_location,
                properties: &properties,
                comment: None,
                owner: Some(&owner),
                tree: "root\n |-- id: long (nullable = true)\n",
            }),
        ),
    ];
    let (_, rows) =
        show_table_extended(&ctx, &catalogs, "SHOW TABLE EXTENDED IN ice.sales LIKE '*'").await;
    assert_eq!(rows, expected_rows);
    let (_, view_rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'vw'",
    )
    .await;
    assert_eq!(view_rows, Vec::<ExtendedRow>::new());
}

#[tokio::test]
async fn show_table_extended_filters_alternation_case_and_ambient_scope() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
    for table in ["lo", "pl"] {
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.{table} (id BIGINT) USING iceberg"),
        )
        .await;
    }
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let lo_location = table_location(&catalogs, "lo").await;
    let pl_location = table_location(&catalogs, "pl").await;
    let tree = "root\n |-- id: long (nullable = true)\n";
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pl\\|lo'",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            managed_row("lo", &lo_location, &properties, Some(&owner), tree),
            managed_row("pl", &pl_location, &properties, Some(&owner), tree),
        ]
    );
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'PL'",
    )
    .await;
    assert_eq!(
        rows,
        vec![managed_row(
            "pl",
            &pl_location,
            &properties,
            Some(&owner),
            tree
        )]
    );
    run(&ctx, &catalogs, "USE ice.sales").await;
    let (_, rows) = show_table_extended(&ctx, &catalogs, "SHOW TABLE EXTENDED LIKE 'pl'").await;
    assert_eq!(
        rows,
        vec![managed_row(
            "pl",
            &pl_location,
            &properties,
            Some(&owner),
            tree
        )]
    );
}

#[tokio::test]
async fn show_table_extended_reports_location_management_owner_and_tree() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
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
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let lo_location = table_location(&catalogs, "lo").await;
    let ownered_location = table_location(&catalogs, "ownered").await;
    let nested_location = table_location(&catalogs, "nested").await;
    let (_, locations) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'lo'",
    )
    .await;
    assert_eq!(
        locations,
        vec![managed_row(
            "lo",
            &lo_location,
            &properties,
            Some(&owner),
            "root\n |-- id: long (nullable = true)\n",
        )]
    );
    let (_, owners) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'ownered'",
    )
    .await;
    assert_eq!(
        owners,
        vec![managed_row(
            "ownered",
            &ownered_location,
            &properties,
            Some("john"),
            "root\n |-- id: long (nullable = true)\n",
        )]
    );
    let (_, nested) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'nested'",
    )
    .await;
    assert_eq!(
        nested,
        vec![managed_row(
            "nested",
            &nested_location,
            &properties,
            Some(&owner),
            "root\n |-- s: struct (nullable = true)\n |    |-- x: integer (nullable = true)\n |    |-- y: array (nullable = true)\n |    |    |-- element: string (containsNull = true)\n",
        )]
    );
}

#[tokio::test]
async fn show_table_extended_reports_measured_deep_tree() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let owner = repark_core::session_owner_snapshot();
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.deep (m MAP<STRING, STRUCT<a: BIGINT, b: ARRAY<DOUBLE>>>, n \
         ARRAY<STRUCT<z: STRING>>, d DECIMAL(10,2), ts TIMESTAMP, tn TIMESTAMP_NTZ, bi BINARY, \
         dt DATE, bo BOOLEAN, f FLOAT) USING iceberg",
    )
    .await;
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let location = table_location(&catalogs, "deep").await;
    let (_, rows) = show_table_extended(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'deep'",
    )
    .await;
    assert_eq!(
        rows,
        vec![managed_row(
            "deep",
            &location,
            &properties,
            Some(&owner),
            concat!(
                "root\n",
                " |-- m: map (nullable = true)\n",
                " |    |-- key: string\n",
                " |    |-- value: struct (valueContainsNull = true)\n",
                " |    |    |-- a: long (nullable = true)\n",
                " |    |    |-- b: array (nullable = true)\n",
                " |    |    |    |-- element: double (containsNull = true)\n",
                " |-- n: array (nullable = true)\n",
                " |    |-- element: struct (containsNull = true)\n",
                " |    |    |-- z: string (nullable = true)\n",
                " |-- d: decimal(10,2) (nullable = true)\n",
                " |-- ts: timestamp (nullable = true)\n",
                " |-- tn: timestamp_ntz (nullable = true)\n",
                " |-- bi: binary (nullable = true)\n",
                " |-- dt: date (nullable = true)\n",
                " |-- bo: boolean (nullable = true)\n",
                " |-- f: float (nullable = true)\n",
            ),
        )]
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
    for (sql, expected) in [
        (
            "SHOW TABLE EXTENDED",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE pc",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'pc'. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE \"pc",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"'. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN `ice.sales LIKE 'pc'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '`'. SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' extra",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'extra': extra input 'extra'. \
             SQLSTATE: 42601",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='a'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
        ),
    ] {
        let error = outcome(&ctx, &catalogs, sql)
            .await
            .expect_err("statement must refuse");
        assert_parse_refusal(sql, error, expected);
    }

    let partition_sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='a')";
    let partition = outcome(&ctx, &catalogs, partition_sql)
        .await
        .expect_err("partition management must refuse");
    assert_analysis_refusal(
        partition_sql,
        partition,
        "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] The partition \
         command is invalid. Table `ice`.`sales`.`pc` does not support partition management. \
         SQLSTATE: 42601",
    );

    for (sql, expected) in [
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE 'absent' PARTITION (cat='a')",
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`absent` cannot be found. \
             Verify the spelling and correctness of the schema and catalog. If you did not qualify \
             the name with a schema, verify the current_schema() output, or qualify the name with \
             the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS \
             or DROP TABLE IF EXISTS. SQLSTATE: 42P01",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE '*' PARTITION (cat='a')",
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`*` cannot be found. \
             Verify the spelling and correctness of the schema and catalog. If you did not qualify \
             the name with a schema, verify the current_schema() output, or qualify the name with \
             the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS \
             or DROP TABLE IF EXISTS. SQLSTATE: 42P01",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.nope LIKE '*'",
            "[SCHEMA_NOT_FOUND] The schema `ice`.`nope` cannot be found. Verify the spelling and \
             correctness of the schema and catalog. If you did not qualify the name with a catalog, \
             verify the current_schema() output, or qualify the name with the correct catalog. To \
             tolerate the error on drop use DROP SCHEMA IF EXISTS. SQLSTATE: 42704",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales.x LIKE '*'",
            "[SCHEMA_NOT_FOUND] The schema `ice`.`sales`.`x` cannot be found. Verify the spelling \
             and correctness of the schema and catalog. If you did not qualify the name with a \
             catalog, verify the current_schema() output, or qualify the name with the correct \
             catalog. To tolerate the error on drop use DROP SCHEMA IF EXISTS. SQLSTATE: 42704",
        ),
    ] {
        let error = outcome(&ctx, &catalogs, sql)
            .await
            .expect_err("statement must refuse");
        assert_analysis_refusal(sql, error, expected);
    }
}
