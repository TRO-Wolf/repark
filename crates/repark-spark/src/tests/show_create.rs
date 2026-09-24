use super::super::*;
use super::common::*;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::parser::ParserError;

const AS_SERDE_MESSAGE: &str = "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] SHOW CREATE TABLE AS SERDE is not supported for v2 \
     tables. SQLSTATE: 0A000";
const INVALID_SHOW_CREATE_TABLE_MESSAGE: &str = "[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not valid. \
     SQLSTATE: 42601";
const INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE: &str = "SQL error: ParserError(\"[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not valid. \
     SQLSTATE: 42601\")";
const UNCLOSED_BRACKETED_COMMENT_MESSAGE: &str = "[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. \
     Please, append */ at the end of the comment. SQLSTATE: 42601";
const UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE: &str = "SQL error: ParserError(\"[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. \
     Please, append */ at the end of the comment. SQLSTATE: 42601\")";
const MISSING_TABLE_MESSAGE: &str = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`nope` cannot be found. Verify the \
     spelling and correctness of the schema and catalog. If you did not qualify the name with a \
     schema, verify the current_schema() output, or qualify the name with the correct schema and \
     catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. \
     SQLSTATE: 42P01";

async fn execution_error(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> DataFusionError {
    execute(ctx, catalogs, sql).await.expect_err(sql)
}

fn parse_error_message(error: DataFusionError, sql: &str) -> String {
    let DataFusionError::SQL(parser_error, _) = error else {
        panic!("{sql} must be a DataFusion SQL error");
    };
    let ParserError::ParserError(message) = parser_error.as_ref() else {
        panic!("{sql} must carry a parser error message: {parser_error}");
    };
    message.clone()
}

fn assert_invalid_show_create_table_error(error: DataFusionError, sql: &str) {
    assert_eq!(
        error.to_string(),
        INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE,
        "{sql}"
    );
    assert_eq!(
        parse_error_message(error, sql),
        INVALID_SHOW_CREATE_TABLE_MESSAGE,
        "{sql}"
    );
}

fn plan_error_message(error: &DataFusionError, sql: &str) -> String {
    let DataFusionError::Plan(message) = &error else {
        panic!("{sql} must be a DataFusion plan error");
    };
    assert_eq!(
        error.to_string(),
        format!("Error during planning: {message}"),
        "{sql}"
    );
    message.clone()
}

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(Schema, Vec<String>), String> {
    let frame = execute(ctx, catalogs, sql)
        .await
        .map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    let schema = batches
        .first()
        .map_or_else(Schema::empty, |batch| batch.schema().as_ref().clone());
    let mut cells = Vec::new();
    for batch in &batches {
        if let Some(texts) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for index in 0..batch.num_rows() {
                cells.push(texts.value(index).to_string());
            }
        }
    }
    Ok((schema, cells))
}

async fn show_create(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> String {
    let (schema, cells) = outcome(ctx, catalogs, &format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    assert_eq!(
        schema,
        Schema::new(vec![Field::new("createtab_stmt", DataType::Utf8, false)])
    );
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

async fn describe_extended_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> (Schema, Vec<(String, String, String)>) {
    let batches = execute(ctx, catalogs, &format!("DESCRIBE TABLE EXTENDED {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let schema = batches
        .first()
        .map_or_else(Schema::empty, |batch| batch.schema().as_ref().clone());
    let mut rows = Vec::new();
    for batch in &batches {
        let columns = (0..3)
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for index in 0..batch.num_rows() {
            rows.push((
                columns[0].value(index).to_string(),
                columns[1].value(index).to_string(),
                columns[2].value(index).to_string(),
            ));
        }
    }
    (schema, rows)
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
    let expected = format!(
        "CREATE TABLE ice.sales.h1 (\n  zz BIGINT NOT NULL,\n  id BIGINT NOT NULL,\n  \
         a STRING NOT NULL)\nUSING iceberg\nLOCATION '{}'\nTBLPROPERTIES (\n  \
         'current-snapshot-id' = 'none',\n  'format' = 'iceberg/parquet',\n  \
         'format-version' = '2',\n  'identifier-fields' = '[zz,a,id]',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "h1").await
    );
    assert_eq!(show_create(&ctx, &catalogs, "ice.sales.h1").await, expected);
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
                .location(at.clone())
                .schema(schema)
                .sort_order(sort_order)
                .properties(std::collections::HashMap::from([(
                    "write.distribution-mode".to_string(),
                    "range".to_string(),
                )]))
                .build(),
        )
        .await
        .unwrap();
    let expected = format!(
        "CREATE TABLE ice.sales.m2 (\n  id BIGINT,\n  data STRING,\n  ts TIMESTAMP)\n\
         USING iceberg\nLOCATION '{at}'\nTBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  \
         'format' = 'iceberg/parquet',\n  'format-version' = '2',\n  \
         'sort-order' = 'id DESC NULLS LAST, bucket(4, data) ASC NULLS FIRST, \
         days(ts) ASC NULLS LAST, truncate(data, 3) ASC NULLS FIRST',\n  \
         'write.distribution-mode' = 'range',\n  \
         'write.parquet.compression-codec' = 'zstd')\n"
    );
    assert_eq!(show_create(&ctx, &catalogs, "ice.sales.m2").await, expected);
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
    let expected = format!(
        "CREATE TABLE ice.sales.`we-ird` (\n  id BIGINT)\nUSING iceberg\nLOCATION '{}'\n\
         TBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  \
         'format' = 'iceberg/parquet',\n  'format-version' = '2',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "we-ird").await
    );
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.`we-ird`").await,
        expected
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
    let expected = format!(
        "CREATE TABLE ice.sales.creds (\n  id BIGINT)\nUSING iceberg\nLOCATION '{}'\n\
         TBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  \
         'format' = 'iceberg/parquet',\n  'format-version' = '2',\n  \
         'my.secret' = '*********(redacted)',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "creds").await
    );
    assert_eq!(
        show_create(&ctx, &catalogs, "ice.sales.creds").await,
        expected
    );
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
    let expected = format!(
        "CREATE TABLE ice.sales.sc5 (\n  id BIGINT)\nUSING iceberg\nLOCATION '{}'\n\
         TBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  \
         'format' = 'iceberg/parquet',\n  'format-version' = '2',\n  \
         'write.parquet.compression-codec' = 'zstd')\n",
        location(&catalogs, "sc5").await
    );
    assert_eq!(show_create(&ctx, &catalogs, "sc5").await, expected);
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
    let sql = "SHOW CREATE TABLE ice.sales.sc2 AS SERDE";
    assert_eq!(
        plan_error_message(&execution_error(&ctx, &catalogs, sql).await, sql),
        AS_SERDE_MESSAGE
    );
}

#[tokio::test]
async fn show_create_missing_table_refuses_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let sql = "SHOW CREATE TABLE ice.sales.nope";
    assert_eq!(
        plan_error_message(&execution_error(&ctx, &catalogs, sql).await, sql),
        MISSING_TABLE_MESSAGE
    );
}

#[tokio::test]
async fn show_create_table_without_a_name_is_a_loud_parse_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let sql = "SHOW CREATE TABLE";
    assert_invalid_show_create_table_error(execution_error(&ctx, &catalogs, sql).await, sql);
}

#[tokio::test]
async fn show_create_lexical_and_trailing_failures_stay_parse_class_errors() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SHOW CREATE TABLE `ice.sales",
        "SHOW CREATE TABLE ice.sales.`t",
        "SHOW CREATE TABLE ice.sales.'t",
        "SHOW CREATE TABLE ice.sales.\"t",
        "SHOW CREATE TABLE ice.sales.t extra",
        "SHOW CREATE TABLE ice.sales.t AS JSON",
    ] {
        assert_invalid_show_create_table_error(execution_error(&ctx, &catalogs, sql).await, sql);
    }
}

#[tokio::test]
async fn show_create_unclosed_bracketed_comments_keep_spark_parse_class() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SHOW CREATE TABLE /* c sc.sales.t",
        "SHOW CREATE TABLE sc.sales.t /* c",
        "SHOW CREATE TABLE sc.sales.t /*",
        "SHOW CREATE TABLE sc.sales.t AS SERDE /* c",
    ] {
        let error = execution_error(&ctx, &catalogs, sql).await;
        assert!(
            matches!(&error, DataFusionError::SQL(_, _)),
            "{sql}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE,
            "{sql}"
        );
        assert_eq!(
            parse_error_message(error, sql),
            UNCLOSED_BRACKETED_COMMENT_MESSAGE,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn show_create_unclosed_before_table_keywords_use_spark_parse_contract() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SHOW CREATE /* c TABLE sc.sales.t",
        "SHOW /* c CREATE TABLE sc.sales.t",
        "/* c SHOW CREATE TABLE sc.sales.t",
    ] {
        let error = execution_error(&ctx, &catalogs, sql).await;
        assert!(
            matches!(&error, DataFusionError::SQL(_, _)),
            "{sql}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE,
            "{sql}"
        );
        assert_eq!(
            parse_error_message(error, sql),
            UNCLOSED_BRACKETED_COMMENT_MESSAGE,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn show_create_multi_statement_keeps_spark_invalid_statement_class() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let sql = "SHOW CREATE TABLE sc.sales.t; SELECT 1";
    let error = execution_error(&ctx, &catalogs, sql).await;
    assert!(matches!(&error, DataFusionError::SQL(_, _)));
    assert_eq!(
        error.to_string(),
        INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE
    );
    assert_eq!(
        parse_error_message(error, sql),
        INVALID_SHOW_CREATE_TABLE_MESSAGE
    );
}

#[tokio::test]
async fn show_create_four_part_name_refuses_as_a_missing_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let sql = "SHOW CREATE TABLE ice.sales.x.t";
    assert_eq!(
        plan_error_message(&execution_error(&ctx, &catalogs, sql).await, sql),
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`x`.`t` cannot be found. \
         Verify the spelling and correctness of the schema and catalog. If you did not qualify the \
         name with a schema, verify the current_schema() output, or qualify the name with the \
         correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP \
         TABLE IF EXISTS. SQLSTATE: 42P01"
    );
}

#[tokio::test]
async fn show_create_table_view_keeps_its_current_analysis_refusal() {
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
    let sql = "SHOW CREATE TABLE ice.sales.v";
    assert_eq!(
        plan_error_message(&execution_error(&ctx, &catalogs, sql).await, sql),
        "SHOW CREATE TABLE is not supported unless information_schema is enabled"
    );
}

#[tokio::test]
async fn show_create_view_keeps_its_current_unsupported_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let sql = "SHOW CREATE VIEW ice.sales.v";
    let error = execution_error(&ctx, &catalogs, sql).await;
    assert!(matches!(&error, DataFusionError::NotImplemented(_)));
    assert_eq!(
        error.to_string(),
        "This feature is not implemented: Only `SHOW CREATE TABLE  ...` statement is supported"
    );
}

#[tokio::test]
async fn show_create_without_an_object_keeps_its_current_parse_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let sql = "SHOW CREATE";
    let error = execution_error(&ctx, &catalogs, sql).await;
    assert!(matches!(&error, DataFusionError::SQL(_, _)));
    assert_eq!(
        error.to_string(),
        "SQL error: ParserError(\"Expected: one of TABLE or TRIGGER or FUNCTION or PROCEDURE or EVENT or VIEW, found: EOF\")"
    );
}

#[tokio::test]
async fn show_tables_matches_the_complete_spark_row_and_schema() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
    let sql = "SHOW TABLES IN ice.sales";
    let batches = execute(&ctx, &catalogs, sql)
        .await
        .expect(sql)
        .collect()
        .await
        .expect(sql);
    assert_eq!(batches.len(), 1, "{sql}");
    let batch = batches.first().expect(sql);
    assert_eq!(
        batch.schema().as_ref(),
        &Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
        ])
    );
    let namespaces = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect(sql);
    let table_names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect(sql);
    let temporary = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect(sql);
    assert_eq!(
        (0..batch.num_rows())
            .map(|index| (
                namespaces.value(index).to_string(),
                table_names.value(index).to_string(),
                temporary.value(index),
            ))
            .collect::<Vec<_>>(),
        vec![("sales".to_string(), "t".to_string(), false)]
    );
}

#[tokio::test]
async fn show_columns_matches_the_complete_spark_row_and_schema() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
    let sql = "SHOW COLUMNS IN ice.sales.t";
    let batches = execute(&ctx, &catalogs, sql)
        .await
        .expect(sql)
        .collect()
        .await
        .expect(sql);
    assert_eq!(batches.len(), 1, "{sql}");
    let batch = batches.first().expect(sql);
    assert_eq!(
        batch.schema().as_ref(),
        &Schema::new(vec![Field::new("col_name", DataType::Utf8, false)])
    );
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect(sql);
    assert_eq!(
        (0..batch.num_rows())
            .map(|index| names.value(index).to_string())
            .collect::<Vec<_>>(),
        vec!["id".to_string()]
    );
}

#[tokio::test]
async fn show_tblproperties_keeps_its_current_analysis_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
    let sql = "SHOW TBLPROPERTIES ice.sales.t";
    assert_eq!(
        plan_error_message(&execution_error(&ctx, &catalogs, sql).await, sql),
        "SHOW [VARIABLE] is not supported unless information_schema is enabled"
    );
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
    let (schema, rows) = describe_extended_rows(&ctx, &catalogs, "ice.sales.de").await;
    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
        ])
    );
    let column_end = rows
        .iter()
        .position(|(name, _, _)| name.is_empty() || name.starts_with('#'))
        .unwrap();
    assert_eq!(
        &rows[..column_end],
        &[
            ("id".to_string(), "bigint".to_string(), String::new()),
            ("data".to_string(), "string".to_string(), String::new())
        ]
    );
    assert_eq!(
        rows.iter().find(|(name, _, _)| name == "Table Properties"),
        Some(&(
            "Table Properties".to_string(),
            "[current-snapshot-id=none,format=iceberg/parquet,format-version=2,\
             write.parquet.compression-codec=zstd]"
                .to_string(),
            String::new(),
        ))
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
    let (schema, rows) = describe_extended_rows(&ctx, &catalogs, "ice.sales.doc").await;
    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
        ])
    );
    let column_end = rows
        .iter()
        .position(|(name, _, _)| name.is_empty() || name.starts_with('#'))
        .unwrap();
    assert_eq!(
        &rows[..column_end],
        &[("Comment".to_string(), "string".to_string(), String::new())]
    );
    let detail = rows
        .iter()
        .position(|(name, _, _)| name == "# Detailed Table Information")
        .unwrap();
    assert_eq!(
        &rows[detail + 1..detail + 5],
        &[
            (
                "Name".to_string(),
                "ice.sales.doc".to_string(),
                String::new()
            ),
            ("Type".to_string(), "MANAGED".to_string(), String::new()),
            (
                "Comment".to_string(),
                "tab'le doc".to_string(),
                String::new()
            ),
            (
                "Location".to_string(),
                location(&catalogs, "doc").await,
                String::new(),
            ),
        ]
    );
    assert_eq!(
        rows.iter().find(|(name, _, _)| name == "Table Properties"),
        Some(&(
            "Table Properties".to_string(),
            "[current-snapshot-id=none,format=iceberg/parquet,format-version=2,\
             write.parquet.compression-codec=zstd]"
                .to_string(),
            String::new(),
        ))
    );
}
