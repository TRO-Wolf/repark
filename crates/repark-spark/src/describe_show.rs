//! Spark namespace `DESCRIBE` and `SHOW` handlers.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::spec::{
    PartitionField, Schema as IcebergSchema, TableMetadata, Transform, Type as IcebergType,
};
use iceberg::table::Table;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use regex::RegexBuilder;

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, resolve_namespace};
use crate::metadata_tables::is_metadata_table_name;
use crate::namespace_ddl::consume_word;
use crate::spark_type_names::spark_ddl_type_name;
use repark_core::CatalogRegistry;

/// A parsed Spark `DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED] catalog.namespace`.
pub(crate) struct DescribeNamespace {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    /// `EXTENDED` was present — adds the `Properties` row (Z2).
    pub(crate) extended: bool,
}

/// Keys rendered as dedicated rows instead of inside `Properties`.
pub(crate) const RESERVED_NAMESPACE_PROPERTIES: [&str; 4] =
    ["comment", "location", "owner", "location_uri"];

/// Spark's default replacement for redacted namespace properties.
pub(crate) const REDACTION_REPLACEMENT_TEXT: &str = "*********(redacted)";

/// Recognise and parse `DESCRIBE|DESC {NAMESPACE|DATABASE|SCHEMA} [EXTENDED] catalog.namespace`.
pub(crate) fn try_parse_describe_namespace(sql: &str) -> Option<Result<DescribeNamespace>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::DESCRIBE) && !parser.parse_keyword(Keyword::DESC) {
        return None;
    }
    let is_namespace = parser.parse_keyword(Keyword::DATABASE)
        || parser.parse_keyword(Keyword::SCHEMA)
        || consume_word(&mut parser, "NAMESPACE");
    if !is_namespace {
        return None;
    }
    let mut extended = parser.parse_keyword(Keyword::EXTENDED);
    // Spark's grammar needs a namespace name, and `EXTENDED` is only the flag.
    if extended && matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        parser.prev_token();
        extended = false;
    }
    // Z6: no object name after the keyword is Spark describe of a table named namespace.
    let name = parser.parse_object_name(false).ok()?;
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return None;
    }
    Some(
        resolve_namespace(&name).map(|(catalog, namespace)| DescribeNamespace {
            catalog,
            namespace,
            extended,
        }),
    )
}

/// DESCRIBE NAMESPACE returns Spark's two-column `info_name` / `info_value` metadata frame.
/// # Errors
/// Returns a plan error when the catalog is unregistered or the namespace does not exist.
pub(crate) async fn execute_describe_namespace(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    describe: DescribeNamespace,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &describe.catalog)?;
    let ident = NamespaceIdent::new(describe.namespace.clone());
    if !handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
        return Err(DataFusionError::Plan(format!(
            "[SCHEMA_NOT_FOUND] The schema `{}` cannot be found. Verify the spelling and \
             correctness of the schema and catalog.",
            describe.namespace
        )));
    }
    let namespace = handle.get_namespace(&ident).await.map_err(iceberg_err)?;
    ctx.read_batch(describe_namespace_batch(&describe, namespace.properties())?)
}

/// Build the `info_name` / `info_value` batch for one namespace.
pub(crate) fn describe_namespace_batch(
    describe: &DescribeNamespace,
    properties: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let mut rows = vec![
        ("Catalog Name", describe.catalog.clone()),
        (
            "Namespace Name",
            quote_namespace_name_if_needed(&describe.namespace),
        ),
    ];
    if let Some(comment) = properties.get("comment") {
        rows.push(("Comment", comment.clone()));
    }
    if let Some(location) = repark_iceberg::catalog::resolve_namespace_location(properties) {
        rows.push(("Location", location.to_string()));
    }
    if let Some(owner) = properties.get("owner") {
        rows.push(("Owner", owner.clone()));
    }
    if describe.extended {
        rows.push(("Properties", render_namespace_properties(properties)));
    }

    let names: Vec<&str> = rows.iter().map(|(name, _)| *name).collect();
    let values: Vec<String> = rows.into_iter().map(|(_, value)| value).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::Utf8, false).with_metadata(HashMap::from([(
            "comment".to_string(),
            "name of the namespace info".to_string(),
        )])),
        Field::new("info_value", DataType::Utf8, true).with_metadata(HashMap::from([(
            "comment".to_string(),
            "value of the namespace info".to_string(),
        )])),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(values)),
        ],
    )?)
}

/// Render the `EXTENDED` `Properties` value in Spark's exact format.
pub(crate) fn render_namespace_properties(properties: &HashMap<String, String>) -> String {
    let mut pairs: Vec<(&String, &String)> = properties
        .iter()
        .filter(|(key, _)| !RESERVED_NAMESPACE_PROPERTIES.contains(&key.as_str()))
        .collect();
    if pairs.is_empty() {
        return String::new();
    }
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let rendered: Vec<String> = pairs
        .iter()
        .map(|(key, value)| {
            let shown = if property_is_redacted(key, value) {
                REDACTION_REPLACEMENT_TEXT
            } else {
                value.as_str()
            };
            format!("({key},{shown})")
        })
        .collect();
    format!("({})", rendered.join(", "))
}

/// Whether a namespace property's VALUE must be redacted in `DESCRIBE … EXTENDED` output.
pub(crate) fn property_is_redacted(key: &str, value: &str) -> bool {
    redaction_pattern_matches(key) || redaction_pattern_matches(value)
}

/// One side of [`property_is_redacted`]: does `text` match either default redaction pattern?
pub(crate) fn redaction_pattern_matches(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        // `spark.redaction.regex` — `(?i)secret|password|token|access[.]?key`.
        "secret",
        "password",
        "token",
        "accesskey",
        "access.key",
        // `spark.sql.redaction.options.regex` — `(?i)url`.
        "url",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Render one namespace-name part the way Spark's `NamespaceHelper.quoted` does.
pub(crate) fn quote_namespace_name_if_needed(part: &str) -> String {
    let bare = !part.is_empty()
        && part
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !part.bytes().all(|byte| byte.is_ascii_digit());
    if bare {
        return part.to_string();
    }
    format!("`{}`", part.replace('`', "``"))
}

pub(crate) struct DescribeTable {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) extended: bool,
}

pub(crate) fn try_parse_describe_table(sql: &str) -> Option<Result<DescribeTable>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::DESCRIBE) && !parser.parse_keyword(Keyword::DESC) {
        return None;
    }
    if matches!(&parser.peek_token().token, Token::Word(word) if is_namespace_head(word)) {
        return None;
    }
    let _ = parser.parse_keyword(Keyword::TABLE);
    let extended =
        parser.parse_keyword(Keyword::EXTENDED) || consume_word(&mut parser, "FORMATTED");
    let name = parser.parse_object_name(false).ok()?;
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return None;
    }
    let parts = name_parts(&name);
    let [catalog, namespace, table] = parts.as_slice() else {
        return None;
    };
    if is_metadata_table_name(table) {
        return None;
    }
    Some(Ok(DescribeTable {
        catalog: catalog.clone(),
        namespace: namespace.clone(),
        table: table.clone(),
        extended,
    }))
}

fn is_namespace_head(word: &Word) -> bool {
    word.value.eq_ignore_ascii_case("namespace")
        || word.value.eq_ignore_ascii_case("database")
        || word.value.eq_ignore_ascii_case("schema")
}

pub(crate) async fn execute_describe_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    describe: DescribeTable,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &describe.catalog)?;
    let ident = TableIdent::new(
        NamespaceIdent::new(describe.namespace.clone()),
        describe.table.clone(),
    );
    let table = match handle.load_table(&ident).await {
        Ok(table) => table,
        Err(error) if error.kind() == ErrorKind::TableNotFound => {
            return Err(DataFusionError::Plan(format!(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{}`.`{}`.`{}` cannot be found. \
                 Verify the spelling and correctness of the schema and catalog. \
                 If you did not qualify the name with a schema, verify the current_schema() output, \
                 or qualify the name with the correct schema and catalog. \
                 To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. \
                 SQLSTATE: 42P01",
                describe.catalog, describe.namespace, describe.table
            )));
        }
        Err(error) => return Err(iceberg_err(error)),
    };
    ctx.read_batch(describe_table_batch(&describe, &table)?)
}

pub(crate) fn describe_table_batch(describe: &DescribeTable, table: &Table) -> Result<RecordBatch> {
    let rows = describe_table_rows(describe, table)?;
    let mut names = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    let mut comments = Vec::with_capacity(rows.len());
    for (name, data_type, comment) in rows {
        names.push(name);
        types.push(data_type);
        comments.push(comment);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, true),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(comments)),
        ],
    )?)
}

fn describe_table_rows(
    describe: &DescribeTable,
    table: &Table,
) -> Result<Vec<(String, String, Option<String>)>> {
    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema();
    let arrow_schema =
        iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(iceberg_err)?;
    let mut rows = Vec::new();
    for (iceberg_field, arrow_field) in iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .zip(arrow_schema.fields())
    {
        rows.push((
            arrow_field.name().clone(),
            spark_ddl_type_name(arrow_field.data_type()),
            iceberg_field.doc.clone(),
        ));
    }
    let spec = metadata.default_partition_spec();
    if !spec.fields().is_empty() {
        rows.push(blank_describe_row());
        rows.push(section_describe_row("# Partitioning"));
        for (index, field) in spec.fields().iter().enumerate() {
            rows.push((
                format!("Part {index}"),
                describe_partition_field(iceberg_schema, field)?,
                Some(String::new()),
            ));
        }
    }
    if describe.extended {
        rows.push(blank_describe_row());
        rows.push(section_describe_row("# Metadata Columns"));
        rows.push(plain_describe_row("_spec_id", "int"));
        rows.push((
            "_partition".to_string(),
            describe_partition_struct_type(iceberg_schema, spec)?,
            Some(String::new()),
        ));
        rows.push(plain_describe_row("_file", "string"));
        rows.push(plain_describe_row("_pos", "bigint"));
        rows.push(plain_describe_row("_deleted", "boolean"));
        rows.push(blank_describe_row());
        rows.push(section_describe_row("# Detailed Table Information"));
        rows.push(plain_describe_row(
            "Name",
            &format!(
                "{}.{}.{}",
                describe.catalog, describe.namespace, describe.table
            ),
        ));
        rows.push(plain_describe_row("Type", "MANAGED"));
        rows.push(plain_describe_row("Location", metadata.location()));
        rows.push(plain_describe_row("Provider", "iceberg"));
        rows.push(plain_describe_row("Owner", &describe_table_owner()));
        rows.push(plain_describe_row(
            "Table Properties",
            &render_table_properties(metadata),
        ));
        rows.push((
            "Statistics".to_string(),
            describe_table_statistics(metadata),
            None,
        ));
    }
    Ok(rows)
}

fn blank_describe_row() -> (String, String, Option<String>) {
    (String::new(), String::new(), Some(String::new()))
}

fn section_describe_row(header: &str) -> (String, String, Option<String>) {
    (header.to_string(), String::new(), Some(String::new()))
}

fn plain_describe_row(name: &str, value: &str) -> (String, String, Option<String>) {
    (name.to_string(), value.to_string(), Some(String::new()))
}

fn describe_partition_field(schema: &IcebergSchema, field: &PartitionField) -> Result<String> {
    let source = schema.field_by_id(field.source_id).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "partition field `{}` refers to unknown source id {}",
            field.name, field.source_id
        ))
    })?;
    Ok(match &field.transform {
        Transform::Identity => source.name.clone(),
        Transform::Year => format!("years({})", source.name),
        Transform::Month => format!("months({})", source.name),
        Transform::Day => format!("days({})", source.name),
        Transform::Hour => format!("hours({})", source.name),
        Transform::Bucket(width) => format!("bucket({width}, {})", source.name),
        Transform::Truncate(width) => format!("truncate({width}, {})", source.name),
        Transform::Void => format!("void({})", source.name),
        Transform::Unknown => format!("unknown({})", source.name),
    })
}

fn describe_partition_struct_type(
    schema: &IcebergSchema,
    spec: &Arc<iceberg::spec::PartitionSpec>,
) -> Result<String> {
    let partition_type = spec.partition_type(schema).map_err(iceberg_err)?;
    let arrow_type = iceberg::arrow::type_to_arrow_type(&IcebergType::Struct(partition_type))
        .map_err(iceberg_err)?;
    Ok(spark_ddl_type_name(&arrow_type))
}

fn describe_table_owner() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn render_table_properties(metadata: &TableMetadata) -> String {
    let mut merged: HashMap<String, String> = metadata.properties().clone();
    merged.insert(
        "current-snapshot-id".to_string(),
        metadata
            .current_snapshot_id()
            .map_or_else(|| "none".to_string(), |id| id.to_string()),
    );
    let mut pairs: Vec<(&String, &String)> = merged.iter().collect();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let rendered: Vec<String> = pairs
        .iter()
        .map(|(key, value)| {
            let shown = if property_is_redacted(key, value) {
                REDACTION_REPLACEMENT_TEXT
            } else {
                value.as_str()
            };
            format!("{key}={shown}")
        })
        .collect();
    format!("[{}]", rendered.join(","))
}

fn describe_table_statistics(metadata: &TableMetadata) -> String {
    let Some(snapshot) = metadata.current_snapshot() else {
        return "0 bytes, 0 rows".to_string();
    };
    let summary = &snapshot.summary().additional_properties;
    let records = summary
        .get("total-records")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let bytes = summary
        .get("total-files-size")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    format!("{bytes} bytes, {records} rows")
}

/// A parsed Spark `SHOW {NAMESPACES|SCHEMAS|DATABASES} [{IN|FROM} catalog] [LIKE] ['pattern']`.
pub(crate) struct ShowNamespaces {
    /// The catalog named by `IN`/`FROM`.
    catalog: String,
    /// The `LIKE` pattern, unevaluated.
    pattern: Option<String>,
}

/// Parse `SHOW NAMESPACES|SCHEMAS|DATABASES` with optional IN/FROM catalog and LIKE pattern.
pub(crate) fn try_parse_show_namespaces(sql: &str) -> Option<Result<ShowNamespaces>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::SHOW) {
        return None;
    }
    let is_namespaces = parser.parse_keyword(Keyword::SCHEMAS)
        || parser.parse_keyword(Keyword::DATABASES)
        || consume_word(&mut parser, "NAMESPACES");
    if !is_namespaces {
        return None;
    }
    Some(parse_show_namespaces_tail(&mut parser))
}

/// The committed half of [`try_parse_show_namespaces`]: everything after the statement head.
pub(crate) fn parse_show_namespaces_tail(parser: &mut Parser) -> Result<ShowNamespaces> {
    let scope = if parser.parse_keyword(Keyword::IN) || parser.parse_keyword(Keyword::FROM) {
        Some(
            parser
                .parse_object_name(false)
                .map_err(show_namespaces_err)?,
        )
    } else {
        None
    };
    let had_like = parser.parse_keyword(Keyword::LIKE);
    let pattern = match &parser.peek_token().token {
        Token::SingleQuotedString(text) | Token::DoubleQuotedString(text) => {
            let text = text.clone();
            parser.next_token();
            Some(text)
        }
        _ => None,
    };
    if had_like && pattern.is_none() {
        return Err(DataFusionError::Plan(
            "SHOW NAMESPACES … LIKE needs a quoted pattern (e.g. SHOW NAMESPACES IN cat LIKE \
             'sales*')"
                .to_string(),
        ));
    }
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Err(DataFusionError::Plan(format!(
            "could not parse `SHOW NAMESPACES` at `{}` — the supported form is \
             SHOW {{NAMESPACES|SCHEMAS|DATABASES}} {{IN|FROM}} <catalog> [LIKE] ['pattern']",
            parser.peek_token()
        )));
    }
    // Spark resolves a missing `IN` against the CURRENT catalog.
    let Some(name) = scope else {
        return Err(DataFusionError::Plan(
            "SHOW NAMESPACES requires an explicit catalog — `SHOW NAMESPACES IN <catalog>` \
             (RePark has no current-catalog concept, so there is no default to resolve against)"
                .to_string(),
        ));
    };
    let parts = name_parts(&name);
    let [catalog] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "expected a one-part `IN <catalog>` name, got `{name}` — RePark namespaces are \
             single-level, so there are no nested namespaces to list under `{name}`"
        )));
    };
    Ok(ShowNamespaces {
        catalog: catalog.clone(),
        pattern,
    })
}

/// Fold a sqlparser error from the `SHOW NAMESPACES` tail into a plan-class [`DataFusionError`].
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn show_namespaces_err(err: ParserError) -> DataFusionError {
    DataFusionError::Plan(format!("could not parse SHOW NAMESPACES: {err}"))
}

/// `SHOW NAMESPACES`, `SHOW SCHEMAS`, and `SHOW DATABASES` return a one-column namespace frame.
/// # Errors
/// Returns a plan error when the catalog is not registered, and propagates listing failures.
pub(crate) async fn execute_show_namespaces(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    show: ShowNamespaces,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &show.catalog)?;
    let namespaces = handle.list_namespaces(None).await.map_err(iceberg_err)?;
    let rows = show_namespace_rows(&namespaces, show.pattern.as_deref());
    ctx.read_batch(show_namespaces_batch(rows)?)
}

/// Spark's `ShowNamespacesExec.run` body: render every namespace.
pub(crate) fn show_namespace_rows(
    namespaces: &[NamespaceIdent],
    pattern: Option<&str>,
) -> Vec<String> {
    namespaces
        .iter()
        .map(quoted_namespace)
        .filter(|rendered| pattern.is_none_or(|pattern| filter_pattern_matches(rendered, pattern)))
        .collect()
}

/// Build the one-column `namespace` batch (schema per [`execute_show_namespaces`]).
pub(crate) fn show_namespaces_batch(rows: Vec<String>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "namespace",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(rows))],
    )?)
}

/// Render a namespace the way Spark's `NamespaceHelper.quoted` does.
pub(crate) fn quoted_namespace(namespace: &NamespaceIdent) -> String {
    namespace
        .iter()
        .map(|part| quote_namespace_name_if_needed(part))
        .collect::<Vec<String>>()
        .join(".")
}

/// Spark's `StringUtils.filterPattern` is a case-insensitive Java-regex matcher, not SQL `LIKE`.
pub(crate) fn filter_pattern_matches(name: &str, pattern: &str) -> bool {
    pattern.trim().split('|').any(|alternative| {
        // Keep each alternative unwrapped so invalid syntax stays invalid.
        RegexBuilder::new(&format!(r"\A{}\z", alternative.replace('*', ".*")))
            .case_insensitive(true)
            .build()
            .is_ok_and(|regex| regex.is_match(name))
    })
}
