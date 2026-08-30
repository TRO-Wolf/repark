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
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::NamespaceIdent;
use regex::RegexBuilder;

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, resolve_namespace};
use crate::namespace_ddl::consume_word;
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
