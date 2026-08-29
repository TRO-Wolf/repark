//! Spark token normalizers, statement sniffers, DML valves, and partition classification.
//!
//! The valves refuse unsafe multi-spec merge-on-read DML and subquery predicates before DataFusion
//! can lose the predicate at its planning boundary.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, FromTable, ObjectName, Query, Statement, TableFactor, TableWithJoins, Value, Visit,
    Visitor,
};
use datafusion::sql::sqlparser::dialect::{DatabricksDialect, GenericDialect};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::{Transform, UnboundPartitionSpec};
use iceberg::{NamespaceIdent, TableIdent};

use repark_core::CatalogRegistry;

use crate::alter;
use crate::catalog_ops::{iceberg_err, name_parts};
use crate::merge;

/// True when the statement's first keyword token is `MERGE` — tokenizer-based (the same pattern
/// as `is_create_table`), so leading whitespace/comments and multi-byte text are handled without
/// byte-offset slicing.
pub(crate) fn starts_with_merge(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    is_merge(&tokens)
}

/// Detect Iceberg snapshot-ref DDL that stock sqlparser cannot model, while skipping the
/// multipart table identifier before matching the clause.
pub(crate) fn starts_with_branch_or_tag_ddl(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    // Significant tokens only — whitespace (incl. comments) and EOF out; Period retained so
    // multipart table names stay structured (O4-C1-L-001).
    let significant: Vec<&Token> = tokens
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();

    let word_at = |index: usize| -> Option<&str> {
        match significant.get(index) {
            Some(Token::Word(word)) => Some(word.value.as_str()),
            _ => None,
        }
    };
    let is_period_at =
        |index: usize| -> bool { matches!(significant.get(index), Some(Token::Period)) };
    let is_branch_or_tag =
        |word: &str| word.eq_ignore_ascii_case("BRANCH") || word.eq_ignore_ascii_case("TAG");
    let is_create_or_replace_branch_or_tag = |start: usize| -> bool {
        word_at(start).is_some_and(|w| w.eq_ignore_ascii_case("CREATE"))
            && word_at(start + 1).is_some_and(|w| w.eq_ignore_ascii_case("OR"))
            && word_at(start + 2).is_some_and(|w| w.eq_ignore_ascii_case("REPLACE"))
            && word_at(start + 3).is_some_and(is_branch_or_tag)
    };
    let is_simple_branch_or_tag_verb = |start: usize| -> bool {
        word_at(start).is_some_and(|verb| {
            verb.eq_ignore_ascii_case("CREATE")
                || verb.eq_ignore_ascii_case("DROP")
                || verb.eq_ignore_ascii_case("REPLACE")
        }) && word_at(start + 1).is_some_and(is_branch_or_tag)
    };

    if significant.len() < 2 {
        return false;
    }

    // CREATE BRANCH|TAG …  /  CREATE OR REPLACE BRANCH|TAG …
    if word_at(0).is_some_and(|w| w.eq_ignore_ascii_case("CREATE")) {
        if word_at(1).is_some_and(is_branch_or_tag) {
            return true;
        }
        if is_create_or_replace_branch_or_tag(0) {
            return true;
        }
    }

    // DROP BRANCH|TAG …
    if word_at(0).is_some_and(|w| w.eq_ignore_ascii_case("DROP"))
        && word_at(1).is_some_and(is_branch_or_tag)
    {
        return true;
    }

    // ALTER TABLE <multipart-name> <branch-tag-clause>
    if !(word_at(0).is_some_and(|w| w.eq_ignore_ascii_case("ALTER"))
        && word_at(1).is_some_and(|w| w.eq_ignore_ascii_case("TABLE")))
    {
        return false;
    }
    // Skip the table object name: Ident (Period Ident)*
    let mut index = 2usize;
    if word_at(index).is_none() {
        return false;
    }
    index += 1;
    while is_period_at(index) && word_at(index + 1).is_some() {
        index += 2;
    }
    // Clause after the table name only — never scan inside the identifier (O4-C1-L-001).
    if is_create_or_replace_branch_or_tag(index) || is_simple_branch_or_tag_verb(index) {
        return true;
    }
    false
}

/// Token-level MERGE check for the normalizer pipeline (first significant token is the `MERGE`
/// keyword).
pub(crate) fn is_merge(tokens: &[Token]) -> bool {
    tokens
        .iter()
        .find_map(|token| match token {
            Token::Word(word) => Some(word.keyword == Keyword::MERGE),
            Token::Whitespace(_) => None,
            _ => Some(false),
        })
        .unwrap_or(false)
}

/// Parse one statement with Spark-isms normalized. Unsupported or unrecognized forms fall through
/// to DataFusion passthrough.
///
/// # Errors
/// A `CREATE TABLE` whose `PARTITIONED BY` clause is malformed (unbalanced parens, empty list,
/// duplicate clause, an unrecognisable element) errors loudly — Spark parse-rejects those forms
/// too, and falling through to the passthrough would only produce an opaque parse error.
pub(crate) fn parse_single_normalized(
    sql: &str,
) -> Result<Option<(Statement, Vec<PartitionedByElement>)>> {
    // Strip Spark's USING clause and extract CTAS partitioning before the stock parser runs.
    let dialect = DatabricksDialect {};
    let Ok(mut tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(None);
    };
    let mut partitioning = Vec::new();
    if is_create_table(&tokens) {
        tokens = strip_create_table_using(&tokens);
        (tokens, partitioning) = extract_partitioned_by(&tokens)?;
    }
    tokens = rewrite_namespace_to_schema(&tokens);
    tokens = alter::rewrite_unset_tblproperties(&tokens);
    tokens = alter::rewrite_add_columns_plural(&tokens);
    tokens = alter::rewrite_drop_columns_plural(&tokens);
    if is_merge(&tokens) {
        tokens = merge::rewrite_merge_stars(&tokens);
    }
    // ALTER TABLE uses GenericDialect so Spark `ADD COLUMN … FIRST|AFTER x` fills
    // `MySQLColumnPosition` (Databricks dialect leaves those tokens unparsed — I6).
    let generic = GenericDialect {};
    let parse_dialect: &dyn datafusion::sql::sqlparser::dialect::Dialect =
        if alter::tokens_are_alter_table(&tokens) {
            &generic
        } else {
            &dialect
        };
    let Ok(mut statements) = Parser::new(parse_dialect)
        .with_tokens(tokens)
        .parse_statements()
    else {
        return Ok(None);
    };
    // BUG-010 defense-in-depth: multi-statement after normalisers still refuse as Parse.
    // (Primary gate is `refuse_multi_statement_sql` at the top of `execute_inner`.)
    if statements.len() > 1 {
        return Err(multi_statement_parse_error());
    }
    if statements.len() == 1 {
        Ok(statements.pop().map(|statement| (statement, partitioning)))
    } else {
        Ok(None)
    }
}

// === Multi-statement and merge-on-read DML safety gates ================================

/// Refuse multiple statements. Spark raises
/// `ParseException` / `[PARSE_SYNTAX_ERROR]` for `SELECT 1; SELECT 2` while allowing a single
/// statement with trailing `;`, whitespace, or line/block comments.
///
/// # Errors
/// [`DataFusionError::SQL`] (→ session `Error::Parse` → Python `ParseException`) when more than
/// one non-empty statement is present, or when a semicolon is followed by non-trailing content
/// even if the second statement fails to parse (fail-closed).
pub(crate) fn refuse_multi_statement_sql(sql: &str) -> Result<()> {
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        // Un-tokenizable input is not a multi-statement claim — fall through to existing paths.
        return Ok(());
    };
    match Parser::new(&dialect)
        .with_tokens(tokens.clone())
        .parse_statements()
    {
        Ok(statements) if statements.len() > 1 => Err(multi_statement_parse_error()),
        Ok(_) => Ok(()),
        Err(_) => {
            // Fail-closed: `;` + non-ws/comment/extra-`;` content → multi-statement class refuse
            // (covers `SELECT 1; XYZZY 2` where parse_statements errors after the first stmt).
            if tokens_have_nontrailing_content_after_semicolon(&tokens) {
                Err(multi_statement_parse_error())
            } else {
                Ok(())
            }
        }
    }
}

/// True when a `;` token is followed later by any non-whitespace, non-`;`, non-EOF token.
/// Comments are `Token::Whitespace` in sqlparser, so trailing `; -- c` / `; /*c*/` stay allowed.
pub(crate) fn tokens_have_nontrailing_content_after_semicolon(tokens: &[Token]) -> bool {
    let mut saw_semicolon = false;
    for token in tokens {
        match token {
            Token::EOF => break,
            Token::Whitespace(_) => {}
            Token::SemiColon => {
                saw_semicolon = true;
            }
            _ if saw_semicolon => return true,
            _ => {}
        }
    }
    false
}

/// Parse-class error matching Spark's multi-statement refuse (error class `PARSE_SYNTAX_ERROR`).
pub(crate) fn multi_statement_parse_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not \
             supported (Spark parity). Only a single statement is accepted; a trailing \
             semicolon, whitespace, or comment after that statement is allowed"
                .to_string(),
        )),
        None,
    )
}

// The valve verb enum is owned by repark-iceberg beside the position-delete path it gates.
// Re-export it so sibling callers keep one local type name.
pub(crate) use repark_iceberg::write::MorDmlKind;

/// [`ObjectName`] from a `TableWithJoins` primary relation (strips aliases — BUG-001 under-refuse fix).
pub(crate) fn object_name_from_table_with_joins(table: &TableWithJoins) -> Option<&ObjectName> {
    match &table.relation {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

/// Target table [`ObjectName`] from a parsed DELETE (`tables` multi-delete form, else FROM relation).
pub(crate) fn delete_target_object_name(
    delete: &datafusion::sql::sqlparser::ast::Delete,
) -> Option<&ObjectName> {
    if let Some(name) = delete.tables.first() {
        return Some(name);
    }
    let from_tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    from_tables
        .first()
        .and_then(object_name_from_table_with_joins)
}

/// ===========================================================================================
/// Resolve the DML target and delegate the merge-on-read hazard predicate to the fork safety valve.
///
/// Use [`ObjectName`] parts so aliases cannot bypass the hazard gate. Nested namespaces resolve via
/// [`NamespaceIdent::from_vec`].
///
/// # Errors
/// [`DataFusionError::Plan`] naming the fork hazard and copy-on-write / `MERGE` workarounds.
/// ===========================================================================================
pub(crate) async fn refuse_mor_unpartitioned_multi_spec_dml(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: Option<&ObjectName>,
    kind: MorDmlKind,
) -> Result<()> {
    let Some(table_name) = table_name else {
        return Ok(());
    };
    let Some((catalog_name, ident)) = dml_target_ident(ctx, table_name) else {
        return Ok(());
    };
    let Some(catalog) = catalogs.get(&catalog_name) else {
        // Unknown catalog handled elsewhere; cannot inspect hazard without a handle.
        return Ok(());
    };
    repark_iceberg::write::refuse_mor_unpartitioned_multi_spec_dml(
        catalog.as_ref(),
        &ident,
        &table_name.to_string(),
        kind,
    )
    .await
}

/// V3R-1 valve (`V3-COW-1`) for the plain-`WHERE` DELETE / UPDATE; runs after the BUG-001 valve.
pub(crate) async fn refuse_v3_cow_dml(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: Option<&ObjectName>,
    kind: MorDmlKind,
) -> Result<()> {
    let Some(table_name) = table_name else {
        return Ok(());
    };
    let Some((catalog_name, ident)) = dml_target_ident(ctx, table_name) else {
        return Ok(());
    };
    let Some(catalog) = catalogs.get(&catalog_name) else {
        return Ok(());
    };
    repark_iceberg::write::refuse_v3_cow_dml(catalog.as_ref(), &ident, kind).await
}

/// Resolve a DML target as DataFusion will: short names complete from the session defaults
/// (SEC-001); `None` when empty or the namespace cannot be built.
fn dml_target_ident(ctx: &SessionContext, table_name: &ObjectName) -> Option<(String, TableIdent)> {
    let mut parts = name_parts(table_name);
    if parts.is_empty() {
        return None;
    }
    if parts.len() < 3 {
        let (default_catalog, default_schema) = session_defaults(ctx);
        if parts.len() == 1 {
            parts.insert(0, default_schema);
        }
        parts.insert(0, default_catalog);
    }
    let namespace = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()).ok()?;
    let table_leaf = parts[parts.len() - 1].clone();
    Some((parts[0].clone(), TableIdent::new(namespace, table_leaf)))
}

/// The DML verb a G3-E8 subquery-predicate refusal names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DmlSubqueryVerb {
    /// SQL `DELETE`.
    Delete,
    /// SQL `UPDATE`.
    Update,
}

impl DmlSubqueryVerb {
    /// The SQL verb, for the refusal message.
    const fn verb(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
        }
    }

    /// What the statement would silently do today, for the refusal message.
    const fn consequence(self) -> &'static str {
        match self {
            Self::Delete => "delete EVERY row of the table",
            Self::Update => "update EVERY row of the table",
        }
    }

    /// The `MERGE INTO` arm the workaround uses, for the refusal message.
    const fn merge_action(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE SET <assignments>",
        }
    }
}

/// ===========================================================================================
/// G3-E8 valve — refuse `DELETE` or `UPDATE` predicates containing subqueries.
///
/// DataFusion can decorrelate such predicates before the provider receives them, leaving an empty
/// filter that means match-all. The fail-safe guard is syntactic and slightly wide; the executing
/// parse in [`crate::spark_ast::execute_passthrough`] is authoritative.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] naming the defect class, the `MERGE INTO` workaround, and that
/// support returns with the fix.
pub(crate) fn refuse_dml_subquery_predicate(
    verb: DmlSubqueryVerb,
    selection: Option<&Expr>,
    table: &str,
) -> Result<()> {
    let Some(selection) = selection else {
        return Ok(());
    };
    if !expression_contains_subquery(selection) {
        return Ok(());
    }
    Err(DataFusionError::Plan(dml_subquery_refusal_message(
        verb, table,
    )))
}

/// ===========================================================================================
/// Apply the G3-E8 valve to the statement parsed for execution. The target comes from the parsed
/// object name, and the allow-list must match the complete statement shape.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] — the same G3-E8 refusal [`refuse_dml_subquery_predicate`] renders.
pub(crate) fn refuse_dml_subquery_predicate_in_statement(statement: &Statement) -> Result<()> {
    // Full-statement allow-list only — never skip on the selection shape alone (USING /
    // RETURNING / 1-part names must stay fail-closed, never DataFusion DML).
    if repark_iceberg::write::predicate_dml::try_allowed_delete_in(statement)?.is_some()
        || repark_iceberg::write::predicate_dml::try_allowed_update_in(statement)?.is_some()
    {
        return Ok(());
    }
    match statement {
        Statement::Delete(delete) => refuse_dml_subquery_predicate(
            DmlSubqueryVerb::Delete,
            delete.selection.as_ref(),
            &delete_target_object_name(delete)
                .map_or_else(|| "<table>".to_string(), ToString::to_string),
        ),
        Statement::Update(update) => refuse_dml_subquery_predicate(
            DmlSubqueryVerb::Update,
            update.selection.as_ref(),
            &object_name_from_table_with_joins(&update.table)
                .map_or_else(|| update.table.to_string(), ToString::to_string),
        ),
        _ => Ok(()),
    }
}

/// True when a `Query` node appears anywhere inside `expr` — i.e. the expression carries a
/// subquery at any depth (`IN (…)`, `NOT IN`, `EXISTS`, `ANY`/`ALL`, a scalar `(SELECT …)`,
/// nested under `NOT` / `OR` / a function argument, or inside another subquery).
fn expression_contains_subquery(expr: &Expr) -> bool {
    struct SawSubquery;
    struct SubqueryProbe;
    impl Visitor for SubqueryProbe {
        type Break = SawSubquery;

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            ControlFlow::Break(SawSubquery)
        }
    }
    expr.visit(&mut SubqueryProbe).is_break()
}

/// The G3-E8 refusal text (pinned by tests in BOTH doors — the needle the parity corpus asserts
/// is `subquery predicates are silently mis-executed`).
fn dml_subquery_refusal_message(verb: DmlSubqueryVerb, table: &str) -> String {
    format!(
        "{verb_name} with a subquery in its WHERE clause is refused on `{table}`: subquery \
         predicates are silently mis-executed today — DataFusion's DML planner decorrelates \
         IN / NOT IN / EXISTS / ANY / ALL / correlated predicates into a semi-join and then \
         recovers NO filter for the Iceberg writer, so this statement would \
         {consequence} instead of only the matching ones (defect G3-E8, silent data loss). \
         Rewrite it as `MERGE INTO {table} AS target USING (<the subquery>) AS source \
         ON <join keys> WHEN MATCHED THEN {action}` — the RePark-owned MERGE executor never \
         crosses that seam, and it is the dbt adapter's proven vehicle. Support returns when the \
         underlying fix lands; non-subquery {verb_name} predicates are unaffected.",
        verb_name = verb.verb(),
        consequence = verb.consequence(),
        action = verb.merge_action(),
    )
}

/// True if the token stream begins a `CREATE [OR REPLACE] [TEMPORARY] TABLE …` statement.
pub(crate) fn is_create_table(tokens: &[Token]) -> bool {
    let keywords: Vec<Keyword> = tokens
        .iter()
        .filter_map(|token| match token {
            Token::Word(word) => Some(word.keyword),
            _ => None,
        })
        .take(6)
        .collect();
    keywords.first() == Some(&Keyword::CREATE) && keywords.contains(&Keyword::TABLE)
}

/// The CTAS `AS` boundary — the position of the first `AS` keyword (or the end of the stream).
/// Clause normalisers (`strip_create_table_using`, `extract_partitioned_by`) only act BEFORE it,
/// so `JOIN … USING` / window `PARTITION BY` shapes inside the SELECT are structurally out of
/// reach.
pub(crate) fn ctas_as_boundary(tokens: &[Token]) -> usize {
    tokens
        .iter()
        .position(|token| matches!(token, Token::Word(word) if word.keyword == Keyword::AS))
        .unwrap_or(tokens.len())
}

/// Strip the Spark `USING <provider>` data-source clause (which stock sqlparser cannot parse) from a
/// CREATE TABLE statement. Only the occurrence before the CTAS `AS` is removed, so a `JOIN … USING`
/// inside the SELECT is left intact. We always create Iceberg tables, so the provider is advisory.
pub(crate) fn strip_create_table_using(tokens: &[Token]) -> Vec<Token> {
    let boundary = ctas_as_boundary(tokens);
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let is_using = matches!(&tokens[i], Token::Word(word) if word.keyword == Keyword::USING);
        if i < boundary && is_using {
            // Skip `USING`, any whitespace, and the following provider word.
            let mut j = i + 1;
            while j < tokens.len() && matches!(tokens[j], Token::Whitespace(_)) {
                j += 1;
            }
            if j < tokens.len() && matches!(tokens[j], Token::Word(_)) {
                i = j + 1;
                continue;
            }
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

/// One element of a Spark `CREATE TABLE … PARTITIONED BY (…)` clause, classified by token shape.
/// Bare names are identity transforms, call forms are named transforms, and typed definitions are
/// rejected by CTAS. Classification keeps unsupported shapes fail-closed in [`build_ctas`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PartitionedByElement {
    /// A bare top-level column reference — an identity partition field.
    Identity(String),
    /// A transform call `name(args…)` (`bucket(4, c)` / `days(ts)` / `truncate(10, c)` / …).
    /// The transform NAME and its rendered arguments are carried through so [`build_ctas`] can
    /// map them onto a real Iceberg `Transform` (validating bucket/truncate width `> 0`).
    Transform { name: String, args: Vec<String> },
    /// A Hive-style typed column def (`name TYPE …`) — carried so the reject can mirror Spark's
    /// "Partition column types may not be specified in Create Table As Select (CTAS)".
    Typed(String),
    /// A multipart reference (`a.b`) — nested-field partitioning, gated `NotImplemented` in v1.
    Nested(String),
}

/// A validated CTAS partition field: the source column plus the transform to apply. Built by
/// [`build_ctas`] from the classified [`PartitionedByElement`]s (transform arguments parsed and
/// validated — bucket/truncate width `> 0`, arity checked) and consumed by
/// [`build_partition_spec`], which resolves the column against the derived schema and builds the
/// real `Transform` + Java-parity partition-field name (`col` / `col_bucket` / `col_trunc` /
/// `col_year` / `col_month` / `col_day` / `col_hour`; apache-iceberg-1.10.0 `PartitionSpec.java`
/// `Builder.{identity,bucket,truncate,year,month,day,hour}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PartitionFieldSpec {
    Identity(String),
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Year(String),
    Month(String),
    Day(String),
    Hour(String),
}

impl PartitionFieldSpec {
    /// The source column this field partitions on.
    pub(crate) fn column(&self) -> &str {
        match self {
            PartitionFieldSpec::Identity(c)
            | PartitionFieldSpec::Bucket { column: c, .. }
            | PartitionFieldSpec::Truncate { column: c, .. }
            | PartitionFieldSpec::Year(c)
            | PartitionFieldSpec::Month(c)
            | PartitionFieldSpec::Day(c)
            | PartitionFieldSpec::Hour(c) => c,
        }
    }

    /// The Iceberg `Transform` this field applies (Java `PartitionSpec.Builder` parity).
    pub(crate) fn transform(&self) -> Transform {
        match self {
            PartitionFieldSpec::Identity(_) => Transform::Identity,
            PartitionFieldSpec::Bucket { num_buckets, .. } => Transform::Bucket(*num_buckets),
            PartitionFieldSpec::Truncate { width, .. } => Transform::Truncate(*width),
            PartitionFieldSpec::Year(_) => Transform::Year,
            PartitionFieldSpec::Month(_) => Transform::Month,
            PartitionFieldSpec::Day(_) => Transform::Day,
            PartitionFieldSpec::Hour(_) => Transform::Hour,
        }
    }

    /// The partition-field NAME Java/Spark generate for this transform: the source column for an
    /// identity field, else `<col>_<suffix>` (apache-iceberg-1.10.0 `PartitionSpec.java`
    /// `Builder.bucket`→`_bucket`, `truncate`→`_trunc`, `year`→`_year`, …; `Spark3Util`
    /// routes Spark's transform expressions there). Read back by the schema-equality pins.
    fn field_name(&self) -> String {
        let column = self.column();
        match self {
            PartitionFieldSpec::Identity(_) => column.to_string(),
            PartitionFieldSpec::Bucket { .. } => format!("{column}_bucket"),
            PartitionFieldSpec::Truncate { .. } => format!("{column}_trunc"),
            PartitionFieldSpec::Year(_) => format!("{column}_year"),
            PartitionFieldSpec::Month(_) => format!("{column}_month"),
            PartitionFieldSpec::Day(_) => format!("{column}_day"),
            PartitionFieldSpec::Hour(_) => format!("{column}_hour"),
        }
    }
}

/// Map one classified transform call onto a validated [`PartitionFieldSpec`], mirroring Spark's
/// `AstBuilder`/`Spark3Util.toPartitionSpec` transform surface. `bucket`/`truncate` take
/// `(width, column)` with a width parsed as a positive integer (Spark + Java `Bucket.get` /
/// `Truncate.get` reject `<= 0` as an ANALYSIS error — the engine rejects it LOUD here, before any
/// table is created, never a panic); the temporal transforms (`year[s]`/`month[s]`/`day[s]`/
/// `hour[s]`) and `identity` take a single column. An unknown transform name, a wrong argument
/// count, or a non-numeric/`<= 0` width is a loud typed error naming the offending form.
pub(crate) fn build_transform_field(name: &str, args: &[String]) -> Result<PartitionFieldSpec> {
    let lower = name.to_ascii_lowercase();
    let arity_err = |want: &str| {
        DataFusionError::Plan(format!(
            "CTAS PARTITIONED BY `{name}(…)` expects {want}, got {} argument(s): [{}]",
            args.len(),
            args.join(", ")
        ))
    };
    let positive_width = |raw: &str, label: &str| -> Result<u32> {
        let parsed: i64 = raw.trim().parse().map_err(|_| {
            DataFusionError::Plan(format!(
                "CTAS PARTITIONED BY `{name}(…)` {label} must be an integer, got `{raw}`"
            ))
        })?;
        if parsed <= 0 {
            return Err(DataFusionError::Plan(format!(
                "CTAS PARTITIONED BY `{name}({raw}, …)` {label} must be > 0 (Spark/Iceberg reject \
                 `{label} = {parsed}` as an analysis error), got `{parsed}`"
            )));
        }
        u32::try_from(parsed).map_err(|_| {
            DataFusionError::Plan(format!(
                "CTAS PARTITIONED BY `{name}(…)` {label} `{parsed}` is too large (max {})",
                u32::MAX
            ))
        })
    };
    match lower.as_str() {
        "bucket" => {
            let [width, column] = args else {
                return Err(arity_err("(numBuckets, column)"));
            };
            Ok(PartitionFieldSpec::Bucket {
                column: column.clone(),
                num_buckets: positive_width(width, "numBuckets")?,
            })
        }
        "truncate" => {
            let [width, column] = args else {
                return Err(arity_err("(width, column)"));
            };
            Ok(PartitionFieldSpec::Truncate {
                column: column.clone(),
                width: positive_width(width, "width")?,
            })
        }
        "year" | "years" | "month" | "months" | "day" | "days" | "hour" | "hours" | "identity" => {
            let [column] = args else {
                return Err(arity_err("a single (column)"));
            };
            let column = column.clone();
            Ok(match lower.as_str() {
                "year" | "years" => PartitionFieldSpec::Year(column),
                "month" | "months" => PartitionFieldSpec::Month(column),
                "day" | "days" => PartitionFieldSpec::Day(column),
                "hour" | "hours" => PartitionFieldSpec::Hour(column),
                _ => PartitionFieldSpec::Identity(column),
            })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "CTAS PARTITIONED BY transform `{name}(…)` is not a supported partition transform \
             (supported: bucket, truncate, year[s], month[s], day[s], hour[s], identity)"
        ))),
    }
}

/// Extract the Spark `PARTITIONED BY ( … )` clause from a `CREATE TABLE` token stream — stock
/// sqlparser cannot parse the Spark CTAS forms at all (bare references and transform calls fail
/// `parse_column_def`'s mandatory data type), and typed columns land in `hive_distribution`, which
/// CTAS does not consume. The clause is located before the CTAS `AS` boundary (the
/// same rule `strip_create_table_using` uses), removed from the stream, and its elements
/// classified by token shape for [`build_ctas`] to validate.
///
/// Returns the remaining tokens and the classified elements (empty when no clause is present).
///
/// # Errors
/// Unbalanced parentheses, an empty field list / empty element, an unrecognisable element shape,
/// or a DUPLICATE `PARTITIONED BY` clause (Spark `checkDuplicateClauses` parity) error loudly —
/// Spark parse-rejects all of these forms too.
pub(crate) fn extract_partitioned_by(
    tokens: &[Token],
) -> Result<(Vec<Token>, Vec<PartitionedByElement>)> {
    let boundary = ctas_as_boundary(tokens);
    let Some((run_start, open_paren)) = find_partitioned_by_run(tokens, boundary) else {
        return Ok((tokens.to_vec(), Vec::new()));
    };
    let mut depth = 0usize;
    let mut close_paren = None;
    for (index, token) in tokens.iter().enumerate().skip(open_paren) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    close_paren = Some(index);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close_paren) = close_paren else {
        return Err(DataFusionError::Plan(
            "malformed PARTITIONED BY clause: unbalanced parentheses".to_string(),
        ));
    };
    let elements = parse_partitioned_by_elements(&tokens[open_paren + 1..close_paren])?;
    let mut remaining = Vec::with_capacity(tokens.len());
    remaining.extend_from_slice(&tokens[..run_start]);
    remaining.extend_from_slice(&tokens[close_paren + 1..]);
    if find_partitioned_by_run(&remaining, ctas_as_boundary(&remaining)).is_some() {
        return Err(DataFusionError::Plan(
            "duplicate PARTITIONED BY clause: found more than one (Spark rejects duplicate \
             clauses)"
                .to_string(),
        ));
    }
    Ok((remaining, elements))
}

/// Locate a well-formed `PARTITIONED BY (` run before `boundary`: returns the index of the
/// unquoted `PARTITIONED` keyword and of the opening paren. Quoted identifiers and a
/// `PARTITIONED` word not followed by `BY (` (e.g. an OPTIONS key named `partitioned`) are
/// skipped, never misread as the clause.
pub(crate) fn find_partitioned_by_run(tokens: &[Token], boundary: usize) -> Option<(usize, usize)> {
    for index in 0..boundary.min(tokens.len()) {
        let Token::Word(word) = &tokens[index] else {
            continue;
        };
        if word.keyword != Keyword::PARTITIONED || word.quote_style.is_some() {
            continue;
        }
        let mut next = index + 1;
        while next < tokens.len() && matches!(tokens[next], Token::Whitespace(_)) {
            next += 1;
        }
        let by_word = matches!(
            tokens.get(next),
            Some(Token::Word(by)) if by.keyword == Keyword::BY && by.quote_style.is_none()
        );
        if !by_word {
            continue;
        }
        let mut paren = next + 1;
        while paren < tokens.len() && matches!(tokens[paren], Token::Whitespace(_)) {
            paren += 1;
        }
        if matches!(tokens.get(paren), Some(Token::LParen)) {
            return Some((index, paren));
        }
    }
    None
}

/// Split the tokens INSIDE the `PARTITIONED BY (…)` parens on top-level commas and classify each
/// element by shape — [`PartitionedByElement::Identity`] for a single bare/quoted word,
/// [`PartitionedByElement::Transform`] for a call, [`PartitionedByElement::Nested`] for a dotted
/// path, [`PartitionedByElement::Typed`] for a Hive-style column def.
///
/// # Errors
/// An empty element (empty parens, a trailing comma) or an unrecognisable shape errors loudly
/// naming the element.
pub(crate) fn parse_partitioned_by_elements(inner: &[Token]) -> Result<Vec<PartitionedByElement>> {
    let mut elements = Vec::new();
    let mut depth = 0usize;
    let mut current: Vec<&Token> = Vec::new();
    for token in inner {
        if matches!(token, Token::Comma) && depth == 0 {
            elements.push(classify_partitioned_by_element(&current)?);
            current.clear();
            continue;
        }
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
        if !matches!(token, Token::Whitespace(_)) {
            current.push(token);
        }
    }
    elements.push(classify_partitioned_by_element(&current)?);
    Ok(elements)
}

/// Classify one comma-separated `PARTITIONED BY` element from its significant tokens (shapes in
/// [`PartitionedByElement`]).
pub(crate) fn classify_partitioned_by_element(tokens: &[&Token]) -> Result<PartitionedByElement> {
    match tokens {
        [] => Err(DataFusionError::Plan(
            "malformed PARTITIONED BY clause: empty partition field (write \
             PARTITIONED BY (col[, col…]))"
                .to_string(),
        )),
        // Bare / backtick-quoted / dialect Word identifiers.
        [Token::Word(word)] => Ok(PartitionedByElement::Identity(word.value.clone())),
        // ANSI double-quoted identifiers: DatabricksDialect tokenizes `"` as a string literal
        // rather than a Word-with-quote, so the facade's `_quote_ident` form lands here
        // (C1-SEC-001 partition-column quoting). The string *value* is the column name.
        [Token::DoubleQuotedString(name)] => Ok(PartitionedByElement::Identity(name.clone())),
        [Token::Word(word), Token::LParen, inner @ .., Token::RParen] => {
            Ok(PartitionedByElement::Transform {
                name: word.value.clone(),
                args: parse_transform_call_args(inner)?,
            })
        }
        [Token::Word(_), Token::Period, ..] => Ok(PartitionedByElement::Nested(
            tokens.iter().map(ToString::to_string).collect::<String>(),
        )),
        [Token::Word(word), Token::Word(_), ..] => {
            Ok(PartitionedByElement::Typed(word.value.clone()))
        }
        other => Err(DataFusionError::Plan(format!(
            "malformed PARTITIONED BY element `{}`: expected a column name, got an \
             unrecognisable shape",
            other.iter().map(ToString::to_string).collect::<String>()
        ))),
    }
}

/// Split the tokens INSIDE a transform call's parens (`bucket(4, id)` → `4`, `id`) on top-level
/// commas and render each argument to its semantic string: a bare/quoted identifier to its name
/// (the facade double-quotes the column arg, C3-SEC-001, so it tokenizes as a string literal),
/// an integer literal to its digits, a `-N` pair to `-N` (so a negative width still reaches the
/// loud `> 0` reject rather than a tokenizer error). A multi-token argument that is none of these
/// is concatenated verbatim — [`build_transform_field`] rejects it when it fails to parse.
///
/// # Errors
/// An empty argument (`bucket(4, )`, a leading/trailing comma) errors loudly.
pub(crate) fn parse_transform_call_args(inner: &[&Token]) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut depth = 0usize;
    let mut current: Vec<&Token> = Vec::new();
    let flush = |group: &[&Token]| -> Result<()> {
        if group.is_empty() {
            return Err(DataFusionError::Plan(
                "malformed PARTITIONED BY transform call: empty argument".to_string(),
            ));
        }
        Ok(())
    };
    for token in inner {
        if matches!(token, Token::Comma) && depth == 0 {
            flush(&current)?;
            args.push(render_transform_arg(&current));
            current.clear();
            continue;
        }
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
        current.push(token);
    }
    flush(&current)?;
    args.push(render_transform_arg(&current));
    Ok(args)
}

/// Render one transform argument's tokens to its semantic string (see [`parse_transform_call_args`]).
pub(crate) fn render_transform_arg(tokens: &[&Token]) -> String {
    match tokens {
        [Token::Word(word)] => word.value.clone(),
        [Token::DoubleQuotedString(name) | Token::SingleQuotedString(name)] => name.clone(),
        _ => tokens.iter().map(ToString::to_string).collect::<String>(),
    }
}

/// Rewrite Spark's `NAMESPACE` object type to `SCHEMA` in a `CREATE`/`DROP` statement, so stock
/// sqlparser (which knows `SCHEMA`/`DATABASE` but not `NAMESPACE`) can parse it. Only the object-type
/// word — the first word after `CREATE`/`DROP` that isn't `OR`/`REPLACE` — is rewritten, so an
/// identifier named `namespace` elsewhere is untouched.
pub(crate) fn rewrite_namespace_to_schema(tokens: &[Token]) -> Vec<Token> {
    let mut words = tokens
        .iter()
        .enumerate()
        .filter_map(|(i, token)| match token {
            Token::Word(word) => Some((i, word.keyword, word.value.clone())),
            _ => None,
        });
    let Some((_, lead, _)) = words.next() else {
        return tokens.to_vec();
    };
    if lead != Keyword::CREATE && lead != Keyword::DROP {
        return tokens.to_vec();
    }
    let object_type =
        words.find(|(_, keyword, _)| !matches!(keyword, Keyword::OR | Keyword::REPLACE));
    if let Some((index, _, value)) = object_type
        && value.eq_ignore_ascii_case("NAMESPACE")
    {
        let mut out = tokens.to_vec();
        out[index] = Token::make_keyword("SCHEMA");
        return out;
    }
    tokens.to_vec()
}

/// Render a `TBLPROPERTIES` value (a string literal in Spark) to its plain string.
pub(crate) fn property_value(value: &Expr) -> String {
    match value {
        Expr::Value(spanned) => match &spanned.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// Build the partition spec from top-level output fields in clause order. Resolution is exact-case;
/// transform values are computed by the fork from the source column.
///
/// # Errors
/// A source column not in the SELECT output errors loudly naming it AND the available columns
/// (Java's "Cannot find source column" class); a duplicate partition-field name is rejected by
/// the fork's builder ("Cannot use partition name more than once"). Bucket/truncate width `<= 0`
/// is rejected EARLIER, at parse time in [`build_transform_field`], so no table is ever created.
pub(crate) fn build_partition_spec(
    schema: &iceberg::spec::Schema,
    partition_fields: &[PartitionFieldSpec],
) -> Result<Option<UnboundPartitionSpec>> {
    if partition_fields.is_empty() {
        return Ok(None);
    }
    let mut builder = UnboundPartitionSpec::builder();
    for partition_field in partition_fields {
        let column = partition_field.column();
        let field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == *column)
            .ok_or_else(|| {
                let available = schema
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                DataFusionError::Plan(format!(
                    "cannot resolve CTAS partition column `{column}`: not an output column of \
                     the SELECT (column names are exact-case; query outputs: [{available}])"
                ))
            })?;
        builder = builder
            .add_partition_field(
                field.id,
                partition_field.field_name(),
                partition_field.transform(),
            )
            .map_err(iceberg_err)?;
    }
    Ok(Some(builder.build()))
}

/// The session's default catalog / schema, cloned out of the (temporary) state snapshot.
fn session_defaults(ctx: &SessionContext) -> (String, String) {
    let state = ctx.state();
    let catalog = &state.config().options().catalog;
    (
        catalog.default_catalog.clone(),
        catalog.default_schema.clone(),
    )
}
