//! The ANSI door's guard set runs before rewrites and on parsed DML arms.
//! G3-E8 runs before the async `MoR` and V3 valves. SEC-02 runs after planning.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    AlterSchemaOperation, AlterTableOperation, ColumnOption, Delete, Expr, FromTable, ObjectName,
    Query, Set, Statement, TableFactor, TableWithJoins, Update, Visit, Visitor,
};
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext};
use repark_iceberg::write::{
    MorDmlKind, refuse_mor_unpartitioned_multi_spec_dml,
    refuse_v3_cow_dml as refuse_v3_cow_dml_in_catalog,
};

use crate::scan::{blank_out_quoted_and_comments, leading_keyword};

/// Needle pinned by the G15 refusal tests (both doors). Byte-identical to the Spark door.
pub(crate) const COLLATION_REFUSAL_NEEDLE: &str = "does not implement collation";

/// The conf key that opens the SEC-02 local-filesystem gate. It matches the Spark key.
pub(crate) const ALLOW_LOCAL_FILESYSTEM_DDL_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

/// DataFusion's `extensions_options!` macro registers this `snake_case` field under the session key.
const ALLOW_LOCAL_FILESYSTEM_DDL_OPTION: &str = "repark.sql.allow_local_filesystem_ddl";

/// ===========================================================================================
/// Run text guards first in [`crate::router::execute`], before any rewrite.
/// ===========================================================================================
pub(crate) fn run_text_guards(cx: &EngineContext<'_>, sql: &str) -> Result<()> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    refuse_multi_statement(&scrubbed)?;
    refuse_read_only_catalog_dml(cx.catalogs, &scrubbed)?;
    refuse_write_to_branch(cx.ctx, &scrubbed)
}

// === Guard 1 — multi-statement (design §2 Q12; runs FIRST) ==================================

/// ===========================================================================================
/// Refuse genuine multi-statement scripts. A trailing terminator and whitespace are allowed.
/// ===========================================================================================
pub(crate) fn refuse_multi_statement(scrubbed: &str) -> Result<()> {
    let mut saw_semicolon = false;
    for ch in scrubbed.chars() {
        if ch == ';' {
            saw_semicolon = true;
        } else if saw_semicolon && !ch.is_whitespace() {
            return Err(multi_statement_error());
        }
    }
    Ok(())
}

/// The multi-statement refusal uses Spark's error class so migrated jobs see a familiar diagnostic.
fn multi_statement_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not \
             supported. Only a single statement is accepted; a trailing semicolon, whitespace, \
             or comment after that statement is allowed"
                .to_string(),
        )),
        None,
    )
}

// === Guard 2 — P11 read-only-catalog DML ====================================================

/// ===========================================================================================
/// Refuse DML whose target's leading name segment is a registered read-only catalog.
/// ===========================================================================================
pub(crate) fn refuse_read_only_catalog_dml(
    catalogs: &CatalogRegistry,
    scrubbed: &str,
) -> Result<()> {
    let Some((verb, target)) = dml_target(scrubbed) else {
        return Ok(());
    };
    let Some(catalog) = target.split('.').next() else {
        return Ok(());
    };
    let catalog = catalog.trim_matches('"');
    if catalogs.is_read_only_catalog(catalog) {
        return Err(DataFusionError::Plan(read_only_catalog_message(
            catalog, verb,
        )));
    }
    Ok(())
}

/// The P11 refusal text (pinned by tests; kept generic — no external-system name).
pub(crate) fn read_only_catalog_message(catalog: &str, verb: &str) -> String {
    format!(
        "catalog `{catalog}` is registered read-only: {verb} against it is not supported. \
         Read from it freely, or use it as the SOURCE of a MERGE INTO <writable table> \
         USING {catalog}.… statement."
    )
}

/// Read the DML verb and target name from scrubbed text. Return `None` for other statements.
fn dml_target(scrubbed: &str) -> Option<(&'static str, String)> {
    let mut words = word_iter(scrubbed);
    let leading = leading_keyword(scrubbed)?;
    words.next()?;
    let verb = match leading.as_str() {
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "MERGE" => "MERGE",
        _ => return None,
    };
    let target = loop {
        let word = words.next()?;
        let upper = word.to_ascii_uppercase();
        if matches!(
            upper.as_str(),
            "INTO" | "OVERWRITE" | "FROM" | "TABLE" | "ONLY"
        ) {
            continue;
        }
        break word;
    };
    Some((verb, target))
}

/// Split scrubbed SQL into "words", where a dotted/quoted name (`a.b."c d"`) is ONE word.
fn word_iter(scrubbed: &str) -> impl Iterator<Item = String> + '_ {
    let mut chars = scrubbed.char_indices().peekable();
    std::iter::from_fn(move || {
        while let Some(&(_, ch)) = chars.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '"' || ch == '$' {
                break;
            }
            chars.next();
        }
        chars.peek()?;
        let mut word = String::new();
        while let Some(&(_, ch)) = chars.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '"' || ch == '$' || ch == '.' {
                word.push(ch);
                chars.next();
            } else {
                break;
            }
        }
        if word.is_empty() { None } else { Some(word) }
    })
}

// === Guard 3 — write-to-branch ==============================================================

/// ===========================================================================================
/// Refuse a WRITE whose target names a snapshot branch.
/// ===========================================================================================
pub(crate) fn refuse_write_to_branch(ctx: &SessionContext, scrubbed: &str) -> Result<()> {
    let Some((_, target)) = dml_target(scrubbed) else {
        return Ok(());
    };
    let parts: Vec<&str> = target
        .split('.')
        .map(|part| part.trim_matches('"'))
        .collect();
    let Some(last) = parts.last() else {
        return Ok(());
    };
    let branch_suffixed = last.to_ascii_lowercase().starts_with("branch_");

    // Writes commit to `main`, so a reference target could land on the wrong branch.
    let refuse = match parts.len() {
        // Four or more parts are unambiguous reference syntax.
        n if n >= 4 => true,
        // Two-part branch names can also be ordinary table names.
        2 if branch_suffixed => {
            let full = datafusion::sql::TableReference::partial(
                parts[0].to_string(),
                parts[1].to_string(),
            );
            let prefix = datafusion::sql::TableReference::bare(parts[0].to_string());
            !ctx.table_exist(full).unwrap_or(false) && ctx.table_exist(prefix).unwrap_or(false)
        }
        _ => false,
    };
    if refuse {
        return Err(DataFusionError::Plan(format!(
            "writing to a snapshot ref is not supported: `{target}` names a branch/tag, and every \
             write commit in this engine sets the `main` branch — the write would silently land on \
             `main`. Write to the table itself (`{}`) and manage refs with branch/tag DDL.",
            parts[..parts.len() - 1].join(".")
        )));
    }
    Ok(())
}

// === Guard 4 — BUG-001 MoR valve (async; runs in the parsed DML arm) ========================

/// ===========================================================================================
/// Refuse delegated `DELETE` / `UPDATE` when current metadata has unpartitioned multi-spec history.
/// ===========================================================================================
pub(crate) async fn refuse_mor_multi_spec_dml(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Result<()> {
    let Some((kind, target, catalog_name, ident)) = dml_target_ident(cx, statement) else {
        return Ok(());
    };
    let Some(catalog) = cx.catalogs.get(&catalog_name) else {
        return Ok(());
    };
    // Unpartitioned position deletes are unsafe after multi-spec history.
    refuse_mor_unpartitioned_multi_spec_dml(catalog.as_ref(), &ident, &target, kind).await
}

/// V3R-1 valve (`V3-COW-1`) for delegated plain-`WHERE` DELETE / UPDATE. It runs after G3-E8.
pub(crate) async fn refuse_v3_cow_dml(cx: &EngineContext<'_>, statement: &Statement) -> Result<()> {
    let Some((kind, _target, catalog_name, ident)) = dml_target_ident(cx, statement) else {
        return Ok(());
    };
    let Some(catalog) = cx.catalogs.get(&catalog_name) else {
        return Ok(());
    };
    refuse_v3_cow_dml_in_catalog(catalog.as_ref(), &ident, kind).await
}

/// Resolve a `DELETE` / `UPDATE` target from the AST as DataFusion will, completing short names.
fn dml_target_ident(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Option<(MorDmlKind, String, String, TableIdent)> {
    let (kind, name) = match statement {
        Statement::Delete(delete) => (MorDmlKind::Delete, delete_target_name(delete)?),
        Statement::Update(update) => (MorDmlKind::Update, object_name_of(&update.table)?),
        _ => return None,
    };
    let mut parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
    if parts.is_empty() {
        return None;
    }
    if parts.len() < 3 {
        let (default_catalog, default_schema) = {
            let state = cx.ctx.state();
            let catalog = &state.config().options().catalog;
            (
                catalog.default_catalog.clone(),
                catalog.default_schema.clone(),
            )
        };
        if parts.len() == 1 {
            parts.insert(0, default_schema);
        }
        parts.insert(0, default_catalog);
    }
    let namespace = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()).ok()?;
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    Some((kind, name.to_string(), parts[0].clone(), ident))
}

/// A parsed `DELETE`'s target: the multi-delete `tables` form first, else the first FROM relation.
fn delete_target_name(delete: &Delete) -> Option<&ObjectName> {
    if let Some(name) = delete.tables.first() {
        return Some(name);
    }
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    tables.first().and_then(object_name_of)
}

// === Guard 5 — SEC-02 local-filesystem plans (runs after planning) ==========================

/// ===========================================================================================
/// Gate DataFusion filesystem-as-data plans. Intercepted Iceberg CREATE uses catalog policy.
/// ===========================================================================================
pub(crate) fn refuse_local_filesystem_plan(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    if allow_local_filesystem_ddl(ctx) {
        return Ok(());
    }
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create)) => {
            refuse_local_path(catalogs, "CREATE EXTERNAL TABLE", &create.location)
        }
        LogicalPlan::Copy(copy_to) => refuse_local_path(catalogs, "COPY TO", &copy_to.output_url),
        _ => Ok(()),
    }
}

/// Read the gate conf from live session options without the `repark-functions` extension.
fn allow_local_filesystem_ddl(ctx: &SessionContext) -> bool {
    ctx.copied_config()
        .options()
        .entries()
        .into_iter()
        .find(|entry| entry.key == ALLOW_LOCAL_FILESYSTEM_DDL_OPTION)
        .and_then(|entry| entry.value)
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "1"))
}

fn refuse_local_path(catalogs: &CatalogRegistry, surface: &str, raw_location: &str) -> Result<()> {
    let Some(local_path) = local_filesystem_path(raw_location) else {
        return Ok(());
    };
    if path_under_any_warehouse(catalogs, &local_path) {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "{surface} to local filesystem path `{raw_location}` is disabled by default. Set conf \
         `{ALLOW_LOCAL_FILESYSTEM_DDL_KEY}` = true to allow local CREATE EXTERNAL TABLE / COPY TO \
         outside the session warehouse root, or use a path under a registered warehouse \
         (grandfather). Remote locations (s3://, s3a://) are unaffected."
    )))
}

/// Map a location to a local path when it is bare or `file:`-schemed; return `None` for remote schemes.
/// Scheme matching is case-insensitive, so `FILE:///etc/passwd` remains local.
fn local_filesystem_path(location: &str) -> Option<std::path::PathBuf> {
    const REMOTE_SCHEMES: &[&str] = &[
        "s3://",
        "s3a://",
        "s3n://",
        "http://",
        "https://",
        "hdfs://",
        "viewfs://",
        "gs://",
        "abfs://",
        "abfss://",
        "wasb://",
        "wasbs://",
    ];
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if REMOTE_SCHEMES
        .iter()
        .any(|scheme| lower.starts_with(scheme))
    {
        return None;
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "file://") {
        let path = strip_prefix_ci(rest, "localhost").unwrap_or(rest);
        return Some(std::path::PathBuf::from(path));
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "file:") {
        return Some(std::path::PathBuf::from(rest));
    }
    // An unknown but well-formed scheme is somebody else's object store — leave it to DataFusion.
    if let Some(index) = trimmed.find("://") {
        let scheme = &trimmed[..index];
        if !scheme.is_empty()
            && scheme
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '+' || ch == '-' || ch == '.')
        {
            return None;
        }
    }
    Some(std::path::PathBuf::from(trimmed))
}

fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&value[prefix.len()..])
    } else {
        None
    }
}

fn path_under_any_warehouse(catalogs: &CatalogRegistry, path: &std::path::Path) -> bool {
    let target = canonicalize_best_effort(path);
    catalogs.local_warehouse_roots().iter().any(|root| {
        let root = canonicalize_best_effort(std::path::Path::new(root));
        target == root || target.starts_with(&root)
    })
}

/// Canonicalize existing paths; fall back to the parent when a COPY destination does not exist.
fn canonicalize_best_effort(path: &std::path::Path) -> std::path::PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    if let Some(parent) = path.parent()
        && let Ok(canonical_parent) = parent.canonicalize()
    {
        return match path.file_name() {
            Some(name) => canonical_parent.join(name),
            None => canonical_parent,
        };
    }
    // Lexical dot and dot-dot handling blocks traversal bypass when canonicalization fails.
    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

// === Guard 6 — G3-E8 subquery-predicate DML valve (runs on the parsed statement) ============

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
/// Refuse delegated `DELETE` / `UPDATE` whose `WHERE` subquery can become a match-all write.
/// ===========================================================================================
pub(crate) fn refuse_dml_subquery_predicate(statement: &Statement) -> Result<()> {
    let (verb, selection, target) = match statement {
        Statement::Delete(delete) => (
            DmlSubqueryVerb::Delete,
            delete.selection.as_ref(),
            delete_target(delete).map_or_else(|| "<table>".to_string(), ToString::to_string),
        ),
        Statement::Update(update) => (
            DmlSubqueryVerb::Update,
            update.selection.as_ref(),
            update_target(update).map_or_else(|| update.table.to_string(), ToString::to_string),
        ),
        _ => return Ok(()),
    };
    if repark_iceberg::write::predicate_dml::try_allowed_delete_in(statement)?.is_some()
        || repark_iceberg::write::predicate_dml::try_allowed_update_in(statement)?.is_some()
    {
        return Ok(());
    }
    let Some(selection) = selection else {
        return Ok(());
    };
    if !expression_contains_subquery(selection) {
        return Ok(());
    }
    Err(DataFusionError::Plan(dml_subquery_refusal_message(
        verb, &target,
    )))
}

/// The `ObjectName` a `DELETE` targets from the parse tree, including `FROM t` forms.
fn delete_target(delete: &Delete) -> Option<&ObjectName> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    object_name_of(tables.first()?)
}

/// The `ObjectName` an `UPDATE` targets — the primary relation of its `TableWithJoins`.
fn update_target(update: &Update) -> Option<&ObjectName> {
    object_name_of(&update.table)
}

/// The plain table name of a `TableWithJoins`' primary relation, if it is one.
fn object_name_of(table: &TableWithJoins) -> Option<&ObjectName> {
    match &table.relation {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

/// Return true when a `Query` node appears inside `expr`, so the expression carries a subquery.
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

/// The G3-E8 refusal text is byte-identical to the Spark door's.
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

// === Guard — G15 collation refuse (parse altitude) ==========================================

/// ===========================================================================================
/// Render the G15 refusal. Byte-identical to the Spark door's message (same needles).
/// ===========================================================================================
pub(crate) fn collation_refusal_message(requested: &str) -> String {
    format!(
        "repark {COLLATION_REFUSAL_NEEDLE}: requested `{requested}`. Spark 4 would apply \
         that collation to comparisons and ORDER BY; repark refuses rather than silently \
         ignore it. Use binary/default ordering — omit COLLATE, keep StringType() / \
         UTF8_BINARY, and do not set a session collation."
    )
}

/// ===========================================================================================
/// Refuse a collation spelling on the router's parsed statement (parse altitude).
/// ===========================================================================================
pub(crate) fn refuse_collation_in_statement(statement: &Statement) -> Result<()> {
    let mut probe = CollationProbe { requested: None };
    if statement.visit(&mut probe).is_break()
        && let Some(requested) = probe.requested
    {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            &requested,
        )));
    }
    Ok(())
}

struct CollationProbe {
    requested: Option<String>,
}

impl Visitor for CollationProbe {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Collate { collation, .. } = expr {
            self.requested = Some(collation.to_string());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if let Some(requested) = collation_requested_by_statement(statement) {
            self.requested = Some(requested);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn collation_requested_by_statement(statement: &Statement) -> Option<String> {
    match statement {
        Statement::CreateTable(create) => {
            if let Some(name) = &create.default_ddl_collation {
                return Some(name.clone());
            }
            first_column_collation(&create.columns)
        }
        Statement::CreateSchema {
            default_collate_spec: Some(spec),
            ..
        } => Some(spec.to_string()),
        Statement::CreateDatabase {
            default_collation,
            default_ddl_collation,
            ..
        } => default_collation
            .clone()
            .or_else(|| default_ddl_collation.clone()),
        Statement::CreateCollation(create) => Some(create.name.to_string()),
        Statement::AlterCollation(alter) => Some(alter.name.to_string()),
        Statement::AlterSchema(alter) => {
            for operation in &alter.operations {
                if let AlterSchemaOperation::SetDefaultCollate { collate } = operation {
                    return Some(collate.to_string());
                }
            }
            None
        }
        Statement::AlterTable(alter) => {
            for operation in &alter.operations {
                if let AlterTableOperation::AddColumn { column_def, .. } = operation
                    && let Some(name) = first_column_collation(std::slice::from_ref(column_def))
                {
                    return Some(name);
                }
            }
            None
        }
        Statement::Set(set) => collation_requested_by_set(set),
        _ => None,
    }
}

fn first_column_collation(
    columns: &[datafusion::sql::sqlparser::ast::ColumnDef],
) -> Option<String> {
    for column in columns {
        for option in &column.options {
            if let ColumnOption::Collation(name) = &option.option {
                return Some(name.to_string());
            }
        }
    }
    None
}

fn collation_requested_by_set(set: &Set) -> Option<String> {
    match set {
        Set::SetNames {
            collation_name: Some(name),
            ..
        } => Some(name.clone()),
        Set::SingleAssignment { variable, .. } => {
            let key = variable.to_string();
            key.to_ascii_lowercase()
                .contains("collation")
                .then_some(key)
        }
        Set::MultipleAssignments { assignments } => {
            for assignment in assignments {
                let key = assignment.name.to_string();
                if key.to_ascii_lowercase().contains("collation") {
                    return Some(key);
                }
            }
            None
        }
        Set::ParenthesizedAssignments { variables, .. } => {
            for variable in variables {
                let key = variable.to_string();
                if key.to_ascii_lowercase().contains("collation") {
                    return Some(key);
                }
            }
            None
        }
        _ => None,
    }
}

/// ===========================================================================================
/// Refuse type-position `STRING COLLATE name` that sqlparser's CAST cannot attach.
/// ===========================================================================================
pub(crate) fn refuse_type_position_collation_in_sql(sql: &str) -> Result<()> {
    if let Some(requested) = type_position_collation(sql) {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            &requested,
        )));
    }
    Ok(())
}

/// ===========================================================================================
/// Refuse `RESET` of a collation session key (DataFusion extension, not `Statement::Set`).
/// ===========================================================================================
pub(crate) fn refuse_collation_reset_variable(variable: &str) -> Result<()> {
    if variable.to_ascii_lowercase().contains("collation") {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            variable,
        )));
    }
    Ok(())
}

fn type_position_collation(sql: &str) -> Option<String> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    let lower = scrubbed.to_ascii_lowercase();
    let mut from = 0;
    while let Some(relative) = lower[from..].find("collate") {
        let at = from + relative;
        if !is_word_boundary(&lower, at, at + 7) || !preceded_by_string_type(&lower, at) {
            from = at + 7;
            continue;
        }
        return collation_ident_after(&scrubbed, at + 7);
    }
    None
}

fn preceded_by_string_type(lower: &str, collate_at: usize) -> bool {
    let before = strip_trailing_length_spec(lower[..collate_at].trim_end());
    for token in ["string", "varchar", "char", "text"] {
        if before.ends_with(token)
            && is_word_boundary(before, before.len() - token.len(), before.len())
        {
            return true;
        }
    }
    false
}

fn strip_trailing_length_spec(text: &str) -> &str {
    let trimmed = text.trim_end();
    if !trimmed.ends_with(')') {
        return trimmed;
    }
    let Some(open) = trimmed.rfind('(') else {
        return trimmed;
    };
    let inner = trimmed[open + 1..trimmed.len() - 1].trim();
    if inner
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte.is_ascii_whitespace())
    {
        return trimmed[..open].trim_end();
    }
    trimmed
}

fn collation_ident_after(sql: &str, after_collate: usize) -> Option<String> {
    let tail = sql[after_collate..].trim_start();
    let mut end = 0;
    for (index, character) in tail.char_indices() {
        if character.is_ascii_alphanumeric() || character == '_' || character == '.' {
            end = index + character.len_utf8();
            continue;
        }
        break;
    }
    (end > 0).then(|| tail[..end].to_string())
}

fn is_word_boundary(text: &str, start: usize, end: usize) -> bool {
    let bytes = text.as_bytes();
    let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
    let after_ok = end >= bytes.len() || !is_ident_byte(bytes[end]);
    before_ok && after_ok
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests;
