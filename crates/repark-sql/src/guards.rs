//! The ANSI door's guard set runs before rewrites and on parsed DML arms.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    AlterSchemaOperation, AlterTableOperation, ColumnOption, Delete, Expr, FromTable, ObjectName,
    Set, Statement, TableFactor, TableWithJoins, Visit, Visitor,
};
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext};
use repark_iceberg::write::{MorDmlKind, refuse_mor_unpartitioned_multi_spec_dml};

use crate::scan::{blank_out_quoted_and_comments, leading_keyword};

/// Needle pinned by the G15 refusal tests (both doors).
pub(crate) const COLLATION_REFUSAL_NEEDLE: &str = "does not implement collation";

/// Run text guards first in [`crate::router::execute`], before any rewrite.
pub(crate) fn run_text_guards(cx: &EngineContext<'_>, sql: &str) -> Result<()> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    refuse_multi_statement(&scrubbed)?;
    refuse_read_only_catalog_dml(cx.catalogs, &scrubbed)?;
    refuse_write_to_branch(cx.ctx, &scrubbed)
}

// Guard 1: multi-statement, design section 2 Q12; runs first.

/// Refuse genuine multi-statement scripts.
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

// Guard 2: P11 read-only-catalog DML.

/// Refuse DML whose target's leading name segment is a registered read-only catalog.
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
        if verb == "INSERT" && crate::pg_insert::is_postgres_source(catalogs, catalog) {
            return Ok(());
        }
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

/// Read the DML verb and target name from scrubbed text.
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

// Guard 3: write-to-branch.

/// Refuse a WRITE whose target names a snapshot branch.
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

// Guard 4: BUG-001 MoR valve, async, runs in the parsed DML arm.

/// Refuse delegated `DELETE` / `UPDATE` when current metadata has unpartitioned multi-spec history.
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

/// Resolve a `DELETE` / `UPDATE` target from the AST as DataFusion will, completing short names.
pub(crate) fn dml_target_ident(
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

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn refuse_encrypted_write_target(
    cx: &EngineContext<'_>,
    table_name: &ObjectName,
) -> Result<()> {
    let parts: Vec<String> = table_name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
    let Some((catalog_name, ident)) = complete_write_parts(cx, parts) else {
        return Ok(());
    };
    let Some(catalog) = cx.catalogs.get(&catalog_name) else {
        return Ok(());
    };
    repark_iceberg::write::refuse_encrypted_write(catalog.as_ref(), &ident).await
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn refuse_encrypted_dml_target(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Result<()> {
    let name = match statement {
        Statement::Delete(delete) => delete_target_name(delete),
        Statement::Update(update) => object_name_of(&update.table),
        _ => None,
    };
    let Some(name) = name else {
        return Ok(());
    };
    refuse_encrypted_write_target(cx, name).await
}

fn complete_write_parts(
    cx: &EngineContext<'_>,
    mut parts: Vec<String>,
) -> Option<(String, TableIdent)> {
    if parts.is_empty() {
        return None;
    }
    if parts.len() < 3 {
        let state = cx.ctx.state();
        let catalog = &state.config().options().catalog;
        if parts.len() == 1 {
            parts.insert(0, catalog.default_schema.clone());
        }
        parts.insert(0, catalog.default_catalog.clone());
    }
    let namespace = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()).ok()?;
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    Some((parts[0].clone(), ident))
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

pub(crate) use repark_core::local_fs_policy::refuse_local_filesystem_plan;

/// The plain table name of a `TableWithJoins`' primary relation, if it is one.
fn object_name_of(table: &TableWithJoins) -> Option<&ObjectName> {
    match &table.relation {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

// Guard: G15 collation refuse at parse altitude.

/// Render the G15 refusal.
pub(crate) fn collation_refusal_message(requested: &str) -> String {
    format!(
        "repark {COLLATION_REFUSAL_NEEDLE}: requested `{requested}`. Spark 4 would apply \
         that collation to comparisons and ORDER BY; repark refuses rather than silently \
         ignore it. Use binary/default ordering — omit COLLATE, keep StringType() / \
         UTF8_BINARY, and do not set a session collation."
    )
}

/// Refuse a collation spelling on the router's parsed statement (parse altitude).
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

/// Refuse type-position `STRING COLLATE name` that sqlparser's CAST cannot attach.
pub(crate) fn refuse_type_position_collation_in_sql(sql: &str) -> Result<()> {
    if let Some(requested) = type_position_collation(sql) {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            &requested,
        )));
    }
    Ok(())
}

/// Refuse `RESET` of a collation session key (DataFusion extension, not `Statement::Set`).
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
mod ansi_door;
#[cfg(test)]
mod tests;
