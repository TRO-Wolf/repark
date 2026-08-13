//! G15 — loud refusal of collation spellings at the executing parse.
//!
//! Collation is unimplemented. Before this valve a collation request was either a raw
//! DataFusion `Unsupported ast node: Collate` or — on `createDataFrame` with
//! `StringType("UNICODE_CI")` — silently ignored (binary compare, wrong distinct count).
//! The valve names what was requested, that repark does not implement collation, and
//! that the caller should use binary/default ordering.
//!
//! Attached at the parse every route agrees on (G3-E8 altitude):
//! [`crate::spark_ast::execute_passthrough`] (the executing parse) **and** the router's
//! successful `parse_single_normalized` result (intercepted `CREATE` / `ALTER` never
//! reach the passthrough). Type-position `CAST(x AS STRING COLLATE name)` is scanned
//! on the executing-parse text because sqlparser cannot attach it. The binding
//! (`F.expr`, `DataFrame.filter` SQL-string form) calls [`refuse_collation_in_sql`]
//! so those fragments see the same message.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    AlterSchemaOperation, AlterTableOperation, ColumnOption, Expr, Set, Statement, Visit, Visitor,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// Needle pinned by the G15 refusal tests (both doors, facade).
pub const COLLATION_REFUSAL_NEEDLE: &str = "does not implement collation";

/// ===========================================================================================
/// Render the G15 refusal. Byte-identical to the ANSI door's message (same needles).
/// ===========================================================================================
#[must_use]
pub fn collation_refusal_message(requested: &str) -> String {
    format!(
        "repark {COLLATION_REFUSAL_NEEDLE}: requested `{requested}`. Spark 4 would apply \
         that collation to comparisons and ORDER BY; repark refuses rather than silently \
         ignore it. Use binary/default ordering — omit COLLATE, keep StringType() / \
         UTF8_BINARY, and do not set a session collation."
    )
}

/// ===========================================================================================
/// Refuse a collation spelling on a parsed statement (the executing parse).
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::NotImplemented`] naming the requested collation.
pub fn refuse_collation_in_statement(statement: &Statement) -> Result<()> {
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

/// ===========================================================================================
/// Refuse a collation spelling in a SQL string or expression fragment.
///
/// Used by the Spark-door router (statement text) and the Python binding (`F.expr`,
/// `filter_sql`). A fragment that is not a full statement is wrapped as `SELECT (…)` so
/// `col COLLATE name` still parses. Unparsable text is left to the caller — this valve
/// only upgrades a *parsed* collation request.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::NotImplemented`] when a collation spelling is present.
pub fn refuse_collation_in_sql(sql: &str) -> Result<()> {
    // Type-position COLLATE (`CAST(x AS STRING COLLATE name)`) is not an
    // `Expr::Collate` — sqlparser's CAST production does not consume COLLATE —
    // so the AST walk never sees it. Scan text first (quote-aware) so the
    // spelling Spark 4 accepts is G15, not a generic ParserError.
    refuse_type_position_collation_in_sql(sql)?;
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, sql) {
        return refuse_parsed(&statements);
    }
    let wrapped = format!("SELECT ({sql})");
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, &wrapped) {
        return refuse_parsed(&statements);
    }
    Ok(())
}

/// ===========================================================================================
/// Refuse `AS STRING COLLATE name` / column-type COLLATE that sqlparser cannot attach.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::NotImplemented`] when a type-position collation is present.
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
///
/// # Errors
/// [`DataFusionError::NotImplemented`] when the variable name contains `collation`.
pub(crate) fn refuse_collation_reset_variable(variable: &str) -> Result<()> {
    if is_collation_session_key(variable) {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            variable,
        )));
    }
    Ok(())
}

fn refuse_parsed(statements: &[Statement]) -> Result<()> {
    for statement in statements {
        refuse_collation_in_statement(statement)?;
    }
    Ok(())
}

/// True when a Spark `SQLConf` / session key would change compare/order collation.
///
/// Spark 4.1.2 keys include `spark.sql.collation.objectLevel.enabled`,
/// `spark.sql.collation.schemaLevel.enabled`, `spark.sql.collation.trim.enabled`,
/// `spark.sql.collation.allowInMapKeys`, and
/// `spark.sql.legacy.collationAwareHashFunctions`.
#[must_use]
pub fn is_collation_session_key(key: &str) -> bool {
    key.to_ascii_lowercase().contains("collation")
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

/// Statement-shaped collation requests that are not `Expr::Collate` (column options,
/// default-collation DDL, SET NAMES, session conf assignments, CREATE/ALTER COLLATION).
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
            is_collation_session_key(&key).then_some(key)
        }
        Set::MultipleAssignments { assignments } => {
            for assignment in assignments {
                let key = assignment.name.to_string();
                if is_collation_session_key(&key) {
                    return Some(key);
                }
            }
            None
        }
        Set::ParenthesizedAssignments { variables, .. } => {
            for variable in variables {
                let key = variable.to_string();
                if is_collation_session_key(&key) {
                    return Some(key);
                }
            }
            None
        }
        _ => None,
    }
}

/// Quote-aware scan for a string-type token immediately before `COLLATE`.
///
/// Spark's `CAST(x AS STRING COLLATE name)` never becomes `Expr::Collate`.
/// `CREATE TABLE … (col STRING COLLATE name)` is also type-position; the AST
/// walk already catches the parsed form, and this scan is the unparsable twin.
fn type_position_collation(sql: &str) -> Option<String> {
    let scrubbed = blank_sql_quotes_and_comments(sql);
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

/// Blank `'…'` / `"…"` / `` `…` `` / `--` / `/* … */` content so COLLATE inside a
/// literal is not a request. Spark quoting includes backticks.
fn blank_sql_quotes_and_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let delimiter = bytes[index];
                out.push(delimiter);
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == delimiter {
                        if bytes.get(index + 1) == Some(&delimiter) {
                            out.push(b' ');
                            out.push(b' ');
                            index += 2;
                            continue;
                        }
                        out.push(delimiter);
                        index += 1;
                        break;
                    }
                    out.push(b' ');
                    index += 1;
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                out.push(b'-');
                out.push(b'-');
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    out.push(b' ');
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                out.push(b'/');
                out.push(b'*');
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    out.push(b' ');
                    index += 1;
                }
                if index + 1 < bytes.len() {
                    out.push(b'*');
                    out.push(b'/');
                    index += 2;
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
