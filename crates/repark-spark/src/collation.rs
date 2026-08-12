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
//! reach the passthrough). The binding (`F.expr`, `DataFrame.filter` SQL-string form)
//! calls [`refuse_collation_in_sql`] so those fragments see the same message.

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
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, sql) {
        return refuse_parsed(&statements);
    }
    let wrapped = format!("SELECT ({sql})");
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, &wrapped) {
        return refuse_parsed(&statements);
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
        _ => None,
    }
}
