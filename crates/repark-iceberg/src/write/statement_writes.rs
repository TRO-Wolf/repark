use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{Ident, Statement, visit_statements};

const EXPLAIN_ANALYZE_REFUSAL: &str = "EXPLAIN ANALYZE is refused: its statement writes to a \
     table, and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same \
     statement still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.";

const PREPARE_REFUSAL_TAIL: &str = "is refused: its statement writes to a table, and a prepared \
     write would run later, at EXECUTE. PREPARE of a query still works.";

#[must_use]
pub fn executed_writes(statement: &Statement) -> Vec<Statement> {
    match statement {
        Statement::Explain {
            analyze, statement, ..
        } => {
            if *analyze {
                executed_writes(statement)
            } else {
                Vec::new()
            }
        }
        Statement::Prepare { statement, .. } => executed_writes(statement),
        carrier => {
            let mut writes = Vec::new();
            let _ = visit_statements(carrier, |nested| {
                if matches!(
                    nested,
                    Statement::Insert(_)
                        | Statement::Update(_)
                        | Statement::Merge(_)
                        | Statement::Delete(_)
                ) {
                    writes.push(nested.clone());
                }
                ControlFlow::<()>::Continue(())
            });
            writes
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_wrapped_write(statement: &DFStatement) -> Result<()> {
    let mut analyzed = false;
    let mut current = statement;
    while let DFStatement::Explain(explain) = current {
        if !explain.analyze {
            return Ok(());
        }
        analyzed = true;
        current = explain.statement.as_ref();
    }
    let DFStatement::Statement(inner) = current else {
        return Ok(());
    };
    let refusal = match inner.as_ref() {
        _ if analyzed => EXPLAIN_ANALYZE_REFUSAL.to_string(),
        Statement::Explain { analyze: true, .. } => EXPLAIN_ANALYZE_REFUSAL.to_string(),
        Statement::Prepare { name, .. } => prepare_refusal(name),
        _ => return Ok(()),
    };
    if executed_writes(inner).is_empty() {
        return Ok(());
    }
    Err(DataFusionError::Plan(refusal))
}

fn prepare_refusal(name: &Ident) -> String {
    format!("PREPARE `{}` {PREPARE_REFUSAL_TAIL}", name.value)
}

#[cfg(test)]
mod tests;
