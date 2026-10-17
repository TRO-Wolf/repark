use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{Statement, visit_statements};

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
