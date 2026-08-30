//! FNP-15/16 declared-absent function names at parse altitude (ANSI door).

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, Statement, Visit, Visitor};

pub(crate) fn refuse_in_statement(statement: &Statement) -> Result<()> {
    let mut probe = FunctionProbe { message: None };
    if statement.visit(&mut probe).is_break()
        && let Some(message) = probe.message
    {
        return Err(DataFusionError::NotImplemented(message));
    }
    Ok(())
}

struct FunctionProbe {
    message: Option<String>,
}

impl Visitor for FunctionProbe {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        let name = object_name_last(&function.name);
        let Some(message) = refusal_message(&name) else {
            return ControlFlow::Continue(());
        };
        self.message = Some(message);
        ControlFlow::Break(())
    }
}

fn object_name_last(name: &ObjectName) -> String {
    name.0.last().and_then(|part| part.as_ident()).map_or_else(
        || name.to_string().to_ascii_lowercase(),
        |ident| ident.value.to_ascii_lowercase(),
    )
}

fn refusal_message(name: &str) -> Option<String> {
    match name {
        "java_method" => Some(
            "java_method is unreachable: it loads a Java class by name and invokes a static \
             method by reflection, which needs a live JVM. repark has no JVM. See \
             docs/spark-sql-iceberg-parity.md (FNP-15 java_method)."
                .to_string(),
        ),
        "reflect" => Some(
            "reflect is unreachable: it is Spark's CallMethodViaReflection spelling of \
             java_method, which needs a live JVM. repark has no JVM. See \
             docs/spark-sql-iceberg-parity.md (FNP-15 reflect)."
                .to_string(),
        ),
        "try_reflect" => Some(
            "try_reflect is unreachable: it is reflect with exception-to-NULL, and still \
             needs a live JVM. repark has no JVM. See docs/spark-sql-iceberg-parity.md \
             (FNP-15 try_reflect)."
                .to_string(),
        ),
        "unwrap_udt" => Some(
            "unwrap_udt is unreachable: Spark UserDefinedType unwrap walks the JVM UDT \
             registry; with no JVM there is no UDT system to unwrap from. See \
             docs/spark-sql-iceberg-parity.md (FNP-15 unwrap_udt)."
                .to_string(),
        ),
        "input_file_block_start" => Some(
            "input_file_block_start is unreachable: it reads Spark's InputFileBlockHolder \
             thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a \
             task. DataFusion has no equivalent surface, and repark's input_file_name is \
             itself still a stub. See docs/spark-sql-iceberg-parity.md \
             (FNP-15 input_file_block_start)."
                .to_string(),
        ),
        "input_file_block_length" => Some(
            "input_file_block_length is unreachable: it reads Spark's InputFileBlockHolder \
             thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a \
             task. DataFusion has no equivalent surface, and repark's input_file_name is \
             itself still a stub. See docs/spark-sql-iceberg-parity.md \
             (FNP-15 input_file_block_length)."
                .to_string(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    // pins: fnp-15-16/C-001, C-002, C-017
    use datafusion::error::DataFusionError;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn parsed(sql: &str) -> Statement {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse");
        statements.pop().expect("one statement")
    }

    #[test]
    fn java_method_refuses_on_ansi_parse() {
        let error = refuse_in_statement(&parsed("SELECT java_method(1)")).expect_err("refuse");
        let text = error.to_string();
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
        assert!(text.contains("unreachable"));
        assert!(text.contains("java_method"));
    }

    #[test]
    fn abs_is_untouched() {
        refuse_in_statement(&parsed("SELECT abs(1)")).expect("abs stays");
    }
}
