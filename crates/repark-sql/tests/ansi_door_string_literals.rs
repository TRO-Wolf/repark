//! **ANSI-door control for SQP-1.** String-literal canonicalisation is Spark-only.

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;

/// A native ANSI session — no `SessionExtension`, `AnsiDialect` as the session dialect.
fn native_ansi_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session")
}

/// The single `Utf8` value of `SELECT <expr> AS s`.
async fn string_value(session: &ReparkSession, expr: &str) -> String {
    let batches = session
        .sql(&format!("SELECT {expr} AS s"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 column")
        .value(0)
        .to_string()
}

/// The ANSI door keeps generic literal semantics — unchanged by the Spark-door SQP-1 canonicaliser.
/// pins: sqp-1-spark-string-literals/C-006
#[tokio::test]
async fn ansi_door_keeps_generic_literals() {
    let session = native_ansi_session();
    // A backslash is a literal character: `'\d'` is two chars, never Spark's one.
    assert_eq!(string_value(&session, "'\\d'").await, "\\d");
    // `'\\d'` stays three characters (Spark folds it to `\d`).
    assert_eq!(string_value(&session, "'\\\\d'").await, "\\\\d");
    // `\'` is not an escape here, so `'\''` does not lex — the string is unterminated.
    assert!(
        session.sql("SELECT '\\'' AS s").await.is_err(),
        "`'\\''` must be a lexer error on the ANSI door"
    );
    assert!(
        session.sql("SELECT r'\\d' AS s").await.is_err(),
        "`r'\\d'` must refuse on the ANSI door"
    );
}
