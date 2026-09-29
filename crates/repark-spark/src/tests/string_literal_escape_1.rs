use super::super::*;
use super::common::*;

use std::borrow::Cow;

fn expr_ctx() -> (SessionContext, CatalogRegistry) {
    let config =
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new());
    let config = repark_functions::ansi::with_spark_ansi_config(config, true);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::register_all(&ctx);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    (ctx, CatalogRegistry::new())
}

fn verbatim_expr_ctx() -> (SessionContext, CatalogRegistry) {
    let config =
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new());
    let config = repark_functions::ansi::with_spark_ansi_config(config, true);
    let config = crate::spark_literals::with_escaped_string_literals_config(config, true);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::register_all(&ctx);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    (ctx, CatalogRegistry::new())
}

async fn string_value(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> String {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS s"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Utf8,
        "`{expr}` must yield a Spark STRING (Arrow Utf8)"
    );
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 column");
    column.value(0).to_string()
}

async fn int_value(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> i64 {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS n"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    let column = datafusion::arrow::compute::cast(batches[0].column(0), &DataType::Int64).unwrap();
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 after cast")
        .value(0)
}

#[tokio::test]
async fn double_quoted_doubled_quotes_collapse_like_spark() {
    let (ctx, catalogs) = expr_ctx();
    let cases: &[(&str, &str)] = &[
        ("\"x\\\\\"\"y\"", "x\\\"y"),
        ("\"\\\\\"\"\"", "\\\""),
        ("\"\"\"\\n\"", "\"\n"),
        ("\"\\n\"\"\"", "\n\""),
        ("\"a\\\"b\"\"c\"", "a\"b\"c"),
        ("\"a\"\"b\\\"c\"", "a\"b\"c"),
        ("\"a\"\"\\n\"\"b\"", "a\"\n\"b"),
        ("\"it\"\"s\" \"!\"", "it\"s!"),
        ("\"a\\''b\"\"c\\''d\"", "a''b\"c''d"),
        ("'a\"\"b'", "a\"\"b"),
        ("'a\"\"b''c\"\"d'", "a\"\"b'c\"\"d"),
    ];
    let mut failures = Vec::new();
    for (literal, expected) in cases {
        let got = string_value(&ctx, &catalogs, literal).await;
        if got != *expected {
            failures.push(format!("{literal} -> {got:?} (expected {expected:?})"));
        }
    }
    assert!(
        failures.is_empty(),
        "double-quote collapse mismatches:\n{}",
        failures.join("\n")
    );
    assert_eq!(
        int_value(&ctx, &catalogs, "length(\"x\\\\\"\"y\")").await,
        4
    );
}

#[test]
fn escape_free_doubles_stay_borrowed_for_the_string_dialect() {
    for sql in [
        "SELECT \"a''b\" AS v",
        "SELECT \"a''b\"\"c''d\" AS v",
        "SELECT \"say 'hi' y\"\"all\" AS v",
        "SELECT \"it\"\"s\" AS v",
    ] {
        assert!(
            matches!(
                crate::spark_literals::canonicalize(sql).unwrap(),
                Cow::Borrowed(_)
            ),
            "`{sql}` carries no backslash and must reach the string dialect untouched"
        );
    }
}

#[tokio::test]
async fn double_quoted_raw_strings_answer_like_spark() {
    let (ctx, catalogs) = expr_ctx();
    let cases: &[(&str, &str)] = &[
        ("r\"\\d\"", "\\d"),
        ("R\"\\d\"", "\\d"),
        ("r\"a\\nb\"", "a\\nb"),
        ("r\"a\"\"b\"", "ab"),
        ("r\"\"", ""),
        ("r\"a\" \"b\"", "ab"),
        ("r\"a\" r\"b\"", "ab"),
    ];
    let mut failures = Vec::new();
    for (literal, expected) in cases {
        let got = string_value(&ctx, &catalogs, literal).await;
        if got != *expected {
            failures.push(format!("{literal} -> {got:?} (expected {expected:?})"));
        }
    }
    assert!(
        failures.is_empty(),
        "double-raw mismatches:\n{}",
        failures.join("\n")
    );
}

#[tokio::test]
async fn raw_doubled_quote_splits_like_spark() {
    let (ctx, catalogs) = expr_ctx();
    let cases: &[(&str, &str)] = &[
        ("r'a''b'", "ab"),
        ("r'a''\\n'", "a\n"),
        ("r'\\n''\\t'", "\\n\t"),
        ("r'a''b''c'", "ab'c"),
        ("r\"a\"\"\\n\"", "a\n"),
        ("r\"\\n\"\"\\t\"", "\\n\t"),
    ];
    let mut failures = Vec::new();
    for (literal, expected) in cases {
        let got = string_value(&ctx, &catalogs, literal).await;
        if got != *expected {
            failures.push(format!("{literal} -> {got:?} (expected {expected:?})"));
        }
    }
    assert!(
        failures.is_empty(),
        "raw-split mismatches:\n{}",
        failures.join("\n")
    );
    let empty = execute(&ctx, &catalogs, "SELECT r'''' AS v").await;
    assert!(empty.is_err(), "`r''''` must refuse (triple-raw edge)");
}

#[tokio::test]
async fn verbatim_keeps_doubled_quotes_and_marks_raw() {
    let (ctx, catalogs) = verbatim_expr_ctx();
    let cases: &[(&str, &str)] = &[
        ("'it''s'", "it''s"),
        ("''''", "''"),
        ("'x\\\\''y'", "x\\\\''y"),
        ("\"it\"\"s\"", "it\"\"s"),
        ("\"x\\\\\"\"y\"", "x\\\\\"\"y"),
        ("\"\\\"\"", "\\\""),
        ("r'a'", "'a"),
        ("r''", "'"),
        ("r'a''b'", "'ab"),
        ("r\"\"", "\""),
        ("r\"a\"\"b\"", "\"ab"),
    ];
    let mut failures = Vec::new();
    for (literal, expected) in cases {
        let got = string_value(&ctx, &catalogs, literal).await;
        if got != *expected {
            failures.push(format!("{literal} -> {got:?} (expected {expected:?})"));
        }
    }
    assert!(
        failures.is_empty(),
        "verbatim mismatches:\n{}",
        failures.join("\n")
    );
}

#[test]
fn unescape_collapses_only_the_literal_quote() {
    let unescape = crate::spark_literals::unescape_spark_literal;
    assert_eq!(unescape("x\\\\\"\"y", '"'), "x\\\"y");
    assert_eq!(unescape("a''b", '"'), "a''b");
    assert_eq!(unescape("a\"\"b", '\''), "a\"\"b");
    assert_eq!(unescape("it''s", '\''), "it's");
    assert_eq!(unescape("a\\tb", '\''), "a\tb");
}
