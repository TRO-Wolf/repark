//! Scanner-level pins for the ANSI `FOR … AS OF` rewrite.

use super::*;

fn spans(sql: &str) -> Vec<TimeTravelSpan> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize()
        .expect("tokenize");
    find_time_travel_spans(&tokens).expect("scan must succeed")
}

fn scan_error(sql: &str) -> String {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize()
        .expect("tokenize");
    find_time_travel_spans(&tokens)
        .expect_err("scan must refuse")
        .to_string()
}

/// The two ANSI spellings are recognized; the Spark spellings and the FOR-less forms are not recognized.
#[test]
fn recognizes_only_the_ansi_for_spellings() {
    assert!(sql_has_time_travel(
        "SELECT * FROM ice.sales.t FOR VERSION AS OF 1"
    ));
    assert!(sql_has_time_travel(
        "SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF '2020-01-01 00:00:00'"
    ));
    // Spark spellings — not this door's grammar.
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t FOR SYSTEM_VERSION AS OF 1"
    ));
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t FOR SYSTEM_TIME AS OF '2020-01-01'"
    ));
    // FOR is mandatory (design §2 Q5).
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t VERSION AS OF 1"
    ));
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t TIMESTAMP AS OF '2020-01-01'"
    ));
    // Ordinary SQL is untouched.
    assert!(!sql_has_time_travel("SELECT * FROM ice.sales.t"));
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t WHERE version = 1"
    ));
    assert!(!sql_has_time_travel("SELECT * FROM t FOR UPDATE"));
}

#[test]
fn span_extracts_table_and_spec() {
    let sql = "SELECT * FROM ice.sales.t FOR VERSION AS OF 42 WHERE id > 0";
    let found = spans(sql);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].table_parts, vec!["ice", "sales", "t"]);
    assert!(matches!(
        found[0].pin,
        TimeTravelPin::Version(TimeTravelSpec::SnapshotId(42))
    ));

    // The splice range must start at the name and end after the value — never eat the WHERE.
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize()
        .expect("tokenize");
    let mut rewritten = tokens.clone();
    rewritten.splice(
        found[0].table_start..found[0].clause_end,
        std::iter::once(Token::make_word("__v", None)),
    );
    assert_eq!(
        tokens_to_sql(&rewritten),
        "SELECT * FROM __v WHERE id > 0",
        "the splice must replace exactly the relation + clause"
    );
}

/// A single-quoted version is a branch or tag reference; a number is a snapshot id.
#[test]
fn version_ref_string_and_timestamp_values() {
    assert!(matches!(
        spans("SELECT * FROM ice.sales.t FOR VERSION AS OF 'audit_branch'")[0].pin,
        TimeTravelPin::Version(TimeTravelSpec::VersionRef(_))
    ));
    assert!(matches!(
        spans("SELECT * FROM ice.sales.t FOR VERSION AS OF 'main'")[0].pin,
        TimeTravelPin::Version(TimeTravelSpec::VersionRef(_))
    ));
    let timestamp = spans("SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF '2020-06-01 00:00:00'");
    let TimeTravelPin::TimestampExpr(tokens) = &timestamp[0].pin else {
        panic!("timestamp form must pin an expression");
    };
    assert_eq!(tokens_to_sql(tokens), "'2020-06-01 00:00:00'");
    let typed =
        spans("SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF TIMESTAMP '2020-06-01 00:00:00'");
    let TimeTravelPin::TimestampExpr(tokens) = &typed[0].pin else {
        panic!("typed-string form must pin an expression");
    };
    assert_eq!(tokens_to_sql(tokens), "TIMESTAMP '2020-06-01 00:00:00'");
}

#[test]
fn timestamp_expression_forms_keep_their_text() {
    for (sql, expected) in [
        (
            "SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF CAST('2020-06-01 00:00:00' AS TIMESTAMP)",
            "CAST('2020-06-01 00:00:00' AS TIMESTAMP)",
        ),
        (
            "SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF 1750000000 WHERE id > 0",
            "1750000000",
        ),
        (
            "SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF (SELECT CAST('2020-06-01' AS TIMESTAMP))",
            "(SELECT CAST('2020-06-01' AS TIMESTAMP))",
        ),
        (
            "SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF current_timestamp() ORDER BY id",
            "current_timestamp()",
        ),
    ] {
        let found = spans(sql);
        assert_eq!(found.len(), 1, "{sql}");
        let TimeTravelPin::TimestampExpr(tokens) = &found[0].pin else {
            panic!("{sql} must pin an expression");
        };
        assert_eq!(tokens_to_sql(tokens), expected, "{sql}");
    }
}

/// Iceberg snapshot ids are signed `i64`; the scanner accepts the split unary-minus token.
#[test]
fn negative_snapshot_id_is_scanned() {
    let found = spans("SELECT * FROM ice.sales.t FOR VERSION AS OF -9223372036854775807");
    assert_eq!(found.len(), 1);
    assert!(matches!(
        found[0].pin,
        TimeTravelPin::Version(TimeTravelSpec::SnapshotId(-9_223_372_036_854_775_807))
    ));
    assert_eq!(found[0].table_parts, vec!["ice", "sales", "t"]);
}

/// Every pinned relation in a join gets its own span, in statement order.
#[test]
fn multi_relation_join_yields_one_span_per_relation() {
    let found = spans(
        "SELECT * FROM ice.sales.a FOR VERSION AS OF 1 \
         JOIN ice.sales.b FOR VERSION AS OF 2 ON true",
    );
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].table_parts, vec!["ice", "sales", "a"]);
    assert!(matches!(
        found[0].pin,
        TimeTravelPin::Version(TimeTravelSpec::SnapshotId(1))
    ));
    assert_eq!(found[1].table_parts, vec!["ice", "sales", "b"]);
    assert!(matches!(
        found[1].pin,
        TimeTravelPin::Version(TimeTravelSpec::SnapshotId(2))
    ));
}

/// The ANSI variant of the quoted-name pin: `"` quotes an identifier, so quoted
/// parts remain table-name components.
#[test]
fn double_quoted_table_parts_are_identifiers() {
    let found = spans(r#"SELECT * FROM ice."sales"."t" FOR VERSION AS OF 7"#);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].table_parts, vec!["ice", "sales", "t"]);
    assert!(matches!(
        found[0].pin,
        TimeTravelPin::Version(TimeTravelSpec::SnapshotId(7))
    ));
}

/// Comments and string literals never produce a span because the tokenizer folds their contents
/// into spaces.
#[test]
fn comments_and_string_literals_do_not_false_positive() {
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t /* FOR VERSION AS OF 1 */"
    ));
    assert!(!sql_has_time_travel(
        "SELECT * FROM ice.sales.t -- FOR VERSION AS OF 1\nWHERE id > 0"
    ));
    assert!(!sql_has_time_travel(
        "SELECT 'FOR VERSION AS OF 1' AS note FROM ice.sales.t"
    ));
    assert!(spans("SELECT 'FOR VERSION AS OF 1' AS note FROM ice.sales.t").is_empty());
}

#[test]
fn recognized_clause_with_a_bad_value_refuses_loud() {
    for sql in [
        "SELECT * FROM ice.sales.t FOR VERSION AS OF main",
        r#"SELECT * FROM ice.sales.t FOR VERSION AS OF "main""#,
        "SELECT * FROM ice.sales.t FOR VERSION AS OF",
        "SELECT * FROM ice.sales.t FOR VERSION AS OF TIMESTAMP '2020-01-01 00:00:00'",
        "SELECT * FROM ice.sales.t FOR VERSION AS OF (SELECT 1)",
    ] {
        let err = scan_error(sql);
        assert!(
            err.contains("Invalid time travel spec"),
            "Spark parse text: {err}"
        );
    }

    let timestamp = spans("SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF 'not-a-time'");
    assert_eq!(timestamp.len(), 1);
    let TimeTravelPin::TimestampExpr(tokens) = &timestamp[0].pin else {
        panic!("unparsable timestamp must still pin an expression");
    };
    assert_eq!(tokens_to_sql(tokens), "'not-a-time'");
}

/// A clause with no table reference in front of it refuses rather than panicking on the walk.
#[test]
fn clause_without_a_relation_refuses() {
    let err = scan_error("FOR VERSION AS OF 1");
    assert!(
        err.contains("must follow a table reference"),
        "names the shape: {err}"
    );
}
