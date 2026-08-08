//! Scanner-level pins for the ANSI `FOR … AS OF` rewrite.
//!
//! This is the v1 span pin set ported as double-quote ANSI variants (design graft G7): the same
//! properties v1's `repark_sql::time_travel` pinned at the port pin `fc3f48102` — span extraction,
//! ref-name strings, negative snapshot ids, multi-relation joins, comment and string-literal
//! immunity, quoted name parts — re-expressed for a door whose identifier quote is `"` and whose
//! `FOR` is mandatory. The end-to-end (session) rows live in `crate::tests`.

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

/// The two ANSI spellings are recognized; the Spark spellings and the FOR-less forms are NOT
/// (they belong to the wrong-door sniff, which names the ANSI spelling).
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

/// A span carries the table name and the resolved spec, and covers exactly the name + clause.
#[test]
fn span_extracts_table_and_spec() {
    let sql = "SELECT * FROM ice.sales.t FOR VERSION AS OF 42 WHERE id > 0";
    let found = spans(sql);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].table_parts, vec!["ice", "sales", "t"]);
    assert_eq!(found[0].spec, TimeTravelSpec::SnapshotId(42));

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

/// A single-quoted version value is a branch/tag REF (the string-ref pin), a number is a snapshot
/// id, and a timestamp value resolves through the hoisted core parser.
#[test]
fn version_ref_string_and_timestamp_values() {
    assert_eq!(
        spans("SELECT * FROM ice.sales.t FOR VERSION AS OF 'audit_branch'")[0].spec,
        TimeTravelSpec::VersionRef("audit_branch".into())
    );
    assert_eq!(
        spans("SELECT * FROM ice.sales.t FOR VERSION AS OF 'main'")[0].spec,
        TimeTravelSpec::VersionRef("main".into())
    );
    assert!(matches!(
        spans("SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF '2020-06-01 00:00:00'")[0].spec,
        TimeTravelSpec::TimestampMs(_)
    ));
    assert!(matches!(
        spans("SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF TIMESTAMP '2020-06-01 00:00:00'")[0]
            .spec,
        TimeTravelSpec::TimestampMs(_)
    ));
}

/// Iceberg snapshot ids are signed `i64` and are routinely negative; sqlparser splits the unary
/// minus from the digits, so the scanner must join them back.
#[test]
fn negative_snapshot_id_is_scanned() {
    let found = spans("SELECT * FROM ice.sales.t FOR VERSION AS OF -9223372036854775807");
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0].spec,
        TimeTravelSpec::SnapshotId(-9_223_372_036_854_775_807)
    );
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
    assert_eq!(found[0].spec, TimeTravelSpec::SnapshotId(1));
    assert_eq!(found[1].table_parts, vec!["ice", "sales", "b"]);
    assert_eq!(found[1].spec, TimeTravelSpec::SnapshotId(2));
}

/// The ANSI variant of the v1 quoted-name-parts pin: `"` quotes an IDENTIFIER here, so a quoted
/// name part is unquoted into the catalog lookup.
#[test]
fn double_quoted_table_parts_are_identifiers() {
    let found = spans(r#"SELECT * FROM ice."sales"."t" FOR VERSION AS OF 7"#);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].table_parts, vec!["ice", "sales", "t"]);
    assert_eq!(found[0].spec, TimeTravelSpec::SnapshotId(7));
}

/// Comments and string literals can never produce a span — the tokenizer folds a comment into
/// whitespace and a literal into one token, so neither can be read as structure.
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

/// A RECOGNIZED clause with an unusable value refuses loud and names the spelling. Falling
/// through would surrender the user to `Expected: one of UPDATE or SHARE, found: VERSION`.
#[test]
fn recognized_clause_with_a_bad_value_refuses_loud() {
    let err = scan_error("SELECT * FROM ice.sales.t FOR VERSION AS OF main");
    assert!(
        err.contains("FOR VERSION AS OF"),
        "names the spelling: {err}"
    );

    // A double-quoted value is an identifier in this door, not a ref string — say so.
    let quoted = scan_error(r#"SELECT * FROM ice.sales.t FOR VERSION AS OF "main""#);
    assert!(
        quoted.contains("quoted IDENTIFIER") && quoted.contains("'main'"),
        "steers to the single-quoted spelling: {quoted}"
    );

    let timestamp = scan_error("SELECT * FROM ice.sales.t FOR TIMESTAMP AS OF 'not-a-time'");
    assert!(
        timestamp.contains("FOR TIMESTAMP AS OF"),
        "names the spelling: {timestamp}"
    );

    let dangling = scan_error("SELECT * FROM ice.sales.t FOR VERSION AS OF");
    assert!(
        dangling.contains("ends after AS OF"),
        "names the shape: {dangling}"
    );

    // A TIMESTAMP literal is not a version.
    let mismatched =
        scan_error("SELECT * FROM ice.sales.t FOR VERSION AS OF TIMESTAMP '2020-01-01 00:00:00'");
    assert!(
        mismatched.contains("not a version"),
        "names the mismatch: {mismatched}"
    );
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
