//! Unit pins for the `SET PROPERTIES` recognizer and its curated vocabulary. The schema-evolution
//! half needs a real catalog and is pinned end to end in `crate::tests`.

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::*;

/// Run text through the recognizer, then the stock parser, and hand back the options it produced.
fn options(sql: &str) -> Vec<SqlOption> {
    let rewritten = rewrite_set_properties(sql).unwrap_or_else(|| panic!("`{sql}` not recognized"));
    let mut statements =
        Parser::parse_sql(&GenericDialect {}, &rewritten).expect("the rewrite must parse");
    let Statement::AlterTable(alter) = statements.remove(0) else {
        panic!("expected an ALTER TABLE");
    };
    match alter.operations.into_iter().next() {
        Some(AlterTableOperation::SetOptionsParens { options }) => options,
        other => panic!("expected SET (…), got {other:?}"),
    }
}

fn parse_error(sql: &str) -> String {
    parse_set_properties(&options(sql))
        .expect_err("must refuse")
        .to_string()
}

/// The recognizer removes exactly the word `PROPERTIES` — nothing else about the user's text
/// changes, so byte offsets, spacing and every value expression survive verbatim.
#[test]
fn rewrite_removes_only_the_properties_keyword() {
    assert_eq!(
        rewrite_set_properties("ALTER TABLE ice.s.t SET PROPERTIES (format = 'PARQUET')").unwrap(),
        "ALTER TABLE ice.s.t SET            (format = 'PARQUET')"
    );
    // Case-insensitive, and the parse must actually succeed afterwards (that is the point).
    assert!(
        rewrite_set_properties("alter table ice.s.t set properties (format = 'PARQUET')").is_some()
    );
}

/// It does NOT fire on other statements, and — critically — a `SET PROPERTIES` inside a string
/// literal or a comment is structurally invisible, so the user's data can never be edited.
#[test]
fn rewrite_does_not_fire_on_other_statements_or_inside_literals() {
    for sql in [
        "ALTER TABLE ice.s.t ADD COLUMN c INT",
        "SELECT 'ALTER TABLE t SET PROPERTIES (a = 1)' AS note",
        "CREATE TABLE ice.s.t (a INT)",
        "ALTER TABLE ice.s.t SET (format = 'PARQUET')",
    ] {
        assert!(
            rewrite_set_properties(sql).is_none(),
            "must not fire on `{sql}`"
        );
    }
    // The literal carries the phrase; the statement does not.
    let sql = "ALTER TABLE ice.s.t ADD COLUMN c INT COMMENT 'SET PROPERTIES'";
    assert!(rewrite_set_properties(sql).is_none(), "{sql}");
}

/// The G4 hatch works here exactly as it does at CREATE — which is the whole reason the
/// recognizer routes through the stock parser instead of hand-reading the values.
#[test]
fn extra_properties_sets_raw_iceberg_keys() {
    let (sets, unsets) = parse_set_properties(&options(
        "ALTER TABLE ice.s.t SET PROPERTIES (extra_properties = \
         MAP(ARRAY['write.merge.mode', 'write.target-file-size-bytes'], \
             ARRAY['merge-on-read', '134217728']))",
    ))
    .expect("must parse");
    assert_eq!(
        sets.get("write.merge.mode").map(String::as_str),
        Some("merge-on-read")
    );
    assert_eq!(
        sets.get("write.target-file-size-bytes").map(String::as_str),
        Some("134217728")
    );
    assert!(unsets.is_empty());
}

/// `format` is validated against the create-time vocabulary and maps onto the real Iceberg key;
/// `format = DEFAULT` is Trino's reset spelling and unsets that key.
#[test]
fn format_sets_and_resets_the_iceberg_property() {
    let (sets, unsets) = parse_set_properties(&options(
        "ALTER TABLE ice.s.t SET PROPERTIES (format = 'parquet')",
    ))
    .expect("must parse");
    assert_eq!(
        sets.get(FORMAT_PROPERTY).map(String::as_str),
        Some("PARQUET")
    );
    assert!(unsets.is_empty());

    let (sets, unsets) = parse_set_properties(&options(
        "ALTER TABLE ice.s.t SET PROPERTIES (format = DEFAULT)",
    ))
    .expect("must parse");
    assert!(sets.is_empty());
    assert_eq!(unsets, vec![FORMAT_PROPERTY.to_string()]);
}

/// The raw-key round trip: the hatch SETS a dotted key, and the quoted dotted spelling with
/// DEFAULT is the only way to UNSET it.
#[test]
fn dotted_key_with_default_unsets_a_raw_property() {
    let (sets, unsets) = parse_set_properties(&options(
        "ALTER TABLE ice.s.t SET PROPERTIES (\"write.merge.mode\" = DEFAULT)",
    ))
    .expect("must parse");
    assert!(sets.is_empty());
    assert_eq!(unsets, vec!["write.merge.mode".to_string()]);

    // Setting a dotted key directly steers to the hatch instead (design §2 Q1).
    let err =
        parse_error("ALTER TABLE ice.s.t SET PROPERTIES (\"write.merge.mode\" = 'copy-on-write')");
    assert!(err.contains("extra_properties = MAP"), "{err}");
}

/// The G9 reserved refusals and the reserved-but-unchangeable keys each refuse loud and name
/// their trigger or their reason.
#[test]
fn reserved_and_unchangeable_keys_refuse_loud() {
    let sorted = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (sorted_by = ARRAY['a'])");
    assert!(sorted.contains("TRIGGER"), "{sorted}");

    let orc = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (format = 'ORC')");
    assert!(orc.contains("TRIGGER"), "{orc}");

    // pins: v3-2-create-v3-opt-in/C-008
    let version = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (format_version = '3')");
    assert!(version.contains("TRIGGER"), "{version}");

    let location = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (location = 's3://x/y')");
    assert!(location.contains("orphan"), "names the danger: {location}");

    let wipe = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (extra_properties = DEFAULT)");
    assert!(wipe.contains("wipe every raw property"), "{wipe}");
}

/// Q3: `partitioning` is the PRE-DESIGNATED future spelling for replace-spec, so its refusal must
/// say so, cite the ruling, and name the callable op that does the job today.
#[test]
fn partitioning_refuses_citing_q3_and_names_the_callable_op() {
    let err = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (partitioning = ARRAY['day(ts)'])");
    assert!(err.contains("Q3"), "cites the ruling: {err}");
    assert!(err.contains("callable operation"), "{err}");
    assert!(err.contains("UpdatePartitionSpec"), "{err}");
    assert!(err.contains("TRIGGER"), "{err}");
}

/// The typo guard: an unknown bare key lists what IS supported and points dotted keys at the
/// hatch, exactly as the CREATE-side guard does.
#[test]
fn unknown_bare_key_refuses_listing_the_supported_set() {
    let err = parse_error("ALTER TABLE ice.s.t SET PROPERTIES (formatt = 'PARQUET')");
    assert!(err.contains("`format`"), "{err}");
    assert!(err.contains("extra_properties"), "{err}");
}

/// An empty property list is a statement that would silently do nothing.
#[test]
fn empty_property_list_refuses() {
    let err = parse_set_properties(&[])
        .expect_err("must refuse")
        .to_string();
    assert!(err.contains("at least one property"), "{err}");
}

/// Only promotion-shaped targets are accepted for `SET DATA TYPE`; the narrowing targets are
/// rejected door-side with a stable message before any transaction opens.
#[test]
fn promotion_targets_are_bounded() {
    assert!(is_promotion_target(&PrimitiveType::Long));
    assert!(is_promotion_target(&PrimitiveType::Double));
    assert!(is_promotion_target(&PrimitiveType::Decimal {
        precision: 10,
        scale: 2
    }));
    assert!(!is_promotion_target(&PrimitiveType::Int));
    assert!(!is_promotion_target(&PrimitiveType::String));
    assert!(!is_promotion_target(&PrimitiveType::Boolean));
}

/// `DEFAULT` is recognized only as the bare keyword — a quoted `"DEFAULT"` is an identifier the
/// user chose, and a string `'DEFAULT'` is a value.
#[test]
fn default_keyword_recognition_is_quote_sensitive() {
    let bare = options("ALTER TABLE ice.s.t SET PROPERTIES (format = DEFAULT)");
    let SqlOption::KeyValue { value, .. } = &bare[0] else {
        panic!("expected key = value");
    };
    assert!(is_default_keyword(value));

    let quoted = options("ALTER TABLE ice.s.t SET PROPERTIES (format = \"DEFAULT\")");
    let SqlOption::KeyValue { value, .. } = &quoted[0] else {
        panic!("expected key = value");
    };
    assert!(
        !is_default_keyword(value),
        "a quoted identifier is not the keyword"
    );
}
