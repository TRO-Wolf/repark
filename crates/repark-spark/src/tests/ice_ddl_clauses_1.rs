use super::super::*;
use super::common::*;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use crate::alter::rewrite_add_columns_plural;
use crate::normalize::has_angle_map_column_type;

fn tokenize(sql: &str) -> Vec<Token> {
    Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize()
        .unwrap_or_else(|error| panic!("{sql:?} must tokenize: {error}"))
}

fn render(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect::<String>()
}

fn rewritten_add_columns(sql: &str) -> String {
    render(&rewrite_add_columns_plural(&tokenize(sql)))
}

#[test]
fn add_columns_plural_splitter_tracks_angle_brackets_at_depth_zero() {
    let rendered = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (s2 STRUCT<p: INT, q: STRING>, m2 MAP<STRING, INT>, \
         a2 ARRAY<STRING>)",
    );
    assert!(
        !rendered.contains("COLUMNS"),
        "the plural keyword must be fully rewritten, got: {rendered}"
    );
    assert_eq!(
        rendered.matches("ADD COLUMN").count(),
        3,
        "each nested def must survive intact, got: {rendered}"
    );
    assert!(
        rendered.contains("s2 STRUCT<p: INT, q: STRING>")
            && rendered.contains("m2 MAP<STRING, INT>")
            && rendered.contains("a2 ARRAY<STRING>"),
        "no def may be split or reordered, got: {rendered}"
    );
}

#[test]
fn add_columns_plural_splitter_handles_paren_depth_one_and_shift_close() {
    let decimal = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (a STRUCT<x: DECIMAL(10, 2), y: ARRAY<INT>>, b INT)",
    );
    assert_eq!(
        decimal.matches("ADD COLUMN").count(),
        2,
        "angle depth at paren depth 1 must hold, got: {decimal}"
    );
    let nested = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (a ARRAY<STRUCT<x: INT, y: STRING>>, b INT)",
    );
    assert_eq!(
        nested.matches("ADD COLUMN").count(),
        2,
        "`>>` closes two angle levels, got: {nested}"
    );
    let triple = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (a STRUCT<m: MAP<STRING, ARRAY<INT>>>, b INT)",
    );
    assert_eq!(
        triple.matches("ADD COLUMN").count(),
        2,
        "`>>>` closes three angle levels, got: {triple}"
    );
}

#[test]
fn angle_map_alter_parses_under_widened_dialect() {
    let sql = "ALTER TABLE ice.ns.t ADD COLUMN m MAP<STRUCT<a: INT>, STRING>";
    assert!(
        has_angle_map_column_type(&tokenize(sql)),
        "a MAP< ALTER must trip the angle-map predicate"
    );
    let parsed = parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
    let Some((statement, _)) = parsed else {
        panic!("{sql:?} must parse after the widening");
    };
    assert!(matches!(statement, Statement::AlterTable(_)));
    let plural = "ALTER TABLE ice.ns.t ADD COLUMNS (s2 STRUCT<p: INT, q: STRING>, m2 MAP<STRING, \
     INT>, a2 ARRAY<STRING>)";
    let parsed =
        parse_single_normalized(plural).unwrap_or_else(|error| panic!("{plural:?}: {error}"));
    let Some((statement, _)) = parsed else {
        panic!("{plural:?} must parse after the splitter fix");
    };
    if let Statement::AlterTable(alter) = &statement {
        assert_eq!(alter.operations.len(), 3);
    } else {
        panic!("{plural:?} must parse as ALTER TABLE, got: {statement}");
    }
}

#[test]
fn clustered_by_reaches_partitioning_before_parse() {
    let sql = "CREATE TABLE ice.ns.t (id BIGINT, data STRING) USING iceberg CLUSTERED BY (id) \
     INTO 4 BUCKETS";
    let parsed = parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
    let Some((statement, partitioning)) = parsed else {
        panic!("{sql:?} must parse with its partitioning extracted");
    };
    assert!(matches!(statement, Statement::CreateTable(_)));
    assert_eq!(
        partitioning,
        vec![PartitionedByElement::Transform {
            name: "bucket".to_string(),
            args: vec!["4".to_string(), "id".to_string()],
        }]
    );
}

#[test]
fn dialect_widening_changes_no_other_alter() {
    for (sql, parses) in [
        ("ALTER TABLE ice.ns.t SET TBLPROPERTIES ('k'='v')", true),
        ("ALTER TABLE ice.ns.t UNSET TBLPROPERTIES ('k')", true),
        ("ALTER TABLE ice.ns.t ADD COLUMN c1 INT", true),
        ("ALTER TABLE ice.ns.t ADD COLUMNS (c2 INT, c3 STRING)", true),
        ("ALTER TABLE ice.ns.t DROP COLUMN c3", true),
        ("ALTER TABLE ice.ns.t DROP COLUMNS (c2, c3)", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN c1 TYPE BIGINT", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN c1 DROP NOT NULL", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN c1 SET NOT NULL", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN b FIRST", false),
        ("ALTER TABLE ice.ns.t ALTER COLUMN b AFTER id", false),
        (
            "ALTER TABLE ice.ns.t ADD PARTITION FIELD bucket(8, id)",
            false,
        ),
        ("ALTER TABLE ice.ns.t DROP PARTITION FIELD id_b8", false),
        (
            "ALTER TABLE ice.ns.t REPLACE PARTITION FIELD id_b8 WITH bucket(16, id)",
            false,
        ),
    ] {
        assert!(
            !has_angle_map_column_type(&tokenize(sql)),
            "{sql:?} must stay off the angle-map predicate"
        );
        let parsed =
            parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        assert_eq!(
            parsed.is_some(),
            parses,
            "{sql:?} must parse exactly as before the widening"
        );
    }
    let select = "SELECT CAST(x AS MAP<STRING, INT>) FROM t";
    let parsed =
        parse_single_normalized(select).unwrap_or_else(|error| panic!("{select:?}: {error}"));
    assert!(
        parsed.is_none(),
        "a MAP<> outside CREATE/ALTER keeps its dialect and stays unparsed"
    );
    let plain = "SELECT id, name FROM t";
    let parsed =
        parse_single_normalized(plain).unwrap_or_else(|error| panic!("{plain:?}: {error}"));
    assert!(parsed.is_some(), "an ordinary SELECT still parses");
}
