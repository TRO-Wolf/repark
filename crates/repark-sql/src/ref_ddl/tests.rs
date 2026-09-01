//! Recognizer pins for the ALTER-scoped branch/tag grammar.

use super::*;

fn parsed(sql: &str) -> RefDdl {
    try_parse_ref_ddl(sql)
        .unwrap_or_else(|| panic!("`{sql}` must be recognized"))
        .unwrap_or_else(|err| panic!("`{sql}` must parse: {err}"))
}

fn refused(sql: &str) -> String {
    try_parse_ref_ddl(sql)
        .unwrap_or_else(|| panic!("`{sql}` must be recognized"))
        .expect_err("must refuse")
        .to_string()
}

/// The plain create forms, for both ref kinds, with and without an explicit pin.
#[test]
fn parses_create_branch_and_tag() {
    assert_eq!(
        parsed("ALTER TABLE ice.sales.orders CREATE BRANCH audit"),
        RefDdl {
            table_parts: vec!["ice".into(), "sales".into(), "orders".into()],
            op: RefOp::Create {
                kind: SnapshotRefKind::Branch,
                name: "audit".into(),
                as_of_version: None,
                or_replace: false,
                retention: SnapshotRefRetention::default(),
            },
        }
    );
    assert_eq!(
        parsed("ALTER TABLE ice.sales.orders CREATE TAG v1 AS OF VERSION 42"),
        RefDdl {
            table_parts: vec!["ice".into(), "sales".into(), "orders".into()],
            op: RefOp::Create {
                kind: SnapshotRefKind::Tag,
                name: "v1".into(),
                as_of_version: Some(42),
                or_replace: false,
                retention: SnapshotRefRetention::default(),
            },
        }
    );
}

/// Snapshot ids are signed `i64`, so a negative pin must parse (the tokenizer splits the minus).
#[test]
fn parses_negative_snapshot_pin() {
    let RefOp::Create { as_of_version, .. } =
        parsed("ALTER TABLE ice.sales.orders CREATE TAG v1 AS OF VERSION -9223372036854775807").op
    else {
        panic!("expected a create");
    };
    assert_eq!(as_of_version, Some(-9_223_372_036_854_775_807));
}

/// `CREATE OR REPLACE` is distinct from create-only and must survive lowering.
#[test]
fn parses_create_or_replace() {
    let RefOp::Create {
        or_replace, name, ..
    } = parsed("ALTER TABLE ice.sales.orders CREATE OR REPLACE BRANCH audit").op
    else {
        panic!("expected a create");
    };
    assert!(or_replace);
    assert_eq!(name, "audit");
}

/// Both retention clauses map onto the tier-1 retention fields, including the snapshot count.
#[test]
fn parses_retention_clauses() {
    let RefOp::Create { retention, .. } = parsed(
        "ALTER TABLE ice.sales.orders CREATE BRANCH audit RETAIN 7 DAYS \
         WITH SNAPSHOT RETENTION 3 SNAPSHOTS",
    )
    .op
    else {
        panic!("expected a create");
    };
    assert_eq!(retention.max_ref_age_ms, Some(7 * 86_400_000));
    assert_eq!(retention.min_snapshots_to_keep, Some(3));
    assert_eq!(retention.max_snapshot_age_ms, None);

    let RefOp::Create { retention, .. } = parsed(
        "ALTER TABLE ice.sales.orders CREATE BRANCH audit \
         WITH SNAPSHOT RETENTION 12 HOURS",
    )
    .op
    else {
        panic!("expected a create");
    };
    assert_eq!(retention.max_snapshot_age_ms, Some(12 * 3_600_000));
    assert_eq!(retention.min_snapshots_to_keep, None);
}

#[test]
fn parses_both_snapshot_retention_halves() {
    let RefOp::Create { retention, .. } = parsed(
        "ALTER TABLE ice.sales.orders CREATE BRANCH audit RETAIN 5 DAYS \
         WITH SNAPSHOT RETENTION 3 SNAPSHOTS 7 DAYS",
    )
    .op
    else {
        panic!("expected a create");
    };
    assert_eq!(retention.max_ref_age_ms, Some(432_000_000));
    assert_eq!(retention.min_snapshots_to_keep, Some(3));
    assert_eq!(retention.max_snapshot_age_ms, Some(604_800_000));

    let RefOp::Create { retention, .. } = parsed(
        "ALTER TABLE ice.sales.orders CREATE BRANCH audit \
         WITH SNAPSHOT RETENTION 2 SNAPSHOTS 12 HOURS",
    )
    .op
    else {
        panic!("expected a create");
    };
    assert_eq!(retention.max_ref_age_ms, None);
    assert_eq!(retention.min_snapshots_to_keep, Some(2));
    assert_eq!(retention.max_snapshot_age_ms, Some(43_200_000));
}

#[test]
fn reversed_snapshot_retention_order_refuses() {
    let err = refused(
        "ALTER TABLE ice.sales.orders CREATE BRANCH audit \
         WITH SNAPSHOT RETENTION 7 DAYS 3 SNAPSHOTS",
    );
    assert!(err.contains("trailing clause"), "{err}");
}

/// Per-branch snapshot retention on a TAG is meaningless because a tag pins one snapshot.
#[test]
fn snapshot_retention_on_a_tag_refuses() {
    let err =
        refused("ALTER TABLE ice.sales.orders CREATE TAG v1 WITH SNAPSHOT RETENTION 3 SNAPSHOTS");
    assert!(err.contains("BRANCHES only"), "{err}");
    assert!(err.contains("RETAIN"), "steers to the tag clause: {err}");
}

/// The drop forms, with and without `IF EXISTS`.
#[test]
fn parses_drop_branch_and_tag() {
    assert_eq!(
        parsed("ALTER TABLE ice.sales.orders DROP BRANCH audit").op,
        RefOp::Drop {
            kind: SnapshotRefKind::Branch,
            name: "audit".into(),
            if_exists: false,
        }
    );
    assert_eq!(
        parsed("ALTER TABLE ice.sales.orders DROP TAG IF EXISTS v1").op,
        RefOp::Drop {
            kind: SnapshotRefKind::Tag,
            name: "v1".into(),
            if_exists: true,
        }
    );
}

/// ANSI quoting: `"audit"` is an identifier and unquotes into the ref name.
#[test]
fn double_quoted_names_are_identifiers() {
    let ddl = parsed(r#"ALTER TABLE ice."sales"."orders" CREATE BRANCH "audit branch""#);
    assert_eq!(
        ddl.table_parts,
        vec!["ice".to_string(), "sales".to_string(), "orders".to_string()]
    );
    let RefOp::Create { name, .. } = ddl.op else {
        panic!("expected a create");
    };
    assert_eq!(name, "audit branch");
}

/// Statements this recognizer must NOT claim — the supported ALTER ops, the Spark-only top-level
/// spelling (which stays a wrong-door sniff steer), and ordinary SQL mentioning `branch`.
#[test]
fn does_not_claim_other_statements() {
    for sql in [
        "ALTER TABLE ice.sales.orders ADD COLUMN c INT",
        "ALTER TABLE ice.sales.orders RENAME TO ice.sales.o2",
        "ALTER TABLE ice.sales.orders SET PROPERTIES (format = 'PARQUET')",
        // Spark-only top-level form: not this door's grammar.
        "CREATE BRANCH audit IN ice.sales.orders",
        "DROP BRANCH audit IN ice.sales.orders",
        "SELECT branch, tag FROM ice.sales.orders",
        "CREATE TABLE ice.sales.branch AS SELECT 1 AS a",
    ] {
        assert!(try_parse_ref_ddl(sql).is_none(), "must not claim `{sql}`");
    }
}

/// A recognized-but-malformed statement refuses with a targeted message.
#[test]
fn malformed_forms_refuse_loud() {
    let trailing = refused("ALTER TABLE ice.sales.orders CREATE BRANCH audit SOMETHING ELSE");
    assert!(trailing.contains("trailing clause"), "{trailing}");

    let bad_pin = refused("ALTER TABLE ice.sales.orders CREATE TAG v1 AS OF VERSION 'main'");
    assert!(bad_pin.contains("snapshot id"), "{bad_pin}");

    let half_pin = refused("ALTER TABLE ice.sales.orders CREATE TAG v1 AS OF 42");
    assert!(half_pin.contains("AS OF VERSION"), "{half_pin}");

    let bad_unit = refused("ALTER TABLE ice.sales.orders CREATE BRANCH audit RETAIN 7 FORTNIGHTS");
    assert!(bad_unit.contains("unknown time unit"), "{bad_unit}");

    let zero = refused("ALTER TABLE ice.sales.orders CREATE BRANCH audit RETAIN 0 DAYS");
    assert!(zero.contains("must be positive"), "{zero}");

    let bad_retention = refused("ALTER TABLE ice.sales.orders CREATE BRANCH a WITH RETENTION 3");
    assert!(
        bad_retention.contains("WITH SNAPSHOT RETENTION"),
        "{bad_retention}"
    );
}

/// EVERY leftover token refuses, not just identifiers — numbers and punctuation included.
/// Mutation: restore the `filter_map(Sig::ident)` → the first four rows red.
#[test]
fn trailing_non_identifier_tokens_refuse_too() {
    for (sql, leftover) in [
        ("ALTER TABLE ice.sales.orders DROP BRANCH audit 5", "5"),
        (
            "ALTER TABLE ice.sales.orders CREATE BRANCH audit AS OF VERSION 7 99",
            "99",
        ),
        ("ALTER TABLE ice.sales.orders DROP TAG v1 ,", ","),
        (
            "ALTER TABLE ice.sales.orders CREATE BRANCH audit RETAIN 7 DAYS )",
            ")",
        ),
        (
            "ALTER TABLE ice.sales.orders CREATE BRANCH audit SOMETHING",
            "SOMETHING",
        ),
    ] {
        let message = refused(sql);
        assert!(
            message.contains("trailing clause") && message.contains(leftover),
            "`{sql}` must refuse and NAME the leftover `{leftover}`: {message}"
        );
    }
}

/// A single trailing statement terminator is not a leftover. One statement ending in `;` is accepted.
#[test]
fn a_trailing_semicolon_is_not_a_trailing_clause() {
    assert_eq!(
        parsed("ALTER TABLE ice.sales.orders DROP BRANCH audit;"),
        RefDdl {
            table_parts: vec!["ice".into(), "sales".into(), "orders".into()],
            op: RefOp::Drop {
                kind: SnapshotRefKind::Branch,
                name: "audit".into(),
                if_exists: false,
            },
        }
    );
    assert!(
        try_parse_ref_ddl("ALTER TABLE ice.sales.orders CREATE TAG v1 AS OF VERSION 42 ;  ")
            .expect("recognized")
            .is_ok()
    );
}

/// A ref name is part of a metadata key, so it uses the same path-escape hygiene as other names.
#[test]
fn path_escaping_ref_names_refuse() {
    let err = refused("ALTER TABLE ice.sales.orders CREATE BRANCH \"../escape\"");
    assert!(
        err.to_lowercase().contains("snapshot ref"),
        "names the segment kind: {err}"
    );
}
