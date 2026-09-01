//! In-module recognizer and sniff pins for `../ref_ddl.rs`.

use super::*;

#[test]
fn parses_alter_create_branch_as_of() {
    let ddl = try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE BRANCH audit AS OF VERSION 42")
        .expect("recognized")
        .expect("ok");
    assert_eq!(ddl.table_parts, ["ice", "sales", "t"]);
    assert!(matches!(
        ddl.op,
        RefOp::Create {
            kind: SnapshotRefKind::Branch,
            ref name,
            as_of_version: Some(42),
            or_replace: false,
            retention,
        } if name == "audit" && retention.is_empty()
    ));
}

#[test]
fn parses_alter_create_tag_current() {
    let ddl = try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE TAG release")
        .expect("recognized")
        .expect("ok");
    assert!(matches!(
        ddl.op,
        RefOp::Create {
            kind: SnapshotRefKind::Tag,
            ref name,
            as_of_version: None,
            or_replace: false,
            ..
        } if name == "release"
    ));
}

#[test]
fn parses_create_or_replace_branch() {
    let ddl =
        try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE OR REPLACE BRANCH audit AS OF VERSION 7")
            .expect("recognized")
            .expect("ok");
    assert!(matches!(
        ddl.op,
        RefOp::Create {
            kind: SnapshotRefKind::Branch,
            ref name,
            as_of_version: Some(7),
            or_replace: true,
            ..
        } if name == "audit"
    ));
}

#[test]
fn parses_bare_replace_branch() {
    let ddl = try_parse_ref_ddl("ALTER TABLE ice.sales.t REPLACE BRANCH audit AS OF VERSION 9")
        .expect("recognized")
        .expect("ok");
    assert!(matches!(
        ddl.op,
        RefOp::Replace {
            kind: SnapshotRefKind::Branch,
            ref name,
            as_of_version: Some(9),
            ..
        } if name == "audit"
    ));
}

#[test]
fn parses_retain_and_snapshot_retention() {
    let ddl = try_parse_ref_ddl(
        "ALTER TABLE ice.sales.t CREATE BRANCH audit RETAIN 7 DAYS \
         WITH SNAPSHOT RETENTION 10 SNAPSHOTS",
    )
    .expect("recognized")
    .expect("ok");
    match ddl.op {
        RefOp::Create { retention, .. } => {
            assert_eq!(retention.max_ref_age_ms, Some(7 * 86_400_000));
            assert_eq!(retention.min_snapshots_to_keep, Some(10));
            assert!(retention.max_snapshot_age_ms.is_none());
        }
        other => panic!("expected Create, got {other:?}"),
    }
}

#[test]
fn parses_snapshot_retention_days() {
    let ddl =
        try_parse_ref_ddl("CREATE BRANCH audit IN ice.sales.t WITH SNAPSHOT RETENTION 2 DAYS")
            .expect("recognized")
            .expect("ok");
    match ddl.op {
        RefOp::Create { retention, .. } => {
            assert_eq!(retention.max_snapshot_age_ms, Some(2 * 86_400_000));
        }
        other => panic!("expected Create, got {other:?}"),
    }
}

#[test]
fn tag_with_snapshot_retention_refuses() {
    let err = try_parse_ref_ddl(
        "ALTER TABLE ice.sales.t CREATE TAG t1 RETAIN 1 DAYS WITH SNAPSHOT RETENTION 2 DAYS",
    )
    .expect("recognized")
    .expect_err("tag snapshot retention");
    assert!(
        err.to_string().contains("BRANCH") || err.to_string().contains("tag"),
        "got: {err}"
    );
}

#[test]
fn non_ref_returns_none() {
    assert!(try_parse_ref_ddl("SELECT 1").is_none());
    assert!(try_parse_ref_ddl("ALTER TABLE ice.sales.t SET TBLPROPERTIES ('a'='b')").is_none());
    assert!(try_parse_ref_ddl("ALTER TABLE ice.create.branch RENAME TO ice.sales.other").is_none());
}

/// Trailing junk / IF EXISTS still refuse loud (not silent drop).
#[test]
fn trailing_tokens_after_as_of_or_drop_refuse_loud() {
    for sql in [
        "ALTER TABLE ice.sales.t CREATE BRANCH audit AS OF VERSION 42 RETENTION 7 DAYS",
        "CREATE BRANCH audit IN ice.sales.t AS OF VERSION 7 IF NOT EXISTS",
        "ALTER TABLE ice.sales.t DROP BRANCH audit IF EXISTS",
        "DROP TAG t1 IN ice.sales.t CASCADE",
        "ALTER TABLE ice.sales.t CREATE TAG release EXTRA",
    ] {
        let err = try_parse_ref_ddl(sql)
            .expect("recognized as ref DDL")
            .expect_err("trailing must refuse");
        let message = err.to_string();
        assert!(
            message.contains("not supported")
                || message.contains("trailing")
                || message.contains("unit"),
            "sql={sql:?} message={message}"
        );
        assert!(
            !message.contains("ParserError"),
            "must not fall through to opaque parse for {sql:?}: {message}"
        );
    }
}

#[test]
fn empty_ref_name_refuses_loud() {
    for sql in [
        "ALTER TABLE ice.sales.t CREATE BRANCH ``",
        "CREATE BRANCH `` IN ice.sales.t AS OF VERSION 1",
    ] {
        let err = try_parse_ref_ddl(sql)
            .expect("recognized")
            .expect_err("empty ref name");
        assert!(err.to_string().contains("empty"), "sql={sql:?} got: {err}");
    }
}

#[test]
fn qi1_unquote_ident_undoubles_embedded_quotes() {
    assert_eq!(unquote_ident(r#""na""me""#), "na\"me");
    assert_eq!(unquote_ident("`plain`"), "plain");
    assert_eq!(unquote_ident("bare"), "bare");
}

#[test]
fn qi1_ref_name_path_escape_shared_needles() {
    for (segment, kind_tag) in [
        ("foo..bar", "traversal"),
        ("../etc", "traversal"),
        ("a/b", "separator"),
        (r"a\b", "separator"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.t CREATE BRANCH `{segment}`");
        let err = try_parse_ref_ddl(&sql)
            .expect("recognized as ref DDL")
            .expect_err("path-escape ref must refuse");
        let text = err.to_string();
        match kind_tag {
            "traversal" => assert!(
                text.contains("path traversal") || text.contains(".."),
                "segment {segment:?}: {text}"
            ),
            "separator" => assert!(
                text.contains("path separators") || text.contains('/') || text.contains('\\'),
                "segment {segment:?}: {text}"
            ),
            other => panic!("unknown kind tag {other}"),
        }
    }
    let ok = try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE BRANCH audit")
        .expect("recognized")
        .expect("safe ref name");
    assert!(matches!(
        ok.op,
        RefOp::Create {
            kind: SnapshotRefKind::Branch,
            ref name,
            ..
        } if name == "audit"
    ));
}

#[test]
fn write_to_branch_sniff_detects_four_part_insert() {
    assert!(
        sniff_write_to_branch("INSERT INTO ice.sales.t.audit SELECT 1 AS id, 'x' AS name")
            .is_some()
    );
    assert!(sniff_write_to_branch("INSERT INTO ice.sales.t.branch_audit SELECT 1").is_some());
    // Two-part `table.branch_foo` under default catalog.
    assert!(sniff_write_to_branch("INSERT INTO t.branch_audit SELECT 1").is_some());
    // Normal three-part INSERT must not trip — including a table named `branch_exp`.
    assert!(sniff_write_to_branch("INSERT INTO ice.sales.t SELECT 1 AS id, 'x' AS name").is_none());
    assert!(
        sniff_write_to_branch("INSERT INTO mem.ns.branch_exp SELECT 4 AS id, 'd' AS name")
            .is_none()
    );
    // Metadata-table DML is a different refuse path (not write-to-branch).
    assert!(sniff_write_to_branch("INSERT INTO ice.sales.t.snapshots SELECT 1").is_none());
}

/// The sniff separates unambiguous four-part names from resolution-ambiguous two-part names.
#[test]
fn write_to_branch_sniff_kinds() {
    assert_eq!(
        sniff_write_to_branch("INSERT INTO ice.sales.t.branch_audit SELECT 1"),
        Some(WriteToBranchSniff::MultiPart)
    );
    assert_eq!(
        sniff_write_to_branch("INSERT INTO t.branch_audit SELECT 1"),
        Some(WriteToBranchSniff::TwoPart {
            parts: ["t".to_string(), "branch_audit".to_string()]
        })
    );
    // A genuine `schema.branch_daily` sniffs TwoPart.
    assert_eq!(
        sniff_write_to_branch("INSERT INTO public.branch_daily SELECT 1"),
        Some(WriteToBranchSniff::TwoPart {
            parts: ["public".to_string(), "branch_daily".to_string()]
        })
    );
    // Two-part without the `branch_` prefix is not sniffed at all.
    assert_eq!(sniff_write_to_branch("INSERT INTO ns.daily SELECT 1"), None);
}
