use super::*;

fn tested(test: &'static str) -> Row {
    Row::Tested {
        test,
        profile: SessionProfile::Unit,
    }
}

fn absent() -> Row {
    Row::DeliberatelyAbsent {
        reason: "r",
        adr: "docs/design/sql-doors.md §2",
    }
}

/// Duplicate IDs could satisfy per-ID counts while masking a missing surface.
/// MUTATION: declare `CTAS;` twice in the macro → this REDs.
#[test]
fn all_ids_are_unique_and_named() {
    let unique: BTreeSet<SurfaceId> = ALL.iter().copied().collect();
    assert_eq!(
        unique.len(),
        ALL.len(),
        "surfaces::ALL must have no duplicates"
    );
    assert!(!ALL.is_empty());
    for id in ALL {
        assert!(!id.name().is_empty());
    }
}

/// IDs use `SCREAMING_SNAKE_CASE` because names appear in audit failures and both door matrices.
#[test]
fn ids_are_screaming_snake_case() {
    for id in ALL {
        let name = id.name();
        assert!(
            name.chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
            "surface id {name} must be SCREAMING_SNAKE_CASE"
        );
        assert!(
            !name.starts_with('_') && !name.ends_with('_'),
            "bad shape: {name}"
        );
    }
}

/// The reviewed registry count prevents a capability from bypassing the ledger's row counts.
/// MUTATION: add an ID without updating this count → this REDs.
#[test]
fn all_has_the_reviewed_surface_count() {
    assert_eq!(
        ALL.len(),
        50,
        "the surface count changed — update task/s2-g8-ledger.md's row-count table too"
    );
}

/// Wire names derive from constant identifiers, preventing copy-paste mismatches.
#[test]
fn ids_are_self_naming() {
    assert_eq!(DELETE.name(), "DELETE");
    assert_eq!(UPDATE.name(), "UPDATE");
    assert_eq!(CTAS.name(), "CTAS");
    assert_eq!(TABLE_OPTION_FORMAT.name(), "TABLE_OPTION_FORMAT");
    assert_eq!(
        TABLE_OPTION_FORMAT_VERSION.name(),
        "TABLE_OPTION_FORMAT_VERSION"
    );
    assert_eq!(format!("{CTAS}"), "CTAS", "Display renders the bare name");
    assert_eq!(SEMANTICS_NULL_ORDERING.name(), "SEMANTICS_NULL_ORDERING");
}

/// A complete matrix passes the audit.
#[test]
fn audit_accepts_a_complete_matrix() {
    let rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    assert_eq!(audit("test-door", &rows), Ok(()));
}

/// A surface without a row fails with the ID and door name.
/// MUTATION: make `audit` skip the zero-count arm → this REDs.
#[test]
fn audit_reports_an_unmapped_surface() {
    let rows: Vec<(SurfaceId, Row)> = ALL
        .iter()
        .filter(|id| **id != MERGE)
        .map(|id| (*id, absent()))
        .collect();
    let err = audit("test-door", &rows).expect_err("a missing row must fail the audit");
    assert!(err.contains("MERGE"), "must name the gap: {err}");
    assert!(err.contains("NO row"), "{err}");
    assert!(err.contains("test-door"), "must name the door: {err}");
}

/// A row naming an ID outside `ALL` fails as a stale-row-after-rename case.
#[test]
fn audit_reports_an_unknown_id() {
    const GHOST: SurfaceId = SurfaceId("MERGE_INTO_LEGACY");
    let mut rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    rows.push((GHOST, absent()));
    let err = audit("test-door", &rows).expect_err("an unknown id must fail the audit");
    assert!(err.contains("MERGE_INTO_LEGACY"), "{err}");
    assert!(err.contains("not a known surface ID"), "{err}");
}

/// Two rows for one surface fail because duplicates can mask an unmapped ID.
#[test]
fn audit_reports_a_duplicate_row() {
    let mut rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    rows.push((CTAS, tested("some::test")));
    let err = audit("test-door", &rows).expect_err("a duplicate must fail the audit");
    assert!(err.contains("2 rows"), "must name the duplication: {err}");
    assert!(err.contains("CTAS"), "{err}");
}

/// A `Tested` row needs a test name, and an `Absent` row needs a reason and ADR.
#[test]
fn audit_reports_untraceable_rows() {
    let mut rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    rows[0].1 = Row::Tested {
        test: "   ",
        profile: SessionProfile::Native,
    };
    rows[1].1 = Row::DeliberatelyAbsent {
        reason: "still on the backlog",
        adr: "",
    };
    let err = audit("test-door", &rows).expect_err("untraceable rows must fail the audit");
    assert!(err.contains("empty test name"), "{err}");
    assert!(err.contains("BOTH a reason and an adr"), "{err}");
}

/// `is_tested` distinguishes the two row variants.
#[test]
fn is_tested_partitions_the_variants() {
    assert!(tested("t").is_tested());
    assert!(!absent().is_tested());
}

/// Session profiles remain distinct because native and Spark-extended evidence is not interchangeable.
#[test]
fn session_profiles_are_distinct() {
    let all = [
        SessionProfile::Unit,
        SessionProfile::Native,
        SessionProfile::SparkExtended,
        SessionProfile::TwoSession,
    ];
    for (i, a) in all.iter().enumerate() {
        for (j, b) in all.iter().enumerate() {
            assert_eq!(i == j, a == b, "{a:?} vs {b:?}");
        }
    }
}
