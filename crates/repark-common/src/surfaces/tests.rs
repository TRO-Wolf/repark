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

/// `ALL` is the audit's universe, so a silent duplicate would let a door "map" a surface twice
/// and still satisfy the per-ID count. The `surface_ids!` macro makes const↔`ALL` drift
/// impossible, but not a repeated declaration.
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

/// IDs are `SCREAMING_SNAKE_CASE`. Not cosmetics: the names appear verbatim in audit failures and
/// in both doors' matrices, and a lowercase or hyphenated straggler is the first sign someone
/// hand-wrote an ID string instead of using the const.
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

/// The registry's size is itself a reviewed number: 43 surfaces as of PR-5. A silent +1 means a
/// capability entered the product vocabulary without the ledger's row counts being revisited.
/// MUTATION: add an ID without updating this count → this REDs.
#[test]
fn all_has_the_reviewed_surface_count() {
    assert_eq!(
        ALL.len(),
        43,
        "the surface count changed — update task/p2f-ansi-m1-ledger.md's row-count table too"
    );
}

/// The macro derives each wire name from the constant's own identifier, so the copy-paste class
/// (`pub const DELETE: SurfaceId = SurfaceId("UPDATE")`) is unrepresentable. This test pins that
/// property for the few IDs most likely to be copied from each other.
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
}

/// A complete, exact mapping passes. The baseline both door matrices ride on.
#[test]
fn audit_accepts_a_complete_matrix() {
    let rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    assert_eq!(audit("test-door", &rows), Ok(()));
}

/// The core guarantee: a surface with no row FAILS, naming the ID and the door. This is the
/// build-enforcement half of "absence is typed" — a capability cannot be dropped quietly.
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

/// A row naming an ID outside `ALL` fails — the stale-row-after-rename case. Reachable only
/// through a const the registry no longer lists, which is why it is worth catching.
#[test]
fn audit_reports_an_unknown_id() {
    const GHOST: SurfaceId = SurfaceId("MERGE_INTO_LEGACY");
    let mut rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    rows.push((GHOST, absent()));
    let err = audit("test-door", &rows).expect_err("an unknown id must fail the audit");
    assert!(err.contains("MERGE_INTO_LEGACY"), "{err}");
    assert!(err.contains("not a known surface ID"), "{err}");
}

/// Two rows for one surface fail. Without this, a duplicate could mask an unmapped ID by
/// keeping the row count right.
#[test]
fn audit_reports_a_duplicate_row() {
    let mut rows: Vec<(SurfaceId, Row)> = ALL.iter().map(|id| (*id, absent())).collect();
    rows.push((CTAS, tested("some::test")));
    let err = audit("test-door", &rows).expect_err("a duplicate must fail the audit");
    assert!(err.contains("2 rows"), "must name the duplication: {err}");
    assert!(err.contains("CTAS"), "{err}");
}

/// An untraceable row fails: a `Tested` with no test name, or an `Absent` with no reason/adr.
/// A row that cites nothing is indistinguishable from an oversight — exactly the state the
/// registry exists to make impossible.
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

/// `is_tested` partitions the two variants — the accessor both doors' row-count pins use.
#[test]
fn is_tested_partitions_the_variants() {
    assert!(tested("t").is_tested());
    assert!(!absent().is_tested());
}

/// The four session profiles are distinct values. Graft G5's whole point is that `Native` and
/// `SparkExtended` are NOT interchangeable, and that `TwoSession` is its own thing.
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
