//! Unit pins for the temp-view name choke point (R6-1).

use super::{TempViewHome, temp_view_ref, temp_view_ref_from_segment};

fn home() -> TempViewHome {
    TempViewHome {
        catalog: "datafusion".to_string(),
        schema: "public".to_string(),
        // NAME-only home: these unit pins are about the name rule. The provider-identity half of
        // the home (`assert_home_intact`) needs a live `SessionContext`, so it is pinned in
        // `repark-core/tests/temp_view_doors.rs` instead.
        provider: None,
    }
}

/// Kills: forwarding the raw name to `register_table` (BASE) — a one-part name must be pinned
/// `Full` against the HOME, not left `Bare` for the live default catalog to resolve.
#[test]
fn a_single_part_name_is_pinned_full_against_the_home() {
    let reference = temp_view_ref(&home(), "v").expect("one-part name registers");
    assert_eq!(reference.catalog(), Some("datafusion"));
    assert_eq!(reference.schema(), Some("public"));
    assert_eq!(reference.table(), "v");
}

/// Kills: dropping the `parse_str` and building the reference from the raw `&str` — that would
/// silently change identifier normalization (BASE lowercased unquoted names via `register_table`).
#[test]
fn identifier_normalization_matches_datafusions_own_parse() {
    assert_eq!(temp_view_ref(&home(), "MyView").unwrap().table(), "myview");
    assert_eq!(
        temp_view_ref(&home(), r#""MyView""#).unwrap().table(),
        "MyView"
    );
    // A QUOTED dot is one identifier, not a qualification (C2-L-006 quote rules).
    assert_eq!(temp_view_ref(&home(), r#""a.b""#).unwrap().table(), "a.b");
}

/// Kills: letting a qualified name through (the R6-1 leak). The four-part row also kills
/// trusting `TableReference::parse_str`'s arity alone — MEASURED: it returns
/// `Bare { table: "a.b.c.d" }` past three parts, so a `Bare` check by itself would let a
/// qualified spelling become one oddly-named temp view.
#[test]
fn qualified_names_refuse_at_every_arity() {
    for name in ["ice.sales.v", "sales.v", "a.b.c.d"] {
        let error = temp_view_ref(&home(), name)
            .expect_err("a qualified temp-view name must refuse")
            .to_string();
        assert!(
            error.contains("SESSION-LOCAL") && error.contains(name),
            "refusal must name the rule and the name, got: {error}"
        );
    }
}

/// Kills: re-parsing an ALREADY-parsed segment on the `table_exists` path — which refused the
/// allowed quoted spelling `"a.b"` as "qualified" (round-6 critic S3) — and kills dropping the
/// case fold, which BASE got for free from `TableReference::parse_str` inside `table_exist(&str)`.
#[test]
fn the_segment_overload_normalizes_like_parse_str() {
    let home = home();
    assert_eq!(
        temp_view_ref_from_segment(&home, "a.b", true).table(),
        "a.b",
        "a quoted segment keeps its dot AND its case"
    );
    assert_eq!(
        temp_view_ref_from_segment(&home, "MyView", false).table(),
        "myview",
        "an unquoted segment folds, exactly as `parse_str` folds it at registration"
    );
    assert_eq!(
        temp_view_ref_from_segment(&home, "MyView", true).table(),
        "MyView"
    );
    let reference = temp_view_ref_from_segment(&home, "v", false);
    assert_eq!(reference.catalog(), Some("datafusion"));
    assert_eq!(reference.schema(), Some("public"));
}
