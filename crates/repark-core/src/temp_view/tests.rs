//! Unit pins for the temp-view name choke point.

use super::{TempViewHome, temp_view_ref, temp_view_ref_from_segment};

fn home() -> TempViewHome {
    TempViewHome {
        catalog: "datafusion".to_string(),
        schema: "public".to_string(),
        // These unit pins cover name resolution; provider identity requires a live session.
        provider: None,
    }
}

/// A single-part name resolves to the build-time home, not the live default catalog.
#[test]
fn a_single_part_name_is_pinned_full_against_the_home() {
    let reference = temp_view_ref(&home(), "v").expect("one-part name registers");
    assert_eq!(reference.catalog(), Some("datafusion"));
    assert_eq!(reference.schema(), Some("public"));
    assert_eq!(reference.table(), "v");
}

/// Identifier normalization follows DataFusion: unquoted names fold; quoted names preserve case.
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

/// Qualified names refuse at every arity, including parser fallback for four-part input.
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

/// The segment overload preserves quoted dots and folds unquoted names like `parse_str`.
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

/// Only the session's own home-qualified spelling resolves; other three-part names refuse.
#[test]
fn the_sessions_own_home_spelling_is_the_home_not_a_qualified_refusal() {
    let home = home();
    for spelling in [
        "datafusion.public.v",
        r#""datafusion"."public"."v""#,
        "`datafusion`.`public`.`v`",
    ] {
        let reference = temp_view_ref(&home, spelling)
            .unwrap_or_else(|error| panic!("home spelling {spelling} must resolve: {error}"));
        assert_eq!(reference.catalog(), Some("datafusion"));
        assert_eq!(reference.schema(), Some("public"));
        assert_eq!(reference.table(), "v");
    }
    // Neighbouring three-part names that are NOT this home still refuse.
    for other in ["ice.public.v", "datafusion.other.v"] {
        let error = temp_view_ref(&home, other)
            .expect_err("only the session's OWN home spelling is the home")
            .to_string();
        assert!(error.contains("SESSION-LOCAL"), "got: {error}");
    }
}
