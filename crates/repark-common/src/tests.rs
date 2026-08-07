use super::*;

/// Pins the enumerated variant→partition routing (`exception_class`). The risk: a variant
/// silently landing in the wrong Python exception (a parse error surfacing as a base
/// `PySparkException`, an analysis error swallowed into the catch-all, or — the audit's
/// CQ-002 — a scope gate's `NotImplemented` collapsing to base instead of the unsupported
/// class). The match is exhaustive with no `_`, so this test plus the compiler together
/// guarantee every variant is deliberately routed. (U4 2026-07-18: `NotImplemented` moved
/// Base → Unsupported; `Iceberg` added → Base — the charter-sanctioned in-commit update.
/// Group X 2026-07-24: `Config` moved `Base` → `IllegalArgument`, per the live pyspark 4.0.0
/// oracle for an invalid config value — the same in-commit update discipline.)
#[test]
fn exception_class_routes_every_variant() {
    assert_eq!(
        Error::Parse("syntax".into()).exception_class(),
        ErrorClass::Parse
    );
    assert_eq!(
        Error::Analysis("no such column".into()).exception_class(),
        ErrorClass::Analysis
    );
    assert_eq!(
        Error::DataFusion("boom".into()).exception_class(),
        ErrorClass::Base
    );
    // Group X: an invalid `.config(...)` value is Spark's JVM `IllegalArgumentException`, NOT
    // the base bucket. MUTATION: route `Config` back to `ErrorClass::Base` → this REDs.
    assert_eq!(
        Error::Config("bad key".into()).exception_class(),
        ErrorClass::IllegalArgument
    );
    assert_eq!(
        Error::NotImplemented("This feature is not implemented: partitioned MERGE".into())
            .exception_class(),
        ErrorClass::Unsupported
    );
    assert_eq!(
        Error::Iceberg("CatalogCommitConflicts => metadata changed concurrently".into())
            .exception_class(),
        ErrorClass::Base
    );
}

/// Pins message preservation (the taxonomy must not lose the original engine diagnostic — a
/// user reading `str(exc)` needs the real cause). `Parse`/`Analysis`/`NotImplemented`/
/// `Iceberg` render the inner text verbatim, with no lossy or double prefix (the engine text
/// arrives already prefixed — "This feature is not implemented: …" from DataFusion, the
/// kind-first `"CatalogCommitConflicts => …"` from iceberg); the base `DataFusion` bucket
/// keeps its descriptive prefix.
#[test]
fn parse_and_analysis_display_preserve_message() {
    assert_eq!(
        Error::Parse("sql parser error: Expected an expression, found: FROM".into()).to_string(),
        "sql parser error: Expected an expression, found: FROM"
    );
    assert_eq!(
        Error::Analysis("No field named a.".into()).to_string(),
        "No field named a."
    );
    assert_eq!(
        Error::NotImplemented(
            "This feature is not implemented: MERGE INTO a partitioned table".into()
        )
        .to_string(),
        "This feature is not implemented: MERGE INTO a partitioned table",
        "the unsupported class must carry the engine text verbatim — no double prefix"
    );
    assert_eq!(
        Error::Iceberg("CatalogCommitConflicts => metadata changed concurrently".into())
            .to_string(),
        "CatalogCommitConflicts => metadata changed concurrently",
        "the iceberg bucket must lead with the kind, verbatim — no misattributing prefix"
    );
    assert!(
        Error::DataFusion("Cast error: cannot cast".into())
            .to_string()
            .contains("Cast error: cannot cast"),
        "the base bucket must also preserve the engine text"
    );
}
