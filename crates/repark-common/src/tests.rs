use super::*;

/// Pins exhaustive variant-to-PySpark exception routing and prevents scope gates from reaching the base class.
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
    // An invalid `.config(...)` value maps to Spark's `IllegalArgumentException`, not the base bucket.
    // MUTATION: route `Config` back to `ErrorClass::Base` → this REDs.
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

/// Pins verbatim preservation for specialized diagnostics and the prefixed DataFusion bucket.
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
