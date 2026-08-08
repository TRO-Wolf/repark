use super::*;
use repark_core::Error;

/// The taxonomy end to end at the PyO3 boundary: each `Error` class maps to its PySpark
/// exception type, and every one subclasses `RuntimeError` (the near-drop-in invariant — code
/// that catches `RuntimeError` keeps catching engine errors). Also pins message preservation
/// (`str(exc)` carries the engine text) and the one-way parse⊂analysis relation (Group S: a
/// parse error IS-A `AnalysisException`, but an analysis error is NOT a parse error). This is
/// the fast, wheel-free pin for the whole partition.
#[test]
fn to_py_err_routes_to_typed_exceptions_subclassing_runtime_error() {
    Python::attach(|py| {
        let parse = to_py_err(Error::Parse("Expected an expression".into()));
        assert!(parse.is_instance_of::<ParseException>(py));
        assert!(parse.is_instance_of::<PySparkException>(py));
        assert!(parse.is_instance_of::<PyRuntimeError>(py));
        // Group S: a parse error IS-A AnalysisException (PySpark parity — `except
        // AnalysisException` catches it), while an analysis error is NOT a parse error (below).
        assert!(parse.is_instance_of::<AnalysisException>(py));
        assert!(parse.to_string().contains("Expected an expression"));

        let analysis = to_py_err(Error::Analysis("No field named zzz".into()));
        assert!(analysis.is_instance_of::<AnalysisException>(py));
        assert!(analysis.is_instance_of::<PySparkException>(py));
        assert!(analysis.is_instance_of::<PyRuntimeError>(py));
        assert!(!analysis.is_instance_of::<ParseException>(py));
        assert!(analysis.to_string().contains("No field named zzz"));

        // The base bucket: a plain execution/config error is the base type (still a
        // RuntimeError), never AnalysisException or ParseException.
        let base = to_py_err(Error::DataFusion("Cast error: boom".into()));
        assert!(base.is_instance_of::<PySparkException>(py));
        assert!(base.is_instance_of::<PyRuntimeError>(py));
        assert!(!base.is_instance_of::<AnalysisException>(py));
        assert!(!base.is_instance_of::<ParseException>(py));
        assert!(base.to_string().contains("Cast error: boom"));

        // U4 (CQ-002): a scope gate's NotImplemented maps to UnsupportedOperationException —
        // the PySpark class for a JVM UnsupportedOperationException — still a PySparkException
        // and a RuntimeError (near-drop-in), never Analysis/Parse; message verbatim.
        let unsupported = to_py_err(Error::NotImplemented(
            "This feature is not implemented: partitioned MERGE".into(),
        ));
        assert!(unsupported.is_instance_of::<UnsupportedOperationException>(py));
        assert!(unsupported.is_instance_of::<PySparkException>(py));
        assert!(unsupported.is_instance_of::<PyRuntimeError>(py));
        assert!(!unsupported.is_instance_of::<AnalysisException>(py));
        assert!(!unsupported.is_instance_of::<ParseException>(py));
        assert!(
            unsupported
                .to_string()
                .contains("This feature is not implemented: partitioned MERGE")
        );

        // U4 (CQ-015): an iceberg-residual error (commit conflict class) is the base type with
        // the kind name visible in str(exc) — not Unsupported, not Analysis.
        let iceberg = to_py_err(Error::Iceberg(
            "CatalogCommitConflicts => metadata changed concurrently".into(),
        ));
        assert!(iceberg.is_instance_of::<PySparkException>(py));
        assert!(iceberg.is_instance_of::<PyRuntimeError>(py));
        assert!(!iceberg.is_instance_of::<UnsupportedOperationException>(py));
        assert!(!iceberg.is_instance_of::<AnalysisException>(py));
        assert!(iceberg.to_string().contains("CatalogCommitConflicts"));

        // Group X: an invalid `.config(...)` value is `IllegalArgumentException` — the class
        // PySpark raises for a JVM IllegalArgumentException (live pyspark 4.0.0 oracle). It
        // stays a PySparkException / RuntimeError (near-drop-in), and is NOT analysis, parse
        // or unsupported. MUTATION: route `Error::Config` back to `ErrorClass::Base` → RED.
        let config = to_py_err(Error::Config(
            "spark.sql.catalog.foo.type has an unrecognized value 'nosuchtype'".into(),
        ));
        assert!(config.is_instance_of::<IllegalArgumentException>(py));
        assert!(config.is_instance_of::<PySparkException>(py));
        assert!(config.is_instance_of::<PyRuntimeError>(py));
        assert!(!config.is_instance_of::<AnalysisException>(py));
        assert!(!config.is_instance_of::<ParseException>(py));
        assert!(!config.is_instance_of::<UnsupportedOperationException>(py));
        assert!(config.to_string().contains("nosuchtype"));

        // ...and the relation is one-way: the OTHER classes must not become
        // IllegalArgumentException (the risk of a too-broad reroute).
        assert!(!base.is_instance_of::<IllegalArgumentException>(py));
        assert!(!analysis.is_instance_of::<IllegalArgumentException>(py));
        assert!(!unsupported.is_instance_of::<IllegalArgumentException>(py));
    });
}

/// ===========================================================================================
/// **EC-1 — the type-identity guard for the re-homed error surface (design §3).**
///
/// The re-home's one silent trap: every `repark_core::Error` / `repark_core::ErrorClass` line in
/// this crate is *textually identical* to the port pin, yet each one now resolves through a
/// different crate — `repark-core` re-exports the taxonomy from `repark-common`. Reviewers must
/// read those unchanged lines as CHANGED lines, and a human rule that depends on remembering
/// that is not a rule. So the fact is mechanized: these coercions only compile while the two
/// paths name the SAME type.
///
/// MUTATION: give `repark-core` its own `Error` enum (a plausible future re-split) and this
/// crate stops compiling here — loudly, at the seam — instead of silently binding
/// `to_py_err`'s exhaustive fold to a type the doors no longer raise.
/// ===========================================================================================
const _: fn(repark_common::Error) -> repark_core::Error = |error| error;
const _: fn(repark_core::Error) -> repark_common::Error = |error| error;
const _: fn(repark_common::ErrorClass) -> repark_core::ErrorClass = |class| class;
const _: fn(repark_core::ErrorClass) -> repark_common::ErrorClass = |class| class;

/// Companion runtime pin for the compile-time coercions above (a `const _` carries no test name,
/// so nothing in the census would record that the guard exists). Builds the value through
/// `repark_common` and folds it through the binding's `repark_core`-typed boundary: one value,
/// one taxonomy, whichever path names it.
#[test]
fn repark_core_error_is_the_repark_common_error_type() {
    Python::attach(|py| {
        let via_common: repark_common::Error =
            repark_common::Error::NotImplemented("re-home identity probe".into());
        // Assignable in BOTH directions without conversion ⇒ one type, not two convertible ones.
        let via_core: repark_core::Error = via_common;
        assert_eq!(
            via_core.exception_class(),
            repark_common::ErrorClass::Unsupported,
            "the classifier hop is the same fold through either path"
        );
        let raised = to_py_err(via_core);
        assert!(raised.is_instance_of::<UnsupportedOperationException>(py));
        assert!(raised.to_string().contains("re-home identity probe"));
    });
}
