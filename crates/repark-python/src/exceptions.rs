//! The five native exception types, in their own module file so the panic-ban's
//! `disallowed_methods` lint stays LIVE for the rest of the crate: `pyo3::create_exception!`
//! expands to `Result::expect` (five macro sites, compile-time registration, unreachable from
//! user input), and a per-call-site `#[expect]` cannot reach inside the macro expansion
//! (provocations P-4/P-5, p3c ledger). Re-exported at the crate root — `crate::…Exception`
//! paths are unchanged. File-backed per the `check_lib_rs` ratchet ("if the exception
//! taxonomy moves to its own module").
#![expect(
    clippy::disallowed_methods,
    reason = "pyo3::create_exception! expands to Result::expect at the five macro \
              sites below; the expansion is compile-time-constant registration, not a \
              reachable panic path. Scoped here so the spawn/panic bans stay live for \
              the whole crate (p3c ledger P-4/P-5)."
)]
use pyo3::exceptions::PyRuntimeError;

pyo3::create_exception!(
    repark._native,
    PySparkException,
    PyRuntimeError,
    "Base class for engine exceptions raised by repark. Subclasses RuntimeError so existing \
     `except RuntimeError` code keeps working after migrating from PySpark."
);
pyo3::create_exception!(
    repark._native,
    AnalysisException,
    PySparkException,
    "A query analysis/planning failure: an unresolved table or column, a type error, an invalid \
     plan. The PySpark name; subclasses PySparkException (hence RuntimeError)."
);
pyo3::create_exception!(
    repark._native,
    ParseException,
    AnalysisException,
    "A SQL or expression syntax error. The PySpark name; subclasses AnalysisException (PySpark \
     parity — `pyspark.errors` defines `ParseException(AnalysisException)`, so `except \
     AnalysisException` catches parse errors), hence PySparkException and RuntimeError."
);
pyo3::create_exception!(
    repark._native,
    UnsupportedOperationException,
    PySparkException,
    "An operation the engine deterministically does not support: the documented scope gates \
     (an unrecognised write.merge.mode, merge-on-read MERGE on a non-V2 table, a non-Parquet \
     write format, ...) and unsupported iceberg features. The PySpark name \
     (pyspark.errors.UnsupportedOperationException — what PySpark raises for a JVM \
     UnsupportedOperationException); subclasses PySparkException (hence RuntimeError)."
);
pyo3::create_exception!(
    repark._native,
    IllegalArgumentException,
    PySparkException,
    "An illegal or inappropriate argument reached the engine — today, an invalid `.config(...)` \
     key/value the session cannot map to a valid engine/catalog configuration. The PySpark name \
     (pyspark.errors.IllegalArgumentException — what PySpark raises for a JVM \
     IllegalArgumentException; live pyspark 4.0.0 raises it for an invalid SQLConf value). \
     Subclasses PySparkException (hence RuntimeError). NOTE: PySpark's \
     `NumberFormatException(IllegalArgumentException)` leaf is deliberately NOT defined here — \
     repark has no reachable raise for it (Group X)."
);
