//! Shared Spark/DataFusion identifier quoting and path-escape needles (r23 QI1 / CQ-006/007).
//!
//! Single source for the triple-maintained path-escape mirrors (Python `session.py`,
//! `repark-session`, `repark-sql`) and the Spark-dialect `quote_ident` used by MERGE SQL
//! generation. PostgreSQL keeps its own dialect helper in `repark-postgres` — never unify
//! across dialects.
//!
//! Security: over-quote is acceptable; under-quote is not.
//! Tags: O3-C4-SEC-001 (path-escape mirror); C2-SEC-003 / C1-SEC-001 (sql CTAS compose).

/// ===========================================================================================
/// Double-quote a SQL identifier for the Spark / DataFusion dialect.
///
/// Embedded `"` become `""` so a name containing quotes remains a single identifier token.
/// Always quotes — even plain bare names — matching the facade always-quote call-site class
/// (session / column / dataframe / ML).
/// ===========================================================================================
#[must_use]
pub fn quote_ident_spark(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Kind of path-escape reject for an identifier segment (shared needle table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathEscapeKind {
    /// Segment is `.`, `..`, or contains `..` (traversal).
    Traversal,
    /// Segment contains `/` or `\` (path separator).
    Separator,
}

/// ===========================================================================================
/// Classify a warehouse-path segment against the shared path-escape needles.
///
/// Returns [`None`] when the segment is safe. Callers map the kind into their error type
/// (DataFusion plan error, `String`, Python `PySparkValueError`). Empty segments are **not**
/// handled here — `repark-sql` additionally refuses empty before calling this (compose-time).
///
/// Needles (byte-identical across Python / repark-session / repark-sql): exact `.` or `..`,
/// or contains `..` → [`PathEscapeKind::Traversal`]; contains `/` or `\` →
/// [`PathEscapeKind::Separator`].
/// ===========================================================================================
#[must_use]
pub fn path_escape_kind(segment: &str) -> Option<PathEscapeKind> {
    if segment == "." || segment == ".." || segment.contains("..") {
        Some(PathEscapeKind::Traversal)
    } else if segment.contains('/') || segment.contains('\\') {
        Some(PathEscapeKind::Separator)
    } else {
        None
    }
}

/// Shared injection / path-escape probe strings (Spark/DF dialect + path-escape).
///
/// Harvested from r21 T5 PG probes (adapted to Spark always-quote expectations) and the
/// 2026-08-03 audit path-escape surface. Used by unit tests in this crate, `repark-sql`,
/// `repark-session`, and the Python `_idents` conformance suite — keep probe strings in lockstep
/// with `python/repark/src/repark/_idents.py::INJECTION_PROBES` /
/// `PATH_ESCAPE_PROBES`.
pub mod probes {
    /// Hostile identifier strings that must remain a single quoted token after Spark quoting.
    pub const SPARK_INJECTION_PROBES: &[&str] = &[
        r#""; DROP TABLE x; --"#,
        r#"id"; DROP TABLE x; --"#,
        r#"na"me"#,
        "order", // reserved word — must still quote
        "a b",
        "a.b",
        "",
    ];

    /// Path-escape segments that must be rejected (segment, expected kind tag).
    /// Kind tags: `"traversal"` | `"separator"`.
    pub const PATH_ESCAPE_PROBES: &[(&str, &str)] = &[
        (".", "traversal"),
        ("..", "traversal"),
        ("foo..bar", "traversal"),
        ("a/b", "separator"),
        (r"a\b", "separator"),
        ("../etc", "traversal"),
    ];

    /// Safe segments that must pass path-escape checks.
    pub const PATH_ESCAPE_SAFE: &[&str] = &["ok_table", "my_table", "t0", "Order"];
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ===========================================================================================
    /// Spark `quote_ident`: plain + embedded quote + injection payloads stay single tokens.
    /// ===========================================================================================
    #[test]
    fn quote_ident_spark_doubles_embedded_quotes() {
        assert_eq!(quote_ident_spark("plain"), "\"plain\"");
        assert_eq!(quote_ident_spark("na\"me"), "\"na\"\"me\"");
        assert_eq!(
            quote_ident_spark(r#"id"; DROP TABLE x; --"#),
            "\"id\"\"; DROP TABLE x; --\""
        );
    }

    /// ===========================================================================================
    /// Injection probes: quoted form starts/ends with `"` and round-trips the payload.
    /// ===========================================================================================
    #[test]
    fn spark_injection_probes_are_single_quoted_tokens() {
        for probe in probes::SPARK_INJECTION_PROBES {
            let quoted = quote_ident_spark(probe);
            // Independent oracle (octo C1-Q-002 / C1-SEC-001): undouble round-trip alone
            // false-passes under-escape (forgetting to double embedded `"`).
            let expected = format!("\"{}\"", probe.replace('"', "\"\""));
            assert_eq!(quoted, expected, "under-quote residual for {probe:?}");
            let inner = &quoted[1..quoted.len() - 1];
            assert!(
                !inner.replace("\"\"", "").contains('"'),
                "unpaired quote inside token for {probe:?}: {quoted}"
            );
        }
    }

    /// ===========================================================================================
    /// Path-escape needle table: every probe maps to the expected kind; safe names pass.
    /// ===========================================================================================
    #[test]
    fn path_escape_probes_match_shared_table() {
        for &(segment, expected) in probes::PATH_ESCAPE_PROBES {
            let kind = path_escape_kind(segment).unwrap_or_else(|| {
                panic!("expected reject for {segment:?}");
            });
            let tag = match kind {
                PathEscapeKind::Traversal => "traversal",
                PathEscapeKind::Separator => "separator",
            };
            assert_eq!(tag, expected, "segment {segment:?}");
        }
        for safe in probes::PATH_ESCAPE_SAFE {
            assert_eq!(path_escape_kind(safe), None, "safe segment {safe:?}");
        }
    }

    /// ===========================================================================================
    /// Cross-lang lockstep freeze (octo C1-Q-003): probe literals must match Python `_idents`.
    /// ===========================================================================================
    #[test]
    fn probe_tables_lockstep_frozen_with_python_ssot() {
        assert_eq!(
            probes::SPARK_INJECTION_PROBES,
            &[
                r#""; DROP TABLE x; --"#,
                r#"id"; DROP TABLE x; --"#,
                r#"na"me"#,
                "order",
                "a b",
                "a.b",
                "",
            ]
        );
        assert_eq!(
            probes::PATH_ESCAPE_PROBES,
            &[
                (".", "traversal"),
                ("..", "traversal"),
                ("foo..bar", "traversal"),
                ("a/b", "separator"),
                (r"a\b", "separator"),
                ("../etc", "traversal"),
            ]
        );
        assert_eq!(
            probes::PATH_ESCAPE_SAFE,
            &["ok_table", "my_table", "t0", "Order"]
        );
    }
}
