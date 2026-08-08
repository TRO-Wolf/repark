//! The wrong-door sniff (design §2 Q10, graft G3) — **error path only**.
//!
//! A user arriving from Spark writes `CREATE TABLE t USING iceberg TBLPROPERTIES (…)` into the
//! ANSI door and gets `sql parser error: Expected: AS, found: USING`. That error is technically
//! correct and practically useless: it does not say the syntax is Spark's, does not name the ANSI
//! equivalent, and does not mention that a Spark door exists.
//!
//! So: when — and ONLY when — a parse or plan has already FAILED, scan the statement for
//! Spark-isms and upgrade the error to name three things: the **token** that gave it away, the
//! **native equivalent**, and the **Spark door**. Three properties fall out of the error-path
//! placement, and all three are why it is placed there:
//! * **Zero happy-path cost** — no scan runs on a statement that worked.
//! * **No false positives that matter** — a successful statement is never second-guessed, so the
//!   sniff cannot break working SQL. (It also scans SCRUBBED text, so `SELECT 'USING'` and
//!   `-- USING` are invisible even on the error path.)
//! * **No grammar commitment** — the door does not have to *parse* Spark to *recognize* it.
//!
//! **Case rules.** The sniff matches tokens case-insensitively. Separately, and worth stating
//! because it is a real divergence users hit: this door plans on stock DataFusion, which follows
//! the ANSI rule that an UNQUOTED identifier folds to lower case while a `"Quoted"` one is taken
//! literally. Spark is case-insensitive for resolution and case-preserving for output. So
//! `SELECT "Id" FROM t` resolves in Spark and fails here unless the column really is `Id`. That
//! difference is a documented property of choosing the ANSI door, not a bug.

use datafusion::error::DataFusionError;

use crate::scan::{blank_out_quoted_and_comments, contains_word};

/// One recognizable Spark-ism: the token, and the steer.
#[derive(Clone, Copy)]
struct SparkIsm {
    /// The token as it appears in the message.
    token: &'static str,
    /// What to do instead in this door.
    equivalent: &'static str,
}

/// ===========================================================================================
/// If `sql` carries a Spark-ism, return an error that names the token, the native equivalent,
/// and the Spark door; otherwise return `original` untouched.
///
/// Call ONLY after a parse/plan failure.
/// ===========================================================================================
pub(crate) fn upgrade_error(sql: &str, original: DataFusionError) -> DataFusionError {
    let scrubbed = blank_out_quoted_and_comments(sql);
    let Some(sniff) = sniff(&scrubbed) else {
        return original;
    };
    DataFusionError::Plan(format!(
        "{original}\n\nThis looks like Spark SQL: `{}` is Spark-specific syntax, which this \
         (ANSI/Trino-flavoured) SQL door does not accept. {} To run Spark SQL unchanged, use the \
         Spark door instead — build the session with the Spark dialect + extension \
         (`SparkDialect` / `SparkExtension`), or route this one statement through it.",
        sniff.token, sniff.equivalent
    ))
}

/// The single-token Spark-isms, as a table: token → steer. Ordered most-specific first, so a
/// statement carrying several reports the one that best explains the failure.
const RULES: &[(&str, SparkIsm)] = &[
    (
        "TBLPROPERTIES",
        SparkIsm {
            token: "TBLPROPERTIES",
            equivalent: "Set table properties with the WITH clause: WITH (format = 'PARQUET', \
                 extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['merge-on-read'])).",
        },
    ),
    (
        "PARTITIONED BY",
        SparkIsm {
            token: "PARTITIONED BY",
            equivalent: "Declare partitioning in the WITH clause: WITH (partitioning = \
                 ARRAY['month(ts)', 'bucket(16, id)']).",
        },
    ),
    (
        "LATERAL VIEW",
        SparkIsm {
            token: "LATERAL VIEW",
            equivalent: "Use UNNEST in the FROM clause: FROM t CROSS JOIN UNNEST(t.arr) AS u(x).",
        },
    ),
    (
        "INSERT OVERWRITE",
        SparkIsm {
            token: "INSERT OVERWRITE",
            equivalent: "This door has no INSERT OVERWRITE. Replace the table with CREATE OR REPLACE \
                 TABLE … AS SELECT, or express the change as MERGE INTO, or DELETE then \
                 INSERT.",
        },
    ),
    (
        "SYSTEM_TIME",
        SparkIsm {
            token: "FOR SYSTEM_TIME AS OF",
            equivalent: "Time travel is spelled FOR TIMESTAMP AS OF <timestamp>.",
        },
    ),
    (
        "SYSTEM_VERSION",
        SparkIsm {
            token: "FOR SYSTEM_VERSION AS OF",
            equivalent: "Time travel is spelled FOR VERSION AS OF <snapshot-id | 'ref'>.",
        },
    ),
    (
        "USING",
        SparkIsm {
            token: "USING",
            equivalent: "Tables created through this door are Iceberg tables — there is no USING \
                 clause. Drop it; use WITH (…) for table properties.",
        },
    ),
    (
        "NAMESPACE",
        SparkIsm {
            token: "NAMESPACE",
            equivalent: "Namespaces are spelled SCHEMA: CREATE SCHEMA c.s WITH (location = '…').",
        },
    ),
    (
        "DATABASE",
        SparkIsm {
            token: "DATABASE",
            equivalent: "Namespaces are spelled SCHEMA: CREATE SCHEMA c.s WITH (location = '…').",
        },
    ),
    (
        "DBPROPERTIES",
        SparkIsm {
            token: "DBPROPERTIES",
            equivalent: "Schema properties go in the WITH clause: CREATE SCHEMA c.s WITH (location = '…').",
        },
    ),
];

/// Recognize a Spark-ism in already-scrubbed text: backticks first (they are both the cause of
/// the failure and the clearest signal of origin), then the single-token table, then the
/// composite forms.
fn sniff(scrubbed: &str) -> Option<SparkIsm> {
    // Backticks are checked FIRST and by character: they never tokenize as quotes in this door
    // (see `crate::scan`), so a backticked identifier is both the cause of the parse failure and
    // the clearest possible signal of origin.
    if scrubbed.contains('`') {
        return Some(SparkIsm {
            token: "`backtick-quoted identifier`",
            equivalent: "ANSI SQL quotes identifiers with double quotes — write \"my table\" instead of \
                 `my table`.",
        });
    }

    for (needle, ism) in RULES {
        if contains_word(scrubbed, needle) {
            return Some(*ism);
        }
    }
    sniff_composite_forms(scrubbed)
}

/// The Spark-isms that need more than a single token to recognize — a keyword PAIR whose ANSI
/// spelling differs only by a missing `FOR`, and two multi-word statement shapes. Split out of
/// [`sniff`] so the token table above stays a table.
fn sniff_composite_forms(scrubbed: &str) -> Option<SparkIsm> {
    // `VERSION AS OF` / `TIMESTAMP AS OF` WITHOUT the mandatory `FOR` — the single most common
    // Spark→ANSI time-travel mistake, and one the parser can only report as nonsense.
    if (contains_word(scrubbed, "VERSION AS OF") || contains_word(scrubbed, "TIMESTAMP AS OF"))
        && !contains_word(scrubbed, "FOR VERSION AS OF")
        && !contains_word(scrubbed, "FOR TIMESTAMP AS OF")
    {
        return Some(SparkIsm {
            token: "VERSION/TIMESTAMP AS OF (without FOR)",
            equivalent: "This door requires the FOR keyword: FROM t FOR VERSION AS OF 12345 (or FOR \
                 TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00').",
        });
    }

    // Top-level snapshot-ref DDL (`CREATE BRANCH b IN t`) — the Spark-only spelling.
    if contains_word(scrubbed, "BRANCH") || contains_word(scrubbed, "TAG") {
        return Some(SparkIsm {
            token: "BRANCH/TAG DDL",
            equivalent: "Branch and tag DDL is scoped to the table: ALTER TABLE t CREATE BRANCH b (the \
                 top-level CREATE BRANCH b IN t spelling is Spark-only).",
        });
    }

    // `CALL cat.system.<proc>(…)` — maintenance procedures.
    if contains_word(scrubbed, "CALL") && contains_word(scrubbed, "system") {
        return Some(SparkIsm {
            token: "CALL <catalog>.system.<procedure>",
            equivalent: "Maintenance runs as a callable operation on the session, not as a SQL statement \
                 in this door.",
        });
    }

    None
}

#[cfg(test)]
mod tests;
