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
//! * **Bounded false positives** — a successful statement is never second-guessed, so the sniff
//!   cannot break working SQL. (It also scans SCRUBBED text, so `SELECT 'USING'` and `-- USING`
//!   are invisible even on the error path.)
//!
//! "Never breaks working SQL" is not the same as "never misleads". Several of the tokens below
//! are ALSO ordinary ANSI SQL: `USING` is a join clause, and `tag` / `branch` / `namespace` /
//! `database` are perfectly good column names. A `SELECT tag FROM t` that fails because `t` does
//! not exist must not be answered with "this looks like Spark SQL". So every rule whose token has
//! an ANSI-legal reading carries a **leading-keyword scope** ([`Scope::Leading`]): it fires only
//! when the statement's first keyword is one the Spark-ism could actually belong to. Tokens with
//! no ANSI reading (`TBLPROPERTIES`, `LATERAL VIEW`, backticks, …) stay unscoped.
//! * **No grammar commitment** — the door does not have to *parse* Spark to *recognize* it.
//!
//! **Case rules.** The sniff matches tokens case-insensitively. Separately, and worth stating
//! because it is a real divergence users hit: this door plans on stock DataFusion, which follows
//! the ANSI rule that an UNQUOTED identifier folds to lower case while a `"Quoted"` one is taken
//! literally. Spark is case-insensitive for resolution and case-preserving for output. So
//! `SELECT "Id" FROM t` resolves in Spark and fails here unless the column really is `Id`. That
//! difference is a documented property of choosing the ANSI door, not a bug.

use datafusion::error::DataFusionError;

use crate::scan::{blank_out_quoted_and_comments, contains_word, leading_keyword};

/// Where a rule is allowed to fire.
#[derive(Clone, Copy)]
enum Scope {
    /// The token has no ANSI-legal reading — match it wherever it appears.
    Anywhere,
    /// The token is ALSO ordinary ANSI SQL, so match it only when the statement's leading
    /// keyword is one of these (uppercase).
    Leading(&'static [&'static str]),
}

impl Scope {
    /// True when this scope admits a statement whose leading keyword is `leading`.
    fn admits(self, leading: Option<&str>) -> bool {
        match self {
            Self::Anywhere => true,
            Self::Leading(keywords) => leading.is_some_and(|word| keywords.contains(&word)),
        }
    }
}

/// The DDL verbs a catalog/namespace Spark-ism can lead with.
const DDL_VERBS: &[&str] = &["CREATE", "DROP", "ALTER", "SHOW", "USE", "DESCRIBE", "DESC"];

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

/// The scope of a single-token rule, keyed by its needle. Kept beside the table rather than in
/// it so the table stays a table; every needle NOT listed here is [`Scope::Anywhere`].
///
/// * `USING` — ANSI join clause (`JOIN b USING (id)`); Spark-ism only in `CREATE TABLE … USING`.
/// * `NAMESPACE` / `DATABASE` / `DBPROPERTIES` — ordinary column and alias names; Spark-isms only
///   as catalog DDL.
fn scope_for(needle: &str) -> Scope {
    match needle {
        "USING" => Scope::Leading(&["CREATE"]),
        "NAMESPACE" | "DATABASE" | "DBPROPERTIES" => Scope::Leading(DDL_VERBS),
        _ => Scope::Anywhere,
    }
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

    let leading = leading_keyword(scrubbed);
    for (needle, ism) in RULES {
        if contains_word(scrubbed, needle) && scope_for(needle).admits(leading.as_deref()) {
            return Some(*ism);
        }
    }
    sniff_composite_forms(scrubbed, leading.as_deref())
}

/// The Spark-isms that need more than a single token to recognize — a keyword PAIR whose ANSI
/// spelling differs only by a missing `FOR`, and two multi-word statement shapes. Split out of
/// [`sniff`] so the token table above stays a table.
fn sniff_composite_forms(scrubbed: &str, leading: Option<&str>) -> Option<SparkIsm> {
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

    // Top-level snapshot-ref DDL (`CREATE BRANCH b IN t`) — the Spark-only spelling. Scoped to
    // the DDL verbs: `branch` and `tag` are ordinary column names in a query.
    if Scope::Leading(DDL_VERBS).admits(leading)
        && (contains_word(scrubbed, "BRANCH") || contains_word(scrubbed, "TAG"))
    {
        return Some(SparkIsm {
            token: "BRANCH/TAG DDL",
            equivalent: "Branch and tag DDL is scoped to the table: ALTER TABLE t CREATE BRANCH b (the \
                 top-level CREATE BRANCH b IN t spelling is Spark-only).",
        });
    }

    // `CALL cat.system.<proc>(…)` — maintenance procedures. The statement must LEAD with CALL:
    // `system` is a plausible identifier, and `call` a plausible column name.
    if leading == Some("CALL") && contains_word(scrubbed, "system") {
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
