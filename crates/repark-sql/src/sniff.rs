//! The wrong-door sniff runs on the error path only.

use datafusion::error::DataFusionError;

use crate::scan::{blank_out_quoted_and_comments, contains_word, leading_keyword};

/// Where a rule is allowed to fire.
#[derive(Clone, Copy)]
enum Scope {
    /// The token has no ANSI-legal reading — match it wherever it appears.
    Anywhere,
    /// The token is also ordinary ANSI SQL, so match it only when the leading keyword matches.
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

/// Add a Spark steer to a failed statement when recognized, preserving the original error first.
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

/// The scope of a single-token rule, keyed by its needle.
fn scope_for(needle: &str) -> Scope {
    match needle {
        "USING" => Scope::Leading(&["CREATE"]),
        "NAMESPACE" | "DATABASE" | "DBPROPERTIES" => Scope::Leading(DDL_VERBS),
        _ => Scope::Anywhere,
    }
}

/// The single-token Spark-isms, as a table: token → steer.
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

/// Recognize a Spark-ism in already-scrubbed text.
fn sniff(scrubbed: &str) -> Option<SparkIsm> {
    // Backticks are checked first and by character because they are not ANSI quotes in this door.
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

/// Recognize Spark-isms that need more than one token, such as a keyword pair without its prefix.
fn sniff_composite_forms(scrubbed: &str, leading: Option<&str>) -> Option<SparkIsm> {
    // `VERSION AS OF` / `TIMESTAMP AS OF` without the mandatory `FOR` is a common Spark spelling.
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

    // Top-level snapshot-ref DDL (`CREATE BRANCH b IN t`) is Spark-only.
    if Scope::Leading(DDL_VERBS).admits(leading)
        && (contains_word(scrubbed, "BRANCH") || contains_word(scrubbed, "TAG"))
    {
        return Some(SparkIsm {
            token: "BRANCH/TAG DDL",
            equivalent: "Branch and tag DDL is scoped to the table: ALTER TABLE t CREATE BRANCH b (the \
                 top-level CREATE BRANCH b IN t spelling is Spark-only).",
        });
    }

    // `CALL cat.system.<proc>(…)` is a maintenance procedure.
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
