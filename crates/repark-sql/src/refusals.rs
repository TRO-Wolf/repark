//! The completed refuse set (design §2 Q7 / Q9, plus `TRUNCATE`).
//!
//! Four statement shapes this door deliberately does not implement. Each one is a **loud refusal
//! with a steer**, never a silent success and never a raw parser error, because each is a shape a
//! migrating user really writes and each has a real answer in this engine today.
//!
//! | shape | why absent | steer |
//! |---|---|---|
//! | `INSERT OVERWRITE` | Q9 — Trino-faithful omission; dbt-trino ships no `insert_overwrite` strategy (graft G10), so the ecosystem this door targets does not depend on it | `MERGE INTO`, `DELETE` + `INSERT`, or `CREATE OR REPLACE TABLE … AS SELECT` |
//! | `CALL c.system.<proc>(…)` | Q7 — maintenance is callable-ops only; the ADR pin stands | the session's callable maintenance operations |
//! | `ALTER TABLE … EXECUTE <proc>` | Q7 — same ruling; this is the **pre-designated future spelling**, held so it cannot be reused | the same callable operations |
//! | `TRUNCATE TABLE` | no Iceberg truncate primitive; a "truncate" is a delete-all commit or a replace, and which one you want is a decision the engine should not make silently | `DELETE FROM t` or `CREATE OR REPLACE TABLE t AS SELECT … WHERE false` |
//!
//! Three of the four parse on the stock Generic dialect and are refused from the router's
//! statement match. `ALTER TABLE … EXECUTE` does NOT parse (the R1 spike recorded the exact
//! error), so it gets a pre-parse recognizer — the smallest one in the crate.

use datafusion::error::DataFusionError;

use crate::scan::{blank_out_quoted_and_comments, leading_keyword, word_spans};

/// ===========================================================================================
/// Q9: `INSERT OVERWRITE` is not a surface of this door.
/// ===========================================================================================
pub(crate) fn insert_overwrite(target: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "INSERT OVERWRITE is not supported by this (ANSI/Trino-flavoured) door — it is a Spark \
         spelling with no Trino equivalent, and the ecosystem this door targets does not use it \
         (dbt-trino ships no `insert_overwrite` materialization strategy). Express the change \
         instead as: MERGE INTO {target} USING … (an upsert), or DELETE FROM {target} WHERE … \
         followed by INSERT INTO {target} …, or CREATE OR REPLACE TABLE {target} AS SELECT … \
         (a full replace). The overwrite machinery itself remains reachable through the Spark \
         door and as a callable operation. See docs/design/sql-doors.md §2 Q9."
    ))
}

/// ===========================================================================================
/// Q7: statement-shaped maintenance (`CALL`) is not a surface of this door.
/// ===========================================================================================
pub(crate) fn maintenance_call(procedure: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "CALL `{procedure}` is not supported: maintenance in this engine runs as a CALLABLE \
         OPERATION on the session, not as a SQL statement (docs/design/sql-doors.md §2 Q7, the \
         ADR-0002 pin). Run the equivalent operation from the session API. TRIGGER for a \
         statement-shaped surface: dbt-repark post-hooks demonstrating a need — a superseding \
         ADR note first, then the surface, spelled ALTER TABLE … EXECUTE."
    ))
}

/// ===========================================================================================
/// Q7: `ALTER TABLE … EXECUTE …` — the PRE-DESIGNATED future spelling, refused today.
/// ===========================================================================================
pub(crate) fn alter_table_execute(procedure: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "ALTER TABLE … EXECUTE {procedure} is not supported yet. This IS the spelling reserved \
         for statement-shaped maintenance in this door (docs/design/sql-doors.md §2 Q7) — it is \
         held so it cannot be reused for anything else, and refusing beats a raw parse error \
         that would suggest the syntax is wrong rather than unimplemented. Maintenance runs \
         today as a callable operation on the session. TRIGGER: dbt-repark post-hooks \
         demonstrating a statement-shaped need."
    ))
}

/// ===========================================================================================
/// `TRUNCATE TABLE` — no Iceberg primitive, and the two plausible meanings differ.
/// ===========================================================================================
pub(crate) fn truncate(target: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "TRUNCATE TABLE {target} is not supported: Iceberg has no truncate primitive, and the \
         two things it could mean commit differently — DELETE FROM {target} removes every row \
         and keeps the table's history, while CREATE OR REPLACE TABLE {target} AS SELECT … \
         WHERE false replaces the table. Write the one you mean."
    ))
}

/// ===========================================================================================
/// The pre-parse recognizer for `ALTER TABLE <name> EXECUTE <procedure>`, which stock sqlparser
/// cannot reach. `None` when the statement is not that shape.
/// ===========================================================================================
pub(crate) fn recognize_alter_table_execute(sql: &str) -> Option<DataFusionError> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    if leading_keyword(&scrubbed).as_deref() != Some("ALTER") {
        return None;
    }
    let words = word_spans(&scrubbed);
    if !words.get(1)?.2.eq_ignore_ascii_case("TABLE") {
        return None;
    }
    // Skip the (possibly dotted) table name: the first word after it is the operation.
    let index = words
        .iter()
        .position(|(_, _, word)| word.eq_ignore_ascii_case("EXECUTE"))?;
    if index < 3 {
        return None;
    }
    let procedure = words.get(index + 1).map_or("<procedure>", |(_, _, w)| *w);
    Some(alter_table_execute(procedure))
}

#[cfg(test)]
mod tests;
