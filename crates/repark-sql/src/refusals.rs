//! The completed refuse set (design §2 Q7 / Q9, plus `TRUNCATE`).

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
/// The pre-parse recognizer for `ALTER TABLE <name> EXECUTE <procedure>`, which stock sqlparser cannot parse.
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
    let verb = verb_slot_after_table_name(&scrubbed, &words)?;
    if !words.get(verb)?.2.eq_ignore_ascii_case("EXECUTE") {
        return None;
    }
    let procedure = words.get(verb + 1).map_or("<procedure>", |(_, _, w)| *w);
    Some(alter_table_execute(procedure))
}

/// The index in `words` of the first word AFTER the table name of an `ALTER TABLE <name> …`.
fn verb_slot_after_table_name(scrubbed: &str, words: &[(usize, usize, &str)]) -> Option<usize> {
    let bytes = scrubbed.as_bytes();
    let skip_ws = |bytes: &[u8], mut at: usize| {
        while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
            at += 1;
        }
        at
    };
    let mut pos = words.get(1)?.1;
    loop {
        pos = skip_ws(bytes, pos);
        if bytes.get(pos) == Some(&b'"') {
            let close = bytes.iter().skip(pos + 1).position(|byte| *byte == b'"')?;
            pos = pos + 1 + close + 1;
        } else {
            let start = pos;
            while bytes
                .get(pos)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
            {
                pos += 1;
            }
            if pos == start {
                return None;
            }
        }
        let after = skip_ws(bytes, pos);
        if bytes.get(after) == Some(&b'.') {
            pos = after + 1;
        } else {
            pos = after;
            break;
        }
    }
    words.iter().position(|(start, _, _)| *start == pos)
}

#[cfg(test)]
mod tests;
