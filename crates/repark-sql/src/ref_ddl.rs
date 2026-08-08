//! Branch / tag DDL — `ALTER TABLE t CREATE|DROP BRANCH|TAG …` (design §2 Q6, graft G6).
//!
//! **Precedent-copying, not invention.** The grammar below is exactly the ALTER-scoped subset the
//! Spark door ships (verified at the port pin `fc3f48102`), and it executes through the SAME
//! tier-1 [`repark_iceberg::write`] `ManageSnapshots` seams that door calls — shared code below
//! both doors, never a door→door import. What this door does NOT take is the Spark-only top-level
//! spelling (`CREATE BRANCH b IN t`): that stays a wrong-door sniff steer.
//!
//! Grammar:
//!
//! ```text
//! ALTER TABLE c.s.t CREATE [OR REPLACE] BRANCH|TAG <name>
//!     [AS OF VERSION <snapshot-id>]
//!     [RETAIN <n> DAYS|HOURS|MINUTES]
//!     [WITH SNAPSHOT RETENTION <n> SNAPSHOTS | <n> DAYS|HOURS|MINUTES]
//! ALTER TABLE c.s.t DROP BRANCH|TAG [IF EXISTS] <name>
//! ```
//!
//! Stock sqlparser cannot reach any of it (`Expected: ADD, RENAME, … found: CREATE`), so this is
//! a pre-parse recognizer, in the class the design pre-authorised for exactly this production.
//! It runs on TOKENS, not on raw text, so a `BRANCH` inside a string literal is invisible.
//!
//! Writing to a ref stays refused ([`crate::guards::refuse_write_to_branch`]): the fork's append
//! always sets `main`, so re-pinning a ref with this DDL is the write path for refs today.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use repark_core::EngineContext;
use repark_iceberg::write::{
    SnapshotRefKind, SnapshotRefRetention, create_or_replace_snapshot_ref,
    create_snapshot_ref_with_retention, drop_snapshot_ref,
};

use crate::create_table::CreateTarget;
use crate::schema_ddl::iceberg_err;

/// A recognized snapshot-ref statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefDdl {
    /// The dotted target name, as written.
    table_parts: Vec<String>,
    op: RefOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RefOp {
    Create {
        kind: SnapshotRefKind,
        name: String,
        /// Pin at this snapshot id; `None` = the table's current snapshot.
        as_of_version: Option<i64>,
        /// `CREATE OR REPLACE` — create-if-absent, re-pin-if-present.
        or_replace: bool,
        retention: SnapshotRefRetention,
    },
    Drop {
        kind: SnapshotRefKind,
        name: String,
        if_exists: bool,
    },
}

/// Significant tokens, reduced to the shapes this grammar cares about.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Sig {
    Word(String),
    /// A `"quoted"` identifier — already unquoted.
    Quoted(String),
    Period,
    Number(String),
    Other,
}

impl Sig {
    /// The identifier this token names, if it is one.
    fn ident(&self) -> Option<&str> {
        match self {
            Self::Word(value) | Self::Quoted(value) => Some(value),
            _ => None,
        }
    }

    /// True when this is a BARE word equal (case-insensitively) to `expected`. A quoted
    /// identifier never matches a keyword — `"branch"` is a name, not the BRANCH keyword.
    fn keyword(&self, expected: &str) -> bool {
        matches!(self, Self::Word(value) if value.eq_ignore_ascii_case(expected))
    }
}

/// ===========================================================================================
/// Recognize `ALTER TABLE … CREATE|DROP BRANCH|TAG …`.
///
/// `None` = not this statement shape (the router carries on). `Some(Err(…))` = it IS this shape
/// but malformed, which must be a targeted error rather than an opaque parse failure.
/// ===========================================================================================
pub(crate) fn try_parse_ref_ddl(sql: &str) -> Option<Result<RefDdl>> {
    let tokens = tokenize_significant(sql)?;
    if !tokens.first()?.keyword("ALTER") || !tokens.get(1)?.keyword("TABLE") {
        return None;
    }
    let (table_parts, after_name) = dotted_name(&tokens, 2)?;

    if tokens.get(after_name)?.keyword("CREATE") {
        let or_replace = tokens.get(after_name + 1).is_some_and(|t| t.keyword("OR"))
            && tokens
                .get(after_name + 2)
                .is_some_and(|t| t.keyword("REPLACE"));
        let kind_index = if or_replace {
            after_name + 3
        } else {
            after_name + 1
        };
        let kind = ref_kind(tokens.get(kind_index)?)?;
        return Some(parse_create(
            &tokens,
            table_parts,
            kind,
            or_replace,
            kind_index + 1,
        ));
    }
    if tokens.get(after_name)?.keyword("DROP") {
        let kind = ref_kind(tokens.get(after_name + 1)?)?;
        return Some(parse_drop(&tokens, table_parts, kind, after_name + 2));
    }
    None
}

/// `BRANCH` / `TAG` as a bare keyword.
fn ref_kind(token: &Sig) -> Option<SnapshotRefKind> {
    if token.keyword("BRANCH") {
        Some(SnapshotRefKind::Branch)
    } else if token.keyword("TAG") {
        Some(SnapshotRefKind::Tag)
    } else {
        None
    }
}

fn parse_create(
    tokens: &[Sig],
    table_parts: Vec<String>,
    kind: SnapshotRefKind,
    or_replace: bool,
    index: usize,
) -> Result<RefDdl> {
    let form = if or_replace {
        "ALTER TABLE … CREATE OR REPLACE BRANCH|TAG"
    } else {
        "ALTER TABLE … CREATE BRANCH|TAG"
    };
    let name = ref_name(tokens, index, form)?;
    let (as_of_version, index) = parse_as_of_version(tokens, index + 1, form)?;
    let (retention, index) = parse_retention(tokens, index, kind, form)?;
    reject_trailing(tokens, index, form)?;
    Ok(RefDdl {
        table_parts,
        op: RefOp::Create {
            kind,
            name,
            as_of_version,
            or_replace,
            retention,
        },
    })
}

fn parse_drop(
    tokens: &[Sig],
    table_parts: Vec<String>,
    kind: SnapshotRefKind,
    index: usize,
) -> Result<RefDdl> {
    let form = "ALTER TABLE … DROP BRANCH|TAG";
    let if_exists = tokens.get(index).is_some_and(|t| t.keyword("IF"))
        && tokens.get(index + 1).is_some_and(|t| t.keyword("EXISTS"));
    let name_index = if if_exists { index + 2 } else { index };
    let name = ref_name(tokens, name_index, form)?;
    reject_trailing(tokens, name_index + 1, form)?;
    Ok(RefDdl {
        table_parts,
        op: RefOp::Drop {
            kind,
            name,
            if_exists,
        },
    })
}

/// The ref name, which must be an identifier and must not contain path-escape characters (the
/// name becomes part of a metadata ref key, so the same identifier hygiene the rest of the door
/// applies to namespace/table segments applies here).
fn ref_name(tokens: &[Sig], index: usize, form: &str) -> Result<String> {
    let name = tokens
        .get(index)
        .and_then(Sig::ident)
        .ok_or_else(|| DataFusionError::Plan(format!("{form}: a ref name is required")))?;
    if name.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form}: the ref name must not be empty"
        )));
    }
    crate::schema_ddl::reject_path_escape_ident(name, "snapshot ref")?;
    Ok(name.to_string())
}

/// `AS OF VERSION <snapshot-id>` (optional). Returns the id and the next index.
fn parse_as_of_version(tokens: &[Sig], index: usize, form: &str) -> Result<(Option<i64>, usize)> {
    if !tokens.get(index).is_some_and(|t| t.keyword("AS")) {
        return Ok((None, index));
    }
    if !tokens.get(index + 1).is_some_and(|t| t.keyword("OF"))
        || !tokens.get(index + 2).is_some_and(|t| t.keyword("VERSION"))
    {
        return Err(DataFusionError::Plan(format!(
            "{form}: the pin clause is spelled AS OF VERSION <snapshot-id>"
        )));
    }
    let (raw, next) = signed_number(tokens, index + 3).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{form}: AS OF VERSION requires a snapshot id (an integer)"
        ))
    })?;
    let snapshot_id = raw.parse::<i64>().map_err(|_| {
        DataFusionError::Plan(format!("{form}: `{raw}` is not a valid snapshot id"))
    })?;
    Ok((Some(snapshot_id), next))
}

/// `RETAIN n <unit>` then `WITH SNAPSHOT RETENTION n SNAPSHOTS|<unit>` (both optional).
fn parse_retention(
    tokens: &[Sig],
    mut index: usize,
    kind: SnapshotRefKind,
    form: &str,
) -> Result<(SnapshotRefRetention, usize)> {
    let mut retention = SnapshotRefRetention::default();

    if tokens.get(index).is_some_and(|t| t.keyword("RETAIN")) {
        let (amount, unit, next) = amount_and_unit(tokens, index + 1, form)?;
        retention.max_ref_age_ms = Some(to_ms(amount, &unit, form)?);
        index = next;
    }

    if tokens.get(index).is_some_and(|t| t.keyword("WITH")) {
        if !tokens.get(index + 1).is_some_and(|t| t.keyword("SNAPSHOT"))
            || !tokens
                .get(index + 2)
                .is_some_and(|t| t.keyword("RETENTION"))
        {
            return Err(DataFusionError::Plan(format!(
                "{form}: the clause is spelled WITH SNAPSHOT RETENTION <n> SNAPSHOTS | <n> \
                 DAYS|HOURS|MINUTES"
            )));
        }
        if kind == SnapshotRefKind::Tag {
            return Err(DataFusionError::Plan(format!(
                "{form}: WITH SNAPSHOT RETENTION applies to BRANCHES only — a tag pins one \
                 snapshot, so per-branch snapshot retention has no meaning for it. Use RETAIN \
                 <n> <unit> to bound the tag's own lifetime"
            )));
        }
        let (amount, unit, next) = amount_and_unit(tokens, index + 3, form)?;
        if unit.eq_ignore_ascii_case("SNAPSHOTS") {
            let count = i32::try_from(amount).map_err(|_| {
                DataFusionError::Plan(format!("{form}: {amount} snapshots is out of range"))
            })?;
            retention.min_snapshots_to_keep = Some(count);
        } else {
            retention.max_snapshot_age_ms = Some(to_ms(amount, &unit, form)?);
        }
        index = next;
    }
    Ok((retention, index))
}

/// `<n> <unit-word>` — returns the amount, the unit word, and the next index.
fn amount_and_unit(tokens: &[Sig], index: usize, form: &str) -> Result<(i64, String, usize)> {
    let (raw, next) = signed_number(tokens, index)
        .ok_or_else(|| DataFusionError::Plan(format!("{form}: a numeric amount is required")))?;
    let amount = raw
        .parse::<i64>()
        .map_err(|_| DataFusionError::Plan(format!("{form}: `{raw}` is not a whole number")))?;
    if amount <= 0 {
        return Err(DataFusionError::Plan(format!(
            "{form}: the amount must be positive (got {amount})"
        )));
    }
    let unit = tokens
        .get(next)
        .and_then(Sig::ident)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{form}: a unit is required (DAYS, HOURS, MINUTES, or SNAPSHOTS)"
            ))
        })?
        .to_string();
    Ok((amount, unit, next + 1))
}

fn to_ms(amount: i64, unit: &str, form: &str) -> Result<i64> {
    let per = match unit.to_ascii_uppercase().as_str() {
        "DAY" | "DAYS" => 86_400_000,
        "HOUR" | "HOURS" => 3_600_000,
        "MINUTE" | "MINUTES" => 60_000,
        other => {
            return Err(DataFusionError::Plan(format!(
                "{form}: unknown time unit `{other}` (use DAYS, HOURS, or MINUTES)"
            )));
        }
    };
    amount.checked_mul(per).ok_or_else(|| {
        DataFusionError::Plan(format!("{form}: {amount} {unit} overflows a duration"))
    })
}

/// A number with an optional leading `-` (Iceberg snapshot ids are signed).
fn signed_number(tokens: &[Sig], index: usize) -> Option<(String, usize)> {
    match tokens.get(index)? {
        Sig::Number(text) => Some((text.clone(), index + 1)),
        Sig::Other if matches!(tokens.get(index + 1), Some(Sig::Number(_))) => {
            // `Other` covers the `-` token; only accept it when a number follows.
            let Some(Sig::Number(text)) = tokens.get(index + 1) else {
                return None;
            };
            Some((format!("-{text}"), index + 2))
        }
        _ => None,
    }
}

/// Anything left over is a clause this door did not understand — refuse rather than ignore it.
fn reject_trailing(tokens: &[Sig], index: usize, form: &str) -> Result<()> {
    let rest: Vec<&str> = tokens[index.min(tokens.len())..]
        .iter()
        .filter_map(Sig::ident)
        .collect();
    if rest.is_empty() {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "{form}: unsupported trailing clause starting at `{}` — the supported clauses are \
         AS OF VERSION <id>, RETAIN <n> <unit>, and WITH SNAPSHOT RETENTION <n> <unit>",
        rest[0]
    )))
}

/// A dotted name starting at `index`; returns the parts and the index after it.
fn dotted_name(tokens: &[Sig], index: usize) -> Option<(Vec<String>, usize)> {
    let mut parts = vec![tokens.get(index)?.ident()?.to_string()];
    let mut cursor = index + 1;
    while matches!(tokens.get(cursor), Some(Sig::Period)) {
        parts.push(tokens.get(cursor + 1)?.ident()?.to_string());
        cursor += 2;
    }
    Some((parts, cursor))
}

/// Tokenize with the stock Generic dialect, dropping whitespace and comments. `"x"` is an
/// identifier (ANSI) and lands as [`Sig::Quoted`]; `'x'` is a STRING and is therefore not usable
/// as a name here at all (it falls into [`Sig::Other`]).
fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
    Some(
        tokens
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
            .map(|token| match token {
                // sqlparser hands back a QUOTED identifier as a `Word` carrying its quote style —
                // there is no separate token for it on a dialect where `"` quotes identifiers. The
                // distinction is load-bearing here: `"branch"` must be a NAME, never the keyword.
                Token::Word(word) if word.quote_style.is_some() => Sig::Quoted(word.value),
                Token::Word(word) => Sig::Word(word.value),
                Token::Number(text, _) => Sig::Number(text),
                Token::Period => Sig::Period,
                _ => Sig::Other,
            })
            .collect(),
    )
}

// === Execution ==============================================================================

/// ===========================================================================================
/// Execute a recognized ref-DDL statement through the tier-1 `ManageSnapshots` seams.
/// ===========================================================================================
///
/// # Errors
/// The Q15 target refusal, a missing current snapshot on a `CREATE` without `AS OF VERSION`, or
/// any fork validation error (unknown snapshot, ref kind mismatch, tag-invalid retention).
pub(crate) async fn execute_ref_ddl(cx: &EngineContext<'_>, ddl: RefDdl) -> Result<DataFrame> {
    let name = object_name(&ddl.table_parts);
    let target = crate::create_table::resolve_target(cx, &name, "ALTER TABLE (branch/tag DDL)")?;
    let ident = target.ident();

    match ddl.op {
        RefOp::Create {
            kind,
            name,
            as_of_version,
            or_replace,
            retention,
        } => {
            let snapshot_id = pin_snapshot(&target, as_of_version, or_replace).await?;
            if or_replace {
                create_or_replace_snapshot_ref(
                    target.catalog.as_ref(),
                    &ident,
                    kind,
                    &name,
                    snapshot_id,
                    retention,
                )
                .await
            } else {
                create_snapshot_ref_with_retention(
                    target.catalog.as_ref(),
                    &ident,
                    kind,
                    &name,
                    snapshot_id,
                    retention,
                )
                .await
            }
            .map_err(iceberg_err)?;
        }
        RefOp::Drop {
            kind,
            name,
            if_exists,
        } => {
            if if_exists && !ref_exists(&target, &name).await? {
                return cx.ctx.read_empty();
            }
            drop_snapshot_ref(target.catalog.as_ref(), &ident, kind, &name)
                .await
                .map_err(iceberg_err)?;
        }
    }
    cx.ctx.read_empty()
}

/// The snapshot the new ref points at: the explicit `AS OF VERSION`, else the table's current
/// snapshot. A schema-only table has no current snapshot, and pinning a ref at "nothing" is not a
/// thing Iceberg can express — so that refuses, naming the fix.
async fn pin_snapshot(
    target: &CreateTarget,
    as_of_version: Option<i64>,
    or_replace: bool,
) -> Result<i64> {
    if let Some(snapshot_id) = as_of_version {
        return Ok(snapshot_id);
    }
    let table = target
        .catalog
        .load_table(&target.ident())
        .await
        .map_err(iceberg_err)?;
    table.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "ALTER TABLE … {} BRANCH|TAG on `{}` needs AS OF VERSION <snapshot-id>: the table \
             has no current snapshot yet (nothing has been written to it)",
            if or_replace {
                "CREATE OR REPLACE"
            } else {
                "CREATE"
            },
            target.full_name
        ))
    })
}

/// True when the table already carries a ref with this name.
async fn ref_exists(target: &CreateTarget, name: &str) -> Result<bool> {
    let table = target
        .catalog
        .load_table(&target.ident())
        .await
        .map_err(iceberg_err)?;
    Ok(table.metadata().snapshot_for_ref(name).is_some())
}

/// Re-render the recognized dotted name as an `ObjectName` so it can go through the ONE Q15
/// target resolver the rest of the door uses (same refusal text, same read-only handling).
fn object_name(parts: &[String]) -> datafusion::sql::sqlparser::ast::ObjectName {
    use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
    ObjectName(
        parts
            .iter()
            .map(|part| ObjectNamePart::Identifier(Ident::new(part.clone())))
            .collect(),
    )
}

#[cfg(test)]
mod tests;
