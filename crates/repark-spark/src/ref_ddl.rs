//! Iceberg snapshot-ref DDL: `CREATE|DROP|REPLACE BRANCH|TAG`.
//!
//! Stock sqlparser does not model these forms. The router sniffs them and routes here. Supported:
//! - `ALTER TABLE t CREATE [OR REPLACE] BRANCH|TAG b [AS OF VERSION n] [RETAIN …] [WITH SNAPSHOT RETENTION …]`
//! - `ALTER TABLE t REPLACE BRANCH|TAG b [AS OF VERSION n] [RETAIN …] [WITH SNAPSHOT RETENTION …]`
//! - `ALTER TABLE t DROP BRANCH b` / `DROP TAG tag`
//! - Top-level `CREATE [OR REPLACE] BRANCH|TAG b IN t` / `DROP … IN t`
//!
//! Retention maps to the fork's snapshot-management actions. Write-to-branch inserts refuse
//! because the fork has no branch-target commit operation.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::write::{
    SnapshotRefKind, SnapshotRefRetention, create_or_replace_snapshot_ref,
    create_snapshot_ref_with_retention, drop_snapshot_ref, replace_snapshot_ref,
};

use repark_core::CatalogRegistry;

use crate::{catalog_handle, iceberg_err, reregister};

/// A parsed snapshot-ref DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefDdl {
    /// Target table as dotted parts (`catalog`, `namespace`, `table`).
    table_parts: Vec<String>,
    op: RefOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RefOp {
    Create {
        kind: SnapshotRefKind,
        name: String,
        /// When set, pin the ref at this snapshot id; otherwise the table's current snapshot.
        as_of_version: Option<i64>,
        /// `true` for `CREATE OR REPLACE` (create-if-absent, replace-if-present).
        or_replace: bool,
        retention: SnapshotRefRetention,
    },
    /// Bare `REPLACE BRANCH|TAG` — requires the ref to already exist (fork `replace_*`).
    Replace {
        kind: SnapshotRefKind,
        name: String,
        as_of_version: Option<i64>,
        retention: SnapshotRefRetention,
    },
    Drop {
        kind: SnapshotRefKind,
        name: String,
    },
}

/// Significant tokens as owned strings for word positions (Period / Number kept separate).
#[derive(Debug, Clone)]
enum Sig {
    Word(String),
    Period,
    Number(String),
    Other,
}

/// ===========================================================================================
/// Try to parse `sql` as CREATE/DROP/REPLACE BRANCH|TAG (including ALTER TABLE forms).
///
/// Returns `None` when the statement is not ref DDL. Unsupported trailing clauses return
/// `Some(Err(…))` so the router does not fall through to an opaque parser error.
/// ===========================================================================================
pub(crate) fn try_parse_ref_ddl(sql: &str) -> Option<Result<RefDdl>> {
    let significant = tokenize_significant(sql)?;
    if significant.len() < 2 {
        return None;
    }
    // Top-level CREATE OR REPLACE BRANCH|TAG name IN t …
    if is_create_or_replace_at(&significant, 0)
        && let Some(kind) = branch_or_tag_at(&significant, 3)
    {
        return Some(parse_create_with_in(&significant, 4, kind, true));
    }
    if word_eq(&significant, 0, "CREATE")
        && let Some(kind) = branch_or_tag_at(&significant, 1)
    {
        return Some(parse_create_with_in(&significant, 2, kind, false));
    }
    if word_eq(&significant, 0, "DROP")
        && let Some(kind) = branch_or_tag_at(&significant, 1)
    {
        return Some(parse_drop_with_in(&significant, 2, kind));
    }
    // Top-level bare REPLACE is not a Spark form we ship — ALTER TABLE … REPLACE only.
    parse_alter_table_ref_ddl(&significant)
}

fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    Some(
        tokens
            .into_iter()
            .filter_map(|token| match token {
                Token::Whitespace(_) | Token::EOF | Token::SemiColon => None,
                Token::Word(word) => Some(Sig::Word(word.value)),
                Token::Period => Some(Sig::Period),
                Token::Number(raw, _) => Some(Sig::Number(raw)),
                _ => Some(Sig::Other),
            })
            .collect(),
    )
}

fn parse_alter_table_ref_ddl(significant: &[Sig]) -> Option<Result<RefDdl>> {
    if !(word_eq(significant, 0, "ALTER") && word_eq(significant, 1, "TABLE")) {
        return None;
    }
    let mut index = 2usize;
    word_at(significant, index)?;
    let table_start = index;
    index += 1;
    while is_period_at(significant, index) && word_at(significant, index + 1).is_some() {
        index += 2;
    }
    let table_parts = collect_name_parts(significant, table_start, index)?;

    // CREATE OR REPLACE BRANCH|TAG
    if is_create_or_replace_at(significant, index)
        && let Some(kind) = branch_or_tag_at(significant, index + 3)
    {
        return Some(finish_create(
            significant,
            index + 4,
            kind,
            table_parts,
            true,
        ));
    }
    // Bare REPLACE BRANCH|TAG
    if word_eq(significant, index, "REPLACE")
        && let Some(kind) = branch_or_tag_at(significant, index + 1)
    {
        return Some(finish_replace(significant, index + 2, kind, table_parts));
    }

    if word_eq(significant, index, "CREATE")
        && let Some(kind) = branch_or_tag_at(significant, index + 1)
    {
        return Some(finish_create(
            significant,
            index + 2,
            kind,
            table_parts,
            false,
        ));
    }
    if word_eq(significant, index, "DROP")
        && let Some(kind) = branch_or_tag_at(significant, index + 1)
    {
        return Some(finish_drop(significant, index + 2, kind, table_parts));
    }
    None
}

fn finish_create(
    significant: &[Sig],
    name_index: usize,
    kind: SnapshotRefKind,
    table_parts: Vec<String>,
    or_replace: bool,
) -> Result<RefDdl> {
    let name = require_ref_name(significant, name_index, "CREATE BRANCH|TAG")?;
    let (as_of_version, after_as_of) = parse_as_of_version(significant, name_index + 1)?;
    let (retention, end_index) = parse_retention_clauses(significant, after_as_of, kind)?;
    reject_trailing_tokens(significant, end_index, "CREATE BRANCH|TAG")?;
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

fn finish_replace(
    significant: &[Sig],
    name_index: usize,
    kind: SnapshotRefKind,
    table_parts: Vec<String>,
) -> Result<RefDdl> {
    let name = require_ref_name(significant, name_index, "REPLACE BRANCH|TAG")?;
    let (as_of_version, after_as_of) = parse_as_of_version(significant, name_index + 1)?;
    let (retention, end_index) = parse_retention_clauses(significant, after_as_of, kind)?;
    reject_trailing_tokens(significant, end_index, "REPLACE BRANCH|TAG")?;
    Ok(RefDdl {
        table_parts,
        op: RefOp::Replace {
            kind,
            name,
            as_of_version,
            retention,
        },
    })
}

fn finish_drop(
    significant: &[Sig],
    name_index: usize,
    kind: SnapshotRefKind,
    table_parts: Vec<String>,
) -> Result<RefDdl> {
    let name = require_ref_name(significant, name_index, "DROP BRANCH|TAG")?;
    // DROP has no AS OF / retention — ref name must be the last significant token.
    reject_trailing_tokens(significant, name_index + 1, "DROP BRANCH|TAG")?;
    Ok(RefDdl {
        table_parts,
        op: RefOp::Drop { kind, name },
    })
}

/// Unquote + refuse empty / path-escape ref names before catalog I/O.
fn require_ref_name(significant: &[Sig], name_index: usize, form: &str) -> Result<String> {
    let name = word_at(significant, name_index)
        .map(unquote_ident)
        .ok_or_else(|| DataFusionError::Plan(format!("{form} requires a ref name")))?;
    if name.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form} ref name must not be empty"
        )));
    }
    // Apply the same path-escape hygiene as table identifiers.
    crate::reject_path_escape_ident(&name, &format!("{form} ref name"))?;
    Ok(name)
}

fn parse_create_with_in(
    significant: &[Sig],
    name_index: usize,
    kind: SnapshotRefKind,
    or_replace: bool,
) -> Result<RefDdl> {
    let form = if or_replace {
        "CREATE OR REPLACE BRANCH|TAG"
    } else {
        "CREATE BRANCH|TAG"
    };
    let name = require_ref_name(significant, name_index, form)?;
    if !word_eq(significant, name_index + 1, "IN") {
        return Err(DataFusionError::Plan(
            "CREATE BRANCH|TAG requires `IN catalog.namespace.table` (or use \
             ALTER TABLE … CREATE BRANCH|TAG)"
                .into(),
        ));
    }
    let (table_parts, after) = parse_dotted_name(significant, name_index + 2)?;
    let (as_of_version, after_as_of) = parse_as_of_version(significant, after)?;
    let (retention, end_index) = parse_retention_clauses(significant, after_as_of, kind)?;
    reject_trailing_tokens(significant, end_index, form)?;
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

fn parse_drop_with_in(
    significant: &[Sig],
    name_index: usize,
    kind: SnapshotRefKind,
) -> Result<RefDdl> {
    let name = require_ref_name(significant, name_index, "DROP BRANCH|TAG")?;
    if !word_eq(significant, name_index + 1, "IN") {
        return Err(DataFusionError::Plan(
            "DROP BRANCH|TAG requires `IN catalog.namespace.table` (or use \
             ALTER TABLE … DROP BRANCH|TAG)"
                .into(),
        ));
    }
    let (table_parts, after) = parse_dotted_name(significant, name_index + 2)?;
    reject_trailing_tokens(significant, after, "DROP BRANCH|TAG")?;
    Ok(RefDdl {
        table_parts,
        op: RefOp::Drop { kind, name },
    })
}

/// Fail loud when significant tokens remain after a fully-parsed ref DDL form.
///
/// Reject trailing tokens and known-but-unsupported `IF EXISTS` spellings instead of dropping them.
fn reject_trailing_tokens(significant: &[Sig], end_index: usize, form: &str) -> Result<()> {
    if end_index < significant.len() {
        let leftover = match significant.get(end_index) {
            Some(Sig::Word(word)) => format!("word {word:?}"),
            Some(Sig::Number(raw)) => format!("number {raw:?}"),
            Some(Sig::Period) => "period".to_string(),
            Some(Sig::Other) => "token".to_string(),
            None => "end".to_string(),
        };
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: trailing clause after the supported form is not supported yet \
             (got {leftover}) — supported: CREATE [OR REPLACE]|REPLACE|DROP BRANCH|TAG \
             [AS OF VERSION n] [RETAIN n DAYS|HOURS|MINUTES] \
             [WITH SNAPSHOT RETENTION n SNAPSHOTS|DAYS|HOURS|MINUTES]; \
             IF EXISTS / IF NOT EXISTS stay out (docs/spark-sql-iceberg-parity.md §2.2 / r25 T2)"
        )));
    }
    Ok(())
}

/// Parse optional retention clauses starting at `index`.
///
/// Grammar (Spark Iceberg branch/tag docs):
/// ```text
/// [RETAIN <n> DAYS|HOURS|MINUTES]
/// [WITH SNAPSHOT RETENTION <n> SNAPSHOTS|DAYS|HOURS|MINUTES]
/// ```
/// Either clause may appear; order is RETAIN then WITH SNAPSHOT RETENTION when both present.
///
/// # Errors
/// Malformed numbers/units, tag + branch-only snapshot retention, non-positive counts.
fn parse_retention_clauses(
    significant: &[Sig],
    index: usize,
    kind: SnapshotRefKind,
) -> Result<(SnapshotRefRetention, usize)> {
    let mut retention = SnapshotRefRetention::default();
    let mut cursor = index;

    if word_eq(significant, cursor, "RETAIN") {
        let (amount, unit, after) = parse_amount_unit(significant, cursor + 1, "RETAIN")?;
        let max_ref_age_ms = duration_to_ms(amount, unit, "RETAIN")?;
        if max_ref_age_ms <= 0 {
            return Err(DataFusionError::Plan(
                "RETAIN duration must be greater than 0".into(),
            ));
        }
        retention.max_ref_age_ms = Some(max_ref_age_ms);
        cursor = after;
    }

    if word_eq(significant, cursor, "WITH")
        && word_eq(significant, cursor + 1, "SNAPSHOT")
        && word_eq(significant, cursor + 2, "RETENTION")
    {
        if kind == SnapshotRefKind::Tag {
            return Err(DataFusionError::Plan(
                "WITH SNAPSHOT RETENTION is only valid on BRANCH (tags only support RETAIN \
                 max-ref-age; fork ManageSnapshots set_min_snapshots_to_keep / \
                 set_max_snapshot_age_ms reject tags)"
                    .into(),
            ));
        }
        let (amount, unit, after) =
            parse_amount_unit(significant, cursor + 3, "WITH SNAPSHOT RETENTION")?;
        match unit {
            TimeUnit::Snapshots => {
                if amount > i64::from(i32::MAX) || amount <= 0 {
                    return Err(DataFusionError::Plan(
                        "WITH SNAPSHOT RETENTION n SNAPSHOTS requires a positive i32 count".into(),
                    ));
                }
                retention.min_snapshots_to_keep = Some(i32::try_from(amount).map_err(|_| {
                    DataFusionError::Plan(
                        "WITH SNAPSHOT RETENTION n SNAPSHOTS count does not fit i32".into(),
                    )
                })?);
            }
            other => {
                let max_snapshot_age_ms = duration_to_ms(amount, other, "WITH SNAPSHOT RETENTION")?;
                if max_snapshot_age_ms <= 0 {
                    return Err(DataFusionError::Plan(
                        "WITH SNAPSHOT RETENTION duration must be greater than 0".into(),
                    ));
                }
                retention.max_snapshot_age_ms = Some(max_snapshot_age_ms);
            }
        }
        cursor = after;
    }

    Ok((retention, cursor))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeUnit {
    Days,
    Hours,
    Minutes,
    Snapshots,
}

fn parse_amount_unit(
    significant: &[Sig],
    index: usize,
    form: &str,
) -> Result<(i64, TimeUnit, usize)> {
    let amount = match significant.get(index) {
        Some(Sig::Number(raw)) => raw.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!("{form} requires an integer count, got {raw:?}"))
        })?,
        Some(Sig::Word(word)) => word.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!("{form} requires an integer count, got {word:?}"))
        })?,
        other => {
            return Err(DataFusionError::Plan(format!(
                "{form} requires an integer count, got {other:?}"
            )));
        }
    };
    let unit = match word_at(significant, index + 1).map(str::to_ascii_uppercase) {
        Some(ref word) if word == "DAYS" || word == "DAY" => TimeUnit::Days,
        Some(ref word) if word == "HOURS" || word == "HOUR" => TimeUnit::Hours,
        Some(ref word) if word == "MINUTES" || word == "MINUTE" => TimeUnit::Minutes,
        Some(ref word) if word == "SNAPSHOTS" || word == "SNAPSHOT" => TimeUnit::Snapshots,
        other => {
            return Err(DataFusionError::Plan(format!(
                "{form} requires unit DAYS|HOURS|MINUTES|SNAPSHOTS, got {other:?}"
            )));
        }
    };
    Ok((amount, unit, index + 2))
}

fn duration_to_ms(amount: i64, unit: TimeUnit, form: &str) -> Result<i64> {
    let multiplier: i64 = match unit {
        TimeUnit::Days => 86_400_000,
        TimeUnit::Hours => 3_600_000,
        TimeUnit::Minutes => 60_000,
        TimeUnit::Snapshots => {
            return Err(DataFusionError::Plan(format!(
                "{form}: SNAPSHOTS is not a duration unit here"
            )));
        }
    };
    amount.checked_mul(multiplier).ok_or_else(|| {
        DataFusionError::Plan(format!("{form}: duration overflows i64 milliseconds"))
    })
}

fn parse_dotted_name(significant: &[Sig], start: usize) -> Result<(Vec<String>, usize)> {
    if word_at(significant, start).is_none() {
        return Err(DataFusionError::Plan(
            "BRANCH|TAG IN requires a three-part table name".into(),
        ));
    }
    let mut end = start + 1;
    while is_period_at(significant, end) && word_at(significant, end + 1).is_some() {
        end += 2;
    }
    let parts = collect_name_parts(significant, start, end)
        .ok_or_else(|| DataFusionError::Plan("BRANCH|TAG IN requires a table name".into()))?;
    Ok((parts, end))
}

fn word_at(significant: &[Sig], index: usize) -> Option<&str> {
    match significant.get(index) {
        Some(Sig::Word(word)) => Some(word.as_str()),
        _ => None,
    }
}

fn word_eq(significant: &[Sig], index: usize, expected: &str) -> bool {
    word_at(significant, index).is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

fn is_period_at(significant: &[Sig], index: usize) -> bool {
    matches!(significant.get(index), Some(Sig::Period))
}

fn branch_or_tag_at(significant: &[Sig], index: usize) -> Option<SnapshotRefKind> {
    match word_at(significant, index) {
        Some(word) if word.eq_ignore_ascii_case("BRANCH") => Some(SnapshotRefKind::Branch),
        Some(word) if word.eq_ignore_ascii_case("TAG") => Some(SnapshotRefKind::Tag),
        _ => None,
    }
}

fn is_create_or_replace_at(significant: &[Sig], start: usize) -> bool {
    word_eq(significant, start, "CREATE")
        && word_eq(significant, start + 1, "OR")
        && word_eq(significant, start + 2, "REPLACE")
        && branch_or_tag_at(significant, start + 3).is_some()
}

fn collect_name_parts(significant: &[Sig], start: usize, end: usize) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut index = start;
    while index < end {
        if let Some(word) = word_at(significant, index) {
            parts.push(unquote_ident(word));
            index += 1;
            if index < end && is_period_at(significant, index) {
                index += 1;
            }
        } else {
            break;
        }
    }
    if parts.is_empty() { None } else { Some(parts) }
}

/// Parse optional `AS OF VERSION <n>` starting at `index`.
///
/// Returns `(None, index)` when absent (caller must still end-of-statement check), or
/// `(Some(id), index_after_version)` when present.
fn parse_as_of_version(significant: &[Sig], index: usize) -> Result<(Option<i64>, usize)> {
    if index >= significant.len() {
        return Ok((None, index));
    }
    if !(word_eq(significant, index, "AS")
        && word_eq(significant, index + 1, "OF")
        && word_eq(significant, index + 2, "VERSION"))
    {
        // Not an AS OF clause — leave index unchanged so retention / reject_trailing can see it.
        return Ok((None, index));
    }
    let snapshot_id = match significant.get(index + 3) {
        Some(Sig::Number(raw)) => raw.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "AS OF VERSION requires an integer snapshot id, got {raw:?}"
            ))
        })?,
        Some(Sig::Word(word)) => word.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "AS OF VERSION requires an integer snapshot id, got {word:?}"
            ))
        })?,
        other => {
            return Err(DataFusionError::Plan(format!(
                "AS OF VERSION requires an integer snapshot id, got {other:?}"
            )));
        }
    };
    Ok((Some(snapshot_id), index + 4))
}

fn unquote_ident(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        // Double-quote dialect: undouble embedded `""`.
        return trimmed[1..trimmed.len() - 1].replace("\"\"", "\"");
    }
    if trimmed.len() >= 2 && trimmed.starts_with('`') && trimmed.ends_with('`') {
        // Backticks: no SQL standard double-backtick escape in our Spark surface.
        return trimmed[1..trimmed.len() - 1].to_string();
    }
    trimmed.to_string()
}

/// Resolve create/replace pin snapshot: explicit AS OF, else table current snapshot.
fn resolve_snapshot_id(
    loaded: &iceberg::table::Table,
    as_of_version: Option<i64>,
    catalog_name: &str,
    namespace: &str,
    table: &str,
    form: &str,
) -> Result<i64> {
    if let Some(snapshot_id) = as_of_version {
        return Ok(snapshot_id);
    }
    loaded.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{form} on `{catalog_name}.{namespace}.{table}` requires \
             AS OF VERSION <id> because the table has no current snapshot \
             (schema-only empty tables need an explicit version)"
        ))
    })
}

/// ===========================================================================================
/// Execute a parsed ref DDL statement against the Iceberg catalog.
/// ===========================================================================================
///
/// # Errors
/// Unknown catalog/table, missing current snapshot (CREATE without AS OF), or fork validation.
pub(crate) async fn execute_ref_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: RefDdl,
) -> Result<DataFrame> {
    let [catalog_name, namespace, table] = ddl.table_parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "BRANCH|TAG target must be a three-part `catalog.namespace.table` name, got `{}`",
            ddl.table_parts.join(".")
        )));
    };
    let handle = catalog_handle(catalogs, catalog_name)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());

    match ddl.op {
        RefOp::Create {
            kind,
            name,
            as_of_version,
            or_replace,
            retention,
        } => {
            let loaded = handle.load_table(&ident).await.map_err(iceberg_err)?;
            let snapshot_id = resolve_snapshot_id(
                &loaded,
                as_of_version,
                catalog_name,
                namespace,
                table,
                if or_replace {
                    "CREATE OR REPLACE BRANCH|TAG"
                } else {
                    "CREATE BRANCH|TAG"
                },
            )?;
            if or_replace {
                create_or_replace_snapshot_ref(
                    handle.as_ref(),
                    &ident,
                    kind,
                    &name,
                    snapshot_id,
                    retention,
                )
                .await
                .map_err(iceberg_err)?;
            } else {
                create_snapshot_ref_with_retention(
                    handle.as_ref(),
                    &ident,
                    kind,
                    &name,
                    snapshot_id,
                    retention,
                )
                .await
                .map_err(iceberg_err)?;
            }
        }
        RefOp::Replace {
            kind,
            name,
            as_of_version,
            retention,
        } => {
            let loaded = handle.load_table(&ident).await.map_err(iceberg_err)?;
            let snapshot_id = resolve_snapshot_id(
                &loaded,
                as_of_version,
                catalog_name,
                namespace,
                table,
                "REPLACE BRANCH|TAG",
            )?;
            replace_snapshot_ref(handle.as_ref(), &ident, kind, &name, snapshot_id, retention)
                .await
                .map_err(iceberg_err)?;
        }
        RefOp::Drop { kind, name } => {
            drop_snapshot_ref(handle.as_ref(), &ident, kind, &name)
                .await
                .map_err(iceberg_err)?;
        }
    }

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), catalog_name, &namespace).await?;
    ctx.read_empty()
}

// Write-to-branch targets must refuse because the fork commits only to MAIN_BRANCH.

/// Loud refuse message for write-to-branch (fork gap seed).
pub(crate) const WRITE_TO_BRANCH_NOT_SUPPORTED: &str = "\
write-to-branch (INSERT/UPDATE/DELETE/MERGE targeting `table.branch_<name>` or \
`table.branch_name`) is not supported: fork pin b009ac158f7584a956fa9292c0e9675a411ecf0d \
FastAppendAction / SnapshotProduce always emit SetSnapshotRef on MAIN_BRANCH only \
(transaction/append.rs + snapshot.rs) — no to_branch / with_branch commit-target API. \
Fork-workstream seed (not a RePark hack). Read-side VERSION AS OF 'branch' works; \
CREATE|REPLACE BRANCH re-pin is the product write path for refs today \
(docs/spark-sql-iceberg-parity.md §2.2 / r25 T2 ledger).";

/// ===========================================================================================
/// A sniffed write-to-branch candidate. `MultiPart` (≥4 dotted parts) is unambiguous — no
/// 4-part name can be a real table. `TwoPart` is AMBIGUOUS with a genuine
/// `schema.branch_daily` table under the default catalog; the caller must disambiguate by
/// resolution before refusing.
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteToBranchSniff {
    /// `cat.ns.table.ref` — always refuse (last segment is not a metadata suffix).
    MultiPart,
    /// `identifier.branch_xxx` — refuse only when the PREFIX resolves as a table and the
    /// full two-part name does not.
    TwoPart { parts: [String; 2] },
}

/// Sniff write-to-branch / write-to-tag targets in raw SQL (pure syntax, no resolution).
#[must_use]
pub(crate) fn sniff_write_to_branch(sql: &str) -> Option<WriteToBranchSniff> {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return None;
    };
    let significant: Vec<&Token> = tokens
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF | Token::SemiColon))
        .collect();
    if significant.len() < 4 {
        return None;
    }
    // INSERT [INTO|OVERWRITE] / UPDATE / DELETE FROM / MERGE INTO
    let is_write_head = match significant.first() {
        Some(Token::Word(word)) => {
            let upper = word.value.to_ascii_uppercase();
            matches!(upper.as_str(), "INSERT" | "UPDATE" | "DELETE" | "MERGE")
        }
        _ => false,
    };
    if !is_write_head {
        return None;
    }
    // Look for a four-part (or more) dotted name after the verb keywords: … t . branch_name
    // Heuristic: last segment of a multipart table ref is not a known metadata suffix AND
    // looks like a branch/tag name used as write target. Spark's product form is
    // `table.branch_<name>` or the branch name as a trailing segment after a real table.
    find_write_target_branch_span(&significant)
}

/// Find a write-target span that ends with a non-metadata trailing segment after a real table path.
fn find_write_target_branch_span(significant: &[&Token]) -> Option<WriteToBranchSniff> {
    // Locate INTO / FROM / UPDATE (for UPDATE t SET) keyword then parse dotted name.
    let mut index = 0usize;
    while index < significant.len() {
        let is_target_kw = matches!(
            significant.get(index),
            Some(Token::Word(word))
                if {
                    let upper = word.value.to_ascii_uppercase();
                    matches!(upper.as_str(), "INTO" | "FROM" | "UPDATE" | "TABLE")
                }
        );
        // Also: `UPDATE cat.ns.t.branch SET` — word at 0 is UPDATE, next is name.
        let at_update_name = index == 1
            && matches!(
                significant.first(),
                Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("UPDATE")
            );
        if is_target_kw || at_update_name {
            let name_start = if is_target_kw { index + 1 } else { index };
            if let Some(parts) = collect_ident_parts(significant, name_start) {
                // Four-or-more parts: `catalog.namespace.table.ref` — last segment is the
                // branch/tag write target when it is not a metadata-table suffix.
                // Three-part names stay alone: a table literally named `branch_exp` is valid
                // (`mem.ns.branch_exp` must not false-positive — expire CALL pin).
                if parts.len() >= 4 {
                    let last = parts.last()?.as_str();
                    if !crate::metadata_tables::is_metadata_table_name(last) {
                        return Some(WriteToBranchSniff::MultiPart);
                    }
                }
                // Two-part `table.branch_foo` under session default catalog — Spark WAP-adjacent
                // spelling; only when the trailing segment starts with `branch_`. Ambiguous with
                // a real `schema.branch_foo` table — caller disambiguates by resolution.
                if parts.len() == 2 {
                    let last = parts.last()?.as_str();
                    if last.to_ascii_lowercase().starts_with("branch_")
                        && !crate::metadata_tables::is_metadata_table_name(last)
                    {
                        let mut iter = parts.into_iter();
                        let first = iter.next()?;
                        let second = iter.next()?;
                        return Some(WriteToBranchSniff::TwoPart {
                            parts: [first, second],
                        });
                    }
                }
            }
        }
        index += 1;
    }
    None
}

fn collect_ident_parts(significant: &[&Token], start: usize) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut index = start;
    loop {
        let word = match significant.get(index) {
            Some(Token::Word(word)) => unquote_ident(&word.value),
            Some(Token::DoubleQuotedString(value)) => value.clone(),
            _ => break,
        };
        parts.push(word);
        index += 1;
        if matches!(significant.get(index), Some(Token::Period)) {
            index += 1;
            continue;
        }
        break;
    }
    if parts.is_empty() { None } else { Some(parts) }
}

/// ===========================================================================================
/// Refuse write-to-branch with the precise fork gap (product STOP).
/// ===========================================================================================
///
/// # Errors
/// Always returns [`DataFusionError::NotImplemented`] with [`WRITE_TO_BRANCH_NOT_SUPPORTED`].
pub(crate) fn refuse_write_to_branch() -> DataFusionError {
    DataFusionError::NotImplemented(WRITE_TO_BRANCH_NOT_SUPPORTED.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_alter_create_branch_as_of() {
        let ddl = try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE BRANCH audit AS OF VERSION 42")
            .expect("recognized")
            .expect("ok");
        assert_eq!(ddl.table_parts, ["ice", "sales", "t"]);
        assert!(matches!(
            ddl.op,
            RefOp::Create {
                kind: SnapshotRefKind::Branch,
                ref name,
                as_of_version: Some(42),
                or_replace: false,
                retention,
            } if name == "audit" && retention.is_empty()
        ));
    }

    #[test]
    fn parses_alter_create_tag_current() {
        let ddl = try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE TAG release")
            .expect("recognized")
            .expect("ok");
        assert!(matches!(
            ddl.op,
            RefOp::Create {
                kind: SnapshotRefKind::Tag,
                ref name,
                as_of_version: None,
                or_replace: false,
                ..
            } if name == "release"
        ));
    }

    #[test]
    fn parses_create_or_replace_branch() {
        let ddl = try_parse_ref_ddl(
            "ALTER TABLE ice.sales.t CREATE OR REPLACE BRANCH audit AS OF VERSION 7",
        )
        .expect("recognized")
        .expect("ok");
        assert!(matches!(
            ddl.op,
            RefOp::Create {
                kind: SnapshotRefKind::Branch,
                ref name,
                as_of_version: Some(7),
                or_replace: true,
                ..
            } if name == "audit"
        ));
    }

    #[test]
    fn parses_bare_replace_branch() {
        let ddl = try_parse_ref_ddl("ALTER TABLE ice.sales.t REPLACE BRANCH audit AS OF VERSION 9")
            .expect("recognized")
            .expect("ok");
        assert!(matches!(
            ddl.op,
            RefOp::Replace {
                kind: SnapshotRefKind::Branch,
                ref name,
                as_of_version: Some(9),
                ..
            } if name == "audit"
        ));
    }

    #[test]
    fn parses_retain_and_snapshot_retention() {
        let ddl = try_parse_ref_ddl(
            "ALTER TABLE ice.sales.t CREATE BRANCH audit RETAIN 7 DAYS \
             WITH SNAPSHOT RETENTION 10 SNAPSHOTS",
        )
        .expect("recognized")
        .expect("ok");
        match ddl.op {
            RefOp::Create { retention, .. } => {
                assert_eq!(retention.max_ref_age_ms, Some(7 * 86_400_000));
                assert_eq!(retention.min_snapshots_to_keep, Some(10));
                assert!(retention.max_snapshot_age_ms.is_none());
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    #[test]
    fn parses_snapshot_retention_days() {
        let ddl =
            try_parse_ref_ddl("CREATE BRANCH audit IN ice.sales.t WITH SNAPSHOT RETENTION 2 DAYS")
                .expect("recognized")
                .expect("ok");
        match ddl.op {
            RefOp::Create { retention, .. } => {
                assert_eq!(retention.max_snapshot_age_ms, Some(2 * 86_400_000));
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    #[test]
    fn tag_with_snapshot_retention_refuses() {
        let err = try_parse_ref_ddl(
            "ALTER TABLE ice.sales.t CREATE TAG t1 RETAIN 1 DAYS WITH SNAPSHOT RETENTION 2 DAYS",
        )
        .expect("recognized")
        .expect_err("tag snapshot retention");
        assert!(
            err.to_string().contains("BRANCH") || err.to_string().contains("tag"),
            "got: {err}"
        );
    }

    #[test]
    fn non_ref_returns_none() {
        assert!(try_parse_ref_ddl("SELECT 1").is_none());
        assert!(try_parse_ref_ddl("ALTER TABLE ice.sales.t SET TBLPROPERTIES ('a'='b')").is_none());
        assert!(
            try_parse_ref_ddl("ALTER TABLE ice.create.branch RENAME TO ice.sales.other").is_none()
        );
    }

    /// Trailing junk / IF EXISTS still refuse loud (not silent drop).
    #[test]
    fn trailing_tokens_after_as_of_or_drop_refuse_loud() {
        for sql in [
            "ALTER TABLE ice.sales.t CREATE BRANCH audit AS OF VERSION 42 RETENTION 7 DAYS",
            "CREATE BRANCH audit IN ice.sales.t AS OF VERSION 7 IF NOT EXISTS",
            "ALTER TABLE ice.sales.t DROP BRANCH audit IF EXISTS",
            "DROP TAG t1 IN ice.sales.t CASCADE",
            "ALTER TABLE ice.sales.t CREATE TAG release EXTRA",
        ] {
            let err = try_parse_ref_ddl(sql)
                .expect("recognized as ref DDL")
                .expect_err("trailing must refuse");
            let message = err.to_string();
            assert!(
                message.contains("not supported")
                    || message.contains("trailing")
                    || message.contains("unit"),
                "sql={sql:?} message={message}"
            );
            assert!(
                !message.contains("ParserError"),
                "must not fall through to opaque parse for {sql:?}: {message}"
            );
        }
    }

    #[test]
    fn empty_ref_name_refuses_loud() {
        for sql in [
            "ALTER TABLE ice.sales.t CREATE BRANCH ``",
            "CREATE BRANCH `` IN ice.sales.t AS OF VERSION 1",
        ] {
            let err = try_parse_ref_ddl(sql)
                .expect("recognized")
                .expect_err("empty ref name");
            assert!(err.to_string().contains("empty"), "sql={sql:?} got: {err}");
        }
    }

    #[test]
    fn qi1_unquote_ident_undoubles_embedded_quotes() {
        assert_eq!(unquote_ident(r#""na""me""#), "na\"me");
        assert_eq!(unquote_ident("`plain`"), "plain");
        assert_eq!(unquote_ident("bare"), "bare");
    }

    #[test]
    fn qi1_ref_name_path_escape_shared_needles() {
        for (segment, kind_tag) in [
            ("foo..bar", "traversal"),
            ("../etc", "traversal"),
            ("a/b", "separator"),
            (r"a\b", "separator"),
        ] {
            let sql = format!("ALTER TABLE ice.sales.t CREATE BRANCH `{segment}`");
            let err = try_parse_ref_ddl(&sql)
                .expect("recognized as ref DDL")
                .expect_err("path-escape ref must refuse");
            let text = err.to_string();
            match kind_tag {
                "traversal" => assert!(
                    text.contains("path traversal") || text.contains(".."),
                    "segment {segment:?}: {text}"
                ),
                "separator" => assert!(
                    text.contains("path separators") || text.contains('/') || text.contains('\\'),
                    "segment {segment:?}: {text}"
                ),
                other => panic!("unknown kind tag {other}"),
            }
        }
        let ok = try_parse_ref_ddl("ALTER TABLE ice.sales.t CREATE BRANCH audit")
            .expect("recognized")
            .expect("safe ref name");
        assert!(matches!(
            ok.op,
            RefOp::Create {
                kind: SnapshotRefKind::Branch,
                ref name,
                ..
            } if name == "audit"
        ));
    }

    #[test]
    fn write_to_branch_sniff_detects_four_part_insert() {
        assert!(
            sniff_write_to_branch("INSERT INTO ice.sales.t.audit SELECT 1 AS id, 'x' AS name")
                .is_some()
        );
        assert!(sniff_write_to_branch("INSERT INTO ice.sales.t.branch_audit SELECT 1").is_some());
        // Two-part `table.branch_foo` under default catalog.
        assert!(sniff_write_to_branch("INSERT INTO t.branch_audit SELECT 1").is_some());
        // Normal three-part INSERT must not trip — including a table named `branch_exp`.
        assert!(
            sniff_write_to_branch("INSERT INTO ice.sales.t SELECT 1 AS id, 'x' AS name").is_none()
        );
        assert!(
            sniff_write_to_branch("INSERT INTO mem.ns.branch_exp SELECT 4 AS id, 'd' AS name")
                .is_none()
        );
        // Metadata-table DML is a different refuse path (not write-to-branch).
        assert!(sniff_write_to_branch("INSERT INTO ice.sales.t.snapshots SELECT 1").is_none());
    }

    /// The sniff separates the unambiguous ≥4-part form from the
    /// resolution-ambiguous two-part form so the router can disambiguate the latter.
    #[test]
    fn write_to_branch_sniff_kinds() {
        assert_eq!(
            sniff_write_to_branch("INSERT INTO ice.sales.t.branch_audit SELECT 1"),
            Some(WriteToBranchSniff::MultiPart)
        );
        assert_eq!(
            sniff_write_to_branch("INSERT INTO t.branch_audit SELECT 1"),
            Some(WriteToBranchSniff::TwoPart {
                parts: ["t".to_string(), "branch_audit".to_string()]
            })
        );
        // A genuine `schema.branch_daily` sniffs TwoPart — the ROUTER must not refuse it
        // when the full name resolves (integration pin in lib.rs).
        assert_eq!(
            sniff_write_to_branch("INSERT INTO public.branch_daily SELECT 1"),
            Some(WriteToBranchSniff::TwoPart {
                parts: ["public".to_string(), "branch_daily".to_string()]
            })
        );
        // Two-part without the `branch_` prefix is not sniffed at all.
        assert_eq!(sniff_write_to_branch("INSERT INTO ns.daily SELECT 1"), None);
    }
}
