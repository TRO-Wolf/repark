//! Iceberg snapshot-ref DDL: `CREATE|DROP|REPLACE BRANCH|TAG`.

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

/// Try to parse `sql` as CREATE/DROP/REPLACE BRANCH|TAG (including ALTER TABLE forms).
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
                if starts_duration_clause(significant, after) {
                    let (age_amount, age_unit, age_after) =
                        parse_amount_unit(significant, after, "WITH SNAPSHOT RETENTION")?;
                    let max_snapshot_age_ms =
                        duration_to_ms(age_amount, age_unit, "WITH SNAPSHOT RETENTION")?;
                    if max_snapshot_age_ms <= 0 {
                        return Err(DataFusionError::Plan(
                            "WITH SNAPSHOT RETENTION duration must be greater than 0".into(),
                        ));
                    }
                    retention.max_snapshot_age_ms = Some(max_snapshot_age_ms);
                    return Ok((retention, age_after));
                }
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

fn starts_duration_clause(significant: &[Sig], index: usize) -> bool {
    let is_count = matches!(
        significant.get(index),
        Some(Sig::Number(raw)) if raw.parse::<i64>().is_ok()
    ) || matches!(
        significant.get(index),
        Some(Sig::Word(word)) if word.parse::<i64>().is_ok()
    );
    if !is_count {
        return false;
    }
    matches!(
        word_at(significant, index + 1)
            .map(str::to_ascii_uppercase)
            .as_deref(),
        Some("DAYS" | "DAY" | "HOURS" | "HOUR" | "MINUTES" | "MINUTE")
    )
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

/// Execute a parsed ref DDL statement against the Iceberg catalog.
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

/// Loud refuse message for write-to-branch (fork gap seed).
pub(crate) const WRITE_TO_BRANCH_NOT_SUPPORTED: &str = "\
write-to-branch (INSERT/UPDATE/DELETE/MERGE targeting `table.branch_<name>` or \
`table.branch_name`) is not supported at fork pin 33be9a0f411c37cd8d7b38c4db81eec30c1344cc. \
F-6 (#244) added SnapshotUpdate.to_branch to the seven transaction actions, but INSERT, \
UPDATE and DELETE execute through iceberg-datafusion's IcebergTableProvider and its commit \
exec, which commit with no branch target — so the statement would still write to main. \
Closing this needs a commit target on that provider, which is fork surface: do not work \
around it here. A write naming a TAG refuses on Apache Spark too. Read-side \
VERSION AS OF 'branch-or-tag' works; CREATE|REPLACE BRANCH re-pin is the product write path \
for refs today (docs/spark-sql-iceberg-parity.md §2.2).";

/// A sniffed write-to-branch candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteToBranchSniff {
    /// `cat.ns.table.ref` — always refuse (last segment is not a metadata suffix).
    MultiPart,
    /// `identifier.branch_xxx`.
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
    // INSERT [INTO|OVERWRITE] / UPDATE / DELETE FROM / MERGE INTO.
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
    // Look for a four-part (or more) dotted name after the verb keywords: … t .
    find_write_target_branch_span(&significant)
}

fn find_write_target_branch_span(significant: &[&Token]) -> Option<WriteToBranchSniff> {
    let parts = collect_ident_parts(significant, write_target_name_start(significant)?)?;
    if parts.len() >= 4 {
        let last = parts.last()?.as_str();
        if !crate::metadata_tables::is_metadata_table_name(last) {
            return Some(WriteToBranchSniff::MultiPart);
        }
    }
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
    None
}

fn write_target_name_start(significant: &[&Token]) -> Option<usize> {
    let head = word_upper(significant, 0)?;
    let mut index = match head.as_str() {
        "UPDATE" => 1,
        "DELETE" => {
            if word_upper(significant, 1).as_deref() == Some("FROM") {
                2
            } else {
                1
            }
        }
        "INSERT" | "MERGE" => {
            let mut cursor = 1;
            if matches!(
                word_upper(significant, cursor).as_deref(),
                Some("INTO" | "OVERWRITE")
            ) {
                cursor += 1;
            }
            cursor
        }
        _ => return None,
    };
    if word_upper(significant, index).as_deref() == Some("TABLE") {
        index += 1;
    }
    Some(index)
}

fn word_upper(significant: &[&Token], index: usize) -> Option<String> {
    match significant.get(index) {
        Some(Token::Word(word)) => Some(word.value.to_ascii_uppercase()),
        _ => None,
    }
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

/// Refuse write-to-branch with the precise fork gap (product STOP).
/// # Errors
/// Always returns [`DataFusionError::NotImplemented`] with [`WRITE_TO_BRANCH_NOT_SUPPORTED`].
pub(crate) fn refuse_write_to_branch() -> DataFusionError {
    DataFusionError::NotImplemented(WRITE_TO_BRANCH_NOT_SUPPORTED.to_string())
}

#[cfg(test)]
mod tests;
