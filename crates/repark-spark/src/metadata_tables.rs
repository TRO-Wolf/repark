//! Spark Iceberg metadata-table name resolution.
//!
//! Fork inspect already ships the full Java table set and exposes them SQL-queryable via
//! iceberg-datafusion as `table$snapshots`, `table$files`, … (R142;
//! `integrations/datafusion/src/schema.rs:151-171`, pin `4723104b`). Spark's surface is a trailing
//! segment — `cat.ns.tbl.snapshots` / `spark.table("tbl.files")` — so this crate's job is name
//! resolution only: rewrite the Spark form onto the fork's `$` form when the parent identifier
//! resolves as an Iceberg base table and the full path is not a real table.
//!
//! Resolution order (hard pin):
//! 1. A **real table** occupying the full multipart path wins (e.g. a table literally named
//!    `files` in `cat.ns`).
//! 2. Else if the last segment is a known [`MetadataTableType`] **and** the parent loads as an
//!    Iceberg base table → rewrite `… .suffix` → `…$suffix`.
//! 3. Else leave the SQL alone (normal missing-table / column resolution).
//!
//! Out of scope v1 (loud refuse):
//! - Time-travel composition (`… .snapshots VERSION AS OF …`).
//! - DML / DDL targeting a resolved metadata table (INSERT/UPDATE/DELETE/MERGE/CTAS/TRUNCATE).
//!
//! Known fork residues — pin-as-documented, not fix (R142 / inspect module docs):
//! - Unpartitioned tables keep an empty-struct `partition` column (Java drops it on
//!   `partitions` / files-family).
//! - `readable_metrics` interior field-id order: compare by name.

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::ErrorKind;
use iceberg::inspect::MetadataTableType;
use iceberg::{NamespaceIdent, TableIdent};

use repark_core::CatalogRegistry;

use crate::catalog_ops::iceberg_err;

/// ===========================================================================================
/// Whether `name` is a known Iceberg metadata-table suffix (case-insensitive).
///
/// Mirrors fork `MetadataTableType::try_from` (`inspect/metadata_table.rs:99-121`, pin
/// `4723104b`). Names: `snapshots`, `manifests`, `files`, `data_files`, `delete_files`,
/// `entries`, `all_files`, `all_data_files`, `all_delete_files`, `all_entries`, `history`,
/// `refs`, `metadata_log_entries`, `partitions`, `all_manifests`.
/// ===========================================================================================
#[must_use]
pub fn is_metadata_table_name(name: &str) -> bool {
    MetadataTableType::try_from(name.to_ascii_lowercase().as_str()).is_ok()
}

/// Static spellings locked to fork `MetadataTableType::as_str` / `all_types` (unit-pinned).
const METADATA_TABLE_NAMES: &[&str] = &[
    "snapshots",
    "manifests",
    "files",
    "data_files",
    "delete_files",
    "entries",
    "all_files",
    "all_data_files",
    "all_delete_files",
    "all_entries",
    "history",
    "refs",
    "metadata_log_entries",
    "partitions",
    "all_manifests",
];

/// Canonical lowercase metadata-table suffix, if `name` is a known type.
///
/// Recognition uses fork `MetadataTableType::try_from`; the returned `&'static str` is the
/// matching entry of [`METADATA_TABLE_NAMES`] (must equal `metadata_type.as_str()`).
#[must_use]
pub fn canonical_metadata_table_name(name: &str) -> Option<&'static str> {
    let lowered = name.to_ascii_lowercase();
    let metadata_type = MetadataTableType::try_from(lowered.as_str()).ok()?;
    METADATA_TABLE_NAMES
        .iter()
        .copied()
        .find(|candidate| *candidate == metadata_type.as_str())
}

/// ===========================================================================================
/// Fast token sniff: SQL may contain a Spark-style multipart metadata-table path.
///
/// Cheap gate before the async resolve/rewrite pass. False positives are fine (resolved away);
/// false negatives would skip a real rewrite.
/// ===========================================================================================
#[must_use]
pub fn sql_may_have_metadata_table_path(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    !find_candidate_spans(&tokens).is_empty()
}

/// One multipart identifier whose last segment is a known metadata-table name.
#[derive(Debug, Clone)]
struct MetadataPathSpan {
    /// Token index of the first identifier part.
    start: usize,
    /// Token index one past the last identifier part (exclusive).
    end: usize,
    /// Dotted parts (catalog…table, suffix).
    parts: Vec<String>,
    /// Canonical lowercase metadata suffix.
    suffix: &'static str,
    /// Whether this identifier sits in a relation position (FROM/JOIN/INTO/UPDATE/TABLE…).
    /// Column references (`SELECT cat.ns.tbl.files`) are **not** relation positions and must not
    /// rewrite (octo C1-L-001).
    is_table_reference: bool,
    /// Whether this identifier is a DML/DDL write target (INSERT/UPDATE/DELETE/MERGE/CTAS…).
    is_write_target: bool,
    /// Whether a time-travel AS OF clause immediately follows this identifier.
    has_as_of: bool,
}

/// Max chars of a metadata path embedded in plan errors (octo C1-SEC-002).
const DISPLAY_PATH_MAX: usize = 200;

/// Truncate a display path for plan errors (octo C1-SEC-002).
fn display_path(parts: &[String], suffix: &str) -> String {
    let parent = parts[..parts.len().saturating_sub(1)].join(".");
    let full = format!("{parent}.{suffix}");
    if full.len() <= DISPLAY_PATH_MAX {
        full
    } else {
        format!("{}…", &full[..DISPLAY_PATH_MAX])
    }
}

/// ===========================================================================================
/// Resolve Spark-style metadata-table paths, refuse DML + AS OF composition, rewrite reads to
/// the fork's `table$meta` form. Returns `Ok(None)` when nothing rewrites (and no error).
/// ===========================================================================================
///
/// # Errors
/// - DML/DDL targeting a resolved metadata table → plan error naming the table.
/// - AS OF composition with a metadata table → plan error (out of scope v1).
pub async fn prepare_metadata_table_sql(
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<Option<String>> {
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(None);
    };
    let candidates = find_candidate_spans(&tokens);
    if candidates.is_empty() {
        return Ok(None);
    }

    let mut rewrites: Vec<(MetadataPathSpan, DollarRewrite)> = Vec::new();
    for candidate in candidates {
        // Column / expression positions must not rewrite (C1-L-001).
        if !candidate.is_table_reference {
            continue;
        }
        let decision = resolve_candidate(catalogs, &candidate).await?;
        match decision {
            ResolveDecision::Skip => {}
            ResolveDecision::Rewrite(dollar) => {
                if candidate.has_as_of {
                    return Err(DataFusionError::Plan(format!(
                        "time travel (VERSION/TIMESTAMP AS OF) composed with Iceberg metadata \
                         table `{}` is not supported in v1 — query the base table with AS OF, \
                         or the metadata table without AS OF \
                         (docs/spark-sql-iceberg-parity.md §2.1 metadata tables)",
                        display_path(&candidate.parts, candidate.suffix)
                    )));
                }
                if candidate.is_write_target {
                    return Err(DataFusionError::Plan(format!(
                        "Iceberg metadata table `{}` is read-only — INSERT/UPDATE/DELETE/MERGE/\
                         CTAS/TRUNCATE/CREATE VIEW/DROP/ALTER targeting a metadata table is not supported",
                        display_path(&candidate.parts, candidate.suffix)
                    )));
                }
                rewrites.push((candidate, dollar));
            }
        }
    }

    if rewrites.is_empty() {
        return Ok(None);
    }

    // Apply right-to-left so token indices stay valid.
    let mut tokens = tokens;
    rewrites.sort_by_key(|(span, _)| span.start);
    for (span, dollar) in rewrites.into_iter().rev() {
        // Replace only the last two name parts (`table` `.` `suffix`) with `table$suffix`,
        // leaving catalog/namespace prefixes intact.
        let last_part_token = span.end - 1;
        let mut table_token = last_part_token;
        while table_token > span.start {
            table_token -= 1;
            if matches!(tokens[table_token], Token::Period) {
                let mut name_index = table_token;
                while name_index > span.start {
                    name_index -= 1;
                    if !matches!(tokens[name_index], Token::Whitespace(_)) {
                        break;
                    }
                }
                // Preserve quoting from the original table token (C1-SEC-001).
                let original_quote = match &tokens[name_index] {
                    Token::Word(word) => word.quote_style,
                    Token::DoubleQuotedString(_) => Some('"'),
                    Token::SingleQuotedString(_) => Some('\''),
                    _ => None,
                };
                let replacement =
                    dollar_ident_token(&dollar.table_name, span.suffix, original_quote);
                tokens.splice(name_index..span.end, std::iter::once(replacement));
                break;
            }
        }
    }

    Ok(Some(tokens_to_sql(&tokens)))
}

/// Rewrite payload: base table name (quote style is read from the original token at splice).
struct DollarRewrite {
    table_name: String,
}

/// Emit `table$suffix` as a Word, preserving quoting when the base name is not a bare ident.
/// Quote styles `Word::Display` accepts without panicking (sqlparser tokenizer).
fn safe_ident_quote(prefer_quote: Option<char>) -> Option<char> {
    match prefer_quote {
        Some('"' | '`' | '[') => prefer_quote,
        // Single-quoted / unknown styles must not reach Word Display (octo C4-SAF-001).
        // sqlparser only accepts ", `, [ — normalize everything else to ANSI double quotes.
        Some(_) => Some('"'),
        None => None,
    }
}

/// Emit `table$suffix` as a Word, preserving quoting when the base name is not a bare ident.
fn dollar_ident_token(table_name: &str, suffix: &str, prefer_quote: Option<char>) -> Token {
    let value = format!("{table_name}${suffix}");
    let quote_style = safe_ident_quote(prefer_quote).or_else(|| {
        let bare_ok = value.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '$'
        });
        if bare_ok { None } else { Some('"') }
    });
    Token::Word(Word {
        value,
        quote_style,
        keyword: datafusion::sql::sqlparser::keywords::Keyword::NoKeyword,
    })
}

enum ResolveDecision {
    Skip,
    Rewrite(DollarRewrite),
}

/// Real table wins; else parent Iceberg table + known suffix → rewrite.
async fn resolve_candidate(
    catalogs: &CatalogRegistry,
    candidate: &MetadataPathSpan,
) -> Result<ResolveDecision> {
    // Real table occupying the full path wins — never treat as metadata.
    if table_exists_parts(catalogs, &candidate.parts).await? {
        return Ok(ResolveDecision::Skip);
    }

    let parent = &candidate.parts[..candidate.parts.len() - 1];
    if parent.len() < 2 {
        // Need at least namespace.table (or catalog.table) as parent.
        return Ok(ResolveDecision::Skip);
    }
    if !table_exists_parts(catalogs, parent).await? {
        return Ok(ResolveDecision::Skip);
    }

    // Parent is a real Iceberg table; suffix is a known metadata type → `$` form.
    // DataFusion GenericDialect accepts `$` as an identifier part; the catalog schema
    // provider resolves `table$suffix` (fork schema.rs:151-171).
    let Some(table_name) = parent.last().cloned() else {
        return Ok(ResolveDecision::Skip);
    };
    // Fork `split_once('$')` cannot round-trip a base name that already contains `$` (C1-SEC-001).
    if table_name.contains('$') {
        return Err(DataFusionError::Plan(format!(
            "cannot resolve Iceberg metadata table `{}`: base table name contains '$', which \
             conflicts with the iceberg-datafusion metadata-table delimiter (schema.rs split_once)",
            display_path(&candidate.parts, candidate.suffix)
        )));
    }
    Ok(ResolveDecision::Rewrite(DollarRewrite { table_name }))
}

/// Whether `parts` names an existing iceberg table under a registered catalog.
///
/// `parts[0]` is the catalog when registered; remaining segments form
/// `NamespaceIdent` (all but last) + table name (last). Single-level and multi-level
/// namespaces are both accepted via [`NamespaceIdent::from_vec`].
///
/// Missing namespace / missing table → `false` (not an error): multi-level probes for the
/// "real table wins" check routinely ask about namespaces that do not exist.
async fn table_exists_parts(catalogs: &CatalogRegistry, parts: &[String]) -> Result<bool> {
    if parts.len() < 2 {
        return Ok(false);
    }
    // Prefer registered-catalog prefix.
    if let Some(catalog) = catalogs.get(&parts[0]) {
        let rest = &parts[1..];
        if rest.is_empty() {
            return Ok(false);
        }
        if let Some(ident) = table_ident_from_parts(rest) {
            return match catalog.table_exists(&ident).await {
                Ok(exists) => Ok(exists),
                Err(error) => match error.kind() {
                    // Multi-level "real table wins" probes routinely miss namespaces.
                    ErrorKind::NamespaceNotFound | ErrorKind::TableNotFound => Ok(false),
                    _ => Err(iceberg_err(error)),
                },
            };
        }
        return Ok(false);
    }
    // No registered catalog prefix — cannot resolve (default-catalog is a tracked follow-up).
    Ok(false)
}

fn table_ident_from_parts(parts: &[String]) -> Option<TableIdent> {
    match parts {
        [] => None,
        [table] => {
            // Table with empty namespace is not used in RePark; reject.
            let _ = table;
            None
        }
        [namespace, table] => Some(TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        )),
        multi => {
            let table = multi.last()?.clone();
            let namespace_parts: Vec<String> = multi[..multi.len() - 1].to_vec();
            let namespace = NamespaceIdent::from_vec(namespace_parts).ok()?;
            Some(TableIdent::new(namespace, table))
        }
    }
}

/// Scan the full token stream for multipart identifiers ending in a metadata-table name.
fn find_candidate_spans(tokens: &[Token]) -> Vec<MetadataPathSpan> {
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    if significant.is_empty() {
        return Vec::new();
    }

    let is_ident = |sig_index: usize| -> bool {
        matches!(
            significant.get(sig_index).map(|(_, t)| *t),
            Some(Token::Word(_) | Token::DoubleQuotedString(_) | Token::SingleQuotedString(_))
        )
    };
    let is_period = |sig_index: usize| -> bool {
        matches!(
            significant.get(sig_index).map(|(_, t)| *t),
            Some(Token::Period)
        )
    };

    let mut spans = Vec::new();
    let mut sig_index = 0usize;
    while sig_index < significant.len() {
        if !is_ident(sig_index) {
            sig_index += 1;
            continue;
        }
        // Extend over Ident (Period Ident)*.
        let start_sig = sig_index;
        let mut end_sig = sig_index + 1;
        while end_sig + 1 < significant.len() && is_period(end_sig) && is_ident(end_sig + 1) {
            end_sig += 2;
        }
        // end_sig is exclusive; parts sit at start, start+2, … → count = (end-start)/2 + 0?
        // start=0,end=7 for a.b.c.d → (7-0)/2 = 3 … need +0 for half-open over Period-linked:
        // number of idents = 1 + number of (Period Ident) pairs = 1 + (end_sig-start_sig-1)/2
        // = (end_sig - start_sig + 1) / 2  with half-open [start, end).
        let part_count = (end_sig - start_sig).div_ceil(2);
        // Need ≥ 3 dotted parts so parent can be catalog.ns.table (or ns.table under a catalog
        // prefix of length ≥ 2 once catalog is stripped — minimum path is cat.ns.suffix for a
        // real-table-named-suffix pin, or cat.ns.tbl.suffix for metadata).
        if part_count >= 3 {
            let parts = collect_parts(&significant[start_sig..end_sig]);
            if let Some(suffix) = parts
                .last()
                .and_then(|last| canonical_metadata_table_name(last))
            {
                // Already `$` form? Not reachable via Period-separated tokens.
                let start = significant[start_sig].0;
                let end = significant[end_sig - 1].0 + 1;
                let is_table_reference = is_table_reference_context(&significant, start_sig);
                let is_write_target = is_write_target_context(&significant, start_sig);
                let has_as_of = has_trailing_as_of(&significant, end_sig);
                spans.push(MetadataPathSpan {
                    start,
                    end,
                    parts,
                    suffix,
                    is_table_reference,
                    is_write_target,
                    has_as_of,
                });
            }
        }
        sig_index = end_sig;
    }
    spans
}

/// Whether the identifier at `name_sig_start` is a relation (table) position, not a column ref.
///
/// Relation positions: FROM / JOIN / USING / INTO / UPDATE / OVERWRITE / CREATE|TRUNCATE TABLE /
/// comma-separated FROM-list entries. SELECT/WHERE/ON/GROUP BY column paths are excluded (C1-L-001).
fn is_table_reference_context(significant: &[(usize, &Token)], name_sig_start: usize) -> bool {
    if is_write_target_context(significant, name_sig_start) {
        return true;
    }
    let word = |index: usize| -> Option<String> {
        match significant.get(index).map(|(_, token)| *token) {
            Some(Token::Word(word)) => Some(word.value.to_ascii_uppercase()),
            _ => None,
        }
    };
    if name_sig_start == 0 {
        return false;
    }
    let mut cursor = name_sig_start;
    // Peel TABLE so FROM/JOIN checks still see the verb for `… TABLE t` non-write forms.
    if cursor > 0 && word(cursor - 1).as_deref() == Some("TABLE") {
        cursor -= 1;
    }
    // Peel opening parens: `FROM (cat.ns.tbl.meta)` / `FROM ((…))` (C1-L-002 composition).
    while cursor > 0
        && matches!(
            significant.get(cursor - 1).map(|(_, token)| *token),
            Some(Token::LParen)
        )
    {
        cursor -= 1;
    }
    if cursor == 0 {
        return false;
    }
    match significant.get(cursor - 1).map(|(_, token)| *token) {
        Some(Token::Word(prev)) => {
            let upper = prev.value.to_ascii_uppercase();
            matches!(
                upper.as_str(),
                "FROM"
                    | "JOIN"
                    | "USING"
                    | "INTO"
                    | "UPDATE"
                    | "OVERWRITE"
                    | "CREATE"
                    | "TRUNCATE"
                    | "REPLACE"
                    // DESCRIBE [TABLE] meta — read path must rewrite (octo C7-Q-001).
                    | "DESCRIBE"
                    | "DESC"
            )
        }
        Some(Token::Comma) => in_from_item_list(significant, cursor - 1),
        _ => false,
    }
}

/// Heuristic: a comma-separated item is a FROM-list table when a FROM appears to the left
/// before a SELECT/WHERE/SET/HAVING/GROUP/ORDER/UNION boundary.
fn in_from_item_list(significant: &[(usize, &Token)], comma_sig: usize) -> bool {
    let word = |index: usize| -> Option<String> {
        match significant.get(index).map(|(_, token)| *token) {
            Some(Token::Word(word)) => Some(word.value.to_ascii_uppercase()),
            _ => None,
        }
    };
    let mut index = comma_sig;
    while index > 0 {
        index -= 1;
        match word(index).as_deref() {
            Some("FROM") => return true,
            Some(
                "SELECT" | "WHERE" | "SET" | "HAVING" | "GROUP" | "ORDER" | "UNION" | "EXCEPT"
                | "INTERSECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE",
            ) => return false,
            _ => {}
        }
    }
    false
}

fn collect_parts(significant_slice: &[(usize, &Token)]) -> Vec<String> {
    let mut parts = Vec::new();
    for (_, token) in significant_slice {
        match token {
            Token::Word(word) => parts.push(word.value.clone()),
            Token::DoubleQuotedString(name) | Token::SingleQuotedString(name) => {
                parts.push(name.clone());
            }
            _ => {}
        }
    }
    parts
}

/// Whether the identifier at `name_sig_start` is the target of a write/DDL statement.
fn is_write_target_context(significant: &[(usize, &Token)], name_sig_start: usize) -> bool {
    // Walk left over optional TABLE keyword and INTO / FROM / OVERWRITE noise.
    let mut cursor = name_sig_start;
    let word = |index: usize| -> Option<String> {
        match significant.get(index).map(|(_, token)| *token) {
            Some(Token::Word(word)) => Some(word.value.to_ascii_uppercase()),
            _ => None,
        }
    };

    // CREATE [OR REPLACE] TABLE t | TRUNCATE TABLE t | INSERT OVERWRITE TABLE t
    // Must inspect left of TABLE *before* peeling it (octo C1: peel-first made CREATE dead).
    if cursor > 0 && word(cursor - 1).as_deref() == Some("TABLE") {
        let mut index = cursor - 1;
        while index > 0 {
            index -= 1;
            match word(index).as_deref() {
                // DROP/ALTER — metadata is read-only (octo C6-Q-001).
                Some("CREATE" | "TRUNCATE" | "REPLACE" | "INSERT" | "DROP" | "ALTER") => {
                    return true;
                }
                Some("OR" | "OVERWRITE" | "INTO" | "TABLE") => {}
                Some(_) | None => break,
            }
        }
        // Not a DDL TABLE target — still peel TABLE so INTO/OVERWRITE paths below can match
        // e.g. rare `INSERT INTO TABLE t` forms.
        cursor -= 1;
    }
    // CREATE [OR REPLACE] VIEW t — metadata is read-only (octo C5-Q-001).
    if cursor > 0 && word(cursor - 1).as_deref() == Some("VIEW") {
        let mut index = cursor - 1;
        while index > 0 {
            index -= 1;
            match word(index).as_deref() {
                Some("CREATE" | "REPLACE") => return true,
                Some("OR") => {}
                Some(_) | None => break,
            }
        }
    }

    // INSERT … [OVERWRITE] [INTO] t  /  DELETE FROM t  /  UPDATE t  /  MERGE INTO t
    if cursor == 0 {
        return false;
    }
    let prev = word(cursor - 1);
    match prev.as_deref() {
        Some("INTO") => {
            // MERGE INTO | INSERT INTO | INSERT OVERWRITE INTO
            let mut index = cursor - 1;
            while index > 0 {
                index -= 1;
                match word(index).as_deref() {
                    Some("INSERT" | "MERGE") => return true,
                    Some("OVERWRITE" | "INTO" | "TABLE") => {}
                    Some(_) | None => break,
                }
            }
            false
        }
        Some("FROM") => {
            // DELETE FROM t
            cursor >= 2 && word(cursor - 2).as_deref() == Some("DELETE")
        }
        Some("UPDATE") => true,
        Some("OVERWRITE") => {
            // INSERT OVERWRITE t (no INTO)
            let mut index = cursor - 1;
            while index > 0 {
                index -= 1;
                match word(index).as_deref() {
                    Some("INSERT") => return true,
                    Some("OVERWRITE" | "TABLE") => {}
                    Some(_) | None => break,
                }
            }
            false
        }
        _ => false,
    }
}

/// Whether `VERSION|TIMESTAMP|SYSTEM_* AS OF` immediately follows the identifier.
fn has_trailing_as_of(significant: &[(usize, &Token)], name_end_sig: usize) -> bool {
    let word_at = |sig_index: usize| -> Option<&str> {
        match significant.get(sig_index).map(|(_, token)| *token) {
            Some(Token::Word(word)) => Some(word.value.as_str()),
            _ => None,
        }
    };
    let mut i = name_end_sig;
    // Skip closing parens so `(cat.ns.tbl.snapshots) VERSION AS OF` still refuses (C1-L-002).
    while matches!(
        significant.get(i).map(|(_, token)| *token),
        Some(Token::RParen)
    ) {
        i += 1;
    }
    // Optional alias: AS alias / bare alias — skip one optional AS+ident or bare ident that is
    // not a time-travel keyword, then look for AS OF. Simpler v1: look for AS OF within the next
    // few tokens without consuming a FROM/WHERE/JOIN boundary.
    // Forms: [FOR] VERSION|TIMESTAMP|SYSTEM_VERSION|SYSTEM_TIME AS OF
    // Optional FOR
    if word_at(i).is_some_and(|w| w.eq_ignore_ascii_case("FOR")) {
        i += 1;
    }
    let kind = word_at(i).map(str::to_ascii_uppercase);
    let is_travel_kind = matches!(
        kind.as_deref(),
        Some("VERSION" | "TIMESTAMP" | "SYSTEM_VERSION" | "SYSTEM_TIME")
    );
    if !is_travel_kind {
        // Bare alias then travel? e.g. `t.snapshots s VERSION AS OF` — rare; also
        // `t.snapshots VERSION AS OF` handled above. Try skip one non-keyword ident.
        if kind.is_some()
            && !matches!(
                kind.as_deref(),
                Some(
                    "WHERE"
                        | "JOIN"
                        | "LEFT"
                        | "RIGHT"
                        | "FULL"
                        | "INNER"
                        | "CROSS"
                        | "ON"
                        | "GROUP"
                        | "ORDER"
                        | "LIMIT"
                        | "UNION"
                        | "EXCEPT"
                        | "INTERSECT"
                        | "HAVING"
                        | "WINDOW"
                        | "AS"
                )
            )
        {
            i += 1;
            if word_at(i).is_some_and(|w| w.eq_ignore_ascii_case("FOR")) {
                i += 1;
            }
            let kind2 = word_at(i).map(str::to_ascii_uppercase);
            if !matches!(
                kind2.as_deref(),
                Some("VERSION" | "TIMESTAMP" | "SYSTEM_VERSION" | "SYSTEM_TIME")
            ) {
                return false;
            }
        } else if word_at(i).is_some_and(|w| w.eq_ignore_ascii_case("AS"))
            && word_at(i + 1).is_some_and(|w| !w.eq_ignore_ascii_case("OF"))
        {
            // `AS alias` then maybe travel.
            i += 2;
            if word_at(i).is_some_and(|w| w.eq_ignore_ascii_case("FOR")) {
                i += 1;
            }
            let kind2 = word_at(i).map(str::to_ascii_uppercase);
            if !matches!(
                kind2.as_deref(),
                Some("VERSION" | "TIMESTAMP" | "SYSTEM_VERSION" | "SYSTEM_TIME")
            ) {
                return false;
            }
        } else {
            return false;
        }
    }
    // Now at travel kind word; need AS OF next.
    let after_kind = if word_at(i).is_some_and(|w| {
        matches!(
            w.to_ascii_uppercase().as_str(),
            "VERSION" | "TIMESTAMP" | "SYSTEM_VERSION" | "SYSTEM_TIME"
        )
    }) {
        i + 1
    } else {
        return false;
    };
    word_at(after_kind).is_some_and(|w| w.eq_ignore_ascii_case("AS"))
        && word_at(after_kind + 1).is_some_and(|w| w.eq_ignore_ascii_case("OF"))
}

fn tokens_to_sql(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_names_cover_fork_set() {
        for name in METADATA_TABLE_NAMES {
            assert!(is_metadata_table_name(name), "{name}");
            assert!(is_metadata_table_name(&name.to_ascii_uppercase()), "{name}");
            assert_eq!(canonical_metadata_table_name(name), Some(*name));
        }
        // Drift pin: every fork MetadataTableType::as_str is in our static set (C1-Q-004).
        for metadata_type in MetadataTableType::all_types() {
            let spelling = metadata_type.as_str();
            assert_eq!(
                canonical_metadata_table_name(spelling),
                Some(spelling),
                "fork as_str {spelling} must round-trip via canonical"
            );
        }
        assert!(!is_metadata_table_name("snapshot"));
        assert!(!is_metadata_table_name("data"));
        assert!(!is_metadata_table_name("events"));
    }

    #[test]
    fn sniff_finds_four_part_and_skips_plain() {
        assert!(sql_may_have_metadata_table_path(
            "SELECT * FROM mem.ns.events.snapshots"
        ));
        assert!(sql_may_have_metadata_table_path(
            "SELECT * FROM mem.ns.events.files WHERE record_count > 0"
        ));
        assert!(sql_may_have_metadata_table_path(
            "INSERT INTO mem.ns.events.history VALUES (1)"
        ));
        assert!(!sql_may_have_metadata_table_path(
            "SELECT * FROM mem.ns.events"
        ));
        assert!(!sql_may_have_metadata_table_path(
            "SELECT snapshots FROM mem.ns.events"
        ));
    }

    #[test]
    fn candidate_marks_write_and_as_of() {
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(&dialect, "INSERT INTO mem.ns.events.files SELECT 1")
            .tokenize()
            .expect("tokenize INSERT");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_write_target);
        assert!(spans[0].is_table_reference);
        assert!(!spans[0].has_as_of);
        assert_eq!(spans[0].suffix, "files");

        let tokens = Tokenizer::new(
            &dialect,
            "SELECT * FROM mem.ns.events.snapshots VERSION AS OF 1",
        )
        .tokenize()
        .expect("tokenize AS OF");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].has_as_of);
        assert!(spans[0].is_table_reference);
        assert!(!spans[0].is_write_target);

        let tokens = Tokenizer::new(&dialect, "DELETE FROM mem.ns.events.history")
            .tokenize()
            .expect("tokenize DELETE");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_write_target);

        let tokens = Tokenizer::new(
            &dialect,
            "MERGE INTO mem.ns.events.refs AS t USING s ON true WHEN MATCHED THEN DELETE",
        )
        .tokenize()
        .expect("tokenize MERGE");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_write_target);

        let tokens = Tokenizer::new(
            &dialect,
            "CREATE TABLE mem.ns.events.entries AS SELECT 1 AS id",
        )
        .tokenize()
        .expect("tokenize CREATE TABLE");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(
            spans[0].is_write_target,
            "CREATE TABLE meta target must be a write target"
        );
        assert!(spans[0].is_table_reference);
    }

    #[test]
    fn column_ref_is_not_table_reference() {
        // C1-L-001: FQ column path must not be treated as a metadata relation.
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(&dialect, "SELECT mem.ns.events.files FROM mem.ns.events")
            .tokenize()
            .expect("tokenize column ref");
        let spans = find_candidate_spans(&tokens);
        // Two candidates may appear: SELECT-list files (column) + none on bare table.
        // Only FROM relation would be 3-part without meta suffix for events itself.
        let select_list = spans
            .iter()
            .find(|span| span.parts.last().map(String::as_str) == Some("files"));
        let select_list = select_list.expect("files path candidate");
        assert!(
            !select_list.is_table_reference,
            "SELECT-list FQ column must not be a table reference: {select_list:?}"
        );
        assert!(!select_list.is_write_target);
    }

    #[test]
    fn as_of_detected_after_closing_paren() {
        // C1-L-002
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(
            &dialect,
            "SELECT * FROM (mem.ns.events.snapshots) VERSION AS OF 1",
        )
        .tokenize()
        .expect("tokenize paren AS OF");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1, "expected one metadata path");
        assert!(
            spans[0].has_as_of,
            "AS OF after closing paren must be detected"
        );
        assert!(spans[0].is_table_reference);
    }

    #[test]
    fn from_list_comma_is_table_reference() {
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(&dialect, "SELECT * FROM mem.ns.other, mem.ns.events.files")
            .tokenize()
            .expect("tokenize from list");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(
            spans[0].is_table_reference,
            "comma FROM-list item must be a relation"
        );
    }

    #[test]
    fn join_and_using_are_table_references() {
        // C2-Q-001
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(
            &dialect,
            "SELECT * FROM mem.ns.events e JOIN mem.ns.events.files f ON true",
        )
        .tokenize()
        .expect("tokenize JOIN");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_table_reference);
        assert_eq!(spans[0].suffix, "files");

        let tokens = Tokenizer::new(
            &dialect,
            "MERGE INTO mem.ns.events t USING mem.ns.events.snapshots s ON true \
             WHEN MATCHED THEN DELETE",
        )
        .tokenize()
        .expect("tokenize USING");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(
            spans[0].is_table_reference && !spans[0].is_write_target,
            "USING source is a read relation, not a write target: {spans:?}"
        );
        assert_eq!(spans[0].suffix, "snapshots");
    }

    #[test]
    fn describe_is_table_reference_not_write() {
        // C7-Q-001
        let dialect = DatabricksDialect {};
        for sql in [
            "DESCRIBE mem.ns.events.files",
            "DESCRIBE TABLE mem.ns.events.snapshots",
            "DESC mem.ns.events.history",
        ] {
            let tokens = Tokenizer::new(&dialect, sql)
                .tokenize()
                .unwrap_or_else(|error| panic!("tokenize {sql}: {error}"));
            let spans = find_candidate_spans(&tokens);
            assert_eq!(spans.len(), 1, "sql={sql}");
            assert!(
                spans[0].is_table_reference,
                "DESCRIBE must be a relation for rewrite: {sql}"
            );
            assert!(
                !spans[0].is_write_target,
                "DESCRIBE must not refuse as write: {sql}"
            );
        }
    }

    #[test]
    fn truncate_and_create_or_replace_are_write_targets() {
        // C2-Q-002
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(&dialect, "TRUNCATE TABLE mem.ns.events.files")
            .tokenize()
            .expect("tokenize TRUNCATE");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_write_target && spans[0].is_table_reference);

        let tokens = Tokenizer::new(
            &dialect,
            "CREATE OR REPLACE TABLE mem.ns.events.snapshots AS SELECT 1 AS id",
        )
        .tokenize()
        .expect("tokenize CREATE OR REPLACE");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(
            spans[0].is_write_target,
            "CREATE OR REPLACE TABLE meta must be a write target"
        );

        let tokens = Tokenizer::new(
            &dialect,
            "CREATE VIEW mem.ns.events.files AS SELECT 1 AS id",
        )
        .tokenize()
        .expect("tokenize CREATE VIEW");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(
            spans[0].is_write_target,
            "CREATE VIEW meta must be a write target (read-only)"
        );

        for ddl in [
            "DROP TABLE mem.ns.events.files",
            "ALTER TABLE mem.ns.events.snapshots ADD COLUMNS (x int)",
        ] {
            let tokens = Tokenizer::new(&dialect, ddl)
                .tokenize()
                .unwrap_or_else(|error| panic!("tokenize {ddl}: {error}"));
            let spans = find_candidate_spans(&tokens);
            assert_eq!(spans.len(), 1, "ddl={ddl}");
            assert!(
                spans[0].is_write_target,
                "DDL meta must be write target: {ddl}"
            );
        }
    }

    #[test]
    fn multi_span_candidates_ordered() {
        // C2-L-001 — two metadata paths in one statement
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(
            &dialect,
            "SELECT * FROM mem.ns.a.snapshots JOIN mem.ns.b.files ON true",
        )
        .tokenize()
        .expect("tokenize multi");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 2, "expected two metadata path spans");
        assert!(spans.iter().all(|span| span.is_table_reference));
        let suffixes: Vec<_> = spans.iter().map(|span| span.suffix).collect();
        assert_eq!(suffixes, vec!["snapshots", "files"]);
    }

    #[test]
    fn as_of_timestamp_and_system_forms() {
        // C3-Q-001
        let dialect = DatabricksDialect {};
        for sql in [
            "SELECT * FROM mem.ns.events.snapshots TIMESTAMP AS OF '2020-01-01'",
            "SELECT * FROM mem.ns.events.snapshots FOR SYSTEM_VERSION AS OF 1",
            "SELECT * FROM mem.ns.events.snapshots FOR SYSTEM_TIME AS OF '2020-01-01'",
        ] {
            let tokens = Tokenizer::new(&dialect, sql)
                .tokenize()
                .unwrap_or_else(|error| panic!("tokenize {sql}: {error}"));
            let spans = find_candidate_spans(&tokens);
            assert_eq!(spans.len(), 1, "sql={sql}");
            assert!(spans[0].has_as_of, "AS OF form must be detected: {sql}");
            assert!(spans[0].is_table_reference, "sql={sql}");
        }
    }

    #[test]
    fn double_quoted_multipart_is_candidate() {
        // C3-Q-002
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(&dialect, r#"SELECT * FROM "mem"."ns"."events"."snapshots""#)
            .tokenize()
            .expect("tokenize quoted");
        let spans = find_candidate_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_table_reference);
        assert_eq!(spans[0].parts, vec!["mem", "ns", "events", "snapshots"]);
        assert_eq!(spans[0].suffix, "snapshots");
    }

    #[test]
    fn dollar_in_base_name_message_shape() {
        // C3-Q-003 — pure guard shape (catalog-independent string contract).
        let path = display_path(
            &[
                "ice".into(),
                "ns".into(),
                "foo$bar".into(),
                "snapshots".into(),
            ],
            "snapshots",
        );
        assert!(path.contains("foo$bar"));
        let token = dollar_ident_token("foo$bar", "snapshots", None);
        match token {
            Token::Word(word) => assert_eq!(word.value, "foo$bar$snapshots"),
            other => panic!("expected Word, got {other:?}"),
        }
    }

    #[test]
    fn dollar_ident_token_quotes_hostile_names() {
        // C1-SEC-001
        let token = dollar_ident_token("foo bar", "snapshots", Some('"'));
        match token {
            Token::Word(word) => {
                assert_eq!(word.value, "foo bar$snapshots");
                assert_eq!(word.quote_style, Some('"'));
            }
            other => panic!("expected Word, got {other:?}"),
        }
        let bare = dollar_ident_token("events", "files", None);
        match bare {
            Token::Word(word) => {
                assert_eq!(word.value, "events$files");
                assert_eq!(word.quote_style, None);
            }
            other => panic!("expected Word, got {other:?}"),
        }
        // C4-SAF-001: single-quote preference must not reach Word Display (panic).
        let normalized = dollar_ident_token("events", "snapshots", Some('\''));
        match normalized {
            Token::Word(word) => {
                assert_eq!(word.quote_style, Some('"'));
                // Display must not panic.
                let rendered = word.to_string();
                assert!(rendered.contains("events$snapshots"), "{rendered}");
            }
            other => panic!("expected Word, got {other:?}"),
        }
        assert_eq!(safe_ident_quote(Some('\'')), Some('"'));
        assert_eq!(safe_ident_quote(Some('`')), Some('`'));
    }

    #[test]
    fn table_ident_from_parts_single_and_nested() {
        let ident =
            table_ident_from_parts(&["ns".into(), "events".into()]).expect("ns.events ident");
        assert_eq!(ident.name, "events");
        assert_eq!(ident.namespace.as_ref(), &vec!["ns".to_string()]);

        let ident = table_ident_from_parts(&["ns".into(), "nested".into(), "files".into()])
            .expect("nested files ident");
        assert_eq!(ident.name, "files");
        assert_eq!(
            ident.namespace.as_ref(),
            &vec!["ns".to_string(), "nested".to_string()]
        );
    }
}

// PROBE - will remove - cycle2 investigation only if we add test in half B
