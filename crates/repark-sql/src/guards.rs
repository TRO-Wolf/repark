//! The ANSI door's guard set (design §2 Q12), called explicitly at the router head.
//!
//! The seam is untouched: `repark-core` has no pre-execution hook, so each door calls its own
//! guards. Order is load-bearing and matches the design's judge-mandated fix — **multi-statement
//! refuses FIRST**, before any text rewrite or sniff, so a script can never have its second
//! statement silently rewritten or its first statement's target sniffed out of a string literal.
//!
//! Four guards run on TEXT at the router head:
//! 1. [`refuse_multi_statement`] — quote-aware (ANSI: `'…'` literals, `"…"` identifiers, no
//!    backticks), Spark-oracle message class.
//! 2. [`refuse_read_only_catalog_dml`] — P11: DML against a catalog the session registered
//!    read-only. Generic message (this door is not postgres-specific).
//! 3. [`refuse_write_to_branch`] — writes targeting a branch-suffixed name; the fork's append
//!    always sets `main`, so a branch-targeted write would silently land on `main`.
//!
//! The fourth, [`refuse_local_filesystem_plan`] (SEC-02), needs a `LogicalPlan`, so it runs in the
//! delegation path immediately after planning and before execution — the same position the Spark
//! door's passthrough uses.
//!
//! **Guard provenance (design §5 / the PR-5 ruling).** The Spark door's `local_fs_ddl` and
//! `ref_ddl::sniff_write_to_branch` are `pub(crate)`/private inside `repark-spark`, and this
//! crate must not take a door→door edge (nor the `repark-functions` edge the Spark gate uses to
//! read its conf). Neither was importable, so both are RE-IMPLEMENTED here against the same
//! observable contract — same conf key, same grandfather rule, same refusal class — and pinned by
//! this module's own tests. Recorded as such in `task/p2f-ansi-m1-ledger.md`.

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::parser::ParserError;
use repark_core::{CatalogRegistry, EngineContext};

use crate::scan::{blank_out_quoted_and_comments, leading_keyword};

/// The conf key that opens the SEC-02 local-filesystem gate. Spelled identically to the Spark
/// door's `repark_functions::cardinality::ALLOW_LOCAL_FILESYSTEM_DDL_KEY` — one user-visible
/// knob governs both doors, and the DataFusion `ConfigExtension` the session installs is read
/// here through the dependency-free `ConfigOptions::entries()` view (the extension TYPE lives in
/// `repark-functions`, which this crate deliberately does not depend on).
pub(crate) const ALLOW_LOCAL_FILESYSTEM_DDL_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

/// The `snake_case` spelling DataFusion's `extensions_options!` macro registers the field under
/// (`PREFIX = "repark.sql"`, field `allow_local_filesystem_ddl`) — this is what actually appears
/// in `ConfigOptions::entries()`.
const ALLOW_LOCAL_FILESYSTEM_DDL_OPTION: &str = "repark.sql.allow_local_filesystem_ddl";

/// ===========================================================================================
/// Run the text-level guard set. Called FIRST in [`crate::router::execute`], before any rewrite,
/// sniff, or parse.
/// ===========================================================================================
///
/// # Errors
/// The first guard that fires, as a classified [`DataFusionError`].
pub(crate) fn run_text_guards(cx: &EngineContext<'_>, sql: &str) -> Result<()> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    refuse_multi_statement(&scrubbed)?;
    refuse_read_only_catalog_dml(cx.catalogs, &scrubbed)?;
    refuse_write_to_branch(cx.ctx, &scrubbed)
}

// === Guard 1 — multi-statement (design §2 Q12; runs FIRST) ==================================

/// ===========================================================================================
/// Refuse genuine multi-statement scripts. A trailing `;`, whitespace, or comment after ONE
/// statement stays legal (that is the shape every client emits); a `;` followed by real content
/// is refused.
///
/// Takes the SCRUBBED text, so `SELECT 'a; b'` and `-- ;` are structurally invisible here. That
/// is the whole reason this guard is text-level and quote-aware rather than parser-level: a
/// parser-level check cannot refuse `SELECT 1; XYZZY 2`, where the second statement does not
/// parse at all — fail-closed matters more than precision.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::SQL`] (→ `Error::Parse` → Python `ParseException`) naming the class.
pub(crate) fn refuse_multi_statement(scrubbed: &str) -> Result<()> {
    let mut saw_semicolon = false;
    for ch in scrubbed.chars() {
        if ch == ';' {
            saw_semicolon = true;
        } else if saw_semicolon && !ch.is_whitespace() {
            return Err(multi_statement_error());
        }
    }
    Ok(())
}

/// The multi-statement refusal, in Spark's own error class so a migrated job sees a familiar
/// diagnostic through either door.
fn multi_statement_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not \
             supported. Only a single statement is accepted; a trailing semicolon, whitespace, \
             or comment after that statement is allowed"
                .to_string(),
        )),
        None,
    )
}

// === Guard 2 — P11 read-only-catalog DML ====================================================

/// ===========================================================================================
/// Refuse `INSERT` / `UPDATE` / `DELETE` / `MERGE` whose target's leading name segment is a
/// catalog the session registered read-only.
///
/// The message is **generic** by ruling: this door is not postgres-flavoured, so it names the
/// catalog and the direction that IS supported rather than a specific external system.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] naming the catalog and the supported direction.
pub(crate) fn refuse_read_only_catalog_dml(
    catalogs: &CatalogRegistry,
    scrubbed: &str,
) -> Result<()> {
    let Some((verb, target)) = dml_target(scrubbed) else {
        return Ok(());
    };
    let Some(catalog) = target.split('.').next() else {
        return Ok(());
    };
    let catalog = catalog.trim_matches('"');
    if catalogs.is_read_only_catalog(catalog) {
        return Err(DataFusionError::Plan(read_only_catalog_message(
            catalog, verb,
        )));
    }
    Ok(())
}

/// The P11 refusal text (pinned by tests; kept generic — no external-system name).
pub(crate) fn read_only_catalog_message(catalog: &str, verb: &str) -> String {
    format!(
        "catalog `{catalog}` is registered read-only: {verb} against it is not supported. \
         Read from it freely, or use it as the SOURCE of a MERGE INTO <writable table> \
         USING {catalog}.… statement."
    )
}

/// The DML verb and its target name, read from scrubbed text. `None` when the statement is not
/// DML (or the target cannot be read, in which case the parser will produce the better error).
fn dml_target(scrubbed: &str) -> Option<(&'static str, String)> {
    let mut words = word_iter(scrubbed);
    // The verb comes from the leading-keyword scan (comment/whitespace tolerant); the word
    // iterator is then advanced past it so the two agree on position.
    let leading = leading_keyword(scrubbed)?;
    words.next()?;
    let verb = match leading.as_str() {
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "MERGE" => "MERGE",
        _ => return None,
    };
    // Skip the verb's connective words to reach the target name.
    let target = loop {
        let word = words.next()?;
        let upper = word.to_ascii_uppercase();
        if matches!(
            upper.as_str(),
            "INTO" | "OVERWRITE" | "FROM" | "TABLE" | "ONLY"
        ) {
            continue;
        }
        break word;
    };
    Some((verb, target))
}

/// Split scrubbed SQL into "words", where a dotted/quoted name (`a.b."c d"`) is ONE word.
fn word_iter(scrubbed: &str) -> impl Iterator<Item = String> + '_ {
    let mut chars = scrubbed.char_indices().peekable();
    std::iter::from_fn(move || {
        // Skip anything that cannot start a name.
        while let Some(&(_, ch)) = chars.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '"' || ch == '$' {
                break;
            }
            chars.next();
        }
        chars.peek()?;
        let mut word = String::new();
        while let Some(&(_, ch)) = chars.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '"' || ch == '$' || ch == '.' {
                word.push(ch);
                chars.next();
            } else {
                break;
            }
        }
        if word.is_empty() { None } else { Some(word) }
    })
}

// === Guard 3 — write-to-branch ==============================================================

/// ===========================================================================================
/// Refuse a WRITE whose target names a snapshot branch.
///
/// The fork's fast-append always sets `SetSnapshotRef(MAIN_BRANCH)` — there is no branch-target
/// commit — so `INSERT INTO cat.ns.t.branch_audit …` would silently write to `main`. Silent
/// wrong-branch data is the worst failure mode available here, so it refuses.
///
/// Two shapes, with the ambiguity handled the way the Spark door settled it: a FOUR-part name
/// after a three-part table is unambiguously a ref suffix and always refuses; a TWO-part
/// `x.branch_y` is ambiguous with a real table literally named `branch_y`, so it refuses only
/// when the full name does NOT resolve while the prefix DOES. Neither resolving falls through to
/// planning's own "table not found", which is the more useful error.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] naming the branch spelling and the supported alternative.
pub(crate) fn refuse_write_to_branch(ctx: &SessionContext, scrubbed: &str) -> Result<()> {
    let Some((_, target)) = dml_target(scrubbed) else {
        return Ok(());
    };
    let parts: Vec<&str> = target
        .split('.')
        .map(|part| part.trim_matches('"'))
        .collect();
    let Some(last) = parts.last() else {
        return Ok(());
    };
    let branch_suffixed = last.to_ascii_lowercase().starts_with("branch_");

    let refuse = match parts.len() {
        // catalog.namespace.table.<ref> — unambiguous.
        n if n >= 4 => true,
        // x.branch_y — ambiguous with a real table called `branch_y`.
        2 if branch_suffixed => {
            let full = datafusion::sql::TableReference::partial(
                parts[0].to_string(),
                parts[1].to_string(),
            );
            let prefix = datafusion::sql::TableReference::bare(parts[0].to_string());
            !ctx.table_exist(full).unwrap_or(false) && ctx.table_exist(prefix).unwrap_or(false)
        }
        _ => false,
    };
    if refuse {
        return Err(DataFusionError::Plan(format!(
            "writing to a snapshot ref is not supported: `{target}` names a branch/tag, and every \
             write commit in this engine sets the `main` branch — the write would silently land on \
             `main`. Write to the table itself (`{}`) and manage refs with branch/tag DDL.",
            parts[..parts.len() - 1].join(".")
        )));
    }
    Ok(())
}

// === Guard 4 — SEC-02 local-filesystem plans (runs after planning) ==========================

/// ===========================================================================================
/// Refuse a planned `CREATE EXTERNAL TABLE … LOCATION` / `COPY … TO` that targets the local
/// filesystem, unless the conf opens the gate or the path sits under a registered warehouse root.
///
/// Delegation is this door's whole strategy, and DataFusion's DDL happily reads and writes the
/// process's local filesystem. A near-drop-in migration must not inherit that surface just
/// because the ANSI door hands statements to DataFusion. Remote schemes (`s3://`, `s3a://`, …)
/// are out of scope — this gate is about the local OS identity.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] naming the surface, the path, and the conf key that opens the gate.
pub(crate) fn refuse_local_filesystem_plan(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    if allow_local_filesystem_ddl(ctx) {
        return Ok(());
    }
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create)) => {
            refuse_local_path(catalogs, "CREATE EXTERNAL TABLE", &create.location)
        }
        LogicalPlan::Copy(copy_to) => refuse_local_path(catalogs, "COPY TO", &copy_to.output_url),
        _ => Ok(()),
    }
}

/// Read the gate conf from the live session options WITHOUT the `repark-functions` extension
/// type: `ConfigOptions::entries()` renders every registered extension field as a flat
/// `prefix.field` key/value pair, which is exactly what is needed and costs no crate edge.
fn allow_local_filesystem_ddl(ctx: &SessionContext) -> bool {
    ctx.copied_config()
        .options()
        .entries()
        .into_iter()
        .find(|entry| entry.key == ALLOW_LOCAL_FILESYSTEM_DDL_OPTION)
        .and_then(|entry| entry.value)
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "1"))
}

fn refuse_local_path(catalogs: &CatalogRegistry, surface: &str, raw_location: &str) -> Result<()> {
    let Some(local_path) = local_filesystem_path(raw_location) else {
        return Ok(());
    };
    if path_under_any_warehouse(catalogs, &local_path) {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "{surface} to local filesystem path `{raw_location}` is disabled by default. Set conf \
         `{ALLOW_LOCAL_FILESYSTEM_DDL_KEY}` = true to allow local CREATE EXTERNAL TABLE / COPY TO \
         outside the session warehouse root, or use a path under a registered warehouse \
         (grandfather). Remote locations (s3://, s3a://) are unaffected."
    )))
}

/// Map a location to a local path when it is bare or `file:`-schemed; `None` for remote schemes.
/// Scheme matching is case-insensitive — `FILE:///etc/passwd` must not classify as "some remote
/// scheme" and skip the gate.
fn local_filesystem_path(location: &str) -> Option<std::path::PathBuf> {
    const REMOTE_SCHEMES: &[&str] = &[
        "s3://",
        "s3a://",
        "s3n://",
        "http://",
        "https://",
        "hdfs://",
        "viewfs://",
        "gs://",
        "abfs://",
        "abfss://",
        "wasb://",
        "wasbs://",
    ];
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if REMOTE_SCHEMES
        .iter()
        .any(|scheme| lower.starts_with(scheme))
    {
        return None;
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "file://") {
        let path = strip_prefix_ci(rest, "localhost").unwrap_or(rest);
        return Some(std::path::PathBuf::from(path));
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "file:") {
        return Some(std::path::PathBuf::from(rest));
    }
    // An unknown but well-formed scheme is somebody else's object store — leave it to DataFusion.
    if let Some(index) = trimmed.find("://") {
        let scheme = &trimmed[..index];
        if !scheme.is_empty()
            && scheme
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '+' || ch == '-' || ch == '.')
        {
            return None;
        }
    }
    Some(std::path::PathBuf::from(trimmed))
}

fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&value[prefix.len()..])
    } else {
        None
    }
}

fn path_under_any_warehouse(catalogs: &CatalogRegistry, path: &std::path::Path) -> bool {
    let target = canonicalize_best_effort(path);
    catalogs.local_warehouse_roots().iter().any(|root| {
        let root = canonicalize_best_effort(std::path::Path::new(root));
        target == root || target.starts_with(&root)
    })
}

/// Canonicalize what exists; fall back to the parent (a COPY TO destination file need not exist
/// yet); fall back to a lexical `.`/`..` collapse so a traversal cannot slip past the check.
fn canonicalize_best_effort(path: &std::path::Path) -> std::path::PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    if let Some(parent) = path.parent()
        && let Ok(canonical_parent) = parent.canonicalize()
    {
        return match path.file_name() {
            Some(name) => canonical_parent.join(name),
            None => canonical_parent,
        };
    }
    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests;
