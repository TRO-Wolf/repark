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
//! More guards need something the text does not carry, so they run later — and the ORDER
//! between the DML pair is load-bearing:
//!
//! 4. [`refuse_collation_in_statement`] (G15) needs the PARSED statement. The router calls it
//!    immediately after the stock parse, before the statement match, so `COLLATE` / column
//!    collation / session collation conf refuse at parse altitude (G3-E8 lesson).
//!    Type-position `CAST AS STRING COLLATE` is refused on the parse-fail arm
//!    ([`refuse_type_position_collation_in_sql`]); `RESET` of a collation key is
//!    refused before delegate.
//! 5. [`refuse_dml_subquery_predicate`] (G3-E8) needs the PARSED statement (it reads the `WHERE`
//!    expression), so the router calls it from its `DELETE` / `UPDATE` arms. It closes a
//!    silent-data-loss window: a subquery predicate is lost at DataFusion's DML planning boundary
//!    and degenerates into match-all.
//! 6. [`refuse_mor_multi_spec_dml`] (the hoisted BUG-001 valve) is `async` — it loads the target
//!    table's Iceberg metadata to decide — so the router calls it from the SAME two arms,
//!    immediately AFTER the G3-E8 valve. Both are data-loss valves, so either message is honest;
//!    the cheap sync AST walk runs before the metadata round-trip, which is the Spark door's
//!    order and rationale exactly (`repark_spark::router::execute_delete`). Pinned by
//!    `guards::tests::mor_valve_runs_after_the_g3e8_valve`.
//!
//! The last, [`refuse_local_filesystem_plan`] (SEC-02), needs a `LogicalPlan`, so it runs in the
//! delegation path immediately after planning and before execution — the same position the Spark
//! door's passthrough uses. Note its scope: it gates the surfaces DataFusion's own DDL would use
//! to read/write the local filesystem as data (`CREATE EXTERNAL TABLE`, `COPY TO`). An
//! INTERCEPTED `CREATE TABLE … WITH (location = 'file:///…')` is NOT in scope — that path creates
//! an Iceberg table under a warehouse root and is governed by the catalog's
//! [`repark_core::LocationPolicy`], which is a different (and stricter, per-catalog) rule.
//!
//! **Guard provenance (design §5 / the PR-5 ruling).** The Spark door's `local_fs_ddl`,
//! `ref_ddl::sniff_write_to_branch` and `normalize::refuse_dml_subquery_predicate` are
//! `pub(crate)`/private inside `repark-spark`, and this crate must not take a door→door edge (nor
//! the `repark-functions` edge the Spark gate uses to read its conf). None was importable, so all
//! are RE-IMPLEMENTED here against the same observable contract — same conf key, same grandfather
//! rule, same refusal class, same refusal text — and pinned by this door's own tests. Recorded as
//! such in `task/p2f-ansi-m1-ledger.md` and `task/g3e8-guard-ledger.md`.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    AlterSchemaOperation, AlterTableOperation, ColumnOption, Delete, Expr, FromTable, ObjectName,
    Query, Set, Statement, TableFactor, TableWithJoins, Update, Visit, Visitor,
};
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext};
use repark_iceberg::write::{
    MorDmlKind, refuse_mor_unpartitioned_multi_spec_dml,
    refuse_v3_cow_dml as refuse_v3_cow_dml_in_catalog,
};

use crate::scan::{blank_out_quoted_and_comments, leading_keyword};

/// Needle pinned by the G15 refusal tests (both doors). Byte-identical to the Spark door.
pub(crate) const COLLATION_REFUSAL_NEEDLE: &str = "does not implement collation";

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

// === Guard 4 — BUG-001 MoR valve (async; runs at the router head) ===========================

/// ===========================================================================================
/// Refuse a delegated `DELETE` / `UPDATE` against a merge-on-read table whose CURRENT partition
/// spec is unpartitioned while its metadata carries more than one spec in history.
///
/// This door delegates DML to the fork's `TableProvider` (ADR-0003), which is exactly the path
/// the hoisted valve gates: the fork's unpartitioned fast path stamps every position delete with
/// `partition_key = None`, so after partition-spec evolution a delete can commit while rows
/// remain visible. The valve is tier-1 (`repark_iceberg::write`); this function is the ANSI
/// door's resolution wrapper — the twin of the Spark door's `ObjectName` wrapper, reading its
/// target from the same scrubbed text the other guards use.
///
/// `INSERT` is not gated (it writes no position deletes) and `MERGE` is never gated (the
/// RePark-owned merge-on-read writer stamps per-file partitions correctly).
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] from the tier-1 valve, naming the fork hazard and the workarounds.
pub(crate) async fn refuse_mor_multi_spec_dml(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Result<()> {
    let Some((kind, target, catalog_name, ident)) = dml_target_ident(cx, statement) else {
        return Ok(());
    };
    let Some(catalog) = cx.catalogs.get(&catalog_name) else {
        return Ok(());
    };
    refuse_mor_unpartitioned_multi_spec_dml(catalog.as_ref(), &ident, &target, kind).await
}

/// ===========================================================================================
/// V3R-1 valve (owner ruling 2026-08-25, registry `V3-COW-1`): the delegated plain-`WHERE`
/// `DELETE` / `UPDATE` never reaches the `predicate_dml` write-mode resolver, so the format-v3
/// copy-on-write refusal needs this seat too. Same target resolution as the BUG-001 valve
/// above; the router calls it right after that valve, before delegation.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::NotImplemented`] naming row lineage, the verb and `V3-COW-1`.
pub(crate) async fn refuse_v3_cow_dml(cx: &EngineContext<'_>, statement: &Statement) -> Result<()> {
    let Some((kind, _target, catalog_name, ident)) = dml_target_ident(cx, statement) else {
        return Ok(());
    };
    let Some(catalog) = cx.catalogs.get(&catalog_name) else {
        return Ok(());
    };
    refuse_v3_cow_dml_in_catalog(catalog.as_ref(), &ident, kind).await
}

/// Resolve a `DELETE` / `UPDATE` statement's target into `(verb, display target, catalog name,
/// table ident)` from the **AST**, the way DataFusion will resolve it: a bare name completes
/// from the session's `datafusion.catalog.default_catalog` and `default_schema`, a two-part
/// name from `default_catalog` alone, and `catalog.ns….table` is taken as written (a quoted
/// identifier is one part whatever it contains). `INSERT` writes no position deletes and
/// `MERGE` runs the RePark-owned writer, so both return `None`; so does a name that cannot be
/// read or a namespace that cannot be built — the planner's own error is the better one.
///
/// Before V3R-1 this read scrubbed text and split on `.`: a quoted `"a.b"` broke the split and
/// a short name returned `None`, and in both cases every valve stepped aside — on a v3 table
/// the DELETE then went to the fork and committed (CCC findings SEC-001, SEC-003).
fn dml_target_ident(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Option<(MorDmlKind, String, String, TableIdent)> {
    let (kind, name) = match statement {
        Statement::Delete(delete) => (MorDmlKind::Delete, delete_target_name(delete)?),
        Statement::Update(update) => (MorDmlKind::Update, object_name_of(&update.table)?),
        _ => return None,
    };
    let mut parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
    if parts.is_empty() {
        return None;
    }
    if parts.len() < 3 {
        let (default_catalog, default_schema) = {
            let state = cx.ctx.state();
            let catalog = &state.config().options().catalog;
            (
                catalog.default_catalog.clone(),
                catalog.default_schema.clone(),
            )
        };
        if parts.len() == 1 {
            parts.insert(0, default_schema);
        }
        parts.insert(0, default_catalog);
    }
    let namespace = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()).ok()?;
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    Some((kind, name.to_string(), parts[0].clone(), ident))
}

/// The target of a parsed `DELETE`: the multi-delete `tables` form first, else the first FROM
/// relation.
fn delete_target_name(delete: &Delete) -> Option<&ObjectName> {
    if let Some(name) = delete.tables.first() {
        return Some(name);
    }
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    tables.first().and_then(object_name_of)
}

// === Guard 5 — SEC-02 local-filesystem plans (runs after planning) ==========================

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

// === Guard 6 — G3-E8 subquery-predicate DML valve (runs on the parsed statement) ============

/// The DML verb a G3-E8 subquery-predicate refusal names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DmlSubqueryVerb {
    /// SQL `DELETE`.
    Delete,
    /// SQL `UPDATE`.
    Update,
}

impl DmlSubqueryVerb {
    /// The SQL verb, for the refusal message.
    const fn verb(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
        }
    }

    /// What the statement would silently do today, for the refusal message.
    const fn consequence(self) -> &'static str {
        match self {
            Self::Delete => "delete EVERY row of the table",
            Self::Update => "update EVERY row of the table",
        }
    }

    /// The `MERGE INTO` arm the workaround uses, for the refusal message.
    const fn merge_action(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE SET <assignments>",
        }
    }
}

/// ===========================================================================================
/// Refuse a delegated `DELETE` / `UPDATE` whose `WHERE` clause contains a **subquery** (G3-E8).
///
/// This door delegates DML to DataFusion, which recovers the `WHERE` clause for the fork's
/// `TableProvider::delete_from` / `::update` by walking the **optimized** plan for `Filter` /
/// `TableScan.filters` nodes (`datafusion::physical_planner::extract_dml_filters`). The optimizer
/// has by then decorrelated `IN` / `NOT IN` / `EXISTS` / `ANY` / `ALL` / correlated predicates
/// into a semi/anti/mark **join**, from which that walk recovers nothing — and an empty filter
/// list is the provider's spelling of "no `WHERE` clause", so the statement matches **every row**.
/// Silent, total, and reproduced identically through BOTH doors.
///
/// **Guard provenance (design §5 / the PR-5 ruling).** The Spark door carries the twin of this
/// valve as `repark_spark::normalize::refuse_dml_subquery_predicate`; that crate is a door, so
/// this crate must not take a product edge to it. Re-implemented here against the same observable
/// contract — same detection rule, same refusal text — and pinned by this door's own tests.
///
/// Detection is "**any `Query` node under the `WHERE` expression**", not an enumeration of
/// subquery-bearing `Expr` variants, so a sqlparser upgrade cannot silently widen the hole. The
/// class is refused wholesale even though an *uncorrelated* scalar subquery executes correctly
/// today: its *correlated* twin is the same parse tree and destroys the table, and the two are
/// not separable without full name resolution (rationale + the over-refused spellings:
/// `task/g3e8-guard-ledger.md`).
///
/// The refused target is read from the PARSED statement, not from the scrubbed text: this door's
/// text scrubber blanks quoted regions, so a quoted target (`DELETE FROM "ice"."sales"."t"`)
/// would otherwise be rendered into the message as blanks and the suggested `MERGE INTO` rewrite
/// would name a table that does not exist. Reading the AST also makes the rendered string equal
/// to the Spark door's for the same statement, which
/// `tests/cross_door.rs::cross_door_g3e8_refusals_render_identically` pins.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] naming the defect class, the `MERGE INTO` workaround, and that
/// support returns with the fix.
pub(crate) fn refuse_dml_subquery_predicate(statement: &Statement) -> Result<()> {
    let (verb, selection, target) = match statement {
        Statement::Delete(delete) => (
            DmlSubqueryVerb::Delete,
            delete.selection.as_ref(),
            delete_target(delete).map_or_else(|| "<table>".to_string(), ToString::to_string),
        ),
        Statement::Update(update) => (
            DmlSubqueryVerb::Update,
            update.selection.as_ref(),
            update_target(update).map_or_else(|| update.table.to_string(), ToString::to_string),
        ),
        _ => return Ok(()),
    };
    if repark_iceberg::write::predicate_dml::try_allowed_delete_in(statement)?.is_some()
        || repark_iceberg::write::predicate_dml::try_allowed_update_in(statement)?.is_some()
    {
        return Ok(());
    }
    let Some(selection) = selection else {
        return Ok(());
    };
    if !expression_contains_subquery(selection) {
        return Ok(());
    }
    Err(DataFusionError::Plan(dml_subquery_refusal_message(
        verb, &target,
    )))
}

/// The `ObjectName` a `DELETE` targets, from the parse tree — `FROM t` and the FROM-less
/// `DELETE t` spellings alike. `None` for the shapes that have no single named relation
/// (`USING`, a derived table), which fall back to `<table>` in the message.
fn delete_target(delete: &Delete) -> Option<&ObjectName> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    object_name_of(tables.first()?)
}

/// The `ObjectName` an `UPDATE` targets — the primary relation of its `TableWithJoins`.
fn update_target(update: &Update) -> Option<&ObjectName> {
    object_name_of(&update.table)
}

/// The plain table name of a `TableWithJoins`' primary relation, if it is one.
fn object_name_of(table: &TableWithJoins) -> Option<&ObjectName> {
    match &table.relation {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

/// True when a `Query` node appears anywhere inside `expr` — i.e. the expression carries a
/// subquery at any depth (`IN (…)`, `NOT IN`, `EXISTS`, `ANY`/`ALL`, a scalar `(SELECT …)`,
/// nested under `NOT` / `OR` / a function argument, or inside another subquery).
fn expression_contains_subquery(expr: &Expr) -> bool {
    struct SawSubquery;
    struct SubqueryProbe;
    impl Visitor for SubqueryProbe {
        type Break = SawSubquery;

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            ControlFlow::Break(SawSubquery)
        }
    }
    expr.visit(&mut SubqueryProbe).is_break()
}

/// The G3-E8 refusal text. Byte-identical to the Spark door's (the parity corpus asserts the
/// needle `subquery predicates are silently mis-executed` through either door).
fn dml_subquery_refusal_message(verb: DmlSubqueryVerb, table: &str) -> String {
    format!(
        "{verb_name} with a subquery in its WHERE clause is refused on `{table}`: subquery \
         predicates are silently mis-executed today — DataFusion's DML planner decorrelates \
         IN / NOT IN / EXISTS / ANY / ALL / correlated predicates into a semi-join and then \
         recovers NO filter for the Iceberg writer, so this statement would \
         {consequence} instead of only the matching ones (defect G3-E8, silent data loss). \
         Rewrite it as `MERGE INTO {table} AS target USING (<the subquery>) AS source \
         ON <join keys> WHEN MATCHED THEN {action}` — the RePark-owned MERGE executor never \
         crosses that seam, and it is the dbt adapter's proven vehicle. Support returns when the \
         underlying fix lands; non-subquery {verb_name} predicates are unaffected.",
        verb_name = verb.verb(),
        consequence = verb.consequence(),
        action = verb.merge_action(),
    )
}

// === Guard — G15 collation refuse (parse altitude) ==========================================

/// ===========================================================================================
/// Render the G15 refusal. Byte-identical to the Spark door's message (same needles).
/// ===========================================================================================
pub(crate) fn collation_refusal_message(requested: &str) -> String {
    format!(
        "repark {COLLATION_REFUSAL_NEEDLE}: requested `{requested}`. Spark 4 would apply \
         that collation to comparisons and ORDER BY; repark refuses rather than silently \
         ignore it. Use binary/default ordering — omit COLLATE, keep StringType() / \
         UTF8_BINARY, and do not set a session collation."
    )
}

/// ===========================================================================================
/// Refuse a collation spelling on the router's parsed statement (G3-E8 altitude).
/// ===========================================================================================
///
/// Called immediately after the stock parse, before the statement match, so SELECT
/// COLLATE, ORDER BY COLLATE, CREATE TABLE column COLLATE, SET collation, and
/// CREATE/ALTER COLLATION all refuse on the parse every route agrees on.
///
/// # Errors
/// [`DataFusionError::NotImplemented`] naming the requested collation.
pub(crate) fn refuse_collation_in_statement(statement: &Statement) -> Result<()> {
    let mut probe = CollationProbe { requested: None };
    if statement.visit(&mut probe).is_break()
        && let Some(requested) = probe.requested
    {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            &requested,
        )));
    }
    Ok(())
}

struct CollationProbe {
    requested: Option<String>,
}

impl Visitor for CollationProbe {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Collate { collation, .. } = expr {
            self.requested = Some(collation.to_string());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if let Some(requested) = collation_requested_by_statement(statement) {
            self.requested = Some(requested);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn collation_requested_by_statement(statement: &Statement) -> Option<String> {
    match statement {
        Statement::CreateTable(create) => {
            if let Some(name) = &create.default_ddl_collation {
                return Some(name.clone());
            }
            first_column_collation(&create.columns)
        }
        Statement::CreateSchema {
            default_collate_spec: Some(spec),
            ..
        } => Some(spec.to_string()),
        Statement::CreateDatabase {
            default_collation,
            default_ddl_collation,
            ..
        } => default_collation
            .clone()
            .or_else(|| default_ddl_collation.clone()),
        Statement::CreateCollation(create) => Some(create.name.to_string()),
        Statement::AlterCollation(alter) => Some(alter.name.to_string()),
        Statement::AlterSchema(alter) => {
            for operation in &alter.operations {
                if let AlterSchemaOperation::SetDefaultCollate { collate } = operation {
                    return Some(collate.to_string());
                }
            }
            None
        }
        Statement::AlterTable(alter) => {
            for operation in &alter.operations {
                if let AlterTableOperation::AddColumn { column_def, .. } = operation
                    && let Some(name) = first_column_collation(std::slice::from_ref(column_def))
                {
                    return Some(name);
                }
            }
            None
        }
        Statement::Set(set) => collation_requested_by_set(set),
        _ => None,
    }
}

fn first_column_collation(
    columns: &[datafusion::sql::sqlparser::ast::ColumnDef],
) -> Option<String> {
    for column in columns {
        for option in &column.options {
            if let ColumnOption::Collation(name) = &option.option {
                return Some(name.to_string());
            }
        }
    }
    None
}

fn collation_requested_by_set(set: &Set) -> Option<String> {
    match set {
        Set::SetNames {
            collation_name: Some(name),
            ..
        } => Some(name.clone()),
        Set::SingleAssignment { variable, .. } => {
            let key = variable.to_string();
            key.to_ascii_lowercase()
                .contains("collation")
                .then_some(key)
        }
        Set::MultipleAssignments { assignments } => {
            for assignment in assignments {
                let key = assignment.name.to_string();
                if key.to_ascii_lowercase().contains("collation") {
                    return Some(key);
                }
            }
            None
        }
        Set::ParenthesizedAssignments { variables, .. } => {
            for variable in variables {
                let key = variable.to_string();
                if key.to_ascii_lowercase().contains("collation") {
                    return Some(key);
                }
            }
            None
        }
        _ => None,
    }
}

/// ===========================================================================================
/// Refuse type-position `STRING COLLATE name` that sqlparser's CAST cannot attach.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::NotImplemented`] when a type-position collation is present.
pub(crate) fn refuse_type_position_collation_in_sql(sql: &str) -> Result<()> {
    if let Some(requested) = type_position_collation(sql) {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            &requested,
        )));
    }
    Ok(())
}

/// ===========================================================================================
/// Refuse `RESET` of a collation session key (DataFusion extension, not `Statement::Set`).
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::NotImplemented`] when the variable name contains `collation`.
pub(crate) fn refuse_collation_reset_variable(variable: &str) -> Result<()> {
    if variable.to_ascii_lowercase().contains("collation") {
        return Err(DataFusionError::NotImplemented(collation_refusal_message(
            variable,
        )));
    }
    Ok(())
}

fn type_position_collation(sql: &str) -> Option<String> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    let lower = scrubbed.to_ascii_lowercase();
    let mut from = 0;
    while let Some(relative) = lower[from..].find("collate") {
        let at = from + relative;
        if !is_word_boundary(&lower, at, at + 7) || !preceded_by_string_type(&lower, at) {
            from = at + 7;
            continue;
        }
        return collation_ident_after(&scrubbed, at + 7);
    }
    None
}

fn preceded_by_string_type(lower: &str, collate_at: usize) -> bool {
    let before = strip_trailing_length_spec(lower[..collate_at].trim_end());
    for token in ["string", "varchar", "char", "text"] {
        if before.ends_with(token)
            && is_word_boundary(before, before.len() - token.len(), before.len())
        {
            return true;
        }
    }
    false
}

fn strip_trailing_length_spec(text: &str) -> &str {
    let trimmed = text.trim_end();
    if !trimmed.ends_with(')') {
        return trimmed;
    }
    let Some(open) = trimmed.rfind('(') else {
        return trimmed;
    };
    let inner = trimmed[open + 1..trimmed.len() - 1].trim();
    if inner
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte.is_ascii_whitespace())
    {
        return trimmed[..open].trim_end();
    }
    trimmed
}

fn collation_ident_after(sql: &str, after_collate: usize) -> Option<String> {
    let tail = sql[after_collate..].trim_start();
    let mut end = 0;
    for (index, character) in tail.char_indices() {
        if character.is_ascii_alphanumeric() || character == '_' || character == '.' {
            end = index + character.len_utf8();
            continue;
        }
        break;
    }
    (end > 0).then(|| tail[..end].to_string())
}

fn is_word_boundary(text: &str, start: usize, end: usize) -> bool {
    let bytes = text.as_bytes();
    let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
    let after_ok = end >= bytes.len() || !is_ident_byte(bytes[end]);
    before_ok && after_ok
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests;
