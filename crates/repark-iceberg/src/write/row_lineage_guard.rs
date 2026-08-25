//! The format-v3 copy-on-write DML guard — registry row `V3-COW-1` (owner ruling 2026-08-25).
//!
//! V3E-1 measured that copy-on-write `DELETE` / `UPDATE` / `MERGE INTO` on a format-v3 table
//! commit the correct rows while **reassigning** row lineage: every survivor rewritten into a
//! new data file takes a fresh `_row_id`, and the snapshot's `first_row_id` / `added_rows`
//! count the survivors as new rows. Spark preserves `_row_id` /
//! `_last_updated_sequence_number` across the same statement. The rows are never wrong, which is
//! what makes the failure quiet: every downstream incremental consumer is told that all rows
//! changed when only the matched ones did.
//!
//! The owner ruled the path guarded, the trade `V3-LINEAGE-1` already took for
//! `rewrite_data_files`: a loud stop rather than a plausible wrong answer, stricter than Spark
//! on purpose, reversible in one line once the fork carries lineage through a row rewrite
//! (handoff F-7). The merge-on-read arms refuse format v3 independently (row R113: v3 mandates
//! deletion vectors), so until then **a v3 table is append-only in this engine** — `INSERT`
//! carries lineage correctly and stays open.
//!
//! The guard runs at write-mode resolution, before any data file is written, so a refusal can
//! never orphan a Parquet file. Whole-file deletes (every row of a file matched) would have been
//! lineage-safe; they are refused with the rest — fail-closed is the cheaper mistake, and the
//! lift is one line.

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use crate::write::position_delete::MorDmlKind;

/// Refuse copy-on-write `verb` on a format-v3 (or later) table, naming registry row `V3-COW-1`,
/// the verb, and row lineage. Format v1/v2 tables pass untouched.
///
/// The comparison is `< V3`, so a format version *above* v3 refuses too — fail-closed is the
/// right default for a version whose lineage rules are not known yet.
pub(crate) fn refuse_v3_cow_dml_that_would_reassign_row_lineage(
    table: &Table,
    verb: &str,
) -> Result<()> {
    let format_version = table.metadata().format_version();
    if format_version < FormatVersion::V3 {
        return Ok(());
    }
    let ident = table.identifier();
    Err(DataFusionError::NotImplemented(format!(
        "copy-on-write {verb} will not run on `{ident}`: it is a {format_version:?} table, and \
         V3 onward mandates row lineage (`_row_id`, `_last_updated_sequence_number`) which this \
         engine's row rewrite does not carry through. The rows would be correct and every \
         surviving row in a rewritten file would take a new `_row_id`, telling downstream \
         consumers that all of them changed. Spark preserves lineage across the same statement \
         — run it there until the fork carries lineage through a rewrite (registry row \
         V3-COW-1; the merge-on-read arm refuses format v3 too, so a v3 table is append-only \
         in this engine for now)"
    )))
}

/// The passthrough-path seat of the same guard. A plain-`WHERE` `DELETE` / `UPDATE` never
/// reaches [`crate::write::predicate_dml`] (that path is the subquery-`WHERE` form): both SQL
/// doors delegate it to DataFusion, which plans it onto the fork's `TableProvider`, so the
/// write-mode resolver above is never consulted. Each door calls this beside its BUG-001
/// merge-on-read valve, before delegation. A missing table passes (the planner's own error is
/// the better one); a merge-on-read table passes (the merge-on-read arm owns v3 refusal — row
/// R113); everything else is the copy-on-write arm and refuses on v3.
///
/// # Errors
/// [`DataFusionError::NotImplemented`] from
/// [`refuse_v3_cow_dml_that_would_reassign_row_lineage`].
pub async fn refuse_v3_cow_dml(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: MorDmlKind,
) -> Result<()> {
    let Ok(table) = catalog.load_table(ident).await else {
        return Ok(());
    };
    let is_merge_on_read = table
        .metadata()
        .properties()
        .get(kind.mode_property())
        .is_some_and(|mode| mode.trim().eq_ignore_ascii_case("merge-on-read"));
    if is_merge_on_read {
        return Ok(());
    }
    refuse_v3_cow_dml_that_would_reassign_row_lineage(&table, kind.verb())
}
