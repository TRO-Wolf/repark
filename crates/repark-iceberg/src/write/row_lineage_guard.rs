//! Format-v3 copy-on-write DML guard — registry `V3-COW-1` (owner ruling 2026-08-25). V3E-1
//! measured COW DML on v3 reassigning every survivor's `_row_id` (Spark preserves it); same trade
//! as `V3-LINEAGE-1`: refuse before any write until fork F-7. merge-on-read refuses v3 too: append-only.

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use crate::write::position_delete::MorDmlKind;

/// Refuse copy-on-write `verb` on a v3-or-later table; below v3 passes (above v3 fails closed).
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

/// Passthrough seat for the plain-`WHERE` DELETE / UPDATE both doors delegate to the fork's
/// `TableProvider` (never reaching `predicate_dml`). Every v3 table refuses — never deciding on
/// the mode alone, which the fork parses differently (SEC-002); a missing table passes.
///
/// # Errors
/// [`DataFusionError::NotImplemented`].
pub async fn refuse_v3_cow_dml(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: MorDmlKind,
) -> Result<()> {
    let Ok(table) = catalog.load_table(ident).await else {
        return Ok(());
    };
    let format_version = table.metadata().format_version();
    if format_version < FormatVersion::V3 {
        return Ok(());
    }
    let is_merge_on_read = table
        .metadata()
        .properties()
        .get(kind.mode_property())
        .is_some_and(|mode| mode.trim().eq_ignore_ascii_case("merge-on-read"));
    if is_merge_on_read {
        return Err(DataFusionError::NotImplemented(format!(
            "merge-on-read {verb} will not run on `{ident}`: it is a {format_version:?} table, \
             and V3 mandates Puffin deletion vectors, which this engine does not write (row \
             R113) — a v3 table is append-only in this engine for now (registry row V3-COW-1 \
             covers the copy-on-write arm)",
            verb = kind.verb(),
        )));
    }
    refuse_v3_cow_dml_that_would_reassign_row_lineage(&table, kind.verb())
}
