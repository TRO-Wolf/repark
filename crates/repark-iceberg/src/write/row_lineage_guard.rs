//! Format-v3 row-DML guard — registry `V3-COW-1`. Plain-`WHERE` DELETE on v3, including on a
//! table that already carries deletion vectors, runs at fork `d408da42` (RP-3 / F-17). UPDATE
//! on v3 still refuses and waits for V3-3.

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
         V3-COW-1; the plain-`WHERE` DELETE on a v3 table with no live deletion vectors runs \
         since RP-2, every other row-DML form refuses)"
    )))
}

/// Passthrough seat for the plain-`WHERE` DELETE / UPDATE both doors delegate to the fork's
/// `TableProvider` (never reaching `predicate_dml`). A missing table passes. `DELETE` on v3
/// passes, including when live deletion vectors are present (RP-3 / F-17 at `d408da42`).
/// `UPDATE` on v3 refuses (V3-3) — without consulting `write.<verb>.mode` (SEC-002).
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
    if matches!(kind, MorDmlKind::Update) {
        let table_ident = table.identifier();
        return Err(DataFusionError::NotImplemented(format!(
            "UPDATE will not run on `{table_ident}`: it is a {format_version:?} table, and V3 \
             onward mandates row lineage (`_row_id`, `_last_updated_sequence_number`) which \
             this engine's row rewrite has not been measured to carry through on UPDATE. The \
             plain-`WHERE` DELETE on v3, including on a table that already carries deletion \
             vectors, runs at fork `d408da42` (RP-3 / F-17). UPDATE waits for V3-3 \
             (registry row V3-COW-1)",
        )));
    }
    Ok(())
}
