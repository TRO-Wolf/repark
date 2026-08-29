//! Format-v3 row-DML guard — registry `V3-COW-1`. RP-2 (2026-08-27, fork `ce92a7bf`) measured
//! the passthrough DELETE on v3: on a table with NO live deletion vectors, merge-on-read
//! commits Puffin deletion vectors the PySpark 4.1.2 + Iceberg 1.11.0 oracle reads back, and
//! copy-on-write preserves every survivor's `_row_id` / `_last_updated_sequence_number` — so
//! the plain-`WHERE` DELETE lifts on both modes. On a table that CARRIES deletion vectors the
//! same statement resurrected a DV-deleted row (measured, ANSI pin) — it refuses. `UPDATE` on
//! v3 is not measured here and waits for V3-3.

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{DataContentType, DataFileFormat, FormatVersion};
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

/// Count live deletion vectors (Puffin position deletes) in the table's CURRENT snapshot.
async fn count_live_deletion_vectors(table: &Table) -> Result<usize> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(0);
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut count = 0usize;
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != iceberg::spec::ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        for entry in manifest.entries() {
            if entry.is_alive()
                && entry.data_file().content_type() == DataContentType::PositionDeletes
                && entry.data_file().file_format() == DataFileFormat::Puffin
            {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Passthrough seat for the plain-`WHERE` DELETE / UPDATE both doors delegate to the fork's
/// `TableProvider` (never reaching `predicate_dml`). A missing table passes. `DELETE` on v3
/// passes since RP-2 measured both modes Spark-clean at fork `ce92a7bf` — but only on a table
/// with no live deletion vectors: on a DV-carrying table the same statement resurrected a
/// DV-deleted row (measured), so it refuses. `UPDATE` on v3 refuses (unmeasured, V3-3's to
/// lift) — without consulting `write.<verb>.mode`, which the fork parses on its own terms
/// (SEC-002).
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
            "copy-on-write {verb} will not run on `{table_ident}`: it is a {format_version:?} \
             table, and V3 onward mandates row lineage (`_row_id`, \
             `_last_updated_sequence_number`) which this engine's row rewrite has not been \
             measured to carry through. The rows would be correct and every surviving row in a \
             rewritten file would take a new `_row_id`, telling downstream consumers that all \
             of them changed. The plain-`WHERE` DELETE on v3 was measured Spark-clean on \
             2026-08-27 (fork `ce92a7bf`) and runs; the UPDATE waits for the same measurement \
             (registry row V3-COW-1; engine unit V3-3)",
            verb = kind.verb(),
        )));
    }
    let live_dvs = count_live_deletion_vectors(&table).await?;
    if live_dvs > 0 {
        let table_ident = table.identifier();
        return Err(DataFusionError::NotImplemented(format!(
            "DELETE will not run on `{table_ident}`: it is a {format_version:?} table carrying \
             {live_dvs} live deletion vector(s), and a DELETE on a v3 table with deletion \
             vectors resurrected a DV-deleted row when measured on 2026-08-27 (fork \
             `ce92a7bf`; registry row V3-COW-1). Run the delete where the vectors were \
             written, or wait for RP-3 — fork F-17 landed the shared-Puffin closure on \
             2026-08-28"
        )));
    }
    Ok(())
}
