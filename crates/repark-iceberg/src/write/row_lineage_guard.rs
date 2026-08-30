//! Format-v3 row-DML guard — registry `V3-COW-1`.

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{FormatVersion, Operation};
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
    let spark_lineage = if verb.eq_ignore_ascii_case("UPDATE") {
        "every existing row keeps its `_row_id` and only the changed row bumps \
         `_last_updated_sequence_number`"
    } else {
        "matched rows keep their ids and only inserts take a new id"
    };
    Err(DataFusionError::NotImplemented(format!(
        "copy-on-write {verb} will not run on `{ident}`: it is a {format_version:?} table. Spark \
         4.1.2 + Iceberg 1.11.0 preserves row lineage (`_row_id`) across {verb}: {spark_lineage}. \
         This engine's row rewrite reassigns every row in a rewritten file. Registry V3-COW-1"
    )))
}

/// Passthrough seat for plain-WHERE DELETE/UPDATE both doors delegate to the fork provider.
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
            "UPDATE will not run on `{table_ident}`: it is a {format_version:?} table. Spark \
             4.1.2 + Iceberg 1.11.0 preserves row lineage (`_row_id`) across UPDATE (the 3-row \
             recipe keeps 0/1/2 and only bumps sequence on the changed row). This engine's \
             copy-on-write rewrite reassigns every survivor; merge-on-read reassigns the \
             updated row. Registry V3-COW-1",
        )));
    }
    if v3_cow_delete_after_overwrite_snapshot(&table, kind) {
        let table_ident = table.identifier();
        return Err(DataFusionError::NotImplemented(format!(
            "DELETE will not run on `{table_ident}`: its current v3 snapshot is an overwrite, and \
             the fork's next copy-on-write DELETE reassigns row lineage. Spark keeps next-row-id \
             unchanged and writes zero added rows after the same second DELETE. This guard refuses \
             before a write until iceberg-datafusion preserves that lineage (V3-COW-1)",
        )));
    }
    Ok(())
}

fn v3_cow_delete_after_overwrite_snapshot(table: &Table, kind: MorDmlKind) -> bool {
    matches!(kind, MorDmlKind::Delete)
        && !table
            .metadata()
            .properties()
            .get("write.delete.mode")
            .is_some_and(|mode| mode.trim().eq_ignore_ascii_case("merge-on-read"))
        && table
            .metadata()
            .current_snapshot()
            .is_some_and(|snapshot| snapshot.summary().operation == Operation::Overwrite)
}
