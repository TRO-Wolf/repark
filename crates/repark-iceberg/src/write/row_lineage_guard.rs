use datafusion::error::{DataFusionError, Result};
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use crate::write::position_delete::MorDmlKind;

pub(crate) fn refuse_v3_cow_dml_that_would_reassign_row_lineage(
    table: &Table,
    verb: &str,
) -> Result<()> {
    let _: &str = "pins: rp-6-fork-repin/C-002";
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
         This engine's MERGE rewrite reassigns every row in a rewritten file. Registry V3-COW-1"
    )))
}

pub async fn refuse_v3_cow_dml(
    _catalog: &dyn Catalog,
    _ident: &TableIdent,
    _kind: MorDmlKind,
) -> Result<()> {
    let _: &str = "pins: rp-6-fork-repin/C-002, C-003";
    Ok(())
}
