use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::Schema;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::write::append::iceberg_err;

pub const ACCEPT_ANY_SCHEMA_PROP: &str = "write.spark.accept-any-schema";

#[must_use]
pub fn accepts_any_schema(table: &Table) -> bool {
    table
        .metadata()
        .properties()
        .get(ACCEPT_ANY_SCHEMA_PROP)
        .is_some_and(|raw| raw.trim().eq_ignore_ascii_case("true"))
}

#[allow(clippy::missing_errors_doc)]
pub fn incoming_schema(arrow: &ArrowSchema) -> Result<Schema> {
    arrow_schema_to_schema_auto_assign_ids(arrow).map_err(iceberg_err)
}

#[allow(clippy::missing_errors_doc)]
pub async fn evolve_schema(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    incoming: Schema,
) -> Result<Table> {
    let tx = Transaction::new(table);
    let action = tx
        .update_schema()
        .case_sensitive(false)
        .union_by_name_with(incoming);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(catalog.as_ref()).await.map_err(iceberg_err)
}

#[cfg(test)]
mod tests;
