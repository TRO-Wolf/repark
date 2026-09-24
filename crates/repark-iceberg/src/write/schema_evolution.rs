use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Error as IcebergError, ErrorKind};

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
    let tx = action.apply(tx).map_err(type_change_err)?;
    tx.commit(catalog.as_ref()).await.map_err(type_change_err)
}

#[allow(clippy::missing_errors_doc)]
pub async fn evolve_merge_schema(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    incoming: Schema,
) -> Result<Table> {
    let current = table.metadata().current_schema();
    let tx = Transaction::new(table);
    let mut action = tx.update_schema().case_sensitive(false);
    for field in incoming.as_struct().fields() {
        let Type::Primitive(source_type) = field.field_type.as_ref() else {
            continue;
        };
        let Some(target) = current.field_by_name_case_insensitive(&field.name) else {
            continue;
        };
        if matches!(target.field_type.as_ref(), Type::Primitive(target_type) if target_type != source_type)
        {
            action = action.update_column(&target.name, source_type.clone());
        }
    }
    let tx = action
        .union_by_name_with(incoming)
        .apply(tx)
        .map_err(type_change_err)?;
    tx.commit(catalog.as_ref()).await.map_err(type_change_err)
}

fn type_change_err(err: IcebergError) -> datafusion::error::DataFusionError {
    if err.kind() == ErrorKind::DataInvalid
        && err.message().starts_with("Cannot change column type:")
    {
        return crate::write::illegal_argument_error(err.message().to_string());
    }
    iceberg_err(err)
}

#[cfg(test)]
mod tests;
