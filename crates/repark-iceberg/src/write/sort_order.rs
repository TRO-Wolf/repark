use iceberg::spec::{NullOrder, SortDirection};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result, TableIdent};

pub const DISTRIBUTION_MODE_PROPERTY: &str = "write.distribution-mode";

pub struct WriteSortField {
    pub name: String,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

/// # Errors
/// An unknown column, or a catalog commit failure, surfaces here.
pub async fn apply_write_order(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    fields: &[WriteSortField],
    distribution_mode: Option<&str>,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let schema = table.metadata().current_schema();
    let mut resolved: Vec<(String, SortDirection, NullOrder)> = Vec::with_capacity(fields.len());
    for field in fields {
        let needle = field.name.to_ascii_lowercase();
        let canonical = schema
            .as_struct()
            .fields()
            .iter()
            .find(|existing| existing.name.to_ascii_lowercase() == needle)
            .map(|existing| existing.name.clone())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {} in table schema", field.name),
                )
            })?;
        resolved.push((canonical, field.direction, field.null_order));
    }
    let tx = Transaction::new(&table);
    let mut action = tx.replace_sort_order();
    for (name, direction, null_order) in &resolved {
        action = match direction {
            SortDirection::Ascending => action.asc(name, *null_order),
            SortDirection::Descending => action.desc(name, *null_order),
        };
    }
    let mut tx = action.apply(tx)?;
    if let Some(mode) = distribution_mode {
        tx = tx
            .update_table_properties()
            .set(DISTRIBUTION_MODE_PROPERTY.to_string(), mode.to_string())
            .apply(tx)?;
    }
    tx.commit(catalog).await?;
    Ok(())
}
