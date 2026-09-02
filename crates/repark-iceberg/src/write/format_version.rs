use std::collections::HashMap;

use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result, TableIdent};

pub const FORMAT_VERSION_PROPERTY: &str = "format-version";

#[allow(clippy::missing_errors_doc)]
pub async fn set_properties_and_format_version(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    loaded: Option<Table>,
    sets: HashMap<String, String>,
    unsets: &[String],
    target: Option<FormatVersion>,
) -> Result<()> {
    if target.is_none() && sets.is_empty() && unsets.is_empty() {
        return Ok(());
    }
    let table = match loaded {
        Some(table) => table,
        None => catalog.load_table(ident).await?,
    };
    let mut tx = Transaction::new(&table);
    if let Some(version) = target {
        tx = tx
            .upgrade_table_version()
            .set_format_version(version)
            .apply(tx)?;
    }
    if !sets.is_empty() || !unsets.is_empty() {
        let mut action = tx.update_table_properties();
        for (key, value) in sets {
            action = action.set(key, value);
        }
        for key in unsets {
            action = action.remove(key.clone());
        }
        tx = action.apply(tx)?;
    }
    tx.commit(catalog).await?;
    Ok(())
}

#[must_use]
pub fn format_version_number(table: &Table) -> i64 {
    i64::from(table.metadata().format_version() as u8)
}

#[allow(clippy::missing_errors_doc)]
pub fn format_version_from_number(number: i64) -> Result<FormatVersion> {
    match number {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        other => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Iceberg format version {other} is not one this engine writes"),
        )),
    }
}
