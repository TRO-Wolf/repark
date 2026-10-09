use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use super::unsupported_error;

pub const ENCRYPTION_KEY_ID_PROPERTY: &str = "encryption.key-id";

fn refusal(table_sql: &str) -> DataFusionError {
    unsupported_error(format!(
        "Table {table_sql} carries property 'encryption.key-id': RePark has no table encryption \
         and refuses to write plaintext into a table that asks for it (ENC-1)."
    ))
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_encrypted_properties<S: std::hash::BuildHasher>(
    properties: &HashMap<String, String, S>,
    table_sql: &str,
) -> Result<()> {
    if properties.contains_key(ENCRYPTION_KEY_ID_PROPERTY) {
        return Err(refusal(table_sql));
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_encrypted_table(table: &Table) -> Result<()> {
    let ident = table.identifier();
    let display = format!("{}.{}", ident.namespace(), ident.name());
    refuse_encrypted_properties(table.metadata().properties(), &display)
}

#[allow(clippy::missing_errors_doc)]
pub async fn refuse_encrypted_write(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    table_sql: &str,
) -> Result<()> {
    let Ok(table) = catalog.load_table(ident).await else {
        return Ok(());
    };
    refuse_encrypted_properties(table.metadata().properties(), table_sql)
}
