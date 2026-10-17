use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use super::unsupported_error;

pub const ENCRYPTION_KEY_ID_PROPERTY: &str = "encryption.key-id";

const REFUSAL_HEAD: &str = "Table ";
const REFUSAL_TAIL: &str = " carries property 'encryption.key-id': RePark has no table encryption \
                            and refuses to write plaintext into a table that asks for it (ENC-1).";
const MAX_SOURCE_DEPTH: usize = 32;

#[must_use]
pub fn refusal_text(table: &str) -> String {
    format!("{REFUSAL_HEAD}{table}{REFUSAL_TAIL}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedTableRefusal {
    table: String,
}

impl EncryptedTableRefusal {
    #[must_use]
    pub fn of(ident: &TableIdent) -> Self {
        Self {
            table: format!("{}.{}", ident.namespace(), ident.name()),
        }
    }

    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    #[must_use]
    pub fn in_error(error: &(dyn std::error::Error + 'static)) -> Option<Self> {
        let mut current = Some(error);
        for _ in 0..MAX_SOURCE_DEPTH {
            let link = current?;
            if let Some(found) = link.downcast_ref::<Self>() {
                return Some(found.clone());
            }
            current = link.source();
        }
        None
    }

    #[must_use]
    pub fn in_text(text: &str) -> Option<Self> {
        let tail = text.find(REFUSAL_TAIL)?;
        let head = text[..tail].rfind(REFUSAL_HEAD)?;
        Some(Self {
            table: text[head + REFUSAL_HEAD.len()..tail].to_string(),
        })
    }

    #[must_use]
    pub fn find(error: &(dyn std::error::Error + 'static)) -> Option<Self> {
        Self::in_error(error).or_else(|| Self::in_text(&error.to_string()))
    }

    #[must_use]
    pub fn into_iceberg(self) -> iceberg::Error {
        iceberg::Error::new(iceberg::ErrorKind::FeatureUnsupported, self.to_string())
            .with_retryable(false)
            .with_source(self)
    }

    #[must_use]
    pub fn into_datafusion(self) -> DataFusionError {
        unsupported_error(self.to_string())
    }
}

impl std::fmt::Display for EncryptedTableRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&refusal_text(&self.table))
    }
}

impl std::error::Error for EncryptedTableRefusal {}

#[must_use]
pub fn carries_encryption_key<S: std::hash::BuildHasher>(
    properties: &HashMap<String, String, S>,
) -> bool {
    properties.contains_key(ENCRYPTION_KEY_ID_PROPERTY)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_encrypted_properties<S: std::hash::BuildHasher>(
    properties: &HashMap<String, String, S>,
    ident: &TableIdent,
) -> Result<()> {
    if carries_encryption_key(properties) {
        return Err(EncryptedTableRefusal::of(ident).into_datafusion());
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_encrypted_table(table: &Table) -> Result<()> {
    refuse_encrypted_properties(table.metadata().properties(), table.identifier())
}

#[allow(clippy::missing_errors_doc)]
pub async fn refuse_encrypted_write(catalog: &dyn Catalog, ident: &TableIdent) -> Result<()> {
    let Ok(table) = catalog.load_table(ident).await else {
        return Ok(());
    };
    refuse_encrypted_table(&table)
}

#[must_use]
pub fn normalize_encrypted_refusal(error: DataFusionError) -> DataFusionError {
    match EncryptedTableRefusal::find(&error) {
        Some(refusal) => refusal.into_datafusion(),
        None => error,
    }
}
