use std::collections::HashMap;
use std::hash::BuildHasher;

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result, TableIdent};

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: property load and commit errors propagate from the fork, recorded in the unit ledger"
)]
pub async fn set_table_properties<S: BuildHasher>(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    properties: &HashMap<String, String, S>,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for (key, value) in properties {
        action = action.set(key.clone(), value.clone());
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: property load and commit errors propagate from the fork, recorded in the unit ledger"
)]
pub async fn unset_table_properties(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    keys: &[String],
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for key in keys {
        action = action.remove(key.clone());
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: the fork rename error propagates unchanged, recorded in the unit ledger"
)]
pub async fn rename_table(
    catalog: &dyn Catalog,
    src: &TableIdent,
    dest: &TableIdent,
) -> Result<()> {
    catalog.rename_table(src, dest).await
}
