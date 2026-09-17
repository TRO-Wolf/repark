use iceberg::spec::Transform;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result, TableIdent};

#[derive(Debug, Clone)]
pub enum PartitionSpecChange {
    AddField {
        source_name: String,
        transform: Transform,
        name: Option<String>,
    },
    RemoveFieldByName {
        name: String,
    },
    RemoveFieldByTransform {
        source_name: String,
        transform: Transform,
    },
    ReplaceField {
        old_name: String,
        source_name: String,
        transform: Transform,
        new_name: Option<String>,
    },
    RenameField {
        name: String,
        new_name: String,
    },
}

#[expect(
    clippy::missing_errors_doc,
    reason = "moved behaviour-identical from alter.rs; its contract prose lives in write/map.md under the round comment ban"
)]
pub async fn apply_partition_spec_changes(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    changes: &[PartitionSpecChange],
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let table = catalog.load_table(ident).await?;
    let mut known_field_names: Vec<String> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    let resolve_field_name = |known: &[String], requested: &str| -> String {
        known
            .iter()
            .find(|name| name.eq_ignore_ascii_case(requested))
            .cloned()
            .unwrap_or_else(|| requested.to_string())
    };
    let forget_field_name = |known: &mut Vec<String>, name: &str| {
        known.retain(|existing| !existing.eq_ignore_ascii_case(name));
    };
    let tx = Transaction::new(&table);
    let mut action = tx.update_partition_spec().case_sensitive(false);
    for change in changes {
        action = match change {
            PartitionSpecChange::AddField {
                source_name,
                transform,
                name,
            } => {
                if let Some(explicit_name) = name {
                    forget_field_name(&mut known_field_names, explicit_name);
                    known_field_names.push(explicit_name.clone());
                }
                action.add_field_with_transform(name.as_deref(), source_name, *transform)
            }
            PartitionSpecChange::RemoveFieldByName { name } => {
                let resolved = resolve_field_name(&known_field_names, name);
                forget_field_name(&mut known_field_names, &resolved);
                action.remove_field(&resolved)
            }
            PartitionSpecChange::RemoveFieldByTransform {
                source_name,
                transform,
            } => action.remove_field_by_transform(source_name, *transform),
            PartitionSpecChange::ReplaceField {
                old_name,
                source_name,
                transform,
                new_name,
            } => {
                let resolved_old = resolve_field_name(&known_field_names, old_name);
                forget_field_name(&mut known_field_names, &resolved_old);
                if let Some(explicit_name) = new_name {
                    forget_field_name(&mut known_field_names, explicit_name);
                    known_field_names.push(explicit_name.clone());
                }
                action.remove_field(&resolved_old).add_field_with_transform(
                    new_name.as_deref(),
                    source_name,
                    *transform,
                )
            }
            PartitionSpecChange::RenameField { name, new_name } => {
                let resolved = resolve_field_name(&known_field_names, name);
                forget_field_name(&mut known_field_names, &resolved);
                known_field_names.push(new_name.clone());
                action.rename_field(&resolved, new_name)
            }
        };
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}
