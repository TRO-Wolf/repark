use iceberg::spec::{NestedFieldRef, PrimitiveType, Transform, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result, TableIdent};

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
    ReplaceFieldByTransform {
        old_source_name: String,
        old_transform: Transform,
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
    let schema = table.metadata().current_schema();
    for source_name in changes.iter().filter_map(PartitionSpecChange::bound_source) {
        if schema.field_by_name(source_name).is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "org.apache.iceberg.exceptions.ValidationException: Cannot find field \
                     '{source_name}' in struct: {}",
                    java_struct_text(schema.as_struct().fields())
                ),
            ));
        }
    }
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
                note_new_field_name(&mut known_field_names, &resolved_old, new_name.as_deref());
                action.remove_field(&resolved_old).add_field_with_transform(
                    new_name.as_deref(),
                    source_name,
                    *transform,
                )
            }
            PartitionSpecChange::ReplaceFieldByTransform {
                old_source_name,
                old_transform,
                source_name,
                transform,
                new_name,
            } => {
                let resolved_old =
                    resolve_field_by_transform(&table, old_source_name, *old_transform)?;
                note_new_field_name(&mut known_field_names, &resolved_old, new_name.as_deref());
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

impl PartitionSpecChange {
    fn bound_source(&self) -> Option<&str> {
        match self {
            PartitionSpecChange::AddField { source_name, .. }
            | PartitionSpecChange::ReplaceField { source_name, .. }
            | PartitionSpecChange::ReplaceFieldByTransform { source_name, .. } => {
                Some(source_name.as_str())
            }
            PartitionSpecChange::RemoveFieldByName { .. }
            | PartitionSpecChange::RemoveFieldByTransform { .. }
            | PartitionSpecChange::RenameField { .. } => None,
        }
    }
}

fn java_struct_text(fields: &[NestedFieldRef]) -> String {
    let fields = fields
        .iter()
        .map(|field| {
            let required = if field.required {
                "required"
            } else {
                "optional"
            };
            let doc = field
                .doc
                .as_ref()
                .map(|doc| format!(" ({doc})"))
                .unwrap_or_default();
            format!(
                "{}: {}: {required} {}{doc}",
                field.id,
                field.name,
                java_type_text(&field.field_type)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("struct<{fields}>")
}

fn java_type_text(data_type: &Type) -> String {
    match data_type {
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            format!("decimal({precision}, {scale})")
        }
        Type::Primitive(primitive) => primitive.to_string(),
        Type::Struct(struct_type) => java_struct_text(struct_type.fields()),
        Type::List(list) => format!("list<{}>", java_type_text(&list.element_field.field_type)),
        Type::Map(map) => format!(
            "map<{}, {}>",
            java_type_text(&map.key_field.field_type),
            java_type_text(&map.value_field.field_type)
        ),
        Type::Variant => String::from("variant"),
    }
}

fn forget_field_name(known_field_names: &mut Vec<String>, name: &str) {
    known_field_names.retain(|existing| !existing.eq_ignore_ascii_case(name));
}

fn note_new_field_name(
    known_field_names: &mut Vec<String>,
    resolved_old: &str,
    new_name: Option<&str>,
) {
    forget_field_name(known_field_names, resolved_old);
    if let Some(explicit_name) = new_name {
        forget_field_name(known_field_names, explicit_name);
        known_field_names.push(explicit_name.to_string());
    }
}

fn resolve_field_by_transform(
    table: &Table,
    source_name: &str,
    transform: Transform,
) -> Result<String> {
    let metadata = table.metadata();
    let source_id = metadata
        .current_schema()
        .field_by_name_case_insensitive(source_name)
        .map(|field| field.id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot find source column in schema: {source_name}"),
            )
        })?;
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .find(|field| field.source_id == source_id && field.transform == transform)
        .map(|field| field.name.clone())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "REPLACE PARTITION FIELD transform `{transform}` on `{source_name}` matches \
                     no partition field in the current spec"
                ),
            )
        })
}

#[cfg(test)]
mod tests;
