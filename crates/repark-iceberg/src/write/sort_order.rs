use iceberg::spec::{
    NullOrder, Schema as IcebergSchema, SortDirection, StructType, Transform, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result, TableIdent};

pub const DISTRIBUTION_MODE_PROPERTY: &str = "write.distribution-mode";

pub struct WriteSortField {
    pub name: String,
    pub transform: Transform,
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
    let mut resolved = Vec::with_capacity(fields.len());
    for field in fields {
        resolved.push((resolve_sort_field(schema, &field.name)?, field));
    }
    let tx = Transaction::new(&table);
    let mut action = tx.replace_sort_order();
    for (name, field) in &resolved {
        action = action.sort_by(name, field.transform, field.direction, field.null_order);
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

fn resolve_sort_field(schema: &IcebergSchema, name: &str) -> Result<String> {
    let mut scope: &StructType = schema.as_struct();
    let mut canonical: Vec<&str> = Vec::new();
    let segments: Vec<&str> = name.split('.').collect();
    for (depth, segment) in segments.iter().enumerate() {
        let needle = segment.to_ascii_lowercase();
        let found = scope
            .fields()
            .iter()
            .find(|existing| existing.name.to_ascii_lowercase() == needle)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {name} in table schema"),
                )
            })?;
        canonical.push(found.name.as_str());
        if depth + 1 < segments.len() {
            let Type::Struct(inner) = found.field_type.as_ref() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {name} in table schema"),
                ));
            };
            scope = inner;
        }
    }
    Ok(canonical.join("."))
}
