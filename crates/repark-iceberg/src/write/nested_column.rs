use iceberg::spec::Type;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result};

use super::alter::ColumnPosition;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnPathChange {
    Add {
        parent: Option<String>,
        name: String,
        field_type: Type,
        doc: Option<String>,
        required: bool,
        position: Option<ColumnPosition>,
    },
    Rename {
        path: String,
        to: String,
    },
    Drop {
        path: String,
    },
}

fn full_name(parent: Option<&str>, name: &str) -> String {
    parent.map_or_else(|| name.to_string(), |parent| format!("{parent}.{name}"))
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: action-apply and commit errors propagate from the fork, recorded in the unit ledger"
)]
pub async fn apply_column_path_changes(
    catalog: &dyn Catalog,
    table: &Table,
    changes: &[ColumnPathChange],
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let tx = Transaction::new(table);
    let mut action = tx.update_schema().case_sensitive(false);
    for change in changes {
        action = match change {
            ColumnPathChange::Add {
                parent,
                name,
                field_type,
                doc,
                required,
                position,
            } => {
                let parent = parent.as_deref();
                let added = if *required {
                    action.add_required_column_to(parent, name, field_type.clone(), doc.as_deref())
                } else {
                    action.add_column_to(parent, name, field_type.clone(), doc.as_deref())
                };
                let moved = full_name(parent, name);
                match position {
                    Some(ColumnPosition::First) => added.move_first(&moved),
                    Some(ColumnPosition::After(reference)) => {
                        added.move_after(&moved, &full_name(parent, reference))
                    }
                    None => added,
                }
            }
            ColumnPathChange::Rename { path, to } => action.rename_column(path, to),
            ColumnPathChange::Drop { path } => action.delete_column(path),
        };
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema, StructType, Type};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    async fn nested_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.path().to_str().unwrap().to_string(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("sales".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let int = || Type::Primitive(PrimitiveType::Int);
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", int()).into(),
                NestedField::optional(
                    2,
                    "s",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(3, "a", int()).into(),
                        NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    5,
                    "arrs",
                    Type::List(ListType::new(
                        NestedField::optional(
                            6,
                            "element",
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(7, "x", int()).into(),
                            ])),
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog.create_table(&namespace, creation).await.unwrap();
        (catalog, TableIdent::new(namespace, "t".to_string()))
    }

    fn add(parent: &str, name: &str, required: bool) -> ColumnPathChange {
        ColumnPathChange::Add {
            parent: Some(parent.to_string()),
            name: name.to_string(),
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            required,
            position: None,
        }
    }

    #[tokio::test]
    async fn column_path_changes_evolve_nested_children_by_field_id() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) = nested_table(&warehouse).await;
        let table = catalog.load_table(&ident).await.unwrap();
        let child_id = table
            .metadata()
            .current_schema()
            .field_by_name("s.a")
            .unwrap()
            .id;
        let changes = [
            add("s", "c", false),
            add("arrs.element", "y", false),
            ColumnPathChange::Rename {
                path: "s.a".to_string(),
                to: "a2".to_string(),
            },
            ColumnPathChange::Drop {
                path: "s.b".to_string(),
            },
        ];
        apply_column_path_changes(catalog.as_ref(), &table, &changes)
            .await
            .unwrap();
        let schema = catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .current_schema()
            .clone();
        assert_eq!(schema.field_by_name("s.a2").unwrap().id, child_id);
        assert!(schema.field_by_name("s.b").is_none());
        assert!(!schema.field_by_name("s.c").unwrap().required);
        assert!(schema.field_by_name("arrs.element.y").is_some());
    }

    #[tokio::test]
    async fn a_required_nested_child_without_a_default_refuses() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) = nested_table(&warehouse).await;
        let table = catalog.load_table(&ident).await.unwrap();
        let refused = apply_column_path_changes(catalog.as_ref(), &table, &[add("s", "r", true)])
            .await
            .expect_err("a required add without a default is incompatible");
        assert!(
            refused
                .to_string()
                .contains("Incompatible change: cannot add required column"),
            "{refused}"
        );
        let after = catalog.load_table(&ident).await.unwrap();
        assert_eq!(
            after.metadata().current_schema_id(),
            table.metadata().current_schema_id()
        );
    }
}
