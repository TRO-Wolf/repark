use iceberg::spec::Schema;
use iceberg::{Error, ErrorKind, Result};

use super::alter::{ColumnPosition, SchemaChange};

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: the Err message shape is the Spark UNRESOLVED_COLUMN framing recorded in the unit ledger"
)]
pub fn resolve_move_names(
    schema: &Schema,
    added_names: &[String],
    name: &str,
    position: &ColumnPosition,
) -> std::result::Result<(String, ColumnPosition), String> {
    if !name_known(schema, added_names, name) {
        return Err(unresolved_column(name, &top_level_names(schema)));
    }
    match position {
        ColumnPosition::First => Ok((name.to_string(), ColumnPosition::First)),
        ColumnPosition::After(reference) => {
            let qualified = match name.rfind('.') {
                Some(at) => format!("{}.{reference}", &name[..at]),
                None => reference.clone(),
            };
            if !name_known(schema, added_names, &qualified) {
                return Err(unresolved_column(&qualified, &top_level_names(schema)));
            }
            Ok((name.to_string(), ColumnPosition::After(qualified)))
        }
    }
}

#[must_use]
pub fn starts_with_alter(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0C => index += 1,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            _ => break,
        }
    }
    let tail = &bytes[index.min(bytes.len())..];
    tail.len() >= 5
        && tail[..5].eq_ignore_ascii_case(b"ALTER")
        && tail
            .get(5)
            .is_none_or(|next| !next.is_ascii_alphanumeric() && *next != b'_')
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: batch resolution failures carry the Spark UNRESOLVED_COLUMN text recorded in the unit ledger"
)]
pub fn resolve_batch_move_names(
    schema: &Schema,
    changes: &[SchemaChange],
) -> Result<Vec<SchemaChange>> {
    let mut added: Vec<String> = Vec::new();
    for change in changes {
        if let SchemaChange::AddColumn { name, .. } = change {
            added.push(name.clone());
        }
    }
    let mut prepared: Vec<SchemaChange> = Vec::with_capacity(changes.len());
    for change in changes {
        match change {
            SchemaChange::MoveColumn { name, position } => {
                let (mover, at) = resolve_move_names(schema, &added, name, position)
                    .map_err(|message| Error::new(ErrorKind::DataInvalid, message))?;
                prepared.push(SchemaChange::MoveColumn {
                    name: mover,
                    position: at,
                });
            }
            _ => prepared.push(change.clone()),
        }
    }
    Ok(prepared)
}

fn name_known(schema: &Schema, added_names: &[String], name: &str) -> bool {
    if schema.field_by_name_case_insensitive(name).is_some() {
        return true;
    }
    let folded = name.to_lowercase();
    added_names
        .iter()
        .any(|added| added.to_lowercase() == folded)
}

pub(super) fn top_level_names(schema: &Schema) -> Vec<String> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

pub(super) fn unresolved_column(name: &str, candidates: &[String]) -> String {
    let rendered = name
        .split('.')
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".");
    if candidates.is_empty() {
        return format!(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter \
             with name {rendered} cannot be resolved. SQLSTATE: 42703"
        );
    }
    let suggestions = candidates
        .iter()
        .map(|candidate| format!("`{candidate}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
         name {rendered} cannot be resolved. Did you mean one of the following? \
         [{suggestions}]. SQLSTATE: 42703"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::super::alter::apply_schema_changes;

    async fn setup(wh: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
        let warehouse = wh.path().to_str().unwrap().to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .unwrap(),
        );
        let ns = NamespaceIdent::new("sales".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();
        (catalog, TableIdent::new(ns, "t".to_string()))
    }

    async fn schema_field_names(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<String> {
        let table = catalog.load_table(ident).await.unwrap();
        table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    }

    fn nested_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(
                    2,
                    "s",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(3, "a", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::optional(5, "tail", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn resolve_move_names_accepts_short_and_dotted_names() {
        let schema = nested_schema();
        assert_eq!(
            resolve_move_names(&schema, &[], "tail", &ColumnPosition::First),
            Ok(("tail".to_string(), ColumnPosition::First))
        );
        assert_eq!(
            resolve_move_names(&schema, &[], "ID", &ColumnPosition::First),
            Ok(("ID".to_string(), ColumnPosition::First))
        );
        assert_eq!(
            resolve_move_names(&schema, &[], "s.b", &ColumnPosition::First),
            Ok(("s.b".to_string(), ColumnPosition::First))
        );
        assert_eq!(
            resolve_move_names(&schema, &[], "s.b", &ColumnPosition::After("a".to_string())),
            Ok(("s.b".to_string(), ColumnPosition::After("s.a".to_string())))
        );
        assert_eq!(
            resolve_move_names(
                &schema,
                &[],
                "id",
                &ColumnPosition::After("nope".to_string())
            ),
            Err(unresolved_column(
                "nope",
                &["id".into(), "s".into(), "tail".into()]
            ))
        );
        assert_eq!(
            resolve_move_names(
                &schema,
                &[],
                "s.b",
                &ColumnPosition::After("nope".to_string())
            ),
            Err(unresolved_column(
                "s.nope",
                &["id".into(), "s".into(), "tail".into()]
            ))
        );
    }

    #[test]
    fn resolve_move_names_suggests_top_level_fields() {
        let schema = nested_schema();
        assert_eq!(
            resolve_move_names(&schema, &[], "nope", &ColumnPosition::First),
            Err(unresolved_column(
                "nope",
                &["id".into(), "s".into(), "tail".into()]
            ))
        );
        assert_eq!(
            resolve_move_names(
                &schema,
                &[],
                "s.b",
                &ColumnPosition::After("nope".to_string())
            ),
            Err(unresolved_column(
                "s.nope",
                &["id".into(), "s".into(), "tail".into()]
            ))
        );
    }

    #[test]
    fn resolve_move_names_covers_batch_added_columns() {
        let schema = nested_schema();
        assert_eq!(
            resolve_move_names(
                &schema,
                &["Z".to_string()],
                "z",
                &ColumnPosition::After("id".to_string())
            ),
            Ok(("z".to_string(), ColumnPosition::After("id".to_string())))
        );
    }

    #[test]
    fn starts_with_alter_skips_space_and_comments() {
        assert!(starts_with_alter("ALTER TABLE t ALTER COLUMN b FIRST"));
        assert!(starts_with_alter("  alter table t alter column b first"));
        assert!(starts_with_alter(
            "-- lead\nALTER TABLE t ALTER COLUMN b FIRST"
        ));
        assert!(starts_with_alter(
            "/* lead */ ALTER TABLE t ALTER COLUMN b FIRST"
        ));
        assert!(!starts_with_alter("SELECT * FROM t"));
        assert!(!starts_with_alter("ALTERED TABLE t"));
        assert!(!starts_with_alter(""));
    }

    #[tokio::test]
    async fn schema_add_then_move_batch_applies_in_order() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        for name in ["a", "b"] {
            apply_schema_changes(
                catalog.as_ref(),
                &ident,
                &[SchemaChange::AddColumn {
                    name: (*name).into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    required: false,
                    position: None,
                }],
            )
            .await
            .unwrap();
        }
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[
                SchemaChange::AddColumn {
                    name: "z".into(),
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                    required: false,
                    position: Some(ColumnPosition::First),
                },
                SchemaChange::MoveColumn {
                    name: "id".into(),
                    position: ColumnPosition::First,
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec![
                "id".to_string(),
                "z".to_string(),
                "a".to_string(),
                "b".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn schema_noop_move_keeps_schema_id() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        for name in ["a", "b"] {
            apply_schema_changes(
                catalog.as_ref(),
                &ident,
                &[SchemaChange::AddColumn {
                    name: (*name).into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    required: false,
                    position: None,
                }],
            )
            .await
            .unwrap();
        }
        let before = catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .current_schema_id();
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::MoveColumn {
                name: "id".into(),
                position: ColumnPosition::First,
            }],
        )
        .await
        .unwrap();
        let after = catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .current_schema_id();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["id".to_string(), "a".to_string(), "b".to_string()]
        );
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn schema_nested_short_after_resolves() {
        let wh = TempDir::new().unwrap();
        let (catalog, _) = setup(&wh).await;
        let ns = NamespaceIdent::new("sales".to_string());
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(
                    2,
                    "s",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(3, "a", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let nested = TableIdent::new(ns.clone(), "nested".to_string());
        catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("nested".to_string())
                    .schema(schema)
                    .properties(HashMap::new())
                    .build(),
            )
            .await
            .unwrap();
        apply_schema_changes(
            catalog.as_ref(),
            &nested,
            &[SchemaChange::MoveColumn {
                name: "s.b".into(),
                position: ColumnPosition::After("a".into()),
            }],
        )
        .await
        .unwrap();
        let table = catalog.load_table(&nested).await.unwrap();
        let top: Vec<String> = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect();
        assert_eq!(top, vec!["id".to_string(), "s".to_string()]);
        let inner = table
            .metadata()
            .current_schema()
            .field_by_name("s")
            .unwrap();
        let Type::Struct(struct_type) = inner.field_type.as_ref() else {
            panic!("s must stay a struct");
        };
        let order: Vec<String> = struct_type
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect();
        assert_eq!(order, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn schema_stale_move_rebases_to_java_order() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        for name in ["a", "b"] {
            apply_schema_changes(
                catalog.as_ref(),
                &ident,
                &[SchemaChange::AddColumn {
                    name: (*name).into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    required: false,
                    position: None,
                }],
            )
            .await
            .unwrap();
        }
        let stale = catalog.load_table(&ident).await.unwrap();
        let idle = Transaction::new(&stale)
            .update_schema()
            .case_sensitive(false)
            .move_first("id");
        let idle = idle.apply(Transaction::new(&stale)).unwrap();
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::MoveColumn {
                name: "b".into(),
                position: ColumnPosition::First,
            }],
        )
        .await
        .unwrap();
        let committed = idle.commit(catalog.as_ref()).await.unwrap();
        let order: Vec<String> = committed
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect();
        assert_eq!(
            order,
            vec!["id".to_string(), "b".to_string(), "a".to_string()]
        );
    }
}
