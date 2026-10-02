use iceberg::spec::{NestedFieldRef, Schema, StructType, Type};
use iceberg::table::Table;
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
    let parts = name.split('.').map(str::to_string).collect::<Vec<_>>();
    unresolved_column_parts(&parts, candidates)
}

fn quoted_parts<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    parts
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn route_schema_changes(
    table: &Table,
    changes: &[SchemaChange],
) -> (bool, Vec<SchemaChange>) {
    let schema = table.metadata().current_schema();
    if schema.try_field_by_name_case_insensitive("").is_ok() {
        return (false, changes.to_vec());
    }
    let mut rewritten = Vec::with_capacity(changes.len());
    for change in changes {
        let Some(next) = route_top_level_change(schema, change) else {
            return (false, changes.to_vec());
        };
        rewritten.push(next);
    }
    (true, rewritten)
}

fn route_top_level_change(schema: &Schema, change: &SchemaChange) -> Option<SchemaChange> {
    let fields = schema.as_struct().fields();
    match change {
        SchemaChange::AddColumn {
            name,
            field_type,
            doc,
            required,
            position,
        } => {
            if !top_level_absent(fields, name) {
                return None;
            }
            let position = match position {
                None => None,
                Some(ColumnPosition::First) => Some(ColumnPosition::First),
                Some(ColumnPosition::After(reference)) => Some(ColumnPosition::After(
                    exact_top_level_name(fields, reference)?,
                )),
            };
            Some(SchemaChange::AddColumn {
                name: name.clone(),
                field_type: field_type.clone(),
                doc: doc.clone(),
                required: *required,
                position,
            })
        }
        SchemaChange::DropColumn { name } => Some(SchemaChange::DropColumn {
            name: exact_top_level_name(fields, name)?,
        }),
        SchemaChange::RenameColumn { from, to } => {
            if !top_level_absent(fields, to) {
                return None;
            }
            Some(SchemaChange::RenameColumn {
                from: exact_top_level_name(fields, from)?,
                to: to.clone(),
            })
        }
        SchemaChange::UpdateColumnType { name, new_type } => Some(SchemaChange::UpdateColumnType {
            name: exact_top_level_name(fields, name)?,
            new_type: new_type.clone(),
        }),
        SchemaChange::MakeColumnOptional { name } => Some(SchemaChange::MakeColumnOptional {
            name: exact_top_level_name(fields, name)?,
        }),
        SchemaChange::UpdateColumnDoc { name, doc } => Some(SchemaChange::UpdateColumnDoc {
            name: exact_top_level_name(fields, name)?,
            doc: doc.clone(),
        }),
        SchemaChange::MoveColumn { name, position } => {
            let position = match position {
                ColumnPosition::First => ColumnPosition::First,
                ColumnPosition::After(reference) => {
                    ColumnPosition::After(exact_top_level_name(fields, reference)?)
                }
            };
            Some(SchemaChange::MoveColumn {
                name: exact_top_level_name(fields, name)?,
                position,
            })
        }
    }
}

fn exact_top_level_name(fields: &[NestedFieldRef], name: &str) -> Option<String> {
    let folded = name.to_lowercase();
    let mut found = None;
    for field in fields {
        if field.name.to_lowercase() == folded {
            if found.is_some() {
                return None;
            }
            found = Some(field.name.clone());
        }
    }
    found
}

fn top_level_absent(fields: &[NestedFieldRef], name: &str) -> bool {
    let folded = name.to_lowercase();
    !fields
        .iter()
        .any(|field| field.name.to_lowercase() == folded)
}

#[derive(Debug, PartialEq)]
pub(crate) enum NestedPathResolution {
    Missing,
    Unique(Vec<String>),
    Ambiguous,
}

pub(crate) fn resolve_nested_path_ci(schema: &Schema, parts: &[String]) -> NestedPathResolution {
    let Some((first, rest)) = parts.split_first() else {
        return NestedPathResolution::Missing;
    };
    let mut exact = Vec::with_capacity(parts.len());
    let mut field_type = match match_struct_child(schema.as_struct(), first) {
        ChildMatch::Ambiguous => return NestedPathResolution::Ambiguous,
        ChildMatch::Missing => return NestedPathResolution::Missing,
        ChildMatch::Unique(root) => {
            exact.push(root.name.clone());
            root.field_type.as_ref()
        }
    };
    for part in rest {
        match (field_type, part.as_str()) {
            (Type::Struct(fields), _) => match match_struct_child(fields, part) {
                ChildMatch::Ambiguous => return NestedPathResolution::Ambiguous,
                ChildMatch::Missing => return NestedPathResolution::Missing,
                ChildMatch::Unique(child) => {
                    exact.push(child.name.clone());
                    field_type = child.field_type.as_ref();
                }
            },
            (Type::Map(map), "key") => {
                exact.push("key".to_string());
                field_type = map.key_field.field_type.as_ref();
            }
            (Type::Map(map), "value") => {
                exact.push("value".to_string());
                field_type = map.value_field.field_type.as_ref();
            }
            (Type::List(list), "element") => {
                exact.push("element".to_string());
                field_type = list.element_field.field_type.as_ref();
            }
            _ => return NestedPathResolution::Missing,
        }
    }
    NestedPathResolution::Unique(exact)
}

enum ChildMatch<'a> {
    Missing,
    Unique(&'a NestedFieldRef),
    Ambiguous,
}

fn match_struct_child<'a>(fields: &'a StructType, part: &str) -> ChildMatch<'a> {
    let mut found = None;
    for field in fields.fields() {
        if field.name.eq_ignore_ascii_case(part) {
            if found.is_some() {
                return ChildMatch::Ambiguous;
            }
            found = Some(field);
        }
    }
    match found {
        None => ChildMatch::Missing,
        Some(field) => ChildMatch::Unique(field),
    }
}

pub(crate) fn split_dotted(dotted: &str) -> Vec<String> {
    dotted.split('.').map(str::to_string).collect()
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: the fork collision error propagates for ambiguous names, recorded in the unit ledger"
)]
pub fn nested_name_known_ci(schema: &Schema, name: &str) -> Result<bool> {
    if schema.try_field_by_name_case_insensitive("").is_ok() {
        return schema
            .try_field_by_name_case_insensitive(name)
            .map(|field| field.is_some());
    }
    match resolve_nested_path_ci(schema, &split_dotted(name)) {
        NestedPathResolution::Unique(_) => Ok(true),
        NestedPathResolution::Missing => Ok(false),
        NestedPathResolution::Ambiguous => schema
            .try_field_by_name_case_insensitive(name)
            .map(|field| field.is_some()),
    }
}

pub(super) fn unresolved_column_parts(parts: &[String], candidates: &[String]) -> String {
    let rendered = quoted_parts(parts.iter().map(String::as_str));
    if candidates.is_empty() {
        return format!(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter \
             with name {rendered} cannot be resolved. SQLSTATE: 42703"
        );
    }
    let suggestions = candidates
        .iter()
        .map(|candidate| quoted_parts(candidate.split('.')))
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

    async fn twin_setup(wh: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
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
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "ID", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "b", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("tw".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();
        (catalog, TableIdent::new(ns, "tw".to_string()))
    }

    #[tokio::test]
    async fn twin_rename_of_unambiguous_column_answers() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = twin_setup(&wh).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::RenameColumn {
                from: "b".into(),
                to: "c".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["id".to_string(), "ID".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn twin_drop_and_add_of_unambiguous_columns_answer() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = twin_setup(&wh).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::DropColumn { name: "b".into() }],
        )
        .await
        .unwrap();
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "c".into(),
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
                required: false,
                position: None,
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["id".to_string(), "ID".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn twin_drop_of_ambiguous_name_keeps_collision_refusal() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = twin_setup(&wh).await;
        let error = apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::DropColumn { name: "id".into() }],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("collide"), "{error}");
    }

    fn twin_nested_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "ID", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(
                    4,
                    "s",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(5, "x", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(6, "X", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(7, "z", Type::Primitive(PrimitiveType::Int)).into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    fn parts(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn twin_nested_resolution_splits_unique_missing_and_ambiguous() {
        let schema = twin_nested_schema();
        assert_eq!(
            resolve_nested_path_ci(&schema, &parts(&["s", "z"])),
            NestedPathResolution::Unique(vec!["s".to_string(), "z".to_string()])
        );
        assert_eq!(
            resolve_nested_path_ci(&schema, &parts(&["S", "Z"])),
            NestedPathResolution::Unique(vec!["s".to_string(), "z".to_string()])
        );
        assert_eq!(
            resolve_nested_path_ci(&schema, &parts(&["s", "x"])),
            NestedPathResolution::Ambiguous
        );
        assert_eq!(
            resolve_nested_path_ci(&schema, &parts(&["s", "q"])),
            NestedPathResolution::Missing
        );
        assert_eq!(
            resolve_nested_path_ci(&schema, &parts(&["id"])),
            NestedPathResolution::Ambiguous
        );
        assert_eq!(
            resolve_nested_path_ci(&schema, &parts(&["B"])),
            NestedPathResolution::Unique(vec!["b".to_string()])
        );
    }

    #[test]
    fn twin_nested_known_answers_unique_and_missing_but_refuses_ambiguous() {
        let schema = twin_nested_schema();
        assert!(nested_name_known_ci(&schema, "s.z").unwrap());
        assert!(nested_name_known_ci(&schema, "S.Z").unwrap());
        assert!(!nested_name_known_ci(&schema, "s.q").unwrap());
        let error = nested_name_known_ci(&schema, "s.x").unwrap_err();
        assert!(error.to_string().contains("collide"), "{error}");
    }

    #[tokio::test]
    async fn twin_rename_to_existing_name_keeps_refusal() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = twin_setup(&wh).await;
        let error = apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::RenameColumn {
                from: "b".into(),
                to: "ID".into(),
            }],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("collide"), "{error}");
    }
}
