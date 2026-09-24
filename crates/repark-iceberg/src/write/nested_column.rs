use iceberg::spec::{MapType, NestedFieldRef, PrimitiveType, Schema, StructType, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result};
use repark_common::spark_error;

use super::alter::ColumnPosition;
use super::column_move::{top_level_names, unresolved_column};

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

fn quote_if_needed(part: &str) -> String {
    let plain = !part.is_empty()
        && part
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        && !part.chars().all(|character| character.is_ascii_digit());
    if plain {
        part.to_string()
    } else {
        format!("`{}`", part.replace('`', "``"))
    }
}

fn spark_sql_primitive(primitive: &PrimitiveType) -> String {
    match primitive {
        PrimitiveType::Boolean => "BOOLEAN".to_string(),
        PrimitiveType::Int => "INT".to_string(),
        PrimitiveType::Long => "BIGINT".to_string(),
        PrimitiveType::Float => "FLOAT".to_string(),
        PrimitiveType::Double => "DOUBLE".to_string(),
        PrimitiveType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        PrimitiveType::Date => "DATE".to_string(),
        PrimitiveType::Timestamp | PrimitiveType::TimestampNs => "TIMESTAMP_NTZ".to_string(),
        PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => "TIMESTAMP".to_string(),
        PrimitiveType::String | PrimitiveType::Uuid => "STRING".to_string(),
        PrimitiveType::Fixed(_) | PrimitiveType::Binary => "BINARY".to_string(),
        other => other.to_string().to_ascii_uppercase(),
    }
}

fn spark_sql_struct(fields: &[NestedFieldRef]) -> String {
    let children = fields
        .iter()
        .map(|field| {
            let not_null = if field.required { " NOT NULL" } else { "" };
            let comment = field.doc.as_deref().map_or_else(String::new, |doc| {
                format!(
                    " COMMENT '{}'",
                    doc.replace('\\', "\\\\").replace('\'', "\\'")
                )
            });
            format!(
                "{}: {}{not_null}{comment}",
                quote_if_needed(&field.name),
                spark_sql_type(&field.field_type)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("STRUCT<{children}>")
}

fn spark_sql_type(field_type: &Type) -> String {
    match field_type {
        Type::Primitive(primitive) => spark_sql_primitive(primitive),
        Type::Struct(fields) => spark_sql_struct(fields.fields()),
        Type::List(list) => format!("ARRAY<{}>", spark_sql_type(&list.element_field.field_type)),
        Type::Map(map) => format!(
            "MAP<{}, {}>",
            spark_sql_type(&map.key_field.field_type),
            spark_sql_type(&map.value_field.field_type)
        ),
        Type::Variant => "VARIANT".to_string(),
    }
}

#[must_use]
pub fn nested_required_add_refusal(changes: &[ColumnPathChange]) -> Option<String> {
    changes.iter().find_map(|change| match change {
        ColumnPathChange::Add {
            name,
            required: true,
            ..
        } => Some(format!(
            "Unsupported table change: Incompatible change: cannot add required column: {name}"
        )),
        _ => None,
    })
}

#[must_use]
pub fn nested_add_refusal(schema: &Schema, changes: &[ColumnPathChange]) -> Option<String> {
    changes.iter().find_map(|change| {
        let ColumnPathChange::Add {
            parent: Some(parent),
            name,
            ..
        } = change
        else {
            return None;
        };
        if schema.field_by_name_case_insensitive(parent).is_none() {
            return Some(unresolved_column(parent, &top_level_names(schema)));
        }
        schema
            .field_by_name_case_insensitive(&full_name(Some(parent), name))
            .map(|_| {
                let rendered = parent
                    .split('.')
                    .chain(std::iter::once(name.as_str()))
                    .map(|part| format!("`{part}`"))
                    .collect::<Vec<_>>()
                    .join(".");
                format!(
                    "[FIELD_ALREADY_EXISTS] Cannot add column, because {rendered} already exists \
                     in \"{}\". SQLSTATE: 42710",
                    spark_sql_struct(schema.as_struct().fields())
                )
            })
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NestedTypeRefusal {
    Analysis(String),
    Unsupported(String),
}

struct ResolvedNestedPath<'a> {
    names: Vec<String>,
    field_type: &'a Type,
    key_of: Option<&'a MapType>,
}

fn quoted_path(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

fn invalid_field_name(path: &[String], resolved: &[String]) -> NestedTypeRefusal {
    NestedTypeRefusal::Analysis(format!(
        "[INVALID_FIELD_NAME] Field name {} is invalid: {} is not a struct. SQLSTATE: 42000",
        quoted_path(path),
        quoted_path(resolved)
    ))
}

fn struct_child<'a>(fields: &'a StructType, part: &str) -> Option<&'a NestedFieldRef> {
    fields
        .fields()
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(part))
}

fn resolve_nested_path<'a>(
    schema: &'a Schema,
    path: &[String],
) -> std::result::Result<ResolvedNestedPath<'a>, NestedTypeRefusal> {
    let unresolved = || {
        NestedTypeRefusal::Analysis(unresolved_column(&path.join("."), &top_level_names(schema)))
    };
    let Some((first, rest)) = path.split_first() else {
        return Err(unresolved());
    };
    let root = struct_child(schema.as_struct(), first).ok_or_else(unresolved)?;
    let mut resolved = ResolvedNestedPath {
        names: vec![root.name.clone()],
        field_type: root.field_type.as_ref(),
        key_of: None,
    };
    for part in rest {
        let (name, field_type, key_of) = match (resolved.field_type, part.as_str()) {
            (Type::Struct(fields), _) => {
                let child = struct_child(fields, part).ok_or_else(unresolved)?;
                (child.name.clone(), child.field_type.as_ref(), None)
            }
            (Type::Map(map), "key") => (
                "key".to_string(),
                map.key_field.field_type.as_ref(),
                Some(map),
            ),
            (Type::Map(map), "value") => (
                "value".to_string(),
                map.value_field.field_type.as_ref(),
                None,
            ),
            (Type::List(list), "element") => (
                "element".to_string(),
                list.element_field.field_type.as_ref(),
                None,
            ),
            _ => return Err(invalid_field_name(path, &resolved.names)),
        };
        resolved.names.push(name);
        resolved.field_type = field_type;
        resolved.key_of = key_of;
    }
    Ok(resolved)
}

fn iceberg_type_name(field_type: &Type) -> String {
    match field_type {
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            format!("decimal({precision}, {scale})")
        }
        Type::Primitive(PrimitiveType::Fixed(length)) => format!("fixed[{length}]"),
        Type::Primitive(primitive) => primitive.to_string(),
        Type::Struct(fields) => {
            let children = fields
                .fields()
                .iter()
                .map(|field| {
                    let optional = if field.required {
                        "required"
                    } else {
                        "optional"
                    };
                    let doc = field
                        .doc
                        .as_deref()
                        .map_or_else(String::new, |doc| format!(" ({doc})"));
                    format!(
                        "{}: {}: {optional} {}{doc}",
                        field.id,
                        field.name,
                        iceberg_type_name(&field.field_type)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("struct<{children}>")
        }
        Type::List(list) => format!(
            "list<{}>",
            iceberg_type_name(&list.element_field.field_type)
        ),
        Type::Map(map) => format!(
            "map<{}, {}>",
            iceberg_type_name(&map.key_field.field_type),
            iceberg_type_name(&map.value_field.field_type)
        ),
        Type::Variant => "variant".to_string(),
    }
}

fn numeric_rank(primitive: &PrimitiveType) -> Option<u8> {
    match primitive {
        PrimitiveType::Int => Some(0),
        PrimitiveType::Long => Some(1),
        PrimitiveType::Float => Some(2),
        PrimitiveType::Double => Some(3),
        _ => None,
    }
}

fn integral_as_decimal(primitive: &PrimitiveType) -> Option<(u32, u32)> {
    match primitive {
        PrimitiveType::Int => Some((10, 0)),
        PrimitiveType::Long => Some((20, 0)),
        PrimitiveType::Decimal { precision, scale } => Some((*precision, *scale)),
        _ => None,
    }
}

fn decimal_holds(wide: (u32, u32), narrow: (u32, u32)) -> bool {
    let (wide_precision, wide_scale) = wide;
    let (narrow_precision, narrow_scale) = narrow;
    wide_precision.saturating_sub(wide_scale) >= narrow_precision.saturating_sub(narrow_scale)
        && wide_scale >= narrow_scale
}

fn is_timestamp(primitive: &PrimitiveType) -> bool {
    matches!(
        primitive,
        PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs
    )
}

fn spark_can_up_cast(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    use PrimitiveType as P;
    if spark_sql_primitive(from) == spark_sql_primitive(to) || matches!(to, P::String) {
        return true;
    }
    if matches!(from, P::Decimal { .. }) || matches!(to, P::Decimal { .. }) {
        return matches!(
            (integral_as_decimal(from), integral_as_decimal(to)),
            (Some(narrow), Some(wide)) if decimal_holds(wide, narrow)
        );
    }
    if is_timestamp(to) && (matches!(from, P::Date) || is_timestamp(from)) {
        return true;
    }
    matches!(
        (from, to),
        (P::Timestamptz, P::Long) | (P::Long, P::Timestamptz)
    ) || matches!(
        (numeric_rank(from), numeric_rank(to)),
        (Some(narrow), Some(wide)) if narrow < wide
    )
}

fn iceberg_promotion_allowed(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    match (from, to) {
        (PrimitiveType::Int, PrimitiveType::Long)
        | (PrimitiveType::Float, PrimitiveType::Double) => true,
        (
            PrimitiveType::Decimal {
                precision: from_precision,
                scale: from_scale,
            },
            PrimitiveType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
        ) => from_scale == to_scale && from_precision <= to_precision,
        _ => false,
    }
}

#[must_use]
pub fn not_supported_change_column(
    table_name: &str,
    column_name: &str,
    from: &Type,
    to: &Type,
) -> String {
    let from_type = spark_sql_type(from);
    let to_type = spark_sql_type(to);
    spark_error::message(
        spark_error::NOT_SUPPORTED_CHANGE_COLUMN,
        &[
            ("tableName", table_name),
            ("columnName", column_name),
            ("fromType", from_type.as_str()),
            ("toType", to_type.as_str()),
        ],
    )
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: the refusal variants are recorded in the unit ledger"
)]
pub fn resolve_nested_type_change(
    schema: &Schema,
    table_name: &str,
    path: &[String],
    to: &Type,
) -> std::result::Result<String, NestedTypeRefusal> {
    let resolved = resolve_nested_path(schema, path)?;
    let dotted = resolved.names.join(".");
    let refuse_analysis = || {
        NestedTypeRefusal::Analysis(not_supported_change_column(
            table_name,
            &quoted_path(&resolved.names),
            resolved.field_type,
            to,
        ))
    };
    let (from_primitive, to_primitive) = match (resolved.field_type, to) {
        (Type::Primitive(from_primitive), Type::Primitive(to_primitive)) => {
            (from_primitive, to_primitive)
        }
        (_, Type::Primitive(_)) => return Err(refuse_analysis()),
        _ => return Ok(dotted),
    };
    if from_primitive == to_primitive {
        return Ok(dotted);
    }
    if !spark_can_up_cast(from_primitive, to_primitive) {
        return Err(refuse_analysis());
    }
    if !iceberg_promotion_allowed(from_primitive, to_primitive) {
        return Err(NestedTypeRefusal::Unsupported(format!(
            "Unsupported table change: Cannot change column type: {dotted}: {} -> {}",
            iceberg_type_name(resolved.field_type),
            iceberg_type_name(to)
        )));
    }
    match resolved.key_of {
        Some(map) => Err(NestedTypeRefusal::Unsupported(format!(
            "Unsupported table change: Cannot update map keys: {}",
            iceberg_type_name(&Type::Map(map.clone()))
        ))),
        None => Ok(dotted),
    }
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
    async fn nested_add_refusal_answers_spark_for_known_and_unknown_paths() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) = nested_table(&warehouse).await;
        let table = catalog.load_table(&ident).await.unwrap();
        let schema = table.metadata().current_schema();
        assert_eq!(nested_add_refusal(schema, &[add("s", "c", false)]), None);
        assert_eq!(nested_required_add_refusal(&[add("s", "c", false)]), None);
        assert_eq!(
            nested_required_add_refusal(&[add("s", "c", false), add("s", "r", true)]).as_deref(),
            Some("Unsupported table change: Incompatible change: cannot add required column: r")
        );
        assert_eq!(
            nested_add_refusal(schema, &[add("arrs.element", "y", false)]),
            None
        );
        assert_eq!(
            nested_add_refusal(schema, &[add("s", "c", false), add("S", "A", false)]).as_deref(),
            Some(
                "[FIELD_ALREADY_EXISTS] Cannot add column, because `S`.`A` already exists in \
                 \"STRUCT<id: INT, s: STRUCT<a: INT, b: STRING>, arrs: ARRAY<STRUCT<x: INT>>>\". \
                 SQLSTATE: 42710"
            )
        );
        assert_eq!(
            nested_add_refusal(schema, &[add("nope", "z", false)]).as_deref(),
            Some(
                "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter \
                 with name `nope` cannot be resolved. Did you mean one of the following? \
                 [`id`, `s`, `arrs`]. SQLSTATE: 42703"
            )
        );
    }

    #[test]
    fn spark_sql_struct_renders_required_doc_and_quoted_names() {
        let fields = vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int))
                .with_doc("it's")
                .into(),
            NestedField::optional(2, "x.y", Type::Primitive(PrimitiveType::Long)).into(),
        ];
        assert_eq!(
            spark_sql_struct(&fields),
            "STRUCT<a: INT NOT NULL COMMENT 'it\\'s', `x.y`: BIGINT>"
        );
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
