use std::collections::HashSet;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use repark_core::CatalogRegistry;
use repark_iceberg::write::alter::starts_with_alter;

use crate::alter::{
    Sig, collect_name_parts, is_period_at, table_parts_to_ident, tokenize_significant, word_at,
    word_eq,
};
use crate::column_move::parse_column_path;
use crate::{catalog_handle, iceberg_err, reregister};

#[derive(Debug)]
pub(crate) struct IdentifierFieldsDdl {
    table_parts: Vec<String>,
    fields: Vec<String>,
    remove: bool,
}

pub(crate) fn try_parse_set_identifier_fields_ddl(
    sql: &str,
) -> Option<Result<IdentifierFieldsDdl>> {
    try_parse_identifier_fields_ddl(sql, false)
}

pub(crate) fn try_parse_drop_identifier_fields_ddl(
    sql: &str,
) -> Option<Result<IdentifierFieldsDdl>> {
    try_parse_identifier_fields_ddl(sql, true)
}

fn try_parse_identifier_fields_ddl(sql: &str, remove: bool) -> Option<Result<IdentifierFieldsDdl>> {
    if !starts_with_alter(sql) {
        return None;
    }
    let significant = tokenize_significant(sql)?;
    if significant.len() < 7 {
        return None;
    }
    if !(word_eq(&significant, 0, "ALTER") && word_eq(&significant, 1, "TABLE")) {
        return None;
    }
    let mut index = 2usize;
    word_at(&significant, index)?;
    let table_start = index;
    index += 1;
    while is_period_at(&significant, index) && word_at(&significant, index + 1).is_some() {
        index += 2;
    }
    let table_parts = collect_name_parts(&significant, table_start, index)?;
    let verb = if remove { "DROP" } else { "SET" };
    if !(word_eq(&significant, index, verb)
        && word_eq(&significant, index + 1, "IDENTIFIER")
        && word_eq(&significant, index + 2, "FIELDS"))
    {
        return None;
    }
    let (fields, end) = parse_field_list(&significant, index + 3)?;
    if end < significant.len() {
        return None;
    }
    Some(Ok(IdentifierFieldsDdl {
        table_parts,
        fields,
        remove,
    }))
}

fn parse_field_list(significant: &[Sig], start: usize) -> Option<(Vec<String>, usize)> {
    let (first, mut index) = parse_column_path(significant, start)?;
    let mut fields = vec![first];
    while matches!(significant.get(index), Some(Sig::Comma)) {
        let (next, end) = parse_column_path(significant, index + 1)?;
        fields.push(next);
        index = end;
    }
    if index < significant.len() {
        return None;
    }
    Some((fields, index))
}

pub(crate) async fn execute_identifier_fields_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: IdentifierFieldsDdl,
) -> Result<DataFrame> {
    let (catalog_name, ident) = table_parts_to_ident(catalogs, &ddl.table_parts)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let table = handle.load_table(&ident).await.map_err(iceberg_err)?;
    let names = resolve_identifier_names(&table, &ddl)?;
    let tx = Transaction::new(&table);
    let action = tx
        .update_schema()
        .case_sensitive(false)
        .set_identifier_fields(names);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(handle.as_ref()).await.map_err(iceberg_err)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    ctx.read_empty()
}

fn resolve_identifier_names(table: &Table, ddl: &IdentifierFieldsDdl) -> Result<Vec<String>> {
    let schema = table.metadata().current_schema();
    if ddl.remove {
        for name in &ddl.fields {
            find_identifier_field(schema, name)?;
        }
        let removed: HashSet<String> = ddl.fields.iter().map(|name| name.to_lowercase()).collect();
        let remaining: Vec<String> = identifier_field_names(schema)
            .into_iter()
            .filter(|name| !removed.contains(&name.to_lowercase()))
            .collect();
        return Ok(remaining);
    }
    let mut names = Vec::with_capacity(ddl.fields.len());
    for name in &ddl.fields {
        let (ancestors, field) = find_identifier_field(schema, name)?;
        validate_identifier_candidate(field, &ancestors)?;
        names.push(field.name.clone());
    }
    Ok(names)
}

fn identifier_field_names(schema: &Schema) -> Vec<String> {
    schema
        .identifier_field_ids()
        .filter_map(|id| schema.field_by_id(id).map(|field| field.name.clone()))
        .collect()
}

fn find_identifier_field<'a>(
    schema: &'a Schema,
    name: &str,
) -> Result<(Vec<&'a NestedField>, &'a NestedField)> {
    let parts: Vec<&str> = name.split('.').collect();
    let Some(mut field) = struct_child(schema.as_struct(), parts[0]) else {
        return Err(unknown_identifier_field(name));
    };
    let mut ancestors: Vec<&NestedField> = Vec::new();
    for part in &parts[1..] {
        ancestors.push(field);
        let Some(child) = field_child(field, part) else {
            return Err(unknown_identifier_field(name));
        };
        field = child;
    }
    Ok((ancestors, field))
}

fn struct_child<'a>(struct_type: &'a StructType, part: &str) -> Option<&'a NestedField> {
    struct_type
        .fields()
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(part))
        .map(Arc::as_ref)
}

fn field_child<'a>(field: &'a NestedField, part: &str) -> Option<&'a NestedField> {
    match field.field_type.as_ref() {
        Type::Struct(struct_type) => struct_child(struct_type, part),
        Type::List(list_type) => (list_type.element_field.name.eq_ignore_ascii_case(part))
            .then_some(list_type.element_field.as_ref()),
        Type::Map(map_type) => {
            if map_type.key_field.name.eq_ignore_ascii_case(part) {
                Some(map_type.key_field.as_ref())
            } else {
                map_type
                    .value_field
                    .name
                    .eq_ignore_ascii_case(part)
                    .then_some(map_type.value_field.as_ref())
            }
        }
        _ => None,
    }
}

fn validate_identifier_candidate(field: &NestedField, ancestors: &[&NestedField]) -> Result<()> {
    let name = field.name.as_str();
    if !matches!(field.field_type.as_ref(), Type::Primitive(_)) {
        return Err(repark_core::illegal_argument_error(format!(
            "Cannot add field {name} as an identifier field: not a primitive type field"
        )));
    }
    if !field.required {
        return Err(repark_core::illegal_argument_error(format!(
            "Cannot add field {name} as an identifier field: not a required field"
        )));
    }
    if matches!(
        field.field_type.as_ref(),
        Type::Primitive(PrimitiveType::Float | PrimitiveType::Double)
    ) {
        return Err(repark_core::illegal_argument_error(format!(
            "Cannot add field {name} as an identifier field: must not be float or double field"
        )));
    }
    for ancestor in ancestors {
        if !ancestor.field_type.is_struct() {
            return Err(repark_core::illegal_argument_error(format!(
                "Cannot add field {name} as an identifier field: must not be nested in {}",
                nested_field_text(ancestor)
            )));
        }
        if !ancestor.required {
            return Err(repark_core::illegal_argument_error(format!(
                "Cannot add field {name} as an identifier field: must not be nested in an \
                 optional field {}",
                nested_field_text(ancestor)
            )));
        }
    }
    Ok(())
}

fn nested_field_text(field: &NestedField) -> String {
    let requirement = if field.required {
        "required"
    } else {
        "optional"
    };
    let mut text = format!(
        "{}: {}: {requirement} {}",
        field.id,
        field.name,
        field_type_text(&field.field_type)
    );
    if let Some(doc) = field.doc.as_ref() {
        text.push_str(&format!(" ({doc})"));
    }
    text
}

fn field_type_text(field_type: &Type) -> String {
    match field_type {
        Type::Primitive(primitive) => primitive_type_text(primitive),
        Type::Struct(struct_type) => {
            let fields: Vec<String> = struct_type
                .fields()
                .iter()
                .map(|field| nested_field_text(field.as_ref()))
                .collect();
            format!("struct<{}>", fields.join(", "))
        }
        Type::List(list_type) => {
            format!(
                "list<{}>",
                field_type_text(&list_type.element_field.field_type)
            )
        }
        Type::Map(map_type) => format!(
            "map<{}, {}>",
            field_type_text(&map_type.key_field.field_type),
            field_type_text(&map_type.value_field.field_type)
        ),
        Type::Variant => "variant".to_string(),
    }
}

fn primitive_type_text(primitive: &PrimitiveType) -> String {
    match primitive {
        PrimitiveType::Boolean => "boolean".to_string(),
        PrimitiveType::Int => "int".to_string(),
        PrimitiveType::Long => "long".to_string(),
        PrimitiveType::Float => "float".to_string(),
        PrimitiveType::Double => "double".to_string(),
        PrimitiveType::Decimal { precision, scale } => {
            format!("decimal({precision}, {scale})")
        }
        PrimitiveType::Date => "date".to_string(),
        PrimitiveType::Time => "time".to_string(),
        PrimitiveType::Timestamp => "timestamp".to_string(),
        PrimitiveType::Timestamptz => "timestamptz".to_string(),
        PrimitiveType::TimestampNs => "timestamp_ns".to_string(),
        PrimitiveType::TimestamptzNs => "timestamptz_ns".to_string(),
        PrimitiveType::String => "string".to_string(),
        PrimitiveType::Uuid => "uuid".to_string(),
        PrimitiveType::Fixed(size) => format!("fixed[{size}]"),
        PrimitiveType::Binary => "binary".to_string(),
        PrimitiveType::Unknown => "unknown".to_string(),
    }
}

fn unknown_identifier_field(name: &str) -> DataFusionError {
    repark_core::illegal_argument_error(format!(
        "Cannot add field {name} as an identifier field: not found in current schema or \
         added columns"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_and_drop_identifier_fields() {
        let set = try_parse_set_identifier_fields_ddl(
            "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS id, k",
        )
        .expect("recognize")
        .expect("parse");
        assert_eq!(set.table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(set.fields, vec!["id", "k"]);
        assert!(!set.remove);
        let drop = try_parse_drop_identifier_fields_ddl(
            "ALTER TABLE ice.sales.t DROP IDENTIFIER FIELDS k",
        )
        .expect("recognize")
        .expect("parse");
        assert_eq!(drop.fields, vec!["k"]);
        assert!(drop.remove);
        let dotted = try_parse_set_identifier_fields_ddl(
            "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS m.key",
        )
        .expect("recognize")
        .expect("parse");
        assert_eq!(dotted.fields, vec!["m.key"]);
    }

    #[test]
    fn parse_identifier_fields_is_case_insensitive() {
        let parsed = try_parse_drop_identifier_fields_ddl(
            "alter table ice.sales.t drop identifier fields id",
        )
        .expect("recognize")
        .expect("parse");
        assert_eq!(parsed.fields, vec!["id"]);
        assert!(parsed.remove);
    }

    #[test]
    fn parse_identifier_fields_leaves_other_shapes_alone() {
        assert!(try_parse_set_identifier_fields_ddl("SELECT 1").is_none());
        assert!(
            try_parse_set_identifier_fields_ddl("ALTER TABLE ice.sales.t SET IDENTIFIER id")
                .is_none()
        );
        assert!(
            try_parse_drop_identifier_fields_ddl("ALTER TABLE ice.sales.t DROP IDENTIFIER id")
                .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl(
                "ALTER TABLE ice.sales.t SET LOCATION 'file:/tmp/x'"
            )
            .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl("ALTER TABLE ice.sales.t ALTER COLUMN b FIRST")
                .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl("ALTER TABLE ice.sales.t ADD COLUMN c STRING")
                .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl(
                "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS id, k"
            )
            .is_some()
        );
    }

    #[test]
    fn parse_identifier_fields_needs_a_well_formed_list() {
        assert!(
            try_parse_set_identifier_fields_ddl("ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS")
                .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl(
                "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS (id, k)"
            )
            .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl(
                "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS id,"
            )
            .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl(
                "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS id, k EXTRA"
            )
            .is_none()
        );
        assert!(
            try_parse_set_identifier_fields_ddl(
                "ALTER TABLE ice.sales.t SET IDENTIFIER FIELDS , id"
            )
            .is_none()
        );
    }
}
