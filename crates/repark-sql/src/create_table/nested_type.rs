use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlDataType};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::{ListType, MapType, NestedField, StructType, Type};
use repark_iceberg::write::nested_type_sql::{
    create_column_list_has_nested_type_opener, rewrite_create_column_types, struct_field_required,
};

use super::cast_type_to_iceberg;

type TypeFuture<'a> = Pin<Box<dyn Future<Output = Result<Type>> + Send + 'a>>;

pub(crate) fn nested_type_parse_error(message: String) -> DataFusionError {
    DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
}

pub(crate) fn needs_structural_mapping(data_type: &SqlDataType) -> bool {
    match data_type {
        SqlDataType::Map(_, _) => true,
        SqlDataType::Struct(fields, _) => fields
            .iter()
            .any(|field| field.options.is_some() || needs_structural_mapping(&field.field_type)),
        SqlDataType::Array(ArrayElemTypeDef::AngleBracket(element)) => {
            needs_structural_mapping(element)
        }
        _ => false,
    }
}

fn allocate(next_id: &mut i32) -> Result<i32> {
    let allocated = *next_id;
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| DataFusionError::Plan("nested type exceeds the field-id range".into()))?;
    Ok(allocated)
}

pub(crate) fn structural_type_to_iceberg<'a>(
    ctx: &'a SessionContext,
    data_type: &'a SqlDataType,
    form: &'a str,
    next_id: &'a mut i32,
) -> TypeFuture<'a> {
    Box::pin(async move {
        match data_type {
            SqlDataType::Map(key, value) => {
                let key_id = allocate(next_id)?;
                let key_type = structural_type_to_iceberg(ctx, key, form, next_id).await?;
                let value_id = allocate(next_id)?;
                let value_type = structural_type_to_iceberg(ctx, value, form, next_id).await?;
                Ok(Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(key_id, key_type)),
                    Arc::new(NestedField::map_value_element(value_id, value_type, false)),
                )))
            }
            SqlDataType::Struct(fields, _) => {
                let mut children = Vec::with_capacity(fields.len());
                for field in fields {
                    let name = field.field_name.as_ref().ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "{form}: STRUCT field `{field}` needs a name"
                        ))
                    })?;
                    let required = struct_field_required(field).map_err(nested_type_parse_error)?;
                    let field_id = allocate(next_id)?;
                    let field_type =
                        structural_type_to_iceberg(ctx, &field.field_type, form, next_id).await?;
                    children.push(Arc::new(if required {
                        NestedField::required(field_id, name.value.clone(), field_type)
                    } else {
                        NestedField::optional(field_id, name.value.clone(), field_type)
                    }));
                }
                Ok(Type::Struct(StructType::new(children)))
            }
            SqlDataType::Array(ArrayElemTypeDef::AngleBracket(element)) => {
                let element_id = allocate(next_id)?;
                let element_type = structural_type_to_iceberg(ctx, element, form, next_id).await?;
                let field = NestedField::optional(element_id, "element", element_type);
                Ok(Type::List(ListType::new(Arc::new(field))))
            }
            other => cast_type_to_iceberg(ctx, other, form).await,
        }
    })
}

fn is_create_table(tokens: &[Token]) -> bool {
    let mut words = tokens.iter().filter_map(|token| match token {
        Token::Word(word) => Some(word.value.to_ascii_uppercase()),
        _ => None,
    });
    words.next().as_deref() == Some("CREATE") && words.take(5).any(|word| word == "TABLE")
}

pub(crate) fn rewrite_nested_create_types(sql: &str) -> Result<Option<String>> {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return Ok(None);
    };
    if !is_create_table(&tokens) || !create_column_list_has_nested_type_opener(&tokens) {
        return Ok(None);
    }
    let rewritten = rewrite_create_column_types(&tokens, true).map_err(nested_type_parse_error)?;
    Ok(Some(rewritten.iter().map(ToString::to_string).collect()))
}
