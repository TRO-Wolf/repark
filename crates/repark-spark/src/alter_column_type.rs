use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{AlterColumnOperation, Ident};
use iceberg::spec::{PrimitiveType, Type};
use iceberg::{Catalog, TableIdent};
use repark_functions::timestamp_type::SparkTimestampType;
use repark_iceberg::write::alter::SchemaChange;

use crate::create_table::sql_type_to_iceberg_with_timestamp_type;

async fn load_alter_column_current_type(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    column_name: &Ident,
    operation: &AlterColumnOperation,
) -> Result<Option<Type>> {
    if !matches!(operation, AlterColumnOperation::SetDataType { .. }) {
        return Ok(None);
    }
    let table = catalog
        .load_table(ident)
        .await
        .map_err(crate::iceberg_err)?;
    let found = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(&column_name.value));
    Ok(found.map(|field| field.field_type.as_ref().clone()))
}

pub(crate) async fn push_alter_column_change(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    column_name: &Ident,
    operation: &AlterColumnOperation,
    timestamp_type: SparkTimestampType,
    batch: &mut Vec<SchemaChange>,
) -> Result<bool> {
    let current = load_alter_column_current_type(catalog, ident, column_name, operation).await?;
    let change =
        schema_change_from_alter_column(column_name, operation, timestamp_type, current.as_ref())?;
    if let Some(change) = change {
        batch.push(change);
        return Ok(true);
    }
    Ok(false)
}

pub(crate) async fn apply_nested_alter_type(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    change: Option<SchemaChange>,
) -> Result<()> {
    if let Some(change) = change {
        repark_iceberg::write::alter::apply_schema_changes_on_table(catalog, table, &[change])
            .await
            .map_err(crate::iceberg_err)?;
    }
    Ok(())
}

pub(crate) fn schema_change_from_alter_column(
    column_name: &Ident,
    op: &AlterColumnOperation,
    timestamp_type: SparkTimestampType,
    current: Option<&Type>,
) -> Result<Option<SchemaChange>> {
    match op {
        AlterColumnOperation::SetDataType {
            data_type, using, ..
        } => {
            if using.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "ALTER COLUMN … TYPE … USING is not supported (Iceberg promotions are \
                     metadata-only; no row rewrite)"
                        .into(),
                ));
            }
            let iceberg_type = sql_type_to_iceberg_with_timestamp_type(data_type, timestamp_type)?;
            let iceberg::spec::Type::Primitive(new_type) = iceberg_type else {
                return Err(DataFusionError::NotImplemented(format!(
                    "ALTER COLUMN `{}` TYPE to a non-primitive is not supported",
                    column_name.value
                )));
            };
            if matches!(new_type, PrimitiveType::String)
                && matches!(current, Some(Type::Primitive(PrimitiveType::Uuid)))
            {
                return Ok(None);
            }
            if !is_iceberg_promotion_target(&new_type) {
                return Err(DataFusionError::Plan(format!(
                    "ALTER COLUMN `{}` TYPE `{data_type}` is not an Iceberg type promotion \
                     target — only int→long, float→double, and decimal(p,s)→decimal(p2,s) with \
                     p2≥p (same scale) are allowed; narrowing refuses loud",
                    column_name.value
                )));
            }
            Ok(Some(SchemaChange::UpdateColumnType {
                name: column_name.value.clone(),
                new_type,
            }))
        }
        AlterColumnOperation::DropNotNull => Ok(Some(SchemaChange::MakeColumnOptional {
            name: column_name.value.clone(),
        })),
        AlterColumnOperation::SetNotNull => Err(DataFusionError::NotImplemented(format!(
            "ALTER COLUMN `{}` SET NOT NULL is not supported — making a column required is an \
             Iceberg incompatible change (existing nulls cannot be backfilled without a default)",
            column_name.value
        ))),
        AlterColumnOperation::SetDefault { .. } | AlterColumnOperation::DropDefault => {
            Err(DataFusionError::NotImplemented(format!(
                "ALTER COLUMN `{}` SET/DROP DEFAULT is not supported yet",
                column_name.value
            )))
        }
        AlterColumnOperation::AddGenerated { .. } => Err(DataFusionError::NotImplemented(format!(
            "ALTER COLUMN `{}` ADD GENERATED is not supported",
            column_name.value
        ))),
    }
}

fn is_iceberg_promotion_target(new_type: &PrimitiveType) -> bool {
    matches!(
        new_type,
        PrimitiveType::Long
            | PrimitiveType::Double
            | PrimitiveType::Decimal { .. }
            | PrimitiveType::Int
            | PrimitiveType::Float
            | PrimitiveType::Boolean
            | PrimitiveType::String
            | PrimitiveType::Date
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::Binary
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_promotion_target_accepts_long_double_decimal() {
        assert!(is_iceberg_promotion_target(&PrimitiveType::Long));
        assert!(is_iceberg_promotion_target(&PrimitiveType::Double));
        assert!(is_iceberg_promotion_target(&PrimitiveType::Decimal {
            precision: 10,
            scale: 2
        }));
    }

    #[test]
    fn a_uuid_column_altered_to_string_is_a_no_op() {
        let operation = AlterColumnOperation::SetDataType {
            data_type: datafusion::sql::sqlparser::ast::DataType::String(None),
            using: None,
            had_set: false,
        };
        let current = Type::Primitive(PrimitiveType::Uuid);
        let change = schema_change_from_alter_column(
            &Ident::new("u"),
            &operation,
            SparkTimestampType::Ltz,
            Some(&current),
        )
        .expect("uuid to string must not refuse");
        assert!(change.is_none(), "uuid to string must be a no-op");
    }

    #[test]
    fn an_int_column_altered_to_string_still_changes() {
        let operation = AlterColumnOperation::SetDataType {
            data_type: datafusion::sql::sqlparser::ast::DataType::String(None),
            using: None,
            had_set: false,
        };
        let current = Type::Primitive(PrimitiveType::Int);
        let change = schema_change_from_alter_column(
            &Ident::new("id"),
            &operation,
            SparkTimestampType::Ltz,
            Some(&current),
        )
        .expect("int to string must not refuse");
        assert!(change.is_some(), "int to string must stay a change");
    }
}
