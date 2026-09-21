use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, SetExpr, TableObject};
use iceberg::{NamespaceIdent, TableIdent};
use repark_common::spark_error;
use repark_core::CatalogRegistry;

use crate::catalog_ops::name_parts;
use crate::write_to_branch::qualify_table_parts;

pub(crate) fn short_values_message(table: &str, columns: &[String], width: usize) -> String {
    let table_columns = columns
        .iter()
        .map(|name| quote_name(name))
        .collect::<Vec<_>>()
        .join(", ");
    let data_columns = (1..=width)
        .map(|index| quote_name(&format!("col{index}")))
        .collect::<Vec<_>>()
        .join(", ");
    spark_error::message(
        spark_error::INSERT_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS,
        &[
            ("tableName", table),
            ("tableColumns", &table_columns),
            ("dataColumns", &data_columns),
        ],
    )
}

fn quote_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(crate) async fn refuse_if_short_values(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
) -> Result<()> {
    if insert.overwrite
        || insert.replace_into
        || insert.partitioned.is_some()
        || !insert.columns.is_empty()
    {
        return Ok(());
    }
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let Some(source) = insert.source.as_ref() else {
        return Ok(());
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(());
    };
    let mut widths = values.rows.iter().map(|row| row.content.len());
    let Some(first) = widths.next() else {
        return Ok(());
    };
    if first == 0 || widths.any(|width| width != first) {
        return Ok(());
    }
    let parts = qualify_table_parts(ctx, name_parts(name));
    if parts.len() < 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let fields = table.metadata().current_schema().as_struct().fields();
    if first >= fields.len() {
        return Ok(());
    }
    let names: Vec<String> = fields.iter().map(|field| field.name.clone()).collect();
    let display = parts
        .iter()
        .map(|part| quote_name(part))
        .collect::<Vec<_>>()
        .join(".");
    Err(DataFusionError::Plan(short_values_message(
        &display, &names, first,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_values_message_carries_condition_prose_and_sqlstate() {
        let text = short_values_message(
            "`ice`.`sales`.`t`",
            &["id".to_string(), "data".to_string(), "cat".to_string()],
            2,
        );
        assert!(text.starts_with("[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS]"));
        assert!(text.contains("not enough data columns"));
        assert!(text.contains("Table columns: `id`, `data`, `cat`."));
        assert!(text.contains("Data columns: `col1`, `col2`."));
        assert!(text.contains("SQLSTATE: 21S01"));
    }
}
