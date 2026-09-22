use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Insert, SetExpr, TableObject};
use iceberg::{NamespaceIdent, TableIdent};
use repark_common::spark_error;
use repark_core::CatalogRegistry;

use crate::schema_ddl::name_parts;

fn short_values_message(table: &str, columns: &[String], width: usize) -> String {
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
    let parts = name_parts(name);
    if parts.len() != 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(vec![parts[1].clone()]) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, parts[2].clone());
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
