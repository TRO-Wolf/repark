use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
use iceberg::maintenance::ComputeTableStats;
use iceberg::{Catalog, table::Table};

use super::{CallArgs, illegal_argument, resolve_table_ident};
use crate::call_args::expr_as_string;
use crate::{iceberg_err, reregister};

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_compute_table_stats(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "snapshot_id", "columns"])?;
    args.reject_excess_positional(3)?;
    let table_arg = args.require_string("table", 0)?;
    let snapshot_id = args.optional_i64("snapshot_id", Some(1))?;
    let columns = optional_string_list(args, "columns", Some(2))?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    if table.metadata().current_snapshot().is_none() && snapshot_id.is_none() {
        return empty_statistics_result(ctx);
    }
    let ordered = order_columns_like_schema(&table, columns.as_deref())?;

    let mut action = ComputeTableStats::new(table);
    if let Some(names) = ordered {
        action = action.columns(names);
    }
    if let Some(id) = snapshot_id {
        action = action.snapshot_id(id);
    }
    let result = action
        .execute(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "statistics_file",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            result.statistics_file.statistics_path,
        ]))],
    )?;
    ctx.read_batches(vec![batch])
}

fn empty_statistics_result(ctx: &SessionContext) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "statistics_file",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(Vec::<String>::new()))],
    )?;
    ctx.read_batches(vec![batch])
}

fn order_columns_like_schema(
    table: &Table,
    columns: Option<&[String]>,
) -> Result<Option<Vec<String>>> {
    let Some(names) = columns else {
        return Ok(None);
    };
    let schema = table.metadata().current_schema();
    for name in names {
        if schema.field_by_name(name).is_none() {
            return Err(illegal_argument(format!(
                "Can't find column {name} in {}",
                schema_debug(schema)
            )));
        }
    }
    let wanted: HashSet<&str> = names.iter().map(String::as_str).collect();
    Ok(Some(
        schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| wanted.contains(field.name.as_str()))
            .map(|field| field.name.clone())
            .collect(),
    ))
}

fn schema_debug(schema: &iceberg::spec::Schema) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("table {\n");
    for field in schema.as_struct().fields() {
        let _ = writeln!(out, "  {field}");
    }
    out.push('}');
    out
}

fn optional_string_list(
    args: &CallArgs,
    name: &str,
    position: Option<usize>,
) -> Result<Option<Vec<String>>> {
    if let Some(expr) = args.named.get(name) {
        return array_of_strings(expr, name).map(Some);
    }
    if let Some(index) = position
        && let Some(expr) = args.positional.get(index)
    {
        return array_of_strings(expr, name).map(Some);
    }
    Ok(None)
}

fn array_of_strings(expr: &Expr, arg_name: &str) -> Result<Vec<String>> {
    match expr {
        Expr::Array(array) => array
            .elem
            .iter()
            .map(|element| expr_as_string(element, arg_name))
            .collect(),
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("array") => {
            match &function.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(element)) => {
                            expr_as_string(element, arg_name)
                        }
                        other => Err(DataFusionError::Plan(format!(
                            "CALL argument `{arg_name}` must be an array of string \
                             literals, got {other}"
                        ))),
                    })
                    .collect(),
                other => Err(DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` must be an array of string literals, \
                     got {other}"
                ))),
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be an array of string literals, got {other}"
        ))),
    }
}
