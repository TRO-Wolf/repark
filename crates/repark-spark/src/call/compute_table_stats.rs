use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
use iceberg::maintenance::ComputeTableStats;
use iceberg::spec::Type;
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
    let ordered = resolve_requested_columns(&table, columns.as_deref())?;

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

fn resolve_requested_columns(
    table: &Table,
    columns: Option<&[String]>,
) -> Result<Option<Vec<String>>> {
    let Some(names) = columns else {
        return Ok(None);
    };
    if names.is_empty() {
        return Err(illegal_argument("Columns cannot be null/empty".to_string()));
    }
    let schema = table.metadata().current_schema();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut ordered = Vec::with_capacity(names.len());
    for name in names {
        let field = schema.field_by_name(name).ok_or_else(|| {
            illegal_argument(format!(
                "Can't find column {name} in {}",
                schema_debug(schema)
            ))
        })?;
        if !field.field_type.is_primitive() {
            return Err(illegal_argument(format!(
                "Can't compute stats on non-primitive type column: {name} ({})",
                spark_type_name(&field.field_type)
            )));
        }
        if seen.insert(name.as_str()) {
            ordered.push(name.clone());
        }
    }
    Ok(Some(ordered))
}

pub(super) fn spark_type_name(rendered: &Type) -> String {
    match rendered {
        Type::Primitive(primitive) => primitive.to_string(),
        Type::Struct(struct_type) => {
            let fields: Vec<String> = struct_type
                .fields()
                .iter()
                .map(|field| {
                    format!(
                        "{}: {}: {} {}",
                        field.id,
                        field.name,
                        if field.required {
                            "required"
                        } else {
                            "optional"
                        },
                        spark_type_name(&field.field_type)
                    )
                })
                .collect();
            format!("struct<{}>", fields.join(", "))
        }
        Type::List(list_type) => {
            let element = &list_type.element_field;
            format!(
                "list<{}: {} {}>",
                element.id,
                if element.required {
                    "required"
                } else {
                    "optional"
                },
                spark_type_name(&element.field_type)
            )
        }
        Type::Map(map_type) => {
            let key = &map_type.key_field;
            let value = &map_type.value_field;
            format!(
                "map<{}: {}, {}: {} {}>",
                key.id,
                spark_type_name(&key.field_type),
                value.id,
                if value.required {
                    "required"
                } else {
                    "optional"
                },
                spark_type_name(&value.field_type)
            )
        }
        Type::Variant => "variant".to_string(),
    }
}

fn schema_debug(schema: &iceberg::spec::Schema) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("table {\n");
    for field in schema.as_struct().fields() {
        let _ = writeln!(out, "  {}", field.to_string().trim_end());
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
