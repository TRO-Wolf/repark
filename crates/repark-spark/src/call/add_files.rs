use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, Value, ValueWithSpan};
use iceberg::maintenance::{AddFiles, AddFilesSource};
use iceberg::spec::{DEFAULT_SCHEMA_NAME_MAPPING, MappedField, NameMapping, create_name_mapping};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, table::Table};

use super::params::params_for;
use super::rewrite_options::pairs_from_map_args;
use super::{CallArgs, bytes_as_i64, resolve_table_ident};
use crate::call_args::bind;
use crate::{iceberg_err, reregister};

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_add_files(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    let bound = bind(args, params_for("add_files"), &[])?;
    let table_arg = bound.require_string("table")?;
    let source_expr = bound.require_expr("source_table")?;
    let source_dir = source_directory(source_expr)?;
    let partition_filter = partition_filter_map(bound.get("partition_filter"))?;
    let check_duplicates = bound
        .optional_bool("check_duplicate_files")?
        .unwrap_or(true);
    let parallelism = validated_parallelism(bound.optional_i32("parallelism")?)?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let table = ensure_name_mapping_pretty(&table, catalog.as_ref()).await?;

    let result = AddFiles::new(table, AddFilesSource::Directory(source_dir))
        .partition_filter(partition_filter)
        .check_duplicate_files(check_duplicates)
        .parallelism(parallelism)
        .execute(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;
    add_files_result_dataframe(ctx, &result)
}

#[allow(clippy::missing_errors_doc)]
fn source_directory(source_expr: &Expr) -> Result<String> {
    if let Expr::CompoundIdentifier(parts) = source_expr
        && let [format, path] = parts.as_slice()
    {
        return dispatch_source_part(&format.value, &path.value);
    }
    let Expr::Value(ValueWithSpan { value, .. }) = source_expr else {
        return source_shape_error(source_expr);
    };
    let text = match value {
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => text,
        _ => return source_shape_error(source_expr),
    };
    let Some((format, path)) = split_backticked_pair(text) else {
        return source_shape_error(source_expr);
    };
    dispatch_source_part(format, path)
}

fn split_backticked_pair(text: &str) -> Option<(&str, &str)> {
    let inner = text.strip_prefix('`')?.strip_suffix('`')?;
    let (first, second) = inner.split_once("`.`")?;
    if first.is_empty() || second.is_empty() || first.contains('`') || second.contains('`') {
        return None;
    }
    Some((first, second))
}

#[allow(clippy::missing_errors_doc)]
fn dispatch_source_part(format: &str, path: &str) -> Result<String> {
    if path.contains('/') {
        if !format.eq_ignore_ascii_case("parquet") {
            return Err(DataFusionError::NotImplemented(format!(
                "CALL add_files source format `{format}` is not supported (only `parquet` \
                 directory imports are supported)"
            )));
        }
        return Ok(path.to_string());
    }
    Err(DataFusionError::NotImplemented(format!(
        "CALL add_files source `{format}.{path}` is a catalog table reference, and catalog \
         table sources are not supported (only parquet directory imports are supported)"
    )))
}

#[allow(clippy::missing_errors_doc)]
fn source_shape_error(source_expr: &Expr) -> Result<String> {
    Err(DataFusionError::Plan(format!(
        "CALL add_files argument `source_table` must be a parquet directory reference \
         (`parquet`.`<directory>`), got {source_expr}"
    )))
}

#[allow(clippy::missing_errors_doc)]
fn partition_filter_map(filter_expr: Option<&Expr>) -> Result<HashMap<String, String>> {
    let Some(expr) = filter_expr else {
        return Ok(HashMap::new());
    };
    let Expr::Function(function) = expr else {
        return Err(DataFusionError::Plan(format!(
            "CALL add_files argument `partition_filter` must be map(k, v, …), got {expr}"
        )));
    };
    if !function.name.to_string().eq_ignore_ascii_case("map") {
        return Err(DataFusionError::Plan(format!(
            "CALL add_files argument `partition_filter` must be map(k, v, …), got {expr}"
        )));
    }
    let mut filter = HashMap::new();
    for (key, value) in pairs_from_map_args(&function.args, "add_files", "partition_filter")? {
        let Some(value) = value else {
            return Err(DataFusionError::Plan(format!(
                "CALL add_files argument `partition_filter` value for `{key}` must be a string \
                 literal, got NULL"
            )));
        };
        filter.insert(key, value);
    }
    Ok(filter)
}

#[allow(clippy::missing_errors_doc)]
fn validated_parallelism(parallelism: Option<i32>) -> Result<usize> {
    match parallelism {
        None => Ok(1),
        Some(value) => {
            if value < 1 {
                return Err(DataFusionError::Plan(format!(
                    "CALL add_files parallelism must be >= 1, got {value}"
                )));
            }
            usize::try_from(value).map_err(|_| {
                DataFusionError::Plan(format!(
                    "CALL argument `parallelism` value {value} does not fit usize"
                ))
            })
        }
    }
}

#[allow(clippy::missing_errors_doc)]
async fn ensure_name_mapping_pretty(table: &Table, catalog: &dyn Catalog) -> Result<Table> {
    if table
        .metadata()
        .properties()
        .contains_key(DEFAULT_SCHEMA_NAME_MAPPING)
    {
        return Ok(table.clone());
    }
    let mapping = create_name_mapping(table.metadata().current_schema()).map_err(iceberg_err)?;
    let json = name_mapping_pretty(&mapping);
    let tx = Transaction::new(table);
    let action = tx
        .update_table_properties()
        .set(DEFAULT_SCHEMA_NAME_MAPPING.to_string(), json);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(catalog).await.map_err(iceberg_err)
}

fn name_mapping_pretty(mapping: &NameMapping) -> String {
    if mapping.fields().is_empty() {
        return "[ ]".to_string();
    }
    let mut out = String::from("[ ");
    for (index, field) in mapping.fields().iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        mapped_field_pretty(field, 0, &mut out);
    }
    out.push_str(" ]");
    out
}

fn mapped_field_pretty(field: &MappedField, depth: usize, out: &mut String) {
    let pad = "  ".repeat(depth);
    let inner = "  ".repeat(depth + 1);
    out.push_str("{\n");
    if let Some(id) = field.field_id() {
        out.push_str(&inner);
        out.push_str("\"field-id\" : ");
        out.push_str(&id.to_string());
        out.push_str(",\n");
    }
    out.push_str(&inner);
    out.push_str("\"names\" : [ ");
    for (index, name) in field.names().iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        json_string(out, name);
    }
    out.push_str(" ]");
    if !field.fields().is_empty() {
        out.push_str(",\n");
        out.push_str(&inner);
        out.push_str("\"fields\" : [ ");
        for (index, child) in field.fields().iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            mapped_field_pretty(child, depth + 1, out);
        }
        out.push_str(" ]");
    }
    out.push('\n');
    out.push_str(&pad);
    out.push('}');
}

fn json_string(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            ch if u32::from(ch) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(ch))),
            ch => out.push(ch),
        }
    }
    out.push('"');
}

#[allow(clippy::missing_errors_doc)]
fn add_files_result_dataframe(
    ctx: &SessionContext,
    result: &iceberg::maintenance::AddFilesResult,
) -> Result<DataFrame> {
    let _ = result.changed_partition_count;
    let schema = Arc::new(Schema::new(vec![
        Field::new("added_files_count", DataType::Int64, false),
        Field::new("changed_partition_count", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![bytes_as_i64(
                result.added_files_count,
            )?])),
            Arc::new(Int64Array::from(vec![None])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}
