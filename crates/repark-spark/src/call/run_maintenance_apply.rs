use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, StringViewArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, Value, ValueWithSpan};
use datafusion::sql::sqlparser::tokenizer::Span;
use repark_core::CatalogRegistry;

use super::CallArgs;
use super::run_maintenance::{PlannedStep, StepAction};

macro_rules! push_scalar {
    ($out:expr, $column:expr, $row:expr, $name:expr, $array:ty) => {{
        let values = $column
            .as_any()
            .downcast_ref::<$array>()
            .ok_or_else(|| json_type_error($name, $column.data_type()))?;
        $out.push_str(&values.value($row).to_string());
    }};
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn apply_steps(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    table_arg: &str,
    steps: &[PlannedStep],
) -> Result<DataFrame> {
    let mut rows: Vec<(i32, String, String, String, String)> = Vec::new();
    let mut stopped = false;
    for step in steps {
        if let Some(reason) = &step.skip_reason {
            rows.push((
                step.ordinal,
                step.procedure.to_string(),
                step.arguments.clone(),
                "skipped".to_string(),
                reason.clone(),
            ));
            continue;
        }
        if stopped {
            rows.push((
                step.ordinal,
                step.procedure.to_string(),
                step.arguments.clone(),
                "skipped".to_string(),
                String::new(),
            ));
            continue;
        }
        match run_step(ctx, catalogs, catalog_name, table_arg, step).await {
            Ok(result) => rows.push((
                step.ordinal,
                step.procedure.to_string(),
                step.arguments.clone(),
                "ran".to_string(),
                result,
            )),
            Err(error) => {
                rows.push((
                    step.ordinal,
                    step.procedure.to_string(),
                    step.arguments.clone(),
                    "failed".to_string(),
                    error.to_string(),
                ));
                stopped = true;
            }
        }
    }
    apply_dataframe(ctx, &rows)
}

async fn run_step(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    table_arg: &str,
    step: &PlannedStep,
) -> Result<String> {
    let frame = match step.action {
        StepAction::RewritePositionDeleteFiles => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            super::execute_rewrite_position_delete_files(
                ctx,
                catalog,
                catalog_name,
                &table_args(table_arg),
            )
            .await?
        }
        StepAction::RewriteDataFiles { target_size } => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            let ident = super::resolve_table_ident(catalog_name, table_arg)?;
            let table = catalog
                .load_table(&ident)
                .await
                .map_err(crate::iceberg_err)?;
            super::rewrite_data_files::run_rewrite(
                ctx,
                catalog,
                catalog_name,
                &ident,
                table,
                false,
                None,
                target_size,
            )
            .await?
        }
        StepAction::RewriteManifests => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            super::rewrite_manifests::execute_rewrite_manifests(
                ctx,
                catalog,
                catalog_name,
                &table_args(table_arg),
            )
            .await?
        }
        StepAction::ExpireSnapshots {
            older_than,
            retain_last,
        } => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            super::execute_expire_snapshots(
                ctx,
                catalog,
                catalog_name,
                &expire_args(table_arg, older_than, retain_last),
            )
            .await?
        }
        StepAction::RemoveOrphanFiles { older_than } => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            super::execute_remove_orphan_files(
                ctx,
                catalog,
                catalog_name,
                catalogs.location_policy(catalog_name),
                &orphan_args(table_arg, older_than),
            )
            .await?
        }
    };
    let batches = frame.collect().await?;
    result_frame_json(&batches)
}

fn string_expr(value: &str) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(value.to_string()),
        span: Span::empty(),
    })
}

fn int_expr(value: i64) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Number(value.to_string(), false),
        span: Span::empty(),
    })
}

fn uint_expr(value: u64) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Number(value.to_string(), false),
        span: Span::empty(),
    })
}

fn bool_expr(value: bool) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Boolean(value),
        span: Span::empty(),
    })
}

fn table_args(table_arg: &str) -> CallArgs {
    CallArgs {
        named: HashMap::from([("table".to_string(), string_expr(table_arg))]),
        positional: Vec::new(),
    }
}

fn expire_args(table_arg: &str, older_than: Option<i64>, retain_last: Option<u64>) -> CallArgs {
    let mut named = HashMap::from([("table".to_string(), string_expr(table_arg))]);
    if let Some(cutoff) = older_than {
        named.insert("older_than".to_string(), int_expr(cutoff));
    }
    if let Some(retain) = retain_last {
        named.insert("retain_last".to_string(), uint_expr(retain));
    }
    CallArgs {
        named,
        positional: Vec::new(),
    }
}

fn orphan_args(table_arg: &str, older_than: i64) -> CallArgs {
    CallArgs {
        named: HashMap::from([
            ("table".to_string(), string_expr(table_arg)),
            ("older_than".to_string(), int_expr(older_than)),
            ("dry_run".to_string(), bool_expr(false)),
        ]),
        positional: Vec::new(),
    }
}

fn apply_dataframe(
    ctx: &SessionContext,
    rows: &[(i32, String, String, String, String)],
) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("procedure", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("result", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.4.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

fn result_frame_json(batches: &[RecordBatch]) -> Result<String> {
    let mut out = String::from("[");
    let mut first_row = true;
    for batch in batches {
        for row in 0..batch.num_rows() {
            if !first_row {
                out.push(',');
            }
            first_row = false;
            out.push('{');
            for (index, field) in batch.schema().fields().iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_json_string(&mut out, field.name());
                out.push(':');
                push_json_value(&mut out, batch.column(index), row, field.name())?;
            }
            out.push('}');
        }
    }
    out.push(']');
    Ok(out)
}

fn push_json_string(out: &mut String, text: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other if (other as u32) < 0x20 => {
                let code = other as u32;
                out.push_str("\\u00");
                out.push(HEX[(code >> 4) as usize] as char);
                out.push(HEX[(code & 0xF) as usize] as char);
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

fn push_json_value(out: &mut String, column: &dyn Array, row: usize, name: &str) -> Result<()> {
    if column.is_null(row) {
        out.push_str("null");
        return Ok(());
    }
    match column.data_type() {
        DataType::Boolean => push_scalar!(out, column, row, name, BooleanArray),
        DataType::Int8 => push_scalar!(out, column, row, name, Int8Array),
        DataType::Int16 => push_scalar!(out, column, row, name, Int16Array),
        DataType::Int32 => push_scalar!(out, column, row, name, Int32Array),
        DataType::Int64 => push_scalar!(out, column, row, name, Int64Array),
        DataType::UInt8 => push_scalar!(out, column, row, name, UInt8Array),
        DataType::UInt16 => push_scalar!(out, column, row, name, UInt16Array),
        DataType::UInt32 => push_scalar!(out, column, row, name, UInt32Array),
        DataType::UInt64 => push_scalar!(out, column, row, name, UInt64Array),
        DataType::Float32 => {
            let values = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| json_type_error(name, column.data_type()))?;
            json_finite(out, f64::from(values.value(row)), name)?;
        }
        DataType::Float64 => {
            let values = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| json_type_error(name, column.data_type()))?;
            json_finite(out, values.value(row), name)?;
        }
        DataType::Utf8 => {
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| json_type_error(name, column.data_type()))?;
            push_json_string(out, values.value(row));
        }
        DataType::LargeUtf8 => {
            let values = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| json_type_error(name, column.data_type()))?;
            push_json_string(out, values.value(row));
        }
        DataType::Utf8View => {
            let values = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| json_type_error(name, column.data_type()))?;
            push_json_string(out, values.value(row));
        }
        _ => return Err(json_type_error(name, column.data_type())),
    }
    Ok(())
}

fn json_finite(out: &mut String, value: f64, name: &str) -> Result<()> {
    if value.is_finite() {
        out.push_str(&value.to_string());
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "run_maintenance result column `{name}` holds a non-finite float (refusing to emit invalid JSON)"
        )))
    }
}

fn json_type_error(name: &str, data_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "run_maintenance result column `{name}` has type `{data_type}` (refusing to guess its JSON shape)"
    ))
}
