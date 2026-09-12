use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, Value, ValueWithSpan};
use datafusion::sql::sqlparser::tokenizer::Span;
use iceberg::TableIdent;
use repark_core::CatalogRegistry;

use super::plan_partitioning::{PlanFrameRow, collect_plan_rows};
use super::{CallArgs, resolve_table_ident};

const LOOKUP_TARGET: u64 = 1;

struct ApplyStep {
    ordinal: i32,
    procedure: String,
    arguments: String,
    kind: StepKind,
}

enum StepKind {
    Alter,
    RewriteDataFiles,
    RewriteManifests,
    ExpireSnapshots,
}

pub(super) async fn execute_apply_partitioning(
    ctx: &SessionContext,
    catalog_name: &str,
    args: &CallArgs,
    catalogs: &CatalogRegistry,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["dry_run", "plan_id", "table"])?;
    args.reject_excess_positional(3)?;
    let table_arg = args.require_string("table", 0)?;
    let plan_id = args.require_string("plan_id", 1)?;
    let dry_run = args.optional_bool("dry_run", Some(2))?.unwrap_or(true);
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let planned = collect_plan_rows(ctx, catalog_name, catalogs, &table_arg, LOOKUP_TARGET).await?;
    let selected = planned
        .iter()
        .find(|row| row.plan_id == plan_id)
        .ok_or_else(|| missing_plan_id(&table_arg, &plan_id))?;
    let steps = build_steps(catalog_name, &ident, selected)?;
    if dry_run {
        return steps_dataframe(ctx, &steps, &plan_id, "dry_run");
    }
    Box::pin(apply_steps(
        ctx,
        catalogs,
        catalog_name,
        &table_arg,
        &steps,
        &plan_id,
    ))
    .await
}

fn missing_plan_id(table_arg: &str, plan_id: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "apply_partitioning refuses table `{table_arg}`: plan_id `{plan_id}` matches no \
         candidate at the current snapshot (the snapshot moved, or the id is not from this \
         table); re-run plan_partitioning and pass a fresh id"
    ))
}

fn qualified_table(catalog_name: &str, ident: &TableIdent) -> String {
    let mut parts = vec![catalog_name.to_string()];
    parts.extend(ident.namespace().as_ref().iter().cloned());
    parts.push(ident.name().to_string());
    parts.join(".")
}

fn alter_from_ddl(catalog_name: &str, ident: &TableIdent, ddl: &str) -> Vec<String> {
    if ddl.is_empty() {
        return Vec::new();
    }
    let table = qualified_table(catalog_name, ident);
    ddl.split("; ")
        .filter(|statement| !statement.is_empty())
        .map(
            |statement| match statement.split_once("ADD PARTITION FIELD ") {
                Some((_, field)) => format!("ALTER TABLE {table} ADD PARTITION FIELD {field}"),
                None => statement.to_string(),
            },
        )
        .collect()
}

fn next_ordinal(ordinal: i32) -> Result<i32> {
    ordinal.checked_add(1).ok_or_else(|| {
        DataFusionError::Plan("apply_partitioning step ordinal overflow".to_string())
    })
}

fn build_steps(
    catalog_name: &str,
    ident: &TableIdent,
    row: &PlanFrameRow,
) -> Result<Vec<ApplyStep>> {
    let mut steps = Vec::new();
    let mut ordinal: i32 = 1;
    for sql in alter_from_ddl(catalog_name, ident, &row.ddl) {
        steps.push(ApplyStep {
            ordinal,
            procedure: "ALTER TABLE".to_string(),
            arguments: sql,
            kind: StepKind::Alter,
        });
        ordinal = next_ordinal(ordinal)?;
    }
    let calls: Vec<&str> = row
        .calls
        .split("; ")
        .filter(|statement| !statement.is_empty())
        .collect();
    if calls.len() != 3 {
        return Err(DataFusionError::Plan(format!(
            "apply_partitioning expected three maintenance calls, got {}",
            calls.len()
        )));
    }
    let kinds = [
        StepKind::RewriteDataFiles,
        StepKind::RewriteManifests,
        StepKind::ExpireSnapshots,
    ];
    let names = [
        "rewrite_data_files",
        "rewrite_manifests",
        "expire_snapshots",
    ];
    for ((kind, name), call) in kinds.into_iter().zip(names).zip(calls) {
        if !call.contains(name) {
            return Err(DataFusionError::Plan(format!(
                "apply_partitioning expected `{name}` call, got `{call}`"
            )));
        }
        steps.push(ApplyStep {
            ordinal,
            procedure: name.to_string(),
            arguments: call.to_string(),
            kind,
        });
        ordinal = next_ordinal(ordinal)?;
    }
    Ok(steps)
}

fn string_expr(value: &str) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(value.to_string()),
        span: Span::empty(),
    })
}

fn table_args(table_arg: &str) -> CallArgs {
    CallArgs {
        named: HashMap::from([("table".to_string(), string_expr(table_arg))]),
        positional: Vec::new(),
    }
}

async fn run_step(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    table_arg: &str,
    step: &ApplyStep,
) -> Result<String> {
    match step.kind {
        StepKind::Alter => {
            let frame = Box::pin(crate::router::execute(ctx, catalogs, &step.arguments)).await?;
            frame.collect().await?;
            Ok("committed".to_string())
        }
        StepKind::RewriteDataFiles => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            let frame = super::rewrite_data_files::execute_rewrite_data_files(
                ctx,
                catalog,
                catalog_name,
                &table_args(table_arg),
            )
            .await?;
            frame.collect().await?;
            Ok("applied".to_string())
        }
        StepKind::RewriteManifests => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            let frame = super::rewrite_manifests::execute_rewrite_manifests(
                ctx,
                catalog,
                catalog_name,
                &table_args(table_arg),
            )
            .await?;
            frame.collect().await?;
            Ok("applied".to_string())
        }
        StepKind::ExpireSnapshots => {
            let catalog = Arc::clone(crate::catalog_handle(catalogs, catalog_name)?);
            let frame =
                super::execute_expire_snapshots(ctx, catalog, catalog_name, &table_args(table_arg))
                    .await?;
            frame.collect().await?;
            Ok("applied".to_string())
        }
    }
}

async fn apply_steps(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    table_arg: &str,
    steps: &[ApplyStep],
    plan_id: &str,
) -> Result<DataFrame> {
    let mut rows: Vec<(i32, String, String, String, String)> = Vec::new();
    for step in steps {
        match run_step(ctx, catalogs, catalog_name, table_arg, step).await {
            Ok(result) => rows.push((
                step.ordinal,
                step.procedure.clone(),
                step.arguments.clone(),
                "applied".to_string(),
                result,
            )),
            Err(error) => {
                return Err(DataFusionError::Plan(format!(
                    "apply_partitioning failed at step {} ({}): {error}; earlier steps stay \
                     committed (this is not a transaction)",
                    step.ordinal, step.arguments
                )));
            }
        }
    }
    filled_dataframe(ctx, &rows, plan_id)
}

fn steps_dataframe(
    ctx: &SessionContext,
    steps: &[ApplyStep],
    plan_id: &str,
    status: &str,
) -> Result<DataFrame> {
    let rows: Vec<(i32, String, String, String, String)> = steps
        .iter()
        .map(|step| {
            (
                step.ordinal,
                step.procedure.clone(),
                step.arguments.clone(),
                status.to_string(),
                String::new(),
            )
        })
        .collect();
    filled_dataframe(ctx, &rows, plan_id)
}

fn filled_dataframe(
    ctx: &SessionContext,
    rows: &[(i32, String, String, String, String)],
    plan_id: &str,
) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("procedure", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("result", DataType::Utf8, false),
        Field::new("plan_id", DataType::Utf8, false),
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
            Arc::new(StringArray::from(vec![plan_id; rows.len()])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}
