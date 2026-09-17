use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, Expr, Value, ValueWithSpan};
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind, TableIdent, table::Table};
use repark_iceberg::write::{SnapshotRefKind, list_snapshot_refs};

use super::{CallArgs, resolve_table_ident};
use crate::call_args::{expr_as_i64, expr_as_string, expr_as_timestamp_ms};
use crate::{iceberg_err, reregister};

struct RefTarget {
    snapshot_id: i64,
    is_branch: bool,
}

async fn load_ref_map(table: &Table) -> Result<HashMap<String, RefTarget>> {
    let mut refs = HashMap::new();
    for (name, kind, snapshot_id) in list_snapshot_refs(table).await.map_err(iceberg_err)? {
        refs.insert(
            name,
            RefTarget {
                snapshot_id,
                is_branch: kind == SnapshotRefKind::Branch,
            },
        );
    }
    Ok(refs)
}

fn is_ancestor_of(table: &Table, ancestor: i64, descendant: i64) -> bool {
    let metadata = table.metadata();
    let mut current = Some(descendant);
    while let Some(id) = current {
        if id == ancestor {
            return true;
        }
        current = metadata
            .snapshot_by_id(id)
            .and_then(|s| s.parent_snapshot_id());
    }
    false
}

fn latest_ancestor_older_than(table: &Table, head: i64, timestamp_ms: i64) -> Option<i64> {
    let metadata = table.metadata();
    let mut best: Option<(i64, i64)> = None;
    let mut current = Some(head);
    while let Some(id) = current {
        let snapshot = metadata.snapshot_by_id(id);
        if let Some(snapshot) = snapshot {
            let ts = snapshot.timestamp_ms();
            if ts < timestamp_ms && best.is_none_or(|(_, best_ts)| ts > best_ts) {
                best = Some((id, ts));
            }
        }
        current = snapshot.and_then(|s| s.parent_snapshot_id());
    }
    best.map(|(id, _)| id)
}

fn illegal_argument(message: String) -> DataFusionError {
    DataFusionError::Configuration(message)
}

fn engine_refusal(message: String) -> DataFusionError {
    iceberg_err(iceberg::Error::new(ErrorKind::DataInvalid, message))
}

fn string_literal_value(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => Some(text),
        _ => None,
    }
}

fn timestamp_typed_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::TypedString(typed) if matches!(typed.data_type, SqlDataType::Timestamp(..)) => {
            match &typed.value.value {
                Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text),
                _ => None,
            }
        }
        _ => None,
    }
}

fn cast_invalid_input(value: &str, from: &str, to: &str) -> DataFusionError {
    engine_refusal(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"{from}\" cannot be cast to \"{to}\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018"
    ))
}

fn missing_routine_arg(routine: &str, param: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `{routine}` because the parameter named `{param}` is required, but the routine call did not supply a value. Please update the routine call to supply an argument value (either positionally at index 0 or by name) and retry the query again. SQLSTATE: 4274K"
    ))
}

fn invalid_typed_literal(type_name: &str, value: &str) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "[INVALID_TYPED_LITERAL] The value of the typed literal \"{type_name}\" is invalid: '{value}'. SQLSTATE: 42604"
        ))),
        None,
    )
}

fn timestamp_type_mismatch(raw: &str) -> DataFusionError {
    let kind = if raw.parse::<i32>().is_ok() {
        "INT"
    } else {
        "BIGINT"
    };
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve CALL due to data type mismatch: The second parameter requires the \"TIMESTAMP\" type, however \"{raw}\" has the type \"{kind}\". SQLSTATE: 42K09"
    ))
}

async fn load_call_table(
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    table_arg: &str,
) -> Result<(TableIdent, Table)> {
    let ident = resolve_table_ident(catalog_name, table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    Ok((ident, table))
}

async fn reregister_namespace(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    ident: &TableIdent,
) -> Result<()> {
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(catalog), catalog_name, &namespace).await
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_fast_forward(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "branch", "to"])?;
    args.reject_excess_positional(3)?;
    let table_arg = args.require_string("table", 0)?;
    let branch = args.require_string("branch", 1)?;
    let target = args.require_string("to", 2)?;
    let (ident, table) = load_call_table(&catalog, catalog_name, &table_arg).await?;
    let refs = load_ref_map(&table).await?;
    let to_snapshot = refs
        .get(&target)
        .map(|r| r.snapshot_id)
        .ok_or_else(|| illegal_argument(format!("Ref does not exist: {target}")))?;
    let previous = match refs.get(&branch) {
        None => None,
        Some(existing) => {
            if !existing.is_branch {
                return Err(illegal_argument(format!(
                    "Ref {branch} is a tag not a branch"
                )));
            }
            if existing.snapshot_id != to_snapshot
                && !is_ancestor_of(&table, existing.snapshot_id, to_snapshot)
            {
                return Err(illegal_argument(format!(
                    "Cannot fast-forward: {branch} is not an ancestor of {target}"
                )));
            }
            Some(existing.snapshot_id)
        }
    };
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().fast_forward(&branch, &target);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = tx.commit(catalog.as_ref()).await.map_err(iceberg_err)?;
    let updated = committed
        .metadata()
        .snapshot_for_ref(&branch)
        .map_or(to_snapshot, |s| s.snapshot_id());
    reregister_namespace(ctx, &catalog, catalog_name, &ident).await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("branch_updated", DataType::Utf8, false),
        Field::new("previous_ref", DataType::Int64, true),
        Field::new("updated_ref", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![branch.as_str()])),
            Arc::new(Int64Array::from(vec![previous])),
            Arc::new(Int64Array::from(vec![updated])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_cherrypick_snapshot(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "snapshot_id"])?;
    args.reject_excess_positional(2)?;
    let table_arg = args.require_string("table", 0)?;
    let snapshot_expr = args.named.get("snapshot_id").or(args.positional.get(1));
    let snapshot_id = match snapshot_expr {
        None => {
            return Err(missing_routine_arg("cherrypick_snapshot", "snapshot_id"));
        }
        Some(expr) => {
            if let Some(text) = string_literal_value(expr) {
                text.trim()
                    .parse::<i64>()
                    .map_err(|_| cast_invalid_input(text, "STRING", "BIGINT"))?
            } else {
                expr_as_i64(expr, "snapshot_id")?
            }
        }
    };
    let (ident, table) = load_call_table(&catalog, catalog_name, &table_arg).await?;
    let tx = Transaction::new(&table);
    let action = tx.cherry_pick(snapshot_id);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = tx.commit(catalog.as_ref()).await.map_err(iceberg_err)?;
    let current_snapshot_id = committed.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan("cherry-pick committed but table has no current snapshot".to_string())
    })?;
    reregister_namespace(ctx, &catalog, catalog_name, &ident).await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_snapshot_id", DataType::Int64, false),
        Field::new("current_snapshot_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![snapshot_id])),
            Arc::new(Int64Array::from(vec![current_snapshot_id])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

fn optional_ref_arg(args: &CallArgs, position: usize) -> Result<Option<String>> {
    if let Some(expr) = args.named.get("ref") {
        return expr_as_string(expr, "ref").map(Some);
    }
    match args.positional.get(position) {
        None => Ok(None),
        Some(expr) => expr_as_string(expr, "ref").map(Some),
    }
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_set_current_snapshot(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "snapshot_id", "ref"])?;
    args.reject_excess_positional(3)?;
    let table_arg = args.require_string("table", 0)?;
    let snapshot_id = args.optional_i64("snapshot_id", Some(1))?;
    let ref_name = optional_ref_arg(args, 2)?;
    let target = match (snapshot_id, ref_name) {
        (Some(_), Some(_)) | (None, None) => {
            return Err(illegal_argument(
                "Either snapshot_id or ref must be provided, not both".to_string(),
            ));
        }
        (Some(id), None) => Target::Id(id),
        (None, Some(name)) => Target::Ref(name),
    };
    let (ident, table) = load_call_table(&catalog, catalog_name, &table_arg).await?;
    let target_snapshot_id = match target {
        Target::Id(id) => {
            if table.metadata().snapshot_by_id(id).is_none() {
                return Err(engine_refusal(format!(
                    "Cannot roll back to unknown snapshot id: {id}"
                )));
            }
            id
        }
        Target::Ref(name) => {
            let refs = load_ref_map(&table).await?;
            refs.get(&name).map(|r| r.snapshot_id).ok_or_else(|| {
                engine_refusal(format!("Cannot find matching snapshot ID for ref {name}"))
            })?
        }
    };
    let previous_snapshot_id = table.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(format!("table `{table_arg}` has no current snapshot"))
    })?;
    let tx = Transaction::new(&table);
    let action = tx
        .manage_snapshots()
        .set_current_snapshot(target_snapshot_id);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = tx.commit(catalog.as_ref()).await.map_err(iceberg_err)?;
    let current_snapshot_id = committed.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(
            "set_current_snapshot committed but table has no current snapshot".to_string(),
        )
    })?;
    reregister_namespace(ctx, &catalog, catalog_name, &ident).await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("previous_snapshot_id", DataType::Int64, false),
        Field::new("current_snapshot_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![previous_snapshot_id])),
            Arc::new(Int64Array::from(vec![current_snapshot_id])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

enum Target {
    Id(i64),
    Ref(String),
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_rollback_to_timestamp(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "timestamp"])?;
    args.reject_excess_positional(2)?;
    let table_arg = args.require_string("table", 0)?;
    let timestamp_expr = args.named.get("timestamp").or(args.positional.get(1));
    let timestamp_ms = match timestamp_expr {
        None => {
            return Err(missing_routine_arg("rollback_to_timestamp", "timestamp"));
        }
        Some(Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        })) => return Err(timestamp_type_mismatch(raw)),
        Some(expr) => match expr_as_timestamp_ms(expr, "timestamp") {
            Ok(timestamp_ms) => timestamp_ms,
            Err(original) => {
                if let Some(text) = string_literal_value(expr) {
                    return Err(cast_invalid_input(text, "STRING", "TIMESTAMP"));
                }
                if let Some(text) = timestamp_typed_literal(expr) {
                    return Err(invalid_typed_literal("TIMESTAMP", text));
                }
                return Err(original);
            }
        },
    };
    let (ident, table) = load_call_table(&catalog, catalog_name, &table_arg).await?;
    let previous_snapshot_id = table.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "table `{table_arg}` has no current snapshot to roll back from"
        ))
    })?;
    let selected = latest_ancestor_older_than(&table, previous_snapshot_id, timestamp_ms)
        .ok_or_else(|| {
            illegal_argument(format!(
                "Cannot roll back, no valid snapshot older than: {timestamp_ms}"
            ))
        })?;
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().rollback_to(selected);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = tx.commit(catalog.as_ref()).await.map_err(iceberg_err)?;
    let current_snapshot_id = committed.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan("rollback committed but table has no current snapshot".to_string())
    })?;
    reregister_namespace(ctx, &catalog, catalog_name, &ident).await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("previous_snapshot_id", DataType::Int64, false),
        Field::new("current_snapshot_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![previous_snapshot_id])),
            Arc::new(Int64Array::from(vec![current_snapshot_id])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}
