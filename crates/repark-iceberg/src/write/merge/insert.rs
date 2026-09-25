//! MERGE `WHEN NOT MATCHED` lowering and write validation.

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use futures::Stream;

use super::{InsertAction, InsertClause, MatchedAction, quote_ident};
use crate::write::insert_defaults::{ColumnDefaults, column_defaults, schema_has_primitive_fill};
use crate::write::name_resolution::{dedup_key, resolve_arrow_field, resolve_write_column};
use crate::write::store_assign::{self, MERGE_SPARK_CLASS};
use iceberg::table::Table;

pub(super) fn table_projection(
    clause: &InsertClause,
    table: &Table,
    write_schema: &ArrowSchema,
    case_insensitive: bool,
) -> Result<String> {
    let current = table.metadata().current_schema();
    if !schema_has_primitive_fill(current) {
        return insert_projection_with_defaults(
            clause,
            write_schema,
            case_insensitive,
            &ColumnDefaults::new(),
        );
    }
    let defaults = column_defaults(current)?;
    insert_projection_with_defaults(clause, write_schema, case_insensitive, &defaults)
}

pub(super) fn insert_projection_with_defaults(
    clause: &InsertClause,
    write_schema: &ArrowSchema,
    case_insensitive: bool,
    defaults: &ColumnDefaults,
) -> Result<String> {
    let InsertAction::Explicit {
        columns: named,
        values_sql,
    } = &clause.action
    else {
        return Err(DataFusionError::Internal(
            "MERGE `INSERT *` reached SQL generation unexpanded (executor bug)".to_string(),
        ));
    };
    let columns: Vec<String> = if named.is_empty() {
        write_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        named.clone()
    };
    if columns.len() != values_sql.len() {
        return Err(DataFusionError::Plan(format!(
            "MERGE INSERT clause has {} columns but {} VALUES expressions",
            columns.len(),
            values_sql.len()
        )));
    }
    let mut seen = HashSet::with_capacity(columns.len());
    let mut canonical_columns: Vec<String> = Vec::with_capacity(columns.len());
    for column in &columns {
        let canonical = resolve_write_column(write_schema, column, case_insensitive, || {
            format!("MERGE INSERT column `{column}` does not exist in the target table")
        })?;
        if !seen.insert(dedup_key(&canonical, case_insensitive)) {
            return Err(DataFusionError::Plan(format!(
                "MERGE INSERT clause names column `{column}` more than once"
            )));
        }
        canonical_columns.push(canonical);
    }
    let assigned: HashMap<&str, &str> = canonical_columns
        .iter()
        .map(String::as_str)
        .zip(values_sql.iter().map(String::as_str))
        .collect();
    let mut projection = Vec::with_capacity(write_schema.fields().len());
    for field in write_schema.fields() {
        let quoted = quote_ident(field.name());
        if let Some(expr) = assigned.get(field.name().as_str()) {
            projection.push(format!("({expr}) AS {quoted}"));
        } else if let Some(fill) = defaults.get(&field.name().to_ascii_lowercase()) {
            projection.push(format!("({}) AS {quoted}", fill.sql_text()?));
        } else if field.is_nullable() {
            projection.push(format!("NULL AS {quoted}"));
        } else {
            return Err(DataFusionError::Plan(format!(
                "MERGE INSERT clause leaves required column `{}` unassigned",
                field.name()
            )));
        }
    }
    Ok(projection.join(", "))
}

/// Plan one insert-clause query, gate its schema through ANSI store assignment, then stream.
pub(super) async fn insert_stream_checked(
    ctx: &SessionContext,
    sql: &str,
    write_schema: &ArrowSchema,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Unpin + use<>> {
    super::note_logical_target_sql_pass();
    let dataframe = ctx.sql(sql).await?;
    validate_insert_store_assignment(dataframe.schema().fields(), write_schema)?;
    dataframe.execute_stream().await
}

/// The M9 gate: every planned insert column must be ANSI-store-assignable to its target column.
fn validate_insert_store_assignment(
    planned: &datafusion::arrow::datatypes::Fields,
    write_schema: &ArrowSchema,
) -> Result<()> {
    if planned.len() != write_schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "MERGE INSERT planned {} columns for a {}-column write schema (executor bug)",
            planned.len(),
            write_schema.fields().len()
        )));
    }
    for (plan_field, target_field) in planned.iter().zip(write_schema.fields()) {
        if plan_field.name() != target_field.name() {
            return Err(DataFusionError::Internal(format!(
                "MERGE INSERT planned column `{}` out of position for target `{}` (executor bug)",
                plan_field.name(),
                target_field.name()
            )));
        }
        refuse_unless_ansi_store_assignable(
            "INSERT",
            target_field.name(),
            plan_field.data_type(),
            target_field.data_type(),
        )?;
    }
    Ok(())
}

/// UPDATE twin of [`insert_stream_checked`]: gate `SET` assignment types, then stream rewrite.
pub(super) async fn update_stream_checked(
    ctx: &SessionContext,
    sql: &super::MergeSql<'_>,
    rewrite_sql: &str,
    write_schema: &ArrowSchema,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Unpin + use<>> {
    validate_update_store_assignment(ctx, sql, write_schema).await?;
    super::note_logical_target_sql_pass();
    let dataframe = ctx.sql(rewrite_sql).await?;
    dataframe.execute_stream().await
}

/// Plan every `UPDATE SET` expression in isolation and run the shared ANSI matrix.
pub(super) async fn validate_update_store_assignment(
    ctx: &SessionContext,
    sql: &super::MergeSql<'_>,
    write_schema: &ArrowSchema,
) -> Result<()> {
    if let Some((probe_sql, target_columns)) = update_assignment_probe_sql(sql, write_schema)? {
        gate_update_probe(ctx, write_schema, &probe_sql, target_columns).await?;
    }
    if let Some((probe_sql, target_columns)) =
        super::not_matched_by_source::update_assignment_probe_sql(sql, write_schema)?
    {
        gate_update_probe(ctx, write_schema, &probe_sql, target_columns).await?;
    }
    Ok(())
}

async fn gate_update_probe(
    ctx: &SessionContext,
    write_schema: &ArrowSchema,
    probe_sql: &str,
    target_columns: Vec<String>,
) -> Result<()> {
    let dataframe = ctx.sql(probe_sql).await?;
    let planned = dataframe.schema().fields();
    if planned.len() != target_columns.len() {
        return Err(DataFusionError::Internal(format!(
            "MERGE UPDATE SET probe planned {} columns for {} assignments (executor bug)",
            planned.len(),
            target_columns.len()
        )));
    }
    for (plan_field, target_name) in planned.iter().zip(target_columns) {
        let target_field = write_schema
            .field_with_name(&target_name)
            .map_err(|error| {
                DataFusionError::Internal(format!(
                    "MERGE UPDATE SET target `{target_name}` missing from write schema: {error}"
                ))
            })?;
        refuse_unless_ansi_store_assignable(
            "UPDATE SET",
            &target_name,
            plan_field.data_type(),
            target_field.data_type(),
        )?;
    }
    Ok(())
}

/// `SELECT (expr) AS aN, … FROM source JOIN target ON on` — one column per SET pair.
fn update_assignment_probe_sql(
    sql: &super::MergeSql<'_>,
    write_schema: &ArrowSchema,
) -> Result<Option<(String, Vec<String>)>> {
    let mut projections = Vec::new();
    let mut target_columns = Vec::new();
    for clause in &sql.spec.matched {
        let MatchedAction::Update { assignments } = &clause.action else {
            continue;
        };
        for (column, expr) in assignments {
            let Some(canonical) =
                resolve_arrow_field(write_schema, column, sql.spec.case_insensitive)
            else {
                return Err(DataFusionError::Internal(format!(
                    "MERGE UPDATE SET column `{column}` missing after validate_update_columns \
                     (executor bug)"
                )));
            };
            let alias = quote_ident(&format!("a{}", projections.len()));
            projections.push(format!("({expr}) AS {alias}"));
            target_columns.push(canonical.to_string());
        }
    }
    if projections.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        format!(
            "SELECT {projection} FROM {source} JOIN {target} ON {on}",
            projection = projections.join(", "),
            source = sql.source_from(),
            target = sql.target_from(),
            on = sql.spec.on_sql,
        ),
        target_columns,
    )))
}

/// CAST a validated SET expression to the target Arrow type so the rewrite `CASE` unifies.
pub(super) fn store_assignment_then_sql(expr: &str, target_type: &DataType) -> String {
    crate::write::update_cast::store_assignment_cast_sql(expr, target_type)
}

/// Shared refusal — path label is `INSERT` or `UPDATE SET`; the matrix is not forked.
fn refuse_unless_ansi_store_assignable(
    path: &str,
    column: &str,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<()> {
    store_assign::refuse_unless_ansi_store_assignable(
        &format!("MERGE {path}"),
        MERGE_SPARK_CLASS,
        column,
        source_type,
        target_type,
    )
}

#[cfg(test)]
mod insert_gate_tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    use crate::write::store_assign::ansi_store_assignable;

    #[test]
    fn spark_ansi_store_assign_matrix() {
        use DataType::*;
        let utc: Option<std::sync::Arc<str>> = Some(std::sync::Arc::from("UTC"));
        // Legal: identity, null-fill, numeric↔numeric, atomic→string, date↔timestamp.
        assert!(ansi_store_assignable(&Int64, &Int64));
        assert!(ansi_store_assignable(&Null, &Int32));
        assert!(ansi_store_assignable(&Int32, &Int64));
        assert!(ansi_store_assignable(&Int64, &Int32)); // narrowing legal; overflow = runtime
        assert!(ansi_store_assignable(&Decimal128(38, 10), &Float64));
        assert!(ansi_store_assignable(&Boolean, &Utf8));
        assert!(ansi_store_assignable(
            &Timestamp(TimeUnit::Microsecond, utc.clone()),
            &Utf8
        ));
        assert!(ansi_store_assignable(
            &Date32,
            &Timestamp(TimeUnit::Microsecond, utc.clone())
        ));
        assert!(ansi_store_assignable(
            &Timestamp(TimeUnit::Microsecond, utc.clone()),
            &Date32
        ));
        // Illegal in Spark ANSI store assignment (audit M9 repro pairs first).
        assert!(!ansi_store_assignable(&Boolean, &Int32)); // r13b
        assert!(!ansi_store_assignable(
            &Timestamp(TimeUnit::Microsecond, utc.clone()),
            &Int64
        )); // r13c
        assert!(!ansi_store_assignable(&Utf8, &Int64)); // string→numeric
        assert!(!ansi_store_assignable(&Int64, &Boolean));
        assert!(!ansi_store_assignable(
            &Int64,
            &Timestamp(TimeUnit::Microsecond, utc)
        ));
        assert!(!ansi_store_assignable(&Utf8, &Boolean));
    }

    #[test]
    fn store_assignment_then_sql_arrow_casts_to_the_target_type() {
        assert_eq!(
            super::store_assignment_then_sql("s.b", &DataType::Utf8),
            "arrow_cast((s.b), 'Utf8')"
        );
    }

    #[test]
    fn update_set_refusal_uses_the_shared_needle_and_path_label() {
        let err = super::refuse_unless_ansi_store_assignable(
            "UPDATE SET",
            "flag",
            &DataType::Boolean,
            &DataType::Int32,
        )
        .expect_err("bool→int must refuse");
        let text = err.to_string();
        assert!(
            text.contains("not ANSI-store-assignable"),
            "needle missing: {text}"
        );
        assert!(text.contains("UPDATE SET"), "path label missing: {text}");
        assert!(
            text.contains("INCOMPATIBLE_DATA_FOR_TABLE"),
            "Spark class missing: {text}"
        );
    }
}
