//! MERGE `WHEN NOT MATCHED` INSERT machinery: clause→projection lowering, the source-only
//! execution seam, and the ANSI store-assignment gate.
//!
//! Split out of `mod.rs` (declared move, 2026-08-15 M4/M9 unit — `mod.rs` sits at its
//! `check_rust_file_size` ceiling). Two Spark-parity rules live here:
//!
//! * **Source-only resolution (audit M4).** Spark resolves NOT MATCHED conditions and `VALUES`
//!   expressions against the SOURCE plan only (`ResolveMergeIntoTable` resolves `InsertAction`
//!   under `Project(Nil, sourceTable)`); a target-column reference is an analysis error. The
//!   generated insert SQL therefore scopes the outer query to the source columns plus a
//!   sentinel copy of the target `_pos` — a target reference fails to resolve loudly instead
//!   of silently reading the LEFT-JOIN NULL (see [`super::MergeSql::insert_sql`]).
//! * **ANSI store assignment (audit M9).** Spark's DML store-assignment policy
//!   (`Cast.canANSIStoreAssign`, default `spark.sql.storeAssignmentPolicy=ANSI`) is far
//!   narrower than a CAST: boolean→int, timestamp→bigint, string→numeric are rejected at
//!   ANALYSIS time (`INCOMPATIBLE_DATA_FOR_TABLE`), never written. The engine's write path
//!   casts with the full arrow kernel (`cast_one_batch_to_write_schema`, strict), so without a
//!   gate those pairs would silently commit reinterpreted values. [`insert_stream_checked`]
//!   validates the PLANNED schema against the write schema before a single batch streams.
//!   [`update_stream_checked`] is the UPDATE twin: it plans each `SET` expression in isolation
//!   (no `CASE` unification) and runs the **same** [`ansi_store_assignable`] matrix before the
//!   copy-on-write / merge-on-read rewrite SQL is required to type-check. DataFusion `CASE`
//!   arms would otherwise refuse bool→int at plan time with an incidental coercion error, so
//!   illegal pairs would never reach a post-plan gate (audit M9 residual BL-4).

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use futures::Stream;

use super::{InsertAction, InsertClause, MatchedAction, quote_ident, resolve_schema_field_name};

/// Project an INSERT clause onto the target schema: named columns take their VALUES expression,
/// everything else becomes NULL. Rejects unknown columns, arity mismatches, and NULL-filling a
/// required column — before anything is written. Only explicit clauses reach this point:
/// `INSERT *` is expanded by [`super::expand_star_clauses`] first.
pub(super) fn insert_projection(
    clause: &InsertClause,
    write_schema: &ArrowSchema,
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
    // Case-insensitive resolution (Spark `caseSensitive=false`); project under canonical schema
    // field names so values land on the right columns (audit BUG-006).
    let mut seen = HashSet::with_capacity(columns.len());
    let mut canonical_columns: Vec<String> = Vec::with_capacity(columns.len());
    for column in &columns {
        let Some(canonical) = resolve_schema_field_name(write_schema, column) else {
            return Err(DataFusionError::Plan(format!(
                "MERGE INSERT column `{column}` does not exist in the target table"
            )));
        };
        if !seen.insert(canonical.to_ascii_lowercase()) {
            return Err(DataFusionError::Plan(format!(
                "MERGE INSERT clause names column `{column}` more than once"
            )));
        }
        canonical_columns.push(canonical.to_string());
    }
    let assigned: HashMap<&str, &str> = canonical_columns
        .iter()
        .map(String::as_str)
        .zip(values_sql.iter().map(String::as_str))
        .collect();
    let mut projection = Vec::with_capacity(write_schema.fields().len());
    for field in write_schema.fields() {
        let quoted = quote_ident(field.name());
        match assigned.get(field.name().as_str()) {
            Some(expr) => projection.push(format!("({expr}) AS {quoted}")),
            None if field.is_nullable() => {
                projection.push(format!("NULL AS {quoted}"));
            }
            None => {
                return Err(DataFusionError::Plan(format!(
                    "MERGE INSERT clause leaves required column `{}` unassigned",
                    field.name()
                )));
            }
        }
    }
    Ok(projection.join(", "))
}

/// Plan one insert-clause query, gate its PLANNED schema through ANSI store assignment, then
/// stream. Counts as one logical SQL pass (PERF-19), same as the [`super::stream_sql`] seam it
/// replaces on the insert path — validation adds no extra plan.
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
/// The insert projection aliases columns to the canonical target names in write-schema order, so
/// the zip is positional with a name assert (a mismatch is an executor bug, not user error).
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

/// ===========================================================================================
/// UPDATE twin of [`insert_stream_checked`]: gate `SET` assignment types, then stream rewrite.
///
/// The probe plans each assignment expression against the same source⋈target join **without**
/// wrapping it in the rewrite `CASE` (THEN assignment ELSE `t.col`). DataFusion CASE-arm
/// unification refuses bool→int at plan time, which would hide this gate behind an incidental
/// coercion error. Illegal pairs therefore fail with the ANSI needle; the CASE rewrite is
/// planned only after every `SET` pair is store-assignable. The probe is analysis-only (not a
/// PERF-19 logical target pass).
/// ===========================================================================================
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
    let Some((probe_sql, target_columns)) = update_assignment_probe_sql(sql, write_schema)? else {
        return Ok(());
    };
    let dataframe = ctx.sql(&probe_sql).await?;
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
///
/// `None` when no UPDATE assignments exist (DELETE-only / empty matched). Aliases are
/// synthetic so two clauses can assign the same target column independently.
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
            let Some(canonical) = resolve_schema_field_name(write_schema, column) else {
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

/// CAST a validated SET expression to the target Arrow type so the rewrite `CASE`
/// (THEN assignment ELSE `t.col`) unifies. Must run **after** the ANSI gate: wrapping
/// first would make bool→int look like Int32→Int32 and silently write `1`.
pub(super) fn store_assignment_then_sql(expr: &str, target_type: &DataType) -> String {
    let type_name = target_type.to_string().replace('\'', "''");
    format!("arrow_cast(({expr}), '{type_name}')")
}

/// Shared refusal — path label is `INSERT` or `UPDATE SET`; the matrix is not forked.
fn refuse_unless_ansi_store_assignable(
    path: &str,
    column: &str,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<()> {
    let src = normalize_for_assignment(source_type);
    let dst = normalize_for_assignment(target_type);
    if !ansi_store_assignable(src, dst) {
        return Err(DataFusionError::Plan(format!(
            "MERGE {path} cannot store-assign column `{column}`: source type {source_type} is not \
             ANSI-store-assignable to target type {target_type} (Spark INCOMPATIBLE_DATA_FOR_TABLE; \
             add an explicit CAST only if the reinterpretation is intended semantics)"
        )));
    }
    Ok(())
}

/// Strip wrappers that do not change assignability (dictionary encoding).
pub(super) fn normalize_for_assignment(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => normalize_for_assignment(value),
        other => other,
    }
}

/// Spark `Cast.canANSIStoreAssign`, translated to Arrow types (v1: nested types must be
/// identical — Spark's per-field recursion is a named residual).
pub(super) fn ansi_store_assignable(src: &DataType, dst: &DataType) -> bool {
    use DataType::{
        Binary, Boolean, Date32, Date64, LargeBinary, LargeUtf8, Null, Timestamp, Utf8, Utf8View,
    };
    if src == dst {
        return true;
    }
    // NullType → anything (the projection NULL-fills nullable columns as untyped NULL).
    if matches!(src, Null) {
        return true;
    }
    // NumericType → NumericType (widening AND narrowing — overflow is the strict runtime cast's
    // ANSI error, exactly Spark's split between analysis-legal and runtime-failing).
    if src.is_numeric() && dst.is_numeric() {
        return true;
    }
    // AtomicType → StringType.
    let is_string = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    let is_atomic = |t: &DataType| {
        t.is_numeric()
            || matches!(
                t,
                Boolean | Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | Date32 | Date64
            )
            || matches!(t, Timestamp(_, _))
    };
    if is_string(dst) && is_atomic(src) {
        return true;
    }
    // String width variants among themselves.
    if is_string(src) && is_string(dst) {
        return true;
    }
    // Date ↔ Timestamp, both directions; timestamp unit/annotation changes within timestamps.
    let is_date = |t: &DataType| matches!(t, Date32 | Date64);
    let is_ts = |t: &DataType| matches!(t, Timestamp(_, _));
    if (is_date(src) && (is_date(dst) || is_ts(dst)))
        || (is_ts(src) && (is_ts(dst) || is_date(dst)))
    {
        return true;
    }
    // Binary width variants.
    if matches!(src, Binary | LargeBinary) && matches!(dst, Binary | LargeBinary) {
        return true;
    }
    false
}

#[cfg(test)]
mod insert_gate_tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    use super::ansi_store_assignable;

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
