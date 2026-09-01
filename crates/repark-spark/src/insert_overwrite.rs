//! `INSERT OVERWRITE` probing, stage-then-swap execution, and assignment-type guards.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{Expr as DataFusionExpr, ExprSchemable, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Insert, ObjectName, TableObject};
use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    name_parts, namespace_schema_name, refuse_read_only_dml_table_sql, reregister,
};
use crate::spark_ast;

// A non-empty source is staged before the table is replaced.

/// Monotonic counter for ephemeral `INSERT OVERWRITE` MemTable-fallback temp views.
pub(crate) static OW_MATERIALIZE_SEQ: AtomicU64 = AtomicU64::new(1);

/// `INSERT OVERWRITE` / `INSERT OVERWRITE TABLE` with a zero-row source must wipe the target.
/// # Errors
/// Propagates planning, schema-validation, and wipe errors as `DataFusionError`.
pub(crate) async fn execute_insert_overwrite(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    insert: &Insert,
) -> Result<DataFrame> {
    let table_name = match &insert.table {
        TableObject::TableName(name) => name,
        // Table-function / table-query targets are not an Iceberg table wipe surface.
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => {
            return spark_ast::execute_passthrough(ctx, catalogs, sql).await;
        }
    };
    let table_sql = table_name.to_string();
    if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
        return Err(DataFusionError::Plan(message));
    }

    if let Some(partition_exprs) = &insert.partitioned {
        return execute_partition_overwrite(
            ctx,
            catalogs,
            table_name,
            &table_sql,
            insert,
            partition_exprs,
        )
        .await;
    }

    if let Some(source) = &insert.source {
        let probe_sql = format!("SELECT 1 FROM ({source}) AS _repark_ow_probe LIMIT 1");
        let probe = spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
        let batches = probe.collect().await?;
        let empty = batches.iter().all(|batch| batch.num_rows() == 0);
        if empty {
            // Validate the original INSERT OVERWRITE plan **before** wiping.
            let _validated = ctx.sql(sql).await?;
            assert_empty_overwrite_types_assignment_compatible(
                ctx,
                catalogs,
                &table_sql,
                source,
                &insert.columns,
            )
            .await?;
            // Re-probe immediately before the wipe.
            let reprobe = spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
            let reprobe_batches = reprobe.collect().await?;
            let still_empty = reprobe_batches.iter().all(|batch| batch.num_rows() == 0);
            if still_empty {
                // Wipe via a **self-scan empty** statement, not a re-exec of the caller's source.
                let wipe_sql =
                    format!("INSERT OVERWRITE {table_sql} SELECT * FROM {table_sql} WHERE false");
                return spark_ast::execute_passthrough(ctx, catalogs, &wipe_sql).await;
            }
            // Fall through: re-probe saw rows → treat as non-empty (stage-then-swap path).
        }
        // Stage non-empty rows before replace-all.
        return insert_overwrite_from_staged_source(
            ctx,
            catalogs,
            table_name,
            &table_sql,
            source,
            &insert.columns,
        )
        .await;
    }

    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

/// Static or dynamic `INSERT OVERWRITE … PARTITION (…)`.
/// # Errors
/// Parse, empty-dynamic guard, staging, or commit failures as [`DataFusionError`].
async fn execute_partition_overwrite(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
    table_sql: &str,
    insert: &Insert,
    partition_exprs: &[datafusion::sql::sqlparser::ast::Expr],
) -> Result<DataFrame> {
    use repark_iceberg::write::{
        PartitionOverwritePlan, partition_overwrite_request_from_exprs, plan_partition_overwrite,
        refuse_empty_dynamic_overwrite, stage_static_partition_overwrite_files,
        write_overwrite_staged_files_from_stream,
    };

    let Some((catalog_name, catalog, table, branch)) =
        try_resolve_iceberg_overwrite_target(catalogs, table_name).await?
    else {
        return Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE … PARTITION requires a 3-part Iceberg table name, got `{table_sql}`"
        )));
    };
    let request = partition_overwrite_request_from_exprs(partition_exprs)?;
    let plan = plan_partition_overwrite(&table, &request)?;
    let source = insert.source.as_ref().ok_or_else(|| {
        DataFusionError::Plan(
            "INSERT OVERWRITE … PARTITION requires a SELECT or VALUES source".to_string(),
        )
    })?;
    let column_names: Vec<String> = insert.columns.iter().map(object_name_last).collect();
    let materialize_sql = format!("SELECT * FROM ({source}) AS _repark_ow_src");
    let source_df = spark_ast::execute_passthrough(ctx, catalogs, &materialize_sql).await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    match plan {
        PartitionOverwritePlan::Static(spec) => {
            let batches = source_df.collect().await?;
            let staged_files = stage_static_partition_overwrite_files(
                &table,
                batches,
                &spec.equalities,
                concurrency,
            )
            .await?;
            repark_iceberg::write::commit_overwrite_by_row_filter_to(
                &catalog,
                &table,
                staged_files,
                spec.predicate,
                branch.as_deref(),
            )
            .await?;
        }
        PartitionOverwritePlan::Dynamic => {
            let stream = source_df.execute_stream().await?;
            let staged_files =
                write_overwrite_staged_files_from_stream(&table, stream, column_names, concurrency)
                    .await?;
            refuse_empty_dynamic_overwrite(&staged_files)?;
            repark_iceberg::write::commit_replace_partitions_to(
                &catalog,
                &table,
                staged_files,
                branch.as_deref(),
            )
            .await?;
        }
    }
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, Arc::clone(&catalog), &catalog_name, &namespace).await?;
    ctx.read_empty()
}

/// Non-empty `INSERT OVERWRITE` — stage-then-swap (OV1 / OTH-004).
/// # Errors
/// Source stream, positional map/cast, write, or commit failures as [`DataFusionError`].
pub(crate) async fn insert_overwrite_from_staged_source(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
    table_sql: &str,
    source: &datafusion::sql::sqlparser::ast::Query,
    columns: &[ObjectName],
) -> Result<DataFrame> {
    match try_resolve_iceberg_overwrite_target(catalogs, table_name).await? {
        Some((catalog_name, catalog, table, branch)) => {
            insert_overwrite_iceberg_stage_then_swap(
                ctx,
                catalogs,
                &catalog_name,
                &catalog,
                &table,
                table_sql,
                source,
                columns,
                branch.as_deref(),
            )
            .await
        }
        None => {
            insert_overwrite_from_materialized_source_fallback(
                ctx, catalogs, table_sql, source, columns,
            )
            .await
        }
    }
}

/// Resolve `catalog.namespace….table` against the registry.
pub(crate) async fn try_resolve_iceberg_overwrite_target(
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
) -> Result<
    Option<(
        String,
        Arc<dyn Catalog>,
        iceberg::table::Table,
        Option<String>,
    )>,
> {
    let mut parts = name_parts(table_name);
    let branch = match crate::write_to_branch::split_write_ref_parts(&parts) {
        Some((table_parts, crate::write_to_branch::RefSelectorKind::Branch(name))) => {
            parts = table_parts;
            Some(name)
        }
        Some((_, crate::write_to_branch::RefSelectorKind::Tag)) => {
            return Err(crate::write_to_branch::tag_write_error("INSERT OVERWRITE"));
        }
        None => None,
    };
    if parts.len() < 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_leaf = parts[parts.len() - 1].clone();
    let namespace_parts = parts[1..parts.len() - 1].to_vec();
    let Ok(namespace) = NamespaceIdent::from_vec(namespace_parts) else {
        return Ok(None);
    };
    let Some(catalog) = catalogs.get(&catalog_name) else {
        return Ok(None);
    };
    let ident = TableIdent::new(namespace, table_leaf);
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(Some((catalog_name, Arc::clone(catalog), table, branch))),
        Err(error) => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE target `{table_name}` could not be loaded as an Iceberg table \
             (catalog `{catalog_name}` is registered — refusing silent MemTable fallback; D7): \
             {error}"
        ))),
    }
}

/// Stream → repark-write positional stage → row-count refuse → `commit_overwrite_replace_all`.
#[allow(clippy::too_many_arguments)] // catalogs threaded for SEC-02 passthrough gate only
pub(crate) async fn insert_overwrite_iceberg_stage_then_swap(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    catalog: &Arc<dyn Catalog>,
    table: &iceberg::table::Table,
    _table_sql: &str,
    source: &datafusion::sql::sqlparser::ast::Query,
    columns: &[ObjectName],
    branch: Option<&str>,
) -> Result<DataFrame> {
    use iceberg::spec::DataFile;

    // Fail isolation parse before staging.
    let _isolation = repark_iceberg::write::parse_overwrite_isolation(table)?;
    let column_names: Vec<String> = columns.iter().map(object_name_last).collect();
    let materialize_sql = format!("SELECT * FROM ({source}) AS _repark_ow_src");
    let source_df = spark_ast::execute_passthrough(ctx, catalogs, &materialize_sql).await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    // OV1 exclusive staging surface (Q9): positional D9 map + write; no catalog mutation yet.
    let staged_files = repark_iceberg::write::write_overwrite_staged_files_from_stream(
        table,
        stream,
        column_names,
        concurrency,
    )
    .await?;
    let total_rows: u64 = staged_files.iter().map(DataFile::record_count).sum();
    if total_rows == 0 {
        // BUG-001: never AlwaysTrue wipe on the non-empty-classified arm.
        return Err(DataFusionError::Execution(
            "INSERT OVERWRITE source became empty between emptiness probe and stream \
             materialization (refusing provider empty-overwrite wipe — audit BUG-001 / r20 A1 / \
             OV1)"
                .to_string(),
        ));
    }
    repark_iceberg::write::commit_overwrite_replace_all_to(catalog, table, staged_files, branch)
        .await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, Arc::clone(catalog), catalog_name, &namespace).await?;
    // Command shape — same as other DML (empty result frame).
    ctx.read_empty()
}

/// Legacy `MemTable` collect path — only when the target is not a resolvable Iceberg 3-part name.
pub(crate) async fn insert_overwrite_from_materialized_source_fallback(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_sql: &str,
    source: &datafusion::sql::sqlparser::ast::Query,
    columns: &[ObjectName],
) -> Result<DataFrame> {
    let materialize_sql = format!("SELECT * FROM ({source}) AS _repark_ow_src");
    let source_df = spark_ast::execute_passthrough(ctx, catalogs, &materialize_sql).await?;
    let batches = source_df.collect().await?;
    let empty = batches.iter().all(|batch| batch.num_rows() == 0);
    if empty {
        return Err(DataFusionError::Execution(
            "INSERT OVERWRITE source became empty between emptiness probe and materialization \
             (refusing provider empty-overwrite wipe — audit BUG-001 / r20 A1)"
                .to_string(),
        ));
    }

    // VALUES / literal materializations often widen every field to nullable.
    let batches = tighten_batch_nullability(batches)?;
    let schema = batches[0].schema();
    let mem_table = MemTable::try_new(schema, vec![batches]).map_err(|error| {
        DataFusionError::Internal(format!(
            "INSERT OVERWRITE materialize MemTable build failed: {error}"
        ))
    })?;
    let temp_name = {
        let sequence = OW_MATERIALIZE_SEQ.fetch_add(1, Ordering::Relaxed);
        format!("__repark_ow_mat_{sequence}")
    };
    let _ = ctx.deregister_table(temp_name.as_str());
    ctx.register_table(temp_name.as_str(), Arc::new(mem_table))
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register INSERT OVERWRITE materialize view {temp_name}: {error}"
            ))
        })?;

    let column_list = if columns.is_empty() {
        String::new()
    } else {
        let names: Vec<String> = columns.iter().map(object_name_last).collect();
        format!(" ({})", names.join(", "))
    };
    let insert_sql = format!("INSERT OVERWRITE {table_sql}{column_list} SELECT * FROM {temp_name}");
    let result = spark_ast::execute_passthrough(ctx, catalogs, &insert_sql).await;
    let _ = ctx.deregister_table(temp_name.as_str());
    result
}

/// Rebuild batches so columns with zero nulls across all batches are non-nullable.
/// # Errors
/// [`DataFusionError::Internal`] if a rebuilt batch fails schema validation.
pub(crate) fn tighten_batch_nullability(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let field_count = schema.fields().len();
    let mut nullable = vec![false; field_count];
    for batch in &batches {
        for (index, column) in batch.columns().iter().enumerate() {
            // Column count must match the lead-batch schema (RecordBatch invariant).
            if index < field_count && column.null_count() > 0 {
                nullable[index] = true;
            }
        }
    }
    // Zero-null columns → non-nullable (required), matching direct VALUES passthrough shape.
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| field.as_ref().clone().with_nullable(nullable[index]))
        .collect();
    let tight_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&tight_schema), batch.columns().to_vec()).map_err(
                |error| {
                    DataFusionError::Internal(format!(
                        "INSERT OVERWRITE materialize nullability tighten failed: {error}"
                    ))
                },
            )
        })
        .collect()
}

/// Empty INSERT OVERWRITE wipe must not run when source types are not assignment-compatible.
pub(crate) fn object_name_last(name: &datafusion::sql::sqlparser::ast::ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map_or_else(|| name.to_string(), |ident| ident.value.clone())
}

pub(crate) async fn assert_empty_overwrite_types_assignment_compatible(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_sql: &str,
    source: &datafusion::sql::sqlparser::ast::Query,
    columns: &[datafusion::sql::sqlparser::ast::ObjectName],
) -> Result<()> {
    let source_df = spark_ast::execute_passthrough(
        ctx,
        catalogs,
        &format!("SELECT * FROM ({source}) AS _repark_ow_types LIMIT 0"),
    )
    .await?;
    // Plan-time CAST rewrites Utf8→Int32.
    if logical_plan_has_unsafe_cast(source_df.logical_plan()) {
        return Err(DataFusionError::Plan(
            "INSERT OVERWRITE empty source evaluates a CAST that can fail at value level — \
             refusing full-table wipe (cast kernels never run on zero rows; the same statement \
             with rows fails at cast and keeps prior rows — P5C1-Q-001 / O4-C2-Q-001)"
                .to_string(),
        ));
    }
    let target_df = spark_ast::execute_passthrough(
        ctx,
        catalogs,
        &format!("SELECT * FROM {table_sql} LIMIT 0"),
    )
    .await?;
    let source_schema = source_df.schema();
    let target_schema = target_df.schema();

    let target_types: Vec<DataType> = if columns.is_empty() {
        target_schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    } else {
        // Case-insensitive name resolve.
        let mut types = Vec::with_capacity(columns.len());
        for column in columns {
            types.push(field_type_case_insensitive(
                target_schema,
                &object_name_last(column),
            )?);
        }
        types
    };

    if source_schema.fields().len() != target_types.len() {
        return Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE empty source has {} column(s) but target expects {} — refusing \
             full-table wipe (O4-C2-Q-001 / C5-Q-001)",
            source_schema.fields().len(),
            target_types.len()
        )));
    }

    for (index, target_type) in target_types.iter().enumerate() {
        let source_type = source_schema.field(index).data_type();
        if !assignment_types_compatible(source_type, target_type) {
            return Err(DataFusionError::Plan(format!(
                "INSERT OVERWRITE empty source column {index} type {source_type} is not \
                 assignment-compatible with target type {target_type} — refusing full-table \
                 wipe (O4-C2-Q-001; non-empty INSERT would fail at cast)"
            )));
        }
    }
    Ok(())
}

/// True when any value-producing expression in `plan` carries a cast that can raise at value level.
pub(crate) fn logical_plan_has_unsafe_cast(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        let schema = expr_typing_schema(node);
        let _ = node.apply_expressions(|expr| {
            if expr_has_unsafe_cast(expr, schema.as_ref()) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// The schema `node`'s own expressions are typed against.
pub(crate) fn expr_typing_schema(node: &LogicalPlan) -> DFSchemaRef {
    match node.inputs().as_slice() {
        [] => Arc::clone(node.schema()),
        [single] => Arc::clone(single.schema()),
        inputs => {
            let mut merged = DFSchema::empty();
            for input in inputs {
                merged.merge(input.schema());
            }
            Arc::new(merged)
        }
    }
}

/// True when `expr` computes a value through a cast that can raise.
pub(crate) fn expr_has_unsafe_cast(expr: &DataFusionExpr, schema: &DFSchema) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        match node {
            DataFusionExpr::Cast(cast) => {
                // An unresolvable input type is left to the schema/arity checks.
                if let Ok(from_type) = cast.expr.get_type(schema)
                    && cast_may_fail_at_runtime(&from_type, cast.field.data_type())
                {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            DataFusionExpr::ScalarSubquery(subquery)
            | DataFusionExpr::Exists(Exists { subquery, .. })
            | DataFusionExpr::InSubquery(InSubquery { subquery, .. })
                if logical_plan_has_unsafe_cast(&subquery.subquery) =>
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// True when casting a value from `from` to `to` can raise for *some* input value.
pub(crate) fn cast_may_fail_at_runtime(from: &DataType, to: &DataType) -> bool {
    // Identity, UTF-8 family aliasing, and the safe integer/float widenings are total.
    if assignment_types_compatible(from, to) {
        return false;
    }
    // `NULL -> anything` is total.
    if matches!(from, DataType::Null) {
        return false;
    }
    // Rendering a scalar as text is total.
    !(utf8_family(to) && renders_as_text_infallibly(from))
}

/// Scalar types whose cast to a UTF-8 type is total (no parse, no overflow, no rounding error).
pub(crate) fn renders_as_text_infallibly(data_type: &DataType) -> bool {
    use DataType::{
        Boolean, Date32, Date64, Decimal128, Decimal256, Float16, Float32, Float64, Int8, Int16,
        Int32, Int64, Time32, Time64, Timestamp, UInt8, UInt16, UInt32, UInt64,
    };
    matches!(
        data_type,
        Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _)
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Timestamp(_, _)
    )
}

/// Resolve `name` against a DataFusion schema case-insensitively (Spark default).
pub(crate) fn field_type_case_insensitive(
    schema: &datafusion::common::DFSchema,
    name: &str,
) -> Result<DataType> {
    let mut found: Option<DataType> = None;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(name) {
            if found.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "INSERT OVERWRITE column `{name}` is ambiguous under case-insensitive matching"
                )));
            }
            found = Some(field.data_type().clone());
        }
    }
    found.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "INSERT OVERWRITE empty source column `{name}` does not exist in the target table"
        ))
    })
}

/// Types that may land via empty-OW provider wipe without a value-level cast check.
pub(crate) fn assignment_types_compatible(source: &DataType, target: &DataType) -> bool {
    use DataType::{Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64};
    if source == target {
        return true;
    }
    if utf8_family(source) && utf8_family(target) {
        return true;
    }
    matches!(
        (source, target),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64 | Int16 | Int32 | Int64)
            | (UInt16, UInt32 | UInt64 | Int32 | Int64)
            | (UInt32, UInt64 | Int64)
            | (Float32, Float64)
    )
}

pub(crate) fn utf8_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[cfg(test)]
mod assignment_type_unit_tests {
    use super::{assignment_types_compatible, utf8_family};
    use datafusion::arrow::datatypes::DataType;

    /// Pure unit pins for the empty-OW assignment matrix.
    #[test]
    fn assignment_types_compatible_matrix() {
        assert!(assignment_types_compatible(
            &DataType::Int32,
            &DataType::Int32
        ));
        assert!(assignment_types_compatible(
            &DataType::Int32,
            &DataType::Int64
        ));
        assert!(assignment_types_compatible(
            &DataType::Utf8,
            &DataType::LargeUtf8
        ));
        assert!(utf8_family(&DataType::Utf8View));
        assert!(!assignment_types_compatible(
            &DataType::Utf8,
            &DataType::Int32
        ));
        assert!(!assignment_types_compatible(
            &DataType::Utf8,
            &DataType::Date32
        ));
        assert!(!assignment_types_compatible(
            &DataType::Int64,
            &DataType::Int32
        ));
        assert!(!assignment_types_compatible(
            &DataType::Float64,
            &DataType::Float32
        ));
    }

    /// Analyzer-inserted infallible casts remain safe.
    #[test]
    fn cast_may_fail_at_runtime_matrix() {
        use super::cast_may_fail_at_runtime;
        use std::sync::Arc;

        // Total: identity, UTF-8 aliasing, and the safe widenings (delegated to the matrix above).
        assert!(!cast_may_fail_at_runtime(
            &DataType::Int32,
            &DataType::Int32
        ));
        assert!(!cast_may_fail_at_runtime(
            &DataType::Utf8,
            &DataType::Utf8View
        ));
        assert!(!cast_may_fail_at_runtime(
            &DataType::Int32,
            &DataType::Int64
        ));
        // Total: rendering a scalar as text, one assertion per `renders_as_text_infallibly` arm.
        for from in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Decimal256(40, 2),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(datafusion::arrow::datatypes::TimeUnit::Second),
            DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Nanosecond),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
        ] {
            assert!(
                !cast_may_fail_at_runtime(&from, &DataType::Utf8),
                "{from} → Utf8 is total and must not block a wipe"
            );
        }
        // Total: `NULL -> anything` (audit G1-H-003).
        for to in [
            DataType::Utf8,
            DataType::Int32,
            DataType::Date32,
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Null,
        ] {
            assert!(
                !cast_may_fail_at_runtime(&DataType::Null, &to),
                "Null → {to} is total and must not block a wipe"
            );
        }
        // Fallible: parsing text, narrowing, and anything unmodelled (fail closed).
        assert!(cast_may_fail_at_runtime(&DataType::Utf8, &DataType::Int32));
        assert!(cast_may_fail_at_runtime(&DataType::Utf8, &DataType::Date32));
        assert!(cast_may_fail_at_runtime(&DataType::Int64, &DataType::Int32));
        assert!(cast_may_fail_at_runtime(
            &DataType::Float64,
            &DataType::Float32
        ));
        // A non-scalar source has no total text rendering modelled here — fail closed.
        assert!(cast_may_fail_at_runtime(
            &DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            &DataType::Utf8
        ));
    }
}
