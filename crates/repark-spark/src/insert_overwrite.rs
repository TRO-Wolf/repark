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

// A non-empty source is staged before the table is replaced. A source that becomes empty during
// staging refuses the wipe so a race cannot erase existing rows.

/// Monotonic counter for ephemeral `INSERT OVERWRITE` MemTable-fallback temp views.
pub(crate) static OW_MATERIALIZE_SEQ: AtomicU64 = AtomicU64::new(1);

/// ===========================================================================================
/// `INSERT OVERWRITE` / `INSERT OVERWRITE TABLE` with a zero-row source must wipe the target.
///
/// Validate schema and assignment types before an empty-source wipe. Use a self-scan source so the
/// wipe cannot re-run a changing caller query. Stage non-empty rows before one replace-all publish;
/// refuse if the source becomes empty between probe and staged write.
/// ===========================================================================================
///
/// # Errors
/// Propagates source planning/execution, schema-validation, and wipe/passthrough errors as
/// [`DataFusionError`].
pub(crate) async fn execute_insert_overwrite(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    insert: &Insert,
) -> Result<DataFrame> {
    let table_name = match &insert.table {
        TableObject::TableName(name) => name,
        // Table-function / table-query targets are not an Iceberg table wipe surface — leave to DataFusion.
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => {
            return spark_ast::execute_passthrough(ctx, catalogs, sql).await;
        }
    };
    let table_sql = table_name.to_string();
    if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
        return Err(DataFusionError::Plan(message));
    }

    // Hive/Spark `INSERT OVERWRITE … PARTITION (…)` is partition-scoped. We do not implement
    // static or dynamic partition overwrite yet (C2-Q-001 / C4-Q-001):
    // - empty source must not become a full-table wipe (sibling partitions);
    // - non-empty source must not silently degrade to whole-table replace either.
    // Refuse **all** PARTITION forms loud until a partition-scoped path exists.
    if insert.partitioned.is_some() {
        return Err(DataFusionError::NotImplemented(
            "INSERT OVERWRITE … PARTITION (…) is not supported yet (static and dynamic \
             partition overwrite). Empty sources must not full-table wipe sibling partitions; \
             non-empty sources must not silently whole-table replace. Use static whole-table \
             INSERT OVERWRITE, or DELETE with a partition predicate + INSERT INTO (tracked: \
             C2-Q-001 / C4-Q-001 / docs/spark-sql-iceberg-parity.md §2.3)"
                .to_string(),
        ));
    }

    if let Some(source) = &insert.source {
        let probe_sql = format!("SELECT 1 FROM ({source}) AS _repark_ow_probe LIMIT 1");
        let probe = spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
        let batches = probe.collect().await?;
        let empty = batches.iter().all(|batch| batch.num_rows() == 0);
        if empty {
            // Validate the original INSERT OVERWRITE plan (column count / schema) **before**
            // wiping. An empty incompatible source must fail loud and leave prior rows — Spark
            // rejects schema mismatch at analysis (C5-Q-001). Plan-only via `ctx.sql` (no collect)
            // so we do not commit the wipe before assignment checks.
            let _validated = ctx.sql(sql).await?;
            // O4-C2-Q-001: plan-only validation does not run cast kernels. Empty Utf8→Int32
            // plans OK while the same non-empty INSERT fails at cast — refuse wipe instead.
            assert_empty_overwrite_types_assignment_compatible(
                ctx,
                catalogs,
                &table_sql,
                source,
                &insert.columns,
            )
            .await?;
            // Re-probe immediately before wipe (P4C1-SAF-001 / L-001): the first probe only
            // classified emptiness; validation work widens the TOCTOU window where a concurrent
            // or non-deterministic source can grow rows. If the source is non-empty now, fall
            // through to the guarded non-empty path — never provider-insert unguarded.
            let reprobe = spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
            let reprobe_batches = reprobe.collect().await?;
            let still_empty = reprobe_batches.iter().all(|batch| batch.num_rows() == 0);
            if still_empty {
                // Wipe via a **self-scan empty** statement, not re-exec of the original source
                // (P4C2-SAF-001): re-running the caller's SQL after emptiness classification can
                // still yield rows (TOCTOU / non-deterministic sources) and would hit the provider
                // without the partition guard. `SELECT * FROM <target> WHERE false` is always
                // empty, schema-identical, and a positional-identity base-table passthrough — so
                // it is guard-safe if a residual race ever made it non-empty (it cannot).
                // Original source emptiness + assignment types were already validated above.
                let wipe_sql =
                    format!("INSERT OVERWRITE {table_sql} SELECT * FROM {table_sql} WHERE false");
                return spark_ast::execute_passthrough(ctx, catalogs, &wipe_sql).await;
            }
            // Fall through: re-probe saw rows → treat as non-empty (stage-then-swap path).
        }
        // Stage non-empty rows before replace-all. A source that becomes empty must not wipe.
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

/// ===========================================================================================
/// Non-empty `INSERT OVERWRITE` — stage-then-swap (OV1 / OTH-004).
///
/// Resolve the Iceberg target, stream positional assignments into staged files, and publish one
/// replace-all commit. A zero-row staged result refuses the wipe. Non-resolvable targets use the
/// existing fallback path; a registered catalog that cannot load the table fails loudly.
/// ===========================================================================================
///
/// # Errors
/// Source stream, positional map/cast, write, or commit failures as [`DataFusionError`].
/// Zero-row stream after non-empty probe → [`DataFusionError::Execution`] (no wipe).
pub(crate) async fn insert_overwrite_from_staged_source(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
    table_sql: &str,
    source: &datafusion::sql::sqlparser::ast::Query,
    columns: &[ObjectName],
) -> Result<DataFrame> {
    match try_resolve_iceberg_overwrite_target(catalogs, table_name).await? {
        Some((catalog_name, catalog, table)) => {
            insert_overwrite_iceberg_stage_then_swap(
                ctx,
                catalogs,
                &catalog_name,
                &catalog,
                &table,
                table_sql,
                source,
                columns,
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

/// Resolve `catalog.namespace….table` against the registry. `< 3` parts → `None` (`MemTable`
/// fallback). Catalog present but `load_table` fails → loud Plan (D7).
pub(crate) async fn try_resolve_iceberg_overwrite_target(
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
) -> Result<Option<(String, Arc<dyn Catalog>, iceberg::table::Table)>> {
    let parts = name_parts(table_name);
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
        Ok(table) => Ok(Some((catalog_name, Arc::clone(catalog), table))),
        Err(error) => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE target `{table_name}` could not be loaded as an Iceberg table \
             (catalog `{catalog_name}` is registered — refusing silent MemTable fallback; D7): \
             {error}"
        ))),
    }
}

/// Stream → repark-write positional stage → row-count refuse → `commit_overwrite_replace_all`.
///
/// Stream map/write lives in `repark_iceberg::write::write_overwrite_staged_files_from_stream` so this
/// crate stays free of a production `futures` dep (Cargo.toml FROZEN / octo C1-Q-001).
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
) -> Result<DataFrame> {
    use iceberg::spec::DataFile;

    // Fail isolation parse before staging (octo C3-Q-001) — invalid property must not pay a full
    // stream write + orphan objects. Same parse as commit_overwrite_replace_all (D10).
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
    repark_iceberg::write::commit_overwrite_replace_all(catalog, table, staged_files).await?;
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

    // VALUES / literal materializations often widen every field to nullable; Iceberg required
    // columns (common on partitioned tables) reject that with "Input schema does not match".
    // Tighten nullability for the residual MemTable path only (Iceberg staged path skips this).
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

/// ===========================================================================================
/// Rebuild batches so columns with zero nulls across all batches are non-nullable.
///
/// Needed so materialised VALUES/`SELECT` literals keep Iceberg required-field compatibility
/// (partitioned targets reject nullable→required schema drift — BUG-001 materialize path).
/// Preserves per-field and schema metadata (field ids / comments) — only nullability flips.
/// ===========================================================================================
///
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
    // `with_nullable` keeps name/type/metadata; `new_with_metadata` keeps schema-level keys.
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

/// ===========================================================================================
/// Empty `INSERT OVERWRITE` wipe must not run when source column types are not assignment-
/// compatible with the target (O4-C2-Q-001).
///
/// `ctx.sql(INSERT…)` plan-only accepts many casts that only fail when values are evaluated.
/// Zero-row sources never evaluate casts, so a type-mismatch empty OW would provider-wipe while
/// the identical non-empty statement errors — refuse the wipe instead.
/// ===========================================================================================
///
/// sqlparser 0.62: insert column list is `ObjectName` (not bare `Ident`).
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
    // P5C1-Q-001: plan-time CAST rewrites Utf8→Int32 (etc.) so the projected schema *looks*
    // assignment-compatible while zero rows never run the cast kernel. Non-empty of the same
    // statement fails at cast and keeps rows — empty would wipe. Refuse when ANY expression of
    // the source plan (projection, aggregate, predicate, join key, …) carries a cast that can
    // raise at value level — see [`logical_plan_has_unsafe_cast`] for why position is not a safe
    // axis to filter on.
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
        // Case-insensitive name resolve (Spark `caseSensitive=false`; P4C1-Q-004 / L-004) —
        // MERGE SET already resolves this way; exact-case here made empty OW refuse
        // `INSERT OVERWRITE t (ID) … WHERE false` while non-empty could succeed.
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

/// ===========================================================================================
/// True when a **value-producing** expression anywhere in `plan` carries a cast that can raise
/// at value level. Walk every expression position and inspect analyzed casts for runtime
/// fallibility. Analyzer coercions that cannot raise remain allowed. Subquery expressions require
/// explicit recursion because they are not logical-plan children.
/// ===========================================================================================
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
///
/// A node's expressions reference its **inputs'** columns, so one input contributes its schema
/// directly and a multi-input node (a `Join`, whose `on` keys straddle both sides) contributes
/// the merged schema. Leaves (`TableScan`, `Values`, `EmptyRelation`) have no input, so their
/// expressions — `TableScan.filters`, literal `Values` rows — type against the node's own schema.
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

/// True when `expr` (or a subquery plan hanging off it) computes a value through a cast that can
/// raise — see [`logical_plan_has_unsafe_cast`].
///
/// `TRY_CAST` is deliberately absent: it is total (unparsable input yields NULL, never an
/// error), so the empty and non-empty forms of the same statement agree and there is no
/// asymmetric wipe to refuse.
pub(crate) fn expr_has_unsafe_cast(expr: &DataFusionExpr, schema: &DFSchema) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        match node {
            DataFusionExpr::Cast(cast) => {
                // An unresolvable input type (a correlated outer reference) is left to the
                // schema/arity checks rather than guessed at — see the residual note in `map.md`.
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
///
/// This is the empty-OW asymmetry oracle: a cast that cannot raise behaves identically with and
/// without rows, so it must never block a wipe. Unknown pairs fail closed (treated as fallible)
/// — refusing is a loud, recoverable error, wiping is not.
pub(crate) fn cast_may_fail_at_runtime(from: &DataType, to: &DataType) -> bool {
    // Identity, UTF-8 family aliasing, and the safe integer/float widenings are total.
    if assignment_types_compatible(from, to) {
        return false;
    }
    // `NULL -> anything` is total: the only value of `DataType::Null` is NULL, and casting NULL
    // yields NULL for every target. This is the `SELECT …, CAST(NULL AS STRING) AS col` widening
    // idiom, whose non-empty form SUCCEEDS — so refusing the empty form would be arbitrary.
    if matches!(from, DataType::Null) {
        return false;
    }
    // Rendering a scalar as text is total. This is the shape TypeCoercion inserts for
    // `concat(name, id)` and `id > '99'`; treating it as unsafe refused legitimate wipes.
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
///
/// Exact match, UTF-8 family aliases, and safe integer/float widenings. Parsing casts
/// (Utf8→numeric/temporal) are refused — those only fail when rows are present.
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

    /// O4-C3-Q-001: pure unit pins for the empty-OW assignment matrix (mutation-proof without
    /// spinning a catalog). Shipping path covered by `empty_insert_overwrite_type_mismatch_*`.
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

    /// Analyzer-inserted infallible casts remain safe; user or analyzer casts that can raise refuse.
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
        // Total: `NULL -> anything` (audit G1-H-003). The only value of `DataType::Null` is NULL
        // and casting NULL yields NULL for every target, so the schema-widening idiom
        // `SELECT …, CAST(NULL AS <type>)` must never block a wipe — its non-empty form succeeds.
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
