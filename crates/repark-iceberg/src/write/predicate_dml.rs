//! Identity DELETE/UPDATE via SELECT over a pinned `(_file, _pos)` streaming target.
//!
//! The G3-E8 valve refuses subquery `WHERE` clauses because DataFusion's DML planner drops them
//! (`extract_dml_filters` sees a join and recovers nothing; empty filters = delete-all). This
//! module is the RePark-owned capability that evaluates the original predicate as a **SELECT**
//! over the MERGE streaming target (data columns + reserved identity) and then commits through
//! the MERGE write arms — honoring `write.delete.mode` / `write.update.mode` and the matching
//! isolation properties, **never** `write.merge.mode`.
//!
//! The capability is general (any `WHERE` that DataFusion can plan as a query). The product hole
//! is the valve allow-list: uncorrelated `DELETE … WHERE col IN (SELECT …)` /
//! `NOT IN (SELECT …)` (including the NULL 3VL trap) and `DELETE … WHERE [NOT] EXISTS
//! (SELECT …)` both uncorrelated (all-or-nothing) and correlated (per-row semi/anti-join).
//! UPDATE, mixed AND/OR, nested, scalar, CTE, USING/RETURNING, and correlated IN stay refused.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, FromTable, GroupByExpr, Ident, ObjectName, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Visit, VisitMut, Visitor, VisitorMut,
};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, FormatVersion};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use std::str::FromStr;
use uuid::Uuid;

use crate::write::concurrency::concurrency_from_ctx;
use crate::write::file_scoped_rewrite::allowlist_from_paths;
use crate::write::merge::{
    FILE_PATH_COL, IsolationLevel, POS_COL, RowDeltaKind, RowDeltaPolicy, TargetScanStream,
    commit_overwrite, commit_row_delta_kind, deregister_merge_scratch, iceberg_err, quote_ident,
    register_streaming_target, reserved_name_guard, resolve_affected_data_files, scratch_schema,
    write_new_data_files_from_stream,
};
use crate::write::position_delete::PositionDeletePair;
use crate::write::scan_concurrency::scan_concurrency_from_ctx;

/// Iceberg standard table property selecting the DELETE write strategy.
const WRITE_DELETE_MODE: &str = "write.delete.mode";

/// Iceberg standard table property selecting DELETE isolation (Java default: serializable).
const WRITE_DELETE_ISOLATION_LEVEL: &str = "write.delete.isolation-level";

const MODE_MERGE_ON_READ: &str = "merge-on-read";
const MODE_COPY_ON_WRITE: &str = "copy-on-write";

/// A lowered identity-DELETE against one Iceberg target.
#[derive(Debug, Clone)]
pub struct PredicateDmlSpec {
    /// Three-part Iceberg target (`namespace` may be multi-level).
    pub target: TableIdent,
    /// Alias the SELECT uses for the pinned scratch (user alias, or the table's last ident).
    pub target_alias: String,
    /// The original `WHERE` predicate, SQL-rendered verbatim.
    pub selection_sql: String,
}

/// Catalog name + identity spec extracted from an allow-listed `DELETE … IN` / `NOT IN` /
/// `[NOT] EXISTS`.
#[derive(Debug, Clone)]
pub struct AllowedDeleteIn {
    /// Leading catalog identifier (`ice` in `ice.sales.tgt`).
    pub catalog_name: String,
    /// The identity-DELETE spec handed to [`execute_predicate_dml`].
    pub spec: PredicateDmlSpec,
}

/// ===========================================================================================
/// True when `selection` is exactly uncorrelated `col IN (SELECT col FROM <table> …)` or
/// `col NOT IN (SELECT col FROM <table> …)` — one 3VL family, both spellings.
///
/// Fail-closed: `NOT (col IN …)`, scalars, mixed AND/OR, nested FROM, aggregates, WITH, and
/// correlated outer refs stay **outside** this helper. `[NOT] EXISTS` is a sibling helper.
/// ===========================================================================================
#[must_use]
pub fn is_allowed_uncorrelated_in_selection(selection: &Expr) -> bool {
    let Expr::InSubquery {
        expr,
        subquery,
        negated: _,
    } = selection
    else {
        return false;
    };
    is_column_expr(expr) && is_simple_uncorrelated_in_subquery(subquery)
}

/// ===========================================================================================
/// True when `selection` is exactly `[NOT] EXISTS (SELECT … FROM <table> [WHERE …])` whose
/// compound refs are only the subquery source or the DELETE target (uncorrelated or correlated
/// to that target). Nested / mixed / CTE / third-table refs stay outside the hole.
/// ===========================================================================================
#[must_use]
pub fn is_allowed_exists_selection(
    selection: &Expr,
    target_parts: &[String],
    target_alias: &str,
) -> bool {
    let Expr::Exists {
        subquery,
        negated: _,
    } = selection
    else {
        return false;
    };
    is_simple_exists_subquery(subquery, target_parts, target_alias)
}

/// ===========================================================================================
/// If `statement` is an allow-listed IN / NOT IN / `[NOT] EXISTS` DELETE, return the catalog +
/// spec; otherwise `None`.
///
/// Unhandled subquery shapes return `None` so the caller can keep the G3-E8 valve. A recognized
/// spelling whose target is not a three-part Iceberg name is not an executable hole
/// (fail-closed — never DataFusion DML). USING / RETURNING / OUTPUT / LIMIT / ORDER BY /
/// multi-table stay outside the hole.
///
/// Target FQN refs inside the predicate are rewritten to the scratch alias so the identity
/// SELECT evaluates the same predicate against the pinned `(_file, _pos)` stream (not a
/// second scan of the user table).
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] when the spelling is allowed but the target is not
/// `catalog.namespace.table`.
pub fn try_allowed_delete_in(statement: &Statement) -> Result<Option<AllowedDeleteIn>> {
    let Statement::Delete(delete) = statement else {
        return Ok(None);
    };
    if delete.using.is_some()
        || delete.returning.is_some()
        || delete.output.is_some()
        || delete.limit.is_some()
        || !delete.order_by.is_empty()
        || !delete.tables.is_empty()
    {
        return Ok(None);
    }
    let Some(selection) = delete.selection.as_ref() else {
        return Ok(None);
    };
    let Some((object_name, alias)) = delete_target_and_alias(delete) else {
        return Ok(None);
    };
    let parts = object_name_parts(object_name);
    // Not an executable hole (and must stay valved) unless the target is three-part Iceberg.
    if parts.len() < 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_name = parts[parts.len() - 1].clone();
    let namespace = parts[1..parts.len() - 1].to_vec();
    let namespace = NamespaceIdent::from_vec(namespace).map_err(|error| {
        DataFusionError::Plan(format!(
            "DELETE target `{object_name}` has an invalid namespace: {error}"
        ))
    })?;
    let target_alias = alias.unwrap_or_else(|| table_name.clone());
    if !is_allowed_uncorrelated_in_selection(selection)
        && !is_allowed_exists_selection(selection, &parts, &target_alias)
    {
        return Ok(None);
    }
    let mut scratch_selection = selection.clone();
    rewrite_target_refs_in_expr(&mut scratch_selection, &parts, &target_alias);
    Ok(Some(AllowedDeleteIn {
        catalog_name,
        spec: PredicateDmlSpec {
            target: TableIdent::new(namespace, table_name),
            target_alias,
            selection_sql: scratch_selection.to_string(),
        },
    }))
}

/// ===========================================================================================
/// Execute an identity DELETE: SELECT `(_file, _pos)` over the pinned scratch, then COW-rewrite
/// or `MoR` position-delete using MERGE write/commit arms and `write.delete.mode`.
/// ===========================================================================================
///
/// # Errors
/// Planning / execution / write / commit errors, plus `NotImplemented` for a non-Parquet default
/// format or merge-on-read on a non-V2 table.
pub async fn execute_predicate_dml(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &PredicateDmlSpec,
) -> Result<()> {
    let table = catalog
        .load_table(&spec.target)
        .await
        .map_err(iceberg_err)?;
    let write_schema =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).map_err(iceberg_err)?);
    reserved_name_guard(&write_schema)?;
    let mode = resolve_delete_mode(&table)?;
    let isolation = resolve_delete_isolation(&table)?;
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());

    let scratch = scratch_schema(&write_schema);
    let scan_concurrency = scan_concurrency_from_ctx(ctx);
    let source: Arc<dyn datafusion::physical_plan::streaming::PartitionStream> =
        Arc::new(TargetScanStream::new(
            table.clone(),
            snapshot_id,
            Arc::clone(&scratch),
            &write_schema,
            None,
            scan_concurrency.concurrency_limit,
            None,
        ));
    let target_name = register_streaming_target(ctx, Arc::clone(&scratch), source)?;
    let result = match collect_identity_pairs(ctx, &target_name, spec).await {
        Ok(pairs) if pairs.is_empty() => Ok(()),
        Ok(pairs) => match mode {
            DeleteWriteMode::CopyOnWrite => {
                commit_identity_cow(
                    ctx,
                    catalog,
                    &table,
                    &write_schema,
                    snapshot_id,
                    &pairs,
                    isolation,
                )
                .await
            }
            DeleteWriteMode::MergeOnRead => {
                commit_row_delta_kind(
                    catalog,
                    &table,
                    snapshot_id,
                    pairs,
                    Vec::new(),
                    concurrency_from_ctx(ctx),
                    RowDeltaPolicy {
                        kind: RowDeltaKind::Delete,
                        isolation,
                    },
                )
                .await
            }
        },
        Err(error) => Err(error),
    };
    let _ = deregister_merge_scratch(ctx, &target_name);
    result
}

/// Run `SELECT _file, _pos FROM scratch AS alias WHERE <original predicate>`.
async fn collect_identity_pairs(
    ctx: &SessionContext,
    target_name: &str,
    spec: &PredicateDmlSpec,
) -> Result<Vec<PositionDeletePair>> {
    let sql = format!(
        "SELECT {file}, {pos} FROM {scratch} AS {alias} WHERE {selection}",
        file = quote_ident(FILE_PATH_COL),
        pos = quote_ident(POS_COL),
        scratch = quote_ident(target_name),
        alias = quote_ident(&spec.target_alias),
        selection = spec.selection_sql,
    );
    let batches = ctx.sql(&sql).await?.collect().await?;
    let mut pairs = Vec::new();
    for batch in &batches {
        let files = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("identity SELECT `_file` column is not Utf8".to_string())
            })?;
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("identity SELECT `_pos` column is not Int64".to_string())
            })?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) {
                return Err(DataFusionError::Internal(
                    "identity SELECT produced a NULL `(_file, _pos)` pair".to_string(),
                ));
            }
            pairs.push((Arc::<str>::from(files.value(row)), positions.value(row)));
        }
    }
    Ok(pairs)
}

/// Rewrite affected files, dropping the identity pairs, then overwrite-commit.
async fn commit_identity_cow(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    snapshot_id: Option<i64>,
    pairs: &[PositionDeletePair],
    isolation: IsolationLevel,
) -> Result<()> {
    let mut affected: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (path, _) in pairs {
        if seen.insert(path.clone()) {
            affected.push(path.to_string());
        }
    }
    let ident_table = register_identity_table(ctx, pairs)?;
    let rewrite_name =
        register_affected_rewrite_target(ctx, table, snapshot_id, write_schema, &affected)?;
    let rewrite_sql = survivor_sql(write_schema, &rewrite_name, &ident_table);
    let rewrite_result = async {
        let stream = ctx.sql(&rewrite_sql).await?.execute_stream().await?;
        let concurrency = concurrency_from_ctx(ctx);
        write_new_data_files_from_stream(table, write_schema, stream, concurrency).await
    }
    .await;
    let _ = ctx.deregister_table(ident_table.as_str());
    let _ = deregister_merge_scratch(ctx, &rewrite_name);
    let new_files = rewrite_result?;
    let affected_entries = resolve_affected_data_files(table, &affected).await?;
    commit_overwrite(
        catalog,
        table,
        snapshot_id,
        affected_entries,
        new_files,
        isolation,
    )
    .await
}

fn register_identity_table(ctx: &SessionContext, pairs: &[PositionDeletePair]) -> Result<String> {
    let name = format!("__repark_pred_ident_{}", Uuid::new_v4().simple());
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(FILE_PATH_COL, DataType::Utf8, false),
        Field::new(POS_COL, DataType::Int64, false),
    ]));
    let files: Vec<Option<&str>> = pairs.iter().map(|(path, _)| Some(path.as_ref())).collect();
    let positions: Vec<i64> = pairs.iter().map(|(_, position)| *position).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(files)),
            Arc::new(Int64Array::from(positions)),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table(name.as_str(), Arc::new(provider))?;
    Ok(name)
}

fn register_affected_rewrite_target(
    ctx: &SessionContext,
    table: &Table,
    snapshot_id: Option<i64>,
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    affected: &[String],
) -> Result<String> {
    let allowlist = allowlist_from_paths(affected);
    let scratch = scratch_schema(write_schema);
    let scan_concurrency = scan_concurrency_from_ctx(ctx);
    let source: Arc<dyn datafusion::physical_plan::streaming::PartitionStream> =
        Arc::new(TargetScanStream::new(
            table.clone(),
            snapshot_id,
            Arc::clone(&scratch),
            write_schema,
            None,
            scan_concurrency.concurrency_limit,
            Some(allowlist),
        ));
    register_streaming_target(ctx, scratch, source)
}

fn survivor_sql(
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    rewrite_name: &str,
    ident_table: &str,
) -> String {
    let columns = write_schema
        .fields()
        .iter()
        .map(|field| format!("t.{}", quote_ident(field.name())))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {columns} FROM {rewrite} AS t WHERE NOT EXISTS (\
         SELECT 1 FROM {idents} AS i WHERE i.{file} = t.{file} AND i.{pos} = t.{pos})",
        rewrite = quote_ident(rewrite_name),
        idents = quote_ident(ident_table),
        file = quote_ident(FILE_PATH_COL),
        pos = quote_ident(POS_COL),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteWriteMode {
    CopyOnWrite,
    MergeOnRead,
}

fn resolve_delete_mode(table: &Table) -> Result<DeleteWriteMode> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "identity DELETE writes only Parquet data files yet (table default is {file_format})"
        )));
    }
    let mode = table
        .metadata()
        .properties()
        .get(WRITE_DELETE_MODE)
        .map(String::as_str);
    match mode {
        Some(value) if value.eq_ignore_ascii_case(MODE_MERGE_ON_READ) => {
            let format_version = table.metadata().format_version();
            if format_version != FormatVersion::V2 {
                return Err(DataFusionError::NotImplemented(format!(
                    "merge-on-read DELETE writes Parquet position deletes, which require a V2 \
                     table (this table is {format_version:?}) — use {WRITE_DELETE_MODE} = \
                     '{MODE_COPY_ON_WRITE}' instead"
                )));
            }
            Ok(DeleteWriteMode::MergeOnRead)
        }
        _ => Ok(DeleteWriteMode::CopyOnWrite),
    }
}

fn resolve_delete_isolation(table: &Table) -> Result<IsolationLevel> {
    match table
        .metadata()
        .properties()
        .get(WRITE_DELETE_ISOLATION_LEVEL)
    {
        Some(name) => match name.to_ascii_lowercase().as_str() {
            "serializable" => Ok(IsolationLevel::Serializable),
            "snapshot" => Ok(IsolationLevel::Snapshot),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid isolation level: {name}"
            ))),
        },
        None => Ok(IsolationLevel::Serializable),
    }
}

fn is_column_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

fn is_simple_select_body(query: &Query) -> Option<&datafusion::sql::sqlparser::ast::Select> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.distinct.is_some()
        || select.from.len() != 1
        || select.having.is_some()
        || select.qualify.is_some()
        || !select.lateral_views.is_empty()
    {
        return None;
    }
    if !matches!(&select.group_by, GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty())
    {
        return None;
    }
    if !is_plain_table(&select.from[0]) {
        return None;
    }
    Some(select)
}

fn is_simple_uncorrelated_in_subquery(query: &Query) -> bool {
    let Some(select) = is_simple_select_body(query) else {
        return false;
    };
    if select.projection.len() != 1 {
        return false;
    }
    let (SelectItem::UnnamedExpr(projected)
    | SelectItem::ExprWithAlias {
        expr: projected, ..
    }) = &select.projection[0]
    else {
        return false;
    };
    if !is_column_expr(projected) {
        return false;
    }
    let (source_parts, aliases) = source_relation_names(&select.from[0]);
    !subquery_has_nested_query(query) && !subquery_has_outer_ref(query, &source_parts, &aliases)
}

fn is_simple_exists_subquery(query: &Query, target_parts: &[String], target_alias: &str) -> bool {
    let Some(select) = is_simple_select_body(query) else {
        return false;
    };
    let (source_parts, aliases) = source_relation_names(&select.from[0]);
    !subquery_has_nested_query(query)
        && !subquery_has_disallowed_ref(query, &source_parts, &aliases, target_parts, target_alias)
}

fn is_plain_table(table: &TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table {
                args: None,
                version: None,
                ..
            }
        )
}

fn source_relation_names(table: &TableWithJoins) -> (Vec<String>, Vec<String>) {
    let TableFactor::Table { name, alias, .. } = &table.relation else {
        return (Vec::new(), Vec::new());
    };
    let parts = object_name_parts(name);
    let mut aliases = Vec::new();
    if let Some(last) = parts.last() {
        aliases.push(last.clone());
    }
    if let Some(alias) = alias {
        aliases.push(alias.name.value.clone());
    }
    (parts, aliases)
}

/// A nested `Query` inside the subquery (derived FROM, scalar, …) is a different spelling.
fn subquery_has_nested_query(query: &Query) -> bool {
    struct Nested {
        seen: usize,
    }
    impl Visitor for Nested {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &Query) -> std::ops::ControlFlow<Self::Break> {
            self.seen += 1;
            if self.seen > 1 {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }
    }
    query.visit(&mut Nested { seen: 0 }).is_break()
}

/// Compound identifiers that do not prefix-match the subquery's own table name or alias are
/// treated as outer (correlated) refs — fail-closed.
fn subquery_has_outer_ref(query: &Query, source_parts: &[String], aliases: &[String]) -> bool {
    struct Outer<'a> {
        source_parts: &'a [String],
        aliases: &'a [String],
        found: bool,
    }
    impl Visitor for Outer<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
            if let Expr::CompoundIdentifier(idents) = expr
                && !compound_refers_to_source(idents, self.source_parts, self.aliases)
            {
                self.found = true;
                return std::ops::ControlFlow::Break(());
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut visitor = Outer {
        source_parts,
        aliases,
        found: false,
    };
    query.visit(&mut visitor).is_break() || visitor.found
}

/// Compound identifiers that are neither the subquery source nor the DELETE target are a
/// third-table (or otherwise unhandled) correlation — fail-closed.
fn subquery_has_disallowed_ref(
    query: &Query,
    source_parts: &[String],
    aliases: &[String],
    target_parts: &[String],
    target_alias: &str,
) -> bool {
    struct Disallowed<'a> {
        source_parts: &'a [String],
        aliases: &'a [String],
        target_parts: &'a [String],
        target_alias: &'a str,
        found: bool,
    }
    impl Visitor for Disallowed<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
            if let Expr::CompoundIdentifier(idents) = expr
                && !compound_refers_to_source(idents, self.source_parts, self.aliases)
                && !compound_refers_to_target(idents, self.target_parts, self.target_alias)
            {
                self.found = true;
                return std::ops::ControlFlow::Break(());
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut visitor = Disallowed {
        source_parts,
        aliases,
        target_parts,
        target_alias,
        found: false,
    };
    query.visit(&mut visitor).is_break() || visitor.found
}

fn compound_refers_to_target(
    idents: &[Ident],
    target_parts: &[String],
    target_alias: &str,
) -> bool {
    let Some(first) = idents.first() else {
        return false;
    };
    if first.value.eq_ignore_ascii_case(target_alias) && idents.len() <= 2 {
        return true;
    }
    if let Some(table_name) = target_parts.last()
        && first.value.eq_ignore_ascii_case(table_name)
        && idents.len() <= 2
    {
        return true;
    }
    idents.len() > target_parts.len()
        && target_parts
            .iter()
            .zip(idents.iter())
            .all(|(part, ident)| part.eq_ignore_ascii_case(&ident.value))
}

/// Rewrite `catalog.ns.tgt.col` (and only that prefix) to `alias.col` so the identity SELECT
/// correlates against the scratch, not a second scan of the user table.
fn rewrite_target_refs_in_expr(expr: &mut Expr, target_parts: &[String], alias: &str) {
    struct Rewrite<'a> {
        target_parts: &'a [String],
        alias: &'a str,
    }
    impl VisitorMut for Rewrite<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
            if let Expr::CompoundIdentifier(idents) = expr
                && let Some(rewritten) =
                    rewrite_target_compound(idents, self.target_parts, self.alias)
            {
                *idents = rewritten;
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = expr.visit(&mut Rewrite {
        target_parts,
        alias,
    });
}

fn rewrite_target_compound(
    idents: &[Ident],
    target_parts: &[String],
    alias: &str,
) -> Option<Vec<Ident>> {
    if target_parts.is_empty() || idents.is_empty() {
        return None;
    }
    if idents[0].value.eq_ignore_ascii_case(alias) {
        return None;
    }
    if idents.len() > target_parts.len()
        && target_parts
            .iter()
            .zip(idents.iter())
            .all(|(part, ident)| part.eq_ignore_ascii_case(&ident.value))
    {
        let mut rewritten = vec![Ident::new(alias)];
        rewritten.extend(idents[target_parts.len()..].iter().cloned());
        return Some(rewritten);
    }
    None
}

fn compound_refers_to_source(
    idents: &[datafusion::sql::sqlparser::ast::Ident],
    source_parts: &[String],
    aliases: &[String],
) -> bool {
    let Some(first) = idents.first() else {
        return false;
    };
    if aliases
        .iter()
        .any(|alias| alias.eq_ignore_ascii_case(&first.value))
        && idents.len() <= 2
    {
        return true;
    }
    if idents.len() > source_parts.len()
        && source_parts
            .iter()
            .zip(idents.iter())
            .all(|(part, ident)| part.eq_ignore_ascii_case(&ident.value))
    {
        return true;
    }
    false
}

fn delete_target_and_alias(
    delete: &datafusion::sql::sqlparser::ast::Delete,
) -> Option<(&ObjectName, Option<String>)> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let table = tables.first()?;
    match &table.relation {
        TableFactor::Table { name, alias, .. } => {
            let alias = alias.as_ref().map(|alias| alias.name.value.clone());
            Some((name, alias))
        }
        _ => None,
    }
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

#[cfg(test)]
#[path = "predicate_dml_tests.rs"]
mod predicate_dml_tests;
