use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::view::ViewTable;
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::functions_table::generate_series::GenerateSeriesTable;
use datafusion::logical_expr::{Expr, LogicalPlan, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::plan_canonical::{
    RelTable, build_relations, user_shaped_comparisons, write_expr, write_exprs, write_opt_expr,
    write_sort,
};

#[must_use]
pub fn input_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    collect_files(plan, &mut seen, &mut out);
    out
}

fn collect_files(plan: &Arc<dyn ExecutionPlan>, seen: &mut HashSet<String>, out: &mut Vec<String>) {
    if let Some(exec) = plan.as_ref().downcast_ref::<DataSourceExec>()
        && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        for group in &config.file_groups {
            for file in group.iter() {
                let uri = render_uri(&config.object_store_url, &file.object_meta.location);
                if seen.insert(uri.clone()) {
                    out.push(uri);
                }
            }
        }
    }
    for child in plan.children() {
        collect_files(child, seen, out);
    }
}

fn render_uri(object_store_url: &ObjectStoreUrl, location: &ObjectPath) -> String {
    let url: &Url = object_store_url.as_ref();
    if url.scheme() == "file" {
        let text: &str = location.as_ref();
        if text.starts_with('/') {
            return format!("file://{text}");
        }
        return format!("file:///{text}");
    }
    let base = object_store_url.as_str().trim_end_matches('/');
    let text: &str = location.as_ref();
    let trimmed = text.trim_start_matches('/');
    format!("{base}/{trimmed}")
}

pub(crate) struct Ctx<'a, S: BuildHasher> {
    pub(crate) lineages: &'a HashMap<String, LogicalPlan, S>,
    pub(crate) relations: RelTable,
    pub(crate) user_comparisons: HashSet<(String, String, i128)>,
}

pub(crate) struct ByteSink(pub(crate) Vec<u8>);

impl Hasher for ByteSink {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
    fn write_u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn write_u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn write_usize(&mut self, value: usize) {
        self.0.extend_from_slice(&(value as u64).to_le_bytes());
    }
    fn write_i128(&mut self, value: i128) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
pub fn semantic_hash<S: BuildHasher>(
    state: &SessionState,
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
) -> crate::Result<i64> {
    let mut hash = DefaultHasher::new();
    hash_plan(plan, state, lineages, &mut hash)?;
    Ok(fold_hash(hash.finish()))
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
pub fn same_semantics<S: BuildHasher>(
    state_a: &SessionState,
    plan_a: &LogicalPlan,
    state_b: &SessionState,
    plan_b: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
) -> crate::Result<bool> {
    Ok(canonical_bytes(state_a, plan_a, lineages)? == canonical_bytes(state_b, plan_b, lineages)?)
}

fn canonical_bytes<S: BuildHasher>(
    state: &SessionState,
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
) -> crate::Result<Vec<u8>> {
    let pre_relations = build_relations(plan, lineages);
    let analyzed = state
        .analyzer()
        .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
        .map_err(crate::engine_err)?;
    let ctx = Ctx {
        lineages,
        relations: build_relations(&analyzed, lineages),
        user_comparisons: user_shaped_comparisons(plan, lineages, &pre_relations),
    };
    let mut sink = ByteSink(Vec::new());
    write_plan(&analyzed, &ctx, &mut sink);
    Ok(sink.0)
}

fn fold_hash(finished: u64) -> i64 {
    let bytes = finished.to_le_bytes();
    i64::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn hash_plan<S: BuildHasher>(
    plan: &LogicalPlan,
    state: &SessionState,
    lineages: &HashMap<String, LogicalPlan, S>,
    hash: &mut impl Hasher,
) -> crate::Result<()> {
    let pre_relations = build_relations(plan, lineages);
    let analyzed = state
        .analyzer()
        .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
        .map_err(crate::engine_err)?;
    let ctx = Ctx {
        lineages,
        relations: build_relations(&analyzed, lineages),
        user_comparisons: user_shaped_comparisons(plan, lineages, &pre_relations),
    };
    write_plan(&analyzed, &ctx, hash);
    Ok(())
}

pub(crate) fn write_str(hash: &mut impl Hasher, value: &str) {
    hash.write_usize(value.len());
    hash.write(value.as_bytes());
}

fn write_opt_usize(value: Option<usize>, hash: &mut impl Hasher) {
    match value {
        None => hash.write_u8(0),
        Some(size) => {
            hash.write_u8(1);
            hash.write_usize(size);
        }
    }
}

pub(crate) fn write_debug(hash: &mut impl Hasher, value: &impl std::fmt::Debug) {
    hash.write(format!("{value:?}").as_bytes());
}

pub(crate) fn write_norm_ident(hash: &mut impl Hasher, value: &str) {
    if !value.contains("_repark_") {
        write_str(hash, value);
        return;
    }
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_hexdigit() {
            let mut end = cursor;
            while end < bytes.len() && bytes[end].is_ascii_hexdigit() {
                end += 1;
            }
            if end - cursor >= 8 {
                out.push(b'#');
            } else {
                out.extend_from_slice(&bytes[cursor..end]);
            }
            cursor = end;
        } else {
            out.push(bytes[cursor]);
            cursor += 1;
        }
    }
    hash.write_usize(out.len());
    hash.write(&out);
}

fn write_norm_table(hash: &mut impl Hasher, reference: &TableReference) {
    match reference {
        TableReference::Bare { table } => {
            let name: &str = table;
            write_norm_ident(hash, name);
        }
        TableReference::Partial { schema, table } => {
            let first: &str = schema;
            let second: &str = table;
            write_norm_ident(hash, first);
            hash.write(b".");
            write_norm_ident(hash, second);
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            let first: &str = catalog;
            let second: &str = schema;
            let third: &str = table;
            write_norm_ident(hash, first);
            hash.write(b".");
            write_norm_ident(hash, second);
            hash.write(b".");
            write_norm_ident(hash, third);
        }
    }
}

pub(crate) fn cache_view_name(reference: &TableReference) -> Option<&str> {
    let table = reference.table();
    if table.starts_with("__repark_cache_") {
        Some(table)
    } else {
        None
    }
}

fn passthrough_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::Column(column) if column.name.as_str() == alias.name.as_str() => {
                Some(alias.name.as_str())
            }
            _ => None,
        },
        _ => None,
    }
}

fn is_passthrough(exprs: &[Expr], input: &LogicalPlan) -> bool {
    let fields = input.schema();
    if exprs.len() != fields.fields().len() {
        return false;
    }
    exprs
        .iter()
        .zip(fields.fields().iter())
        .all(|(expr, field)| passthrough_name(expr) == Some(field.name().as_str()))
}

pub(crate) fn write_plan<S: BuildHasher>(
    plan: &LogicalPlan,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    match plan {
        LogicalPlan::Projection(node) => {
            if is_passthrough(&node.expr, &node.input) {
                write_plan(&node.input, ctx, hash);
            } else {
                hash.write(b"Projection");
                hash.write_usize(node.expr.len());
                write_exprs(&node.expr, ctx, hash);
                write_plan(&node.input, ctx, hash);
            }
        }
        LogicalPlan::Filter(node) => {
            hash.write(b"Filter");
            write_expr(&node.predicate, ctx, hash);
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Sort(node) => {
            hash.write(b"Sort");
            hash.write_usize(node.expr.len());
            for sort in &node.expr {
                write_sort(sort, ctx, hash);
            }
            write_opt_usize(node.fetch, hash);
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Aggregate(node) => {
            hash.write(b"Aggregate");
            hash.write_usize(node.group_expr.len());
            write_exprs(&node.group_expr, ctx, hash);
            hash.write_usize(node.aggr_expr.len());
            write_exprs(&node.aggr_expr, ctx, hash);
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Window(node) => {
            hash.write(b"Window");
            hash.write_usize(node.window_expr.len());
            write_exprs(&node.window_expr, ctx, hash);
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Join(node) => {
            hash.write(b"Join");
            write_debug(hash, &node.join_type);
            write_debug(hash, &node.join_constraint);
            write_debug(hash, &node.null_equality);
            hash.write_usize(node.on.len());
            for (left, right) in &node.on {
                write_expr(left, ctx, hash);
                write_expr(right, ctx, hash);
            }
            write_opt_expr(node.filter.as_ref(), ctx, hash);
            write_plan(&node.left, ctx, hash);
            write_plan(&node.right, ctx, hash);
        }
        LogicalPlan::Repartition(node) => {
            hash.write(b"Repartition");
            write_debug(hash, &node.partitioning_scheme);
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Union(node) => {
            hash.write(b"Union");
            hash.write_usize(node.inputs.len());
            for input in &node.inputs {
                write_plan(input, ctx, hash);
            }
        }
        other => write_plan_leaf(other, ctx, hash),
    }
}

pub(crate) fn write_plan_leaf<S: BuildHasher>(
    plan: &LogicalPlan,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            if let Some(view) = cache_view_name(&scan.table_name)
                && let Some(definition) = ctx.lineages.get(view)
            {
                write_plan(definition, ctx, hash);
                return;
            }
            hash.write(b"TableScan");
            write_norm_table(hash, &scan.table_name);
            match &scan.projection {
                None => hash.write_u8(0),
                Some(columns) => {
                    hash.write_u8(1);
                    hash.write_usize(columns.len());
                    for column in columns {
                        hash.write_usize(*column);
                    }
                }
            }
            write_opt_usize(scan.fetch, hash);
            write_table_source(&scan.source, ctx, hash);
        }
        LogicalPlan::EmptyRelation(node) => {
            hash.write(b"EmptyRelation");
            hash.write_u8(u8::from(node.produce_one_row));
        }
        LogicalPlan::Subquery(node) => {
            hash.write(b"Subquery");
            hash.write_usize(node.outer_ref_columns.len());
            write_exprs(&node.outer_ref_columns, ctx, hash);
            write_plan(&node.subquery, ctx, hash);
        }
        LogicalPlan::SubqueryAlias(node) => {
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Limit(node) => {
            hash.write(b"Limit");
            write_opt_expr(node.skip.as_deref(), ctx, hash);
            write_opt_expr(node.fetch.as_deref(), ctx, hash);
            write_plan(&node.input, ctx, hash);
        }
        LogicalPlan::Values(node) => {
            hash.write(b"Values");
            hash.write_usize(node.values.len());
            for row in &node.values {
                hash.write_usize(row.len());
                write_exprs(row, ctx, hash);
            }
        }
        other => {
            hash.write(b"Opaque");
            write_debug(hash, other);
        }
    }
}

pub(crate) fn write_table_source<S: BuildHasher>(
    source: &Arc<dyn TableSource>,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    let provider = datafusion::datasource::default_table_source::source_as_provider(source);
    let Ok(provider) = provider else {
        hash.write(b"OpaqueSource");
        return;
    };
    if let Some(table) = provider.downcast_ref::<ListingTable>() {
        hash.write(b"Listing");
        hash.write_usize(table.table_paths().len());
        for url in table.table_paths() {
            write_str(hash, url.as_str());
        }
        return;
    }
    if let Some(table) = provider.downcast_ref::<GenerateSeriesTable>() {
        hash.write(b"GenerateSeries");
        write_debug(hash, table);
        return;
    }
    if provider.downcast_ref::<MemTable>().is_some() {
        hash.write(b"Mem");
        write_debug(hash, &Arc::as_ptr(&provider));
        return;
    }
    if let Some(table) = provider.downcast_ref::<ViewTable>() {
        hash.write(b"View");
        write_plan(table.logical_plan(), ctx, hash);
        return;
    }
    hash.write(b"Provider");
    match provider.table_type() {
        datafusion::logical_expr::TableType::Base => hash.write_u8(0),
        datafusion::logical_expr::TableType::View => hash.write_u8(1),
        datafusion::logical_expr::TableType::Temporary => hash.write_u8(2),
    }
}

#[cfg(test)]
mod tests {
    use super::semantic_hash;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;

    async fn fingerprint_of_sql(sql: &str) -> i64 {
        let context = SessionContext::new();
        let frame = context.sql(sql).await.expect("test sql must plan");
        let state = context.state();
        semantic_hash(&state, frame.logical_plan(), &HashMap::new()).expect("test plan must hash")
    }

    #[tokio::test]
    async fn alias_and_case_normalize_to_same_fingerprint() {
        let first = fingerprint_of_sql("SELECT 1 AS one").await;
        let second = fingerprint_of_sql("select 1 as ONE").await;
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn passthrough_projection_does_not_change_fingerprint() {
        let context = SessionContext::new();
        let frame = context
            .sql("SELECT 1 AS one, 2 AS two")
            .await
            .expect("test sql must plan");
        let plan = frame.logical_plan().clone();
        let state = context.state();
        let bare = semantic_hash(&state, &plan, &HashMap::new()).expect("test plan must hash");
        let wrapped = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
            .project(vec![
                datafusion::logical_expr::col("one"),
                datafusion::logical_expr::col("two"),
            ])
            .expect("test projection must build")
            .build()
            .expect("test plan must build");
        let projected =
            semantic_hash(&state, &wrapped, &HashMap::new()).expect("test plan must hash");
        assert_eq!(bare, projected);
    }

    #[tokio::test]
    async fn aliased_identity_projection_does_not_change_fingerprint() {
        let context = SessionContext::new();
        let frame = context
            .sql("SELECT 1 AS one, 2 AS two")
            .await
            .expect("test sql must plan");
        let plan = frame.logical_plan().clone();
        let state = context.state();
        let bare = semantic_hash(&state, &plan, &HashMap::new()).expect("test plan must hash");
        let wrapped = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
            .project(vec![
                datafusion::logical_expr::col("one").alias("one"),
                datafusion::logical_expr::col("two").alias("two"),
            ])
            .expect("test projection must build")
            .build()
            .expect("test plan must build");
        let projected =
            semantic_hash(&state, &wrapped, &HashMap::new()).expect("test plan must hash");
        assert_eq!(bare, projected);
    }

    #[tokio::test]
    async fn distinct_predicates_change_fingerprint() {
        let first = fingerprint_of_sql("SELECT 1 WHERE 1 = 1").await;
        let second = fingerprint_of_sql("SELECT 1 WHERE 1 = 2").await;
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn cube_and_rollup_have_distinct_fingerprints() {
        let cube = fingerprint_of_sql(
            "SELECT a, b, count(*) FROM (SELECT 1 AS a, 2 AS b) AS source GROUP BY CUBE (a, b)",
        )
        .await;
        let rollup = fingerprint_of_sql(
            "SELECT a, b, count(*) FROM (SELECT 1 AS a, 2 AS b) AS source GROUP BY ROLLUP (a, b)",
        )
        .await;
        assert_ne!(cube, rollup);
    }

    #[tokio::test]
    async fn join_column_relations_change_fingerprint() {
        let across = fingerprint_of_sql(
            "SELECT * FROM (SELECT 1 AS x) AS a JOIN (SELECT 2 AS x) AS b ON a.x = b.x",
        )
        .await;
        let same = fingerprint_of_sql(
            "SELECT * FROM (SELECT 1 AS x) AS a JOIN (SELECT 2 AS x) AS b ON a.x = a.x",
        )
        .await;
        assert_ne!(across, same);
        let project_across = fingerprint_of_sql(
            "SELECT a.x AS left, b.x AS right FROM (SELECT 1 AS x) AS a JOIN (SELECT 2 AS x) AS b ON a.x = b.x",
        )
        .await;
        let project_same = fingerprint_of_sql(
            "SELECT a.x AS left, a.x AS right FROM (SELECT 1 AS x) AS a JOIN (SELECT 2 AS x) AS b ON a.x = b.x",
        )
        .await;
        assert_ne!(project_across, project_same);
    }

    fn int32_context() -> SessionContext {
        let context = SessionContext::new();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int32, true),
        ]));
        let table = datafusion::datasource::memory::MemTable::try_new(schema, vec![Vec::new()])
            .expect("test table must build");
        context
            .register_table("t", std::sync::Arc::new(table))
            .expect("test table must register");
        context
    }

    async fn fingerprint_in(context: &SessionContext, sql: &str) -> i64 {
        let frame = context.sql(sql).await.expect("test sql must plan");
        let state = context.state();
        semantic_hash(&state, frame.logical_plan(), &HashMap::new()).expect("test plan must hash")
    }

    #[tokio::test]
    async fn user_written_cast_blocks_comparison_strip() {
        let context = int32_context();
        let plain = fingerprint_in(&context, "SELECT * FROM t WHERE x > 1").await;
        let cast = fingerprint_in(&context, "SELECT * FROM t WHERE CAST(x AS BIGINT) > 1").await;
        assert_ne!(plain, cast);
    }

    #[tokio::test]
    async fn swapped_comparison_operands_share_fingerprint() {
        let right =
            fingerprint_of_sql("SELECT * FROM (SELECT CAST(1 AS INT) AS x) AS v WHERE x > 1").await;
        let left =
            fingerprint_of_sql("SELECT * FROM (SELECT CAST(1 AS INT) AS x) AS v WHERE 1 < x").await;
        assert_eq!(left, right);
        let less =
            fingerprint_of_sql("SELECT * FROM (SELECT 1 AS x, 2 AS y) AS v WHERE x < y").await;
        let greater =
            fingerprint_of_sql("SELECT * FROM (SELECT 1 AS x, 2 AS y) AS v WHERE y > x").await;
        assert_eq!(less, greater);
    }

    #[tokio::test]
    async fn reordered_and_operands_share_fingerprint() {
        let first =
            fingerprint_of_sql("SELECT * FROM (SELECT 1 AS x, 2 AS y) AS v WHERE x > 1 AND y > 1")
                .await;
        let second =
            fingerprint_of_sql("SELECT * FROM (SELECT 1 AS x, 2 AS y) AS v WHERE y > 1 AND x > 1")
                .await;
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn out_of_range_literal_strips_on_both_cast_shapes() {
        let context = int32_context();
        let plain = fingerprint_in(&context, "SELECT * FROM t WHERE x > 5000000000").await;
        let cast = fingerprint_in(
            &context,
            "SELECT * FROM t WHERE CAST(x AS BIGINT) > 5000000000",
        )
        .await;
        assert_eq!(plain, cast);
        let wrapped = fingerprint_in(&context, "SELECT * FROM t WHERE x > 705032704").await;
        assert_ne!(plain, wrapped);
    }
}
