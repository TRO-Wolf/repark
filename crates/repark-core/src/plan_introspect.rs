use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::TableReference;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::view::ViewTable;
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::functions_table::generate_series::GenerateSeriesTable;
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path as ObjectPath;
use url::Url;

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

struct Ctx<'a, S: BuildHasher> {
    state: &'a SessionState,
    lineages: &'a HashMap<String, LogicalPlan, S>,
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
pub fn semantic_hash<S: BuildHasher>(
    state: &SessionState,
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
) -> crate::Result<i64> {
    let ctx = Ctx { state, lineages };
    let mut hash = DefaultHasher::new();
    hash_plan(plan, &ctx, &mut hash)?;
    Ok(fold_hash(hash.finish()))
}

fn fold_hash(finished: u64) -> i64 {
    let bytes = finished.to_le_bytes();
    i64::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn hash_plan<S: BuildHasher>(
    plan: &LogicalPlan,
    ctx: &Ctx<'_, S>,
    hash: &mut DefaultHasher,
) -> crate::Result<()> {
    if needs_analyze(plan) {
        let analyzed = ctx
            .state
            .analyzer()
            .execute_and_check(plan.clone(), ctx.state.config_options(), |_, _| {})
            .map_err(crate::engine_err)?;
        write_plan(&analyzed, ctx, hash);
    } else {
        write_plan(plan, ctx, hash);
    }
    Ok(())
}

fn needs_analyze(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(node) => needs_analyze(&node.input),
        LogicalPlan::Filter(node) => needs_analyze(&node.input),
        LogicalPlan::Sort(node) => needs_analyze(&node.input),
        LogicalPlan::Aggregate(node) => needs_analyze(&node.input),
        LogicalPlan::Window(node) => needs_analyze(&node.input),
        LogicalPlan::Join(node) => needs_analyze(&node.left) || needs_analyze(&node.right),
        LogicalPlan::Repartition(node) => needs_analyze(&node.input),
        LogicalPlan::Union(node) => node.inputs.iter().any(|input| needs_analyze(input)),
        LogicalPlan::Subquery(node) => needs_analyze(&node.subquery),
        LogicalPlan::SubqueryAlias(node) => needs_analyze(&node.input),
        LogicalPlan::Limit(node) => needs_analyze(&node.input),
        LogicalPlan::TableScan(scan) => is_view_source(&scan.source),
        LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => false,
        _ => true,
    }
}

fn is_view_source(source: &Arc<dyn TableSource>) -> bool {
    datafusion::datasource::default_table_source::source_as_provider(source)
        .is_ok_and(|provider| provider.downcast_ref::<ViewTable>().is_some())
}

fn write_str(hash: &mut DefaultHasher, value: &str) {
    hash.write_usize(value.len());
    hash.write(value.as_bytes());
}

fn write_opt_usize(value: Option<usize>, hash: &mut DefaultHasher) {
    match value {
        None => hash.write_u8(0),
        Some(size) => {
            hash.write_u8(1);
            hash.write_usize(size);
        }
    }
}

fn write_debug(hash: &mut DefaultHasher, value: &impl std::fmt::Debug) {
    hash.write(format!("{value:?}").as_bytes());
}

fn write_norm_ident(hash: &mut DefaultHasher, value: &str) {
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

fn write_norm_table(hash: &mut DefaultHasher, reference: &TableReference) {
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

fn write_column_ref<S: BuildHasher>(
    hash: &mut DefaultHasher,
    ctx: &Ctx<'_, S>,
    relation: Option<&TableReference>,
    name: &str,
) {
    if let Some(reference) = relation {
        let table = reference.table();
        if table != "?table?" && !ctx.lineages.contains_key(table) {
            hash.write_u8(1);
            write_norm_table(hash, reference);
            write_norm_ident(hash, name);
            return;
        }
    }
    hash.write_u8(0);
    write_norm_ident(hash, name);
}

fn cache_view_name(reference: &TableReference) -> Option<&str> {
    let table = reference.table();
    if table.starts_with("__repark_cache_") {
        Some(table)
    } else {
        None
    }
}

fn is_passthrough(exprs: &[Expr], input: &LogicalPlan) -> bool {
    let fields = input.schema();
    if exprs.len() != fields.fields().len() {
        return false;
    }
    exprs.iter().zip(fields.fields().iter()).all(|(expr, field)| {
        matches!(expr, Expr::Column(column) if column.name.as_str() == field.name().as_str())
    })
}

fn write_plan<S: BuildHasher>(plan: &LogicalPlan, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
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

fn write_plan_leaf<S: BuildHasher>(plan: &LogicalPlan, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
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

fn write_table_source<S: BuildHasher>(
    source: &Arc<dyn TableSource>,
    ctx: &Ctx<'_, S>,
    hash: &mut DefaultHasher,
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

fn write_exprs<S: BuildHasher>(exprs: &[Expr], ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
    for expr in exprs {
        write_expr(expr, ctx, hash);
    }
}

fn write_opt_expr<S: BuildHasher>(expr: Option<&Expr>, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
    match expr {
        None => hash.write_u8(0),
        Some(inner) => {
            hash.write_u8(1);
            write_expr(inner, ctx, hash);
        }
    }
}

fn write_sort<S: BuildHasher>(sort: &SortExpr, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
    hash.write_u8(u8::from(sort.asc));
    hash.write_u8(u8::from(sort.nulls_first));
    write_expr(&sort.expr, ctx, hash);
}

fn write_expr<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
    match expr {
        Expr::Alias(alias) => {
            hash.write(b"Alias");
            write_expr(&alias.expr, ctx, hash);
        }
        Expr::Column(column) => {
            hash.write(b"C");
            write_column_ref(hash, ctx, column.relation.as_ref(), &column.name);
        }
        Expr::Literal(value, _) => {
            hash.write(b"L");
            write_debug(hash, value);
        }
        Expr::BinaryExpr(binary) => {
            hash.write(b"B");
            write_expr(&binary.left, ctx, hash);
            write_debug(hash, &binary.op);
            write_expr(&binary.right, ctx, hash);
        }
        Expr::Not(inner) => write_wrapped(b"Not", inner, ctx, hash),
        Expr::Negative(inner) => write_wrapped(b"Neg", inner, ctx, hash),
        Expr::IsNull(inner) => write_wrapped(b"IsNull", inner, ctx, hash),
        Expr::IsNotNull(inner) => write_wrapped(b"IsNotNull", inner, ctx, hash),
        Expr::IsTrue(inner) => write_wrapped(b"IsTrue", inner, ctx, hash),
        Expr::IsFalse(inner) => write_wrapped(b"IsFalse", inner, ctx, hash),
        Expr::IsUnknown(inner) => write_wrapped(b"IsUnknown", inner, ctx, hash),
        Expr::IsNotTrue(inner) => write_wrapped(b"IsNotTrue", inner, ctx, hash),
        Expr::IsNotFalse(inner) => write_wrapped(b"IsNotFalse", inner, ctx, hash),
        Expr::IsNotUnknown(inner) => write_wrapped(b"IsNotUnknown", inner, ctx, hash),
        other => write_expr_compound(other, ctx, hash),
    }
}

fn write_wrapped<S: BuildHasher>(
    tag: &[u8],
    inner: &Expr,
    ctx: &Ctx<'_, S>,
    hash: &mut DefaultHasher,
) {
    hash.write(tag);
    write_expr(inner, ctx, hash);
}

fn write_expr_compound<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
    match expr {
        Expr::Between(between) => {
            hash.write(b"Between");
            write_expr(&between.expr, ctx, hash);
            hash.write_u8(u8::from(between.negated));
            write_expr(&between.low, ctx, hash);
            write_expr(&between.high, ctx, hash);
        }
        Expr::Case(case) => {
            hash.write(b"Case");
            write_opt_expr(case.expr.as_deref(), ctx, hash);
            hash.write_usize(case.when_then_expr.len());
            for (when, then) in &case.when_then_expr {
                write_expr(when, ctx, hash);
                write_expr(then, ctx, hash);
            }
            write_opt_expr(case.else_expr.as_deref(), ctx, hash);
        }
        Expr::Cast(cast) => {
            hash.write(b"Cast");
            write_expr(&cast.expr, ctx, hash);
            write_data_type(cast.field.data_type(), hash);
        }
        Expr::TryCast(cast) => {
            hash.write(b"TryCast");
            write_expr(&cast.expr, ctx, hash);
            write_data_type(cast.field.data_type(), hash);
        }
        Expr::Like(like) => {
            hash.write(b"Like");
            hash.write_u8(u8::from(like.negated));
            match like.escape_char {
                None => hash.write_u8(0),
                Some(ch) => {
                    hash.write_u8(1);
                    hash.write_u32(u32::from(ch));
                }
            }
            hash.write_u8(u8::from(like.case_insensitive));
            write_expr(&like.expr, ctx, hash);
            write_expr(&like.pattern, ctx, hash);
        }
        Expr::SimilarTo(like) => {
            hash.write(b"SimilarTo");
            hash.write_u8(u8::from(like.negated));
            write_expr(&like.expr, ctx, hash);
            write_expr(&like.pattern, ctx, hash);
        }
        Expr::InList(list) => {
            hash.write(b"InList");
            write_expr(&list.expr, ctx, hash);
            hash.write_u8(u8::from(list.negated));
            hash.write_usize(list.list.len());
            write_exprs(&list.list, ctx, hash);
        }
        Expr::GroupingSet(set) => match set {
            datafusion::logical_expr::GroupingSet::Rollup(exprs) => {
                hash.write(b"Rollup");
                hash.write_usize(exprs.len());
                write_exprs(exprs, ctx, hash);
            }
            datafusion::logical_expr::GroupingSet::Cube(exprs) => {
                hash.write(b"Cube");
                hash.write_usize(exprs.len());
                write_exprs(exprs, ctx, hash);
            }
            datafusion::logical_expr::GroupingSet::GroupingSets(groups) => {
                hash.write(b"GroupingSets");
                hash.write_usize(groups.len());
                for group in groups {
                    hash.write_usize(group.len());
                    write_exprs(group, ctx, hash);
                }
            }
        },
        other => write_expr_function(other, ctx, hash),
    }
}

fn write_expr_function<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>, hash: &mut DefaultHasher) {
    match expr {
        Expr::ScalarFunction(function) => {
            hash.write(b"ScalarFunction");
            write_str(hash, function.func.name());
            hash.write_usize(function.args.len());
            write_exprs(&function.args, ctx, hash);
        }
        Expr::AggregateFunction(function) => {
            hash.write(b"AggregateFunction");
            write_str(hash, function.func.name());
            hash.write_u8(u8::from(function.params.distinct));
            hash.write_usize(function.params.args.len());
            write_exprs(&function.params.args, ctx, hash);
            write_opt_expr(function.params.filter.as_deref(), ctx, hash);
            hash.write_usize(function.params.order_by.len());
            for sort in &function.params.order_by {
                write_sort(sort, ctx, hash);
            }
            write_debug(hash, &function.params.null_treatment);
        }
        Expr::WindowFunction(function) => {
            hash.write(b"WindowFunction");
            write_debug(hash, &function.fun);
            hash.write_usize(function.params.args.len());
            write_exprs(&function.params.args, ctx, hash);
            hash.write_usize(function.params.partition_by.len());
            write_exprs(&function.params.partition_by, ctx, hash);
            hash.write_usize(function.params.order_by.len());
            for sort in &function.params.order_by {
                write_sort(sort, ctx, hash);
            }
            write_debug(hash, &function.params.window_frame);
            write_debug(hash, &function.params.null_treatment);
            hash.write_u8(u8::from(function.params.distinct));
            write_opt_expr(function.params.filter.as_deref(), ctx, hash);
        }
        Expr::Exists(exists) => {
            hash.write(b"Exists");
            hash.write_u8(u8::from(exists.negated));
            write_plan(&exists.subquery.subquery, ctx, hash);
        }
        Expr::InSubquery(query) => {
            hash.write(b"InSubquery");
            write_expr(&query.expr, ctx, hash);
            hash.write_u8(u8::from(query.negated));
            write_plan(&query.subquery.subquery, ctx, hash);
        }
        Expr::ScalarSubquery(query) => {
            hash.write(b"ScalarSubquery");
            write_plan(&query.subquery, ctx, hash);
        }
        Expr::Unnest(unnest) => {
            hash.write(b"Unnest");
            write_expr(&unnest.expr, ctx, hash);
        }
        Expr::OuterReferenceColumn(_, column) => {
            hash.write(b"OC");
            write_column_ref(hash, ctx, column.relation.as_ref(), &column.name);
        }
        other => {
            hash.write(b"OpaqueExpr");
            write_debug(hash, other);
        }
    }
}

fn write_data_type(kind: &DataType, hash: &mut DefaultHasher) {
    write_debug(hash, kind);
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
}
