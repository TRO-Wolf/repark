use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::catalog::TableProvider;
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

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
pub fn semantic_hash(state: &SessionState, plan: &LogicalPlan) -> crate::Result<i64> {
    let analyzed = state
        .analyzer()
        .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
        .map_err(crate::engine_err)?;
    let mut canonical = String::new();
    write_plan(&analyzed, &mut canonical);
    let mut hasher = DefaultHasher::new();
    canonical.hash(&mut hasher);
    Ok(fold_hash(hasher.finish()))
}

fn fold_hash(finished: u64) -> i64 {
    let bytes = finished.to_le_bytes();
    i64::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn norm_ident(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut run = String::new();
    for ch in value.chars() {
        if ch.is_ascii_hexdigit() {
            run.push(ch);
        } else {
            flush_run(&mut run, &mut out);
            out.push(ch);
        }
    }
    flush_run(&mut run, &mut out);
    out
}

fn flush_run(run: &mut String, out: &mut String) {
    if run.len() >= 8 {
        out.push('#');
    } else {
        out.push_str(run);
    }
    run.clear();
}

fn norm_table(reference: &TableReference) -> String {
    match reference {
        TableReference::Bare { table } => norm_ident(table),
        TableReference::Partial { schema, table } => {
            format!("{}.{}", norm_ident(schema), norm_ident(table))
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => format!(
            "{}.{}.{}",
            norm_ident(catalog),
            norm_ident(schema),
            norm_ident(table)
        ),
    }
}

fn norm_qualifier(relation: Option<&TableReference>) -> String {
    relation.map_or_else(String::new, norm_table)
}

fn push_debug(out: &mut String, value: &impl std::fmt::Debug) {
    use std::fmt::Write as _;
    let _ = write!(out, "{value:?}");
}

fn write_plan(plan: &LogicalPlan, out: &mut String) {
    match plan {
        LogicalPlan::Projection(node) => {
            out.push_str("Projection[");
            write_exprs(&node.expr, out);
            out.push(']');
            write_plan(&node.input, out);
        }
        LogicalPlan::Filter(node) => {
            out.push_str("Filter(");
            write_expr(&node.predicate, out);
            out.push(')');
            write_plan(&node.input, out);
        }
        LogicalPlan::Sort(node) => {
            out.push_str("Sort[");
            for sort in &node.expr {
                write_sort(sort, out);
                out.push(';');
            }
            out.push_str("]f");
            push_debug(out, &node.fetch);
            write_plan(&node.input, out);
        }
        LogicalPlan::Aggregate(node) => {
            out.push_str("Aggregate[");
            write_exprs(&node.group_expr, out);
            out.push('|');
            write_exprs(&node.aggr_expr, out);
            out.push(']');
            write_plan(&node.input, out);
        }
        LogicalPlan::Window(node) => {
            out.push_str("Window[");
            write_exprs(&node.window_expr, out);
            out.push(']');
            write_plan(&node.input, out);
        }
        LogicalPlan::Join(node) => {
            out.push_str("Join(");
            push_debug(out, &node.join_type);
            out.push('|');
            push_debug(out, &node.join_constraint);
            out.push('|');
            push_debug(out, &node.null_equality);
            out.push_str("#[");
            for (left, right) in &node.on {
                write_expr(left, out);
                out.push('=');
                write_expr(right, out);
                out.push(';');
            }
            out.push_str("]F");
            if let Some(filter) = &node.filter {
                write_expr(filter, out);
            }
            out.push(')');
            write_plan(&node.left, out);
            write_plan(&node.right, out);
        }
        LogicalPlan::Repartition(node) => {
            out.push_str("Repartition(");
            push_debug(out, &node.partitioning_scheme);
            out.push(')');
            write_plan(&node.input, out);
        }
        LogicalPlan::Union(node) => {
            out.push_str("Union[");
            for input in &node.inputs {
                write_plan(input, out);
                out.push(';');
            }
            out.push(']');
        }
        other => write_plan_leaf(other, out),
    }
}

fn write_plan_leaf(plan: &LogicalPlan, out: &mut String) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            out.push_str("TableScan(");
            out.push_str(&norm_table(&scan.table_name));
            out.push_str("#p");
            push_debug(out, &scan.projection);
            out.push_str("#f");
            write_exprs(&scan.filters, out);
            out.push_str("#t");
            push_debug(out, &scan.fetch);
            out.push_str("#s");
            write_table_source(&scan.source, out);
            out.push(')');
        }
        LogicalPlan::EmptyRelation(node) => {
            out.push_str("EmptyRelation(");
            push_debug(out, &node.produce_one_row);
            out.push(')');
        }
        LogicalPlan::Subquery(node) => {
            out.push_str("Subquery[");
            write_exprs(&node.outer_ref_columns, out);
            out.push(']');
            write_plan(&node.subquery, out);
        }
        LogicalPlan::SubqueryAlias(node) => {
            out.push_str("SubqueryAlias(");
            write_plan(&node.input, out);
            out.push(')');
        }
        LogicalPlan::Limit(node) => {
            out.push_str("Limit(");
            out.push('s');
            if let Some(skip) = &node.skip {
                write_expr(skip, out);
            }
            out.push('f');
            if let Some(fetch) = &node.fetch {
                write_expr(fetch, out);
            }
            out.push(')');
            write_plan(&node.input, out);
        }
        LogicalPlan::Values(node) => {
            out.push_str("Values[");
            for row in &node.values {
                write_exprs(row, out);
                out.push(';');
            }
            out.push(']');
        }
        other => {
            push_debug(out, other);
        }
    }
}

fn write_table_source(source: &Arc<dyn TableSource>, out: &mut String) {
    let provider = datafusion::datasource::default_table_source::source_as_provider(source);
    let Ok(provider) = provider else {
        out.push_str("OpaqueSource");
        return;
    };
    if let Some(table) = provider.downcast_ref::<ListingTable>() {
        out.push_str("Listing[");
        for url in table.table_paths() {
            push_debug(out, url);
            out.push(';');
        }
        out.push(']');
        return;
    }
    if let Some(table) = provider.downcast_ref::<GenerateSeriesTable>() {
        out.push_str("GenerateSeries(");
        push_debug(out, table);
        out.push(')');
        return;
    }
    if let Some(table) = provider.downcast_ref::<MemTable>() {
        out.push_str("Mem[");
        for field in table.schema().fields() {
            out.push_str(&norm_ident(field.name()));
            out.push(':');
            push_debug(out, field.data_type());
            out.push_str(if field.is_nullable() { "?;" } else { "!;" });
        }
        out.push(']');
        return;
    }
    if let Some(table) = provider.downcast_ref::<ViewTable>() {
        out.push_str("View(");
        write_plan(table.logical_plan(), out);
        out.push(')');
        return;
    }
    out.push_str("Provider(");
    push_debug(out, &provider.table_type());
    out.push(')');
}

fn write_exprs(exprs: &[Expr], out: &mut String) {
    for expr in exprs {
        write_expr(expr, out);
        out.push(';');
    }
}

fn write_sort(expr: &SortExpr, out: &mut String) {
    out.push_str(if expr.asc { "A" } else { "D" });
    out.push_str(if expr.nulls_first { "N" } else { "n" });
    write_expr(&expr.expr, out);
}

fn write_expr(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Alias(alias) => {
            out.push_str("Alias(");
            write_expr(&alias.expr, out);
            out.push(')');
        }
        Expr::Column(column) => {
            out.push_str("C(");
            out.push_str(&norm_qualifier(column.relation.as_ref()));
            out.push('.');
            out.push_str(&norm_ident(&column.name));
            out.push(')');
        }
        Expr::Literal(value, _) => {
            out.push_str("L(");
            push_debug(out, value);
            out.push(')');
        }
        Expr::BinaryExpr(binary) => {
            out.push_str("B(");
            write_expr(&binary.left, out);
            push_debug(out, &binary.op);
            write_expr(&binary.right, out);
            out.push(')');
        }
        Expr::Not(inner) => write_wrapped("Not", inner, out),
        Expr::Negative(inner) => write_wrapped("Neg", inner, out),
        Expr::IsNull(inner) => write_wrapped("IsNull", inner, out),
        Expr::IsNotNull(inner) => write_wrapped("IsNotNull", inner, out),
        Expr::IsTrue(inner) => write_wrapped("IsTrue", inner, out),
        Expr::IsFalse(inner) => write_wrapped("IsFalse", inner, out),
        Expr::IsUnknown(inner) => write_wrapped("IsUnknown", inner, out),
        Expr::IsNotTrue(inner) => write_wrapped("IsNotTrue", inner, out),
        Expr::IsNotFalse(inner) => write_wrapped("IsNotFalse", inner, out),
        Expr::IsNotUnknown(inner) => write_wrapped("IsNotUnknown", inner, out),
        other => write_expr_compound(other, out),
    }
}

fn write_wrapped(tag: &str, inner: &Expr, out: &mut String) {
    out.push_str(tag);
    out.push('(');
    write_expr(inner, out);
    out.push(')');
}

fn write_expr_compound(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Between(between) => {
            out.push_str("Between(");
            write_expr(&between.expr, out);
            push_debug(out, &between.negated);
            write_expr(&between.low, out);
            write_expr(&between.high, out);
            out.push(')');
        }
        Expr::Case(case) => {
            out.push_str("Case(");
            if let Some(operand) = &case.expr {
                write_expr(operand, out);
            }
            for (when, then) in &case.when_then_expr {
                write_expr(when, out);
                write_expr(then, out);
            }
            if let Some(other) = &case.else_expr {
                write_expr(other, out);
            }
            out.push(')');
        }
        Expr::Cast(cast) => {
            out.push_str("Cast(");
            write_expr(&cast.expr, out);
            push_debug(out, cast.field.data_type());
            out.push(')');
        }
        Expr::TryCast(cast) => {
            out.push_str("TryCast(");
            write_expr(&cast.expr, out);
            push_debug(out, cast.field.data_type());
            out.push(')');
        }
        Expr::Like(like) => {
            out.push_str("Like(");
            push_debug(out, &like.negated);
            push_debug(out, &like.escape_char);
            push_debug(out, &like.case_insensitive);
            write_expr(&like.expr, out);
            write_expr(&like.pattern, out);
            out.push(')');
        }
        Expr::SimilarTo(like) => {
            out.push_str("SimilarTo(");
            push_debug(out, &like.negated);
            push_debug(out, &like.escape_char);
            write_expr(&like.expr, out);
            write_expr(&like.pattern, out);
            out.push(')');
        }
        Expr::InList(list) => {
            out.push_str("InList(");
            write_expr(&list.expr, out);
            push_debug(out, &list.negated);
            write_exprs(&list.list, out);
            out.push(')');
        }
        Expr::GroupingSet(set) => {
            out.push_str("GroupingSet(");
            match set {
                datafusion::logical_expr::GroupingSet::Rollup(exprs)
                | datafusion::logical_expr::GroupingSet::Cube(exprs) => {
                    write_exprs(exprs, out);
                }
                datafusion::logical_expr::GroupingSet::GroupingSets(groups) => {
                    for group in groups {
                        write_exprs(group, out);
                        out.push('|');
                    }
                }
            }
            out.push(')');
        }
        other => write_expr_function(other, out),
    }
}

fn write_expr_function(expr: &Expr, out: &mut String) {
    match expr {
        Expr::ScalarFunction(function) => {
            out.push_str("ScalarFunction(");
            out.push_str(function.func.name());
            write_exprs(&function.args, out);
            out.push(')');
        }
        Expr::AggregateFunction(function) => {
            out.push_str("AggregateFunction(");
            out.push_str(function.func.name());
            push_debug(out, &function.params.distinct);
            write_exprs(&function.params.args, out);
            if let Some(filter) = &function.params.filter {
                write_expr(filter, out);
            }
            for sort in &function.params.order_by {
                write_sort(sort, out);
            }
            push_debug(out, &function.params.null_treatment);
            out.push(')');
        }
        Expr::WindowFunction(function) => {
            out.push_str("WindowFunction(");
            push_debug(out, &function.fun);
            write_exprs(&function.params.args, out);
            write_exprs(&function.params.partition_by, out);
            for sort in &function.params.order_by {
                write_sort(sort, out);
            }
            push_debug(out, &function.params.window_frame);
            push_debug(out, &function.params.null_treatment);
            push_debug(out, &function.params.distinct);
            if let Some(filter) = &function.params.filter {
                write_expr(filter, out);
            }
            out.push(')');
        }
        Expr::Exists(exists) => {
            out.push_str("Exists(");
            push_debug(out, &exists.negated);
            write_plan(&exists.subquery.subquery, out);
            out.push(')');
        }
        Expr::InSubquery(query) => {
            out.push_str("InSubquery(");
            write_expr(&query.expr, out);
            push_debug(out, &query.negated);
            write_plan(&query.subquery.subquery, out);
            out.push(')');
        }
        Expr::ScalarSubquery(query) => {
            out.push_str("ScalarSubquery(");
            write_plan(&query.subquery, out);
            out.push(')');
        }
        Expr::Unnest(unnest) => {
            out.push_str("Unnest(");
            write_expr(&unnest.expr, out);
            out.push(')');
        }
        Expr::OuterReferenceColumn(_, column) => {
            out.push_str("OC(");
            out.push_str(&norm_qualifier(column.relation.as_ref()));
            out.push('.');
            out.push_str(&norm_ident(&column.name));
            out.push(')');
        }
        other => {
            push_debug(out, other);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn norm_ident_replaces_long_hex_runs() {
        assert_eq!(norm_ident("id"), "id");
        assert_eq!(norm_ident("a"), "a");
        assert_eq!(norm_ident("key"), "key");
        assert_eq!(
            norm_ident("__repark_sel_abc123def456_2_a"),
            "__repark_sel_#_2_a"
        );
        assert_eq!(
            norm_ident("__repark_cache_0123456789abcdef0123456789abcdef"),
            "__repark_cache_#"
        );
    }

    #[test]
    fn fold_hash_stays_in_java_int_range() {
        for finished in [
            0_u64,
            1_u64,
            u64::MAX,
            0x1_0000_0000_u64,
            0xFFFF_FFFF_FFFF_FFFF_u64,
        ] {
            let folded = fold_hash(finished);
            assert!(
                i32::try_from(folded).is_ok(),
                "folded {folded} out of range"
            );
        }
        assert_eq!(fold_hash(0), 0);
        assert_eq!(fold_hash(0xFFFF_FFFF), -1);
        assert_eq!(fold_hash(0x7FFF_FFFF), i64::from(i32::MAX));
    }

    #[test]
    fn render_uri_names_local_files_with_triple_slash() {
        let url = ObjectStoreUrl::parse("file://").expect("file url parses");
        let absolute = ObjectPath::from("/tmp/plan/a_0.parquet");
        assert_eq!(render_uri(&url, &absolute), "file:///tmp/plan/a_0.parquet");
        let relative = ObjectPath::from("tmp/plan/a_0.parquet");
        assert_eq!(render_uri(&url, &relative), "file:///tmp/plan/a_0.parquet");
    }

    #[test]
    fn render_uri_keeps_remote_scheme_unchanged() {
        let url = ObjectStoreUrl::parse("s3://bucket").expect("s3 url parses");
        let location = ObjectPath::from("warehouse/a_0.parquet");
        assert_eq!(
            render_uri(&url, &location),
            "s3://bucket/warehouse/a_0.parquet"
        );
    }

    #[tokio::test]
    async fn semantic_hash_ignores_output_alias_names() {
        let context = datafusion::prelude::SessionContext::new();
        let first = context.sql("SELECT 1 AS a").await.expect("first plans");
        let second = context.sql("SELECT 1 AS b").await.expect("second plans");
        let (state, first_plan) = first.into_parts();
        let (_, second_plan) = second.into_parts();
        let first_hash = semantic_hash(&state, &first_plan).expect("first hashes");
        let second_hash = semantic_hash(&state, &second_plan).expect("second hashes");
        assert_eq!(first_hash, second_hash);
    }

    #[tokio::test]
    async fn semantic_hash_sees_literals_limits_and_order() {
        let context = datafusion::prelude::SessionContext::new();
        let low = context
            .sql("SELECT * FROM (VALUES (1), (2)) AS source(id) WHERE id > 1")
            .await
            .expect("low plans");
        let high = context
            .sql("SELECT * FROM (VALUES (1), (2)) AS source(id) WHERE id > 2")
            .await
            .expect("high plans");
        let (state, low_plan) = low.into_parts();
        let (_, high_plan) = high.into_parts();
        let low_hash = semantic_hash(&state, &low_plan).expect("low hashes");
        let high_hash = semantic_hash(&state, &high_plan).expect("high hashes");
        assert_ne!(low_hash, high_hash);
        let limited = context
            .sql("SELECT * FROM (VALUES (1), (2)) AS source(id) LIMIT 1")
            .await
            .expect("limit plans");
        let (_, limited_plan) = limited.into_parts();
        let limited_hash = semantic_hash(&state, &limited_plan).expect("limit hashes");
        assert_ne!(low_hash, limited_hash);
    }

    #[tokio::test]
    async fn input_files_is_empty_without_a_scan() {
        let context = datafusion::prelude::SessionContext::new();
        let frame = context.sql("SELECT 1 AS a").await.expect("select plans");
        let physical = frame.create_physical_plan().await.expect("physical builds");
        assert_eq!(input_files(&physical), Vec::<String>::new());
    }

    #[tokio::test]
    async fn input_files_lists_a_csv_scan() {
        let directory = tempfile::tempdir().expect("tempdir builds");
        let path = directory.path().join("scan.csv");
        std::fs::write(&path, "id\n1\n2\n").expect("csv writes");
        let context = datafusion::prelude::SessionContext::new();
        let frame = context
            .read_csv(
                path.to_str().expect("path is utf8"),
                datafusion::prelude::CsvReadOptions::default(),
            )
            .await
            .expect("csv plans");
        let physical = frame.create_physical_plan().await.expect("physical builds");
        let files = input_files(&physical);
        assert_eq!(files.len(), 1);
        assert!(files[0].starts_with("file://"));
        assert!(files[0].ends_with("scan.csv"));
    }
}
