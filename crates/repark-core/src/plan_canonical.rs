use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};

use arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, TableReference};
use datafusion::datasource::view::ViewTable;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, SortExpr};

use super::plan_introspect::{
    ByteSink, Ctx, cache_view_name, write_debug, write_norm_ident, write_plan, write_str,
};

#[derive(Default)]
pub(crate) struct RelTable {
    ordinals: HashMap<String, usize>,
    fields: Vec<HashMap<String, DataType>>,
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[must_use]
pub(crate) fn build_relations<S: BuildHasher>(
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
) -> RelTable {
    let mut table = RelTable::default();
    walk_plan(plan, lineages, &mut table);
    table
}

fn record_wrapper(table: &mut RelTable, reference: &TableReference, before: usize) {
    let name = reference.table().to_string();
    if table.ordinals.contains_key(&name) {
        return;
    }
    if table.fields.len() == before {
        let ordinal = table.fields.len();
        table.fields.push(HashMap::new());
        table.ordinals.insert(name, ordinal);
    } else {
        table.ordinals.insert(name, before);
    }
}

fn record_scan(table: &mut RelTable, plan: &LogicalPlan, reference: &TableReference) {
    let ordinal = table.fields.len();
    table
        .ordinals
        .entry(reference.table().to_string())
        .or_insert(ordinal);
    let mut columns = HashMap::new();
    for field in plan.schema().fields() {
        columns.insert(field.name().clone(), field.data_type().clone());
    }
    table.fields.push(columns);
}

fn walk_plan<S: BuildHasher>(
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
    table: &mut RelTable,
) {
    if let LogicalPlan::TableScan(scan) = plan {
        if let Some(view) = cache_view_name(&scan.table_name)
            && let Some(definition) = lineages.get(view)
        {
            let before = table.fields.len();
            walk_plan(definition, lineages, table);
            record_wrapper(table, &scan.table_name, before);
            return;
        }
        if let Ok(provider) =
            datafusion::datasource::default_table_source::source_as_provider(&scan.source)
            && let Some(view) = provider.downcast_ref::<ViewTable>()
        {
            walk_plan(view.logical_plan(), lineages, table);
            return;
        }
        record_scan(table, plan, &scan.table_name);
        return;
    }
    if let LogicalPlan::SubqueryAlias(node) = plan {
        let before = table.fields.len();
        walk_plan(&node.input, lineages, table);
        record_wrapper(table, &node.alias, before);
        return;
    }
    for expr in plan.expressions() {
        walk_expr(&expr, lineages, table);
    }
    for input in plan.inputs() {
        walk_plan(input, lineages, table);
    }
}

fn walk_expr<S: BuildHasher>(
    expr: &Expr,
    lineages: &HashMap<String, LogicalPlan, S>,
    table: &mut RelTable,
) {
    let _ = expr.apply(|node| {
        match node {
            Expr::Exists(exists) => walk_plan(&exists.subquery.subquery, lineages, table),
            Expr::InSubquery(query) => walk_plan(&query.subquery.subquery, lineages, table),
            Expr::ScalarSubquery(query) => walk_plan(&query.subquery, lineages, table),
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

#[must_use]
pub(crate) fn resolve_column(
    table: &RelTable,
    relation: Option<&TableReference>,
    name: &str,
) -> usize {
    if let Some(reference) = relation
        && let Some(ordinal) = table.ordinals.get(reference.table())
    {
        return *ordinal;
    }
    let mut found = None;
    for (ordinal, columns) in table.fields.iter().enumerate() {
        if columns.contains_key(name) {
            if found.is_some() {
                return usize::MAX;
            }
            found = Some(ordinal);
        }
    }
    found.unwrap_or(usize::MAX)
}

#[must_use]
pub(crate) fn column_native_type(table: &RelTable, ordinal: usize, name: &str) -> Option<DataType> {
    table.fields.get(ordinal)?.get(name).cloned()
}

fn write_column_ref(hash: &mut impl Hasher, table: &RelTable, column: &Column) {
    hash.write_u8(0);
    hash.write_usize(resolve_column(
        table,
        column.relation.as_ref(),
        &column.name,
    ));
    write_norm_ident(hash, &column.name);
}

fn scalar_int_value(value: &datafusion::common::ScalarValue) -> Option<i128> {
    match value {
        datafusion::common::ScalarValue::Int8(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::Int16(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::Int32(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::Int64(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::UInt8(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::UInt16(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::UInt32(inner) => inner.map(i128::from),
        datafusion::common::ScalarValue::UInt64(inner) => inner.map(i128::from),
        _ => None,
    }
}

fn is_int_type(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn fits_int_value(bits: i128, kind: &DataType) -> bool {
    match kind {
        DataType::Int8 => i8::try_from(bits).is_ok(),
        DataType::Int16 => i16::try_from(bits).is_ok(),
        DataType::Int32 => i32::try_from(bits).is_ok(),
        DataType::Int64 => i64::try_from(bits).is_ok(),
        DataType::UInt8 => u8::try_from(bits).is_ok(),
        DataType::UInt16 => u16::try_from(bits).is_ok(),
        DataType::UInt32 => u32::try_from(bits).is_ok(),
        DataType::UInt64 => u64::try_from(bits).is_ok(),
        _ => false,
    }
}

fn comparison_literal(expr: &Expr) -> Option<(i128, DataType)> {
    match expr {
        Expr::Literal(value, _) => scalar_int_value(value).map(|bits| (bits, value.data_type())),
        Expr::Cast(cast) if is_int_type(cast.field.data_type()) => {
            comparison_literal(&cast.expr).map(|(bits, _)| (bits, cast.field.data_type().clone()))
        }
        _ => None,
    }
}

fn record_column(expr: &Expr) -> Option<(&Column, bool)> {
    match expr {
        Expr::Column(column) => Some((column, false)),
        Expr::Cast(cast) if is_int_type(cast.field.data_type()) => {
            record_column(&cast.expr).map(|(column, _)| (column, true))
        }
        Expr::TryCast(cast) if is_int_type(cast.field.data_type()) => {
            record_column(&cast.expr).map(|(column, _)| (column, true))
        }
        _ => None,
    }
}

fn record_bare_literal_bits(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Literal(value, _) => scalar_int_value(value),
        _ => None,
    }
}

#[must_use]
pub(crate) fn user_shaped_comparisons<S: BuildHasher>(
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
    relations: &RelTable,
) -> std::collections::HashSet<(String, String, i128)> {
    let mut out = std::collections::HashSet::new();
    collect_plan_comparisons(plan, lineages, relations, &mut out);
    out
}

fn collect_plan_comparisons<S: BuildHasher>(
    plan: &LogicalPlan,
    lineages: &HashMap<String, LogicalPlan, S>,
    relations: &RelTable,
    out: &mut std::collections::HashSet<(String, String, i128)>,
) {
    if let LogicalPlan::TableScan(scan) = plan {
        if let Some(view) = cache_view_name(&scan.table_name)
            && let Some(definition) = lineages.get(view)
        {
            collect_plan_comparisons(definition, lineages, relations, out);
            return;
        }
        if let Ok(provider) =
            datafusion::datasource::default_table_source::source_as_provider(&scan.source)
            && let Some(view) = provider.downcast_ref::<ViewTable>()
        {
            collect_plan_comparisons(view.logical_plan(), lineages, relations, out);
            return;
        }
    }
    for expr in plan.expressions() {
        collect_expr_comparisons(&expr, lineages, relations, out);
    }
    for input in plan.inputs() {
        collect_plan_comparisons(input, lineages, relations, out);
    }
}

fn collect_expr_comparisons<S: BuildHasher>(
    expr: &Expr,
    lineages: &HashMap<String, LogicalPlan, S>,
    relations: &RelTable,
    out: &mut std::collections::HashSet<(String, String, i128)>,
) {
    let _ = expr.apply(|node| {
        if let Expr::BinaryExpr(binary) = node {
            record_user_shaped(binary, relations, out);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    let _ = expr.apply(|node| {
        match node {
            Expr::Exists(exists) => {
                collect_plan_comparisons(&exists.subquery.subquery, lineages, relations, out);
            }
            Expr::InSubquery(query) => {
                collect_plan_comparisons(&query.subquery.subquery, lineages, relations, out);
            }
            Expr::ScalarSubquery(query) => {
                collect_plan_comparisons(&query.subquery, lineages, relations, out);
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

fn record_user_shaped(
    binary: &BinaryExpr,
    relations: &RelTable,
    out: &mut std::collections::HashSet<(String, String, i128)>,
) {
    match binary.op {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {}
        _ => return,
    }
    for (column_side, literal_side, flipped) in [
        (&binary.left, &binary.right, false),
        (&binary.right, &binary.left, true),
    ] {
        if let Some((column, true)) = record_column(column_side)
            && let Some(bits) = record_bare_literal_bits(literal_side)
        {
            let ordinal = resolve_column(relations, column.relation.as_ref(), &column.name);
            let user = match column_native_type(relations, ordinal, &column.name) {
                Some(native) => is_int_type(&native) && fits_int_value(bits, &native),
                None => true,
            };
            if user {
                let canonical = if flipped {
                    flip_comparison(binary.op)
                } else {
                    binary.op
                };
                out.insert((column.name.clone(), format!("{canonical:?}"), bits));
            }
        }
    }
}

fn comparison_column<'a>(expr: &'a Expr, literal_kind: &DataType) -> Option<(&'a Column, bool)> {
    match expr {
        Expr::Column(column) => Some((column, false)),
        Expr::Cast(cast)
            if cast.field.data_type() == literal_kind && is_int_type(cast.field.data_type()) =>
        {
            comparison_column(&cast.expr, literal_kind).map(|(column, _)| (column, true))
        }
        _ => None,
    }
}

fn flip_comparison(operator: Operator) -> Operator {
    match operator {
        Operator::Lt => Operator::Gt,
        Operator::Gt => Operator::Lt,
        Operator::LtEq => Operator::GtEq,
        Operator::GtEq => Operator::LtEq,
        _ => operator,
    }
}

fn comparison_key<'a, S: BuildHasher>(
    binary: &'a BinaryExpr,
    ctx: &Ctx<'_, S>,
) -> Option<(Operator, usize, &'a str, i128)> {
    match binary.op {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {}
        _ => return None,
    }
    for (column_side, literal_side, flipped) in [
        (&binary.left, &binary.right, false),
        (&binary.right, &binary.left, true),
    ] {
        if let Some((bits, kind)) = comparison_literal(literal_side)
            && let Some((column, had_cast)) = comparison_column(column_side, &kind)
        {
            let canonical = if flipped {
                flip_comparison(binary.op)
            } else {
                binary.op
            };
            if had_cast
                && ctx.user_comparisons.contains(&(
                    column.name.clone(),
                    format!("{canonical:?}"),
                    bits,
                ))
            {
                return None;
            }
            let ordinal = resolve_column(&ctx.relations, column.relation.as_ref(), &column.name);
            return Some((canonical, ordinal, column.name.as_str(), bits));
        }
    }
    None
}

fn fold_int_literal_cast(
    value: &datafusion::common::ScalarValue,
    target: &DataType,
) -> Option<datafusion::common::ScalarValue> {
    let bits = scalar_int_value(value)?;
    match target {
        DataType::Int8 => i8::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::Int8(Some(inner))),
        DataType::Int16 => i16::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::Int16(Some(inner))),
        DataType::Int32 => i32::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::Int32(Some(inner))),
        DataType::Int64 => i64::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::Int64(Some(inner))),
        DataType::UInt8 => u8::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::UInt8(Some(inner))),
        DataType::UInt16 => u16::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::UInt16(Some(inner))),
        DataType::UInt32 => u32::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::UInt32(Some(inner))),
        DataType::UInt64 => u64::try_from(bits)
            .ok()
            .map(|inner| datafusion::common::ScalarValue::UInt64(Some(inner))),
        _ => None,
    }
}

fn render_expr<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>) -> Vec<u8> {
    let mut sink = ByteSink(Vec::new());
    write_expr(expr, ctx, &mut sink);
    sink.0
}

fn write_canonical_binary<S: BuildHasher>(
    binary: &BinaryExpr,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    let left = render_expr(&binary.left, ctx);
    let right = render_expr(&binary.right, ctx);
    let (operator, swap) = match binary.op {
        Operator::Eq | Operator::NotEq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => {
            if right < left {
                (binary.op, true)
            } else {
                (binary.op, false)
            }
        }
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            if right < left {
                (flip_comparison(binary.op), true)
            } else {
                (binary.op, false)
            }
        }
        _ => {
            hash.write(b"B");
            write_debug(hash, &binary.op);
            hash.write(&left);
            hash.write(&right);
            return;
        }
    };
    hash.write(b"B");
    write_debug(hash, &operator);
    if swap {
        hash.write(&right);
        hash.write(&left);
    } else {
        hash.write(&left);
        hash.write(&right);
    }
}

fn collect_chain_operands<'a>(expr: &'a Expr, operator: Operator, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == operator
    {
        collect_chain_operands(&binary.left, operator, out);
        collect_chain_operands(&binary.right, operator, out);
    } else {
        out.push(expr);
    }
}

fn write_logic_chain<S: BuildHasher>(
    operator: Operator,
    expr: &Expr,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    let mut operands = Vec::new();
    collect_chain_operands(expr, operator, &mut operands);
    let mut rendered: Vec<Vec<u8>> = operands
        .iter()
        .map(|operand| render_expr(operand, ctx))
        .collect();
    rendered.sort();
    hash.write(if operator == Operator::And {
        b"And"
    } else {
        b"Or"
    });
    hash.write_usize(rendered.len());
    for bytes in &rendered {
        hash.write(bytes);
    }
}

pub(crate) fn write_exprs<S: BuildHasher>(
    exprs: &[Expr],
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    for expr in exprs {
        write_expr(expr, ctx, hash);
    }
}

pub(crate) fn write_opt_expr<S: BuildHasher>(
    expr: Option<&Expr>,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    match expr {
        None => hash.write_u8(0),
        Some(inner) => {
            hash.write_u8(1);
            write_expr(inner, ctx, hash);
        }
    }
}

pub(crate) fn write_sort<S: BuildHasher>(
    sort: &SortExpr,
    ctx: &Ctx<'_, S>,
    hash: &mut impl Hasher,
) {
    hash.write_u8(u8::from(sort.asc));
    hash.write_u8(u8::from(sort.nulls_first));
    write_expr(&sort.expr, ctx, hash);
}

pub(crate) fn write_expr<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>, hash: &mut impl Hasher) {
    match expr {
        Expr::Alias(alias) => {
            hash.write(b"Alias");
            write_expr(&alias.expr, ctx, hash);
        }
        Expr::Column(column) => {
            hash.write(b"C");
            write_column_ref(hash, &ctx.relations, column);
        }
        Expr::Literal(value, _) => {
            hash.write(b"L");
            write_debug(hash, value);
        }
        Expr::BinaryExpr(binary) => {
            if binary.op == Operator::And || binary.op == Operator::Or {
                write_logic_chain(binary.op, expr, ctx, hash);
            } else if let Some((operator, ordinal, name, bits)) = comparison_key(binary, ctx) {
                hash.write(b"Cmp");
                write_debug(hash, &operator);
                hash.write_usize(ordinal);
                write_norm_ident(hash, name);
                hash.write_i128(bits);
            } else {
                write_canonical_binary(binary, ctx, hash);
            }
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
    hash: &mut impl Hasher,
) {
    hash.write(tag);
    write_expr(inner, ctx, hash);
}

fn write_expr_compound<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>, hash: &mut impl Hasher) {
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
            if let Expr::Literal(value, _) = cast.expr.as_ref()
                && let Some(folded) = fold_int_literal_cast(value, cast.field.data_type())
            {
                hash.write(b"L");
                write_debug(hash, &folded);
            } else {
                hash.write(b"Cast");
                write_expr(&cast.expr, ctx, hash);
                write_data_type(cast.field.data_type(), hash);
            }
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

fn write_expr_function<S: BuildHasher>(expr: &Expr, ctx: &Ctx<'_, S>, hash: &mut impl Hasher) {
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
            write_column_ref(hash, &ctx.relations, column);
        }
        other => {
            hash.write(b"OpaqueExpr");
            write_debug(hash, other);
        }
    }
}

fn write_data_type(kind: &DataType, hash: &mut impl Hasher) {
    write_debug(hash, kind);
}
