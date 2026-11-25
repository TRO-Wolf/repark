use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::sql::sqlparser::ast::{ObjectName, Query, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use repark_core::CatalogRegistry;

const WRITER_VIEW: &str = "_repark_writer_";
const WRITER_V2_VIEW: &str = "_repark_writer_v2_";
const PROBE: &str = "__repark_const_cast_probe";
const MAX_DEPTH: usize = 64;
const CAST_FUNCTIONS: [&str; 4] = [
    "__repark_parse_java_double__",
    "__repark_parse_java_float__",
    "to_timestamp",
    repark_common::udf_names::TIMESTAMP_NTZ_CAST_NAME,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Door {
    Overwrite,
    SaveAsTable,
    OverwriteWhere,
}

#[derive(Clone, Debug)]
enum Value {
    Text(String),
    Cast(String, Expr, DataType),
    Other,
}

pub(crate) async fn refuse_constant_cast(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source_sql: &str,
    door: Door,
) -> Result<()> {
    if !repark_functions::ansi::spark_ansi_enabled_from_options(ctx.state().config().options())
        || !reads_only_literals(source_sql, door)
    {
        return Ok(());
    }
    let Ok(frame) = crate::spark_ast::execute_passthrough(ctx, catalogs, source_sql).await else {
        return Ok(());
    };
    for (text, cast, target) in constant_casts(frame.logical_plan()) {
        if !spark_cast_raises(&text, &target) {
            continue;
        }
        if let Err(error) = run_cast(ctx, &text, cast).await
            && error.to_string().contains("[CAST_INVALID_INPUT]")
        {
            return Err(error);
        }
    }
    Ok(())
}

async fn run_cast(ctx: &SessionContext, text: &str, cast: Expr) -> Result<()> {
    let plan = LogicalPlanBuilder::empty(true)
        .project(vec![lit(text).alias(PROBE)])?
        .project(vec![cast])?
        .build()?;
    ctx.execute_logical_plan(plan).await?.collect().await?;
    Ok(())
}

pub(super) fn spark_cast_raises(text: &str, target: &DataType) -> bool {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return false;
    }
    match target {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp(TimeUnit::Microsecond, _) => true,
        DataType::Float32 | DataType::Float64 => !floating_word(text),
        _ => false,
    }
}

fn floating_word(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let word = lower.strip_suffix(['d', 'f']).unwrap_or(&lower);
    ["nan", "inf", "infinity"]
        .iter()
        .any(|special| lower == *special || word == *special)
}

#[derive(Default)]
struct Relations {
    ctes: HashSet<String>,
    names: Vec<String>,
}

impl Visitor for Relations {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        let leaf = relation
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map_or_else(|| relation.to_string(), |ident| ident.value.clone());
        self.names.push(leaf.to_ascii_lowercase());
        ControlFlow::Continue(())
    }
}

pub(super) fn reads_only_literals(source_sql: &str, door: Door) -> bool {
    let Ok(query) = Parser::new(&GenericDialect {})
        .try_with_sql(source_sql)
        .and_then(|mut parser| parser.parse_query())
    else {
        return false;
    };
    let mut seen = Relations::default();
    let _ = query.visit(&mut seen);
    let frame = |name: &String| match door {
        Door::Overwrite => name.starts_with(WRITER_VIEW),
        Door::SaveAsTable => name.starts_with(WRITER_VIEW) && !name.starts_with(WRITER_V2_VIEW),
        Door::OverwriteWhere => name.starts_with(WRITER_V2_VIEW),
    };
    let allowed = seen
        .names
        .iter()
        .all(|name| (door == Door::Overwrite && seen.ctes.contains(name)) || frame(name));
    allowed && (door == Door::Overwrite || !seen.names.is_empty())
}

pub(super) fn constant_casts(plan: &LogicalPlan) -> Vec<(String, Expr, DataType)> {
    values(plan, 0)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| match value {
            Value::Cast(text, cast, target) => Some((text, cast, target)),
            Value::Text(_) | Value::Other => None,
        })
        .collect()
}

fn values(plan: &LogicalPlan, depth: usize) -> Option<Vec<Value>> {
    if depth > MAX_DEPTH {
        return None;
    }
    match plan {
        LogicalPlan::EmptyRelation(empty) if empty.produce_one_row => Some(Vec::new()),
        LogicalPlan::Filter(filter) if !has_subquery(&filter.predicate) => {
            values(&filter.input, depth + 1)
        }
        LogicalPlan::SubqueryAlias(alias) => values(&alias.input, depth + 1),
        LogicalPlan::Projection(projection) => {
            let input = values(&projection.input, depth + 1)?;
            let schema = projection.input.schema();
            projection
                .expr
                .iter()
                .map(|expr| value_of(expr, &input, schema))
                .collect()
        }
        _ => None,
    }
}

fn has_subquery(expr: &Expr) -> bool {
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)
        ))
    })
    .unwrap_or(true)
}

fn unaliased(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

fn value_of(expr: &Expr, input: &[Value], schema: &DFSchema) -> Option<Value> {
    if has_subquery(expr) {
        return None;
    }
    let inner = unaliased(expr);
    if let Some(text) = text_of(inner, input, schema) {
        return Some(Value::Text(text));
    }
    if let Expr::Column(column) = inner {
        let index = schema.index_of_column(column).ok()?;
        return Some(input.get(index).cloned().unwrap_or(Value::Other));
    }
    let Some(text) = cast_operand(inner).and_then(|operand| text_of(operand, input, schema)) else {
        return Some(Value::Other);
    };
    let Ok(target) = inner.get_type(schema) else {
        return Some(Value::Other);
    };
    Some(match with_probe_operand(inner) {
        Some(cast) => Value::Cast(text, cast, target),
        None => Value::Other,
    })
}

fn text_of(expr: &Expr, input: &[Value], schema: &DFSchema) -> Option<String> {
    match unaliased(expr) {
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => Some(text.clone()),
        Expr::Column(column) => match input.get(schema.index_of_column(column).ok()?) {
            Some(Value::Text(text)) => Some(text.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn cast_operand(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Cast(cast) => Some(cast.expr.as_ref()),
        Expr::ScalarFunction(function)
            if CAST_FUNCTIONS.contains(&function.func.name())
                || repark_functions::cast_map::string_integral_cast_parts(expr).is_some() =>
        {
            match function.args.as_slice() {
                [operand] => Some(operand),
                _ => None,
            }
        }
        _ => None,
    }
}

fn with_probe_operand(expr: &Expr) -> Option<Expr> {
    match expr {
        Expr::Cast(cast) => {
            let mut probe = cast.clone();
            probe.expr = Box::new(col(PROBE));
            Some(Expr::Cast(probe))
        }
        Expr::ScalarFunction(function) => {
            let mut probe = function.clone();
            probe.args = vec![col(PROBE)];
            Some(Expr::ScalarFunction(probe))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::expr_fn::exists;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, cast, table_scan, try_cast};
    use datafusion::prelude::{col, lit};

    use super::{Door, constant_casts, reads_only_literals, spark_cast_raises};

    fn ltz() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    fn raising_targets() -> Vec<DataType> {
        vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            ltz(),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ]
    }

    fn one_row(text: &str) -> LogicalPlanBuilder {
        LogicalPlanBuilder::empty(true)
            .project(vec![lit(text).alias("s")])
            .expect("literal projection")
            .alias("src")
            .expect("alias")
    }

    fn texts(plan: &LogicalPlan) -> Vec<(String, DataType)> {
        constant_casts(plan)
            .into_iter()
            .map(|(text, _, target)| (text, target))
            .collect()
    }

    #[test]
    fn letters_only_strings_raise_for_every_table_type() {
        for target in raising_targets() {
            for text in [
                "x", "abc", "Z", "nana", "infinite", "d", "f", "e", "T", "now",
            ] {
                assert!(spark_cast_raises(text, &target), "{text} -> {target}");
            }
        }
    }

    #[test]
    fn floating_special_words_stay_out_of_the_table() {
        for text in [
            "nan",
            "NaN",
            "NAN",
            "inf",
            "INF",
            "Infinity",
            "iNfInItY",
            "nand",
            "NaNf",
            "infd",
            "Infinityd",
            "infinityf",
        ] {
            assert!(!spark_cast_raises(text, &DataType::Float64), "{text}");
            assert!(!spark_cast_raises(text, &DataType::Float32), "{text}");
            assert!(spark_cast_raises(text, &DataType::Int32), "{text}");
        }
    }

    #[test]
    fn values_outside_letters_only_stay_out_of_the_table() {
        for target in raising_targets() {
            for text in [
                "", " ", " x", "x ", "1", "1x", "x1", "x-y", "x.y", "\u{e9}", "\u{ff58}", "x\t",
            ] {
                assert!(!spark_cast_raises(text, &target), "{text:?} -> {target}");
            }
        }
    }

    #[test]
    fn other_target_types_stay_out_of_the_table() {
        for target in [
            DataType::Date32,
            DataType::Decimal128(10, 2),
            DataType::Boolean,
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            assert!(!spark_cast_raises("x", &target), "{target}");
        }
    }

    #[test]
    fn the_card_shape_yields_its_cast() {
        let plan = one_row("x")
            .filter(lit(1).eq(lit(0)))
            .and_then(|builder| builder.project(vec![cast(col("src.s"), DataType::Float64)]))
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        assert_eq!(texts(&plan), vec![("x".to_string(), DataType::Float64)]);
        let direct = LogicalPlanBuilder::empty(true)
            .filter(lit(false))
            .and_then(|builder| builder.project(vec![cast(lit("abc"), ltz()).alias("y")]))
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        assert_eq!(texts(&direct), vec![("abc".to_string(), ltz())]);
    }

    #[test]
    fn a_cast_carries_through_projections_and_aliases() {
        let plan = one_row("x")
            .project(vec![
                cast(col("src.s"), DataType::Int32).alias("d"),
                col("src.s"),
            ])
            .and_then(|builder| builder.alias("b"))
            .and_then(|builder| builder.filter(lit(false)))
            .and_then(|builder| builder.project(vec![col("b.d").alias("z")]))
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        assert_eq!(texts(&plan), vec![("x".to_string(), DataType::Int32)]);
        let unprojected = one_row("x")
            .project(vec![
                cast(col("src.s"), DataType::Int32).alias("d"),
                col("src.s"),
            ])
            .and_then(|builder| builder.project(vec![col("s")]))
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        assert!(texts(&unprojected).is_empty());
    }

    #[test]
    fn shapes_outside_the_chain_yield_nothing() {
        let float = || cast(col("s"), DataType::Float64);
        let limit = one_row("x")
            .limit(0, Some(0))
            .and_then(|b| b.project(vec![float()]));
        let union = one_row("x")
            .union(one_row("y").build().expect("branch"))
            .and_then(|b| b.project(vec![float()]));
        let aggregate = one_row("x")
            .aggregate(vec![col("s")], Vec::<datafusion::prelude::Expr>::new())
            .and_then(|b| b.project(vec![float()]));
        let exists_filter = one_row("x")
            .filter(exists(Arc::new(
                LogicalPlanBuilder::empty(true).build().expect("sub"),
            )))
            .and_then(|b| b.project(vec![float()]));
        let nested = one_row("x").project(vec![float() + lit(1.0)]);
        let try_cast_column = one_row("x").project(vec![try_cast(col("s"), DataType::Float64)]);
        let values_column = LogicalPlanBuilder::values(vec![vec![lit("x")]])
            .and_then(|b| b.project(vec![cast(col("column1"), DataType::Float64)]));
        let values_literal = LogicalPlanBuilder::values(vec![vec![lit(1)]])
            .and_then(|b| b.project(vec![cast(lit("x"), DataType::Float64)]));
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let scan = table_scan(Some("t"), &schema, None)
            .and_then(|b| b.project(vec![cast(col("t.s"), DataType::Float64)]));
        for (name, builder) in [
            ("limit", limit),
            ("union", union),
            ("aggregate", aggregate),
            ("exists", exists_filter),
            ("nested", nested),
            ("try_cast", try_cast_column),
            ("values_column", values_column),
            ("values_literal", values_literal),
            ("scan", scan),
        ] {
            let plan = builder.and_then(LogicalPlanBuilder::build).expect(name);
            assert!(texts(&plan).is_empty(), "{name}");
        }
    }

    #[test]
    fn named_relations_put_a_source_outside_the_guard() {
        let inside = [
            "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            "WITH c AS (SELECT 'x' AS s) SELECT CAST(s AS DOUBLE) FROM c WHERE 1 = 0",
            "SELECT CAST('x' AS TIMESTAMP_NTZ) WHERE 1 = 0",
            "SELECT * FROM `datafusion`.`public`.`_repark_writer_ab12`",
        ];
        for sql in inside {
            assert!(reads_only_literals(sql, Door::Overwrite), "{sql}");
        }
        for sql in [
            "SELECT CAST(s AS DOUBLE) FROM t WHERE 1 = 0",
            "SELECT CAST(s AS DOUBLE) FROM (SELECT s FROM ice.ns.t) q",
            "SELECT CAST(s AS DOUBLE) FROM tv_x",
            "not a query",
        ] {
            assert!(!reads_only_literals(sql, Door::Overwrite), "{sql}");
        }
        let v1 = "SELECT * FROM `datafusion`.`public`.`_repark_writer_ab12`";
        let v2 = "SELECT `x` FROM `datafusion`.`public`.`_repark_writer_v2_ab12`";
        assert!(reads_only_literals(v1, Door::SaveAsTable));
        assert!(!reads_only_literals(v2, Door::SaveAsTable));
        assert!(reads_only_literals(v2, Door::OverwriteWhere));
        assert!(!reads_only_literals(v1, Door::OverwriteWhere));
        assert!(!reads_only_literals(inside[0], Door::SaveAsTable));
        assert!(!reads_only_literals(inside[1], Door::OverwriteWhere));
    }
}
