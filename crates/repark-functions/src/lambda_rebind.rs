use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ExprSchema, JoinType, ScalarValue, exec_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::decimal_cast::SPARK_NONNULL_NAME;
use crate::spark_result_types::{
    narrow_provisional_integer_literal, narrow_provisional_integer_literals,
};

const HOF_ARRAY_FIELD_NAME: &str = "__repark_hof_array_field__";

#[expect(
    clippy::missing_errors_doc,
    reason = "The error contract is documented in map.md under the owner comment ban."
)]
pub fn analyzer_rules_with_higher_order_preparation(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return plan_err!(
            "Spark higher-order preparation requires the default type_coercion analyzer rule"
        );
    };
    rules.insert(position, Arc::new(HigherOrderPreparation));
    Ok(rules)
}

#[derive(Debug, Default)]
pub struct HigherOrderPreparation;

impl AnalyzerRule for HigherOrderPreparation {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(prepare_plan).data()
    }

    fn name(&self) -> &'static str {
        "higher_order_preparation"
    }
}

#[derive(Debug, Default)]
pub struct LambdaRebind;

impl AnalyzerRule for LambdaRebind {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let packed = plan
            .map_expressions(|expr| {
                refuse_lambda_arity(&expr)?;
                pack_unreferenced_params(expr)
            })
            .data()?;
        let resolved = packed.resolve_lambda_variables().data()?;
        let schema = resolved.schema().clone();
        resolved
            .map_expressions(|expr| {
                check_aggregate_merge(&expr, schema.as_ref())?;
                Ok(Transformed::no(expr))
            })
            .data()
    }

    fn name(&self) -> &'static str {
        "lambda_rebind"
    }
}

fn prepare_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let inputs: Vec<LogicalPlan> = plan.inputs().into_iter().cloned().collect();
    let mut schema = DFSchema::empty();
    for input in &inputs {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let prepared = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let transformed = expr.transform_down(|node| prepare_expr(node, &schema, &inputs))?;
        Ok(transformed.update_data(|node| saved_name.restore(node)))
    })?;
    if !prepared.transformed {
        return Ok(prepared);
    }
    let prepared = prepared.map_data(LogicalPlan::recompute_schema)?;
    let resolved = prepared.data.resolve_lambda_variables()?;
    Ok(Transformed::yes(resolved.data))
}

fn prepare_expr(
    expr: Expr,
    schema: &DFSchema,
    inputs: &[LogicalPlan],
) -> Result<Transformed<Expr>> {
    let Expr::HigherOrderFunction(mut hof) = expr else {
        return Ok(Transformed::no(expr));
    };
    let aggregate = hof.func.name() == "aggregate";
    let defer_aggregate_numeric =
        aggregate && should_defer_aggregate_numeric_preparation(&hof, inputs);
    let mut changed = false;
    let value_count = hof
        .args
        .iter()
        .take_while(|arg| !matches!(arg, Expr::Lambda(_)))
        .count();
    let traced_provisional = !aggregate
        && (0..value_count).any(|position| {
            hof.args
                .get(position)
                .is_some_and(|arg| traces_to_provisional_constructor(arg, inputs))
        });
    let narrow_numeric = if aggregate {
        !defer_aggregate_numeric
    } else {
        !traced_provisional
    };
    for position in 0..value_count {
        if aggregate && position == 1 {
            continue;
        }
        if narrow_numeric
            && let Some(value) = hof.args.get_mut(position)
            && (is_array_constructor(value) || is_map_constructor(value))
        {
            let narrowed = narrow_provisional_integer_literals(value.clone())?;
            changed |= narrowed.transformed;
            *value = narrowed.data;
        }
        let Some(element_nullable) = hof
            .args
            .get(position)
            .and_then(|arg| array_constructor_element_nullable(arg, schema, inputs))
        else {
            continue;
        };
        if let Some(value) = hof.args.get_mut(position) {
            if is_array_constructor(value) {
                let wrapped = wrap_nested_constructors(value.clone(), schema)?;
                *value = wrapped.data;
            } else {
                *value = array_field_call(value.clone(), element_nullable);
            }
            changed = true;
        }
    }
    if aggregate
        && !defer_aggregate_numeric
        && let Some(initial) = hof.args.get_mut(1)
    {
        let narrowed = narrow_provisional_integer_literal(initial.clone());
        changed |= narrowed.transformed;
        *initial = narrowed.data;
    }
    if narrow_numeric {
        for arg in &mut hof.args {
            let Expr::Lambda(lambda) = arg else {
                continue;
            };
            let narrowed =
                narrow_provisional_integer_literals(std::mem::take(lambda.body.as_mut()))?;
            changed |= narrowed.transformed;
            *lambda.body = narrowed.data;
        }
    }
    Ok(Transformed::new(
        Expr::HigherOrderFunction(HigherOrderFunction::new(hof.func, hof.args)),
        changed,
        TreeNodeRecursion::Stop,
    ))
}

fn should_defer_aggregate_numeric_preparation(
    hof: &HigherOrderFunction,
    inputs: &[LogicalPlan],
) -> bool {
    let Some(Expr::Column(column)) = hof.args.first() else {
        return false;
    };
    if !matches!(
        hof.args.get(1),
        Some(Expr::Literal(ScalarValue::Int64(_), _))
    ) {
        return false;
    }
    inputs.iter().any(|input| {
        let Some(index) = input.schema().index_of_column(column).ok() else {
            return false;
        };
        let Some((source, source_schema)) = output_expression(input, index) else {
            return false;
        };
        if has_nonnull_wrapper(source) || !is_array_constructor(source) {
            return false;
        }
        let Some(field) = source.to_field(source_schema).ok().map(|(_, field)| field) else {
            return false;
        };
        matches!(
            field.data_type(),
            DataType::List(element) | DataType::LargeList(element)
                if element.data_type() == &DataType::Int64
        )
    })
}

fn array_constructor_element_nullable(
    expr: &Expr,
    schema: &DFSchema,
    inputs: &[LogicalPlan],
) -> Option<bool> {
    if is_array_constructor(expr) {
        return constructor_element_nullable(expr, schema);
    }
    if let Expr::ScalarSubquery(query) = expr {
        return source_element_nullable(query.subquery.as_ref(), 0);
    }
    let Expr::Column(column) = expr else {
        return None;
    };
    inputs.iter().find_map(|input| {
        let index = input.schema().index_of_column(column).ok()?;
        source_element_nullable(input, index)
    })
}

fn source_element_nullable(plan: &LogicalPlan, index: usize) -> Option<bool> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let mut source = projection.expr.get(index)?;
            while let Expr::Alias(alias) = source {
                source = alias.expr.as_ref();
            }
            if let Expr::Column(inner) = source {
                let inner_index = projection.input.schema().index_of_column(inner).ok()?;
                source_element_nullable(projection.input.as_ref(), inner_index)
            } else if is_array_constructor(source) {
                constructor_element_nullable(source, projection.input.schema().as_ref())
            } else {
                None
            }
        }
        LogicalPlan::SubqueryAlias(alias) => source_element_nullable(alias.input.as_ref(), index),
        LogicalPlan::Subquery(subquery) => {
            source_element_nullable(subquery.subquery.as_ref(), index)
        }
        LogicalPlan::Filter(filter) => source_element_nullable(filter.input.as_ref(), index),
        LogicalPlan::Sort(sort) => source_element_nullable(sort.input.as_ref(), index),
        LogicalPlan::Distinct(distinct) => {
            source_element_nullable(distinct.input().as_ref(), index)
        }
        LogicalPlan::Limit(limit) => source_element_nullable(limit.input.as_ref(), index),
        LogicalPlan::Repartition(repartition) => {
            source_element_nullable(repartition.input.as_ref(), index)
        }
        LogicalPlan::Join(join) => {
            let left_width = join.left.schema().fields().len();
            let padded = match join.join_type {
                JoinType::Left => index >= left_width,
                JoinType::Right => index < left_width,
                JoinType::Full => true,
                _ => false,
            };
            if padded {
                return None;
            }
            let (input, position) = join_lineage(
                join.left.as_ref(),
                join.right.as_ref(),
                join.join_type,
                index,
            )?;
            source_element_nullable(input, position)
        }
        LogicalPlan::Union(union) => {
            if union.inputs.is_empty() {
                return None;
            }
            for input in &union.inputs {
                if source_element_nullable(input.as_ref(), index) != Some(false) {
                    return None;
                }
            }
            Some(false)
        }
        LogicalPlan::Aggregate(aggregate) => {
            let mut source = aggregate.group_expr.get(index)?;
            while let Expr::Alias(alias) = source {
                source = alias.expr.as_ref();
            }
            if let Expr::Column(inner) = source {
                let inner_index = aggregate.input.schema().index_of_column(inner).ok()?;
                source_element_nullable(aggregate.input.as_ref(), inner_index)
            } else if is_array_constructor(source) {
                constructor_element_nullable(source, aggregate.input.schema().as_ref())
            } else {
                None
            }
        }
        LogicalPlan::Window(window) => {
            if index < window.input.schema().fields().len() {
                source_element_nullable(window.input.as_ref(), index)
            } else {
                None
            }
        }
        LogicalPlan::Values(values) => {
            if values.values.is_empty() {
                return None;
            }
            let mut nullable = false;
            for row in &values.values {
                match row.get(index) {
                    Some(cell) if is_array_constructor(cell) => {
                        nullable |= constructor_element_nullable(cell, values.schema.as_ref())?;
                    }
                    _ => return None,
                }
            }
            Some(nullable)
        }
        _ => None,
    }
}

fn join_lineage<'a>(
    left: &'a LogicalPlan,
    right: &'a LogicalPlan,
    join_type: JoinType,
    position: usize,
) -> Option<(&'a LogicalPlan, usize)> {
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (position < left.schema().fields().len()).then_some((left, position))
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (position < right.schema().fields().len()).then_some((right, position))
        }
        _ => {
            let left_width = left.schema().fields().len();
            if position < left_width {
                Some((left, position))
            } else {
                Some((right, position - left_width))
            }
        }
    }
}

fn output_expression(mut plan: &LogicalPlan, index: usize) -> Option<(&Expr, &DFSchema)> {
    loop {
        match plan {
            LogicalPlan::Projection(projection) => {
                return projection
                    .expr
                    .get(index)
                    .map(|expr| (expr, projection.input.schema().as_ref()));
            }
            LogicalPlan::SubqueryAlias(alias) => plan = alias.input.as_ref(),
            LogicalPlan::Filter(filter) => plan = filter.input.as_ref(),
            LogicalPlan::Sort(sort) => plan = sort.input.as_ref(),
            _ => return None,
        }
    }
}

fn has_nonnull_wrapper(mut expr: &Expr) -> bool {
    loop {
        match expr {
            Expr::Alias(alias) => expr = alias.expr.as_ref(),
            Expr::ScalarFunction(function) => {
                return function.func.name() == SPARK_NONNULL_NAME && function.args.len() == 1;
            }
            _ => return false,
        }
    }
}

fn unwrap_nonnull(mut expr: &Expr) -> &Expr {
    loop {
        match expr {
            Expr::Alias(alias) => expr = alias.expr.as_ref(),
            Expr::ScalarFunction(function)
                if function.func.name() == SPARK_NONNULL_NAME && function.args.len() == 1 =>
            {
                expr = &function.args[0];
            }
            _ => return expr,
        }
    }
}

fn is_array_constructor(expr: &Expr) -> bool {
    matches!(
        unwrap_nonnull(expr),
        Expr::ScalarFunction(function) if is_array_constructor_name(function.func.name())
    )
}

fn is_array_constructor_name(name: &str) -> bool {
    matches!(name, "array" | "make_array")
}

fn is_map_constructor(expr: &Expr) -> bool {
    matches!(
        unwrap_nonnull(expr),
        Expr::ScalarFunction(function) if function.func.name() == "map"
    )
}

fn contains_bare_integer(expr: &Expr) -> bool {
    let mut found = false;
    expr.apply(|node| match node {
        Expr::Literal(ScalarValue::Int64(_), _) => {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        }
        Expr::Cast(_) => Ok(TreeNodeRecursion::Jump),
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .is_ok()
        && found
}

fn traces_to_provisional_constructor(expr: &Expr, inputs: &[LogicalPlan]) -> bool {
    let mut provisional = false;
    expr.apply(|node| match node {
        Expr::Column(column) => {
            if inputs.iter().any(|input| {
                input
                    .schema()
                    .index_of_column(column)
                    .ok()
                    .is_some_and(|index| source_feeds_bare_integer(input, index))
            }) {
                provisional = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        }
        Expr::ScalarSubquery(query) => {
            if source_feeds_bare_integer(query.subquery.as_ref(), 0) {
                provisional = true;
            }
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .is_ok()
        && provisional
}

fn source_feeds_bare_integer(plan: &LogicalPlan, index: usize) -> bool {
    let mut pending = vec![(plan, index)];
    while let Some((current, position)) = pending.pop() {
        match current {
            LogicalPlan::Projection(projection) => {
                let Some(mut source) = projection.expr.get(position) else {
                    continue;
                };
                while let Expr::Alias(alias) = source {
                    source = alias.expr.as_ref();
                }
                if let Expr::Column(inner) = source {
                    if let Ok(inner_index) = projection.input.schema().index_of_column(inner) {
                        pending.push((projection.input.as_ref(), inner_index));
                    }
                } else if (is_array_constructor(source) || is_map_constructor(source))
                    && contains_bare_integer(source)
                {
                    return true;
                }
            }
            LogicalPlan::SubqueryAlias(alias) => pending.push((alias.input.as_ref(), position)),
            LogicalPlan::Subquery(subquery) => {
                pending.push((subquery.subquery.as_ref(), position));
            }
            LogicalPlan::Filter(filter) => pending.push((filter.input.as_ref(), position)),
            LogicalPlan::Sort(sort) => pending.push((sort.input.as_ref(), position)),
            LogicalPlan::Distinct(distinct) => pending.push((distinct.input(), position)),
            LogicalPlan::Limit(limit) => pending.push((limit.input.as_ref(), position)),
            LogicalPlan::Repartition(repartition) => {
                pending.push((repartition.input.as_ref(), position));
            }
            LogicalPlan::Join(join) => {
                if let Some((input, index)) = join_lineage(
                    join.left.as_ref(),
                    join.right.as_ref(),
                    join.join_type,
                    position,
                ) {
                    pending.push((input, index));
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                if let Some(group) = aggregate.group_expr.get(position) {
                    let mut source = group;
                    while let Expr::Alias(alias) = source {
                        source = alias.expr.as_ref();
                    }
                    if let Expr::Column(inner) = source {
                        if let Ok(inner_index) = aggregate.input.schema().index_of_column(inner) {
                            pending.push((aggregate.input.as_ref(), inner_index));
                        }
                    } else if (is_array_constructor(source) || is_map_constructor(source))
                        && contains_bare_integer(source)
                    {
                        return true;
                    }
                }
            }
            LogicalPlan::Window(window) => {
                if position < window.input.schema().fields().len() {
                    pending.push((window.input.as_ref(), position));
                }
            }
            LogicalPlan::Unnest(unnest) => {
                if let Some(dependency) = unnest.dependency_indices.get(position) {
                    pending.push((unnest.input.as_ref(), *dependency));
                }
            }
            LogicalPlan::RecursiveQuery(recursive) => {
                pending.push((recursive.static_term.as_ref(), position));
            }
            LogicalPlan::Values(values) => {
                if values
                    .values
                    .iter()
                    .any(|row| row.get(position).is_some_and(contains_bare_integer))
                {
                    return true;
                }
            }
            LogicalPlan::Union(union) => {
                pending.extend(union.inputs.iter().map(|input| (input.as_ref(), position)));
            }
            _ => {}
        }
    }
    false
}

fn constructor_element_nullable(expr: &Expr, schema: &DFSchema) -> Option<bool> {
    let Expr::ScalarFunction(function) = unwrap_nonnull(expr) else {
        return None;
    };
    for arg in &function.args {
        if arg.to_field(schema).ok()?.1.is_nullable() {
            return Some(true);
        }
    }
    Some(false)
}

fn array_field_call(expr: Expr, element_nullable: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(HofArrayField::new(element_nullable))),
        vec![expr],
    ))
}

fn wrap_nested_constructors(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    expr.transform_up(|node| {
        if !is_array_constructor(&node) {
            return Ok(Transformed::no(node));
        }
        let Some(nullable) = constructor_element_nullable(&node, schema) else {
            return Ok(Transformed::no(node));
        };
        Ok(Transformed::yes(array_field_call(node, nullable)))
    })
}

#[derive(Debug)]
struct HofArrayField {
    signature: Signature,
    element_nullable: bool,
}

impl HofArrayField {
    fn new(element_nullable: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            element_nullable,
        }
    }
}

impl PartialEq for HofArrayField {
    fn eq(&self, other: &Self) -> bool {
        self.element_nullable == other.element_nullable
    }
}

impl Eq for HofArrayField {}

impl Hash for HofArrayField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.element_nullable.hash(state);
    }
}

impl ScalarUDFImpl for HofArrayField {
    crate::shim_udf_boilerplate!("__repark_hof_array_field__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(first) = arg_types.first() else {
            return plan_err!("'{HOF_ARRAY_FIELD_NAME}' expects one argument");
        };
        array_type_with_element_nullability(first, self.element_nullable)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(first) = args.arg_fields.first() else {
            return plan_err!("'{HOF_ARRAY_FIELD_NAME}' expects one argument");
        };
        Ok(Arc::new(Field::new(
            self.name(),
            array_type_with_element_nullability(first.data_type(), self.element_nullable)?,
            false,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return exec_err!("'{HOF_ARRAY_FIELD_NAME}' expects one argument");
        };
        let array = first.to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(cast(&array, args.return_type())?))
    }
}

fn array_type_with_element_nullability(data_type: &DataType, nullable: bool) -> Result<DataType> {
    match data_type {
        DataType::List(field) => Ok(DataType::List(Arc::new(
            field.as_ref().clone().with_nullable(nullable),
        ))),
        DataType::LargeList(field) => Ok(DataType::LargeList(Arc::new(
            field.as_ref().clone().with_nullable(nullable),
        ))),
        other => plan_err!("'{HOF_ARRAY_FIELD_NAME}' expected ARRAY, got {other}"),
    }
}

fn refuse_lambda_arity(expr: &Expr) -> Result<()> {
    expr.apply(|node| match node {
        Expr::HigherOrderFunction(hof) => {
            check_lambda_params(hof)?;
            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(())
}

fn check_lambda_params(hof: &HigherOrderFunction) -> Result<()> {
    let mut lambdas = hof.args.iter().filter_map(|arg| match arg {
        Expr::Lambda(lambda) => Some(lambda.params.len()),
        _ => None,
    });
    match hof.func.name() {
        "aggregate" => {
            if let Some(merge) = lambdas.next()
                && merge < 2
            {
                return arity_mismatch(merge, 2);
            }
        }
        "zip_with" | "transform_keys" | "transform_values" | "map_filter" => {
            if let Some(only) = lambdas.next()
                && lambdas.next().is_none()
                && only < 2
            {
                return arity_mismatch(only, 2);
            }
        }
        "map_zip_with" => {
            if let Some(only) = lambdas.next()
                && lambdas.next().is_none()
                && only < 3
            {
                return arity_mismatch(only, 3);
            }
        }
        _ => {}
    }
    Ok(())
}

fn arity_mismatch(user: usize, expected: usize) -> Result<()> {
    plan_err!(
        "[INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH] Invalid lambda function call. A \
         higher order function expects {user} arguments, but got {expected}."
    )
}

fn check_aggregate_merge(expr: &Expr, schema: &dyn ExprSchema) -> Result<()> {
    expr.apply(|node| match node {
        Expr::HigherOrderFunction(hof) if hof.func.name() == "aggregate" => {
            check_merge_types(hof, schema)?;
            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(())
}

fn check_merge_types(hof: &HigherOrderFunction, schema: &dyn ExprSchema) -> Result<()> {
    let mut lambdas = hof.args.iter().filter_map(|arg| match arg {
        Expr::Lambda(lambda) => Some(lambda),
        _ => None,
    });
    let Some(merge) = lambdas.next() else {
        return Ok(());
    };
    let Some(initial) = hof
        .args
        .get(1)
        .filter(|arg| !matches!(arg, Expr::Lambda(_)))
    else {
        return Ok(());
    };
    let Ok((_, initial_field)) = initial.to_field(schema) else {
        return Ok(());
    };
    let Ok((_, merge_field)) = merge.body.to_field(schema) else {
        return Ok(());
    };
    if initial_field.data_type() == merge_field.data_type() {
        return Ok(());
    }
    plan_err!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"aggregate\" due to data type \
         mismatch: The third parameter requires the \"{}\" type, however the merge lambda has \
         the type \"{}\".",
        spark_type_name(initial_field.data_type()),
        spark_type_name(merge_field.data_type())
    )
}

fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => String::from("VOID"),
        DataType::Boolean => String::from("BOOLEAN"),
        DataType::Int8 => String::from("TINYINT"),
        DataType::Int16 => String::from("SMALLINT"),
        DataType::Int32 => String::from("INT"),
        DataType::Int64 => String::from("BIGINT"),
        DataType::Float32 => String::from("FLOAT"),
        DataType::Float64 => String::from("DOUBLE"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => String::from("STRING"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => String::from("BINARY"),
        DataType::Date32 => String::from("DATE"),
        DataType::Timestamp(_, _) => String::from("TIMESTAMP"),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(entries) if entries.len() == 2 => format!(
                "MAP<{},{}>",
                spark_type_name(entries[0].data_type()),
                spark_type_name(entries[1].data_type())
            ),
            _ => String::from("MAP"),
        },
        DataType::Struct(fields) => {
            let rendered: Vec<String> = fields
                .iter()
                .map(|field| format!("{}:{}", field.name(), spark_type_name(field.data_type())))
                .collect();
            format!("STRUCT<{}>", rendered.join(","))
        }
        DataType::Dictionary(_, values) => spark_type_name(values),
        other => other.to_string(),
    }
}

fn pack_unreferenced_params(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_down(|node| match node {
        Expr::HigherOrderFunction(hof) => pack_hof(hof),
        _ => Ok(Transformed::no(node)),
    })
}

fn pack_hof(hof: HigherOrderFunction) -> Result<Transformed<Expr>> {
    let mut changed = false;
    let mut args = Vec::with_capacity(hof.args.len());
    for arg in hof.args {
        match arg {
            Expr::Lambda(lambda) if lambda.params.len() >= 2 => {
                let referenced = referenced_params(&lambda.body)?;
                if lambda.params.iter().all(|name| referenced.contains(name)) {
                    args.push(Expr::Lambda(lambda));
                } else {
                    let kept =
                        crate::higher_order::hof_keep::keep_call(*lambda.body, &lambda.params);
                    args.push(Expr::Lambda(Lambda::new(lambda.params, kept)));
                    changed = true;
                }
            }
            _ => args.push(arg),
        }
    }
    Ok(Transformed::new(
        Expr::HigherOrderFunction(HigherOrderFunction::new(hof.func, args)),
        changed,
        TreeNodeRecursion::Continue,
    ))
}

fn referenced_params(body: &Expr) -> Result<HashSet<String>> {
    let mut names = HashSet::new();
    body.apply(|node| match node {
        Expr::LambdaVariable(var) => {
            names.insert(var.name.clone());
            Ok(TreeNodeRecursion::Continue)
        }
        Expr::Lambda(_) => Ok(TreeNodeRecursion::Jump),
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(names)
}
