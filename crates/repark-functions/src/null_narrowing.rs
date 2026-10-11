use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, ScalarFunction};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableType, Union, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::{TypeCoercionRewriter, coerce_union_schema};

pub const NARROWED_BESIDE_NULL_NAME: &str = "__repark_narrowed_beside_null__";
pub const NARROWED_BESIDE_VALUE_NAME: &str = "__repark_narrowed_beside_value__";
const UNTYPED_NULL_TAG: &str = "repark.untyped_null";

static NARROWED_BESIDE_NULL: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NarrowedBeside::new(
        NARROWED_BESIDE_NULL_NAME,
    )))
});
static NARROWED_BESIDE_VALUE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NarrowedBeside::new(
        NARROWED_BESIDE_VALUE_NAME,
    )))
});

#[must_use]
pub fn narrowed_beside_udf(untyped_null: bool) -> Arc<ScalarUDF> {
    if untyped_null {
        Arc::clone(&NARROWED_BESIDE_NULL)
    } else {
        Arc::clone(&NARROWED_BESIDE_VALUE)
    }
}

#[must_use]
pub fn holds_nanoseconds(schema: &DFSchema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| nested_nanoseconds(field.data_type()))
}

fn nested_nanoseconds(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(unit, _) => *unit == TimeUnit::Nanosecond,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| nested_nanoseconds(field.data_type())),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => nested_nanoseconds(field.data_type()),
        _ => false,
    }
}

#[must_use]
pub fn is_narrowing_mark(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function) if matches!(
            function.func.name(),
            NARROWED_BESIDE_NULL_NAME | NARROWED_BESIDE_VALUE_NAME
        )
    )
}

fn marked(value: Expr, beside_untyped_null: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        narrowed_beside_udf(beside_untyped_null),
        vec![value],
    ))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NarrowedBeside {
    name: &'static str,
    signature: Signature,
}

impl NarrowedBeside {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NarrowedBeside {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(one_argument)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        (args.arg_fields.first().map(Arc::clone)).ok_or_else(one_argument)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let value = args.first().ok_or_else(one_argument)?;
        Ok(value.schema_name().to_string())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.into_iter().next().ok_or_else(one_argument)
    }

    fn simplify(&self, mut args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        match args.pop() {
            Some(value) if args.is_empty() => Ok(ExprSimplifyResult::Simplified(value)),
            _ => Err(one_argument()),
        }
    }
}

fn one_argument() -> DataFusionError {
    DataFusionError::Plan(format!(
        "'{NARROWED_BESIDE_NULL_NAME}' expects one argument"
    ))
}

#[derive(Debug, Default)]
pub struct FloatStringifyBeforeCoercion;

impl AnalyzerRule for FloatStringifyBeforeCoercion {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi = crate::ansi::spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| {
            let tagged = tag_union_nulls(node)?;
            if tagged.transformed {
                return Ok(tagged);
            }
            crate::java_double::rewrite_float_plan(tagged.data, ansi, true)
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "spark_float_stringify"
    }
}

pub(crate) fn before_coercion(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if let Expr::Cast(cast) = &expr
        && instant(cast.field.data_type())
        && !is_narrowing_mark(&cast.expr)
        && !null_literal(&cast.expr)
        && holds(&cast.expr, schema, true)
    {
        return Ok(Transformed::yes(
            crate::timestamp_ns_cast::narrow_timestamp_ns_expr((*cast.expr).clone()),
        ));
    }
    let written = |branch: &Expr| {
        matches!(branch, Expr::Cast(cast)
            if sql_timestamp(cast.field.data_type())
                && !is_narrowing_mark(&cast.expr)
                && holds(&cast.expr, schema, true))
    };
    if matches!(expr, Expr::Alias(_)) || !any_branch(&expr, written) {
        return tag_untyped_nulls(expr, schema);
    }
    expr.map_children(|branch| match branch {
        Expr::Cast(cast) if written(&Expr::Cast(cast.clone())) => Ok(Transformed::yes(
            crate::timestamp_ns_cast::narrow_timestamp_ns_expr(*cast.expr),
        )),
        other => Ok(Transformed::no(other)),
    })?
    .transform_data(|node| tag_untyped_nulls(node, schema))
}

fn coerced(expr: &Expr, schema: &DFSchema) -> bool {
    matches!(expr, Expr::Cast(cast)
    if !is_narrowing_mark(&cast.expr)
        && !null_literal(&cast.expr)
        && !to_the_wire_type(&cast.expr)
        && !matches!(cast.expr.as_ref(), Expr::Literal(_, _))
        && cast.expr.get_type(schema).is_ok_and(|source| {
            narrows(&source, cast.field.data_type())
        }))
}

fn narrows(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Timestamp(TimeUnit::Nanosecond, zone), DataType::Timestamp(unit, kept)) => {
            *unit != TimeUnit::Nanosecond || (zone.is_some() && kept.is_none())
        }
        (
            DataType::List(from) | DataType::LargeList(from) | DataType::FixedSizeList(from, _),
            DataType::List(to) | DataType::LargeList(to) | DataType::FixedSizeList(to, _),
        )
        | (DataType::Map(from, _), DataType::Map(to, _)) => {
            narrows(from.data_type(), to.data_type())
        }
        (DataType::Struct(from), DataType::Struct(to)) => from
            .iter()
            .zip(to.iter())
            .any(|(from, to)| narrows(from.data_type(), to.data_type())),
        _ => false,
    }
}

fn to_the_wire_type(expr: &Expr) -> bool {
    matches!(expr, Expr::Cast(cast) if sql_timestamp(cast.field.data_type()))
}

pub(crate) struct Marks {
    scoped: bool,
    produces: bool,
    kept: Cell<bool>,
    untagged: Cell<bool>,
}

impl Marks {
    pub(crate) fn of(plan: &LogicalPlan, schema: &DFSchema) -> Self {
        let produces = produces_columns(plan);
        Self {
            scoped: produces && holds_nanoseconds(schema),
            produces,
            kept: Cell::new(false),
            untagged: Cell::new(false),
        }
    }

    pub(crate) fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        cast: &dyn Fn(Expr) -> Transformed<Expr>,
    ) -> Result<Transformed<Expr>> {
        if !self.produces {
            return expr.transform_up(|node| Ok(cast(node)));
        }
        let asks = self.scoped || expr.exists(|node| Ok(produces_nanoseconds(node)))?;
        let settling = |node: Expr| {
            self.keep_written_cast(node, schema)
                .transform_data(|node| Ok(cast(node)))?
                .transform_data(|node| {
                    if unifies_branches(&node) && any_branch(&node, is_tagged_null) {
                        self.untagged.set(true);
                    }
                    settle_branches(node, schema, asks)
                })
        };
        self.untagged.set(false);
        let rewritten = if asks {
            let marking = |node| mark_coerced_branches(node, schema);
            mark_coerced_narrowing(expr, schema, false)
                .transform_data(|root| root.transform_down_up(marking, settling))?
        } else {
            expr.transform_up(settling)?
        };
        if !self.untagged.get() {
            return Ok(rewritten);
        }
        Ok(rewritten.update_data(named_by_itself))
    }

    fn keep_written_cast(&self, expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
        let Expr::Cast(cast) = &expr else {
            return Transformed::no(expr);
        };
        if !sql_timestamp(cast.field.data_type()) {
            return Transformed::no(expr);
        }
        let source = cast.expr.get_type(schema);
        let marked = holds_mark(&cast.expr);
        let over_a_mark = marked && matches!(source, Ok(DataType::Timestamp(_, _)));
        let over_a_column = !marked
            && source.is_ok_and(|found| instant(&found))
            && (cast.expr)
                .exists(|node| Ok(matches!(node, Expr::Column(_))))
                .unwrap_or(false);
        if !over_a_mark && !over_a_column {
            return Transformed::no(expr);
        }
        if over_a_column {
            self.kept.set(true);
        }
        let Expr::Cast(cast) = expr else {
            return Transformed::no(expr);
        };
        Transformed::yes(crate::timestamp_ns_cast::narrow_timestamp_ns_expr(
            *cast.expr,
        ))
    }

    pub(crate) fn settle_plan(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if !self.kept.get() || plan.inputs().into_iter().any(plan_holds_mark) {
            return settle_union_branches(with_branch_types(plan)?);
        }
        let schema = input_schema(&plan);
        plan.map_expressions(|expr| expr.transform_up(|node| Ok(drop_kept_cast(node, &schema))))?
            .map_data(LogicalPlan::recompute_schema)
    }
}

fn named_by_itself(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias)
            if alias.relation.is_none()
                && alias.metadata.is_none()
                && alias.expr.schema_name().to_string() == alias.name =>
        {
            *alias.expr
        }
        other => other,
    }
}

fn with_branch_types(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Union(union) = plan else {
        return Ok(plan);
    };
    let stale = |(index, field): (usize, &Arc<Field>)| {
        matches!(field.data_type(), DataType::Timestamp(_, _))
            && union.inputs.iter().any(|input| {
                (input.schema().fields().get(index))
                    .is_some_and(|branch| branch.data_type() != field.data_type())
            })
    };
    if !union.schema.fields().iter().enumerate().any(stale) {
        return Ok(LogicalPlan::Union(union));
    }
    let Ok(coerced) = coerce_union_schema(&union.inputs) else {
        return Ok(LogicalPlan::Union(union));
    };
    let fields = union
        .schema
        .iter()
        .zip(coerced.fields())
        .map(|((qualifier, field), found)| {
            let typed = field
                .as_ref()
                .clone()
                .with_data_type(found.data_type().clone());
            (qualifier.cloned(), Arc::new(typed))
        })
        .collect();
    let typed = DFSchema::new_with_metadata(fields, union.schema.as_arrow().metadata().clone())?;
    let dependencies = union.schema.functional_dependencies().clone();
    Ok(LogicalPlan::Union(Union {
        inputs: union.inputs,
        schema: Arc::new(typed.with_functional_dependencies(dependencies)?),
    }))
}

fn input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn drop_kept_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let kept = crate::timestamp_ns_cast::is_narrow_timestamp_ns(&expr);
    match expr {
        Expr::ScalarFunction(mut function)
            if kept
                && function.args.len() == 1
                && !holds_mark(&function.args[0])
                && (function.args[0].get_type(schema)).is_ok_and(|found| instant(&found)) =>
        {
            Transformed::yes(function.args.remove(0))
        }
        other => Transformed::no(other),
    }
}

fn holds_mark(expr: &Expr) -> bool {
    expr.exists(|node| Ok(is_narrowing_mark(node)))
        .unwrap_or(false)
}

fn plan_holds_mark(plan: &LogicalPlan) -> bool {
    plan.exists(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let held = match scan.source.get_logical_plan() {
                Some(view) => plan_holds_mark(view.as_ref()),
                None => scan.source.table_type() != TableType::Base,
            };
            if held {
                return Ok(true);
            }
        }
        let mut marked = false;
        node.apply_expressions(|expr| {
            marked = holds_mark(expr);
            Ok(if marked {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(marked)
    })
    .unwrap_or(false)
}

#[must_use]
pub fn produces_columns(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::Filter(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::TableScan(_)
    )
}

fn produces_nanoseconds(expr: &Expr) -> bool {
    match expr {
        Expr::OuterReferenceColumn(field, _) => nested_nanoseconds(field.data_type()),
        Expr::LambdaVariable(variable) => {
            (variable.field.as_ref()).is_some_and(|field| nested_nanoseconds(field.data_type()))
        }
        Expr::Cast(cast) => produced(cast.field.data_type()),
        Expr::TryCast(cast) => produced(cast.field.data_type()),
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            name.contains("_ns") || name.contains("nanos")
        }
        Expr::ScalarSubquery(subquery) => holds_nanoseconds(subquery.subquery.schema()),
        _ => false,
    }
}

fn produced(target: &DataType) -> bool {
    nested_nanoseconds(target) && !sql_timestamp(target)
}

pub(crate) fn mark_coerced_narrowing(
    expr: Expr,
    schema: &DFSchema,
    wire: bool,
) -> Transformed<Expr> {
    match expr {
        Expr::Alias(mut alias) => {
            let value = mark_coerced_narrowing(*alias.expr, schema, wire);
            alias.expr = Box::new(value.data);
            Transformed::new_transformed(Expr::Alias(alias), value.transformed)
        }
        Expr::Cast(cast) => {
            let found = Expr::Cast(cast);
            if !coerced(&found, schema) || (!wire && to_the_wire_type(&found)) {
                return Transformed::no(found);
            }
            let Expr::Cast(cast) = found else {
                return Transformed::no(found);
            };
            Transformed::yes(Expr::Cast(Cast::new_from_field(
                Box::new(marked(*cast.expr, false)),
                cast.field,
            )))
        }
        other => Transformed::no(other),
    }
}

pub(crate) fn mark_coerced_branches(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let narrowed = |branch: &Expr| coerced(branch, schema);
    let unified = match &expr {
        Expr::Case(_) => any_branch(&expr, narrowed),
        Expr::ScalarFunction(_) => {
            any_branch(&expr, narrowed)
                && any_branch(&expr, |branch| {
                    (narrowed(branch) && to_the_wire_type(branch))
                        || (!narrowed(branch) && kind_of(branch, schema) == Kind::Coarser)
                })
        }
        _ => false,
    };
    if !unified {
        return Ok(Transformed::no(expr));
    }
    expr.map_children(|branch| Ok(mark_coerced_narrowing(branch, schema, true)))
}

fn tag_untyped_nulls(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !unifies_branches(&expr)
        || !any_branch(&expr, is_untyped_null)
        || !any_branch(&expr, |branch| holds(branch, schema, true))
    {
        return Ok(Transformed::no(expr));
    }
    expr.map_children(|branch| {
        Ok(if is_untyped_null(&branch) {
            Transformed::yes(Expr::Literal(ScalarValue::Null, Some(tag())))
        } else {
            Transformed::no(branch)
        })
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Nanoseconds,
    Coarser,
    Other,
}

fn kind_of(branch: &Expr, schema: &DFSchema) -> Kind {
    match branch.get_type(schema) {
        Ok(found) if nested_nanoseconds(&found) => {
            if is_narrowing_mark(branch) {
                Kind::Other
            } else {
                Kind::Nanoseconds
            }
        }
        Ok(found) if nested_coarser(&found) => Kind::Coarser,
        _ => Kind::Other,
    }
}

pub(crate) fn settle_branches(
    expr: Expr,
    schema: &DFSchema,
    asks: bool,
) -> Result<Transformed<Expr>> {
    if !unifies_branches(&expr) {
        return Ok(Transformed::no(expr));
    }
    let tagged = any_branch(&expr, is_tagged_null);
    if !tagged && !asks {
        return Ok(Transformed::no(expr));
    }
    let holds = |kind: Kind| any_branch(&expr, |branch| kind_of(branch, schema) == kind);
    let narrows =
        holds(Kind::Coarser) && holds(Kind::Nanoseconds) && loses_nanoseconds(&expr, schema);
    if !tagged && !narrows {
        return Ok(Transformed::no(expr));
    }
    expr.map_children(|branch| {
        if is_tagged_null(&branch) {
            branch.transform_up(|node| Ok(untagged(node)))
        } else if narrows && kind_of(&branch, schema) == Kind::Nanoseconds {
            Ok(Transformed::yes(marked(branch, tagged)))
        } else {
            Ok(Transformed::no(branch))
        }
    })
}

fn loses_nanoseconds(expr: &Expr, schema: &DFSchema) -> bool {
    let coerced = TypeCoercionRewriter::new(schema).f_up(expr.clone());
    let result = coerced.and_then(|node| node.data.get_type(schema));
    result.is_ok_and(|found| nested_coarser(&found) && !nested_nanoseconds(&found))
}

fn nested_coarser(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(unit, _) => *unit != TimeUnit::Nanosecond,
        DataType::Struct(fields) => fields.iter().any(|field| nested_coarser(field.data_type())),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => nested_coarser(field.data_type()),
        _ => false,
    }
}

pub(crate) fn tag_union_nulls(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Union(union) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let mut changed = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        let schema = Arc::clone(input.schema());
        let untyped =
            |index: usize| untyped_column(input, index) && beside_nanoseconds(union, index);
        let zoned = |index: usize| zoned_beside_a_date(union, input, index);
        if !(0..schema.fields().len()).any(|index| untyped(index) || zoned(index)) {
            inputs.push(Arc::clone(input));
            continue;
        }
        let (mut exprs, source) = match input.as_ref() {
            LogicalPlan::Projection(projection) => {
                (projection.expr.clone(), Arc::clone(&projection.input))
            }
            _ => (
                schema
                    .iter()
                    .map(|column| Expr::Column(column.into()))
                    .collect(),
                Arc::clone(input),
            ),
        };
        for (index, (qualifier, field)) in schema.iter().enumerate() {
            if untyped(index) {
                exprs[index] = Expr::Literal(ScalarValue::Null, Some(tag()))
                    .alias_qualified(qualifier.cloned(), field.name());
            } else if zoned(index) {
                let value = match std::mem::take(&mut exprs[index]) {
                    Expr::Alias(alias) => *alias.expr,
                    value => value,
                };
                exprs[index] =
                    marked(value, false).alias_qualified(qualifier.cloned(), field.name());
            }
        }
        let rebuilt = Projection::try_new(exprs, source)?;
        inputs.push(Arc::new(LogicalPlan::Projection(rebuilt)));
        changed = true;
    }
    if !changed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs,
        schema: Arc::clone(&union.schema),
    })))
}

pub(crate) fn settle_union_branches(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Union(union) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let width = union.schema.fields().len();
    let tagged = |index: usize| {
        union.inputs.iter().any(|input| {
            matches!(input.as_ref(), LogicalPlan::Projection(projection)
                if projection.expr.get(index).is_some_and(|expr| is_tagged_null(peeled(expr))))
        })
    };
    let narrows = |index: usize| {
        let kinds = |nanoseconds: bool| {
            union.inputs.iter().any(|input| {
                matches!(quick_type(input, index), Some(DataType::Timestamp(unit, _))
                    if (unit == TimeUnit::Nanosecond) == nanoseconds)
            })
        };
        kinds(true) && kinds(false)
    };
    let columns: Vec<(bool, bool)> = (0..width)
        .map(|index| (tagged(index), narrows(index)))
        .collect();
    if !columns.iter().any(|(tagged, narrows)| *tagged || *narrows) {
        return Ok(Transformed::no(plan));
    }
    let mut settled = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        let schema = Arc::clone(input.schema());
        let (mut exprs, source) = match input.as_ref() {
            LogicalPlan::Projection(projection) => {
                (projection.expr.clone(), Arc::clone(&projection.input))
            }
            _ => (
                schema
                    .iter()
                    .map(|column| Expr::Column(column.into()))
                    .collect(),
                Arc::clone(input),
            ),
        };
        let mut changed = false;
        for (index, (tagged, narrows)) in columns.iter().enumerate() {
            let Some(branch) = exprs.get(index).cloned() else {
                continue;
            };
            let nanoseconds = *narrows
                && !is_narrowing_mark(peeled(&branch))
                && holds(peeled(&branch), source.schema().as_ref(), true);
            if !nanoseconds && !is_tagged_null(peeled(&branch)) {
                continue;
            }
            let (qualifier, field) = schema.qualified_field(index);
            let value = branch.transform_up(|node| Ok(untagged(node)))?.data;
            let value = match value {
                Expr::Alias(alias) => *alias.expr,
                value => value,
            };
            let value = if nanoseconds {
                marked(value, *tagged)
            } else {
                value
            };
            exprs[index] = value.alias_qualified(qualifier.cloned(), field.name());
            changed = true;
        }
        if changed {
            settled = true;
            let rebuilt = Projection::try_new(exprs, source)?;
            inputs.push(Arc::new(LogicalPlan::Projection(rebuilt)));
        } else {
            inputs.push(Arc::clone(input));
        }
    }
    if !settled {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs,
        schema: untagged_schema(&union.schema)?,
    })))
}

fn untagged_schema(schema: &Arc<DFSchema>) -> Result<Arc<DFSchema>> {
    let tagged = |field: &Arc<Field>| field.metadata().contains_key(UNTYPED_NULL_TAG);
    if !schema.fields().iter().any(tagged) {
        return Ok(Arc::clone(schema));
    }
    let fields = schema
        .iter()
        .map(|(qualifier, field)| {
            let mut metadata = field.metadata().clone();
            metadata.remove(UNTYPED_NULL_TAG);
            let field = field.as_ref().clone().with_metadata(metadata);
            (qualifier.cloned(), Arc::new(field))
        })
        .collect();
    let untagged = DFSchema::new_with_metadata(fields, schema.as_arrow().metadata().clone())?;
    let dependencies = schema.functional_dependencies().clone();
    Ok(Arc::new(
        untagged.with_functional_dependencies(dependencies)?,
    ))
}

fn quick_type(input: &LogicalPlan, index: usize) -> Option<DataType> {
    let declared = || (input.schema().fields().get(index)).map(|field| field.data_type().clone());
    let LogicalPlan::Projection(projection) = input else {
        return declared();
    };
    match projection.expr.get(index).map(peeled) {
        Some(Expr::Column(column)) => (projection.input.schema().field_from_column(column))
            .ok()
            .map(|field| field.data_type().clone()),
        Some(Expr::Cast(cast)) => Some(cast.field.data_type().clone()),
        Some(Expr::Literal(value, _)) => Some(value.data_type()),
        _ => declared(),
    }
}

fn zoned_beside_a_date(union: &Union, input: &LogicalPlan, index: usize) -> bool {
    let zoned = matches!(
        branch_type(input, index),
        Some(DataType::Timestamp(TimeUnit::Nanosecond, Some(_)))
    );
    let marked = matches!(input, LogicalPlan::Projection(projection)
        if projection.expr.get(index).is_some_and(|expr| is_narrowing_mark(peeled(expr))));
    zoned
        && !marked
        && union.inputs.iter().any(|other| {
            matches!(
                branch_type(other, index),
                Some(DataType::Date32 | DataType::Date64)
            )
        })
}

fn beside_nanoseconds(union: &Union, index: usize) -> bool {
    union.inputs.iter().any(|input| {
        matches!(
            branch_type(input, index),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, _))
        )
    })
}

fn untyped_column(input: &LogicalPlan, index: usize) -> bool {
    let untyped = |data_type: &DataType| data_type == &DataType::Null;
    let LogicalPlan::Projection(projection) = input else {
        return (input.schema().fields().get(index))
            .is_some_and(|field| untyped(field.data_type()));
    };
    match projection.expr.get(index).map(peeled) {
        Some(Expr::Literal(ScalarValue::Null, _)) => true,
        Some(Expr::Column(column)) => (projection.input.schema().field_from_column(column))
            .is_ok_and(|field| untyped(field.data_type())),
        Some(Expr::Cast(cast)) => untyped(cast.field.data_type()),
        _ => false,
    }
}

fn branch_type(input: &LogicalPlan, index: usize) -> Option<DataType> {
    match input {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(index)?
            .get_type(projection.input.schema().as_ref())
            .ok(),
        other => other
            .schema()
            .fields()
            .get(index)
            .map(|field| field.data_type().clone()),
    }
}

fn peeled(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => peeled(&alias.expr),
        other => other,
    }
}

fn unifies_branches(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(_) | Expr::Case(_))
}

fn any_branch(expr: &Expr, test: impl Fn(&Expr) -> bool) -> bool {
    let mut found = false;
    let walked = expr.apply_children(|branch| {
        found = test(branch);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    walked.is_ok() && found
}

fn is_untyped_null(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Null, None))
}

fn is_tagged_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, Some(metadata)) => {
            value.is_null() && metadata.inner().contains_key(UNTYPED_NULL_TAG)
        }
        Expr::Cast(cast) => is_tagged_null(&cast.expr),
        Expr::TryCast(cast) => is_tagged_null(&cast.expr),
        _ => false,
    }
}

fn untagged(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::Literal(value, Some(metadata)) if metadata.inner().contains_key(UNTYPED_NULL_TAG) => {
            Transformed::yes(Expr::Literal(value, None))
        }
        other => Transformed::no(other),
    }
}

fn tag() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        UNTYPED_NULL_TAG.to_string(),
        String::new(),
    )]))
}

fn holds(expr: &Expr, schema: &DFSchema, nanoseconds: bool) -> bool {
    matches!(
        expr.get_type(schema),
        Ok(DataType::Timestamp(unit, _)) if (unit == TimeUnit::Nanosecond) == nanoseconds
    )
}

fn instant(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

fn sql_timestamp(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, None))
}

fn null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => null_literal(&cast.expr),
        Expr::TryCast(cast) => null_literal(&cast.expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
