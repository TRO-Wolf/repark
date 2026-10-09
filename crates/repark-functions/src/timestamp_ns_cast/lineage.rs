use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::nested::{Unstorable, holds_timestamp_ns, nested_refusal};
use super::{
    NARROW_TIMESTAMP_NS_NAME, TIMESTAMP_NS_CAST_NAME, TIMESTAMPTZ_NS_CAST_NAME, timestamp_ns_target,
};

type TypeOf<'a> = &'a dyn Fn(&Expr) -> Option<DataType>;

#[derive(Debug, Default)]
pub struct NestedNanosecondGuard;

impl OptimizerRule for NestedNanosecondGuard {
    fn name(&self) -> &'static str {
        "repark_nested_timestamp_ns_guard"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.apply_with_subqueries(|node| {
            let inputs = node.inputs();
            let mut schema = DFSchema::empty();
            for input in &inputs {
                schema.merge(input.schema());
            }
            for expr in node.expressions() {
                expr.apply(|part| {
                    refuse_narrowed(part, &inputs, &schema)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(Transformed::no(plan))
    }
}

fn refuse_narrowed(part: &Expr, inputs: &[&LogicalPlan], schema: &DFSchema) -> Result<()> {
    let Expr::ScalarFunction(call) = part else {
        return Ok(());
    };
    let zoned = match call.func.name() {
        TIMESTAMP_NS_CAST_NAME => false,
        TIMESTAMPTZ_NS_CAST_NAME => true,
        _ => return Ok(()),
    };
    let [value, shape, _, Expr::Literal(column, _)] = call.args.as_slice() else {
        return Ok(());
    };
    let type_of = |source: &Expr| source.get_type(schema).ok();
    let Some(target) = type_of(shape) else {
        return Ok(());
    };
    let below = match inputs {
        [input] => match carried(value) {
            Expr::Column(carried) => input
                .schema()
                .index_of_column(carried)
                .is_ok_and(|index| narrows_nanoseconds(input, index, &target)),
            built => columns_narrow(built, input),
        },
        _ => false,
    };
    if !(below || narrows_into(value, &target, zoned, &type_of)) {
        return Ok(());
    }
    let leaf = [column
        .try_as_str()
        .flatten()
        .unwrap_or_default()
        .to_string()];
    let found = type_of(value).unwrap_or(DataType::Null);
    Err(nested_refusal(&leaf, &found, zoned, Unstorable::Narrowed))
}

fn narrows_nanoseconds(plan: &LogicalPlan, column: usize, target: &DataType) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            let Some(expr) = projection.expr.get(column) else {
                return false;
            };
            let input = projection.input.as_ref();
            let schema = input.schema();
            if let Expr::Column(carried) = carried(expr) {
                return schema
                    .index_of_column(carried)
                    .is_ok_and(|index| narrows_nanoseconds(input, index, target));
            }
            narrows_into(expr, target, false, &|source| source.get_type(schema).ok())
                || columns_narrow(expr, input)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            narrows_nanoseconds(alias.input.as_ref(), column, target)
        }
        LogicalPlan::Filter(filter) => narrows_nanoseconds(filter.input.as_ref(), column, target),
        LogicalPlan::Sort(sort) => narrows_nanoseconds(sort.input.as_ref(), column, target),
        LogicalPlan::Limit(limit) => narrows_nanoseconds(limit.input.as_ref(), column, target),
        LogicalPlan::Distinct(_) | LogicalPlan::Repartition(_) => plan
            .inputs()
            .into_iter()
            .any(|input| narrows_nanoseconds(input, column, target)),
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| narrows_nanoseconds(input.as_ref(), column, target)),
        LogicalPlan::Values(values) => {
            let empty = DFSchema::empty();
            values
                .values
                .iter()
                .filter_map(|row| row.get(column))
                .any(|cell| {
                    narrows_into(cell, target, false, &|source| source.get_type(&empty).ok())
                })
        }
        LogicalPlan::Join(join) => {
            let left = join.left.schema().fields().len();
            match join.join_type {
                JoinType::RightSemi | JoinType::RightAnti => {
                    narrows_nanoseconds(join.right.as_ref(), column, target)
                }
                _ if column < left => narrows_nanoseconds(join.left.as_ref(), column, target),
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => false,
                _ => narrows_nanoseconds(join.right.as_ref(), column - left, target),
            }
        }
        LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => false,
        other => anywhere(other),
    }
}

fn carried(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => carried(&alias.expr),
        other => other,
    }
}

fn carries_coarse(data_type: &DataType) -> bool {
    let coarse = |field: &FieldRef| carries_coarse(field.data_type());
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => false,
        DataType::Timestamp(_, _) => true,
        DataType::Struct(fields) => fields.iter().any(coarse),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => coarse(field),
        DataType::Dictionary(_, values) => carries_coarse(values),
        _ => false,
    }
}

fn columns_narrow(expr: &Expr, input: &LogicalPlan) -> bool {
    let mut columns: Vec<Column> = Vec::new();
    let walked = expr.apply(|node| {
        if let Expr::Column(column) = node {
            columns.push(column.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    let leaf = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let schema = input.schema();
    walked.is_err()
        || columns.iter().any(|column| {
            schema.index_of_column(column).is_ok_and(|index| {
                carries_coarse(schema.field(index).data_type())
                    && narrows_nanoseconds(input, index, &leaf)
            })
        })
}

fn anywhere(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let walked = plan.apply(|node| {
        let mut schema = DFSchema::empty();
        for input in node.inputs() {
            schema.merge(input.schema());
        }
        found |= node
            .expressions()
            .iter()
            .any(|expr| narrows_with(expr, &|source| source.get_type(&schema).ok()));
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found || walked.is_err()
}

pub(super) fn narrows_into(expr: &Expr, target: &DataType, zoned: bool, type_of: TypeOf) -> bool {
    if timestamp_ns_target(target) == Some(zoned) {
        return narrows_with(expr, type_of);
    }
    if !holds_timestamp_ns(target, zoned) {
        return false;
    }
    let into = |value: &Expr, target: &DataType| narrows_into(value, target, zoned, type_of);
    match expr {
        Expr::Alias(alias) => into(&alias.expr, target),
        Expr::Cast(cast) => into(&cast.expr, target),
        Expr::TryCast(cast) => into(&cast.expr, target),
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .any(|value| into(value, target)),
        Expr::ScalarFunction(function) => match (function.func.name(), target, element(target)) {
            ("named_struct", DataType::Struct(fields), _) => {
                let named = function
                    .args
                    .chunks(2)
                    .enumerate()
                    .map(|(index, pair)| match pair {
                        [Expr::Literal(name, _), value] => {
                            Some((index, name.try_as_str().flatten()?, value))
                        }
                        _ => None,
                    });
                named.into_iter().any(|member| match member {
                    Some((index, name, value)) => paired(fields, index, name)
                        .into_iter()
                        .any(|field| into(value, field.data_type())),
                    None => narrows_with(expr, type_of),
                })
            }
            ("struct", DataType::Struct(fields), _) => function
                .args
                .iter()
                .zip(fields)
                .any(|(value, field)| into(value, field.data_type())),
            ("make_array" | "array", _, Some(item)) => function
                .args
                .iter()
                .any(|value| into(value, item.data_type())),
            (crate::decimal_cast::SPARK_NONNULL_NAME, _, _) => {
                function.args.iter().any(|value| into(value, target))
            }
            (TIMESTAMP_NS_CAST_NAME | TIMESTAMPTZ_NS_CAST_NAME | "arrow_cast", _, _) => function
                .args
                .first()
                .is_some_and(|value| into(value, target)),
            ("get_field", _, _) => match member(&function.args) {
                Some(value) => into(value, target),
                None => narrows_with(expr, type_of),
            },
            _ => narrows_with(expr, type_of),
        },
        other => narrows_with(other, type_of),
    }
}

fn paired<'a>(fields: &'a Fields, index: usize, name: &str) -> Vec<&'a FieldRef> {
    let by_name = fields
        .iter()
        .find(|field| field.name().eq_ignore_ascii_case(name));
    by_name.into_iter().chain(fields.get(index)).collect()
}

fn member(args: &[Expr]) -> Option<&Expr> {
    let [
        Expr::ScalarFunction(whole),
        Expr::Literal(ScalarValue::Utf8(Some(name)), _),
    ] = args
    else {
        return None;
    };
    if whole.func.name() != "named_struct" {
        return None;
    }
    whole.args.chunks(2).find_map(|pair| match pair {
        [Expr::Literal(found, _), value] if found.try_as_str().flatten() == Some(name) => {
            Some(value)
        }
        _ => None,
    })
}

fn element(target: &DataType) -> Option<&FieldRef> {
    match target {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Some(field),
        _ => None,
    }
}

pub(super) fn narrows_with(expr: &Expr, type_of: TypeOf) -> bool {
    let mut found = false;
    let walked = expr.apply(|node| {
        found |= match node {
            Expr::ScalarFunction(function) => function.func.name() == NARROW_TIMESTAMP_NS_NAME,
            Expr::Cast(cast) => coarser(&cast.expr, cast.field.data_type(), type_of),
            Expr::TryCast(cast) => coarser(&cast.expr, cast.field.data_type(), type_of),
            _ => false,
        };
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found || walked.is_err()
}

fn coarser(source: &Expr, target: &DataType, type_of: TypeOf) -> bool {
    let nanoseconds = matches!(
        type_of(source),
        Some(DataType::Timestamp(TimeUnit::Nanosecond, _))
    );
    nanoseconds
        && !is_null(source)
        && matches!(
            target,
            DataType::Timestamp(
                TimeUnit::Microsecond | TimeUnit::Millisecond | TimeUnit::Second,
                _
            )
        )
}

fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => scalar.is_null(),
        Expr::Cast(cast) => is_null(&cast.expr),
        Expr::TryCast(cast) => is_null(&cast.expr),
        Expr::Alias(alias) => is_null(&alias.expr),
        _ => false,
    }
}
