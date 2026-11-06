use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, Projection,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Union, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use repark_functions::cast_map::spark_sql_name;

use super::map_ordering::render;

const STRUCT_BY_POSITION: &str = "__repark_struct_by_position__";
const ORDINALS: [&str; 10] = [
    "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth",
];

#[derive(Debug, Default)]
pub struct StructOperandsByPosition;

impl AnalyzerRule for StructOperandsByPosition {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let case_sensitive =
            repark_functions::case_sensitive::spark_case_sensitive_from_options(config);
        plan.transform_up_with_subqueries(|node| align_node(node, case_sensitive))
            .data()
    }

    fn name(&self) -> &'static str {
        "struct_operands_by_position"
    }
}

pub fn insert_struct_position_rule_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Plan(
            "struct operands by position require the default type_coercion analyzer rule"
                .to_string(),
        ));
    };
    rules.insert(position, Arc::new(StructOperandsByPosition));
    Ok(rules)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Seat {
    Comparison,
    Promoting,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pairing {
    Same,
    Widen,
    Clash,
}

fn pairing(left: &DataType, right: &DataType, seat: Seat, case_sensitive: bool) -> Pairing {
    match (left, right) {
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            if left_fields.len() != right_fields.len() {
                return Pairing::Clash;
            }
            let named = left_fields.iter().zip(right_fields.iter()).all(|(l, r)| {
                if case_sensitive {
                    l.name() == r.name()
                } else {
                    l.name().eq_ignore_ascii_case(r.name())
                }
            });
            let mut result = Pairing::Same;
            for (l, r) in left_fields.iter().zip(right_fields.iter()) {
                match pairing(l.data_type(), r.data_type(), seat, case_sensitive) {
                    Pairing::Same => {}
                    Pairing::Widen if named => result = Pairing::Widen,
                    _ => return Pairing::Clash,
                }
            }
            result
        }
        (DataType::Struct(_), _) | (_, DataType::Struct(_)) => Pairing::Clash,
        _ if spark_sql_name(left) == spark_sql_name(right) => Pairing::Same,
        _ if widens(left, right, seat, case_sensitive) => Pairing::Widen,
        _ => Pairing::Clash,
    }
}

fn widens(left: &DataType, right: &DataType, seat: Seat, case_sensitive: bool) -> bool {
    let decimal_digits = |data_type: &DataType| match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Some(i16::from(*precision) - i16::from(*scale)),
        _ => None,
    };
    let integer_digits = |data_type: &DataType| match data_type {
        DataType::Int8 | DataType::UInt8 => Some(3),
        DataType::Int16 | DataType::UInt16 => Some(5),
        DataType::Int32 | DataType::UInt32 => Some(10),
        DataType::Int64 | DataType::UInt64 => Some(20),
        _ => None,
    };
    let plain_numeric =
        |data_type: &DataType| integer_digits(data_type).is_some() || data_type.is_floating();
    let temporal = |data_type: &DataType| data_type.is_temporal();
    match (left, right) {
        (DataType::Null, _) | (_, DataType::Null) => true,
        (
            DataType::List(l) | DataType::LargeList(l) | DataType::FixedSizeList(l, _),
            DataType::List(r) | DataType::LargeList(r) | DataType::FixedSizeList(r, _),
        ) => pairing(l.data_type(), r.data_type(), seat, case_sensitive) != Pairing::Clash,
        (DataType::Map(l, _), DataType::Map(r, _)) => {
            pairing(l.data_type(), r.data_type(), seat, case_sensitive) != Pairing::Clash
        }
        _ if seat == Seat::Promoting
            && (left.is_numeric() || left.is_temporal() || is_string(left))
            && (right.is_numeric() || right.is_temporal() || is_string(right)) =>
        {
            true
        }
        _ if plain_numeric(left) && plain_numeric(right) => true,
        _ => match (
            integer_digits(left),
            decimal_digits(right),
            decimal_digits(left),
            integer_digits(right),
        ) {
            (Some(digits), Some(room), _, _) | (_, _, Some(room), Some(digits)) => room >= digits,
            _ => temporal(left) && temporal(right),
        },
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn align_node(node: LogicalPlan, case_sensitive: bool) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Union(union) = node {
        return align_union(union, case_sensitive);
    }
    if !has_struct_candidate(&node) {
        return Ok(Transformed::no(node));
    }
    let mut schema = DFSchema::empty();
    for input in node.inputs() {
        schema.merge(input.schema());
    }
    let preserver = NamePreserver::new(&node);
    node.map_expressions(|expr| {
        let name = preserver.save(&expr);
        expr.transform_up(|leaf| align_expr(leaf, &schema, case_sensitive))
            .map(|aligned| aligned.update_data(|expr| name.restore(expr)))
    })
}

fn has_struct_candidate(node: &LogicalPlan) -> bool {
    node.expressions().iter().any(|expr| {
        expr.exists(|leaf| {
            Ok(matches!(
                leaf,
                Expr::BinaryExpr(BinaryExpr { op, .. }) if is_comparison(*op)
            ) || matches!(leaf, Expr::InList(_)))
        })
        .unwrap_or(false)
    })
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn spark_operator(op: Operator) -> &'static str {
    match op {
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        Operator::IsNotDistinctFrom => "<=>",
        Operator::IsDistinctFrom => "NOT <=>",
        _ => "=",
    }
}

fn align_expr(expr: Expr, schema: &DFSchema, case_sensitive: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let target = left.get_type(schema).ok();
            let current = right.get_type(schema).ok();
            if let (Some(target), Some(current)) = (&target, &current)
                && clashes(target, current, Seat::Comparison, case_sensitive)
            {
                return Err(DataFusionError::Plan(format!(
                    "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve \"({} {} {})\" due to \
                     data type mismatch: the left and right operands of the binary operator have \
                     incompatible types (\"{}\" and \"{}\"). SQLSTATE: 42K09",
                    render(&left),
                    spark_operator(op),
                    render(&right),
                    spark_sql_name(target),
                    spark_sql_name(current)
                )));
            }
            let (right, changed) = aligned_operand(target.as_ref(), *right, schema);
            let expr = Expr::BinaryExpr(BinaryExpr::new(left, op, Box::new(right)));
            Ok(if changed {
                Transformed::yes(expr)
            } else {
                Transformed::no(expr)
            })
        }
        Expr::InList(InList {
            expr: value,
            list,
            negated,
        }) => {
            let target = value.get_type(schema).ok();
            if let Some(target) = &target {
                for item in &list {
                    if let Ok(current) = item.get_type(schema)
                        && clashes(target, &current, Seat::Promoting, case_sensitive)
                    {
                        return Err(DataFusionError::Plan(format!(
                            "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"({} IN ({}))\" \
                             due to data type mismatch: Input to `in` should all be the same \
                             type, but it's [\"{}\", \"{}\"]. SQLSTATE: 42K09",
                            render(&value),
                            list.iter().map(render).collect::<Vec<_>>().join(", "),
                            spark_sql_name(target),
                            spark_sql_name(&current)
                        )));
                    }
                }
            }
            let mut changed = false;
            let list = list
                .into_iter()
                .map(|item| {
                    let (item, renamed) = aligned_operand(target.as_ref(), item, schema);
                    changed |= renamed;
                    item
                })
                .collect();
            let expr = Expr::InList(InList::new(value, list, negated));
            Ok(if changed {
                Transformed::yes(expr)
            } else {
                Transformed::no(expr)
            })
        }
        other => Ok(Transformed::no(other)),
    }
}

fn clashes(target: &DataType, current: &DataType, seat: Seat, case_sensitive: bool) -> bool {
    matches!(
        (target, current),
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len()
    ) && pairing(target, current, seat, case_sensitive) == Pairing::Clash
}

fn aligned_operand(target: Option<&DataType>, operand: Expr, schema: &DFSchema) -> (Expr, bool) {
    let Some(target) = target else {
        return (operand, false);
    };
    let Ok(current) = operand.get_type(schema) else {
        return (operand, false);
    };
    match renamed_type(target, &current) {
        Some(renamed) if renamed != current => (by_position(operand, renamed), true),
        _ => (operand, false),
    }
}

fn renamed_type(target: &DataType, current: &DataType) -> Option<DataType> {
    let (DataType::Struct(target_fields), DataType::Struct(current_fields)) = (target, current)
    else {
        return None;
    };
    if target_fields.len() != current_fields.len() {
        return None;
    }
    let fields = target_fields
        .iter()
        .zip(current_fields.iter())
        .map(|(target_field, current_field)| {
            let data_type = renamed_type(target_field.data_type(), current_field.data_type())
                .unwrap_or_else(|| current_field.data_type().clone());
            Field::new(target_field.name(), data_type, current_field.is_nullable())
                .with_metadata(current_field.metadata().clone())
        })
        .collect::<Vec<_>>();
    Some(DataType::Struct(Fields::from(fields)))
}

fn by_position(operand: Expr, target: DataType) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(StructByPosition::new(target))),
        vec![operand],
    ))
}

fn align_union(union: Union, case_sensitive: bool) -> Result<Transformed<LogicalPlan>> {
    let Some(first) = union.inputs.first().cloned() else {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    };
    let mut changed = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for (position, input) in union.inputs.iter().enumerate() {
        match (position > 0)
            .then(|| renamed_input(first.schema(), input, position, case_sensitive))
            .transpose()?
            .flatten()
        {
            Some(renamed) => {
                changed = true;
                inputs.push(Arc::new(renamed));
            }
            None => inputs.push(Arc::clone(input)),
        }
    }
    let schema = union.schema;
    let plan = LogicalPlan::Union(Union { inputs, schema });
    Ok(if changed {
        Transformed::yes(plan)
    } else {
        Transformed::no(plan)
    })
}

fn ordinal(index: usize) -> String {
    ORDINALS
        .get(index)
        .map_or_else(|| format!("{}th", index + 1), |word| (*word).to_string())
}

fn renamed_input(
    target: &DFSchema,
    input: &Arc<LogicalPlan>,
    position: usize,
    case_sensitive: bool,
) -> Result<Option<LogicalPlan>> {
    let schema = input.schema();
    if schema.fields().len() != target.fields().len() {
        return Ok(None);
    }
    let mut changed = false;
    let mut exprs = Vec::with_capacity(schema.fields().len());
    for (index, ((qualifier, field), target_field)) in
        schema.iter().zip(target.fields().iter()).enumerate()
    {
        if clashes(
            target_field.data_type(),
            field.data_type(),
            Seat::Promoting,
            case_sensitive,
        ) {
            return Err(DataFusionError::Plan(format!(
                "[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible \
                 column types. The {} column of the {} table is \"{}\" type which is not \
                 compatible with \"{}\" at the same column of the first table.. SQLSTATE: 42825",
                ordinal(index),
                ordinal(position),
                spark_sql_name(field.data_type()),
                spark_sql_name(target_field.data_type())
            )));
        }
        let column = Expr::Column(Column::from((qualifier, field)));
        exprs.push(
            match renamed_type(target_field.data_type(), field.data_type()) {
                Some(renamed) if &renamed != field.data_type() => {
                    changed = true;
                    by_position(column, renamed).alias(field.name())
                }
                _ => column,
            },
        );
    }
    if !changed {
        return Ok(None);
    }
    Projection::try_new(exprs, Arc::clone(input))
        .map(LogicalPlan::Projection)
        .map(Some)
}

#[derive(Debug)]
struct StructByPosition {
    target: DataType,
    signature: Signature,
}

impl StructByPosition {
    fn new(target: DataType) -> Self {
        Self {
            target,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for StructByPosition {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl Eq for StructByPosition {}

impl Hash for StructByPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        STRUCT_BY_POSITION.hash(state);
        self.target.hash(state);
    }
}

impl ScalarUDFImpl for StructByPosition {
    fn name(&self) -> &str {
        STRUCT_BY_POSITION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        Ok(args.first().map(ToString::to_string).unwrap_or_default())
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(args
            .first()
            .map(|arg| arg.schema_name().to_string())
            .unwrap_or_default())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "{STRUCT_BY_POSITION} expects one argument"
            )));
        };
        match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(renamed_array(array, &self.target)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = renamed_array(&scalar.to_array()?, &self.target)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }
}

fn renamed_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let DataType::Struct(fields) = target else {
        return Ok(Arc::clone(array));
    };
    let Some(source) = array.as_struct_opt() else {
        return Err(DataFusionError::Execution(format!(
            "{STRUCT_BY_POSITION} expects a struct, got {}",
            array.data_type()
        )));
    };
    let columns = source
        .columns()
        .iter()
        .zip(fields.iter())
        .map(|(column, field)| renamed_array(column, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    let fields: Vec<FieldRef> = fields.iter().cloned().collect();
    Ok(Arc::new(StructArray::try_new(
        Fields::from(fields),
        columns,
        source.nulls().cloned(),
    )?))
}
