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

const STRUCT_BY_POSITION: &str = "__repark_struct_by_position__";

#[derive(Debug, Default)]
pub struct StructOperandsByPosition;

impl AnalyzerRule for StructOperandsByPosition {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(align_node).data()
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

fn align_node(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Union(union) = node {
        return align_union(union);
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
        expr.transform_up(|leaf| align_expr(leaf, &schema))
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

fn align_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let target = left.get_type(schema).ok();
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
    match (target, current) {
        (DataType::Struct(target_fields), DataType::Struct(current_fields))
            if target_fields.len() == current_fields.len() =>
        {
            let fields = target_fields
                .iter()
                .zip(current_fields.iter())
                .map(|(target_field, current_field)| {
                    renamed_type(target_field.data_type(), current_field.data_type()).map(
                        |data_type| {
                            Field::new(target_field.name(), data_type, current_field.is_nullable())
                                .with_metadata(current_field.metadata().clone())
                        },
                    )
                })
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Struct(Fields::from(fields)))
        }
        (DataType::Struct(_), _) | (_, DataType::Struct(_)) => None,
        _ if target == current => Some(current.clone()),
        _ => None,
    }
}

fn by_position(operand: Expr, target: DataType) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(StructByPosition::new(target))),
        vec![operand],
    ))
}

fn align_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let Some(first) = union.inputs.first().cloned() else {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    };
    let mut changed = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for (position, input) in union.inputs.iter().enumerate() {
        match (position > 0)
            .then(|| renamed_input(first.schema(), input))
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

fn renamed_input(target: &DFSchema, input: &Arc<LogicalPlan>) -> Result<Option<LogicalPlan>> {
    let schema = input.schema();
    if schema.fields().len() != target.fields().len() {
        return Ok(None);
    }
    let mut changed = false;
    let exprs = schema
        .iter()
        .zip(target.fields().iter())
        .map(|((qualifier, field), target_field)| {
            let column = Expr::Column(Column::from((qualifier, field)));
            match renamed_type(target_field.data_type(), field.data_type()) {
                Some(renamed) if &renamed != field.data_type() => {
                    changed = true;
                    by_position(column, renamed).alias(field.name())
                }
                _ => column,
            }
        })
        .collect::<Vec<_>>();
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
