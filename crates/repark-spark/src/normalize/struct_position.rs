use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{InList, InSubquery, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, Projection, ScalarUDF, Subquery,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use repark_core::struct_walk::{holds_struct, list_element, peel_encoding};
use repark_functions::cast_map::spark_sql_name;

use super::map_ordering::{refuse_in_expr, render};
use udf::StructByPosition;

mod udf;

const STRUCT_BY_POSITION: &str = "__repark_struct_by_position__";
const PLANNED_BY_POSITION: &str = "__repark_struct_by_position_planned__";
const ELEMENT_AT: &str = "element_at";

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

#[allow(clippy::missing_errors_doc)]
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

#[must_use]
pub fn duplicate_field_operands() -> Arc<dyn ExprPlanner> {
    Arc::new(DuplicateFieldOperands)
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
    let (left, right) = (peel_encoding(left), peel_encoding(right));
    match (left, right) {
        (DataType::Null, _) | (_, DataType::Null) => Pairing::Widen,
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            let named = left_fields
                .iter()
                .zip(right_fields.iter())
                .all(|(l, r)| same_name(l.name(), r.name(), case_sensitive));
            fields_pairing(left_fields, right_fields, named, seat, case_sensitive)
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            match (left_entries.data_type(), right_entries.data_type()) {
                (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
                    fields_pairing(left_fields, right_fields, true, seat, case_sensitive)
                }
                _ => Pairing::Clash,
            }
        }
        (DataType::FixedSizeList(_, left_size), DataType::FixedSizeList(_, right_size))
            if left_size != right_size =>
        {
            Pairing::Clash
        }
        _ => match (list_element(left), list_element(right)) {
            (Some(l), Some(r)) => pairing(l.data_type(), r.data_type(), seat, case_sensitive),
            _ if holds_struct(left) || holds_struct(right) => Pairing::Clash,
            _ if spark_sql_name(left) == spark_sql_name(right) => Pairing::Same,
            _ if widens(left, right, seat) => Pairing::Widen,
            _ => Pairing::Clash,
        },
    }
}

fn fields_pairing(
    left_fields: &Fields,
    right_fields: &Fields,
    named: bool,
    seat: Seat,
    case_sensitive: bool,
) -> Pairing {
    if left_fields.len() != right_fields.len() {
        return Pairing::Clash;
    }
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

fn widens(left: &DataType, right: &DataType, seat: Seat) -> bool {
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
    match (left, right) {
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
            _ => left.is_temporal() && right.is_temporal(),
        },
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn same_name(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

fn struct_pair(left: &DataType, right: &DataType) -> bool {
    left != right && holds_struct(left) && holds_struct(right)
}

fn clashes(target: &DataType, current: &DataType, seat: Seat, case_sensitive: bool) -> bool {
    struct_pair(target, current) && pairing(target, current, seat, case_sensitive) == Pairing::Clash
}

fn align_node(node: LogicalPlan, case_sensitive: bool) -> Result<Transformed<LogicalPlan>> {
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
        expr.transform_up(|leaf| {
            refuse_in_expr(&leaf, &schema)?;
            align_expr(leaf, &schema, case_sensitive)
        })
        .map(|aligned| aligned.update_data(|expr| name.restore(expr)))
    })
}

fn has_struct_candidate(node: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = node.apply_expressions(|expr| {
        found = expr
            .exists(|leaf| {
                Ok(matches!(
                    leaf,
                    Expr::BinaryExpr(BinaryExpr { op, .. }) if is_comparison(*op)
                ) || matches!(leaf, Expr::InList(_) | Expr::InSubquery(_))
                    || matches!(leaf, Expr::ScalarFunction(function) if function.func.name() == ELEMENT_AT))
            })
            .unwrap_or(false);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
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

fn changed<T>(value: T, changed: bool) -> Transformed<T> {
    if changed {
        Transformed::yes(value)
    } else {
        Transformed::no(value)
    }
}

fn align_expr(expr: Expr, schema: &DFSchema, case_sensitive: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let (left, left_planned) = planned_operand(*left);
            let (right, right_planned) = planned_operand(*right);
            if left_planned || right_planned {
                return positional_comparison(left, op, right, schema, case_sensitive);
            }
            let (Ok(target), Ok(current)) = (left.get_type(schema), read_type(&right, schema))
            else {
                return Ok(Transformed::no(binary(left, op, right)));
            };
            if !struct_pair(&target, &current) {
                return Ok(Transformed::no(binary(left, op, right)));
            }
            if clashes(&target, &current, Seat::Comparison, case_sensitive) {
                return Err(binary_op_diff_types(&left, op, &right, &target, &current));
            }
            if has_duplicate_names(&target) {
                return positional_comparison(left, op, right, schema, case_sensitive);
            }
            let (right, renamed) = aligned_operand(&target, right, schema)?;
            Ok(changed(binary(left, op, right), renamed))
        }
        Expr::InList(InList {
            expr: value,
            list,
            negated,
        }) => align_in_list(*value, list, negated, schema, case_sensitive),
        Expr::InSubquery(in_subquery) => align_in_subquery(in_subquery, schema, case_sensitive),
        Expr::ScalarFunction(function)
            if function.func.name() == ELEMENT_AT && function.args.len() == 2 =>
        {
            refuse_reordered_map_key(&function, schema, case_sensitive)?;
            Ok(Transformed::no(Expr::ScalarFunction(function)))
        }
        other => Ok(Transformed::no(other)),
    }
}

fn names_match(left: &DataType, right: &DataType, case_sensitive: bool) -> bool {
    match (peel_encoding(left), peel_encoding(right)) {
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            left_fields.len() == right_fields.len()
                && left_fields.iter().zip(right_fields.iter()).all(|(l, r)| {
                    same_name(l.name(), r.name(), case_sensitive)
                        && names_match(l.data_type(), r.data_type(), case_sensitive)
                })
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            match (left_entries.data_type(), right_entries.data_type()) {
                (DataType::Struct(l), DataType::Struct(r)) => {
                    l.len() == r.len()
                        && l.iter()
                            .zip(r.iter())
                            .all(|(l, r)| names_match(l.data_type(), r.data_type(), case_sensitive))
                }
                _ => true,
            }
        }
        (left, right) => match (list_element(left), list_element(right)) {
            (Some(l), Some(r)) => names_match(l.data_type(), r.data_type(), case_sensitive),
            _ => true,
        },
    }
}

fn refuse_reordered_map_key(
    function: &ScalarFunction,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<()> {
    let (Ok(container), Ok(key)) = (
        function.args[0].get_type(schema),
        function.args[1].get_type(schema),
    ) else {
        return Ok(());
    };
    let DataType::Map(entries, _) = peel_encoding(&container) else {
        return Ok(());
    };
    let DataType::Struct(entry_fields) = entries.data_type() else {
        return Ok(());
    };
    let Some(map_key) = entry_fields.first().map(|field| field.data_type()) else {
        return Ok(());
    };
    if !struct_pair(map_key, &key) || names_match(map_key, &key, case_sensitive) {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.MAP_FUNCTION_DIFF_TYPES] Cannot resolve \"element_at({}, {})\" due \
         to data type mismatch: Input to `element_at` should have been \"MAP\" followed by a \
         value with same key type, but it's [\"{}\", \"{}\"]. SQLSTATE: 42K09",
        render(&function.args[0]),
        render(&function.args[1]),
        spark_sql_name(&container),
        spark_sql_name(&key)
    )))
}

fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

fn align_in_list(
    value: Expr,
    list: Vec<Expr>,
    negated: bool,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Transformed<Expr>> {
    let Ok(target) = value.get_type(schema) else {
        return Ok(Transformed::no(Expr::InList(InList::new(
            Box::new(value),
            list,
            negated,
        ))));
    };
    let mut paired = false;
    for item in &list {
        let Ok(current) = read_type(item, schema) else {
            continue;
        };
        if clashes(&target, &current, Seat::Promoting, case_sensitive) {
            return Err(data_diff_types(&value, &list, &target, &current));
        }
        paired |= struct_pair(&target, &current);
    }
    if !paired {
        return Ok(Transformed::no(Expr::InList(InList::new(
            Box::new(value),
            list,
            negated,
        ))));
    }
    let (value, target, value_moved) = if has_duplicate_names(&target) {
        let positional = positional_type(&target);
        (by_position(value, positional.clone()), positional, true)
    } else {
        (value, target, false)
    };
    let mut renamed = value_moved;
    let mut items = Vec::with_capacity(list.len());
    for item in list {
        let (item, moved) = aligned_operand(&target, item, schema)?;
        renamed |= moved;
        items.push(item);
    }
    Ok(changed(
        Expr::InList(InList::new(Box::new(value), items, negated)),
        renamed,
    ))
}

fn data_diff_types(
    value: &Expr,
    list: &[Expr],
    target: &DataType,
    current: &DataType,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"({} IN ({}))\" due to data type \
         mismatch: Input to `in` should all be the same type, but it's [\"{}\", \"{}\"]. \
         SQLSTATE: 42K09",
        render(value),
        list.iter().map(render).collect::<Vec<_>>().join(", "),
        spark_sql_name(target),
        spark_sql_name(current)
    ))
}

fn align_in_subquery(
    in_subquery: InSubquery,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Transformed<Expr>> {
    let Ok(left) = in_subquery.expr.get_type(schema) else {
        return Ok(Transformed::no(Expr::InSubquery(in_subquery)));
    };
    let output = Arc::clone(&in_subquery.subquery.subquery);
    let Some((qualifier, field)) = output.schema().iter().next() else {
        return Ok(Transformed::no(Expr::InSubquery(in_subquery)));
    };
    let right = field.data_type().clone();
    if !struct_pair(&left, &right) {
        return Ok(Transformed::no(Expr::InSubquery(in_subquery)));
    }
    if clashes(&left, &right, Seat::Promoting, case_sensitive) {
        return Err(in_subquery_mismatch(&in_subquery.expr, &left, &right));
    }
    let InSubquery {
        expr,
        subquery,
        negated,
    } = in_subquery;
    let (expr, target) = if has_duplicate_names(&left) {
        let positional = positional_type(&left);
        (by_position(*expr, positional.clone()), positional)
    } else {
        (*expr, left.clone())
    };
    let Some(renamed) = renamed_type(&target, &right) else {
        return Err(in_subquery_mismatch(&expr, &left, &right));
    };
    let column = Expr::Column(Column::from((qualifier, field)));
    let mut exprs = vec![by_position(column, renamed).alias(field.name())];
    exprs.extend(
        output
            .schema()
            .iter()
            .skip(1)
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field)))),
    );
    let projected = Projection::try_new(exprs, output).map(LogicalPlan::Projection)?;
    Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
        Box::new(expr),
        Subquery {
            subquery: Arc::new(projected),
            outer_ref_columns: subquery.outer_ref_columns,
            spans: subquery.spans,
        },
        negated,
    ))))
}

fn in_subquery_mismatch(value: &Expr, left: &DataType, right: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.IN_SUBQUERY_DATA_TYPE_MISMATCH] Cannot resolve \"({} IN \
         (listquery()))\" due to data type mismatch: The data type of one or more elements in \
         the left hand side of an IN subquery is not compatible with the data type of the output \
         of the subquery. Mismatched columns: [({}:{}, {}:{})], Left side: [{}], Right side: \
         [{}]. SQLSTATE: 42K09",
        render(value),
        render(value),
        spark_sql_name(left),
        render(value),
        spark_sql_name(right),
        spark_sql_name(left),
        spark_sql_name(right)
    ))
}

fn binary_op_diff_types(
    left: &Expr,
    op: Operator,
    right: &Expr,
    left_type: &DataType,
    right_type: &DataType,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve \"({} {} {})\" due to data type \
         mismatch: the left and right operands of the binary operator have incompatible types \
         (\"{}\" and \"{}\"). SQLSTATE: 42K09",
        render(left),
        spark_operator(op),
        render(right),
        spark_sql_name(left_type),
        spark_sql_name(right_type)
    ))
}

fn beneath_cast(expr: &Expr) -> Option<&Expr> {
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    match inner {
        Expr::Cast(cast) => Some(cast.expr.as_ref()),
        Expr::TryCast(cast) => Some(cast.expr.as_ref()),
        _ => None,
    }
}

fn cast_read(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    let inner = beneath_cast(expr)?.get_type(schema).ok()?;
    (holds_struct(&inner) && expr.get_type(schema).ok()? != inner).then_some(inner)
}

fn read_type(expr: &Expr, schema: &DFSchema) -> Result<DataType> {
    match cast_read(expr, schema) {
        Some(inner) => Ok(inner),
        None => expr.get_type(schema),
    }
}

fn aligned_operand(target: &DataType, operand: Expr, schema: &DFSchema) -> Result<(Expr, bool)> {
    let Ok(current) = read_type(&operand, schema) else {
        return Ok((operand, false));
    };
    if !struct_pair(target, &current) {
        return Ok((operand, false));
    }
    let Some(renamed) = renamed_type(target, &current) else {
        return Err(DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] RePark cannot pair the struct fields of \
             \"{}\" with \"{}\" by position. SQLSTATE: 42K09",
            spark_sql_name(target),
            spark_sql_name(&current)
        )));
    };
    if renamed == current {
        return Ok((operand, false));
    }
    if cast_read(&operand, schema).is_some() {
        return Ok((renamed_beneath(operand, renamed), true));
    }
    if let Some(field_read) = renamed_beneath_map_field(&operand, &renamed, schema) {
        return Ok((field_read, true));
    }
    Ok((by_position(operand, renamed), true))
}

fn renamed_beneath_map_field(
    operand: &Expr,
    renamed: &DataType,
    schema: &DFSchema,
) -> Option<Expr> {
    let Expr::ScalarFunction(function) = operand else {
        return None;
    };
    let [base, key @ Expr::Literal(..)] = function.args.as_slice() else {
        return None;
    };
    if function.func.name() != "get_field" {
        return None;
    }
    let DataType::Map(entries, sorted) = base.get_type(schema).ok()? else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    let [map_key, value] = &fields[..] else {
        return None;
    };
    let renamed_fields = Fields::from(vec![Arc::clone(map_key), with_type(value, renamed.clone())]);
    let renamed_entries = Field::new(
        entries.name(),
        DataType::Struct(renamed_fields),
        entries.is_nullable(),
    );
    let base = by_position(
        base.clone(),
        DataType::Map(Arc::new(renamed_entries), sorted),
    );
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&function.func),
        vec![base, key.clone()],
    )))
}

fn renamed_beneath(operand: Expr, renamed: DataType) -> Expr {
    let (alias, inner) = match operand {
        Expr::Alias(alias) => (Some((alias.relation, alias.name)), *alias.expr),
        other => (None, other),
    };
    let rebuilt = match inner {
        Expr::Cast(cast) if cast.field.data_type() == &renamed => by_position(*cast.expr, renamed),
        Expr::TryCast(cast) if cast.field.data_type() == &renamed => {
            by_position(*cast.expr, renamed)
        }
        Expr::Cast(mut cast) => {
            cast.expr = Box::new(by_position(*cast.expr, renamed));
            Expr::Cast(cast)
        }
        Expr::TryCast(mut cast) => {
            cast.expr = Box::new(by_position(*cast.expr, renamed));
            Expr::TryCast(cast)
        }
        other => other,
    };
    match alias {
        Some((relation, name)) => rebuilt.alias_qualified(relation, name),
        None => rebuilt,
    }
}

fn renamed_field(target: &DataType, current: &FieldRef, name: &str) -> FieldRef {
    let data_type =
        renamed_type(target, current.data_type()).unwrap_or_else(|| current.data_type().clone());
    Arc::new(
        Field::new(name, data_type, current.is_nullable())
            .with_metadata(current.metadata().clone()),
    )
}

fn renamed_type(target: &DataType, current: &DataType) -> Option<DataType> {
    match current {
        DataType::Dictionary(key, value) => {
            return renamed_type(target, value)
                .map(|value| DataType::Dictionary(key.clone(), Box::new(value)));
        }
        DataType::RunEndEncoded(run_ends, values) => {
            let renamed = renamed_type(target, values.data_type())?;
            return Some(DataType::RunEndEncoded(
                Arc::clone(run_ends),
                Arc::new(values.as_ref().clone().with_data_type(renamed)),
            ));
        }
        _ => {}
    }
    let target = peel_encoding(target);
    match (target, current) {
        (DataType::Struct(target_fields), DataType::Struct(current_fields)) => {
            if target_fields.len() != current_fields.len() {
                return None;
            }
            let fields = target_fields
                .iter()
                .zip(current_fields.iter())
                .map(|(t, c)| renamed_field(t.data_type(), c, t.name()))
                .collect::<Vec<_>>();
            Some(DataType::Struct(Fields::from(fields)))
        }
        (DataType::Map(target_entries, _), DataType::Map(entries, sorted)) => {
            let (DataType::Struct(target_fields), DataType::Struct(fields)) =
                (target_entries.data_type(), entries.data_type())
            else {
                return None;
            };
            if target_fields.len() != fields.len() {
                return None;
            }
            let fields = target_fields
                .iter()
                .zip(fields.iter())
                .map(|(t, c)| renamed_field(t.data_type(), c, c.name()))
                .collect::<Vec<_>>();
            let entries = Field::new(
                entries.name(),
                DataType::Struct(Fields::from(fields)),
                entries.is_nullable(),
            );
            Some(DataType::Map(Arc::new(entries), *sorted))
        }
        _ => {
            let target = list_element(target)?;
            let element = list_element(current)?;
            renamed_type(target.data_type(), element.data_type())?;
            let element = renamed_field(target.data_type(), element, element.name());
            Some(match current {
                DataType::List(_) => DataType::List(element),
                DataType::LargeList(_) => DataType::LargeList(element),
                DataType::FixedSizeList(_, size) => DataType::FixedSizeList(element, *size),
                DataType::ListView(_) => DataType::ListView(element),
                _ => DataType::LargeListView(element),
            })
        }
    }
}

fn with_type(field: &FieldRef, data_type: DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

fn positional_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    Arc::new(
                        Field::new(
                            format!("_{}", index + 1),
                            positional_type(field.data_type()),
                            field.is_nullable(),
                        )
                        .with_metadata(field.metadata().clone()),
                    )
                })
                .collect::<Vec<FieldRef>>(),
        )),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(positional_type(value)))
        }
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            Arc::clone(run_ends),
            with_type(values, positional_type(values.data_type())),
        ),
        DataType::Map(entries, sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return data_type.clone();
            };
            let fields = fields
                .iter()
                .map(|field| with_type(field, positional_type(field.data_type())))
                .collect::<Vec<FieldRef>>();
            DataType::Map(
                with_type(entries, DataType::Struct(Fields::from(fields))),
                *sorted,
            )
        }
        DataType::List(field) => {
            DataType::List(with_type(field, positional_type(field.data_type())))
        }
        DataType::LargeList(field) => {
            DataType::LargeList(with_type(field, positional_type(field.data_type())))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(with_type(field, positional_type(field.data_type())), *size)
        }
        DataType::ListView(field) => {
            DataType::ListView(with_type(field, positional_type(field.data_type())))
        }
        DataType::LargeListView(field) => {
            DataType::LargeListView(with_type(field, positional_type(field.data_type())))
        }
        other => other.clone(),
    }
}

fn has_duplicate_names(data_type: &DataType) -> bool {
    match peel_encoding(data_type) {
        DataType::Struct(fields) => {
            let mut names: Vec<&str> = fields.iter().map(|field| field.name().as_str()).collect();
            names.sort_unstable();
            names.windows(2).any(|pair| pair[0] == pair[1])
                || fields
                    .iter()
                    .any(|field| has_duplicate_names(field.data_type()))
        }
        DataType::Map(entries, _) => has_duplicate_names(entries.data_type()),
        peeled => list_element(peeled).is_some_and(|field| has_duplicate_names(field.data_type())),
    }
}

fn by_position(operand: Expr, target: DataType) -> Expr {
    by_position_named(STRUCT_BY_POSITION, operand, target)
}

fn by_position_named(name: &'static str, operand: Expr, target: DataType) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(StructByPosition::new(
            name, target,
        ))),
        vec![operand],
    ))
}

fn planned_operand(expr: Expr) -> (Expr, bool) {
    match expr {
        Expr::ScalarFunction(mut function)
            if function.func.name() == PLANNED_BY_POSITION && function.args.len() == 1 =>
        {
            (function.args.remove(0), true)
        }
        other => (other, false),
    }
}

fn positional_comparison(
    left: Expr,
    op: Operator,
    right: Expr,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Transformed<Expr>> {
    let left_type = left.get_type(schema)?;
    let right_type = right.get_type(schema)?;
    if clashes(&left_type, &right_type, Seat::Comparison, case_sensitive) {
        return Err(binary_op_diff_types(
            &left,
            op,
            &right,
            &left_type,
            &right_type,
        ));
    }
    Ok(Transformed::yes(binary(
        by_position(left, positional_type(&left_type)),
        op,
        by_position(right, positional_type(&right_type)),
    )))
}

#[derive(Debug, Default)]
pub struct DuplicateFieldOperands;

impl ExprPlanner for DuplicateFieldOperands {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let Some(op) = comparison_operator(&expr.op) else {
            return Ok(PlannerResult::Original(expr));
        };
        let (Ok(left_type), Ok(right_type)) =
            (expr.left.get_type(schema), expr.right.get_type(schema))
        else {
            return Ok(PlannerResult::Original(expr));
        };
        if !struct_pair(&left_type, &right_type)
            || !(has_duplicate_names(&left_type) || has_duplicate_names(&right_type))
        {
            return Ok(PlannerResult::Original(expr));
        }
        let left_positional = positional_type(&left_type);
        let right_positional = positional_type(&right_type);
        if comparison_coercion(&left_positional, &right_positional).is_none() {
            return Err(binary_op_diff_types(
                &expr.left,
                op,
                &expr.right,
                &left_type,
                &right_type,
            ));
        }
        Ok(PlannerResult::Planned(binary(
            by_position_named(PLANNED_BY_POSITION, expr.left, left_positional),
            op,
            by_position_named(PLANNED_BY_POSITION, expr.right, right_positional),
        )))
    }
}

fn comparison_operator(op: &BinaryOperator) -> Option<Operator> {
    match op {
        BinaryOperator::Eq => Some(Operator::Eq),
        BinaryOperator::NotEq => Some(Operator::NotEq),
        BinaryOperator::Lt => Some(Operator::Lt),
        BinaryOperator::LtEq => Some(Operator::LtEq),
        BinaryOperator::Gt => Some(Operator::Gt),
        BinaryOperator::GtEq => Some(Operator::GtEq),
        BinaryOperator::Spaceship => Some(Operator::IsNotDistinctFrom),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
