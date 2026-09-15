mod bigint;
mod dtoa;
mod format_float;
#[cfg(test)]
mod tables_doubles;
#[cfg(test)]
mod tables_floats;
#[cfg(test)]
mod tests_corpus;

pub(crate) use dtoa::{java_double_strings, java_float_strings, with_java_double_text};
pub use dtoa::{
    java_double_text, java_double_text_len, java_float_text, java_float_text_len,
    with_java_float_text,
};

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::{Like, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Case, Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

pub(crate) fn spark_float_to_string_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFloatToString::new()))
}

#[derive(Debug)]
struct SparkFloatToString {
    signature: Signature,
}

impl SparkFloatToString {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFloatToString {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFloatToString {}

impl Hash for SparkFloatToString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkFloatToString {
    crate::shim_udf_boilerplate!("__repark_float_to_string__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Float32 | DataType::Float64) => Ok(DataType::Utf8),
            _ => Err(DataFusionError::Plan(format!(
                "'{}' expects a FLOAT or DOUBLE argument",
                self.name()
            ))),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(arg) = arrays.into_iter().next() else {
            return exec_err!("'{}' expects 1 argument", self.name());
        };
        match arg.data_type() {
            DataType::Float64 => {
                let values = arg.as_primitive::<Float64Type>();
                Ok(ColumnarValue::Array(Arc::new(java_double_strings(values))))
            }
            DataType::Float32 => {
                let values = arg.as_primitive::<Float32Type>();
                Ok(ColumnarValue::Array(Arc::new(java_float_strings(values))))
            }
            other => Err(DataFusionError::Plan(format!(
                "'{}' expects a FLOAT or DOUBLE argument, got {other}",
                self.name()
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SparkFloatStringify;

impl AnalyzerRule for SparkFloatStringify {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi = crate::ansi::spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_float_plan(node, ansi))
            .data()
    }

    fn name(&self) -> &'static str {
        "spark_float_stringify"
    }
}

fn rewrite_float_plan(plan: LogicalPlan, ansi: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let literals = float_literal_inputs(&plan);
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| {
            let resolved = resolve_float_literal_input(node, &literals, &schema);
            rewrite_float_expr(resolved.data, &schema, ansi)
        })?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn float_literal_inputs(plan: &LogicalPlan) -> Vec<(String, ScalarValue)> {
    let inputs = plan.inputs();
    if inputs.len() != 1 {
        return Vec::new();
    }
    let mut inner = inputs[0];
    if let LogicalPlan::SubqueryAlias(alias) = inner {
        inner = alias.input.as_ref();
    }
    let LogicalPlan::Projection(projection) = inner else {
        return Vec::new();
    };
    let mut literals = Vec::new();
    for expr in &projection.expr {
        if let Expr::Alias(alias) = expr
            && let Expr::Literal(literal, _) = alias.expr.as_ref()
            && utf8_literal_text(literal).is_some()
        {
            if literals.iter().any(|(name, _)| name == &alias.name) {
                literals.retain(|(name, _)| name != &alias.name);
            } else {
                literals.push((alias.name.clone(), literal.clone()));
            }
        }
    }
    literals
}

fn resolve_float_literal_input(
    expr: Expr,
    literals: &[(String, ScalarValue)],
    schema: &DFSchema,
) -> Transformed<Expr> {
    let (target, inner) = match &expr {
        Expr::Cast(cast) => (cast.field.data_type(), cast.expr.as_ref()),
        Expr::TryCast(try_cast) => (try_cast.field.data_type(), try_cast.expr.as_ref()),
        _ => return Transformed::no(expr),
    };
    if !matches!(target, DataType::Float32 | DataType::Float64) {
        return Transformed::no(expr);
    }
    let Ok(source) = inner.get_type(schema) else {
        return Transformed::no(expr);
    };
    if !is_string_type(&source) {
        return Transformed::no(expr);
    }
    let column = match inner {
        Expr::Column(column) => column,
        Expr::ScalarFunction(function)
            if function.func.name() == crate::decimal_cast::DECIMAL_CAST_NULLABLE_NAME
                && let [argument] = function.args.as_slice()
                && let Expr::Column(column) = argument =>
        {
            column
        }
        _ => return Transformed::no(expr),
    };
    let mut found = None;
    for (name, literal) in literals {
        if name == &column.name {
            if found.is_some() {
                return Transformed::no(expr);
            }
            found = Some(literal);
        }
    }
    let Some(literal) = found else {
        return Transformed::no(expr);
    };
    let literal = Expr::Literal(literal.clone(), None);
    match expr {
        Expr::Cast(cast) => Transformed::yes(Expr::Cast(Cast::new(
            Box::new(literal),
            cast.field.data_type().clone(),
        ))),
        Expr::TryCast(try_cast) => Transformed::yes(Expr::TryCast(TryCast::new(
            Box::new(literal),
            try_cast.field.data_type().clone(),
        ))),
        _ => Transformed::no(expr),
    }
}

fn rewrite_float_expr(expr: Expr, schema: &DFSchema, ansi: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::Cast(cast) => rewrite_float_cast(cast, schema, ansi),
        Expr::TryCast(try_cast) => {
            let Ok(source_type) = try_cast.expr.get_type(schema) else {
                return Ok(Transformed::no(Expr::TryCast(try_cast)));
            };
            if is_string_type(&source_type)
                && matches!(
                    try_cast.field.data_type(),
                    DataType::Float32 | DataType::Float64
                )
                && let Expr::Literal(literal, _) = try_cast.expr.as_ref()
                && let Some(text) = utf8_literal_text(literal)
            {
                let target = try_cast.field.data_type().clone();
                match fold_utf8_to_float_literal(literal, &text, &target, true) {
                    Ok(Some(folded)) => return Ok(Transformed::yes(folded)),
                    Ok(None) => return Ok(Transformed::no(Expr::TryCast(try_cast))),
                    Err(_) => {
                        return Ok(Transformed::yes(float_null_literal(&target)));
                    }
                }
            }
            if !matches!(source_type, DataType::Float32 | DataType::Float64) {
                return Ok(Transformed::no(Expr::TryCast(try_cast)));
            }
            if !matches!(
                try_cast.field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Ok(Transformed::no(Expr::TryCast(try_cast)));
            }
            Ok(Transformed::yes(float_to_string_call(*try_cast.expr)))
        }
        Expr::Like(like) => Ok(rewrite_float_like(like, schema)),
        Expr::Case(case) => rewrite_float_case(case, schema, ansi),
        Expr::ScalarFunction(function) => rewrite_float_call(function, schema, ansi),
        other => Ok(Transformed::no(other)),
    }
}

fn float_to_string_call(arg: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_float_to_string_udf(),
        vec![arg],
    ))
}

fn is_float_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn rewrite_float_cast(cast: Cast, schema: &DFSchema, ansi: bool) -> Result<Transformed<Expr>> {
    let Ok(source_type) = cast.expr.get_type(schema) else {
        return Ok(Transformed::no(Expr::Cast(cast)));
    };
    if is_float_type(&source_type) && is_string_type(cast.field.data_type()) {
        return Ok(Transformed::yes(float_to_string_call(*cast.expr)));
    }
    if matches!(
        cast.field.data_type(),
        DataType::Float32 | DataType::Float64
    ) {
        let target = cast.field.data_type().clone();
        if let Expr::Literal(literal, _) = cast.expr.as_ref()
            && let Some(text) = utf8_literal_text(literal)
            && let Some(folded) = fold_utf8_to_float_literal(literal, &text, &target, ansi)?
        {
            return Ok(Transformed::yes(folded));
        }
    }
    Ok(Transformed::no(Expr::Cast(cast)))
}

fn utf8_literal_text(literal: &ScalarValue) -> Option<String> {
    match literal {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => value.clone(),
        ScalarValue::Utf8View(value) => value.as_deref().map(str::to_owned),
        _ => None,
    }
}

fn float_null_literal(target: &DataType) -> Expr {
    match target {
        DataType::Float32 => Expr::Literal(ScalarValue::Float32(None), None),
        _ => Expr::Literal(ScalarValue::Float64(None), None),
    }
}

fn spark_float_type_name(target: &DataType) -> &'static str {
    match target {
        DataType::Float32 => "FLOAT",
        _ => "DOUBLE",
    }
}

fn parse_float_text(trimmed: &str, target: &DataType) -> Option<ScalarValue> {
    match target {
        DataType::Float32 => trimmed
            .parse::<f32>()
            .ok()
            .map(|value| ScalarValue::Float32(Some(value))),
        _ => trimmed
            .parse::<f64>()
            .ok()
            .map(|value| ScalarValue::Float64(Some(value))),
    }
}

fn fold_utf8_to_float_literal(
    literal: &ScalarValue,
    text: &str,
    target: &DataType,
    ansi: bool,
) -> Result<Option<Expr>> {
    if literal.cast_to(target).is_ok() {
        return Ok(None);
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Some(float_null_literal(target)));
    }
    if let Some(value) = parse_float_text(trimmed, target) {
        return Ok(Some(Expr::Literal(value, None)));
    }
    if let Some(stem) = trimmed.strip_suffix(|cell| matches!(cell, 'd' | 'D' | 'f' | 'F'))
        && let Some(value) = parse_float_text(stem, target)
    {
        return Ok(Some(Expr::Literal(value, None)));
    }
    if !ansi {
        return Ok(Some(float_null_literal(target)));
    }
    Err(DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{text}' of the type \"STRING\" cannot be cast to \"{}\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018",
        spark_float_type_name(target)
    )))
}

fn rewrite_float_like(like: Like, schema: &DFSchema) -> Transformed<Expr> {
    let Ok(left_type) = like.expr.get_type(schema) else {
        return Transformed::no(Expr::Like(like));
    };
    let Ok(right_type) = like.pattern.get_type(schema) else {
        return Transformed::no(Expr::Like(like));
    };
    let left_float = is_float_type(&left_type);
    let right_float = is_float_type(&right_type);
    if !left_float && !right_float {
        return Transformed::no(Expr::Like(like));
    }
    if (left_float && !is_string_type(&right_type)) || (right_float && !is_string_type(&left_type))
    {
        return Transformed::no(Expr::Like(like));
    }
    let Like {
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    } = like;
    let expr = if left_float {
        Box::new(float_to_string_call(*expr))
    } else {
        expr
    };
    let pattern = if right_float {
        Box::new(float_to_string_call(*pattern))
    } else {
        pattern
    };
    Transformed::yes(Expr::Like(Like::new(
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    )))
}

fn rewrite_float_case(case: Case, schema: &DFSchema, ansi: bool) -> Result<Transformed<Expr>> {
    let mut target: Option<DataType> = None;
    for (_, then) in &case.when_then_expr {
        if let Ok(data_type) = then.get_type(schema)
            && is_float_type(&data_type)
            && !matches!(target, Some(DataType::Float64))
        {
            target = Some(data_type);
        }
    }
    if let Some(else_expr) = &case.else_expr
        && let Ok(data_type) = else_expr.get_type(schema)
        && is_float_type(&data_type)
        && !matches!(target, Some(DataType::Float64))
    {
        target = Some(data_type);
    }
    let Some(target) = target else {
        return Ok(Transformed::no(Expr::Case(case)));
    };
    let mut has_string = false;
    for (_, then) in &case.when_then_expr {
        if let Ok(data_type) = then.get_type(schema)
            && is_string_type(&data_type)
        {
            has_string = true;
        }
    }
    if let Some(else_expr) = &case.else_expr
        && let Ok(data_type) = else_expr.get_type(schema)
        && is_string_type(&data_type)
    {
        has_string = true;
    }
    if !has_string {
        return Ok(Transformed::no(Expr::Case(case)));
    }
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let mut new_whens = Vec::with_capacity(when_then_expr.len());
    for (when, then) in when_then_expr {
        if let Ok(data_type) = then.get_type(schema)
            && is_string_type(&data_type)
        {
            new_whens.push((
                when,
                Box::new(string_branch_to_float(*then, &target, ansi)?),
            ));
        } else {
            new_whens.push((when, then));
        }
    }
    let new_else = match else_expr {
        Some(else_expr)
            if matches!(
                else_expr.get_type(schema).as_ref(),
                Ok(data_type) if is_string_type(data_type)
            ) =>
        {
            Some(Box::new(string_branch_to_float(*else_expr, &target, ansi)?))
        }
        other => other,
    };
    Ok(Transformed::yes(Expr::Case(Case {
        expr,
        when_then_expr: new_whens,
        else_expr: new_else,
    })))
}

fn string_branch_to_float(branch: Expr, target: &DataType, ansi: bool) -> Result<Expr> {
    if let Expr::Literal(literal, _) = &branch
        && let Some(text) = utf8_literal_text(literal)
        && let Some(folded) = fold_utf8_to_float_literal(literal, &text, target, ansi)?
    {
        return Ok(folded);
    }
    Ok(Expr::Cast(Cast::new(Box::new(branch), target.clone())))
}

fn rewrite_float_call(
    function: ScalarFunction,
    schema: &DFSchema,
    ansi: bool,
) -> Result<Transformed<Expr>> {
    let name = function.func.name();
    if name == "format_string" || name == "printf" {
        return Ok(rewrite_format_string_args(function, schema));
    }
    if name == "array_to_string"
        || name == "array_join"
        || name == "list_join"
        || name == "list_to_string"
    {
        return Ok(rewrite_array_join_arg(function, schema));
    }
    if name == "coalesce" {
        return rewrite_coalesce_args(function, schema, ansi);
    }
    Ok(Transformed::no(Expr::ScalarFunction(function)))
}

fn rewrite_coalesce_args(
    function: ScalarFunction,
    schema: &DFSchema,
    ansi: bool,
) -> Result<Transformed<Expr>> {
    let mut target: Option<DataType> = None;
    for arg in &function.args {
        if let Ok(data_type) = arg.get_type(schema)
            && is_float_type(&data_type)
            && !matches!(target, Some(DataType::Float64))
        {
            target = Some(data_type);
        }
    }
    let Some(target) = target else {
        return Ok(Transformed::no(Expr::ScalarFunction(function)));
    };
    let mut rewritten = function;
    let mut changed = false;
    for index in 0..rewritten.args.len() {
        if let Expr::Literal(literal, _) = &rewritten.args[index]
            && let Some(text) = utf8_literal_text(literal)
            && let Some(folded) = fold_utf8_to_float_literal(literal, &text, &target, ansi)?
        {
            rewritten.args[index] = folded;
            changed = true;
        }
    }
    if changed {
        Ok(Transformed::yes(Expr::ScalarFunction(rewritten)))
    } else {
        Ok(Transformed::no(Expr::ScalarFunction(rewritten)))
    }
}

fn rewrite_format_string_args(function: ScalarFunction, schema: &DFSchema) -> Transformed<Expr> {
    if function.args.len() < 2 {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let Some(fmt) = scalar_string(&function.args[0]) else {
        return Transformed::no(Expr::ScalarFunction(function));
    };
    if function.args.len() == 2
        && format_float::parse_float_format(&fmt).is_some()
        && matches!(
            function.args[1].get_type(schema),
            Ok(DataType::Float32 | DataType::Float64)
        )
    {
        return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
            format_float::java_format_float_udf(),
            vec![function.args[0].clone(), function.args[1].clone()],
        )));
    }
    if !format_uses_only_string_verbs(&fmt) {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let mut rewritten = function;
    let mut changed = false;
    for arg in rewritten.args.iter_mut().skip(1) {
        if let Ok(data_type) = arg.get_type(schema)
            && is_float_type(&data_type)
        {
            *arg = float_to_string_call(std::mem::replace(
                arg,
                Expr::Literal(ScalarValue::Null, None),
            ));
            changed = true;
        }
    }
    if changed {
        Transformed::yes(Expr::ScalarFunction(rewritten))
    } else {
        Transformed::no(Expr::ScalarFunction(rewritten))
    }
}

fn scalar_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(literal, _) => utf8_literal_text(literal),
        _ => None,
    }
}

fn format_uses_only_string_verbs(fmt: &str) -> bool {
    let bytes = fmt.as_bytes();
    let mut index = 0usize;
    let mut saw_string = false;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        index += 1;
        if index >= bytes.len() {
            return false;
        }
        match bytes[index] {
            b'%' | b'n' => {
                index += 1;
            }
            b's' | b'S' => {
                saw_string = true;
                index += 1;
            }
            _ => {
                while index < bytes.len()
                    && matches!(
                        bytes[index],
                        b'-' | b'#' | b'+' | b' ' | b'0' | b',' | b'(' | b'<'
                    )
                {
                    index += 1;
                }
                while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'*')
                {
                    index += 1;
                }
                if index < bytes.len() && bytes[index] == b'.' {
                    index += 1;
                    while index < bytes.len()
                        && (bytes[index].is_ascii_digit() || bytes[index] == b'*')
                    {
                        index += 1;
                    }
                }
                if index < bytes.len() && (bytes[index] == b's' || bytes[index] == b'S') {
                    saw_string = true;
                    index += 1;
                } else {
                    return false;
                }
            }
        }
    }
    saw_string
}

fn rewrite_array_join_arg(function: ScalarFunction, schema: &DFSchema) -> Transformed<Expr> {
    if function.args.len() < 2 || function.args.len() > 3 {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let Ok(first) = function.args[0].get_type(schema) else {
        return Transformed::no(Expr::ScalarFunction(function));
    };
    let element = match &first {
        DataType::List(field) | DataType::LargeList(field) => field.data_type(),
        _ => return Transformed::no(Expr::ScalarFunction(function)),
    };
    if !is_float_type(element) {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::string::spark_array_join_udf(),
        function.args,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float32Array, Float64Array};

    #[test]
    fn double_text_matches_java_thresholds() {
        assert_eq!(java_double_text(f64::INFINITY), "Infinity");
        assert_eq!(java_double_text(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(java_double_text(f64::NAN), "NaN");
        assert_eq!(java_double_text(10_000_000.0), "1.0E7");
        assert_eq!(java_double_text(1_000_000.0), "1000000.0");
        assert_eq!(java_double_text(123_456_789.0), "1.23456789E8");
        assert_eq!(java_double_text(0.0001), "1.0E-4");
        assert_eq!(java_double_text(0.001), "0.001");
        assert_eq!(java_double_text(1.0e21), "1.0E21");
        assert_eq!(java_double_text(-0.0), "-0.0");
        assert_eq!(java_double_text(0.0), "0.0");
        assert_eq!(java_double_text(1.0), "1.0");
        assert_eq!(java_double_text(f64::from_bits(1)), "4.9E-324");
        assert_eq!(java_double_text(-f64::from_bits(1)), "-4.9E-324");
    }

    #[test]
    fn float_text_matches_java_thresholds() {
        assert_eq!(java_float_text(f32::INFINITY), "Infinity");
        assert_eq!(java_float_text(f32::NAN), "NaN");
        assert_eq!(java_float_text(10_000_000_000.0), "1.0E10");
        assert_eq!(java_float_text(0.1), "0.1");
        assert_eq!(java_float_text(123_456.7), "123456.7");
        assert_eq!(java_float_text(0.00001), "1.0E-5");
        assert_eq!(java_float_text(f32::from_bits(1)), "1.4E-45");
        assert_eq!(java_float_text(-f32::from_bits(1)), "-1.4E-45");
        assert_eq!(java_float_text(f32::MAX), "3.4028235E38");
    }

    fn utf8_scalar(text: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(text.to_owned()))
    }

    #[test]
    fn fold_utf8_to_float_leaves_arrow_accepted_literals() {
        assert!(
            fold_utf8_to_float_literal(&utf8_scalar("1.0E7"), "1.0E7", &DataType::Float64, true)
                .unwrap()
                .is_none()
        );
        assert!(
            fold_utf8_to_float_literal(
                &utf8_scalar("Infinity"),
                "Infinity",
                &DataType::Float64,
                true
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn fold_utf8_to_float_repairs_arrow_rejections() {
        let repaired =
            fold_utf8_to_float_literal(&utf8_scalar(" 1.5 "), " 1.5 ", &DataType::Float64, true)
                .unwrap()
                .unwrap();
        assert!(matches!(
            repaired,
            Expr::Literal(ScalarValue::Float64(_), _)
        ));
        let nulled = fold_utf8_to_float_literal(&utf8_scalar(""), "", &DataType::Float64, true)
            .unwrap()
            .unwrap();
        assert!(matches!(
            nulled,
            Expr::Literal(ScalarValue::Float64(None), _)
        ));
        let ansi_off =
            fold_utf8_to_float_literal(&utf8_scalar("x"), "x", &DataType::Float64, false)
                .unwrap()
                .unwrap();
        assert!(matches!(
            ansi_off,
            Expr::Literal(ScalarValue::Float64(None), _)
        ));
    }

    fn double_cast_of(text: &str) -> Cast {
        Cast::new(
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(text.to_owned())),
                None,
            )),
            DataType::Float64,
        )
    }

    #[test]
    fn rule_arm_folds_bad_string_to_double_literal() {
        let schema = DFSchema::empty();
        let error = rewrite_float_cast(double_cast_of("x"), &schema, true).unwrap_err();
        assert!(error.to_string().contains("CAST_INVALID_INPUT"));
        let nulled = rewrite_float_cast(double_cast_of("x"), &schema, false).unwrap();
        assert!(matches!(
            nulled.data,
            Expr::Literal(ScalarValue::Float64(None), _)
        ));
        let left = rewrite_float_cast(double_cast_of("1.0E7"), &schema, true).unwrap();
        assert!(!left.transformed);
    }

    #[test]
    fn fold_utf8_to_float_raises_spark_error_when_ansi_on() {
        let error = fold_utf8_to_float_literal(&utf8_scalar("x"), "x", &DataType::Float64, true)
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("CAST_INVALID_INPUT"), "{message}");
    }

    #[test]
    fn rule_propagates_suffixed_literal_through_projection() {
        use datafusion::logical_expr::{LogicalPlanBuilder, col};
        for (text, ansi) in [("1d", true), ("1f", true), ("0x10", false)] {
            for wrapped in [false, true] {
                let inner = LogicalPlanBuilder::empty(false)
                    .project(vec![
                        Expr::Literal(ScalarValue::Utf8(Some(text.to_owned())), None).alias("x"),
                    ])
                    .unwrap()
                    .build()
                    .unwrap();
                let cast_input = if wrapped {
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        crate::decimal_cast::spark_decimal_cast_nullable_udf(),
                        vec![col("x")],
                    ))
                } else {
                    col("x")
                };
                let outer = LogicalPlanBuilder::new(inner)
                    .project(vec![
                        Expr::Cast(Cast::new(Box::new(cast_input), DataType::Float64)).alias("r"),
                    ])
                    .unwrap()
                    .build()
                    .unwrap();
                let analyzed = rewrite_float_plan(outer, ansi).unwrap().data;
                let LogicalPlan::Projection(projection) = analyzed else {
                    panic!("projection plan for {text}");
                };
                let [Expr::Alias(alias)] = projection.expr.as_slice() else {
                    panic!("aliased projection for {text}");
                };
                match alias.expr.as_ref() {
                    Expr::Literal(ScalarValue::Float64(Some(value)), _) => {
                        assert_eq!(
                            value.to_bits(),
                            1.0f64.to_bits(),
                            "{text} wrapped={wrapped}"
                        );
                    }
                    Expr::Literal(ScalarValue::Float64(None), _) => {
                        assert_eq!(text, "0x10", "{text} wrapped={wrapped}");
                    }
                    other => panic!("unexpected folded expr {other:?} for {text}"),
                }
            }
        }
    }

    fn format_call(format: &str, arg: Expr) -> ScalarFunction {
        ScalarFunction::new_udf(
            datafusion_spark::function::string::format_string(),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some(format.to_owned())), None),
                arg,
            ],
        )
    }

    fn double_literal(value: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(Some(value)), None)
    }

    #[test]
    fn rule_routes_single_float_verb_to_half_up_shim() {
        let schema = DFSchema::empty();
        for format in ["%f", "%.2f", "%-+, (.4F"] {
            let rewritten =
                rewrite_format_string_args(format_call(format, double_literal(0.125)), &schema);
            assert!(rewritten.transformed, "{format}");
            let Expr::ScalarFunction(shimmed) = rewritten.data else {
                panic!("shimmed call for {format}");
            };
            assert_eq!(shimmed.func.name(), "__repark_format_float__");
            assert_eq!(shimmed.args.len(), 2);
        }
    }

    #[test]
    fn rule_leaves_other_format_calls_on_upstream() {
        let schema = DFSchema::empty();
        for call in [
            format_call("%d", double_literal(0.125)),
            format_call("%s %f", double_literal(0.125)),
            format_call("%f %f", double_literal(0.125)),
            format_call("%e", double_literal(0.125)),
            format_call("%.3e", double_literal(0.125)),
            format_call("%g", double_literal(0.125)),
            format_call(
                "%f",
                Expr::Literal(ScalarValue::Utf8(Some("0.125".to_owned())), None),
            ),
        ] {
            assert!(!rewrite_format_string_args(call, &schema).transformed);
        }
    }

    #[test]
    fn fold_utf8_to_float_accepts_java_type_suffix() {
        for (text, target, expected) in [
            ("1d", DataType::Float64, 1.0f64),
            ("1f", DataType::Float64, 1.0f64),
            ("1.5D", DataType::Float64, 1.5f64),
            ("1e2d", DataType::Float64, 100.0f64),
        ] {
            let folded = fold_utf8_to_float_literal(&utf8_scalar(text), text, &target, true)
                .unwrap()
                .unwrap();
            assert!(
                matches!(folded, Expr::Literal(ScalarValue::Float64(Some(value)), _) if value.to_bits() == expected.to_bits()),
                "{text}"
            );
        }
        let folded = fold_utf8_to_float_literal(&utf8_scalar("1d"), "1d", &DataType::Float32, true)
            .unwrap()
            .unwrap();
        assert!(matches!(
            folded,
            Expr::Literal(ScalarValue::Float32(Some(_)), _)
        ));
        let hex =
            fold_utf8_to_float_literal(&utf8_scalar("0x10"), "0x10", &DataType::Float64, true)
                .unwrap_err();
        assert!(hex.to_string().contains("CAST_INVALID_INPUT"));
        let hex_nulled =
            fold_utf8_to_float_literal(&utf8_scalar("0x10"), "0x10", &DataType::Float64, false)
                .unwrap()
                .unwrap();
        assert!(matches!(
            hex_nulled,
            Expr::Literal(ScalarValue::Float64(None), _)
        ));
    }

    #[test]
    fn text_len_matches_text_bytes() {
        for value in [
            f64::INFINITY,
            f64::NAN,
            10_000_000.0,
            1_000_000.0,
            0.0001,
            0.001,
            1.0e21,
            -0.0,
            0.300_000_000_000_000_04,
            f64::from_bits(1),
        ] {
            assert_eq!(java_double_text_len(value), java_double_text(value).len());
        }
        for value in [
            f32::INFINITY,
            10_000_000_000.0,
            0.1,
            f32::from_bits(1),
            f32::MAX,
        ] {
            assert_eq!(java_float_text_len(value), java_float_text(value).len());
        }
    }

    #[test]
    fn string_arrays_preserve_nulls() {
        let doubles: Float64Array = vec![Some(10_000_000.0), None, Some(1.0)].into();
        let rendered = java_double_strings(&doubles);
        assert_eq!(rendered.len(), 3);
        assert!(rendered.is_null(1));
        assert_eq!(rendered.value(0), "1.0E7");
        assert_eq!(rendered.value(2), "1.0");
        let floats: Float32Array = vec![None, Some(0.1)].into();
        let rendered = java_float_strings(&floats);
        assert_eq!(rendered.len(), 2);
        assert!(rendered.is_null(0));
        assert_eq!(rendered.value(1), "0.1");
    }
}
