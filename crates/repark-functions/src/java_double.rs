mod dtoa;

pub(crate) use dtoa::{
    java_double_strings, java_double_text, java_double_text_len, java_float_strings,
    java_float_text, java_float_text_len, with_java_double_text, with_java_float_text,
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
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
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
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_float_expr(node, &schema, ansi))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_float_expr(expr: Expr, schema: &DFSchema, ansi: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::Cast(cast) => rewrite_float_cast(cast, schema, ansi),
        Expr::TryCast(try_cast) => {
            let Ok(source_type) = try_cast.expr.get_type(schema) else {
                return Ok(Transformed::no(Expr::TryCast(try_cast)));
            };
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
    let parsed = match target {
        DataType::Float32 => trimmed
            .parse::<f32>()
            .ok()
            .map(|value| ScalarValue::Float32(Some(value))),
        _ => trimmed
            .parse::<f64>()
            .ok()
            .map(|value| ScalarValue::Float64(Some(value))),
    };
    if let Some(value) = parsed {
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
