use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::logical_expr::{ReturnFieldArgs, ScalarFunctionArgs};
use datafusion::optimizer::AnalyzerRule;
use repark_functions::java_double::{java_double_text, java_float_text};

pub const SPARK_AS_NAME: &str = "__repark_spark_as__";

#[derive(Debug, Default)]
pub struct FoldSparkNumericCasts;

impl AnalyzerRule for FoldSparkNumericCasts {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(fold_plan).data()
    }

    fn name(&self) -> &'static str {
        "fold_spark_numeric_casts"
    }
}

fn fold_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(fold_expr(node)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn fold_expr(expr: Expr) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let scalar = match cast.expr.as_ref() {
        Expr::Literal(scalar, _) => scalar,
        Expr::ScalarFunction(function)
            if function.func.name() == SUFFIX_LITERAL_NAME && function.args.len() == 1 =>
        {
            let Expr::Literal(scalar, _) = &function.args[0] else {
                return Transformed::no(expr);
            };
            scalar
        }
        _ => return Transformed::no(expr),
    };
    let is_foldable_target = matches!(
        cast.field.data_type(),
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    );
    if !is_foldable_target {
        return Transformed::no(expr);
    }
    let Ok(folded) = scalar.cast_to(cast.field.data_type()) else {
        return Transformed::no(expr);
    };
    if folded.is_null() {
        return Transformed::no(expr);
    }
    Transformed::yes(Expr::Literal(folded, None))
}

#[derive(Debug, Default)]
pub struct SparkProjectionDisplay;

impl AnalyzerRule for SparkProjectionDisplay {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        rewrite_projection_display(plan)
    }

    fn name(&self) -> &'static str {
        "spark_projection_display"
    }
}

fn rewrite_projection_display(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(mut projection) = plan else {
        return Ok(plan);
    };
    let mut changed = false;
    for expr in &mut projection.expr {
        let (name, explicit) = match &*expr {
            Expr::Alias(alias) => (
                alias.name.clone(),
                !alias.name.contains("__repark_") && !scalar_dump_name(&alias.name),
            ),
            other => (other.schema_name().to_string(), false),
        };
        if explicit || !needs_spark_display(&name) {
            continue;
        }
        let inner = match &*expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        };
        let display = spark_display(inner);
        if display == name {
            continue;
        }
        let inner = match expr.clone() {
            Expr::Alias(alias) => *alias.expr,
            other => other,
        };
        *expr = Expr::Alias(Alias::new(inner, None::<&str>, display));
        changed = true;
    }
    if changed {
        LogicalPlan::Projection(projection)
            .recompute_schema()
            .map_err(|error| {
                DataFusionError::Plan(format!("spark projection display schema: {error}"))
            })
    } else {
        Ok(LogicalPlan::Projection(projection))
    }
}

fn integer_arith_op(name: &str) -> Option<&'static str> {
    match name {
        "__repark_spark_int_add__" => Some("+"),
        "__repark_spark_int_sub__" => Some("-"),
        "__repark_spark_int_mul__" => Some("*"),
        _ => None,
    }
}

fn needs_spark_display(name: &str) -> bool {
    name.contains("__repark_selx_")
        || name.contains(SUFFIX_LITERAL_NAME)
        || name.contains("named_struct(")
        || name.contains("get_field(")
        || name.contains('`')
        || scalar_dump_name(name)
}

fn scalar_dump_name(name: &str) -> bool {
    is_single_dump(name)
}

fn is_single_dump(name: &str) -> bool {
    if let Some(inner) = name
        .strip_prefix("(- ")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return is_single_dump(inner) || is_bare_number(inner);
    }
    let rest = name.strip_prefix('-').unwrap_or(name);
    let rest = strip_table_qualifier(rest);
    DUMP_PREFIXES
        .iter()
        .any(|prefix| is_balanced_dump(rest, prefix))
}

fn strip_table_qualifier(name: &str) -> &str {
    let Some(paren) = name.find('(') else {
        return name;
    };
    let cut = name[..paren].rfind('.').map_or(0, |dot| dot + 1);
    &name[cut..]
}

fn is_bare_number(text: &str) -> bool {
    let (mantissa, exponent) = match text.bytes().position(|byte| byte == b'e' || byte == b'E') {
        Some(index) => {
            let (mantissa, exponent) = text.split_at(index);
            (mantissa, Some(&exponent[1..]))
        }
        None => (text, None),
    };
    if let Some(exponent) = exponent {
        let exponent = exponent
            .strip_prefix('+')
            .or_else(|| exponent.strip_prefix('-'))
            .unwrap_or(exponent);
        if !is_digit_run(exponent) {
            return false;
        }
    }
    match mantissa.split_once('.') {
        Some((int_part, frac_part)) => is_digit_run(int_part) && is_digit_run(frac_part),
        None => is_digit_run(mantissa),
    }
}

fn is_digit_run(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_balanced_dump(name: &str, prefix: &str) -> bool {
    let Some(rest) = name.strip_prefix(prefix) else {
        return false;
    };
    let mut depth = 0usize;
    for (index, byte) in rest.bytes().enumerate() {
        if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            if depth == 0 {
                return index + 1 == rest.len();
            }
            depth -= 1;
        }
    }
    false
}

const DUMP_PREFIXES: [&str; 17] = [
    "Int64(",
    "Int32(",
    "Int16(",
    "Int8(",
    "UInt64(",
    "UInt32(",
    "UInt16(",
    "UInt8(",
    "Float64(",
    "Float32(",
    "Decimal128(",
    "Decimal256(",
    "Utf8(\"",
    "LargeUtf8(\"",
    "Utf8View(\"",
    "Boolean(",
    "Some(",
];

fn spark_display(expr: &Expr) -> String {
    clean_spark_display(&spark_display_inner(expr))
}

fn spark_display_inner(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => spark_display(alias.expr.as_ref()),
        Expr::Column(column) => column.name.clone(),
        Expr::Literal(scalar, _) => literal_spark(scalar),
        Expr::Negative(inner) => format!("-{}", spark_display(inner.as_ref())),
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Literal(scalar, _)
                if matches!(
                    scalar,
                    ScalarValue::Int8(Some(_))
                        | ScalarValue::Int16(Some(_))
                        | ScalarValue::Int32(Some(_))
                        | ScalarValue::Int64(Some(_))
                ) && matches!(
                    cast.field.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                ) =>
            {
                literal_spark(scalar)
            }
            _ => expr.schema_name().to_string(),
        },
        Expr::BinaryExpr(binary) => format!(
            "({} {} {})",
            spark_display(binary.left.as_ref()),
            binary.op,
            spark_display(binary.right.as_ref())
        ),
        Expr::ScalarFunction(function) if let Some(op) = integer_arith_op(function.func.name()) => {
            if function.args.len() == 2 {
                format!(
                    "({} {op} {})",
                    spark_display(&function.args[0]),
                    spark_display(&function.args[1])
                )
            } else {
                expr.schema_name().to_string()
            }
        }
        Expr::ScalarFunction(function) if function.func.name() == "get_field" => {
            let Some((base, field)) = function.args.split_first() else {
                return expr.schema_name().to_string();
            };
            let field_name = field
                .first()
                .map_or_else(|| expr.schema_name().to_string(), spark_display);
            format!("{}.{field_name}", spark_display(base))
        }
        Expr::ScalarFunction(function) if function.func.name() == "named_struct" => {
            named_struct_spark(&function.args)
        }
        Expr::ScalarFunction(function) if function.func.name() == SPARK_AS_NAME => function
            .args
            .first()
            .map_or_else(|| expr.schema_name().to_string(), spark_display),
        Expr::ScalarFunction(function) if function.func.name() == SUFFIX_LITERAL_NAME => function
            .args
            .first()
            .map_or_else(|| expr.schema_name().to_string(), spark_display),
        other => other.schema_name().to_string(),
    }
}

fn clean_spark_display(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut index = 0usize;
    while index < name.len() {
        if let Some(relative) = name[index..].find("datafusion.public.__repark_") {
            out.push_str(&name[index..index + relative]);
            let after = index + relative + "datafusion.public.__repark_".len();
            if let Some(dot) = name[after..].find('.') {
                index = after + dot + 1;
                continue;
            }
            out.push_str(&name[index + relative..]);
            break;
        }
        out.push_str(&name[index..]);
        break;
    }
    strip_arrow_constructors(&out)
}

fn strip_arrow_constructors(name: &str) -> String {
    let mut out = name.to_string();
    for prefix in ["Int64(", "Int32(", "Int16(", "Int8("] {
        while let Some(start) = out.find(prefix) {
            let after = start + prefix.len();
            let Some(end) = out[after..].find(')') else {
                break;
            };
            let digits = out[after..after + end].to_string();
            out.replace_range(start..=after + end, &digits);
        }
    }
    while let Some(start) = out.find("Utf8(\"") {
        let after = start + "Utf8(\"".len();
        let Some(end) = out[after..].find("\")") else {
            break;
        };
        let text = out[after..after + end].to_string();
        out.replace_range(start..after + end + 2, &text);
    }
    out
}

fn named_struct_spark(args: &[Expr]) -> String {
    let mut parts = Vec::new();
    for chunk in args.chunks_exact(2) {
        parts.push(format!(
            "{}, {}",
            spark_display(&chunk[0]),
            spark_display(&chunk[1])
        ));
    }
    format!("named_struct({})", parts.join(", "))
}

fn literal_spark(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => text.clone(),
        ScalarValue::Int8(Some(value)) => value.to_string(),
        ScalarValue::Int16(Some(value)) => value.to_string(),
        ScalarValue::Int32(Some(value)) => value.to_string(),
        ScalarValue::Int64(Some(value)) => value.to_string(),
        ScalarValue::UInt8(Some(value)) => value.to_string(),
        ScalarValue::UInt16(Some(value)) => value.to_string(),
        ScalarValue::UInt32(Some(value)) => value.to_string(),
        ScalarValue::UInt64(Some(value)) => value.to_string(),
        ScalarValue::Float32(Some(value)) => java_float_text(*value),
        ScalarValue::Float64(Some(value)) => java_double_text(*value),
        ScalarValue::Decimal128(Some(value), _, scale) => decimal_literal_text(*value, *scale),
        ScalarValue::Boolean(Some(value)) => value.to_string(),
        other => other.to_string(),
    }
}

#[derive(Debug)]
struct SparkAs {
    signature: Signature,
}

impl SparkAs {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkAs {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkAs {}

impl Hash for SparkAs {
    fn hash<H: Hasher>(&self, state: &mut H) {
        SPARK_AS_NAME.hash(state);
    }
}

impl ScalarUDFImpl for SparkAs {
    fn name(&self) -> &str {
        SPARK_AS_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.get(1).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("'{SPARK_AS_NAME}' expects a value argument"))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let value = args.arg_fields.get(1).ok_or_else(|| {
            DataFusionError::Plan(format!("'{SPARK_AS_NAME}' expects a value argument"))
        })?;
        let display = args
            .scalar_arguments
            .first()
            .and_then(|scalar| scalar.as_ref())
            .and_then(|scalar| scalar.try_as_str().flatten())
            .unwrap_or(SPARK_AS_NAME);
        Ok(Field::new(display, value.data_type().clone(), value.is_nullable()).into())
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(name)), _)) => Ok(name.clone()),
            Some(other) => Ok(other.to_string()),
            None => Ok(SPARK_AS_NAME.to_string()),
        }
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(name)), _)) => Ok(name.clone()),
            Some(other) => Ok(other.to_string()),
            None => Ok(SPARK_AS_NAME.to_string()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.get(1).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("'{SPARK_AS_NAME}' expects a value argument"))
        })
    }
}

#[must_use]
pub fn spark_as_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkAs::new()))
}

pub use repark_functions::java_double::SUFFIX_LITERAL_NAME;

#[derive(Debug)]
struct SuffixLiteral {
    signature: Signature,
}

impl SuffixLiteral {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SuffixLiteral {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SuffixLiteral {}

impl Hash for SuffixLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        SUFFIX_LITERAL_NAME.hash(state);
    }
}

impl ScalarUDFImpl for SuffixLiteral {
    fn name(&self) -> &str {
        SUFFIX_LITERAL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("'{SUFFIX_LITERAL_NAME}' expects one argument"))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{SUFFIX_LITERAL_NAME}' expects one argument"))
        })?;
        let scalar = args
            .scalar_arguments
            .first()
            .and_then(|scalar| scalar.as_ref())
            .copied();
        let name = suffix_literal_name(scalar);
        Ok(Field::new(name, first.data_type().clone(), true).into())
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        Ok(suffix_expr_name(args.first()))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(suffix_expr_name(args.first()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.first().cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("'{SUFFIX_LITERAL_NAME}' expects one argument"))
        })
    }
}

#[must_use]
pub fn suffix_literal_name(scalar: Option<&ScalarValue>) -> String {
    match scalar {
        Some(ScalarValue::Float64(Some(value))) => java_double_text(*value),
        Some(ScalarValue::Float32(Some(value))) => java_float_text(*value),
        Some(ScalarValue::Int8(Some(value))) => value.to_string(),
        Some(ScalarValue::Int16(Some(value))) => value.to_string(),
        Some(ScalarValue::Int32(Some(value))) => value.to_string(),
        Some(ScalarValue::Int64(Some(value))) => value.to_string(),
        Some(ScalarValue::Decimal128(Some(value), _, scale)) => {
            decimal_literal_text(*value, *scale)
        }
        Some(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
        ) => match text.parse::<f64>() {
            Ok(value) => java_double_text(value),
            Err(_) => text.clone(),
        },
        Some(scalar) => scalar.to_string(),
        None => "NULL".to_string(),
    }
}

fn suffix_expr_name(arg: Option<&Expr>) -> String {
    match arg {
        Some(Expr::Literal(scalar, _)) => suffix_literal_name(Some(scalar)),
        Some(other) => other.to_string(),
        None => SUFFIX_LITERAL_NAME.to_string(),
    }
}

fn decimal_literal_text(value: i128, scale: i8) -> String {
    if scale <= 0 {
        let zeros = "0".repeat(scale.unsigned_abs() as usize);
        return format!("{value}{zeros}");
    }
    let width = usize::from(u8::try_from(scale).unwrap_or(0));
    let digits = value.unsigned_abs().to_string();
    let text = if digits.len() > width {
        let split = digits.len() - width;
        format!("{}.{}", &digits[..split], &digits[split..])
    } else {
        format!("0.{digits:0>width$}")
    };
    if value < 0 { format!("-{text}") } else { text }
}

#[must_use]
pub fn suffix_literal_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SuffixLiteral::new()))
}
