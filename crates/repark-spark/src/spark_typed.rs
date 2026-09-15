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
    let is_string = matches!(
        scalar,
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_)
    );
    let is_float_target = matches!(
        cast.field.data_type(),
        DataType::Float32 | DataType::Float64
    );
    let is_decimal_target = matches!(
        cast.field.data_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    );
    if !((is_string && is_float_target) || is_decimal_target) {
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
        plan.transform_up_with_subqueries(rewrite_projection_display)
            .data()
    }

    fn name(&self) -> &'static str {
        "spark_projection_display"
    }
}

fn rewrite_projection_display(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(mut projection) = plan else {
        return Ok(Transformed::no(plan));
    };
    let mut changed = false;
    for expr in &mut projection.expr {
        let current = expr.clone();
        let (inner, name) = match current {
            Expr::Alias(alias) => (*alias.expr, alias.name),
            other => {
                let name = other.schema_name().to_string();
                (other, name)
            }
        };
        if !needs_spark_display(&name) {
            continue;
        }
        let display = spark_display(&inner);
        if display == name {
            continue;
        }
        *expr = Expr::Alias(Alias::new(inner, None::<&str>, display));
        changed = true;
    }
    if changed {
        let plan = LogicalPlan::Projection(projection)
            .recompute_schema()
            .map_err(|error| {
                DataFusionError::Plan(format!("spark projection display schema: {error}"))
            })?;
        Ok(Transformed::yes(plan))
    } else {
        Ok(Transformed::no(LogicalPlan::Projection(projection)))
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
    name.contains("Int64(")
        || name.contains("Int32(")
        || name.contains("Utf8(")
        || name.contains("__repark_spark_")
        || name.contains("__repark_selx_")
        || name.contains('`')
}

fn spark_display(expr: &Expr) -> String {
    clean_spark_display(&spark_display_inner(expr))
}

fn spark_display_inner(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => spark_display(alias.expr.as_ref()),
        Expr::Column(column) => column.name.clone(),
        Expr::Literal(scalar, _) => literal_spark(scalar),
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
        Expr::ScalarFunction(function) if function.func.name() == SUFFIX_LITERAL_NAME => {
            function.args.first().map_or_else(
                || expr.schema_name().to_string(),
                spark_display,
            )
        }
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
        ScalarValue::Utf8(Some(text)) | ScalarValue::Utf8View(Some(text)) => text.clone(),
        ScalarValue::Int8(Some(value)) => value.to_string(),
        ScalarValue::Int16(Some(value)) => value.to_string(),
        ScalarValue::Int32(Some(value)) => value.to_string(),
        ScalarValue::Int64(Some(value)) => value.to_string(),
        ScalarValue::Float32(Some(value)) => value.to_string(),
        ScalarValue::Float64(Some(value)) => value.to_string(),
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

pub const SUFFIX_LITERAL_NAME: &str = "__repark_suffix_literal__";

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
        Ok(Field::new(
            SUFFIX_LITERAL_NAME,
            first.data_type().clone(),
            true,
        )
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.first().cloned().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "'{SUFFIX_LITERAL_NAME}' expects one argument"
            ))
        })
    }
}

#[must_use]
pub fn suffix_literal_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SuffixLiteral::new()))
}
