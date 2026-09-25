use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectNamePart, UnaryOperator, Value,
};

const NAME_DEPTH: usize = 32;

const PLAIN_FUNCTIONS: [&str; 10] = [
    "abs",
    "coalesce",
    "concat",
    "current_date",
    "length",
    "lower",
    "max",
    "nvl",
    "ucase",
    "upper",
];

pub(crate) fn expression_name(expr: &Expr, qualifiers: &[String]) -> Option<String> {
    render(expr, qualifiers, 0)
}

pub(super) fn literal_name(value: &Value) -> Option<String> {
    match value {
        Value::Number(text, false) if plain_number(text) => Some(text.clone()),
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.clone()),
        Value::Boolean(flag) => Some(flag.to_string()),
        Value::Null => Some("NULL".to_string()),
        _ => None,
    }
}

fn plain_number(text: &str) -> bool {
    let (whole, fraction) = text
        .split_once('.')
        .map_or((text, None), |(whole, fraction)| (whole, Some(fraction)));
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let whole_plain = whole == "0" || (digits(whole) && !whole.starts_with('0'));
    whole_plain && fraction.is_none_or(digits)
}

fn negative_literal(value: &Value) -> Option<String> {
    let Value::Number(text, false) = value else {
        return None;
    };
    if !plain_number(text) {
        return None;
    }
    if text.bytes().all(|byte| byte == b'0' || byte == b'.') {
        Some(text.clone())
    } else {
        Some(format!("-{text}"))
    }
}

fn plain_ident(ident: &Ident) -> Option<String> {
    let plain = !ident.value.is_empty()
        && ident
            .value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    plain.then(|| ident.value.clone())
}

fn column_name(parts: &[Ident], qualifiers: &[String]) -> Option<String> {
    match parts {
        [column] => plain_ident(column),
        [qualifier, column]
            if qualifiers
                .iter()
                .any(|known| known.eq_ignore_ascii_case(&qualifier.value)) =>
        {
            plain_ident(column)
        }
        _ => None,
    }
}

fn operator(op: &BinaryOperator) -> Option<&'static str> {
    Some(match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::Eq => "=",
        BinaryOperator::Gt => ">",
        BinaryOperator::Lt => "<",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        _ => return None,
    })
}

fn render(expr: &Expr, qualifiers: &[String], depth: usize) -> Option<String> {
    if depth > NAME_DEPTH {
        return None;
    }
    let inner = |child: &Expr| render(child, qualifiers, depth + 1);
    match expr {
        Expr::Identifier(ident) => plain_ident(ident),
        Expr::CompoundIdentifier(parts) => column_name(parts, qualifiers),
        Expr::Value(value) => literal_name(&value.value),
        Expr::Nested(child) => inner(child),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: child,
        } => match child.as_ref() {
            Expr::Value(value) => negative_literal(&value.value),
            other => Some(format!("(- {})", inner(other)?)),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr: child,
        } => Some(format!("(+ {})", inner(child)?)),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: child,
        } => Some(format!("(NOT {})", inner(child)?)),
        Expr::IsNull(child) => Some(format!("({} IS NULL)", inner(child)?)),
        Expr::IsNotNull(child) => Some(format!("({} IS NOT NULL)", inner(child)?)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            right,
        } => Some(format!("concat({}, {})", inner(left)?, inner(right)?)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::NotEq,
            right,
        } => Some(format!("(NOT ({} = {}))", inner(left)?, inner(right)?)),
        Expr::BinaryOp { left, op, right } => Some(format!(
            "({} {} {})",
            inner(left)?,
            operator(op)?,
            inner(right)?
        )),
        Expr::Function(function) => function_name(function, qualifiers, depth),
        _ => None,
    }
}

fn function_name(function: &Function, qualifiers: &[String], depth: usize) -> Option<String> {
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    let decorated = name.quote_style.is_some()
        || function.uses_odbc_syntax
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None);
    let lower = name.value.to_ascii_lowercase();
    if decorated || !PLAIN_FUNCTIONS.contains(&lower.as_str()) {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    let args = list
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                render(expr, qualifiers, depth + 1)
            }
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some(format!("{lower}({})", args.join(", ")))
}
