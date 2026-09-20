//! CALL argument parsing and scalar coercion helpers.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan,
};
use repark_core::time_travel::parse_timestamp_to_ms;

// === Argument bag ===

/// Parsed CALL arguments — named map + ordered positional list.
#[derive(Debug, Default)]
pub(crate) struct CallArgs {
    pub(crate) named: HashMap<String, Expr>,
    pub(crate) positional: Vec<Expr>,
}

impl CallArgs {
    pub(crate) fn parse(args: &FunctionArguments) -> Result<Self> {
        match args {
            FunctionArguments::None => Ok(Self::default()),
            FunctionArguments::Subquery(_) => Err(DataFusionError::Plan(
                "CALL does not accept a subquery argument list".to_string(),
            )),
            FunctionArguments::List(list) => {
                let mut named = HashMap::new();
                let mut positional = Vec::new();
                for arg in &list.args {
                    // A quoted name carries dashed option keys (RP-2 / C-006).
                    let key = match arg {
                        FunctionArg::Named { name, .. }
                        | FunctionArg::ExprNamed {
                            name: Expr::Identifier(name),
                            ..
                        } => name.value.to_ascii_lowercase(),
                        FunctionArg::ExprNamed {
                            name:
                                Expr::Value(ValueWithSpan {
                                    value:
                                        Value::SingleQuotedString(name)
                                        | Value::DoubleQuotedString(name),
                                    ..
                                }),
                            ..
                        } => name.to_ascii_lowercase(),
                        FunctionArg::ExprNamed { name, .. } => {
                            return Err(DataFusionError::Plan(format!(
                                "CALL named argument name must be an identifier, got {name}"
                            )));
                        }
                        FunctionArg::Unnamed(_) => String::new(),
                    };
                    match arg {
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            let expr = match arg {
                                FunctionArgExpr::Expr(expr) => expr.clone(),
                                other => {
                                    return Err(DataFusionError::Plan(format!(
                                        "CALL named argument `{key}` must be a scalar \
                                         expression, got {other}"
                                    )));
                                }
                            };
                            if named.insert(key.clone(), expr).is_some() {
                                return Err(DataFusionError::Plan(format!(
                                    "duplicate CALL argument `{key}`"
                                )));
                            }
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            positional.push(expr.clone());
                        }
                        FunctionArg::Unnamed(other) => {
                            return Err(DataFusionError::Plan(format!(
                                "CALL positional argument must be a scalar expression, got {other}"
                            )));
                        }
                    }
                }
                Ok(Self { named, positional })
            }
        }
    }

    pub(crate) fn require_string(&self, name: &str, position: usize) -> Result<String> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_string(expr, name);
        }
        self.positional
            .get(position)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CALL argument `{name}` is required (named `{name} => …` or positional \
                     #{position})"
                ))
            })
            .and_then(|expr| expr_as_string(expr, name))
    }

    pub(crate) fn optional_string(&self, name: &str) -> Result<Option<String>> {
        self.named
            .get(name)
            .map(|expr| expr_as_string(expr, name))
            .transpose()
    }

    pub(crate) fn optional_string_array(&self, name: &str) -> Result<Option<Vec<String>>> {
        let Some(expr) = self.named.get(name) else {
            return Ok(None);
        };
        let elements: Vec<Expr> = match expr {
            Expr::Array(array) => array.elem.clone(),
            Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("array") => {
                let FunctionArguments::List(list) = &function.args else {
                    return Err(DataFusionError::Plan(format!(
                        "CALL argument `{name}` must be array('a', 'b', …)"
                    )));
                };
                let mut elements = Vec::with_capacity(list.args.len());
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            elements.push(inner.clone());
                        }
                        other => {
                            return Err(DataFusionError::Plan(format!(
                                "CALL argument `{name}` must be array of string literals, got {other}"
                            )));
                        }
                    }
                }
                elements
            }
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }) => Vec::new(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "CALL argument `{name}` must be array('a', 'b', …), got {other}"
                )));
            }
        };
        elements
            .iter()
            .map(|element| expr_as_string(element, name))
            .collect::<Result<Vec<String>>>()
            .map(Some)
    }

    pub(crate) fn optional_i64(&self, name: &str, position: Option<usize>) -> Result<Option<i64>> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_i64(expr, name).map(Some);
        }
        if let Some(index) = position
            && let Some(expr) = self.positional.get(index)
        {
            return expr_as_i64(expr, name).map(Some);
        }
        Ok(None)
    }

    pub(crate) fn optional_bool(
        &self,
        name: &str,
        position: Option<usize>,
    ) -> Result<Option<bool>> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_bool(expr, name).map(Some);
        }
        if let Some(index) = position
            && let Some(expr) = self.positional.get(index)
        {
            return expr_as_bool(expr, name).map(Some);
        }
        Ok(None)
    }

    pub(crate) fn optional_f64(&self, name: &str, position: Option<usize>) -> Result<Option<f64>> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_f64(expr, name).map(Some);
        }
        if let Some(index) = position
            && let Some(expr) = self.positional.get(index)
        {
            return expr_as_f64(expr, name).map(Some);
        }
        Ok(None)
    }

    pub(crate) fn optional_i32(&self, name: &str, position: Option<usize>) -> Result<Option<i32>> {
        match self.optional_i64(name, position)? {
            None => Ok(None),
            Some(value) => i32::try_from(value).map(Some).map_err(|_| {
                DataFusionError::Plan(format!(
                    "CALL argument `{name}` value {value} does not fit i32"
                ))
            }),
        }
    }

    pub(crate) fn optional_timestamp_ms(
        &self,
        name: &str,
        position: Option<usize>,
    ) -> Result<Option<i64>> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_timestamp_ms(expr, name).map(Some);
        }
        if let Some(index) = position
            && let Some(expr) = self.positional.get(index)
        {
            return expr_as_timestamp_ms(expr, name).map(Some);
        }
        Ok(None)
    }

    pub(crate) fn has_named(&self, name: &str) -> bool {
        self.named.contains_key(name)
    }

    pub(crate) fn reject_unknown_named(&self, allowed: &[&str]) -> Result<()> {
        for key in self.named.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "unknown CALL argument `{key}`; allowed: {}",
                    allowed.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// Reject more positional arguments than the procedure arity (C1-L-001 / C1-L-002).
    pub(crate) fn reject_excess_positional(&self, max_arity: usize) -> Result<()> {
        if self.positional.len() > max_arity {
            return Err(DataFusionError::Plan(format!(
                "CALL accepts at most {max_arity} positional argument(s); got {}",
                self.positional.len()
            )));
        }
        Ok(())
    }
}

pub(crate) struct ParamDecl {
    pub(crate) name: &'static str,
    #[allow(dead_code)]
    pub(crate) data_type: &'static str,
    pub(crate) required: bool,
}

#[derive(Debug, Default)]
pub(crate) struct BoundArgs {
    bound: Vec<(String, Option<Expr>)>,
}

impl BoundArgs {
    pub(crate) fn get(&self, name: &str) -> Option<&Expr> {
        self.bound
            .iter()
            .find(|(key, _)| key == name)
            .and_then(|(_, expr)| expr.as_ref())
    }

    fn declared_position(&self, name: &str) -> usize {
        self.bound
            .iter()
            .position(|(key, _)| key == name)
            .unwrap_or(0)
    }

    pub(crate) fn require_string(&self, name: &str) -> Result<String> {
        if let Some(expr) = self.get(name) {
            return expr_as_string(expr, name);
        }
        let position = self.declared_position(name);
        Err(DataFusionError::Plan(format!(
            "CALL argument `{name}` is required (named `{name} => …` or positional #{position})"
        )))
    }

    pub(crate) fn optional_string(&self, name: &str) -> Result<Option<String>> {
        self.get(name)
            .map(|expr| expr_as_string(expr, name))
            .transpose()
    }

    pub(crate) fn optional_bool(&self, name: &str) -> Result<Option<bool>> {
        self.get(name)
            .map(|expr| expr_as_bool(expr, name))
            .transpose()
    }
}

fn is_sql_null(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: Value::Null,
            ..
        })
    )
}

pub(crate) fn bind(args: &CallArgs, params: &[ParamDecl], extras: &[&str]) -> Result<BoundArgs> {
    let mut names: Vec<&str> = params.iter().map(|param| param.name).collect();
    names.extend_from_slice(extras);
    for key in args.named.keys() {
        if !names.contains(&key.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "unknown CALL argument `{key}`; allowed: {}",
                names.join(", ")
            )));
        }
    }
    let max_arity = params.len();
    if args.positional.len() > max_arity {
        return Err(DataFusionError::Plan(format!(
            "CALL accepts at most {max_arity} positional argument(s); got {}",
            args.positional.len()
        )));
    }
    let mut slots: Vec<Option<Expr>> = vec![None; params.len()];
    for (index, expr) in args.positional.iter().enumerate() {
        let name = params[index].name;
        if args.named.contains_key(name) {
            return Err(DataFusionError::Plan(format!(
                "CALL argument `{name}` is bound twice (positionally and by name)"
            )));
        }
        if !is_sql_null(expr) {
            slots[index] = Some(expr.clone());
        }
    }
    for (index, param) in params.iter().enumerate() {
        if slots[index].is_none()
            && let Some(expr) = args.named.get(param.name)
            && !is_sql_null(expr)
        {
            slots[index] = Some(expr.clone());
        }
    }
    let mut bound: Vec<(String, Option<Expr>)> = params
        .iter()
        .zip(slots)
        .map(|(param, expr)| (param.name.to_string(), expr))
        .collect();
    for extra in extras {
        let expr = args.named.get(*extra).and_then(|candidate| {
            if is_sql_null(candidate) {
                None
            } else {
                Some(candidate.clone())
            }
        });
        bound.push(((*extra).to_string(), expr));
    }
    for (index, param) in params.iter().enumerate() {
        if param.required && bound[index].1.is_none() {
            let name = param.name;
            return Err(DataFusionError::Plan(format!(
                "CALL argument `{name}` is required (named `{name} => …` or positional #{index})"
            )));
        }
    }
    Ok(BoundArgs { bound })
}

pub(crate) fn expr_as_string(expr: &Expr, arg_name: &str) -> Result<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => Ok(text.clone()),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be a string literal, got {other}"
        ))),
    }
}

pub(crate) fn expr_as_i64(expr: &Expr, arg_name: &str) -> Result<i64> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => raw.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` is not an integer: {raw}"
            ))
        }),
        Expr::UnaryOp {
            op: datafusion::sql::sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let value = expr_as_i64(expr, arg_name)?;
            value.checked_neg().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` integer negation overflows i64: {value}"
                ))
            })
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => text.trim().parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` string is not an integer: {text}"
            ))
        }),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be an integer, got {other}"
        ))),
    }
}

fn value_to_string(value: &Value) -> Option<&str> {
    match value {
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.as_str()),
        _ => None,
    }
}

pub(crate) fn expr_as_timestamp_ms(expr: &Expr, arg_name: &str) -> Result<i64> {
    match expr {
        // TIMESTAMP '…' / DATE '…'
        Expr::TypedString(typed) => {
            let raw = value_to_string(&typed.value.value).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` TIMESTAMP payload must be a string, got {}",
                    typed.value.value
                ))
            })?;
            parse_timestamp_to_ms(raw)
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => parse_timestamp_to_ms(text),
        Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => raw.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` is not a timestamp or epoch-ms integer: {raw}"
            ))
        }),
        Expr::Cast { expr, .. } => expr_as_timestamp_ms(expr, arg_name),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be a TIMESTAMP literal, string, or epoch-ms \
             integer, got {other}"
        ))),
    }
}

pub(crate) fn expr_as_f64(expr: &Expr, arg_name: &str) -> Result<f64> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => raw.parse::<f64>().map_err(|_| {
            DataFusionError::Plan(format!("CALL argument `{arg_name}` is not a number: {raw}"))
        }),
        Expr::UnaryOp {
            op: datafusion::sql::sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => Ok(-expr_as_f64(expr, arg_name)?),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => text.trim().parse::<f64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` string is not a number: {text}"
            ))
        }),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be a number, got {other}"
        ))),
    }
}

/// A boolean CALL argument.
pub(crate) fn expr_as_bool(expr: &Expr, name: &str) -> Result<bool> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(value),
            ..
        }) => Ok(*value),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{name}` must be a boolean literal (true / false), got `{other}`"
        ))),
    }
}

#[allow(dead_code)]
pub(crate) fn expr_as_string_array(expr: &Expr, arg_name: &str) -> Result<Vec<String>> {
    match expr {
        Expr::Array(array) => array
            .elem
            .iter()
            .map(|element| expr_as_string(element, arg_name))
            .collect(),
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("array") => {
            match &function.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(element)) => {
                            expr_as_string(element, arg_name)
                        }
                        other => Err(DataFusionError::Plan(format!(
                            "CALL argument `{arg_name}` must be an array of string \
                             literals, got {other}"
                        ))),
                    })
                    .collect(),
                other => Err(DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` must be an array of string \
                     literals, got {other}"
                ))),
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be an array of string \
             literals, got {other}"
        ))),
    }
}

#[allow(dead_code)]
pub(crate) fn expr_as_i64_array(expr: &Expr, arg_name: &str) -> Result<Vec<i64>> {
    match expr {
        Expr::Array(array) => array
            .elem
            .iter()
            .map(|element| expr_as_i64(element, arg_name))
            .collect(),
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("array") => {
            match &function.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(element)) => {
                            expr_as_i64(element, arg_name)
                        }
                        other => Err(DataFusionError::Plan(format!(
                            "CALL argument `{arg_name}` must be an array of integer \
                             literals, got {other}"
                        ))),
                    })
                    .collect(),
                other => Err(DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` must be an array of integer \
                     literals, got {other}"
                ))),
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be an array of integer \
             literals, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{SelectItem, SetExpr, Statement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn select_expr(sql: &str) -> Expr {
        let statements = Parser::parse_sql(&GenericDialect {}, sql).expect("test sql parses");
        let Statement::Query(query) = &statements[0] else {
            panic!("test sql must be a query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("test sql must be a plain SELECT");
        };
        let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("test SELECT must project one expression");
        };
        expr.clone()
    }

    fn test_params() -> Vec<ParamDecl> {
        vec![
            ParamDecl {
                name: "table",
                data_type: "StringType",
                required: true,
            },
            ParamDecl {
                name: "strategy",
                data_type: "StringType",
                required: false,
            },
            ParamDecl {
                name: "options",
                data_type: "None",
                required: false,
            },
        ]
    }

    fn plan_message(error: DataFusionError) -> String {
        let DataFusionError::Plan(message) = error else {
            panic!("expected a Plan error, got {error}");
        };
        message
    }

    #[test]
    fn bind_positional_follows_declared_order_and_null_means_unset() {
        let args = CallArgs {
            named: HashMap::from([("options".to_string(), select_expr("SELECT 'o'"))]),
            positional: vec![select_expr("SELECT 't'"), select_expr("SELECT NULL")],
        };
        let bound = bind(&args, &test_params(), &[]).expect("bind");
        assert_eq!(bound.require_string("table").expect("table"), "t");
        assert_eq!(bound.optional_string("strategy").expect("strategy"), None);
        assert_eq!(
            bound.optional_string("options").expect("options"),
            Some("o".to_string())
        );
        assert_eq!(bound.get("missing"), None);
    }

    #[test]
    fn bind_unknown_named_names_allowed() {
        let args = CallArgs {
            named: HashMap::from([("bogus".to_string(), select_expr("SELECT 1"))]),
            positional: Vec::new(),
        };
        let error = bind(&args, &test_params(), &["extra"]).expect_err("unknown must refuse");
        assert_eq!(
            plan_message(error),
            "unknown CALL argument `bogus`; allowed: table, strategy, options, extra"
        );
    }

    #[test]
    fn bind_excess_positional_names_arity() {
        let args = CallArgs {
            named: HashMap::new(),
            positional: vec![
                select_expr("SELECT 't'"),
                select_expr("SELECT 's'"),
                select_expr("SELECT 'o'"),
                select_expr("SELECT 'x'"),
            ],
        };
        let error = bind(&args, &test_params(), &[]).expect_err("excess must refuse");
        assert_eq!(
            plan_message(error),
            "CALL accepts at most 3 positional argument(s); got 4"
        );
    }

    #[test]
    fn bind_duplicate_positional_and_named_refuses() {
        let args = CallArgs {
            named: HashMap::from([("table".to_string(), select_expr("SELECT 'n'"))]),
            positional: vec![select_expr("SELECT 't'")],
        };
        let error = bind(&args, &test_params(), &[]).expect_err("duplicate must refuse");
        assert_eq!(
            plan_message(error),
            "CALL argument `table` is bound twice (positionally and by name)"
        );
    }

    #[test]
    fn bind_missing_required_names_position() {
        let args = CallArgs {
            named: HashMap::from([("strategy".to_string(), select_expr("SELECT 's'"))]),
            positional: Vec::new(),
        };
        let error = bind(&args, &test_params(), &[]).expect_err("missing must refuse");
        assert_eq!(
            plan_message(error),
            "CALL argument `table` is required (named `table => …` or positional #0)"
        );
        let nulled = CallArgs {
            named: HashMap::from([("table".to_string(), select_expr("SELECT NULL"))]),
            positional: Vec::new(),
        };
        let error = bind(&nulled, &test_params(), &[]).expect_err("null required must refuse");
        assert_eq!(
            plan_message(error),
            "CALL argument `table` is required (named `table => …` or positional #0)"
        );
    }

    #[test]
    fn expr_as_string_array_accepts_call_and_literal() {
        assert_eq!(
            expr_as_string_array(&select_expr("SELECT array('a', 'b')"), "columns")
                .expect("call form"),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            expr_as_string_array(&select_expr("SELECT ARRAY['a', 'b']"), "columns")
                .expect("literal form"),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn expr_as_string_array_rejects_non_arrays_and_non_strings() {
        let error = expr_as_string_array(&select_expr("SELECT 'x'"), "columns")
            .expect_err("scalar must refuse");
        assert_eq!(
            plan_message(error),
            "CALL argument `columns` must be an array of string literals, got 'x'"
        );
        let error = expr_as_string_array(&select_expr("SELECT array('a', 1)"), "columns")
            .expect_err("mixed element must refuse");
        assert!(plan_message(error).contains("must be a string literal"));
    }

    #[test]
    fn expr_as_i64_array_accepts_call_and_literal() {
        assert_eq!(
            expr_as_i64_array(&select_expr("SELECT array(1, -2)"), "snapshot_ids")
                .expect("call form"),
            vec![1, -2]
        );
        assert_eq!(
            expr_as_i64_array(&select_expr("SELECT ARRAY[3]"), "snapshot_ids").expect("literal"),
            vec![3]
        );
        let error = expr_as_i64_array(&select_expr("SELECT 'x'"), "snapshot_ids")
            .expect_err("scalar must refuse");
        assert_eq!(
            plan_message(error),
            "CALL argument `snapshot_ids` must be an array of integer literals, got 'x'"
        );
    }

    #[test]
    fn expr_as_i64_unary_minus_min_refuses_overflow() {
        use datafusion::sql::sqlparser::ast::{
            Expr as AstExpr, UnaryOperator, Value as AstValue, ValueWithSpan,
        };
        use datafusion::sql::sqlparser::tokenizer::Span;
        let min = AstExpr::Value(ValueWithSpan {
            value: AstValue::Number(i64::MIN.to_string(), false),
            span: Span::empty(),
        });
        let negated = AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(min),
        };
        // -i64::MIN cannot be represented — must Plan-error, not panic/wrap (C1-SAF-001).
        assert!(expr_as_i64(&negated, "snapshot_id").is_err());
    }
}
