//! CALL argument parsing and scalar coercion helpers.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan,
};
use repark_core::parse_timestamp_to_ms;

// Argument bag

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
                if !named.is_empty() && !positional.is_empty() {
                    return Err(DataFusionError::Plan(
                        "CALL does not support mixing named and positional arguments \
                         (Iceberg Spark Procedures — named or positional, not both)"
                            .to_string(),
                    ));
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

#[cfg(test)]
mod tests {
    use super::*;

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
