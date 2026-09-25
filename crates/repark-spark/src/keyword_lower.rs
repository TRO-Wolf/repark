use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, TimezoneInfo, UnaryOperator,
    Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Span;
use repark_functions::timestamp_ns_cast::{TIMESTAMP_NS_CAST_NAME, TIMESTAMPTZ_NS_CAST_NAME};

fn null_expr() -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Null,
        span: Span::empty(),
    })
}

fn regexp_like_call(left: Box<Expr>, right: Box<Expr>) -> Expr {
    function_call("regexp_like", vec![*left, *right])
}

fn function_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn timestamp_ns_cast_name(data_type: &DataType) -> Option<&'static str> {
    let DataType::Custom(name, modifiers) = data_type else {
        return None;
    };
    if !modifiers.is_empty() {
        return None;
    }
    let spelled = name.to_string();
    if spelled.eq_ignore_ascii_case("timestamp_ns") {
        Some(TIMESTAMP_NS_CAST_NAME)
    } else if spelled.eq_ignore_ascii_case("timestamptz_ns") {
        Some(TIMESTAMPTZ_NS_CAST_NAME)
    } else {
        None
    }
}

fn lower_timestamp_ns_cast(node: &mut Expr) -> bool {
    let Expr::Cast {
        kind: CastKind::Cast | CastKind::DoubleColon,
        expr,
        data_type,
        array: false,
        format: None,
    } = node
    else {
        return false;
    };
    let Some(name) = timestamp_ns_cast_name(data_type) else {
        return false;
    };
    let value = std::mem::replace(expr, Box::new(null_expr()));
    *node = function_call(name, vec![*value]);
    true
}

fn is_empty_map_call(node: &Expr) -> bool {
    let Expr::Function(function) = node else {
        return false;
    };
    let empty_arguments = match &function.args {
        FunctionArguments::List(list) => list.args.is_empty() && list.clauses.is_empty(),
        FunctionArguments::None | FunctionArguments::Subquery(_) => false,
    };
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return false;
    };
    empty_arguments
        && function.over.is_none()
        && function.filter.is_none()
        && name.value.eq_ignore_ascii_case("map")
}

fn lower_empty_map_call(node: &mut Expr) -> bool {
    if !is_empty_map_call(node) {
        return false;
    }
    *node = function_call(
        "map",
        vec![
            function_call("make_array", Vec::new()),
            function_call("make_array", Vec::new()),
        ],
    );
    true
}

fn lower_expression(node: &mut Expr) {
    if lower_timestamp_ns_cast(node) || lower_empty_map_call(node) {
        return;
    }
    if let Expr::RLike {
        negated,
        expr,
        pattern,
        ..
    } = node
    {
        let negated = *negated;
        let left = std::mem::replace(expr, Box::new(null_expr()));
        let right = std::mem::replace(pattern, Box::new(null_expr()));
        let call = regexp_like_call(left, right);
        *node = if negated {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(call),
            }
        } else {
            call
        };
        return;
    }
    if let Expr::Cast { data_type, .. } = node
        && data_type.to_string().eq_ignore_ascii_case("timestamp_ltz")
    {
        *data_type = DataType::Timestamp(None, TimezoneInfo::None);
    }
}

struct KeywordLower;

impl VisitorMut for KeywordLower {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        lower_expression(expr);
        ControlFlow::Continue(())
    }
}

pub(crate) fn lower_spark_keywords(statement: &mut Statement) {
    let _ = statement.visit(&mut KeywordLower);
}

struct TimestampNsCastLower {
    empty_maps: bool,
}

impl VisitorMut for TimestampNsCastLower {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if !lower_timestamp_ns_cast(expr) && self.empty_maps {
            lower_empty_map_call(expr);
        }
        ControlFlow::Continue(())
    }
}

pub(crate) fn lower_timestamp_ns_casts<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut TimestampNsCastLower { empty_maps: false });
}

pub(crate) fn lower_empty_maps_and_timestamp_ns_casts<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut TimestampNsCastLower { empty_maps: true });
}

pub(crate) fn lower_empty_map_calls<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut EmptyMapLower);
}

struct EmptyMapLower;

impl VisitorMut for EmptyMapLower {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        lower_empty_map_call(expr);
        ControlFlow::Continue(())
    }
}

struct TimestampNsCastProbe;

impl Visitor for TimestampNsCastProbe {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                data_type,
                array: false,
                format: None,
                ..
            } if timestamp_ns_cast_name(data_type).is_some() => ControlFlow::Break(()),
            _ if is_empty_map_call(expr) => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

pub(crate) fn has_empty_map_or_timestamp_ns_cast<T: Visit>(node: &T) -> bool {
    node.visit(&mut TimestampNsCastProbe).is_break()
}

#[must_use]
pub(crate) fn map_unsupported_timestamp_ntz(error: DataFusionError) -> DataFusionError {
    if error
        .to_string()
        .contains("Unsupported SQL type TIMESTAMP_NTZ")
    {
        DataFusionError::Plan(
            "[UNSUPPORTED_TIMESTAMP_NTZ] The data type TIMESTAMP_NTZ is not a SQL-door type: \
             declare TIMESTAMP instead. The naive/instant contract is TZ-6 \
             (docs/spark-sql-iceberg-parity.md). SQLSTATE: 0A000"
                .to_string(),
        )
    } else {
        error
    }
}

pub(crate) fn map_door_keyword_errors(error: DataFusionError) -> DataFusionError {
    map_unsupported_timestamp_ntz(crate::bare_nullary::map_bare_nullary_column_error(error))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn lowered(sql: &str) -> Statement {
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap()
            .remove(0);
        lower_spark_keywords(&mut statement);
        statement
    }

    #[test]
    fn rlike_lowers_to_regexp_like() {
        let text = lowered("SELECT 'abc' RLIKE '^a' AS v").to_string();
        assert!(text.contains("regexp_like('abc', '^a')"), "{text}");
    }

    #[test]
    fn not_rlike_lowers_to_not_regexp_like() {
        let text = lowered("SELECT 'abc' NOT RLIKE '^a' AS v").to_string();
        assert!(text.contains("NOT regexp_like('abc', '^a')"), "{text}");
    }

    #[test]
    fn an_empty_map_call_lowers_to_a_map_of_two_empty_arrays() {
        let text =
            lowered("SELECT map() AS a, MAP() AS b, map('k', 1) AS c, s.map() AS d").to_string();
        assert!(
            text.contains("map(make_array(), make_array()) AS a")
                && text.contains("map(make_array(), make_array()) AS b"),
            "{text}"
        );
        assert!(text.contains("map('k', 1) AS c"), "{text}");
        assert!(text.contains("s.map() AS d"), "{text}");
        let quoted = lowered("SELECT `map`() AS a, `MAP`() AS b, `s`.`map`() AS c").to_string();
        assert!(
            quoted.contains("map(make_array(), make_array()) AS a")
                && quoted.contains("map(make_array(), make_array()) AS b")
                && quoted.contains("`s`.`map`() AS c"),
            "{quoted}"
        );
    }

    #[test]
    fn ltz_cast_lowers_to_bare_timestamp() {
        let text = lowered("SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_LTZ) AS v").to_string();
        assert!(!text.contains("TIMESTAMP_LTZ"), "{text}");
        assert!(text.contains("TIMESTAMP"), "{text}");
    }

    #[test]
    fn ns_casts_lower_to_the_embedded_cast_calls() {
        let text = lowered(
            "SELECT CAST('2026-01-02' AS TIMESTAMP_NS) AS a, '2026-01-02'::timestamptz_ns AS b",
        )
        .to_string();
        assert!(
            text.contains("__repark_cast_timestamp_ns__('2026-01-02')")
                && text.contains("__repark_cast_timestamptz_ns__('2026-01-02')"),
            "{text}"
        );
    }

    #[test]
    fn the_probe_finds_only_ns_casts_and_empty_map_calls() {
        let parse = |sql: &str| {
            Parser::parse_sql(&DatabricksDialect {}, sql)
                .unwrap()
                .remove(0)
        };
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT id FROM t WHERE ts > CAST('2026-01-02' AS timestamptz_ns)"
        )));
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT '2026-01-02'::TIMESTAMP_NS AS v"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT CAST('2026-01-02' AS TIMESTAMP) AS v"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT TRY_CAST('x' AS timestamp_ns) AS v"
        )));
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "UPDATE t SET c = `MAP`() WHERE id = 0"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "UPDATE t SET c = s.map() WHERE id = 0"
        )));
    }

    #[test]
    fn try_cast_to_ns_is_left_to_the_planner() {
        let text = lowered("SELECT TRY_CAST('x' AS timestamp_ns) AS v").to_string();
        assert!(text.contains("TRY_CAST"), "{text}");
    }

    #[test]
    fn other_casts_are_untouched() {
        let before = "SELECT CAST('1' AS INT) AS v";
        assert_eq!(lowered(before).to_string(), before);
    }

    #[test]
    fn ntz_refusal_names_the_registry_row() {
        let mapped = map_unsupported_timestamp_ntz(DataFusionError::NotImplemented(
            "This feature is not implemented: Unsupported SQL type TIMESTAMP_NTZ".to_string(),
        ));
        let text = mapped.to_string();
        assert!(
            text.contains("[UNSUPPORTED_TIMESTAMP_NTZ]") && text.contains("TZ-6"),
            "{text}"
        );
    }

    #[test]
    fn unrelated_errors_pass_through() {
        let error = DataFusionError::Plan("No field named 'revenue'.".to_string());
        let text = error.to_string();
        assert_eq!(map_door_keyword_errors(error).to_string(), text);
    }
}
