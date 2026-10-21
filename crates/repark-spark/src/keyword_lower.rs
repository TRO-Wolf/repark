use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, Statement, TimezoneInfo,
    UnaryOperator, Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Span;
use repark_functions::collection::SQL_MAP_NAME;

mod ltz_values_cast;
mod ntz_cast_lower;

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

const CREATE_MAP_ARITY_MARKER: &str = "`create_map` requires an even number of arguments, got ";

fn map_call_argument_count(node: &Expr) -> Option<usize> {
    let Expr::Function(function) = node else {
        return None;
    };
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    let plain = list.clauses.is_empty()
        && list.duplicate_treatment.is_none()
        && function.over.is_none()
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.within_group.is_empty()
        && list
            .args
            .iter()
            .all(|argument| matches!(argument, FunctionArg::Unnamed(FunctionArgExpr::Expr(_))));
    (plain && name.value.eq_ignore_ascii_case("map")).then_some(list.args.len())
}

fn is_empty_map_call(node: &Expr) -> bool {
    map_call_argument_count(node) == Some(0)
}

fn is_odd_map_call(node: &Expr) -> bool {
    map_call_argument_count(node).is_some_and(|count| count % 2 == 1)
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

fn lower_map_call(node: &mut Expr) -> bool {
    if lower_empty_map_call(node) {
        return true;
    }
    if !is_odd_map_call(node) {
        return false;
    }
    if let Expr::Function(function) = node {
        function.name = ObjectName::from(vec![Ident::new(SQL_MAP_NAME)]);
    }
    true
}

fn lower_expression(node: &mut Expr) {
    if ntz_cast_lower::lower_timestamp_ntz_cast(node) || lower_map_call(node) {
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

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        ltz_values_cast::mark_values_timestamp_casts(query);
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
        if !ntz_cast_lower::lower_timestamp_ntz_cast(expr) && self.empty_maps {
            lower_map_call(expr);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        ltz_values_cast::mark_values_timestamp_casts(query);
        ControlFlow::Continue(())
    }
}

pub(crate) fn lower_timestamp_ns_casts<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut TimestampNsCastLower { empty_maps: false });
}

/// Lower every empty or odd-arity `map(...)` call and every `TIMESTAMP_NTZ` cast.
pub(crate) fn lower_empty_maps_and_timestamp_ns_casts<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut TimestampNsCastLower { empty_maps: true });
}

/// Lower every empty or odd-arity `map(...)` call.
pub(crate) fn lower_empty_map_calls<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut MapCallLower);
}

struct MapCallLower;

impl VisitorMut for MapCallLower {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        lower_map_call(expr);
        ControlFlow::Continue(())
    }
}

struct TimestampNsCastProbe;

impl Visitor for TimestampNsCastProbe {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if ltz_values_cast::has_values_timestamp_cast(query) {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Cast {
                data_type: DataType::TimestampNtz(_),
                array: false,
                format: None,
                ..
            } => ControlFlow::Break(()),
            Expr::TypedString(typed) if matches!(typed.data_type, DataType::TimestampNtz(_)) => {
                ControlFlow::Break(())
            }
            _ if is_empty_map_call(expr) || is_odd_map_call(expr) => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

/// Find an empty or odd-arity `map(...)` call or a timestamp cast that lowering rewrites.
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
            "[UNSUPPORTED_TIMESTAMP_NTZ] TIMESTAMP_NTZ inside a nested cast target (ARRAY, STRUCT \
             or MAP) is not supported yet; the scalar TIMESTAMP_NTZ literal and cast are. See TZ-6 \
             (docs/spark-sql-iceberg-parity.md). SQLSTATE: 0A000"
                .to_string(),
        )
    } else {
        error
    }
}

fn map_wrong_arity_error(error: DataFusionError) -> DataFusionError {
    let text = error.to_string();
    let Some((_, tail)) = text.split_once(CREATE_MAP_ARITY_MARKER) else {
        return error;
    };
    let count: String = tail.chars().take_while(char::is_ascii_digit).collect();
    if count.is_empty() {
        return error;
    }
    DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `map` requires 2n (n > 0) parameters but the \
         actual number is {count}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

pub(crate) fn map_door_keyword_errors(error: DataFusionError) -> DataFusionError {
    map_wrong_arity_error(map_unsupported_timestamp_ntz(
        crate::bare_nullary::map_bare_nullary_column_error(error),
    ))
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
    fn an_odd_map_call_lowers_to_the_internal_kernel_so_the_error_carries_the_count() {
        let text = lowered(
            "SELECT map('a', id, 'b') AS a, map('a') AS b, `MAP`('a', 1, 'b') AS c, \
             map('a', 1, 'b', 2, 'c') AS d FROM t",
        )
        .to_string();
        for expected in [
            "__repark_create_map__('a', id, 'b') AS a",
            "__repark_create_map__('a') AS b",
            "__repark_create_map__('a', 1, 'b') AS c",
            "__repark_create_map__('a', 1, 'b', 2, 'c') AS d",
        ] {
            assert!(text.contains(expected), "{expected}: {text}");
        }
    }

    #[test]
    fn an_even_or_unusual_map_call_is_left_to_the_planner() {
        let before = "SELECT map('a', id) AS a, map(k, 1, 'b', v) AS b, map(NULL, 1) AS c, \
                      s.map('a', id, 'b') AS d, map(a => 'x', b => id, c => 1) AS e, \
                      map(DISTINCT 'a', id, 'b') AS f, map('k', map('j', 1)) AS g";
        assert_eq!(lowered(before).to_string(), before);
    }

    #[test]
    fn lowering_a_map_call_twice_changes_nothing_more() {
        let mut statement =
            Parser::parse_sql(&DatabricksDialect {}, "SELECT map('a', id, 'b') AS m")
                .unwrap()
                .remove(0);
        lower_spark_keywords(&mut statement);
        let once = statement.to_string();
        lower_spark_keywords(&mut statement);
        assert_eq!(statement.to_string(), once);
    }

    #[test]
    fn the_probe_finds_an_odd_map_call_in_dml_and_merge() {
        let parse = |sql: &str| {
            Parser::parse_sql(&DatabricksDialect {}, sql)
                .unwrap()
                .remove(0)
        };
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "UPDATE t SET c = map('a', id, 'b') WHERE id = 0"
        )));
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET c = map('a')"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "UPDATE t SET c = map('a', id) WHERE id = 0"
        )));
        let mut merge =
            parse("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET c = map('a')");
        lower_empty_maps_and_timestamp_ns_casts(&mut merge);
        assert!(
            merge.to_string().contains("c = __repark_create_map__('a')"),
            "{merge}"
        );
        let mut update = parse("UPDATE t SET c = map('a', id, 'b'), d = map() WHERE id = 0");
        lower_empty_map_calls(&mut update);
        let text = update.to_string();
        assert!(
            text.contains("c = __repark_create_map__('a', id, 'b')"),
            "{text}"
        );
        assert!(
            text.contains("d = map(make_array(), make_array())"),
            "{text}"
        );
    }

    #[test]
    fn an_odd_argument_count_error_names_spark_map_and_the_count() {
        let error = DataFusionError::Plan(
            "Error during planning: Execution error: Function '__repark_create_map__' \
             user-defined coercion failed with: Execution error: \
             [WRONG_NUM_ARGS.WITHOUT_SUGGESTION] `create_map` requires an even number of \
             arguments, got 3. No function matches the given name and argument types."
                .to_string(),
        );
        let mapped = map_door_keyword_errors(error);
        assert!(matches!(mapped, DataFusionError::Plan(_)), "{mapped:?}");
        assert_eq!(
            mapped.to_string(),
            "Error during planning: [WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `map` requires 2n \
             (n > 0) parameters but the actual number is 3. Please, refer to \
             'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. \
             SQLSTATE: 42605"
        );
    }

    #[test]
    fn ltz_cast_lowers_to_bare_timestamp() {
        let text = lowered("SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_LTZ) AS v").to_string();
        assert!(!text.contains("TIMESTAMP_LTZ"), "{text}");
        assert!(text.contains("TIMESTAMP"), "{text}");
    }

    #[test]
    fn ns_casts_are_left_to_the_type_planner() {
        let before =
            "SELECT CAST('2026-01-02' AS TIMESTAMP_NS) AS a, '2026-01-02'::timestamptz_ns AS b";
        let text = lowered(before).to_string();
        assert_eq!(text, before);
        assert!(!text.contains("__repark_cast_timestamp"), "{text}");
    }

    #[test]
    fn the_probe_finds_ntz_casts_and_empty_map_calls_and_no_ns_cast() {
        let parse = |sql: &str| {
            Parser::parse_sql(&DatabricksDialect {}, sql)
                .unwrap()
                .remove(0)
        };
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT id FROM t WHERE ts > CAST('2026-01-02' AS timestamptz_ns)"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT '2026-01-02'::TIMESTAMP_NS AS v"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT CAST('2026-01-02' AS TIMESTAMP) AS v"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT TRY_CAST('x' AS timestamp_ns) AS v"
        )));
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT CAST('2024-01-01' AS TIMESTAMP_NTZ) AS v"
        )));
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT TRY_CAST('x' AS timestamp_ntz) AS v"
        )));
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS v"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "SELECT CAST(a AS ARRAY<TIMESTAMP_NTZ>) AS v"
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
            text.contains("[UNSUPPORTED_TIMESTAMP_NTZ]")
                && text.contains("nested cast target")
                && text.contains("the scalar TIMESTAMP_NTZ literal and cast are")
                && text.contains("TZ-6")
                && text.contains("SQLSTATE: 0A000"),
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
