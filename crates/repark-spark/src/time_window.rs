use datafusion::common::Result;
use datafusion::common::plan_err;
use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, OrderBy,
    OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins, WildcardAdditionalOptions,
};

const WINDOW_NAME: &str = "window";
const WINDOW_TIME_NAME: &str = "window_time";
const SESSION_FUNCTION_NAME: &str = "session_window";
const SESSION_OUTPUT_NAME: &str = "session_window";
const WINDOWED_ALIAS: &str = "__repark_windowed";

fn is_window_call(expression: &Expr) -> bool {
    match expression {
        Expr::Function(function) => is_window_function(function),
        _ => false,
    }
}

fn is_window_function(function: &Function) -> bool {
    function.name.to_string().eq_ignore_ascii_case(WINDOW_NAME)
}

fn is_window_time_function(function: &Function) -> bool {
    function
        .name
        .to_string()
        .eq_ignore_ascii_case(WINDOW_TIME_NAME)
}

fn function_call_arguments(function: &Function) -> Vec<&Expr> {
    let FunctionArguments::List(list) = &function.args else {
        return Vec::new();
    };
    list.args
        .iter()
        .filter_map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expression),
                ..
            } => Some(expression),
            _ => None,
        })
        .collect()
}

fn window_time_call_text(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Function(function) if is_window_time_function(function) => {
            Some(expression.to_string())
        }
        Expr::Function(function) => function_call_arguments(function)
            .iter()
            .find_map(|argument| window_time_call_text(argument)),
        Expr::Cast { expr, .. } => window_time_call_text(expr),
        Expr::AtTimeZone { timestamp, .. } => window_time_call_text(timestamp),
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => window_time_call_text(inner),
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            window_time_call_text(left).or_else(|| window_time_call_text(right))
        }
        Expr::InList { expr, list, .. } => {
            window_time_call_text(expr).or_else(|| list.iter().find_map(window_time_call_text))
        }
        Expr::Between {
            expr, low, high, ..
        } => window_time_call_text(expr)
            .or_else(|| window_time_call_text(low))
            .or_else(|| window_time_call_text(high)),
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            window_time_call_text(expr).or_else(|| window_time_call_text(pattern))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => operand
            .as_ref()
            .and_then(|inner| window_time_call_text(inner))
            .or_else(|| {
                conditions.iter().find_map(|when| {
                    window_time_call_text(&when.condition)
                        .or_else(|| window_time_call_text(&when.result))
                })
            })
            .or_else(|| {
                else_result
                    .as_ref()
                    .and_then(|inner| window_time_call_text(inner))
            }),
        Expr::Tuple(items) => items.iter().find_map(window_time_call_text),
        _ => None,
    }
}

fn select_item_window_time(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expression) => window_time_call_text(expression),
        SelectItem::ExprWithAlias { expr, .. } => window_time_call_text(expr),
        _ => None,
    }
}

fn replace_matching_call(expression: &mut Expr, call_text: &str, name: &str) {
    let is_target = match expression {
        Expr::Function(function) => function.name.to_string().eq_ignore_ascii_case(name),
        _ => false,
    };
    if is_target && expression.to_string() == call_text {
        *expression = Expr::Identifier(Ident::new(name));
    }
}

fn replace_bare_call(expression: &mut Expr, call_text: &str) {
    replace_matching_call(expression, call_text, WINDOW_NAME);
}

fn replace_session_call(expression: &mut Expr, call_text: &str) {
    replace_matching_call(expression, call_text, SESSION_OUTPUT_NAME);
}

fn replace_item_call(item: &mut SelectItem, call_text: &str) {
    match item {
        SelectItem::UnnamedExpr(expression) => replace_bare_call(expression, call_text),
        SelectItem::ExprWithAlias { expr, .. } => replace_bare_call(expr, call_text),
        _ => {}
    }
}

fn replace_session_item_call(item: &mut SelectItem, call_text: &str) {
    match item {
        SelectItem::UnnamedExpr(expression) => replace_session_call(expression, call_text),
        SelectItem::ExprWithAlias { expr, .. } => replace_session_call(expr, call_text),
        _ => {}
    }
}

fn is_session_call(expression: &Expr) -> bool {
    match expression {
        Expr::Function(function) => is_session_function(function),
        _ => false,
    }
}

fn is_session_function(function: &Function) -> bool {
    function
        .name
        .to_string()
        .eq_ignore_ascii_case(SESSION_FUNCTION_NAME)
}

fn group_session_call(group_items: &[Expr]) -> Result<Option<(String, Expr)>> {
    let mut call_text: Option<String> = None;
    let mut call_expression: Option<Expr> = None;
    for item in group_items {
        if !is_session_call(item) {
            continue;
        }
        let text = item.to_string();
        match call_text.as_ref() {
            Some(previous) if previous != &text => {
                return plan_err!(
                    "{}",
                    repark_functions::spark_session_window::MULTIPLE_SESSION_EXPRESSIONS
                );
            }
            Some(_) => {}
            None => {
                call_text = Some(text);
                call_expression = Some(item.clone());
            }
        }
    }
    Ok(call_text.zip(call_expression))
}

fn grouped_window_time_text(select: &Select, order: Option<&OrderBy>) -> Option<String> {
    select
        .projection
        .iter()
        .find_map(select_item_window_time)
        .or_else(|| select.having.as_ref().and_then(window_time_call_text))
        .or_else(|| {
            order.and_then(|clause| match &clause.kind {
                OrderByKind::Expressions(items) => items
                    .iter()
                    .find_map(|item| window_time_call_text(&item.expr)),
                OrderByKind::All(_) => None,
            })
        })
}

fn group_window_call(group_items: &[Expr]) -> Result<Option<(String, Expr)>> {
    let mut call_text: Option<String> = None;
    let mut call_expression: Option<Expr> = None;
    for item in group_items {
        if !is_window_call(item) {
            continue;
        }
        let text = item.to_string();
        match call_text.as_ref() {
            Some(previous) if previous != &text => {
                return plan_err!(
                    "'window' takes one window specification per query block; found a second one"
                );
            }
            Some(_) => {}
            None => {
                call_text = Some(text);
                call_expression = Some(item.clone());
            }
        }
    }
    Ok(call_text.zip(call_expression))
}

pub(crate) fn wrap_time_window_grouping(statement: &mut Statement) -> Result<bool> {
    let Statement::Query(query) = statement else {
        return Ok(false);
    };
    wrap_query(query)
}

fn wrap_query(query: &mut Query) -> Result<bool> {
    let mut applied = false;
    if let Some(with) = query.with.as_mut() {
        for table in &mut with.cte_tables {
            applied |= wrap_query(&mut table.query)?;
        }
    }
    applied |= wrap_set_expr(&mut query.body, query.order_by.as_ref())?;
    Ok(applied)
}

fn wrap_set_expr(body: &mut SetExpr, order: Option<&OrderBy>) -> Result<bool> {
    match body {
        SetExpr::Select(select) => {
            let mut applied = false;
            for table in &mut select.from {
                applied |= wrap_table_factor(&mut table.relation)?;
                for join in &mut table.joins {
                    applied |= wrap_table_factor(&mut join.relation)?;
                }
            }
            applied |= wrap_select_grouping(select, order)?;
            Ok(applied)
        }
        SetExpr::Query(inner) => wrap_query(inner),
        SetExpr::SetOperation { left, right, .. } => {
            Ok(wrap_set_expr(left, None)? | wrap_set_expr(right, None)?)
        }
        _ => Ok(false),
    }
}

fn wrap_table_factor(factor: &mut TableFactor) -> Result<bool> {
    match factor {
        TableFactor::Derived { subquery, .. } => wrap_query(subquery),
        _ => Ok(false),
    }
}

fn wrap_select_grouping(select: &mut Select, order: Option<&OrderBy>) -> Result<bool> {
    let GroupByExpr::Expressions(group_items, _) = &select.group_by else {
        return Ok(false);
    };
    let window_spec = group_window_call(group_items)?;
    let session_spec = group_session_call(group_items)?;
    match (window_spec, session_spec) {
        (Some(_), Some(_)) => plan_err!(
            "'window' and 'session_window' cannot share one GROUP BY; use one window specification per query block"
        ),
        (Some((call_text, call_expression)), None) => {
            if let Some(text) = grouped_window_time_text(select, order) {
                return plan_err!(
                    "[MISSING_AGGREGATION] The non-aggregating expression \"{text}\" is based on columns \
                     which are not participating in the GROUP BY clause."
                );
            }
            if let GroupByExpr::Expressions(group_items, _) = &mut select.group_by {
                for item in group_items.iter_mut() {
                    replace_bare_call(item, call_text.as_str());
                }
            }
            for item in &mut select.projection {
                replace_item_call(item, call_text.as_str());
            }
            if let Some(having) = select.having.as_mut() {
                replace_bare_call(having, call_text.as_str());
            }
            stage_grouping_subquery(select, call_expression, WINDOW_NAME);
            Ok(true)
        }
        (None, Some((session_text, session_expression))) => {
            if let GroupByExpr::Expressions(group_items, _) = &mut select.group_by {
                for item in group_items.iter_mut() {
                    replace_session_call(item, session_text.as_str());
                }
            }
            for item in &mut select.projection {
                replace_session_item_call(item, session_text.as_str());
            }
            if let Some(having) = select.having.as_mut() {
                replace_session_call(having, session_text.as_str());
            }
            stage_grouping_subquery(select, session_expression, SESSION_OUTPUT_NAME);
            Ok(true)
        }
        (None, None) => Ok(false),
    }
}

fn stage_grouping_subquery(select: &mut Select, marker: Expr, alias_name: &str) {
    let mut inner = select.clone();
    inner.projection = vec![
        SelectItem::Wildcard(WildcardAdditionalOptions::default()),
        SelectItem::ExprWithAlias {
            expr: marker,
            alias: Ident::new(alias_name),
        },
    ];
    inner.from = std::mem::take(&mut select.from);
    inner.selection = select.selection.take();
    inner.lateral_views = std::mem::take(&mut select.lateral_views);
    inner.prewhere = select.prewhere.take();
    inner.connect_by = std::mem::take(&mut select.connect_by);
    inner.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
    inner.cluster_by = Vec::new();
    inner.distribute_by = Vec::new();
    inner.sort_by = Vec::new();
    inner.having = None;
    inner.named_window = Vec::new();
    inner.qualify = None;
    inner.value_table_mode = None;
    inner.distinct = None;
    inner.select_modifiers = None;
    inner.top = None;
    inner.exclude = None;
    inner.into = None;
    let inner_query = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(inner))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    select.from = vec![TableWithJoins {
        relation: TableFactor::Derived {
            lateral: false,
            subquery: Box::new(inner_query),
            alias: Some(TableAlias {
                explicit: false,
                name: Ident::new(WINDOWED_ALIAS),
                columns: Vec::new(),
                at: None,
            }),
            sample: None,
        },
        joins: Vec::new(),
    }];
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn wrapped(sql: &str) -> String {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        let applied = wrap_time_window_grouping(&mut statements[0]).unwrap();
        assert!(applied);
        statements[0].to_string()
    }

    fn untouched(sql: &str) -> String {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        let applied = wrap_time_window_grouping(&mut statements[0]).unwrap();
        assert!(!applied);
        statements[0].to_string()
    }

    fn refused(sql: &str) -> String {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        wrap_time_window_grouping(&mut statements[0])
            .expect_err("must refuse")
            .to_string()
    }

    #[test]
    fn group_by_window_wraps_from_and_names_window() {
        let rewritten = wrapped(
            "SELECT CAST(window.start AS STRING) s, count(*) c FROM t GROUP BY window(ts, '10 minutes') ORDER BY s",
        );
        assert!(
            rewritten.contains("window(ts, '10 minutes') AS window"),
            "subquery computes window: {rewritten}"
        );
        assert!(
            rewritten.contains("GROUP BY window ORDER BY s"),
            "outer groups by the column: {rewritten}"
        );
        assert!(
            rewritten.contains("__repark_windowed"),
            "derived table is aliased: {rewritten}"
        );
    }

    #[test]
    fn bare_projection_call_resolves_to_the_column() {
        let rewritten = wrapped(
            "SELECT window(ts, '10 minutes'), count(*) FROM t GROUP BY window(ts, '10 minutes')",
        );
        assert!(
            !rewritten.contains("GROUP BY window("),
            "group call is replaced: {rewritten}"
        );
    }

    #[test]
    fn queries_without_window_grouping_pass_through() {
        let sql = "SELECT a, count(*) FROM t GROUP BY a";
        assert_eq!(untouched(sql), sql);
        let plain = "SELECT window(ts, '10 minutes') FROM t LIMIT 0";
        assert_eq!(untouched(plain), plain);
    }

    #[test]
    fn window_time_in_grouped_projection_reports_missing_aggregation() {
        let message = refused(
            "SELECT CAST(window_time(window) AS STRING) wt, count(*) FROM t GROUP BY window(ts, '10 minutes') ORDER BY wt",
        );
        assert!(
            message.contains("[MISSING_AGGREGATION]"),
            "expected the condition, got {message}"
        );
        assert!(
            message.contains("window_time(window)"),
            "expected the quoted call, got {message}"
        );
    }

    #[test]
    fn bare_and_nested_window_time_in_grouped_projection_refuse() {
        for sql in [
            "SELECT window_time(window), count(*) FROM t GROUP BY window(ts, '10 minutes')",
            "SELECT coalesce(CAST(window_time(window) AS STRING), 'x') wt, count(*) FROM t GROUP BY window(ts, '10 minutes')",
        ] {
            let message = refused(sql);
            assert!(
                message.contains("[MISSING_AGGREGATION]"),
                "{sql}: expected the condition, got {message}"
            );
        }
    }

    #[test]
    fn window_time_without_window_grouping_passes_through() {
        let sql = "SELECT window_time(w), count(*) FROM t GROUP BY w";
        assert_eq!(untouched(sql), sql);
    }

    #[test]
    fn group_by_session_stages_marker_and_names_column() {
        let rewritten = wrapped(
            "SELECT key, CAST(session_window.start AS STRING) s, count(*) c FROM t GROUP BY key, session_window(ts, '5 minutes') ORDER BY key, s",
        );
        assert!(
            rewritten.contains("session_window(ts, '5 minutes') AS session_window"),
            "subquery plants the marker: {rewritten}"
        );
        assert!(
            rewritten.contains("GROUP BY key, session_window ORDER BY key, s"),
            "outer groups by the column so the field refs resolve: {rewritten}"
        );
    }

    #[test]
    fn bare_session_projection_call_resolves_to_the_column() {
        let rewritten = wrapped(
            "SELECT session_window(ts, '5 minutes'), count(*) FROM t GROUP BY session_window(ts, '5 minutes')",
        );
        assert!(
            rewritten.contains("SELECT session_window,"),
            "projection call is replaced: {rewritten}"
        );
    }

    #[test]
    fn second_session_specification_refuses() {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(
            &dialect,
            "SELECT count(*) FROM t GROUP BY session_window(ts, '5 minutes'), session_window(ts, '10 minutes')",
        )
        .unwrap();
        let error = wrap_time_window_grouping(&mut statements[0]).expect_err("must refuse");
        assert!(
            error.to_string().contains("_LEGACY_ERROR_TEMP_1039"),
            "unexpected: {error}"
        );
    }

    #[test]
    fn mixed_window_and_session_refuses() {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(
            &dialect,
            "SELECT count(*) FROM t GROUP BY window(ts, '5 minutes'), session_window(ts, '5 minutes')",
        )
        .unwrap();
        let error = wrap_time_window_grouping(&mut statements[0]).expect_err("must refuse");
        assert!(
            error.to_string().contains("one window specification"),
            "unexpected: {error}"
        );
    }

    #[test]
    fn second_window_specification_refuses() {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(
            &dialect,
            "SELECT count(*) FROM t GROUP BY window(ts, '10 minutes'), window(ts, '5 minutes')",
        )
        .unwrap();
        let error = wrap_time_window_grouping(&mut statements[0]).expect_err("must refuse");
        assert!(
            error.to_string().contains("one window specification"),
            "unexpected: {error}"
        );
    }

    #[test]
    fn nested_group_by_window_stages_the_inner_query() {
        let rewritten = wrapped(
            "SELECT * FROM (SELECT CAST(window.start AS STRING) s, count(*) c FROM t GROUP BY window(ts, '10 minutes')) AS t ORDER BY s",
        );
        assert!(
            rewritten.contains("window(ts, '10 minutes') AS window"),
            "inner query plants the marker: {rewritten}"
        );
        assert!(
            rewritten.contains("__repark_windowed"),
            "derived table is aliased: {rewritten}"
        );
    }

    #[test]
    fn cte_group_by_window_stages_the_cte() {
        let rewritten = wrapped(
            "WITH w AS (SELECT CAST(window.start AS STRING) s, count(*) c FROM t GROUP BY window(ts, '10 minutes')) SELECT * FROM w ORDER BY s",
        );
        assert!(
            rewritten.contains("window(ts, '10 minutes') AS window"),
            "cte plants the marker: {rewritten}"
        );
    }

    #[test]
    fn union_branches_stage_each_grouping() {
        let rewritten = wrapped(
            "SELECT CAST(window.start AS STRING) s, count(*) c FROM t GROUP BY window(ts, '10 minutes') UNION ALL SELECT CAST(window.start AS STRING) s, count(*) c FROM t GROUP BY window(ts, '30 minutes') ORDER BY s",
        );
        assert!(
            rewritten.contains("window(ts, '10 minutes') AS window"),
            "first branch stages: {rewritten}"
        );
        assert!(
            rewritten.contains("window(ts, '30 minutes') AS window"),
            "second branch stages: {rewritten}"
        );
    }

    #[test]
    fn nested_group_by_session_stages_the_inner_query() {
        let rewritten = wrapped(
            "SELECT * FROM (SELECT key, CAST(session_window.start AS STRING) s, count(*) c FROM t GROUP BY key, session_window(ts, '5 minutes')) AS t ORDER BY s",
        );
        assert!(
            rewritten.contains("session_window(ts, '5 minutes') AS session_window"),
            "inner query plants the marker: {rewritten}"
        );
    }

    #[test]
    fn plain_nested_query_passes_through() {
        let sql = "SELECT * FROM (SELECT a, count(*) FROM t GROUP BY a) AS t ORDER BY a";
        assert_eq!(untouched(sql), sql);
    }
}
