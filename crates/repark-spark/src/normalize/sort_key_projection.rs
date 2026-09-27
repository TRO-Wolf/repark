use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    BinaryOperator, CastKind, DataType, Distinct, DuplicateTreatment, ExactNumberInfo, Expr,
    Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, ObjectNamePart,
    OrderBy, OrderByExpr, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr, Statement,
    TableAlias, TableFactor, TableWithJoins, TimezoneInfo, Value, Visit, Visitor,
};

pub(crate) const SORT_SUBQUERY_ALIAS: &str = "__repark_sort";
const SEL_PREFIX: &str = "__repark_sort_sel_";
const KEY_PREFIX: &str = "__repark_sort_key_";

pub(crate) fn rewrite_statement(statement: &mut Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    let Some(planned) = plan(query) else {
        return false;
    };
    apply(query, planned);
    true
}

struct Planned {
    names: Vec<String>,
    extra: Vec<Expr>,
    keys: Vec<OrderByExpr>,
}

enum Placement {
    Select(usize),
    Key(usize),
    Verbatim,
}

enum KeyHit {
    Miss,
    One(usize),
    Many,
}

fn plan(query: &Query) -> Option<Planned> {
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let order_by = query.order_by.as_ref()?;
    if order_by.interpolate.is_some() {
        return None;
    }
    let OrderByKind::Expressions(keys) = &order_by.kind else {
        return None;
    };
    if keys.is_empty() || keys.iter().any(|key| key.with_fill.is_some()) {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !select_shape_ok(select) {
        return None;
    }
    let mut names = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        names.push(select_display_name(item)?);
    }
    let mut extra: Vec<Expr> = Vec::new();
    let mut rebound = false;
    let mut placed = Vec::with_capacity(keys.len());
    for key in keys {
        placed.push(place_key(
            &select.projection,
            &names,
            &key.expr,
            &mut extra,
            &mut rebound,
        )?);
    }
    if matches!(select.distinct, Some(Distinct::Distinct)) && !extra.is_empty() {
        return None;
    }
    if extra.is_empty() && !rebound {
        return None;
    }
    if query.to_string().contains(SORT_SUBQUERY_ALIAS) {
        return None;
    }
    let mut remapped = Vec::with_capacity(keys.len());
    for (key, placement) in keys.iter().zip(placed) {
        let mut next = key.clone();
        next.expr = match placement {
            Placement::Select(index) => Expr::Identifier(sel_alias(index)),
            Placement::Key(index) => Expr::Identifier(key_alias(index)),
            Placement::Verbatim => next.expr,
        };
        remapped.push(next);
    }
    Some(Planned {
        names,
        extra,
        keys: remapped,
    })
}

fn select_shape_ok(select: &Select) -> bool {
    matches!(
        select.distinct,
        None | Some(Distinct::Distinct | Distinct::All)
    ) && select.top.is_none()
        && select.into.is_none()
        && select.exclude.is_none()
        && select.prewhere.is_none()
        && select.connect_by.is_empty()
        && select.cluster_by.is_empty()
        && select.distribute_by.is_empty()
        && select.sort_by.is_empty()
        && select.select_modifiers.is_none()
        && select.value_table_mode.is_none()
        && matches!(select.flavor, SelectFlavor::Standard)
}

fn place_key(
    items: &[SelectItem],
    names: &[String],
    key: &Expr,
    extra: &mut Vec<Expr>,
    rebound: &mut bool,
) -> Option<Placement> {
    let bare = unnested(key);
    match bare {
        Expr::Value(literal) => match &literal.value {
            Value::Number(text, _) => {
                let ordinal = text.parse::<usize>().ok()?;
                if ordinal == 0 || ordinal > items.len() {
                    return None;
                }
                Some(Placement::Select(ordinal - 1))
            }
            _ => Some(Placement::Verbatim),
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let parts = ref_parts(bare)?;
            match single_match(items, &parts) {
                KeyHit::Many => None,
                KeyHit::One(index) => Some(Placement::Select(index)),
                KeyHit::Miss if parts.len() > 1 => {
                    if key_name_clashes(items, &parts) {
                        None
                    } else {
                        Some(new_key(extra, key.clone()))
                    }
                }
                KeyHit::Miss => match display_hit(names, &parts[0].value) {
                    KeyHit::Many => None,
                    KeyHit::One(index) => {
                        *rebound = true;
                        Some(Placement::Select(index))
                    }
                    KeyHit::Miss => Some(new_key(extra, key.clone())),
                },
            }
        }
        _ => {
            let refs = column_refs(key);
            if refs.is_empty() {
                return Some(Placement::Verbatim);
            }
            for reference in &refs {
                if reference.len() == 1 && items.iter().any(|item| alias_named(item, &reference[0]))
                {
                    return None;
                }
            }
            Some(new_key(extra, key.clone()))
        }
    }
}

fn new_key(extra: &mut Vec<Expr>, key: Expr) -> Placement {
    if let Some(index) = extra.iter().position(|known| known == &key) {
        return Placement::Key(index);
    }
    extra.push(key);
    Placement::Key(extra.len() - 1)
}

fn single_match(items: &[SelectItem], key: &[&Ident]) -> KeyHit {
    let mut hits = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if alias_hit(item, key) || column_hit(item, key) {
            hits.push(index);
        }
    }
    if hits.len() > 1 {
        return KeyHit::Many;
    }
    hits.into_iter().next().map_or(KeyHit::Miss, KeyHit::One)
}

fn alias_hit(item: &SelectItem, key: &[&Ident]) -> bool {
    if key.len() != 1 {
        return false;
    }
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.eq_ignore_ascii_case(&key[0].value),
        _ => false,
    }
}

fn alias_named(item: &SelectItem, name: &str) -> bool {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.eq_ignore_ascii_case(name),
        _ => false,
    }
}

fn column_hit(item: &SelectItem, key: &[&Ident]) -> bool {
    let SelectItem::UnnamedExpr(expr) = item else {
        return false;
    };
    let Some(item_parts) = ref_parts(unnested(expr)) else {
        return false;
    };
    let Some((item_name, item_qual)) = item_parts.split_last() else {
        return false;
    };
    let Some((key_name, key_qual)) = key.split_last() else {
        return false;
    };
    if !item_name.value.eq_ignore_ascii_case(&key_name.value) {
        return false;
    }
    if key_qual.is_empty() {
        return true;
    }
    item_qual.len() == key_qual.len()
        && item_qual
            .iter()
            .zip(key_qual.iter())
            .all(|(left, right)| left.value.eq_ignore_ascii_case(&right.value))
}

fn display_hit(names: &[String], key: &str) -> KeyHit {
    let mut hits = names
        .iter()
        .enumerate()
        .filter(|(_, name)| name.eq_ignore_ascii_case(key))
        .map(|(index, _)| index);
    match (hits.next(), hits.next()) {
        (Some(_), Some(_)) => KeyHit::Many,
        (Some(index), None) => KeyHit::One(index),
        (None, _) => KeyHit::Miss,
    }
}

fn key_name_clashes(items: &[SelectItem], key: &[&Ident]) -> bool {
    let Some(key_name) = key.last() else {
        return false;
    };
    items.iter().any(|item| {
        let SelectItem::UnnamedExpr(expr) = item else {
            return false;
        };
        ref_parts(unnested(expr)).is_some_and(|parts| {
            parts
                .last()
                .is_some_and(|last| last.value.eq_ignore_ascii_case(&key_name.value))
        })
    })
}

fn ref_parts(expr: &Expr) -> Option<Vec<&Ident>> {
    match expr {
        Expr::Identifier(ident) => Some(vec![ident]),
        Expr::CompoundIdentifier(parts) => Some(parts.iter().collect()),
        _ => None,
    }
}

fn unnested(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

fn is_bare_column(expr: &Expr) -> bool {
    matches!(
        unnested(expr),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
    )
}

struct RefWalk {
    refs: Vec<Vec<String>>,
}

impl Visitor for RefWalk {
    type Break = std::convert::Infallible;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => {
                self.refs.push(vec![ident.value.clone()]);
            }
            Expr::CompoundIdentifier(parts) => {
                self.refs
                    .push(parts.iter().map(|part| part.value.clone()).collect());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn column_refs(expr: &Expr) -> Vec<Vec<String>> {
    let mut walk = RefWalk { refs: Vec::new() };
    let _ = expr.visit(&mut walk);
    walk.refs
}

fn sel_alias(index: usize) -> Ident {
    Ident::new(format!("{SEL_PREFIX}{index}"))
}

fn key_alias(index: usize) -> Ident {
    Ident::new(format!("{KEY_PREFIX}{index}"))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Render {
    Display,
    Sql,
}

fn select_display_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => render_expression(expr, Render::Display),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::ExprWithAliases { .. }
        | SelectItem::QualifiedWildcard(..)
        | SelectItem::Wildcard(_) => None,
    }
}

fn render_expression(expr: &Expr, mode: Render) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
        Expr::Nested(inner) => render_expression(inner, mode),
        Expr::Cast {
            kind,
            expr,
            data_type,
            array: false,
            format: None,
        } => {
            if mode == Render::Display
                && matches!(kind, CastKind::Cast | CastKind::DoubleColon)
                && is_bare_column(expr)
            {
                return render_expression(expr, mode);
            }
            if !matches!(kind, CastKind::Cast) {
                return None;
            }
            Some(format!(
                "CAST({} AS {})",
                render_expression(expr, Render::Sql)?,
                spark_type_name(data_type)?
            ))
        }
        Expr::BinaryOp { left, op, right } => Some(format!(
            "({} {} {})",
            render_expression(left, mode)?,
            binary_symbol(op)?,
            render_expression(right, mode)?
        )),
        Expr::Function(function) => render_function(function, mode),
        Expr::Value(literal) => render_literal(&literal.value, mode),
        _ => None,
    }
}

fn render_function(function: &Function, mode: Render) -> Option<String> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.over.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    let FunctionArguments::List(list) = &function.args else {
        if matches!(function.args, FunctionArguments::None) {
            return Some(format!("{}()", name.value.to_ascii_lowercase()));
        }
        return None;
    };
    if !list.clauses.is_empty() {
        return None;
    }
    let distinct = match &list.duplicate_treatment {
        None => String::new(),
        Some(DuplicateTreatment::Distinct) => String::from("DISTINCT "),
        Some(DuplicateTreatment::All) => return None,
    };
    let mut args = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return None;
        };
        args.push(render_expression(expr, mode)?);
    }
    Some(format!(
        "{}({distinct}{})",
        name.value.to_ascii_lowercase(),
        args.join(", ")
    ))
}

fn render_literal(value: &Value, mode: Render) -> Option<String> {
    match value {
        Value::Number(text, _) => Some(text.clone()),
        Value::SingleQuotedString(text) => match mode {
            Render::Display => Some(text.clone()),
            Render::Sql => Some(format!("'{}'", text.replace('\'', "''"))),
        },
        Value::Boolean(true) => Some(String::from("true")),
        Value::Boolean(false) => Some(String::from("false")),
        Value::Null => Some(String::from("NULL")),
        _ => None,
    }
}

fn binary_symbol(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Plus => Some("+"),
        BinaryOperator::Minus => Some("-"),
        BinaryOperator::Multiply => Some("*"),
        BinaryOperator::Divide => Some("/"),
        BinaryOperator::Modulo => Some("%"),
        BinaryOperator::StringConcat => Some("||"),
        BinaryOperator::Gt => Some(">"),
        BinaryOperator::Lt => Some("<"),
        BinaryOperator::GtEq => Some(">="),
        BinaryOperator::LtEq => Some("<="),
        BinaryOperator::Eq => Some("="),
        BinaryOperator::NotEq => Some("<>"),
        BinaryOperator::And => Some("AND"),
        BinaryOperator::Or => Some("OR"),
        _ => None,
    }
}

fn spark_type_name(data_type: &DataType) -> Option<String> {
    match data_type {
        DataType::String(_) => Some(String::from("STRING")),
        DataType::TinyInt(_) => Some(String::from("TINYINT")),
        DataType::SmallInt(_) => Some(String::from("SMALLINT")),
        DataType::Int(_) | DataType::Integer(_) => Some(String::from("INT")),
        DataType::BigInt(_) => Some(String::from("BIGINT")),
        DataType::Float(_) => Some(String::from("FLOAT")),
        DataType::Double(_) | DataType::DoublePrecision => Some(String::from("DOUBLE")),
        DataType::Boolean | DataType::Bool => Some(String::from("BOOLEAN")),
        DataType::Date => Some(String::from("DATE")),
        DataType::Timestamp(_, TimezoneInfo::None) => Some(String::from("TIMESTAMP")),
        DataType::Binary(_) | DataType::Varbinary(_) | DataType::Bytea => {
            Some(String::from("BINARY"))
        }
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => {
            Some(decimal_name(info))
        }
        _ => None,
    }
}

fn decimal_name(info: &ExactNumberInfo) -> String {
    match info {
        ExactNumberInfo::None => String::from("DECIMAL"),
        ExactNumberInfo::Precision(precision) => format!("DECIMAL({precision})"),
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
    }
}

fn apply(query: &mut Query, planned: Planned) {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return;
    };
    let mut inner = select.clone();
    let mut projection = Vec::with_capacity(planned.names.len() + planned.extra.len());
    for (index, item) in select.projection.iter().enumerate() {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            SelectItem::ExprWithAliases { .. }
            | SelectItem::QualifiedWildcard(..)
            | SelectItem::Wildcard(_) => return,
        };
        projection.push(SelectItem::ExprWithAlias {
            expr,
            alias: sel_alias(index),
        });
    }
    for (index, key) in planned.extra.iter().enumerate() {
        projection.push(SelectItem::ExprWithAlias {
            expr: key.clone(),
            alias: key_alias(index),
        });
    }
    inner.projection = projection;
    let inner_query = Query {
        with: None,
        body: Box::new(SetExpr::Select(inner)),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    let derived = TableWithJoins {
        relation: TableFactor::Derived {
            lateral: false,
            subquery: Box::new(inner_query),
            alias: Some(TableAlias {
                explicit: true,
                name: Ident::new(SORT_SUBQUERY_ALIAS),
                columns: Vec::new(),
                at: None,
            }),
            sample: None,
        },
        joins: Vec::new(),
    };
    let mut outer_projection = Vec::with_capacity(planned.names.len());
    for (index, name) in planned.names.iter().enumerate() {
        outer_projection.push(SelectItem::ExprWithAlias {
            expr: Expr::Identifier(sel_alias(index)),
            alias: Ident::with_quote('"', name.clone()),
        });
    }
    select.distinct = None;
    select.top = None;
    select.top_before_distinct = false;
    select.projection = outer_projection;
    select.exclude = None;
    select.into = None;
    select.from = vec![derived];
    select.lateral_views = Vec::new();
    select.prewhere = None;
    select.selection = None;
    select.connect_by = Vec::new();
    select.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
    select.cluster_by = Vec::new();
    select.distribute_by = Vec::new();
    select.sort_by = Vec::new();
    select.having = None;
    select.named_window = Vec::new();
    select.qualify = None;
    select.window_before_qualify = false;
    select.value_table_mode = None;
    select.optimizer_hints = Vec::new();
    query.order_by = Some(OrderBy {
        kind: OrderByKind::Expressions(planned.keys),
        interpolate: None,
    });
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::rewrite_statement;

    fn rewritten(sql: &str) -> String {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(rewrite_statement(&mut statements[0]), "must fire: {sql}");
        statements[0].to_string()
    }

    fn untouched(sql: &str) -> String {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(!rewrite_statement(&mut statements[0]), "must bail: {sql}");
        statements[0].to_string()
    }

    #[test]
    fn cast_over_its_key_binds_the_output_column() {
        let out = rewritten("SELECT CAST(ts AS STRING) FROM t ORDER BY ts");
        assert!(
            out.contains("CAST(ts AS STRING) AS __repark_sort_sel_0"),
            "{out}"
        );
        assert!(!out.contains("__repark_sort_key"), "{out}");
        assert!(out.contains("__repark_sort_sel_0 AS \"ts\""), "{out}");
        assert!(out.contains("ORDER BY __repark_sort_sel_0"), "{out}");
    }

    #[test]
    fn bare_key_matching_a_display_name_binds_the_output_column() {
        let out = rewritten("SELECT CAST(id AS STRING) FROM t ORDER BY id");
        assert!(!out.contains("__repark_sort_key"), "{out}");
        assert!(out.contains("ORDER BY __repark_sort_sel_0"), "{out}");
        let out = rewritten("SELECT CAST(id AS STRING) FROM t ORDER BY id DESC");
        assert!(out.contains("ORDER BY __repark_sort_sel_0 DESC"), "{out}");
    }

    #[test]
    fn qualified_key_keeps_binding_the_column() {
        let out = rewritten("SELECT CAST(id AS STRING) FROM t ORDER BY t.id");
        assert!(out.contains("t.id AS __repark_sort_key_0"), "{out}");
        assert!(out.contains("ORDER BY __repark_sort_key_0"), "{out}");
    }

    #[test]
    fn order_options_and_limit_stay_on_the_outer_query() {
        let out = rewritten("SELECT CAST(ts AS STRING) FROM t ORDER BY ts DESC NULLS LAST LIMIT 1");
        assert!(
            out.contains("ORDER BY __repark_sort_sel_0 DESC NULLS LAST LIMIT 1"),
            "{out}"
        );
    }

    #[test]
    fn aliased_cast_with_base_key_projects_the_key() {
        let out = rewritten("SELECT CAST(ts AS STRING) AS t2 FROM t ORDER BY ts");
        assert!(out.contains("__repark_sort_sel_0 AS \"t2\""), "{out}");
        assert!(out.contains("ORDER BY __repark_sort_key_0"), "{out}");
    }

    #[test]
    fn expression_keys_become_key_items_with_spark_outer_names() {
        let out = rewritten("SELECT id + 1 FROM t ORDER BY id");
        assert!(out.contains("__repark_sort_sel_0 AS \"(id + 1)\""), "{out}");
        let out = rewritten("SELECT upper(s) FROM t ORDER BY s");
        assert!(out.contains("__repark_sort_sel_0 AS \"upper(s)\""), "{out}");
    }

    #[test]
    fn cast_of_aggregate_names_the_full_cast() {
        let out = rewritten("SELECT CAST(max(ts) AS STRING) FROM t GROUP BY s ORDER BY max(ts)");
        assert!(
            out.contains("__repark_sort_sel_0 AS \"CAST(max(ts) AS STRING)\""),
            "{out}"
        );
        assert!(out.contains("max(ts) AS __repark_sort_key_0"), "{out}");
        assert!(out.contains("GROUP BY s"), "{out}");
    }

    #[test]
    fn distinct_keeps_the_key_inside_the_dedup() {
        let out = rewritten("SELECT DISTINCT CAST(ts AS STRING) FROM t ORDER BY ts");
        assert!(out.contains("SELECT DISTINCT"), "{out}");
        assert!(out.contains("__repark_sort_sel_0 AS \"ts\""), "{out}");
        assert!(out.contains("ORDER BY __repark_sort_sel_0"), "{out}");
    }

    #[test]
    fn distinct_with_a_key_that_cannot_bind_bails() {
        untouched("SELECT DISTINCT s FROM t ORDER BY ts");
        untouched("SELECT DISTINCT CAST(s AS STRING) FROM t ORDER BY ts");
        untouched("SELECT DISTINCT s FROM t ORDER BY st.s");
    }

    #[test]
    fn ordinal_keys_resolve_to_select_items() {
        let before = untouched("SELECT id, ts FROM t ORDER BY 1");
        assert!(before.contains("ORDER BY 1"), "{before}");
        let out = rewritten("SELECT id, ts FROM t ORDER BY 1, s");
        assert!(
            out.contains("ORDER BY __repark_sort_sel_0, __repark_sort_key_0"),
            "{out}"
        );
    }

    #[test]
    fn projected_keys_leave_the_statement_alone() {
        untouched("SELECT ts FROM t ORDER BY ts");
        untouched("SELECT CAST(ts AS STRING) AS ts FROM t ORDER BY ts");
        untouched("SELECT a, b FROM t ORDER BY a, b DESC");
        untouched("SELECT t.ts FROM t ORDER BY ts");
    }

    #[test]
    fn stars_unions_and_exotic_items_bail() {
        untouched("SELECT * FROM t ORDER BY ts");
        untouched("SELECT a FROM t UNION ALL SELECT a FROM t ORDER BY a");
        untouched("SELECT count(*) FROM t GROUP BY s ORDER BY s");
        untouched("SELECT a FROM t ORDER BY a LIMIT 1 OFFSET 0");
    }

    #[test]
    fn fetch_and_offset_stay_on_the_outer_query() {
        let out = rewritten("SELECT a FROM t ORDER BY a + 1 OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY");
        assert!(out.contains("FETCH FIRST 1 ROWS ONLY"), "{out}");
    }

    #[test]
    fn alias_inside_a_key_expression_bails() {
        untouched("SELECT a AS x FROM t ORDER BY x + 1");
    }

    #[test]
    fn double_cast_names_the_full_inner_cast() {
        let out = rewritten("SELECT CAST(CAST(x AS INT) AS STRING) FROM t ORDER BY y");
        assert!(
            out.contains("AS \"CAST(CAST(x AS INT) AS STRING)\""),
            "{out}"
        );
    }

    #[test]
    fn compound_key_clashing_with_an_item_name_bails() {
        untouched("SELECT s FROM t ORDER BY st.s, ts");
        untouched("SELECT s FROM t ORDER BY st.s");
        untouched("SELECT a.s FROM t ORDER BY b.s");
    }

    #[test]
    fn compound_key_without_a_name_clash_stays_a_hidden_key() {
        let out = rewritten("SELECT id FROM t ORDER BY st.a");
        assert!(out.contains("st.a AS __repark_sort_key_0"), "{out}");
        assert!(out.contains("ORDER BY __repark_sort_key_0"), "{out}");
        untouched("SELECT s, st.s FROM t ORDER BY st.s");
    }

    #[test]
    fn sort_marker_collision_bails() {
        untouched("SELECT __repark_sort_sel_0 FROM t ORDER BY y");
    }
}
