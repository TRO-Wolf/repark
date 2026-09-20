use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, NestedFieldRef, PrimitiveType, Schema, Type};
use iceberg::table::Table;

const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, Copy)]
enum ColumnScope<'a> {
    TargetOrBare(&'a str),
    QualifiedTarget(&'a str),
}

impl<'a> ColumnScope<'a> {
    fn alias(self) -> &'a str {
        match self {
            Self::TargetOrBare(alias) | Self::QualifiedTarget(alias) => alias,
        }
    }

    fn accepts_bare(self) -> bool {
        matches!(self, Self::TargetOrBare(_))
    }
}

#[must_use]
pub(crate) fn for_identity_dml(
    table: &Table,
    selection_sql: &str,
    target_alias: &str,
) -> Predicate {
    from_selection(
        selection_sql,
        target_alias,
        table.metadata().current_schema(),
    )
}

#[must_use]
pub(crate) fn from_selection(
    selection_sql: &str,
    target_alias: &str,
    schema: &Schema,
) -> Predicate {
    scoped(
        selection_sql,
        ColumnScope::TargetOrBare(target_alias),
        schema,
    )
}

#[must_use]
pub(crate) fn from_merge_on(
    on_sql: &str,
    target_alias: &str,
    source_alias: &str,
    schema: &Schema,
) -> Predicate {
    if target_alias.eq_ignore_ascii_case(source_alias) {
        return Predicate::AlwaysTrue;
    }
    scoped(on_sql, ColumnScope::QualifiedTarget(target_alias), schema)
}

fn scoped(sql: &str, scope: ColumnScope<'_>, schema: &Schema) -> Predicate {
    let Some(expr) = parse_expr(sql) else {
        return Predicate::AlwaysTrue;
    };
    widest(&expr, schema, scope, 0).unwrap_or(Predicate::AlwaysTrue)
}

fn parse_expr(sql: &str) -> Option<Expr> {
    Parser::new(&GenericDialect {})
        .try_with_sql(sql)
        .ok()?
        .parse_expr()
        .ok()
}

fn widest(expr: &Expr, schema: &Schema, scope: ColumnScope<'_>, depth: usize) -> Option<Predicate> {
    if depth >= MAX_DEPTH {
        return None;
    }
    match expr {
        Expr::Nested(inner) => widest(inner, schema, scope, depth + 1),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let kept = widest(left, schema, scope, depth + 1);
            let other = widest(right, schema, scope, depth + 1);
            match (kept, other) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (left, right) => left.or(right),
            }
        }
        _ => exact(expr, schema, scope, depth),
    }
}

fn exact(expr: &Expr, schema: &Schema, scope: ColumnScope<'_>, depth: usize) -> Option<Predicate> {
    if depth >= MAX_DEPTH {
        return None;
    }
    match expr {
        Expr::Nested(inner) => exact(inner, schema, scope, depth + 1),
        Expr::IsNull(inner) => Some(Reference::new(column(inner, schema, scope)?).is_null()),
        Expr::IsNotNull(inner) => Some(Reference::new(column(inner, schema, scope)?).is_not_null()),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Some(!exact(inner, schema, scope, depth + 1)?),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => in_list(inner, list, *negated, schema, scope),
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => between(inner, low, high, *negated, schema, scope),
        Expr::BinaryOp { left, op, right } => binary(left, op, right, schema, scope, depth),
        _ => None,
    }
}

fn binary(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &Schema,
    scope: ColumnScope<'_>,
    depth: usize,
) -> Option<Predicate> {
    match op {
        BinaryOperator::And => Some(exact(left, schema, scope, depth + 1)?.and(exact(
            right,
            schema,
            scope,
            depth + 1,
        )?)),
        BinaryOperator::Or => {
            Some(exact(left, schema, scope, depth + 1)?.or(exact(right, schema, scope, depth + 1)?))
        }
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => comparison(left, op, right, schema, scope),
        _ => None,
    }
}

fn comparison(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &Schema,
    scope: ColumnScope<'_>,
) -> Option<Predicate> {
    if let Some(name) = column(left, schema, scope) {
        let field_type = primitive_type_of(schema, &name)?;
        let datum = ordered_literal(op, right, &field_type)?;
        return apply(name, op, datum, false);
    }
    let name = column(right, schema, scope)?;
    let field_type = primitive_type_of(schema, &name)?;
    let datum = ordered_literal(op, left, &field_type)?;
    apply(name, op, datum, true)
}

fn ordered_literal(op: &BinaryOperator, expr: &Expr, field_type: &PrimitiveType) -> Option<Datum> {
    if is_floating(field_type) && !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return None;
    }
    literal_datum(expr, field_type)
}

fn is_floating(field_type: &PrimitiveType) -> bool {
    matches!(field_type, PrimitiveType::Float | PrimitiveType::Double)
}

fn apply(name: String, op: &BinaryOperator, datum: Datum, swapped: bool) -> Option<Predicate> {
    let reference = Reference::new(name);
    match (op, swapped) {
        (BinaryOperator::Eq, _) => Some(reference.equal_to(datum)),
        (BinaryOperator::NotEq, _) => Some(reference.not_equal_to(datum)),
        (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => {
            Some(reference.less_than(datum))
        }
        (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => {
            Some(reference.less_than_or_equal_to(datum))
        }
        (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => {
            Some(reference.greater_than(datum))
        }
        (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => {
            Some(reference.greater_than_or_equal_to(datum))
        }
        _ => None,
    }
}

fn in_list(
    inner: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &Schema,
    scope: ColumnScope<'_>,
) -> Option<Predicate> {
    if list.is_empty() {
        return None;
    }
    let name = column(inner, schema, scope)?;
    let field_type = primitive_type_of(schema, &name)?;
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        values.push(literal_datum(item, &field_type)?);
    }
    let predicate = Reference::new(name).is_in(values);
    Some(if negated { !predicate } else { predicate })
}

fn between(
    inner: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    schema: &Schema,
    scope: ColumnScope<'_>,
) -> Option<Predicate> {
    let name = column(inner, schema, scope)?;
    let field_type = primitive_type_of(schema, &name)?;
    if is_floating(&field_type) {
        return None;
    }
    let lower =
        Reference::new(name.clone()).greater_than_or_equal_to(literal_datum(low, &field_type)?);
    let upper = Reference::new(name).less_than_or_equal_to(literal_datum(high, &field_type)?);
    let predicate = lower.and(upper);
    Some(if negated { !predicate } else { predicate })
}

fn column(expr: &Expr, schema: &Schema, scope: ColumnScope<'_>) -> Option<String> {
    let raw = match expr {
        Expr::Identifier(ident) if scope.accepts_bare() => ident.value.as_str(),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].value.eq_ignore_ascii_case(scope.alias()) {
                return None;
            }
            parts[1].value.as_str()
        }
        _ => return None,
    };
    top_level_field(schema, raw).map(|field| field.name.clone())
}

pub(crate) fn top_level_field<'a>(schema: &'a Schema, raw: &str) -> Option<&'a NestedFieldRef> {
    if let Some(field) = schema.as_struct().field_by_name(raw) {
        return Some(field);
    }
    let mut folded = schema
        .as_struct()
        .fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(raw));
    let only = folded.next()?;
    folded.next().is_none().then_some(only)
}

fn primitive_type_of(schema: &Schema, name: &str) -> Option<PrimitiveType> {
    match top_level_field(schema, name)?.field_type.as_ref() {
        Type::Primitive(primitive) => Some(primitive.clone()),
        _ => None,
    }
}

pub(crate) fn literal_datum(expr: &Expr, field_type: &PrimitiveType) -> Option<Datum> {
    match expr {
        Expr::Nested(inner) => literal_datum(inner, field_type),
        Expr::Value(ValueWithSpan { value, .. }) => value_datum(value, field_type),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            let Expr::Value(ValueWithSpan {
                value: Value::Number(raw, _),
                ..
            }) = inner.as_ref()
            else {
                return None;
            };
            number_datum(&format!("-{raw}"), field_type)
        }
        _ => None,
    }
}

fn value_datum(value: &Value, field_type: &PrimitiveType) -> Option<Datum> {
    match (value, field_type) {
        (Value::Number(raw, _), _) => number_datum(raw, field_type),
        (
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            PrimitiveType::String,
        ) => Some(Datum::string(text)),
        (Value::Boolean(flag), PrimitiveType::Boolean) => Some(Datum::bool(*flag)),
        _ => None,
    }
}

fn number_datum(raw: &str, field_type: &PrimitiveType) -> Option<Datum> {
    match field_type {
        PrimitiveType::Int => raw.parse::<i32>().ok().map(Datum::int),
        PrimitiveType::Long => raw.parse::<i64>().ok().map(Datum::long),
        PrimitiveType::Float => {
            let value = raw.parse::<f32>().ok()?;
            is_exactly(raw, f64::from(value)).then(|| Datum::float(value))
        }
        PrimitiveType::Double => {
            let value = raw.parse::<f64>().ok()?;
            is_exactly(raw, value).then(|| Datum::double(value))
        }
        _ => None,
    }
}

fn is_exactly(raw: &str, value: f64) -> bool {
    let Some(literal) = Decimal::parse(raw) else {
        return false;
    };
    value.is_finite() && Decimal::parse(&format!("{value:.800e}")) == Some(literal)
}

#[derive(Debug, PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: String,
    exponent: i64,
}

impl Decimal {
    fn parse(raw: &str) -> Option<Self> {
        let (negative, unsigned) = match raw.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, raw.strip_prefix('+').unwrap_or(raw)),
        };
        let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
            None => (unsigned, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let all_digits = whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit());
        if !all_digits || (whole.is_empty() && fraction.is_empty()) {
            return None;
        }
        let joined = format!("{whole}{fraction}");
        let significant = joined.trim_start_matches('0');
        let digits = significant.trim_end_matches('0');
        if digits.is_empty() {
            return None;
        }
        let scale = i64::try_from(fraction.len()).ok()?;
        let trailing = i64::try_from(significant.len() - digits.len()).ok()?;
        Some(Self {
            negative,
            digits: digits.to_owned(),
            exponent: exponent.checked_sub(scale)?.checked_add(trailing)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::NestedField;

    use super::*;

    fn target_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "k", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "v", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("target schema")
    }

    fn on(sql: &str) -> String {
        format!("{}", from_merge_on(sql, "t", "s", &target_schema()))
    }

    fn where_clause(sql: &str) -> String {
        format!("{}", from_selection(sql, "d", &target_schema()))
    }

    #[test]
    fn a_target_only_partition_conjunct_scopes_a_merge_filter() {
        assert_eq!(on("t.k = 'a' AND t.k = s.k AND t.id = s.id"), "k = \"a\"");
    }

    #[test]
    fn a_target_only_range_conjunct_scopes_a_merge_filter() {
        assert_eq!(on("t.id < 50 AND t.id = s.id"), "id < 50");
        assert_eq!(on("t.id >= 50 AND t.id = s.id"), "id >= 50");
    }

    #[test]
    fn a_join_equality_alone_leaves_the_filter_unscoped() {
        assert_eq!(on("t.id = s.id"), "TRUE");
    }

    #[test]
    fn a_source_only_conjunct_never_reaches_the_filter() {
        assert_eq!(on("s.k = 'a' AND t.id = s.id"), "TRUE");
    }

    #[test]
    fn a_bare_identifier_in_an_on_condition_is_not_target_qualified() {
        assert_eq!(on("k = 'a' AND t.id = s.id"), "TRUE");
    }

    #[test]
    fn an_alias_the_source_shadows_refuses_to_scope() {
        assert_eq!(
            format!("{}", from_merge_on("t.k = 'a'", "t", "T", &target_schema())),
            "TRUE"
        );
    }

    #[test]
    fn an_unconvertible_conjunct_widens_instead_of_narrowing() {
        assert_eq!(on("t.k = 'a' AND lower(t.v) = 'x'"), "k = \"a\"");
        assert_eq!(where_clause("k = 'a' AND lower(v) = 'x'"), "k = \"a\"");
    }

    #[test]
    fn an_unconvertible_disjunct_drops_the_whole_disjunction() {
        assert_eq!(where_clause("k = 'a' OR lower(v) = 'x'"), "TRUE");
        assert_eq!(
            where_clause("id > 90 AND (k = 'a' OR lower(v) = 'x')"),
            "id > 90"
        );
    }

    #[test]
    fn a_disjunction_of_convertible_sides_survives() {
        assert_eq!(
            where_clause("k = 'a' OR k = 'b'"),
            "(k = \"a\") OR (k = \"b\")"
        );
    }

    #[test]
    fn a_selection_takes_bare_and_target_qualified_columns() {
        assert_eq!(
            where_clause("k = 'a' AND id > 90"),
            "(k = \"a\") AND (id > 90)"
        );
        assert_eq!(where_clause("d.k = 'a'"), "k = \"a\"");
    }

    #[test]
    fn a_selection_refuses_another_relations_qualifier() {
        assert_eq!(where_clause("other.k = 'a'"), "TRUE");
    }

    #[test]
    fn a_column_the_schema_does_not_carry_leaves_the_filter_unscoped() {
        assert_eq!(where_clause("missing = 'a'"), "TRUE");
    }

    fn nested_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "outer",
                    Type::Struct(iceberg::spec::StructType::new(vec![
                        NestedField::optional(2, "leaf", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .expect("nested schema")
    }

    #[test]
    fn a_nested_leaf_never_resolves_as_a_top_level_column() {
        let schema = nested_schema();
        assert_eq!(
            format!("{}", from_selection("leaf = 1", "t", &schema)),
            "TRUE"
        );
        assert_eq!(
            format!("{}", from_selection("\"outer.leaf\" = 1", "t", &schema)),
            "TRUE"
        );
    }

    #[test]
    fn a_column_spelled_in_another_case_resolves_to_the_schema_spelling() {
        assert_eq!(where_clause("K = 'a'"), "k = \"a\"");
    }

    #[test]
    fn a_literal_that_does_not_fit_the_column_type_leaves_the_filter_unscoped() {
        assert_eq!(where_clause("id = 'abc'"), "TRUE");
        assert_eq!(where_clause("k = 7"), "TRUE");
    }

    fn floating_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "f", Type::Primitive(PrimitiveType::Float)).into(),
                NestedField::optional(2, "d", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("floating schema")
    }

    fn floating(sql: &str) -> String {
        format!("{}", from_selection(sql, "t", &floating_schema()))
    }

    #[test]
    fn an_underflowing_float_literal_leaves_the_filter_unscoped() {
        assert_eq!(floating("f < 1e-50"), "TRUE");
        assert_eq!(floating("d < 1e-400"), "TRUE");
        assert_eq!(floating("f = 1e-50"), "TRUE");
        assert_eq!(floating("d = 1e-400"), "TRUE");
        assert_eq!(floating("f = 1e-50 AND d = 0.5"), "d = 0.5");
    }

    #[test]
    fn an_inexact_float_literal_leaves_the_filter_unscoped() {
        assert_eq!(floating("f = 0.1"), "TRUE");
        assert_eq!(floating("d = 0.1"), "TRUE");
        assert_eq!(floating("f = 1e39"), "TRUE");
        assert_eq!(floating("d = 1e309"), "TRUE");
        assert_eq!(floating("f = 16777217"), "TRUE");
        assert_eq!(floating("f IN (0.5, 0.1)"), "TRUE");
    }

    #[test]
    fn a_zero_float_literal_leaves_the_filter_unscoped() {
        assert_eq!(floating("f = 0.0"), "TRUE");
        assert_eq!(floating("d = -0"), "TRUE");
    }

    #[test]
    fn an_exact_float_literal_still_scopes_an_equality() {
        assert_eq!(floating("f = 0.5"), "f = 0.5");
        assert_eq!(floating("d = -1.25e2"), "d = -125");
        assert_eq!(floating("f = 16777216"), "f = 16777216");
        assert_eq!(floating("d <> 0.25"), "d != 0.25");
        assert!(floating("f IN (0.5, 2)").starts_with("f IN"));
    }

    #[test]
    fn a_float_range_leaves_the_filter_unscoped() {
        assert_eq!(floating("f < 0.5"), "TRUE");
        assert_eq!(floating("d >= 2"), "TRUE");
        assert_eq!(floating("2 < d"), "TRUE");
        assert_eq!(floating("f BETWEEN 0.5 AND 2"), "TRUE");
    }

    #[test]
    fn a_reversed_comparison_keeps_its_direction() {
        assert_eq!(where_clause("90 < id"), "id > 90");
        assert_eq!(where_clause("50 >= id"), "id <= 50");
    }

    #[test]
    fn negative_literals_convert() {
        assert_eq!(where_clause("id > -5"), "id > -5");
    }

    #[test]
    fn null_tests_and_lists_and_ranges_convert() {
        assert_eq!(where_clause("k IS NULL"), "k IS NULL");
        assert_eq!(where_clause("k IS NOT NULL"), "k IS NOT NULL");
        assert_eq!(
            where_clause("id BETWEEN 1 AND 5"),
            "(id >= 1) AND (id <= 5)"
        );
        assert!(where_clause("id IN (1, 2)").starts_with("id IN"));
        let negated = where_clause("id NOT IN (1, 2)");
        assert!(negated.starts_with("NOT (id IN"), "{negated}");
    }

    #[test]
    fn a_subquery_selection_leaves_the_filter_unscoped() {
        assert_eq!(where_clause("id IN (SELECT id FROM other)"), "TRUE");
    }

    #[test]
    fn a_predicate_that_will_not_parse_leaves_the_filter_unscoped() {
        assert_eq!(where_clause("k = "), "TRUE");
        assert_eq!(on(""), "TRUE");
    }
}
