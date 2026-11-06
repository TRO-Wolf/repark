use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectNamePart, Subscript, Value, VisitMut, VisitorMut,
};

use crate::spark_typed::SPARK_AS_NAME;

pub(crate) fn name_struct_fields<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut StructNames);
}

struct StructNames;

impl VisitorMut for StructNames {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Some(values) = struct_call_values(expr) {
            *expr = Expr::Struct {
                values: values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| named(value, index))
                    .collect(),
                fields: Vec::new(),
            };
        }
        ControlFlow::Continue(())
    }
}

fn struct_call_values(expr: &mut Expr) -> Option<Vec<Expr>> {
    match expr {
        Expr::Struct { values, fields } if fields.is_empty() && !values.is_empty() => {
            Some(std::mem::take(values))
        }
        Expr::Function(function)
            if function.over.is_none()
                && function.filter.is_none()
                && function.within_group.is_empty()
                && matches!(
                    function.name.0.as_slice(),
                    [ObjectNamePart::Identifier(name)]
                        if name.quote_style.is_none() && name.value.eq_ignore_ascii_case("struct")
                ) =>
        {
            let FunctionArguments::List(list) = &function.args else {
                return None;
            };
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return None;
            }
            list.args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .filter(|values| !values.is_empty())
        }
        _ => None,
    }
}

fn named(value: Expr, index: usize) -> Expr {
    if matches!(value, Expr::Named { .. }) {
        return value;
    }
    let name = spark_field_name(&value).unwrap_or_else(|| format!("col{}", index + 1));
    Expr::Named {
        expr: Box::new(value),
        name: Ident::new(name),
    }
}

fn spark_field_name(value: &Expr) -> Option<String> {
    match value {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
        Expr::Nested(inner) => spark_field_name(inner),
        Expr::CompoundFieldAccess { access_chain, .. } => match access_chain.last()? {
            AccessExpr::Dot(Expr::Identifier(ident)) => Some(ident.value.clone()),
            AccessExpr::Subscript(Subscript::Index { index }) => string_key(index),
            _ => None,
        },
        Expr::Function(function) => extracted_field(function),
        _ => None,
    }
}

fn extracted_field(function: &Function) -> Option<String> {
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(display)))) = list.args.first()
    else {
        return None;
    };
    let Value::SingleQuotedString(display) = &display.value else {
        return None;
    };
    let (base, path) = display.rsplit_once(").")?;
    let field = path.rsplit('.').next()?.trim_matches('`');
    (name.value == SPARK_AS_NAME
        && base.to_ascii_lowercase().starts_with("named_struct(")
        && !field.is_empty())
    .then(|| field.to_string())
}

fn string_key(index: &Expr) -> Option<String> {
    match index {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.clone()),
            _ => None,
        },
        Expr::Identifier(ident) if ident.quote_style == Some('"') => Some(ident.value.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::{DFParser, Statement as DfStatement};

    use super::*;

    fn named_sql(sql: &str) -> String {
        let DfStatement::Statement(mut statement) =
            DFParser::parse_sql(sql).unwrap().pop_front().unwrap()
        else {
            unreachable!("a SELECT parses to a SQL statement")
        };
        name_struct_fields(statement.as_mut());
        statement.to_string()
    }

    #[test]
    fn unnamed_fields_take_sparks_names() {
        assert_eq!(
            named_sql(
                "SELECT struct(1, 'x'), struct(id, t.v, (w), id + 1), struct(1 AS col2, 2) FROM t"
            ),
            "SELECT STRUCT(1 AS col1, 'x' AS col2), STRUCT(id AS id, t.v AS v, (w) AS w, id + 1 AS col4), \
             STRUCT(1 AS col2, 2 AS col2) FROM t"
        );
    }

    #[test]
    fn a_subscript_or_field_access_takes_its_string_key_or_field_name() {
        let rows = [
            ("m['a']", "a"),
            ("m[\"a\"]", "a"),
            ("s['x']", "x"),
            ("m['B']", "B"),
            ("(m['a'])", "a"),
            ("map('a', 1)['a']", "a"),
            ("named_struct('x', 1).x", "x"),
            ("n.a['x']", "x"),
            ("ls[0].a", "a"),
            ("n.b.x", "x"),
            ("m.a", "a"),
            ("l[0]", "col1"),
            ("l[1 - 1]", "col1"),
            ("m[NULL]", "col1"),
            ("m[k]", "col1"),
            ("m['a'] + 1", "col1"),
            ("CAST(id AS BIGINT)", "col1"),
            ("__repark_spark_as__('named_struct(x, 1).x', (1))", "x"),
            (
                "__repark_spark_as__('named_struct(a, named_struct(b, 1)).a.b', (1))",
                "b",
            ),
            ("__repark_spark_as__('1L', (1))", "col1"),
        ];
        for (value, name) in rows {
            let sql = named_sql(&format!("SELECT struct({value}) FROM t"));
            assert!(
                sql.ends_with(&format!(" AS {name}) FROM t")),
                "struct({value}) should name its field {name}: {sql}"
            );
        }
        assert_eq!(
            named_sql("SELECT struct(m['a'], s['x'], l[0]) FROM t"),
            "SELECT STRUCT(m['a'] AS a, s['x'] AS x, l[0] AS col3) FROM t"
        );
    }
}
