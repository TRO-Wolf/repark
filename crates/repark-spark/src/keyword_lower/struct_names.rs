use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectNamePart, VisitMut,
    VisitorMut,
};

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
}
