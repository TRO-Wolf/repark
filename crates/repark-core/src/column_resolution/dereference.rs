use std::ops::ControlFlow;

use datafusion::sql::parser::{CopyToSource, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr as SqlExpr, Value, VisitMut, visit_expressions_mut,
};

pub fn spell_dereferences_as_field_names(statement: &mut DfStatement) {
    match statement {
        DfStatement::Statement(inner) => spell_in(inner.as_mut()),
        DfStatement::Explain(explain) => {
            spell_dereferences_as_field_names(explain.statement.as_mut());
        }
        DfStatement::CopyTo(copy) => {
            if let CopyToSource::Query(query) = &mut copy.source {
                spell_in(query.as_mut());
            }
        }
        DfStatement::CreateExternalTable(_) | DfStatement::Reset(_) => {}
    }
}

fn spell_in<V: VisitMut>(node: &mut V) {
    let _ = visit_expressions_mut(node, |expr| {
        spell_chain(expr);
        ControlFlow::<()>::Continue(())
    });
}

fn spell_chain(expr: &mut SqlExpr) {
    let SqlExpr::CompoundFieldAccess { root, access_chain } = expr else {
        return;
    };
    let absorbed = if matches!(root.as_ref(), SqlExpr::Identifier(_)) {
        access_chain
            .iter()
            .position(|access| !matches!(access, AccessExpr::Dot(SqlExpr::Identifier(_))))
            .unwrap_or(access_chain.len())
    } else {
        0
    };
    for access in access_chain.iter_mut().skip(absorbed) {
        if let AccessExpr::Dot(field) = access
            && let SqlExpr::Identifier(ident) = field
        {
            let name = std::mem::take(&mut ident.value);
            *field = SqlExpr::Value(Value::SingleQuotedString(name).with_empty_span());
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::DFParser;

    use super::*;

    fn respelled(sql: &str) -> String {
        let mut statement = DFParser::parse_sql(sql).unwrap().pop_front().unwrap();
        spell_dereferences_as_field_names(&mut statement);
        statement.to_string()
    }

    #[test]
    fn a_field_after_a_subscript_or_a_call_becomes_a_field_name() {
        assert_eq!(
            respelled("SELECT m['k'].x.y, f(a).b, (c).d, e[0].`w n` FROM t"),
            "SELECT m['k'].'x'.'y', f(a).'b', (c).'d', e[0].'w n' FROM t"
        );
    }

    #[test]
    fn a_leading_column_path_stays_an_identifier_chain() {
        assert_eq!(
            respelled("SELECT t.s.a, s.a.b, s.a['k'].c FROM t"),
            "SELECT t.s.a, s.a.b, s.a['k'].'c' FROM t"
        );
    }

    #[test]
    fn explain_and_nested_queries_are_respelled() {
        assert_eq!(
            respelled("EXPLAIN SELECT (SELECT m['k'].x FROM t) WHERE g(1).y = 1"),
            "EXPLAIN SELECT (SELECT m['k'].'x' FROM t) WHERE g(1).'y' = 1"
        );
    }
}
