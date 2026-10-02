use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{Cte, ObjectName, ObjectNamePart, Select, Visit, Visitor};

use super::select_values_arms::ArmMap;

pub(crate) fn unmapped_arm<'arm>(
    select: &Select,
    scope: &[&Cte],
    case_insensitive: bool,
) -> ArmMap<'arm> {
    ArmMap {
        positions: Vec::new(),
        provenance: Vec::new(),
        arm_sql: scoped_arm_sql(select, scope, case_insensitive),
    }
}

pub(crate) fn scoped_arm_sql(
    select: &Select,
    scope: &[&Cte],
    case_insensitive: bool,
) -> Option<String> {
    if scope.is_empty() {
        return Some(select.to_string());
    }
    let mut kept: Vec<&Cte> = Vec::with_capacity(scope.len());
    for cte in scope {
        let name = cte.alias.name.value.as_str();
        if let Some(position) = kept
            .iter()
            .position(|kept| names_equal(&kept.alias.name.value, name, case_insensitive))
        {
            kept.remove(position);
        }
        kept.push(*cte);
    }
    if has_case_twins(&kept) {
        return None;
    }
    let mut sql = String::from(if scope_needs_recursive(&kept, case_insensitive) {
        "WITH RECURSIVE "
    } else {
        "WITH "
    });
    for (position, cte) in kept.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&cte.to_string());
    }
    sql.push(' ');
    sql.push_str(&select.to_string());
    Some(sql)
}

fn has_case_twins(kept: &[&Cte]) -> bool {
    kept.iter().enumerate().any(|(index, first)| {
        kept.iter().skip(index + 1).any(|second| {
            first.alias.name.value != second.alias.name.value
                && first
                    .alias
                    .name
                    .value
                    .eq_ignore_ascii_case(&second.alias.name.value)
        })
    })
}

fn scope_needs_recursive(kept: &[&Cte], case_insensitive: bool) -> bool {
    kept.iter().enumerate().any(|(index, cte)| {
        let mut seen = RelationNames { names: Vec::new() };
        let _ = cte.query.visit(&mut seen);
        seen.names.iter().any(|name| {
            kept.iter()
                .skip(index)
                .any(|kept| names_equal(&kept.alias.name.value, name, case_insensitive))
        })
    })
}

fn names_equal(first: &str, second: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        first.eq_ignore_ascii_case(second)
    } else {
        first == second
    }
}

struct RelationNames {
    names: Vec<String>,
}

impl Visitor for RelationNames {
    type Break = std::convert::Infallible;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if let [ObjectNamePart::Identifier(ident)] = relation.0.as_slice() {
            self.names.push(ident.value.clone());
        }
        ControlFlow::Continue(())
    }
}
