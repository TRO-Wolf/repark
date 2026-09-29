use std::ops::ControlFlow;

use datafusion::common::{DFSchema, TableReference};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Ident, visit_expressions_mut};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use repark_common::names::NameRule;

use super::written_names::qualifier_matches;

#[must_use]
pub fn rebind_predicate_qualifiers(
    frame_schema: &DFSchema,
    displays: &[String],
    predicate: &str,
    rule: NameRule,
) -> String {
    let Ok(mut parsed) = Parser::new(&DatabricksDialect {})
        .try_with_sql(predicate)
        .and_then(|mut parser| parser.parse_expr())
    else {
        return predicate.to_string();
    };
    let mut rewritten = false;
    let _ = visit_expressions_mut(&mut parsed, |node| {
        if let SqlExpr::CompoundIdentifier(parts) = node
            && let Some((name, qualifier)) = parts.split_last()
            && let Some(respelled) = held_reference(frame_schema, displays, qualifier, name, rule)
        {
            *node = SqlExpr::CompoundIdentifier(
                respelled
                    .into_iter()
                    .map(|part| Ident::with_quote('`', part))
                    .collect(),
            );
            rewritten = true;
        }
        ControlFlow::<()>::Continue(())
    });
    if rewritten {
        parsed.to_string()
    } else {
        predicate.to_string()
    }
}

fn held_reference(
    frame_schema: &DFSchema,
    displays: &[String],
    qualifier: &[Ident],
    name: &Ident,
    rule: NameRule,
) -> Option<Vec<String>> {
    let want = match qualifier {
        [table] => TableReference::bare(table.value.as_str()),
        [schema, table] => TableReference::partial(schema.value.as_str(), table.value.as_str()),
        [catalog, schema, table] => TableReference::full(
            catalog.value.as_str(),
            schema.value.as_str(),
            table.value.as_str(),
        ),
        _ => return None,
    };
    let paired = frame_schema.fields().len() == displays.len();
    let mut found: Vec<(&TableReference, &str)> = Vec::new();
    for (index, (held, field)) in frame_schema.iter().enumerate() {
        let display = if paired {
            displays[index].as_str()
        } else {
            field.name().as_str()
        };
        if let Some(held) = held
            && qualifier_matches(&want, held, rule)
            && rule.matches(name.value.as_str(), display)
            && !found.contains(&(held, field.name().as_str()))
        {
            found.push((held, field.name().as_str()));
        }
    }
    let [(held, engine)] = found.as_slice() else {
        return None;
    };
    let spelled: Vec<String> = [held.catalog(), held.schema(), Some(held.table())]
        .into_iter()
        .flatten()
        .map(str::to_string)
        .collect();
    let mut respelled = spelled
        .get(spelled.len().checked_sub(qualifier.len())?..)?
        .to_vec();
    respelled.push((*engine).to_string());
    let read_as = qualifier.iter().chain(std::iter::once(name)).map(|part| {
        if part.quote_style.is_some() || matches!(rule, NameRule::Exact) {
            part.value.clone()
        } else {
            part.value.to_ascii_lowercase()
        }
    });
    if read_as.eq(respelled.iter().cloned()) {
        return None;
    }
    Some(respelled)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{DFSchema, TableReference};

    use super::rebind_predicate_qualifiers;
    use repark_common::names::NameRule::{Exact, IgnoreCase};

    fn held_schema(names: &[(&str, &str)]) -> DFSchema {
        let fields = names
            .iter()
            .map(|(qualifier, name)| {
                (
                    Some(TableReference::Bare {
                        table: (*qualifier).into(),
                    }),
                    Arc::new(Field::new(*name, DataType::Int64, true)),
                )
            })
            .collect::<Vec<_>>();
        DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
    }

    #[test]
    fn predicate_qualifiers_rebind_to_the_held_spelling_by_rule() {
        let frame = held_schema(&[("T", "id"), ("T", "Val")]);
        let names = ["id".to_string(), "Val".to_string()];
        assert_eq!(
            rebind_predicate_qualifiers(&frame, &names, "T.`id` > 1", IgnoreCase),
            "`T`.`id` > 1"
        );
        assert_eq!(
            rebind_predicate_qualifiers(&frame, &names, "t.val > 10 AND T.id < 5", IgnoreCase),
            "`T`.`Val` > 10 AND `T`.`id` < 5"
        );
        for untouched in ["`T`.`id` > 1", "x.id > 1", "id > 1", "T.nope > 1"] {
            assert_eq!(
                rebind_predicate_qualifiers(&frame, &names, untouched, IgnoreCase),
                untouched
            );
        }
        for exact_untouched in ["t.id > 1", "T.id > 1", "T.ID > 1"] {
            assert_eq!(
                rebind_predicate_qualifiers(&frame, &names, exact_untouched, Exact),
                exact_untouched
            );
        }
        let lower = held_schema(&[("t", "id")]);
        assert_eq!(
            rebind_predicate_qualifiers(&lower, &["id".to_string()], "t.id  >  1", IgnoreCase),
            "t.id  >  1"
        );
        let twins = held_schema(&[("l", "id"), ("L", "id")]);
        let twin_names = ["id".to_string(), "id".to_string()];
        assert_eq!(
            rebind_predicate_qualifiers(&twins, &twin_names, "L.id > 1", IgnoreCase),
            "L.id > 1"
        );
        let joined = held_schema(&[("L", "e0"), ("L", "e1"), ("R", "e2"), ("R", "e3")]);
        let displays = ["id", "Data", "ID", "Data"].map(str::to_string);
        assert_eq!(
            rebind_predicate_qualifiers(&joined, &displays, "r.data = 'x'", IgnoreCase),
            "`R`.`e3` = 'x'"
        );
        assert_eq!(
            rebind_predicate_qualifiers(&joined, &displays, "R.Data = 'x'", Exact),
            "`R`.`e3` = 'x'"
        );
        assert_eq!(
            rebind_predicate_qualifiers(&joined, &displays, "r.Data = 'x'", Exact),
            "r.Data = 'x'"
        );
    }
}
