use std::ops::ControlFlow;

use datafusion::common::{Column, DFSchema, DataFusionError, Result, TableReference};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr as SqlExpr, Ident, LambdaFunctionParameter, OneOrManyWithParens, Query,
    Subscript, Value, VisitMut, VisitorMut,
};
use repark_common::names::NameRule;

use super::case_bind::{Hit, ambiguous_reference};
use super::written_names::qualifier_matches;

#[allow(clippy::missing_errors_doc)]
pub fn bind_predicate_qualifiers(
    predicate: &mut SqlExpr,
    frame_schema: &DFSchema,
    displays: Option<&[String]>,
    attributes: &[String],
    rule: NameRule,
) -> Result<()> {
    let mut binder = QualifierBinder {
        frame_schema,
        displays,
        attributes,
        rule,
        scopes: Vec::new(),
        query_depth: 0,
    };
    match predicate.visit(&mut binder) {
        ControlFlow::Break(error) => Err(error),
        ControlFlow::Continue(()) => Ok(()),
    }
}

struct QualifierBinder<'a> {
    frame_schema: &'a DFSchema,
    displays: Option<&'a [String]>,
    attributes: &'a [String],
    rule: NameRule,
    scopes: Vec<Vec<String>>,
    query_depth: usize,
}

impl VisitorMut for QualifierBinder<'_> {
    type Break = DataFusionError;

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if self.query_depth > 0 {
            return ControlFlow::Continue(());
        }
        let mut parts: Vec<&mut Ident> = match expr {
            SqlExpr::Lambda(lambda) => {
                let params: Vec<&mut LambdaFunctionParameter> = match &mut lambda.params {
                    OneOrManyWithParens::One(param) => vec![param],
                    OneOrManyWithParens::Many(params) => params.iter_mut().collect(),
                };
                let mut names = Vec::with_capacity(params.len());
                for param in params {
                    if matches!(self.rule, NameRule::Exact) {
                        param.name.quote_style = Some('`');
                    }
                    names.push(param.name.value.clone());
                }
                self.scopes.push(names);
                return ControlFlow::Continue(());
            }
            SqlExpr::Identifier(ident) => {
                return match self.bound_identifier(ident) {
                    Ok(()) => ControlFlow::Continue(()),
                    Err(error) => ControlFlow::Break(error),
                };
            }
            SqlExpr::CompoundIdentifier(parts) => {
                if let Some((first, rest)) = parts.split_first()
                    && self.lambda_parameter(&first.value).is_some()
                {
                    *expr = SqlExpr::CompoundFieldAccess {
                        root: Box::new(SqlExpr::Identifier(first.clone())),
                        access_chain: rest
                            .iter()
                            .map(|part| {
                                AccessExpr::Subscript(Subscript::Index {
                                    index: SqlExpr::Value(
                                        Value::SingleQuotedString(part.value.clone())
                                            .with_empty_span(),
                                    ),
                                })
                            })
                            .collect(),
                    };
                    return ControlFlow::Continue(());
                }
                parts.iter_mut().collect()
            }
            SqlExpr::CompoundFieldAccess { root, access_chain } => {
                let SqlExpr::Identifier(root) = root.as_mut() else {
                    return ControlFlow::Continue(());
                };
                std::iter::once(root)
                    .chain(access_chain.iter_mut().map_while(|access| match access {
                        AccessExpr::Dot(SqlExpr::Identifier(part)) => Some(part),
                        _ => None,
                    }))
                    .collect()
            }
            _ => return ControlFlow::Continue(()),
        };
        if self.displays.is_none() {
            return ControlFlow::Continue(());
        }
        match self.bound_parts(&parts) {
            Ok(Some(respelled)) => {
                for (part, held) in parts.iter_mut().zip(respelled) {
                    **part = Ident {
                        value: held,
                        quote_style: Some('`'),
                        span: part.span,
                    };
                }
                ControlFlow::Continue(())
            }
            Ok(None) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if self.query_depth == 0 && matches!(expr, SqlExpr::Lambda(_)) {
            self.scopes.pop();
        }
        ControlFlow::Continue(())
    }
}

impl QualifierBinder<'_> {
    fn lambda_parameter(&self, written: &str) -> Option<&String> {
        self.scopes
            .iter()
            .rev()
            .flatten()
            .find(|name| self.rule.matches(written, name))
    }

    fn bound_identifier(&self, ident: &mut Ident) -> Result<()> {
        if let Some(param) = self.lambda_parameter(&ident.value) {
            if matches!(self.rule, NameRule::Exact) {
                *ident = Ident {
                    value: param.clone(),
                    quote_style: Some('`'),
                    span: ident.span,
                };
            }
            return Ok(());
        }
        let Some(displays) = self.displays else {
            return Ok(());
        };
        if self.attributes.len() != displays.len()
            || self.frame_schema.fields().len() != displays.len()
        {
            return Ok(());
        }
        let hits: Vec<usize> = displays
            .iter()
            .enumerate()
            .filter(|(_, display)| self.rule.matches(&ident.value, display))
            .map(|(index, _)| index)
            .collect();
        let [first, _, ..] = hits.as_slice() else {
            return Ok(());
        };
        let mut identities = hits
            .iter()
            .map(|index| self.attributes[*index].as_str())
            .filter(|identity| !identity.is_empty());
        let one = identities
            .next()
            .is_none_or(|seen| identities.all(|identity| identity == seen));
        if one {
            *ident = Ident {
                value: self.frame_schema.field(*first).name().clone(),
                quote_style: Some('`'),
                span: ident.span,
            };
            return Ok(());
        }
        let found: Vec<Hit<'_>> = hits
            .iter()
            .map(|index| {
                let (qualifier, field) = self.frame_schema.qualified_field(*index);
                (qualifier, field.as_ref())
            })
            .collect();
        Err(ambiguous_reference(
            &Column::new_unqualified(ident.value.as_str()),
            &found,
        ))
    }

    fn bound_parts(&self, parts: &[&mut Ident]) -> Result<Option<Vec<String>>> {
        let Some(first) = parts.first() else {
            return Ok(None);
        };
        if self.lambda_parameter(&first.value).is_some() {
            return Ok(None);
        }
        for width in (1..parts.len().min(4)).rev() {
            let written: Vec<&str> = parts[..width]
                .iter()
                .map(|part| part.value.as_str())
                .collect();
            let want = match written.as_slice() {
                [table] => TableReference::bare(*table),
                [schema, table] => TableReference::partial(*schema, *table),
                [catalog, schema, table] => TableReference::full(*catalog, *schema, *table),
                _ => return Ok(None),
            };
            let name = parts[width].value.as_str();
            let hits = self.hits(&want, name);
            match hits.as_slice() {
                [] => {}
                [(held, engine)] => return Ok(respelled(*held, engine.name(), width)),
                _ => {
                    let column = Column::new(Some(want), name);
                    return Err(ambiguous_reference(&column, &hits));
                }
            }
        }
        Ok(None)
    }

    fn hits(&self, want: &TableReference, name: &str) -> Vec<Hit<'_>> {
        let displays = self.displays.unwrap_or_default();
        let paired = self.frame_schema.fields().len() == displays.len();
        let mut found: Vec<Hit<'_>> = Vec::new();
        for (index, (held, field)) in self.frame_schema.iter().enumerate() {
            let display = if paired {
                displays[index].as_str()
            } else {
                field.name().as_str()
            };
            if let Some(held) = held
                && qualifier_matches(want, held, self.rule)
                && self.rule.matches(name, display)
                && !found
                    .iter()
                    .any(|(seen, engine)| *seen == Some(held) && engine.name() == field.name())
            {
                found.push((Some(held), field.as_ref()));
            }
        }
        found
    }
}

fn respelled(held: Option<&TableReference>, engine: &str, width: usize) -> Option<Vec<String>> {
    let held = held?;
    let spelled: Vec<String> = [held.catalog(), held.schema(), Some(held.table())]
        .into_iter()
        .flatten()
        .map(str::to_string)
        .collect();
    let mut respelled = spelled.get(spelled.len().checked_sub(width)?..)?.to_vec();
    respelled.push(engine.to_string());
    Some(respelled)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{DFSchema, TableReference};
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::sqlparser::tokenizer::Token;

    use super::bind_predicate_qualifiers;
    use repark_common::names::NameRule::{self, Exact, IgnoreCase};

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

    fn bound(schema: &DFSchema, displays: &[&str], sql: &str, rule: NameRule) -> String {
        let mut parser = Parser::new(&DatabricksDialect {})
            .try_with_sql(sql)
            .unwrap();
        let mut parsed = parser.parse_expr().unwrap();
        assert_eq!(parser.peek_token().token, Token::EOF, "{sql}");
        let displays: Vec<String> = displays.iter().map(|name| (*name).to_string()).collect();
        bind_predicate_qualifiers(&mut parsed, schema, Some(&displays), &[], rule).unwrap();
        parsed.to_string()
    }

    #[test]
    fn alias_qualifiers_bind_on_the_parsed_tree_by_rule() {
        let frame = held_schema(&[("T", "id"), ("T", "Val"), ("T", "s"), ("T", "arr")]);
        let names = ["id", "Val", "s", "arr"];
        assert_eq!(
            bound(&frame, &names, "t.val > 10 AND T.id < 5", IgnoreCase),
            "`T`.`Val` > 10 AND `T`.`id` < 5"
        );
        assert_eq!(
            bound(&frame, &names, "t.s.f > 1", IgnoreCase),
            "`T`.`s`.f > 1"
        );
        assert_eq!(
            bound(&frame, &names, "tb.s.f > 1", IgnoreCase),
            "tb.s.f > 1"
        );
        assert_eq!(
            bound(&frame, &names, "t.arr[0] = 1", IgnoreCase),
            "`T`.`arr`[0] = 1"
        );
        assert_eq!(
            bound(&frame, &names, "(T.Val = 1) OR t.id IN (3, 4)", IgnoreCase),
            "(`T`.`Val` = 1) OR `T`.`id` IN (3, 4)"
        );
        for untouched in ["id > 1", "x.id > 1", "s.f > 1", "T.nope > 1"] {
            assert_eq!(bound(&frame, &names, untouched, IgnoreCase), untouched);
        }
        assert_eq!(
            bound(&frame, &names, "t.id IN (SELECT t.id FROM o t)", IgnoreCase),
            "`T`.`id` IN (SELECT t.id FROM o t)"
        );
        for exact_untouched in ["t.id > 1", "T.ID > 1", "t.s.f > 1"] {
            assert_eq!(
                bound(&frame, &names, exact_untouched, Exact),
                exact_untouched
            );
        }
        assert_eq!(bound(&frame, &names, "T.s.f > 1", Exact), "`T`.`s`.f > 1");
        let joined = held_schema(&[("L", "e0"), ("L", "e1"), ("R", "e2"), ("R", "e3")]);
        let displays = ["id", "Data", "ID", "Data"];
        assert_eq!(
            bound(&joined, &displays, "r.data = 'x'", IgnoreCase),
            "`R`.`e3` = 'x'"
        );
        assert_eq!(
            bound(&joined, &displays, "R.Data = 'x'", Exact),
            "`R`.`e3` = 'x'"
        );
        assert_eq!(
            bound(&joined, &displays, "r.Data = 'x'", Exact),
            "r.Data = 'x'"
        );
    }

    #[test]
    fn two_attributes_under_one_qualifier_refuse_ambiguous() {
        let twins = held_schema(&[("T", "id"), ("T", "ID")]);
        let mut parsed = Parser::new(&DatabricksDialect {})
            .try_with_sql("T.id > 1")
            .unwrap()
            .parse_expr()
            .unwrap();
        let error =
            bind_predicate_qualifiers(&mut parsed, &twins, Some(&[]), &[], IgnoreCase).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("[AMBIGUOUS_REFERENCE] Reference `T`.`id` is ambiguous"),
            "{error}"
        );
        assert_eq!(bound(&twins, &[], "T.ID > 1", Exact), "`T`.`ID` > 1");
    }

    #[test]
    fn a_lambda_parameter_shadows_names_inside_its_own_body_only() {
        let frame = held_schema(&[("T", "id"), ("T", "arr"), ("T", "k")]);
        let names = ["id", "arr", "k"];
        for (sql, rule, want) in [
            (
                "exists(arr, T -> T > 4) AND T.id = 1",
                IgnoreCase,
                "exists(arr, T -> T > 4) AND `T`.`id` = 1",
            ),
            (
                "exists(t.arr, t -> t > 4)",
                IgnoreCase,
                "exists(`T`.`arr`, t -> t > 4)",
            ),
            (
                "exists(arr, T -> T.id > 1)",
                IgnoreCase,
                "exists(arr, T -> T['id'] > 1)",
            ),
            (
                "exists(arr, x -> x > T.id)",
                IgnoreCase,
                "exists(arr, x -> x > `T`.`id`)",
            ),
            (
                "exists(arr, T -> T > 4)",
                Exact,
                "exists(arr, `T` -> `T` > 4)",
            ),
            (
                "exists(arr, T -> t > 4)",
                Exact,
                "exists(arr, `T` -> t > 4)",
            ),
            (
                "exists(arr, T -> exists(arr, t -> T > t)) AND T.k > 0",
                Exact,
                "exists(arr, `T` -> exists(arr, `t` -> `T` > `t`)) AND `T`.`k` > 0",
            ),
            (
                "aggregate(arr, 0, (Acc, X) -> Acc + X) = 18",
                Exact,
                "aggregate(arr, 0, (`Acc`, `X`) -> `Acc` + `X`) = 18",
            ),
        ] {
            assert_eq!(bound(&frame, &names, sql, rule), want, "{sql}");
        }
    }

    #[test]
    fn exact_bare_names_over_twin_displays_bind_one_attribute_or_refuse() {
        let twins = held_schema(&[("P", "e0"), ("P", "e1"), ("P", "w")]);
        let displays: Vec<String> = ["v", "v", "w"].map(str::to_string).to_vec();
        let bind = |sql: &str, attributes: &[&str]| {
            let mut parsed = Parser::new(&DatabricksDialect {})
                .try_with_sql(sql)
                .unwrap()
                .parse_expr()
                .unwrap();
            let attributes: Vec<String> = attributes.iter().map(|a| (*a).to_string()).collect();
            bind_predicate_qualifiers(&mut parsed, &twins, Some(&displays), &attributes, Exact)
                .map(|()| parsed.to_string())
        };
        assert_eq!(bind("v > 15", &["o", "o", "w"]).unwrap(), "`e0` > 15");
        assert_eq!(bind("v > 15", &["", "o", "w"]).unwrap(), "`e0` > 15");
        assert_eq!(
            bind("w > 1 AND V > 2", &["o", "a", "w"]).unwrap(),
            "w > 1 AND V > 2"
        );
        assert_eq!(
            bind("exists(arr, v -> v > 1)", &["o", "a", "w"]).unwrap(),
            "exists(arr, `v` -> `v` > 1)"
        );
        let error = bind("v > 15", &["o", "a", "w"]).unwrap_err().to_string();
        assert!(
            error.contains("[AMBIGUOUS_REFERENCE] Reference `v` is ambiguous"),
            "{error}"
        );
        assert_eq!(bind("v > 15", &[]).unwrap(), "v > 15");
    }
}
