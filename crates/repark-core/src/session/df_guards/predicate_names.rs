use std::ops::ControlFlow;

use datafusion::common::{DataFusionError, Result, plan_datafusion_err};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr as SqlExpr, Ident, LambdaFunctionParameter, OneOrManyWithParens, Query,
    VisitMut, VisitorMut, visit_expressions_mut,
};
use repark_common::names::NameRule;
use repark_common::spark_error;

use super::attr_id::{Resolution, resolve};
use super::self_join::JoinSide;

const MAX_QUALIFIER_PARTS: usize = 3;

#[allow(clippy::missing_errors_doc)]
pub fn bind_condition_qualifiers(
    condition: &mut SqlExpr,
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    rule: NameRule,
    placeholder: &str,
) -> Result<bool> {
    let mut binder = QualifierBinder {
        sides: [left, right],
        rule,
        placeholder,
        scopes: Vec::new(),
        query_depth: 0,
        rewritten: false,
    };
    match condition.visit(&mut binder) {
        ControlFlow::Break(error) => Err(error),
        ControlFlow::Continue(()) => Ok(binder.rewritten),
    }
}

pub fn restore_placeholders(condition: &mut SqlExpr, placeholder: &str, texts: &[String]) {
    let _ = visit_expressions_mut(condition, |node| {
        match node {
            SqlExpr::Identifier(ident) => restore(ident, placeholder, texts),
            SqlExpr::CompoundIdentifier(parts) => {
                if let Some(first) = parts.first_mut() {
                    restore(first, placeholder, texts);
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
}

fn restore(ident: &mut Ident, placeholder: &str, texts: &[String]) {
    if ident.quote_style.is_some() {
        return;
    }
    let Some(text) = ident
        .value
        .strip_prefix(placeholder)
        .and_then(|index| index.parse::<usize>().ok())
        .and_then(|index| texts.get(index))
    else {
        return;
    };
    *ident = Ident {
        value: text.clone(),
        quote_style: None,
        span: ident.span,
    };
}

struct QualifierBinder<'a> {
    sides: [&'a JoinSide<'a>; 2],
    rule: NameRule,
    placeholder: &'a str,
    scopes: Vec<Vec<String>>,
    query_depth: usize,
    rewritten: bool,
}

struct Binding {
    width: usize,
    alias: String,
    engine: String,
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
        let outcome = match expr {
            SqlExpr::Lambda(lambda) => {
                let params: Vec<&LambdaFunctionParameter> = match &lambda.params {
                    OneOrManyWithParens::One(param) => vec![param],
                    OneOrManyWithParens::Many(params) => params.iter().collect(),
                };
                self.scopes.push(
                    params
                        .iter()
                        .map(|param| param.name.value.clone())
                        .collect(),
                );
                Ok(())
            }
            SqlExpr::CompoundIdentifier(parts) => self.bind_compound(parts),
            SqlExpr::CompoundFieldAccess { root, access_chain } => {
                self.bind_field_access(root, access_chain)
            }
            _ => Ok(()),
        };
        match outcome {
            Ok(()) => ControlFlow::Continue(()),
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
    fn shadowed(&self, written: &str) -> bool {
        self.scopes
            .iter()
            .flatten()
            .any(|name| self.rule.matches(written, name))
    }

    fn skipped(&self, root: &Ident) -> bool {
        self.shadowed(&root.value)
            || (root.quote_style.is_none() && root.value.starts_with(self.placeholder))
    }

    fn bind_compound(&mut self, parts: &mut Vec<Ident>) -> Result<()> {
        let Some(root) = parts.first() else {
            return Ok(());
        };
        if self.skipped(root) {
            return Ok(());
        }
        let written = parts
            .iter()
            .map(|part| part.value.clone())
            .collect::<Vec<_>>();
        let Some(binding) = self.binding(&written)? else {
            return Ok(());
        };
        let rest = parts.split_off(binding.width + 1);
        *parts = vec![
            Ident::new(binding.alias),
            Ident::with_quote('`', binding.engine),
        ];
        parts.extend(rest);
        self.rewritten = true;
        Ok(())
    }

    fn bind_field_access(
        &mut self,
        root: &mut SqlExpr,
        access_chain: &mut Vec<AccessExpr>,
    ) -> Result<()> {
        let SqlExpr::Identifier(head) = root else {
            return Ok(());
        };
        if self.skipped(head) {
            return Ok(());
        }
        let written = std::iter::once(head.value.clone())
            .chain(access_chain.iter().map_while(|access| match access {
                AccessExpr::Dot(SqlExpr::Identifier(part)) => Some(part.value.clone()),
                _ => None,
            }))
            .collect::<Vec<_>>();
        let Some(binding) = self.binding(&written)? else {
            return Ok(());
        };
        *head = Ident::new(binding.alias);
        access_chain.drain(..binding.width);
        access_chain.insert(
            0,
            AccessExpr::Dot(SqlExpr::Identifier(Ident::with_quote('`', binding.engine))),
        );
        self.rewritten = true;
        Ok(())
    }

    fn binding(&self, written: &[String]) -> Result<Option<Binding>> {
        let widest = written.len().saturating_sub(1).min(MAX_QUALIFIER_PARTS);
        for width in (1..=widest).rev() {
            let qualifier = written[..width].join(".");
            let name = &written[width];
            let mut bound = Vec::new();
            for side in self.sides {
                match resolve(
                    side.schema,
                    name,
                    Some(&qualifier),
                    self.rule,
                    side.displays,
                    side.qualifiers,
                )? {
                    Resolution::Bound(hits) => {
                        if let Some(&position) = hits.first() {
                            bound.push((side, position));
                        }
                    }
                    Resolution::Ambiguous(hits) => {
                        return Err(ambiguous(&written[..width], name, side.displays, &hits));
                    }
                    Resolution::Missing => {}
                }
            }
            if let [(side, position)] = bound.as_slice() {
                return Ok(Some(Binding {
                    width,
                    alias: side.alias.to_string(),
                    engine: side.schema.field(*position).name().clone(),
                }));
            }
            if !bound.is_empty() {
                return Ok(None);
            }
        }
        Ok(None)
    }
}

fn quoted(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn ambiguous(
    qualifier: &[String],
    name: &str,
    displays: &[String],
    hits: &[usize],
) -> DataFusionError {
    let prefix = qualifier
        .iter()
        .map(|part| quoted(part))
        .collect::<Vec<_>>()
        .join(".");
    let options = hits
        .iter()
        .filter_map(|&position| displays.get(position))
        .map(|display| format!("{prefix}.{}", quoted(display)))
        .collect::<Vec<_>>()
        .join(", ");
    plan_datafusion_err!(
        "{}",
        spark_error::message(
            spark_error::AMBIGUOUS_REFERENCE,
            &[
                ("reference", &format!("{prefix}.{}", quoted(name))),
                ("options", &options),
            ],
        )
    )
}
