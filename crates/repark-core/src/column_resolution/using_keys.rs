use std::sync::Arc;

use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, JoinConstraint as PlanConstraint, Location, Result, Span};
use datafusion::logical_expr::expr::Sort as SortExpr;
use datafusion::logical_expr::{
    Explain, Expr, ExprSchemable, Join, JoinType, LogicalPlan, PlanType, Projection, Sort,
    SubqueryAlias, ToStringifiedPlan,
};

use super::using_marks::EXPLICIT_LINE;
use crate::frame_names::full_key;

const HELPER: &str = "__repark_using_k";

fn explicit(column: &Column) -> bool {
    column.relation.is_some()
        && column
            .spans()
            .iter()
            .any(|span| span.start.line == EXPLICIT_LINE)
}

fn single_coalesce(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ScalarFunction(call) if call.name() == "coalesce" => match call.args.as_slice() {
            [held] => Some(held),
            _ => None,
        },
        _ => None,
    }
}

fn unmarked(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform(|node| {
        let held = single_coalesce(&node)
            .and_then(single_coalesce)
            .and_then(|inner| match inner {
                Expr::Column(column) => Some(column.clone()),
                _ => None,
            });
        Ok(match held {
            Some(mut column) => {
                column.spans_mut().add_span(Span::new(
                    Location {
                        line: EXPLICIT_LINE,
                        column: 1,
                    },
                    Location {
                        line: EXPLICIT_LINE,
                        column: 2,
                    },
                ));
                Transformed::yes(Expr::Column(column))
            }
            None => Transformed::no(node),
        })
    })
}

fn unmark_order(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| match node {
        LogicalPlan::Sort(sort) => {
            let mut moved = false;
            let expr = sort
                .expr
                .into_iter()
                .map(|key| {
                    let held = unmarked(key.expr)?;
                    moved |= held.transformed;
                    Ok(SortExpr {
                        expr: held.data,
                        ..key
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let sorted = LogicalPlan::Sort(Sort {
                expr,
                input: sort.input,
                fetch: sort.fetch,
            });
            Ok(if moved {
                Transformed::yes(sorted)
            } else {
                Transformed::no(sorted)
            })
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}
#[derive(Clone)]
struct KeySet {
    cols: Vec<Column>,
    merged: Expr,
}

struct Walked {
    plan: LogicalPlan,
    open: Vec<KeySet>,
    swapped: Vec<Column>,
}

#[derive(Default)]
struct Pass {
    bail: bool,
    changed: bool,
}

fn set_of<'a>(open: &'a [KeySet], column: &Column) -> Option<&'a KeySet> {
    open.iter().find(|set| set.cols.contains(column))
}

fn each_column(expr: &Expr, mut visit: impl FnMut(&Column)) {
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node {
            visit(column);
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

fn names_explicit(exprs: &[Expr], columns: &[Column]) -> bool {
    let mut found = false;
    for expr in exprs {
        each_column(expr, |column| {
            found |= explicit(column) && columns.contains(column);
        });
    }
    found
}

impl Pass {
    fn merge(&mut self, expr: Expr, open: &[KeySet]) -> Result<Expr> {
        if open.is_empty() {
            return Ok(expr);
        }
        let merged = expr.transform(|node| match &node {
            Expr::Column(column) if !explicit(column) => match set_of(open, column) {
                Some(set) if set.merged != node => Ok(Transformed::yes(set.merged.clone())),
                _ => Ok(Transformed::no(node)),
            },
            _ => Ok(Transformed::no(node)),
        })?;
        self.changed |= merged.transformed;
        Ok(merged.data)
    }

    fn walk(&mut self, plan: LogicalPlan) -> Result<Walked> {
        let walked = self.node(plan)?;
        let plan = walked
            .plan
            .map_subqueries(|sub| self.walk(sub).map(|inner| Transformed::yes(inner.plan)));
        Ok(Walked {
            plan: plan?.data,
            open: walked.open,
            swapped: walked.swapped,
        })
    }

    fn children(&mut self, plan: LogicalPlan) -> Result<(LogicalPlan, Vec<Walked>)> {
        let mut seen = Vec::new();
        let rebuilt = plan.map_children(|child| {
            let walked = self.walk(child)?;
            let plan = walked.plan.clone();
            seen.push(walked);
            Ok(Transformed::yes(plan))
        })?;
        Ok((rebuilt.data, seen))
    }

    fn node(&mut self, plan: LogicalPlan) -> Result<Walked> {
        match plan {
            LogicalPlan::Join(join) => self.join(join),
            LogicalPlan::Projection(projection) => self.projection(&projection),
            LogicalPlan::Sort(sort) => self.sort(sort),
            LogicalPlan::Filter(_) => self.passing(plan),
            LogicalPlan::Limit(_) | LogicalPlan::Distinct(_) | LogicalPlan::Repartition(_) => {
                let exprs = plan.expressions();
                let (plan, mut seen) = self.children(plan)?;
                let below = seen.pop();
                let swapped = below
                    .as_ref()
                    .map(|walked| walked.swapped.clone())
                    .unwrap_or_default();
                self.bail |= names_explicit(&exprs, &swapped);
                Ok(Walked {
                    plan,
                    open: below.map(|walked| walked.open).unwrap_or_default(),
                    swapped,
                })
            }
            LogicalPlan::Aggregate(_) | LogicalPlan::Window(_) => self.redefining(plan),
            LogicalPlan::SubqueryAlias(alias) => {
                let below = self.walk(alias.input.as_ref().clone())?;
                let plan = SubqueryAlias::try_new(Arc::new(below.plan), alias.alias.clone())?;
                Ok(Walked {
                    plan: LogicalPlan::SubqueryAlias(plan),
                    open: Vec::new(),
                    swapped: Vec::new(),
                })
            }
            LogicalPlan::Explain(explain) => {
                let below = self.walk(explain.plan.as_ref().clone())?;
                let shown = below.plan.to_stringified(PlanType::InitialLogicalPlan);
                Ok(Walked {
                    plan: LogicalPlan::Explain(Explain {
                        plan: Arc::new(below.plan),
                        stringified_plans: vec![shown],
                        ..explain
                    }),
                    open: Vec::new(),
                    swapped: Vec::new(),
                })
            }
            other => {
                let exprs = other.expressions();
                let (plan, seen) = self.children(other)?;
                for walked in &seen {
                    self.bail |= names_explicit(&exprs, &walked.swapped);
                }
                Ok(Walked {
                    plan,
                    open: Vec::new(),
                    swapped: Vec::new(),
                })
            }
        }
    }

    fn passing(&mut self, plan: LogicalPlan) -> Result<Walked> {
        let exprs = plan.expressions();
        let (plan, mut seen) = self.children(plan)?;
        let Some(below) = seen.pop() else {
            return Ok(Walked {
                plan,
                open: Vec::new(),
                swapped: Vec::new(),
            });
        };
        self.bail |= names_explicit(&exprs, &below.swapped);
        let merged = exprs
            .into_iter()
            .map(|expr| self.merge(expr, &below.open))
            .collect::<Result<Vec<_>>>()?;
        let plan = plan.with_new_exprs(merged, vec![below.plan])?;
        Ok(Walked {
            plan,
            open: below.open,
            swapped: below.swapped,
        })
    }

    fn redefining(&mut self, plan: LogicalPlan) -> Result<Walked> {
        let exprs = plan.expressions();
        let (plan, mut seen) = self.children(plan)?;
        let Some(below) = seen.pop() else {
            return Ok(Walked {
                plan,
                open: Vec::new(),
                swapped: Vec::new(),
            });
        };
        self.bail |= names_explicit(&exprs, &below.swapped);
        let mut redefined: Vec<Column> = Vec::new();
        for expr in &exprs {
            each_column(expr, |column| {
                if !explicit(column)
                    && !redefined.contains(column)
                    && set_of(&below.open, column)
                        .is_some_and(|set| set.merged != Expr::Column(column.clone()))
                {
                    redefined.push(column.clone());
                }
            });
        }
        if redefined.is_empty() {
            return Ok(Walked {
                plan,
                open: Vec::new(),
                swapped: below.swapped,
            });
        }
        self.bail |= names_explicit(&exprs, &redefined);
        let schema = below.plan.schema();
        let carried = schema
            .iter()
            .map(|(qualifier, field)| {
                let column = Column::new(qualifier.cloned(), field.name());
                match set_of(&below.open, &column) {
                    Some(set) if redefined.contains(&column) => set
                        .merged
                        .clone()
                        .alias_qualified(qualifier.cloned(), field.name()),
                    _ => Expr::Column(column),
                }
            })
            .collect::<Vec<_>>();
        let input = LogicalPlan::Projection(Projection::try_new(carried, Arc::new(below.plan))?);
        self.changed = true;
        Ok(Walked {
            plan: plan.with_new_exprs(exprs, vec![input])?,
            open: Vec::new(),
            swapped: redefined,
        })
    }

    fn projection(&mut self, projection: &Projection) -> Result<Walked> {
        let below = self.walk(projection.input.as_ref().clone())?;
        self.bail |= names_explicit(&projection.expr, &below.swapped);
        let mut swapped = Vec::new();
        let mut exprs = Vec::with_capacity(projection.expr.len());
        for (expr, (qualifier, field)) in projection.expr.iter().zip(projection.schema.iter()) {
            let merged = self.merge(expr.clone(), &below.open)?;
            if &merged == expr {
                if let Expr::Column(column) = expr
                    && below.swapped.contains(column)
                {
                    swapped.push(column.clone());
                }
                exprs.push(merged);
                continue;
            }
            if let Expr::Column(column) = expr {
                swapped.push(column.clone());
            }
            exprs.push(if matches!(expr, Expr::Alias(_)) {
                merged
            } else {
                merged.alias_qualified_with_metadata(
                    qualifier.cloned(),
                    field.name().clone(),
                    Some(FieldMetadata::new_from_field(field)),
                )
            });
        }
        let plan = Projection::try_new(exprs, Arc::new(below.plan))?;
        Ok(Walked {
            plan: LogicalPlan::Projection(plan),
            open: Vec::new(),
            swapped,
        })
    }

    fn sort(&mut self, sort: Sort) -> Result<Walked> {
        let below = self.walk(sort.input.as_ref().clone())?;
        let keys = sort
            .expr
            .iter()
            .map(|key| key.expr.clone())
            .collect::<Vec<_>>();
        if !names_explicit(&keys, &below.swapped) {
            let expr = sort
                .expr
                .into_iter()
                .map(|key| {
                    Ok(SortExpr {
                        expr: self.merge(key.expr, &below.open)?,
                        ..key
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Walked {
                plan: LogicalPlan::Sort(Sort {
                    expr,
                    input: Arc::new(below.plan),
                    fetch: sort.fetch,
                }),
                open: below.open,
                swapped: below.swapped,
            });
        }
        self.carried_sort(sort, below, &keys)
    }

    fn carried_sort(&mut self, sort: Sort, below: Walked, keys: &[Expr]) -> Result<Walked> {
        let LogicalPlan::Projection(shown) = &below.plan else {
            self.bail = true;
            return Ok(Walked {
                plan: LogicalPlan::Sort(Sort {
                    input: Arc::new(below.plan),
                    ..sort
                }),
                open: Vec::new(),
                swapped: Vec::new(),
            });
        };
        let mut carried = shown.expr.clone();
        let mut helpers: Vec<(Column, String)> = Vec::new();
        let taken = |name: &str| {
            let held = |schema: &datafusion::common::DFSchema| {
                schema
                    .fields()
                    .iter()
                    .any(|field| field.name().eq_ignore_ascii_case(name))
            };
            held(shown.schema.as_ref()) || held(shown.input.schema().as_ref())
        };
        let mut next = 0usize;
        for key in keys {
            each_column(key, |column| {
                if explicit(column)
                    && below.swapped.contains(column)
                    && !helpers.iter().any(|(held, _)| held == column)
                {
                    let mut name = format!("{HELPER}{next}");
                    while taken(&name) {
                        next += 1;
                        name = format!("{HELPER}{next}");
                    }
                    next += 1;
                    helpers.push((column.clone(), name));
                }
            });
        }
        if helpers
            .iter()
            .any(|(column, _)| !shown.input.schema().has_column(column))
        {
            self.bail = true;
        }
        for (column, name) in &helpers {
            carried.push(Expr::Column(column.clone()).alias(name));
        }
        let outputs = shown
            .schema
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
            .collect::<Vec<_>>();
        let wide = Projection::try_new(carried, Arc::clone(&shown.input))?;
        let expr = sort
            .expr
            .into_iter()
            .map(|key| {
                let moved = key.expr.transform(|node| match &node {
                    Expr::Column(column) if explicit(column) => {
                        match helpers.iter().find(|(held, _)| held == column) {
                            Some((_, name)) => Ok(Transformed::yes(Expr::Column(
                                Column::new_unqualified(name),
                            ))),
                            None => Ok(Transformed::no(node)),
                        }
                    }
                    _ => Ok(Transformed::no(node)),
                })?;
                Ok(SortExpr {
                    expr: moved.data,
                    ..key
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sorted = LogicalPlan::Sort(Sort {
            expr,
            input: Arc::new(LogicalPlan::Projection(wide)),
            fetch: sort.fetch,
        });
        self.changed = true;
        Ok(Walked {
            plan: LogicalPlan::Projection(Projection::try_new(outputs, Arc::new(sorted))?),
            open: Vec::new(),
            swapped: below.swapped,
        })
    }

    fn join(&mut self, join: Join) -> Result<Walked> {
        let left = self.walk(join.left.as_ref().clone())?;
        let right = self.walk(join.right.as_ref().clone())?;
        let mut open = left.open.clone();
        if !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            open.extend(right.open.clone());
        }
        if !matches!(join.join_constraint, PlanConstraint::Using) {
            return Ok(Walked {
                plan: LogicalPlan::Join(Join {
                    left: Arc::new(left.plan),
                    right: Arc::new(right.plan),
                    ..join
                }),
                open,
                swapped: Vec::new(),
            });
        }
        let mut on = Vec::with_capacity(join.on.len());
        for (left_key, right_key) in &join.on {
            let merged_left = self.merge(left_key.clone(), &left.open)?;
            let merged_right = self.merge(right_key.clone(), &right.open)?;
            if let (Expr::Column(left_col), Expr::Column(right_col)) = (left_key, right_key) {
                let mut cols = Vec::new();
                for column in [left_col, right_col] {
                    match open.iter().position(|set| set.cols.contains(column)) {
                        Some(index) => cols.extend(open.remove(index).cols),
                        None => cols.push(column.clone()),
                    }
                }
                let merged = match join.join_type {
                    JoinType::Right => Some(merged_right.clone()),
                    JoinType::Full => {
                        let left_type = merged_left.get_type(join.schema.as_ref())?;
                        let right_type = merged_right.get_type(join.schema.as_ref())?;
                        full_key(
                            merged_left.clone(),
                            &left_type,
                            merged_right.clone(),
                            &right_type,
                        )
                    }
                    _ => Some(merged_left.clone()),
                };
                match merged {
                    Some(merged) => open.push(KeySet { cols, merged }),
                    None => self.bail = true,
                }
            }
            on.push((merged_left, merged_right));
        }
        Ok(Walked {
            plan: LogicalPlan::Join(Join {
                left: Arc::new(left.plan),
                right: Arc::new(right.plan),
                on,
                ..join
            }),
            open,
            swapped: Vec::new(),
        })
    }
}

#[allow(clippy::missing_errors_doc)]
pub(super) fn merge_using_keys(plan: LogicalPlan, merging: bool) -> Result<LogicalPlan> {
    let plan = unmark_order(plan)?;
    if !merging {
        return Ok(plan);
    }
    let mut pass = Pass::default();
    let walked = pass.walk(plan.clone())?;
    if pass.bail || !pass.changed {
        return Ok(plan);
    }
    Ok(walked.plan)
}
