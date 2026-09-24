use std::convert::Infallible;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{Expr, Query, SetExpr, TableFactor, Visit, Visitor};

const BYTES_PER_LEVEL: usize = 32 * 1024;
const BASE_BYTES: usize = 256 * 1024;

pub(super) fn stack_bytes_for(statement: &DfStatement) -> usize {
    let DfStatement::Statement(inner) = statement else {
        return BASE_BYTES;
    };
    let mut depth = Depth {
        open: 0,
        deepest: 0,
        bodies: Vec::new(),
    };
    let _ = inner.visit(&mut depth);
    depth
        .deepest
        .saturating_mul(BYTES_PER_LEVEL)
        .saturating_add(BASE_BYTES)
}

struct Depth {
    open: usize,
    deepest: usize,
    bodies: Vec<usize>,
}

impl Depth {
    fn enter(&mut self, levels: usize) {
        self.open = self.open.saturating_add(levels);
        self.deepest = self.deepest.max(self.open);
    }

    fn leave(&mut self, levels: usize) {
        self.open = self.open.saturating_sub(levels);
    }
}

impl Visitor for Depth {
    type Break = Infallible;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let levels = set_height(&query.body).saturating_add(1);
        self.bodies.push(levels);
        self.enter(levels);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        let levels = self.bodies.pop().unwrap_or(0);
        self.leave(levels);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.enter(1);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.leave(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, _factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.enter(1);
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, _factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.leave(1);
        ControlFlow::Continue(())
    }
}

pub(super) fn set_height(body: &SetExpr) -> usize {
    let mut deepest = 0;
    let mut pending = vec![(body, 1_usize)];
    while let Some((node, depth)) = pending.pop() {
        deepest = deepest.max(depth);
        if let SetExpr::SetOperation { left, right, .. } = node {
            pending.push((left, depth + 1));
            pending.push((right, depth + 1));
        }
    }
    deepest
}

pub struct GrownStack<F> {
    red_zone: usize,
    segment: usize,
    future: Pin<Box<F>>,
}

pub fn on_grown_stack<F: Future>(bytes: usize, future: F) -> GrownStack<F> {
    on_grown_stack_with(bytes, bytes, future)
}

pub fn on_grown_stack_with<F: Future>(red_zone: usize, segment: usize, future: F) -> GrownStack<F> {
    GrownStack {
        red_zone,
        segment,
        future: Box::pin(future),
    }
}

impl<F: Future> Future for GrownStack<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (red_zone, segment) = (self.red_zone, self.segment);
        let future = self.future.as_mut();
        stacker::maybe_grow(red_zone, segment, || future.poll(cx))
    }
}
