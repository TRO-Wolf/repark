use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind};

use super::append_fence::AppendFence;
use super::{BatchScopeGuard, Began, ScopeToken, over_a_stray, scopes, stray_since};
use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::TableUuid;

thread_local! {
    static BODY: RefCell<Option<BodyScope>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub(super) struct BodyScope {
    sink: TableUuid,
    token: ScopeToken,
}

struct ScopedBody<Body> {
    scope: BodyScope,
    body: Pin<Box<Body>>,
}

struct Restore(Option<BodyScope>);

impl Drop for Restore {
    fn drop(&mut self) {
        BODY.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

impl<Body: Future> Future for ScopedBody<Body> {
    type Output = Body::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let outer = BODY.with(|slot| slot.borrow_mut().replace(self.scope.clone()));
        let _restore = Restore(outer);
        self.body.as_mut().poll(context)
    }
}

impl BatchScopeGuard {
    pub fn scope_body<Body: Future>(&self, body: Body) -> impl Future<Output = Body::Output> {
        ScopedBody {
            scope: BodyScope {
                sink: self.sink,
                token: self.token.clone(),
            },
            body: Box::pin(body),
        }
    }
}

fn ambient() -> Option<BodyScope> {
    BODY.with(|slot| slot.borrow().clone())
}

#[must_use]
pub fn in_body_scope() -> bool {
    BODY.with(|slot| slot.borrow().is_some())
}

pub(super) fn ambient_token(table: &Table) -> Option<ScopeToken> {
    ambient()
        .filter(|scope| scope.sink == TableUuid::of(table))
        .map(|scope| scope.token)
}

#[must_use]
pub fn guard_body_catalog(catalog: &Arc<dyn Catalog>) -> Arc<dyn Catalog> {
    match ambient() {
        Some(scope) => AppendFence::for_body(catalog, scope),
        None => Arc::clone(catalog),
    }
}

#[must_use]
pub fn refuse_planned_sink_write(table: &Table) -> Option<MicroBatchError> {
    let scope = ambient().filter(|scope| scope.sink == TableUuid::of(table))?;
    let mut entries = scopes();
    let entry = entries
        .get_mut(&scope.sink)
        .filter(|entry| entry.token == scope.token)?;
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: table.identifier().to_string(),
        epoch: entry.stamp.record.epoch,
    };
    entry.violation = Some(refusal.clone());
    Some(refusal)
}

pub(super) fn watches(scope: &BodyScope, table: &Table) -> bool {
    TableUuid::of(table) == scope.sink
}

pub(super) fn refuse_sink_commit(scope: &BodyScope, table: &Table) -> Option<Error> {
    if !watches(scope, table) {
        return None;
    }
    admit_sink_commit(scope, table).map(refused)
}

fn admit_sink_commit(scope: &BodyScope, table: &Table) -> Option<MicroBatchError> {
    let mut entries = scopes();
    let entry = entries
        .get_mut(&scope.sink)
        .filter(|entry| entry.token == scope.token)?;
    if entry.claimed && entry.committed.is_none() {
        let Began::At(head) = entry.began else {
            return None;
        };
        let stray = stray_since(table.metadata(), head, &entry.stamp)?;
        return Some(over_a_stray(stray, &entry.stamp));
    }
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: table.identifier().to_string(),
        epoch: entry.stamp.record.epoch,
    };
    entry.violation = Some(refusal.clone());
    Some(refusal)
}

fn refused(refusal: MicroBatchError) -> Error {
    Error::new(ErrorKind::DataInvalid, refusal.to_string())
        .with_retryable(false)
        .with_source(refusal)
}

pub(super) fn note_unknown_outcome(scope: &BodyScope) {
    if let Some(entry) = scopes()
        .get_mut(&scope.sink)
        .filter(|entry| entry.token == scope.token)
    {
        entry.outcome_unknown = true;
    }
}
