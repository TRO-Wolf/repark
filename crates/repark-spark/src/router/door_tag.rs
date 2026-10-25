use std::sync::{Arc, Mutex, PoisonError};

use datafusion::logical_expr::{LogicalPlan, WriteOp};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::write_door::{WriteDoor, tag};

#[derive(Debug, Default)]
pub(crate) struct WriteDoorRecord {
    doors: Mutex<Vec<WriteDoor>>,
}

pub(crate) fn with_write_door_record(config: SessionConfig) -> SessionConfig {
    config.with_extension(Arc::new(WriteDoorRecord::default()))
}

fn session_record(ctx: &SessionContext) -> Option<Arc<WriteDoorRecord>> {
    ctx.state_ref()
        .read()
        .config()
        .get_extension::<WriteDoorRecord>()
}

pub(crate) fn clear(ctx: &SessionContext) {
    if let Some(record) = session_record(ctx) {
        record
            .doors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

pub(crate) fn record(ctx: &SessionContext, door: WriteDoor) {
    if let Some(record) = session_record(ctx) {
        let mut doors = record.doors.lock().unwrap_or_else(PoisonError::into_inner);
        if !doors.contains(&door) {
            doors.push(door);
        }
    }
}

pub(crate) fn record_planned_update(ctx: &SessionContext, plan: &LogicalPlan) {
    if let LogicalPlan::Dml(dml) = plan
        && matches!(dml.op, WriteOp::Update)
    {
        record(ctx, WriteDoor::D10Update);
    }
}

pub(crate) fn enter(ctx: &SessionContext, door: WriteDoor) {
    tag(Some(door));
    record(ctx, door);
}

#[must_use]
pub fn last_write_doors(ctx: &SessionContext) -> Vec<WriteDoor> {
    session_record(ctx).map_or_else(Vec::new, |record| {
        record
            .doors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    })
}
