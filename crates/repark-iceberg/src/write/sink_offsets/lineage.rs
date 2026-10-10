use std::collections::{BTreeMap, BTreeSet};

use std::sync::Arc;

use iceberg::spec::{MAIN_BRANCH, SnapshotRef, TableMetadata};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind};
use serde_json::Value;

use super::append_fence::{AppendFence, refusal_of};
use super::{main_lineage, masked, stamped_by};
use crate::microbatch::error::{MicroBatchError, RecoveryReason};
use crate::microbatch::offset::{
    OFFSETS_PROPERTY_PREFIX, QUERY_ID_KEY, QueryId, SnapshotId, TableUuid,
};
use crate::microbatch::starting_mark::StartingMark;

#[derive(Debug, Clone)]
pub struct SinkMark {
    before: Table,
}

fn refs_of(metadata: &TableMetadata) -> BTreeMap<String, Value> {
    let Ok(Value::Object(mut document)) = serde_json::to_value(metadata) else {
        return BTreeMap::new();
    };
    let Some(Value::Object(refs)) = document.remove("refs") else {
        return BTreeMap::new();
    };
    refs.into_iter()
        .filter(|(name, _)| name != MAIN_BRANCH)
        .collect()
}

fn properties_of(metadata: &TableMetadata) -> BTreeMap<String, String> {
    metadata
        .properties()
        .iter()
        .filter(|(key, _)| !key.starts_with(OFFSETS_PROPERTY_PREFIX))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn shape_of(metadata: &TableMetadata) -> (i32, i32, i64) {
    (
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
        metadata.default_sort_order_id(),
    )
}

fn unstamped(snapshot: &SnapshotRef) -> bool {
    !snapshot
        .summary()
        .additional_properties
        .contains_key(QUERY_ID_KEY)
}

fn commit_of(snapshot: &SnapshotRef) -> RecoveryReason {
    RecoveryReason::UnstampedSinkCommit {
        snapshot: SnapshotId::new(snapshot.snapshot_id()),
        operation: Some(snapshot.summary().operation.as_str().to_string()),
    }
}

fn change(what: String) -> RecoveryReason {
    RecoveryReason::UnstampedSinkChange { what }
}

fn first_difference<Value: PartialEq>(
    before: &BTreeMap<String, Value>,
    after: &BTreeMap<String, Value>,
) -> Option<String> {
    before
        .iter()
        .find(|(key, value)| after.get(*key) != Some(value))
        .or_else(|| after.iter().find(|(key, _)| !before.contains_key(*key)))
        .map(|(key, _)| key.clone())
}

impl SinkMark {
    #[must_use]
    pub fn of(table: &Table) -> SinkMark {
        SinkMark {
            before: table.clone(),
        }
    }

    fn one_commit_since(&self, after: &Table) -> bool {
        let newest_previous = after.metadata().metadata_log().last();
        match (self.before.metadata_location(), newest_previous) {
            (Some(location), Some(previous)) => previous.metadata_file == location,
            _ => false,
        }
    }

    #[must_use]
    pub fn violation(&self, after: &Table) -> Option<RecoveryReason> {
        let (was, metadata) = (self.before.metadata(), after.metadata());
        let (before_uuid, now) = (TableUuid::of(&self.before), TableUuid::of(after));
        if now != before_uuid {
            return Some(change(format!(
                "the table under the sink's name was replaced (uuid {before_uuid}, now {now})"
            )));
        }
        if self.before.metadata_location().is_some()
            && self.before.metadata_location() == after.metadata_location()
        {
            return None;
        }
        let known: BTreeSet<i64> = was
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();
        let added = |snapshot: &&SnapshotRef| !known.contains(&snapshot.snapshot_id());
        if let Some(stray) = main_lineage(metadata)
            .chain(metadata.snapshots())
            .filter(added)
            .find(|snapshot| unstamped(snapshot))
        {
            return Some(commit_of(stray));
        }
        if let Some(gone) = known
            .iter()
            .find(|id| metadata.snapshot_by_id(**id).is_none())
        {
            return Some(change(format!("snapshot {gone} was removed")));
        }
        let head = metadata.current_snapshot_id();
        if head != was.current_snapshot_id() && head.is_none_or(|id| known.contains(&id)) {
            return Some(change(format!(
                "main was moved to the existing snapshot {to}",
                to = head.map_or_else(|| String::from("none"), |id| id.to_string())
            )));
        }
        if let Some(key) = first_difference(&properties_of(was), &properties_of(metadata)) {
            return Some(change(format!("table property {key} changed")));
        }
        if shape_of(was) != shape_of(metadata) {
            return Some(change(String::from(
                "the schema, the partition spec or the sort order changed",
            )));
        }
        let stamped_alone = metadata.snapshots().filter(added).count() == 1;
        if stamped_alone && self.one_commit_since(after) {
            return None;
        }
        first_difference(&refs_of(was), &refs_of(metadata))
            .map(|name| change(format!("branch or tag {name} changed")))
    }
}

#[must_use]
pub fn unstamped_since_stamp(
    table: &Table,
    query: QueryId,
    baseline: Option<i64>,
) -> Option<RecoveryReason> {
    main_lineage(table.metadata())
        .take_while(|snapshot| {
            Some(snapshot.snapshot_id()) != baseline
                && !stamped_by(&snapshot.summary().additional_properties, query)
        })
        .find(|snapshot| unstamped(snapshot))
        .map(commit_of)
}

#[allow(clippy::missing_errors_doc)]
pub fn read_starting_mark(
    table: &Table,
    query: QueryId,
) -> Result<Option<StartingMark>, MicroBatchError> {
    table
        .metadata()
        .properties()
        .get(&StartingMark::property_key(query))
        .map_or(Ok(None), |value| StartingMark::from_property(value))
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_starting_mark(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    query: QueryId,
) -> Result<Table, MicroBatchError> {
    let mark = StartingMark {
        head: table.metadata().current_snapshot_id().map(SnapshotId::new),
    };
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set(StartingMark::property_key(query), mark.property_value())
        .apply(tx)
        .map_err(|error| masked(&error))?;
    let fenced = AppendFence::for_starting_mark(catalog, query);
    match tx.commit(fenced.as_ref()).await {
        Ok(marked) => Ok(marked),
        Err(error) if settled_elsewhere(&error) => catalog
            .load_table(table.identifier())
            .await
            .map_err(|error| masked(&error)),
        Err(error) => Err(masked(&error)),
    }
}

fn settled_elsewhere(error: &iceberg::Error) -> bool {
    error.kind() == ErrorKind::CommitStateUnknown || refusal_of(error).is_some()
}
