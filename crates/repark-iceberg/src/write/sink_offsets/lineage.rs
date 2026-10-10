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
    Epoch, OFFSETS_PROPERTY_PREFIX, QUERY_ID_KEY, QueryId, SPARK_QUERY_ID_KEY, SinkRecord,
    SnapshotId, TableUuid,
};
use crate::microbatch::starting_mark::StartingMark;
use crate::microbatch::stray_remedy::{
    Discard, HEAD_GONE, HEAD_UNRECORDED, INSIDE_A_SNAPSHOT, PREVIOUS_GONE, Restart, SHARED_STRETCH,
    StrayRemedy,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamped {
    pub snapshot: SnapshotId,
    pub record: SinkRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Floor {
    Newest,
    Previous(Stamped),
    Head(Option<SnapshotId>),
    Shared,
    Lost(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stray {
    pub snapshot: SnapshotId,
    pub operation: String,
    pub newest: Option<Stamped>,
    pub below: bool,
    pub floor: Floor,
}

fn stamped_at(snapshot: &SnapshotRef) -> Result<Stamped, MicroBatchError> {
    let id = SnapshotId::new(snapshot.snapshot_id());
    match SinkRecord::from_summary(&snapshot.summary().additional_properties)? {
        Some(record) => Ok(Stamped {
            snapshot: id,
            record,
        }),
        None => Err(MicroBatchError::Catalog(format!(
            "repark.cdc stamp on snapshot {id} misses its format version"
        ))),
    }
}

fn found(
    stretch: &[&SnapshotRef],
    newest: Option<Stamped>,
    below: bool,
    floor: Floor,
) -> Option<Stray> {
    let stray = stretch.iter().find(|snapshot| unstamped(snapshot))?;
    let shared = stretch.iter().any(|snapshot| !unstamped(snapshot));
    Some(Stray {
        snapshot: SnapshotId::new(stray.snapshot_id()),
        operation: stray.summary().operation.as_str().to_string(),
        newest,
        below,
        floor: if shared { Floor::Shared } else { floor },
    })
}

#[allow(clippy::missing_errors_doc)]
pub fn stray_on_main(
    table: &Table,
    query: QueryId,
    baseline: Option<i64>,
) -> Result<Option<Stray>, MicroBatchError> {
    walked(table, query, &Entry::Foreach(baseline))
}

#[allow(clippy::missing_errors_doc)]
pub fn stray_at_a_table_start(
    table: &Table,
    query: QueryId,
) -> Result<Option<Stray>, MicroBatchError> {
    walked(table, query, &Entry::Table)
}

fn table_door_stamp(snapshot: &SnapshotRef) -> bool {
    let summary = &snapshot.summary().additional_properties;
    summary.contains_key(SPARK_QUERY_ID_KEY)
}

fn head_floor(head: Option<i64>, reached: bool) -> Floor {
    match head {
        Some(_) if !reached => Floor::Lost(HEAD_GONE),
        head => Floor::Head(head.map(SnapshotId::new)),
    }
}

enum Entry {
    Foreach(Option<i64>),
    Table,
}

fn walked(table: &Table, query: QueryId, entry: &Entry) -> Result<Option<Stray>, MicroBatchError> {
    let table_door = matches!(entry, Entry::Table);
    let baseline = match entry {
        Entry::Foreach(baseline) => *baseline,
        Entry::Table => match read_starting_mark(table, query)? {
            Some(mark) => mark.head.map(SnapshotId::get),
            None if newest_is_foreach(table, query) => None,
            None => return Ok(None),
        },
    };
    let mine =
        |snapshot: &SnapshotRef| stamped_by(&snapshot.summary().additional_properties, query);
    let mut walk = main_lineage(table.metadata());
    let mut above = Vec::new();
    let mut stamp = None;
    let mut reached = false;
    for snapshot in walk.by_ref() {
        if mine(snapshot) {
            stamp = Some(snapshot);
            break;
        }
        if Some(snapshot.snapshot_id()) == baseline {
            reached = true;
            break;
        }
        above.push(snapshot);
    }
    let Some(stamp) = stamp else {
        return Ok(found(&above, None, false, head_floor(baseline, reached)));
    };
    if table_door && table_door_stamp(stamp) {
        return Ok(None);
    }
    let newest = stamped_at(stamp)?;
    if let Some(stray) = found(&above, Some(newest.clone()), false, Floor::Newest) {
        return Ok(Some(stray));
    }
    let started = StartingMark::from_summary(&stamp.summary().additional_properties);
    let head = started.and_then(|mark| mark.head).map(SnapshotId::get);
    let mut under = Vec::new();
    let mut previous = None;
    let mut reached = false;
    for snapshot in walk {
        if mine(snapshot) {
            previous = Some(stamped_at(snapshot)?);
            break;
        }
        if Some(snapshot.snapshot_id()) == head {
            reached = true;
            break;
        }
        under.push(snapshot);
    }
    let floor = match (previous, started) {
        (Some(previous), _) => Floor::Previous(previous),
        (None, Some(_)) => head_floor(head, reached),
        (None, None) if newest.record.epoch != Epoch::FIRST => Floor::Lost(PREVIOUS_GONE),
        (None, None) if table_door_stamp(stamp) => return Ok(None),
        (None, None) => Floor::Lost(HEAD_UNRECORDED),
    };
    Ok(found(&under, Some(newest), true, floor))
}

fn newest_is_foreach(table: &Table, query: QueryId) -> bool {
    main_lineage(table.metadata())
        .find(|snapshot| stamped_by(&snapshot.summary().additional_properties, query))
        .is_some_and(|stamp| !table_door_stamp(stamp))
}

impl Stray {
    pub fn records(&self) -> impl Iterator<Item = &SinkRecord> {
        let previous = match &self.floor {
            Floor::Previous(stamped) => Some(&stamped.record),
            _ => None,
        };
        self.newest
            .iter()
            .map(|stamped| &stamped.record)
            .chain(previous)
    }

    #[must_use]
    pub fn reason(&self, position: &dyn Fn(&SinkRecord) -> Option<SnapshotId>) -> RecoveryReason {
        let after = |stamped: &Stamped| match position(&stamped.record) {
            Some(at) => Restart::NewName(Some(at)),
            None => Restart::Unnamed,
        };
        let keep = self.newest.as_ref().map_or(Restart::NewName(None), after);
        let then = if self.below {
            Restart::NewName(None)
        } else {
            Restart::SameName
        };
        let discard = match (&self.floor, &self.newest) {
            (Floor::Shared, _) => Discard::Unproven(SHARED_STRETCH),
            (Floor::Lost(why), _) => Discard::Unproven(why),
            (Floor::Newest, Some(newest)) => Discard::RollBack {
                to: newest.snapshot,
                then,
            },
            (Floor::Previous(previous), _) => match after(previous) {
                Restart::NewName(at) => Discard::RollBack {
                    to: previous.snapshot,
                    then: Restart::NewName(at),
                },
                Restart::SameName | Restart::Unnamed => Discard::Unproven(INSIDE_A_SNAPSHOT),
            },
            (Floor::Head(Some(head)), _) => Discard::RollBack { to: *head, then },
            (Floor::Head(None) | Floor::Newest, _) => Discard::EmptyStart,
        };
        let under = match &self.newest {
            Some(newest) if self.below => Some(newest.snapshot),
            _ => None,
        };
        RecoveryReason::StraySinkCommit {
            snapshot: self.snapshot,
            operation: self.operation.clone(),
            remedy: StrayRemedy {
                under,
                discard,
                keep,
            },
        }
    }
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
