use std::fmt;

use crate::microbatch::offset::SnapshotId;

const START_AFTER: &str = "repark.cdc.start-after-snapshot-id";

pub const SHARED_STRETCH: &str =
    "another streaming query's batches share that stretch of the sink and would leave with it";
pub const INSIDE_A_SNAPSHOT: &str = "the batch before it ends inside a source snapshot, so no new query name can deliver the rest exactly";

pub const PREVIOUS_GONE: &str =
    "the stamped batch before it is no longer in the table, so no snapshot is left to roll back to";
pub const HEAD_GONE: &str = "the snapshot the query started on is no longer on the sink's main branch, so the driver cannot name a snapshot to roll back to";
pub const HEAD_UNRECORDED: &str = "the query's first stamp does not record the head it started on, so rows that were in the sink before the query cannot be told from a stray";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restart {
    SameName,
    NewName(Option<SnapshotId>),
    Unnamed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discard {
    RollBack { to: SnapshotId, then: Restart },
    EmptyStart,
    Unproven(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrayRemedy {
    pub under: Option<SnapshotId>,
    pub discard: Discard,
    pub keep: Restart,
}

fn new_name(formatter: &mut fmt::Formatter<'_>, after: Option<SnapshotId>) -> fmt::Result {
    formatter.write_str("start the query under a new name")?;
    match after {
        Some(position) => write!(
            formatter,
            " with the reader option {START_AFTER}={position}"
        ),
        None => Ok(()),
    }
}

impl fmt::Display for Discard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Discard::RollBack { to, then } => {
                write!(
                    formatter,
                    "To discard its rows, roll the sink back to snapshot {to} (CALL system.rollback_to_snapshot) and "
                )?;
                match then {
                    Restart::NewName(after) => new_name(formatter, *after)?,
                    Restart::SameName | Restart::Unnamed => {
                        formatter.write_str("start the query again")?;
                    }
                }
                formatter.write_str(".")
            }
            Discard::EmptyStart => formatter.write_str(
                "The query first started on an empty sink, so there is no snapshot to roll back to: discarding its rows is a repair for the operator.",
            ),
            Discard::Unproven(why) => write!(
                formatter,
                "No rollback is offered, because {why}: discarding its rows is a repair for the operator."
            ),
        }
    }
}

impl fmt::Display for StrayRemedy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.under {
            Some(stamp) => write!(
                formatter,
                "It sits under the stamped batch at snapshot {stamp}: rolling the sink back to its newest stamped snapshot does not remove it, and every start refuses while it is there. "
            )?,
            None => formatter.write_str(
                "Every start refuses while that snapshot sits above the newest stamped batch. ",
            )?,
        }
        write!(formatter, "{} ", self.discard)?;
        match self.keep {
            Restart::NewName(None) => {
                formatter.write_str("To keep its rows, ")?;
                new_name(formatter, None)?;
                formatter.write_str(".")
            }
            Restart::NewName(after) => {
                formatter.write_str("To keep its rows, ")?;
                new_name(formatter, after)?;
                formatter.write_str(
                    "; a new name without that option delivers every stamped batch again.",
                )
            }
            Restart::Unnamed | Restart::SameName => formatter.write_str(
                "A new query name is not offered: the newest stamped batch ends inside a source snapshot, so no start-after position continues exactly from it.",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Discard, HEAD_GONE, HEAD_UNRECORDED, INSIDE_A_SNAPSHOT, PREVIOUS_GONE, Restart,
        SHARED_STRETCH, StrayRemedy,
    };
    use crate::microbatch::offset::SnapshotId;

    fn id(value: i64) -> SnapshotId {
        SnapshotId::new(value)
    }

    #[test]
    fn a_stray_above_the_newest_stamp_prints_the_rollback_and_the_full_new_name_recipe() {
        let remedy = StrayRemedy {
            under: None,
            discard: Discard::RollBack {
                to: id(7),
                then: Restart::SameName,
            },
            keep: Restart::NewName(Some(id(41))),
        };
        assert_eq!(
            remedy.to_string(),
            "Every start refuses while that snapshot sits above the newest stamped batch. To discard its rows, roll the sink back to snapshot 7 (CALL system.rollback_to_snapshot) and start the query again. To keep its rows, start the query under a new name with the reader option repark.cdc.start-after-snapshot-id=41; a new name without that option delivers every stamped batch again."
        );
    }

    #[test]
    fn a_stray_under_a_stamp_never_offers_the_rollback_to_the_newest_stamp() {
        let remedy = StrayRemedy {
            under: Some(id(9)),
            discard: Discard::RollBack {
                to: id(7),
                then: Restart::NewName(Some(id(40))),
            },
            keep: Restart::NewName(Some(id(41))),
        };
        assert_eq!(
            remedy.to_string(),
            "It sits under the stamped batch at snapshot 9: rolling the sink back to its newest stamped snapshot does not remove it, and every start refuses while it is there. To discard its rows, roll the sink back to snapshot 7 (CALL system.rollback_to_snapshot) and start the query under a new name with the reader option repark.cdc.start-after-snapshot-id=40. To keep its rows, start the query under a new name with the reader option repark.cdc.start-after-snapshot-id=41; a new name without that option delivers every stamped batch again."
        );
        let first = StrayRemedy {
            discard: Discard::RollBack {
                to: id(3),
                then: Restart::NewName(None),
            },
            ..remedy
        };
        assert!(first.to_string().contains(
            "roll the sink back to snapshot 3 (CALL system.rollback_to_snapshot) and start the query under a new name. To keep"
        ));
    }

    #[test]
    fn a_remedy_the_driver_cannot_prove_is_not_printed() {
        let empty = StrayRemedy {
            under: None,
            discard: Discard::EmptyStart,
            keep: Restart::NewName(None),
        };
        assert_eq!(
            empty.to_string(),
            "Every start refuses while that snapshot sits above the newest stamped batch. The query first started on an empty sink, so there is no snapshot to roll back to: discarding its rows is a repair for the operator. To keep its rows, start the query under a new name."
        );
        let reasons = [
            SHARED_STRETCH,
            INSIDE_A_SNAPSHOT,
            PREVIOUS_GONE,
            HEAD_GONE,
            HEAD_UNRECORDED,
        ];
        for why in reasons {
            let unproven = StrayRemedy {
                under: Some(id(9)),
                discard: Discard::Unproven(why),
                keep: Restart::Unnamed,
            };
            let text = unproven.to_string();
            assert!(!text.contains("roll the sink back to snapshot"));
            assert!(!text.contains("To keep its rows"));
            assert!(text.contains(&format!(
                "No rollback is offered, because {why}: discarding its rows is a repair for the operator. A new query name is not offered: the newest stamped batch ends inside a source snapshot"
            )));
        }
    }
}
