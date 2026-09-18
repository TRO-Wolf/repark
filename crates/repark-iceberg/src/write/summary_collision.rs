use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{DataFile, SnapshotSummaryCollector, TableProperties};
use iceberg::table::Table;

const ENGINE_RESERVED_KEYS: [&str; 2] = ["engine-name", "engine-version"];
const ENGINE_RESERVED_VALUE: &str = "<engine-reserved>";
const RESOLVED_AT_COMMIT: &str = "<resolved at commit>";
const TOTALS: [(&str, &str); 6] = [
    ("total-data-files", "added-data-files"),
    ("total-delete-files", "added-delete-files"),
    ("total-records", "added-records"),
    ("total-files-size", "added-files-size"),
    ("total-position-deletes", "added-position-deletes"),
    ("total-equality-deletes", "added-equality-deletes"),
];
const CHANGED_PARTITION_COUNT: &str = "changed-partition-count";
const DATA_REMOVAL_KEYS: [&str; 3] = [
    "deleted-data-files",
    "deleted-records",
    "removed-files-size",
];

#[derive(Debug, Default, Clone)]
pub struct EngineSummary {
    keys: HashMap<String, Option<String>>,
}

impl EngineSummary {
    #[must_use]
    pub fn for_append(table: &Table, added: &[DataFile], branch: Option<&str>) -> Self {
        let computed = added_side(table, added);
        let previous = previous_summary(table, branch);
        let mut keys: HashMap<String, Option<String>> = computed
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect();
        for (total, added_key) in TOTALS {
            let added_value = computed
                .get(added_key)
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            let previous_total = match previous {
                None => Some(0),
                Some(props) => props.get(total).and_then(|value| value.parse::<i64>().ok()),
            };
            let next = previous_total
                .and_then(|value| {
                    if value >= 0 {
                        value.checked_add(added_value)
                    } else {
                        None
                    }
                })
                .filter(|value| *value >= 0);
            if let Some(value) = next {
                keys.insert(total.to_string(), Some(value.to_string()));
            }
        }
        Self { keys }
    }

    #[must_use]
    pub fn for_overwrite(table: &Table, added: &[DataFile], branch: Option<&str>) -> Self {
        let mut keys: HashMap<String, Option<String>> = added_side(table, added)
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect();
        for (total, _) in TOTALS {
            keys.insert(total.to_string(), None);
        }
        keys.insert(CHANGED_PARTITION_COUNT.to_string(), None);
        if previous_summary(table, branch).is_some() {
            for key in DATA_REMOVAL_KEYS {
                keys.insert(key.to_string(), None);
            }
        }
        Self { keys }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn refuse_collision(&self, key: &str, value: &str) -> Result<()> {
        let engine_value = match self.keys.get(key) {
            Some(Some(computed)) => computed.as_str(),
            Some(None) => RESOLVED_AT_COMMIT,
            None if ENGINE_RESERVED_KEYS.contains(&key) => ENGINE_RESERVED_VALUE,
            None => return Ok(()),
        };
        Err(DataFusionError::Plan(format!(
            "Multiple entries with same key: {key}={engine_value} and {key}={value} \
             (snapshot-property.{key} collides with an engine-computed snapshot summary key; \
             ICE-WRITE-OPTIONS-1)"
        )))
    }
}

fn added_side(table: &Table, added: &[DataFile]) -> HashMap<String, String> {
    let metadata = table.metadata();
    let limit = metadata
        .properties()
        .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT);
    let mut collector = SnapshotSummaryCollector::default();
    collector.set_partition_summary_limit(limit);
    for file in added {
        let spec = metadata
            .partition_spec_by_id(file.partition_spec_id())
            .cloned()
            .unwrap_or_else(|| metadata.default_partition_spec().clone());
        collector.add_file(file, metadata.current_schema().clone(), spec);
    }
    collector.build()
}

fn previous_summary<'a>(
    table: &'a Table,
    branch: Option<&str>,
) -> Option<&'a HashMap<String, String>> {
    let metadata = table.metadata();
    let snapshot = match branch {
        Some(name) => metadata.snapshot_for_ref(name),
        None => metadata.current_snapshot(),
    };
    snapshot.map(|snapshot| &snapshot.summary().additional_properties)
}
