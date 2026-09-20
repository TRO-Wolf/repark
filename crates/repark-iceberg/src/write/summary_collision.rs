use std::collections::HashMap;

use datafusion::error::Result;
use iceberg::spec::{
    DataContentType, DataFile, ManifestContentType, SnapshotSummaryCollector, TableProperties,
};
use iceberg::table::Table;

use crate::write::illegal_argument::illegal_argument_error;
use crate::write::partition_overwrite::{
    PartitionEquality, bind_partition_field, equality_literal, resolve_binding,
};

const ENGINE_RESERVED_KEYS: [&str; 2] = ["engine-name", "engine-version"];
const ENGINE_RESERVED_VALUE: &str = "<engine-reserved>";
const RESOLVED_AT_COMMIT: &str = "<resolved at commit>";
const TOTALS: [(&str, &str, &str); 6] = [
    ("total-data-files", "added-data-files", "deleted-data-files"),
    (
        "total-delete-files",
        "added-delete-files",
        "removed-delete-files",
    ),
    ("total-records", "added-records", "deleted-records"),
    ("total-files-size", "added-files-size", "removed-files-size"),
    (
        "total-position-deletes",
        "added-position-deletes",
        "removed-position-deletes",
    ),
    (
        "total-equality-deletes",
        "added-equality-deletes",
        "removed-equality-deletes",
    ),
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
        Self::for_changes(table, added, &[], branch)
    }

    #[must_use]
    pub fn for_changes(
        table: &Table,
        added: &[DataFile],
        removed: &[DataFile],
        branch: Option<&str>,
    ) -> Self {
        let computed = collected(table, added, removed);
        let previous = previous_summary(table, branch);
        let mut keys: HashMap<String, Option<String>> = computed
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect();
        for (total, added_key, removed_key) in TOTALS {
            let next = updated_total(&computed, previous, total, added_key, removed_key);
            if let Some(value) = next {
                keys.insert(total.to_string(), Some(value.to_string()));
            }
        }
        Self { keys }
    }

    #[must_use]
    pub fn for_overwrite(table: &Table, added: &[DataFile], branch: Option<&str>) -> Self {
        let mut keys: HashMap<String, Option<String>> = collected(table, added, &[])
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect();
        for (total, _, _) in TOTALS {
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
        Err(illegal_argument_error(format!(
            "Multiple entries with same key: {key}={engine_value} and {key}={value}"
        )))
    }
}

#[must_use]
pub fn extras_need_removed_files(extra: &[(String, String)]) -> bool {
    extra.iter().any(|(key, _)| {
        let folded = key.to_ascii_lowercase();
        DATA_REMOVAL_KEYS.contains(&folded.as_str())
            || folded == CHANGED_PARTITION_COUNT
            || TOTALS
                .iter()
                .any(|(total, _, removed)| *total == folded || *removed == folded)
    })
}

async fn live_files(
    table: &Table,
    branch: Option<&str>,
    content: ManifestContentType,
) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let snapshot = match branch {
        Some(name) => metadata.snapshot_for_ref(name),
        None => metadata.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map_err(crate::catalog::iceberg_to_datafusion)?;
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != content {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(crate::catalog::iceberg_to_datafusion)?;
        for entry in manifest.entries() {
            if entry.is_alive() && holds(entry.data_file(), content) {
                files.push(entry.data_file().clone());
            }
        }
    }
    Ok(files)
}

fn holds(file: &DataFile, content: ManifestContentType) -> bool {
    let is_data = file.content_type() == DataContentType::Data;
    match content {
        ManifestContentType::Data => is_data,
        ManifestContentType::Deletes => !is_data,
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn live_data_files(table: &Table, branch: Option<&str>) -> Result<Vec<DataFile>> {
    live_files(table, branch, ManifestContentType::Data).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn live_delete_files(table: &Table, branch: Option<&str>) -> Result<Vec<DataFile>> {
    live_files(table, branch, ManifestContentType::Deletes).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn replaced_data_files(
    table: &Table,
    branch: Option<&str>,
    staged: &[DataFile],
) -> Result<Vec<DataFile>> {
    let replaced: Vec<PartitionKey> = staged
        .iter()
        .map(|file| partition_key_of(table, file))
        .collect();
    let live = live_data_files(table, branch).await?;
    Ok(live
        .into_iter()
        .filter(|file| replaced.contains(&partition_key_of(table, file)))
        .collect())
}

#[allow(clippy::missing_errors_doc)]
pub async fn row_filter_removed_files(
    table: &Table,
    branch: Option<&str>,
    equalities: &[PartitionEquality],
) -> Result<Vec<DataFile>> {
    if equalities.is_empty() {
        return Ok(Vec::new());
    }
    let expected = expected_partition_pairs(table, equalities)?;
    let mut removed = in_partition(table, live_data_files(table, branch).await?, &expected);
    removed.extend(in_partition(
        table,
        live_delete_files(table, branch).await?,
        &expected,
    ));
    Ok(removed)
}

fn in_partition(
    table: &Table,
    files: Vec<DataFile>,
    expected: &[(String, Option<iceberg::spec::Literal>)],
) -> Vec<DataFile> {
    files
        .into_iter()
        .filter(|file| {
            let key = partition_key_of(table, file);
            expected.iter().all(|(name, value)| {
                key.iter()
                    .any(|(field, actual)| field == name && actual == value)
            })
        })
        .collect()
}

fn expected_partition_pairs(
    table: &Table,
    equalities: &[PartitionEquality],
) -> Result<Vec<(String, Option<iceberg::spec::Literal>)>> {
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let bindings = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| bind_partition_field(schema.as_ref(), field))
        .collect::<Result<Vec<_>>>()?;
    equalities
        .iter()
        .map(|equality| {
            let binding = resolve_binding(&bindings, &equality.name)?;
            Ok((
                binding.spec_field_name.clone(),
                equality_literal(binding, equality)?,
            ))
        })
        .collect()
}

type PartitionKey = Vec<(String, Option<iceberg::spec::Literal>)>;

fn partition_key_of(table: &Table, file: &DataFile) -> PartitionKey {
    spec_of(table, file)
        .fields()
        .iter()
        .zip(file.partition().fields())
        .map(|(field, value)| (field.name.clone(), value.clone()))
        .collect()
}

fn updated_total(
    computed: &HashMap<String, String>,
    previous: Option<&HashMap<String, String>>,
    total: &str,
    added_key: &str,
    removed_key: &str,
) -> Option<i64> {
    let added = numeric(computed.get(added_key));
    let removed = numeric(computed.get(removed_key));
    let previous_total = match previous {
        None => Some(0),
        Some(props) => props.get(total).and_then(|value| value.parse::<i64>().ok()),
    };
    previous_total
        .and_then(|value| {
            let mut running = value;
            if running >= 0 {
                running = running.checked_add(added)?;
            }
            if running >= 0 {
                running = running.checked_sub(removed)?;
            }
            Some(running)
        })
        .filter(|value| *value >= 0)
}

fn numeric(raw: Option<&String>) -> i64 {
    raw.and_then(|value| value.parse::<i64>().ok()).unwrap_or(0)
}

fn collected(table: &Table, added: &[DataFile], removed: &[DataFile]) -> HashMap<String, String> {
    let metadata = table.metadata();
    let limit = metadata
        .properties()
        .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT);
    let mut collector = SnapshotSummaryCollector::default();
    collector.set_partition_summary_limit(limit);
    for file in added {
        collector.add_file(
            file,
            metadata.current_schema().clone(),
            spec_of(table, file),
        );
    }
    for file in removed {
        collector.remove_file(
            file,
            metadata.current_schema().clone(),
            spec_of(table, file),
        );
    }
    collector.build()
}

fn spec_of(table: &Table, file: &DataFile) -> iceberg::spec::PartitionSpecRef {
    let metadata = table.metadata();
    metadata
        .partition_spec_by_id(file.partition_spec_id())
        .cloned()
        .unwrap_or_else(|| metadata.default_partition_spec().clone())
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
