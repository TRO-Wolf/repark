use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMillisecondType};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use iceberg::maintenance::{DeleteReachableFiles, PrefixMismatchMode};
use iceberg::spec::TableProperties;
use iceberg::table::Table;
use repark_common::spark_error;

use super::remove_orphan_files::normalize_orphan_scan_path;
use crate::iceberg_err;

pub(super) struct FileListRequest {
    pub(super) view: String,
    pub(super) scope: String,
    pub(super) older_than_ms: i64,
    pub(super) prefix_mismatch_mode: PrefixMismatchMode,
    pub(super) equal_schemes: HashMap<String, String>,
    pub(super) equal_authorities: HashMap<String, String>,
}

struct UriParts {
    scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

struct UriMaps {
    schemes: HashMap<String, String>,
    authorities: HashMap<String, String>,
}

pub(super) async fn listed_orphans(
    ctx: &SessionContext,
    table: &Table,
    request: &FileListRequest,
) -> Result<Vec<String>> {
    refuse_gc_disabled(table)?;
    let candidates = view_candidates(ctx, request).await?;
    let referenced = referenced_files(table).await?;
    let maps = UriMaps {
        schemes: equal_schemes_with_defaults(&request.equal_schemes),
        authorities: flatten_map(&request.equal_authorities),
    };
    let mut valid_by_path: HashMap<String, Vec<UriParts>> = HashMap::new();
    for location in &referenced {
        let valid = split_uri(location, &maps);
        valid_by_path
            .entry(valid.path.clone())
            .or_default()
            .push(valid);
    }
    let mut orphans = BTreeSet::new();
    let mut scheme_conflicts = BTreeSet::new();
    let mut authority_conflicts = BTreeSet::new();
    for candidate in candidates {
        let actual = split_uri(&candidate, &maps);
        let Some(valid) = valid_by_path.get(&actual.path) else {
            orphans.insert(candidate);
            continue;
        };
        if valid.iter().any(|valid| prefix_compatible(valid, &actual)) {
            continue;
        }
        match request.prefix_mismatch_mode {
            PrefixMismatchMode::Delete => {
                orphans.insert(candidate);
            }
            PrefixMismatchMode::Ignore => {}
            PrefixMismatchMode::Error => {
                scheme_conflicts.extend(first_conflict(
                    valid,
                    |parts| parts.scheme.as_deref(),
                    &actual,
                ));
                authority_conflicts.extend(first_conflict(
                    valid,
                    |parts| parts.authority.as_deref(),
                    &actual,
                ));
            }
        }
    }
    if !scheme_conflicts.is_empty() || !authority_conflicts.is_empty() {
        return Err(prefix_conflict_error(
            &scheme_conflicts,
            &authority_conflicts,
        ));
    }
    Ok(orphans.into_iter().collect())
}

fn first_conflict(
    valid: &[UriParts],
    component: impl Fn(&UriParts) -> Option<&str>,
    actual: &UriParts,
) -> Option<(String, String)> {
    valid
        .iter()
        .find(|valid| !component_matches(component(valid), component(actual)))
        .map(|valid| {
            (
                component(valid).unwrap_or_default().to_string(),
                component(actual).unwrap_or_default().to_string(),
            )
        })
}

fn prefix_conflict_error(
    scheme_conflicts: &BTreeSet<(String, String)>,
    authority_conflicts: &BTreeSet<(String, String)>,
) -> DataFusionError {
    let mut conflicts: Vec<String> = scheme_conflicts
        .iter()
        .chain(authority_conflicts)
        .map(|(valid, actual)| format!("({valid}, {actual})"))
        .collect();
    conflicts.sort();
    iceberg_err(iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        format!(
            "Unable to determine whether certain files are orphan. Metadata references files that \
             match listed/provided files except for authority/scheme. Please, inspect the \
             conflicting authorities/schemes and provide which of them are equal by further \
             configuring the action via equalSchemes() and equalAuthorities() methods. Set the \
             prefix mismatch mode to 'IGNORE' to skip remaining locations with conflicting \
             authorities/schemes or to 'DELETE' iff you are ABSOLUTELY confident that remaining \
             conflicting authorities/schemes are different. It will be impossible to recover \
             deleted files. Conflicting authorities/schemes: [{}].",
            conflicts.join(", ")
        ),
    ))
}

fn refuse_gc_disabled(table: &Table) -> Result<()> {
    let properties = table.metadata().properties();
    let key = TableProperties::PROPERTY_GC_ENABLED;
    let enabled = match properties.get(key) {
        None => TableProperties::PROPERTY_GC_ENABLED_DEFAULT,
        Some(raw) => raw.parse::<bool>().map_err(|error| {
            iceberg_err(
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("Invalid boolean value '{raw}' for table property '{key}'"),
                )
                .with_source(error),
            )
        })?,
    };
    if enabled {
        return Ok(());
    }
    Err(iceberg_err(iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)"
            .to_string(),
    )))
}

async fn view_candidates(ctx: &SessionContext, request: &FileListRequest) -> Result<Vec<String>> {
    let view = request.view.as_str();
    if !ctx.table_exist(view)? {
        let relation = view
            .split('.')
            .map(|part| format!("`{part}`"))
            .collect::<Vec<_>>()
            .join(".");
        return Err(DataFusionError::Plan(spark_error::message(
            spark_error::TABLE_OR_VIEW_NOT_FOUND,
            &[("relationName", relation.as_str())],
        )));
    }
    let batches = ctx.table(view).await?.collect().await?;
    let scope = normalize_orphan_scan_path(&request.scope);
    let mut out = Vec::new();
    for batch in &batches {
        let (paths, stamps) = file_list_columns(batch)?;
        let paths = paths.as_string::<i32>();
        let stamps = stamps.as_primitive::<TimestampMillisecondType>();
        for row in 0..batch.num_rows() {
            if paths.is_null(row) || stamps.is_null(row) {
                continue;
            }
            let path = paths.value(row);
            if stamps.value(row) < request.older_than_ms
                && normalize_orphan_scan_path(path).starts_with(&scope)
            {
                out.push(path.to_string());
            }
        }
    }
    Ok(out)
}

fn file_list_columns(batch: &RecordBatch) -> Result<(Arc<dyn Array>, Arc<dyn Array>)> {
    let schema = batch.schema();
    let column = |name: &str| {
        schema.index_of(name).map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL remove_orphan_files `file_list_view` has no `{name}` column; the view \
                 must carry file_path STRING and last_modified TIMESTAMP"
            ))
        })
    };
    let path_index = column("file_path")?;
    let stamp_index = column("last_modified")?;
    let path_type = schema.field(path_index).data_type();
    if !matches!(
        path_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(DataFusionError::Plan(format!(
            "Invalid file_path column: {path_type} is not a string"
        )));
    }
    let DataType::Timestamp(_, zone) = schema.field(stamp_index).data_type() else {
        return Err(DataFusionError::Plan(format!(
            "Invalid last_modified column: {} is not a timestamp",
            schema.field(stamp_index).data_type()
        )));
    };
    let paths = cast(batch.column(path_index), &DataType::Utf8)?;
    let stamps = cast(
        batch.column(stamp_index),
        &DataType::Timestamp(TimeUnit::Millisecond, zone.clone()),
    )?;
    Ok((paths, stamps))
}

async fn referenced_files(table: &Table) -> Result<Vec<String>> {
    let metadata_location = table.metadata_location().ok_or_else(|| {
        DataFusionError::Execution(
            "CALL remove_orphan_files cannot compare a file_list_view against a table with no \
             metadata location — refusing rather than deleting against an unknown reference set"
                .to_string(),
        )
    })?;
    let collected: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&collected);
    DeleteReachableFiles::new(metadata_location)
        .io(table.file_io().clone())
        .delete_with(move |path| {
            sink.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(path);
            Box::pin(async { Ok(()) })
        })
        .execute()
        .await
        .map_err(iceberg_err)?;
    let referenced = std::mem::take(&mut *collected.lock().unwrap_or_else(PoisonError::into_inner));
    Ok(referenced)
}

fn equal_schemes_with_defaults(user: &HashMap<String, String>) -> HashMap<String, String> {
    let mut schemes = HashMap::from([
        ("s3n".to_string(), "s3".to_string()),
        ("s3a".to_string(), "s3".to_string()),
    ]);
    schemes.extend(flatten_map(user));
    schemes
}

fn flatten_map(map: &HashMap<String, String>) -> HashMap<String, String> {
    let mut flattened = HashMap::new();
    for (key, value) in map {
        for split_key in key.split(',') {
            flattened.insert(split_key.trim().to_string(), value.trim().to_string());
        }
    }
    flattened
}

fn prefix_compatible(valid: &UriParts, actual: &UriParts) -> bool {
    component_matches(valid.scheme.as_deref(), actual.scheme.as_deref())
        && component_matches(valid.authority.as_deref(), actual.authority.as_deref())
}

fn component_matches(valid: Option<&str>, actual: Option<&str>) -> bool {
    match valid {
        None | Some("") => true,
        Some(valid) => actual.is_some_and(|actual| valid.eq_ignore_ascii_case(actual)),
    }
}

fn split_uri(location: &str, maps: &UriMaps) -> UriParts {
    let (scheme, rest) = match scheme_end(location) {
        Some(end) => (
            location.get(..end),
            location.get(end + 1..).unwrap_or_default(),
        ),
        None => (None, location),
    };
    let scheme = scheme.map(|raw| {
        maps.schemes
            .get(raw)
            .map_or(raw, String::as_str)
            .to_string()
    });
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find(['/', '?', '#']).unwrap_or(after.len());
        let (authority, path) = after.split_at(end);
        let authority = maps
            .authorities
            .get(authority)
            .map_or(authority, String::as_str)
            .to_string();
        return UriParts {
            scheme,
            authority: Some(authority),
            path: path.to_string(),
        };
    }
    let end = rest.find(['?', '#']).unwrap_or(rest.len());
    UriParts {
        scheme,
        authority: None,
        path: rest.get(..end).unwrap_or(rest).to_string(),
    }
}

fn scheme_end(location: &str) -> Option<usize> {
    let bytes = location.as_bytes();
    if !bytes.first()?.is_ascii_alphabetic() {
        return None;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b':' => return Some(index),
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.') => {}
            _ => return None,
        }
    }
    None
}
