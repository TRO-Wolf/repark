//! COW rewrite file-scoped scan helpers (R-MERGE-FILE-SCAN / M2).
//!
//! After `affected_files` discovery, the rewrite pass only needs data files that contain at
//! least one mutated row. Opening the full snapshot and residual-filtering `_file IN (…)`
//! post-download is wasteful on multi-file S3 tables.
//!
//! Session conf lives on [`crate::write::scan_prune::ReparkMergeConfig::file_scoped_rewrite`]
//! (`repark.merge.file-scoped-rewrite`, default **true**). Whole-file reads of the **subset**
//! are survivor-safe (P2 STOP only forbids residual *row* filters).

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::scan::FileScanTask;

/// Session conf key (hyphen form).
pub const FILE_SCOPED_REWRITE_KEY: &str = "repark.merge.file-scoped-rewrite";

/// Underscore alt accepted at parse time.
pub const FILE_SCOPED_REWRITE_KEY_ALT: &str = "repark.merge.file_scoped_rewrite";

/// Parse a raw conf string (`"true"` / `"false"`, case-insensitive). Missing key → default true
/// is handled by the caller (extension default).
///
/// # Errors
/// Unknown values fail loud naming the key and accepted set.
pub fn parse_file_scoped_rewrite(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "config `{FILE_SCOPED_REWRITE_KEY}` must be true or false (got {raw:?})"
        ))),
    }
}

/// ===========================================================================================
/// Pull the conf from a builder map (hyphen + underscore). Missing → default **true**.
/// ===========================================================================================
///
/// # Errors
/// Present but unparsable value.
pub fn file_scoped_rewrite_from_config_map<S>(
    config: &std::collections::HashMap<String, String, S>,
) -> Result<bool>
where
    S: std::hash::BuildHasher,
{
    match config
        .get(FILE_SCOPED_REWRITE_KEY)
        .or_else(|| config.get(FILE_SCOPED_REWRITE_KEY_ALT))
    {
        Some(raw) => parse_file_scoped_rewrite(raw),
        None => Ok(true),
    }
}

/// ===========================================================================================
/// Keep only tasks whose [`FileScanTask::data_file_path`] is in `allowlist`.
/// ===========================================================================================
#[must_use]
pub fn filter_tasks_to_allowlist<S: std::hash::BuildHasher>(
    tasks: Vec<FileScanTask>,
    allowlist: &HashSet<String, S>,
) -> Vec<FileScanTask> {
    tasks
        .into_iter()
        .filter(|task| allowlist.contains(task.data_file_path()))
        .collect()
}

/// ===========================================================================================
/// File-scoped filter that **fails loud** when a non-empty allowlist does not fully match
/// planned tasks (audit BUG-009 / r22 A2 + critic-octo C2): a path-identity miss would yield
/// a partial/empty rewrite stream while COW still deletes every affected path → silent
/// survivor loss. Over-refuse of a true empty snapshot with a stale allowlist is preferred
/// to under-refuse. **Partial** misses (some allowlist paths match, others do not) refuse too —
/// COW deletes the full allowlist set, not only the matched subset.
///
/// # Errors
/// [`DataFusionError::Plan`] when `allowlist` is non-empty and the set of matched
/// `data_file_path` values is empty **or** a proper subset of the allowlist.
/// ===========================================================================================
pub fn filter_tasks_to_allowlist_nonempty<S: std::hash::BuildHasher>(
    tasks: Vec<FileScanTask>,
    allowlist: &HashSet<String, S>,
) -> Result<Vec<FileScanTask>> {
    let allowlist_len = allowlist.len();
    if allowlist_len == 0 {
        return Ok(filter_tasks_to_allowlist(tasks, allowlist));
    }
    let planned_len = tasks.len();
    let filtered = filter_tasks_to_allowlist(tasks, allowlist);
    let matched_paths: HashSet<&str> = filtered.iter().map(FileScanTask::data_file_path).collect();
    if matched_paths.len() < allowlist_len {
        return Err(DataFusionError::Plan(format!(
            "repark MERGE file-scoped COW rewrite: allowlist selected {allowlist_len} path(s) but \
             only {} path(s) matched FileScanTask(s) (planned {planned_len}) — refusing commit to \
             avoid silent survivor loss from a path-identity miss (audit BUG-009). Workaround: set \
             session conf `repark.merge.file-scoped-rewrite=false` for a full-scan rewrite, or \
             verify affected `_file` paths match the snapshot manifest paths exactly",
            matched_paths.len()
        )));
    }
    Ok(filtered)
}

/// Build an Arc allowlist from affected-file path strings.
#[must_use]
pub fn allowlist_from_paths(paths: &[String]) -> Arc<HashSet<String>> {
    Arc::new(paths.iter().cloned().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc as StdArc;

    fn dummy_task(path: &str) -> FileScanTask {
        let schema = Schema::builder()
            .with_fields(vec![StdArc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 1,
            record_count: None,
            data_file_path: path.to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema: StdArc::new(schema),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
            split_offsets: None,
        }
    }

    #[test]
    fn filter_tasks_keeps_only_allowlisted_paths() {
        let tasks = vec![
            dummy_task("file-a.parquet"),
            dummy_task("file-b.parquet"),
            dummy_task("file-c.parquet"),
        ];
        let allow: HashSet<String> = ["file-b.parquet".to_string()].into_iter().collect();
        let filtered = filter_tasks_to_allowlist(tasks, &allow);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].data_file_path(), "file-b.parquet");
    }

    /// BUG-009: non-empty allowlist + zero matching tasks → loud refuse (path-identity miss).
    #[test]
    fn filter_tasks_nonempty_refuses_allowlist_miss() {
        let tasks = vec![dummy_task("file-a.parquet"), dummy_task("file-b.parquet")];
        let allow: HashSet<String> = ["no-such-path.parquet".to_string()].into_iter().collect();
        let err = filter_tasks_to_allowlist_nonempty(tasks, &allow).expect_err("must refuse");
        let text = err.to_string();
        assert!(
            text.contains("BUG-009") || text.contains("survivor loss"),
            "message must name the hazard, got {text}"
        );
        // Matching path still passes.
        let tasks = vec![dummy_task("file-a.parquet")];
        let allow: HashSet<String> = ["file-a.parquet".to_string()].into_iter().collect();
        let ok = filter_tasks_to_allowlist_nonempty(tasks, &allow).expect("match");
        assert_eq!(ok.len(), 1);
    }

    /// BUG-009 critic F-A2-C2-001: partial allowlist match still refuses (COW deletes full set).
    #[test]
    fn filter_tasks_nonempty_refuses_partial_allowlist_miss() {
        let tasks = vec![
            dummy_task("file-a.parquet"),
            dummy_task("file-b.parquet"),
            // split twin for file-a (multiple tasks per path must not inflate matched count)
            dummy_task("file-a.parquet"),
        ];
        let allow: HashSet<String> = [
            "file-a.parquet".to_string(),
            "missing-path.parquet".to_string(),
        ]
        .into_iter()
        .collect();
        let err = filter_tasks_to_allowlist_nonempty(tasks, &allow).expect_err("partial miss");
        let text = err.to_string();
        assert!(
            text.contains("only 1 path") || text.contains("survivor loss"),
            "partial miss must report matched subset, got {text}"
        );
        // Full allowlist coverage still passes (2 tasks, 1 path, allowlist size 1).
        let tasks = vec![dummy_task("file-a.parquet"), dummy_task("file-a.parquet")];
        let allow: HashSet<String> = ["file-a.parquet".to_string()].into_iter().collect();
        let ok = filter_tasks_to_allowlist_nonempty(tasks, &allow).expect("full cover");
        assert_eq!(ok.len(), 2);
    }

    #[test]
    fn parse_true_false() {
        assert!(parse_file_scoped_rewrite("true").unwrap());
        assert!(!parse_file_scoped_rewrite("false").unwrap());
        assert!(parse_file_scoped_rewrite("maybe").is_err());
    }

    #[test]
    fn config_map_default_and_underscore() {
        let empty = std::collections::HashMap::<String, String>::new();
        assert!(file_scoped_rewrite_from_config_map(&empty).unwrap());
        let mut map = std::collections::HashMap::new();
        map.insert(FILE_SCOPED_REWRITE_KEY_ALT.to_string(), "false".to_string());
        assert!(!file_scoped_rewrite_from_config_map(&map).unwrap());
    }
}
