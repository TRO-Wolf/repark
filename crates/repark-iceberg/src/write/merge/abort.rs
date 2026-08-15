//! Best-effort abort of files this MERGE attempt wrote.
//!
//! Design A (M14): on a commit-path `Err`, delete every newly written file this attempt
//! produced, then re-raise the original error. The delete set is threaded from writer
//! results already in hand — never re-derived from the table or manifests (a re-derivation
//! can pick up committed files).

use iceberg::spec::DataFile;
use iceberg::table::Table;

/// Collect `file_path`s from writer-result [`DataFile`]s before the `Vec` moves into
/// `add_files` / `add_deletes`.
pub(super) fn written_file_paths(files: &[DataFile]) -> Vec<String> {
    files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect()
}

/// ===========================================================================================
/// Best-effort `FileIO::delete` of files this attempt wrote. Runs only on the commit-error
/// path, and ONLY when the error proves the commit did not happen: on
/// [`ErrorKind::CommitStateUnknown`] the catalog may have persisted the update (Java:
/// `catch (CommitStateUnknownException) { throw }` AHEAD of the cleanup catch), so deleting
/// the files could corrupt a successful commit — the deletes are skipped and the paths are
/// logged for `remove_orphan_files`-style maintenance instead. Per-file delete failures are
/// logged and never returned, so they cannot mask the original commit error.
/// ===========================================================================================
pub(super) async fn delete_written_files_best_effort(
    table: &Table,
    paths: &[String],
    commit_error: &iceberg::Error,
) {
    if commit_error.kind() == iceberg::ErrorKind::CommitStateUnknown {
        tracing::warn!(
            files = paths.len(),
            "MERGE commit state unknown; leaving written files in place (cleanup skipped so a \
             possibly-successful commit is never corrupted; reclaim via orphan-file maintenance)"
        );
        return;
    }
    let file_io = table.file_io();
    for path in paths {
        if let Err(error) = file_io.delete(path).await {
            tracing::warn!(
                path = %path,
                error = %error,
                "failed to delete written file after rejected MERGE commit"
            );
        }
    }
}
