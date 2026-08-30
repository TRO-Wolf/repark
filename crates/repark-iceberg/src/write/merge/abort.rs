//! Best-effort abort of files this MERGE attempt wrote.

use iceberg::spec::DataFile;
use iceberg::table::Table;

/// Collect `file_path`s from writer-result `DataFile`s before the `Vec` moves into `add_files`.
pub(super) fn written_file_paths(files: &[DataFile]) -> Vec<String> {
    files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect()
}

/// Best-effort `FileIO::delete` of files this attempt wrote.
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
