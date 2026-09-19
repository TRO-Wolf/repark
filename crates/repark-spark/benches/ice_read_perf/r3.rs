use std::io::Write;
use std::path::PathBuf;

use datafusion::arrow::array::{Array, Int64Array};
use repark_core::ReparkSession;

use crate::BoxError;
use crate::cli::boxed;

pub const R3_TABLE_SIZE_LIMIT_BYTES: u64 = 3 * 1024 * 1024 * 1024;

pub const R3_EXIT_CODE: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum R3Verdict {
    WithinLimit,
    Flagged,
}

#[must_use]
pub fn r3_verdict(bytes: u64) -> R3Verdict {
    if bytes > R3_TABLE_SIZE_LIMIT_BYTES {
        R3Verdict::Flagged
    } else {
        R3Verdict::WithinLimit
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepSummary {
    pub path: Option<PathBuf>,
}

impl StepSummary {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            path: std::env::var_os("GITHUB_STEP_SUMMARY")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
        }
    }

    fn append(&self, line: &str) -> Result<(), BoxError> {
        if let Some(path) = &self.path {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            writeln!(file, "{line}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableFootprint {
    pub bytes: u64,
    pub files: u64,
    pub delete_files: u64,
    pub data_rows: u64,
}

impl TableFootprint {
    #[must_use]
    pub fn data_files(&self) -> u64 {
        self.files.saturating_sub(self.delete_files)
    }
}

#[must_use]
pub fn quoted_files_table(table: &str) -> String {
    table
        .split('.')
        .chain(std::iter::once("files"))
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

pub async fn table_footprint(
    session: &ReparkSession,
    table: &str,
) -> Result<TableFootprint, BoxError> {
    let files_table = quoted_files_table(table);
    let sql = format!(
        "SELECT CAST(coalesce(sum(file_size_in_bytes), 0) AS BIGINT) AS bytes, \
         CAST(count(*) AS BIGINT) AS files, \
         CAST(coalesce(sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END), 0) AS BIGINT) AS deletes, \
         CAST(coalesce(sum(CASE WHEN content = 0 THEN record_count ELSE 0 END), 0) AS BIGINT) \
         AS data_rows FROM {files_table}"
    );
    let batches = session.sql(&sql).await?.collect().await?;
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .ok_or_else(|| boxed(format!("`{sql}` returned no row")))?;
    let cell = |index: usize| -> Result<u64, BoxError> {
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| boxed(format!("files column {index} is not BIGINT")))?;
        if column.is_null(0) {
            return Err(boxed(format!("files column {index} is NULL")));
        }
        Ok(u64::try_from(column.value(0))?)
    };
    Ok(TableFootprint {
        bytes: cell(0)?,
        files: cell(1)?,
        delete_files: cell(2)?,
        data_rows: cell(3)?,
    })
}

pub fn r3_check(table: &str, bytes: u64, summary: &StepSummary) -> Result<R3Verdict, BoxError> {
    println!("R3 table={table} bytes={bytes}");
    let verdict = r3_verdict(bytes);
    if verdict == R3Verdict::Flagged {
        let line =
            format!("R3-SIZE-FLAG table={table} bytes={bytes} limit={R3_TABLE_SIZE_LIMIT_BYTES}");
        println!("{line}");
        eprintln!("{line}");
        summary.append(&line)?;
    }
    Ok(verdict)
}

pub async fn r3_gate(
    session: &ReparkSession,
    table: &str,
    summary: &StepSummary,
    size_override: Option<u64>,
) -> Result<(TableFootprint, R3Verdict), BoxError> {
    let measured = table_footprint(session, table).await?;
    let footprint = TableFootprint {
        bytes: size_override.unwrap_or(measured.bytes),
        ..measured
    };
    let verdict = r3_check(table, footprint.bytes, summary)?;
    Ok((footprint, verdict))
}
