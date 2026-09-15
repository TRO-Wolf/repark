use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::DataType;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::SortOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

use crate::text_partition::{
    partition_leaf_writer, render_partition_key, write_partition_body_row,
};
use crate::{Error, Result, engine_err};

pub(crate) struct PartitionTail<'a> {
    pub(crate) partition_columns: &'a [String],
    pub(crate) partition_at: &'a [usize],
    pub(crate) body_at: usize,
    pub(crate) body_type: &'a DataType,
    pub(crate) dir: &'a Path,
    pub(crate) separator: &'a [u8],
    pub(crate) parts: &'a mut HashMap<Vec<String>, PathBuf>,
    pub(crate) created: &'a mut HashSet<Vec<String>>,
    pub(crate) zone: Tz,
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn append_remaining_sorted(
    head: RecordBatch,
    stream: &mut SendableRecordBatchStream,
    tail: PartitionTail<'_>,
) -> Result<()> {
    let schema = head.schema();
    let mut batches = vec![head];
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(engine_err)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    let mut order = Vec::with_capacity(tail.partition_at.len());
    for (position, column_at) in tail.partition_at.iter().enumerate() {
        order.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(
                tail.partition_columns[position].as_str(),
                *column_at,
            )),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        });
    }
    let mut ordered = batches;
    if let Some(ordering) = LexOrdering::new(order) {
        let memory =
            MemorySourceConfig::try_new_exec(&[ordered], schema, None).map_err(engine_err)?;
        let sort = SortExec::new(ordering, memory);
        let mut stream = sort
            .execute(0, Arc::new(TaskContext::default()))
            .map_err(engine_err)?;
        ordered = Vec::new();
        while let Some(batch) = stream.next().await {
            ordered.push(batch.map_err(engine_err)?);
        }
    }
    let mut open: HashMap<Vec<String>, (BufWriter<File>, PathBuf)> = HashMap::new();
    let mut touched: HashMap<Vec<String>, u64> = HashMap::new();
    let mut tick = 0u64;
    let mut current: Option<Vec<String>> = None;
    for batch in &ordered {
        for row in 0..batch.num_rows() {
            let key = render_partition_key(
                &batch,
                row,
                tail.partition_at,
                tail.partition_columns,
                tail.zone,
            )?;
            if current.as_ref() != Some(&key) {
                for (_, (mut writer, part)) in open.drain() {
                    writer.flush().map_err(|error| {
                        Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                    })?;
                }
                tick += 1;
                partition_leaf_writer(
                    &mut open,
                    &mut touched,
                    tail.parts,
                    tail.created,
                    tail.dir,
                    &key,
                    tick,
                )?;
                current = Some(key);
            }
            let active = current.as_ref().ok_or_else(|| {
                engine_err(DataFusionError::Internal(
                    "text partition fallback lost its writer key".to_string(),
                ))
            })?;
            let (writer, part) = open.get_mut(active).ok_or_else(|| {
                engine_err(DataFusionError::Internal(
                    "text partition fallback writer vanished".to_string(),
                ))
            })?;
            write_partition_body_row(
                writer,
                &batch,
                tail.body_at,
                tail.body_type,
                row,
                part,
                tail.separator,
            )?;
        }
    }
    for (_, (mut writer, part)) in open.drain() {
        writer.flush().map_err(|error| {
            Error::Analysis(format!("text write to {} failed: {error}", part.display()))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    async fn leaf_stats(target: &Path) -> (usize, usize, usize) {
        let mut leaves = 0usize;
        let mut files = 0usize;
        let mut rows = 0usize;
        for entry in std::fs::read_dir(target).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_dir() {
                continue;
            }
            leaves += 1;
            for leaf in std::fs::read_dir(entry.path()).unwrap() {
                let leaf = leaf.unwrap();
                if leaf.path().extension().is_some_and(|ext| ext == "txt") {
                    files += 1;
                    rows += std::fs::read_to_string(leaf.path())
                        .unwrap()
                        .lines()
                        .count();
                }
            }
        }
        (leaves, files, rows)
    }

    #[tokio::test]
    async fn text_partition_fallback_holds_one_part_per_leaf() {
        let session = test_session();
        let mut values: Vec<String> = Vec::new();
        for round in 0..4 {
            for key in 0..300 {
                values.push(format!("('k{key}', 'r{round}-k{key}')"));
            }
        }
        let query = format!(
            "SELECT * FROM (VALUES {}) AS t(k, value)",
            values.join(", ")
        );
        let frame = session.sql(&query).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        crate::text_partition::write_text_partitioned(
            &frame,
            &target,
            "\n",
            &[String::from("k")],
            "UTC",
        )
        .await
        .unwrap();
        assert_eq!(leaf_stats(&target).await, (300, 300, 1200));
        for key in [0, 256, 299] {
            let body =
                std::fs::read_to_string(target.join(format!("k=k{key}")).join("part-00000.txt"))
                    .unwrap();
            let mut lines: Vec<&str> = body.lines().collect();
            lines.sort_unstable();
            let expected: Vec<String> = (0..4).map(|round| format!("r{round}-k{key}")).collect();
            assert_eq!(lines, expected);
        }
    }

    #[tokio::test]
    async fn text_partition_fallback_keeps_below_cap_path() {
        let session = test_session();
        let mut values: Vec<String> = Vec::new();
        for round in 0..2 {
            for key in 0..256 {
                values.push(format!("('k{key}', 'r{round}-k{key}')"));
            }
        }
        let query = format!(
            "SELECT * FROM (VALUES {}) AS t(k, value)",
            values.join(", ")
        );
        let frame = session.sql(&query).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        crate::text_partition::write_text_partitioned(
            &frame,
            &target,
            "\n",
            &[String::from("k")],
            "UTC",
        )
        .await
        .unwrap();
        assert_eq!(leaf_stats(&target).await, (256, 256, 512));
    }
}
