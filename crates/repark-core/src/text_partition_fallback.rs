use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::SortOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
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

struct TailSourceExec {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl TailSourceExec {
    fn chained(head: RecordBatch, rest: SendableRecordBatchStream) -> SendableRecordBatchStream {
        let schema = head.schema();
        let stream = futures::stream::once(async move { Ok(head) }).chain(rest);
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    fn new(head: RecordBatch, rest: SendableRecordBatchStream) -> Self {
        let schema = head.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            stream: Mutex::new(Some(Self::chained(head, rest))),
            properties,
        }
    }
}

impl std::fmt::Debug for TailSourceExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TailSourceExec")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for TailSourceExec {
    fn fmt_as(
        &self,
        _mode: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "TailSourceExec")
    }
}

impl ExecutionPlan for TailSourceExec {
    fn name(&self) -> &'static str {
        "TailSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "TailSourceExec takes no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "TailSourceExec has one partition, got {partition}"
            )));
        }
        self.stream
            .lock()
            .map_err(|_| {
                DataFusionError::Internal("TailSourceExec lost its tail stream".to_string())
            })?
            .take()
            .ok_or_else(|| DataFusionError::Internal("TailSourceExec runs once".to_string()))
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn append_remaining_sorted(
    head: RecordBatch,
    stream: SendableRecordBatchStream,
    tail: PartitionTail<'_>,
    task_ctx: TaskContext,
) -> Result<usize> {
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
    let Some(ordering) = LexOrdering::new(order) else {
        let mut direct = TailSourceExec::chained(head, stream);
        write_sorted_stream(&mut direct, tail).await?;
        return Ok(0);
    };
    let source: Arc<dyn ExecutionPlan> = Arc::new(TailSourceExec::new(head, stream));
    let sort = SortExec::new(ordering, source);
    let mut out = sort.execute(0, Arc::new(task_ctx)).map_err(engine_err)?;
    write_sorted_stream(&mut out, tail).await?;
    let spilled = sort
        .metrics()
        .and_then(|metrics| metrics.spill_count())
        .unwrap_or(0);
    Ok(spilled)
}

#[allow(clippy::missing_errors_doc)]
async fn write_sorted_stream(
    stream: &mut SendableRecordBatchStream,
    tail: PartitionTail<'_>,
) -> Result<()> {
    let mut open: HashMap<Vec<String>, (BufWriter<File>, PathBuf)> = HashMap::new();
    let mut touched: HashMap<Vec<String>, u64> = HashMap::new();
    let mut tick = 0u64;
    let mut current: Option<Vec<String>> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(engine_err)?;
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
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    #[tokio::test]
    async fn text_partition_fallback_spills_under_small_memory_limit() {
        let session = crate::ReparkSession::builder()
            .memory_limit_bytes(16 * 1024 * 1024)
            .batch_size(512)
            .build()
            .unwrap();
        let keys = 400usize;
        let per_key = 3000usize;
        let rows = keys * per_key;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let padding = "v".repeat(32);
        let mut batches: Vec<RecordBatch> = Vec::new();
        for start in (0..rows).step_by(2_000) {
            let end = (start + 2_000).min(rows);
            let names: Vec<String> = (start..end)
                .map(|index| format!("k{}", index % keys))
                .collect();
            let bodies: Vec<String> = (start..end).map(|_| padding.clone()).collect();
            batches.push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(names)) as ArrayRef,
                        Arc::new(StringArray::from(bodies)) as ArrayRef,
                    ],
                )
                .unwrap(),
            );
        }
        let input = futures::stream::iter(batches.into_iter().map(Ok));
        let mut stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), input));
        let head = stream.next().await.unwrap().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        std::fs::create_dir_all(&target).unwrap();
        let mut parts: HashMap<Vec<String>, PathBuf> = HashMap::new();
        let mut created: HashSet<Vec<String>> = HashSet::new();
        let columns = vec![String::from("k")];
        let at = vec![0usize];
        let body_type = DataType::Utf8;
        let zone: Tz = "UTC".parse().unwrap();
        let probe = session.sql("SELECT 1 AS one").await.unwrap();
        let spilled = append_remaining_sorted(
            head,
            stream,
            PartitionTail {
                partition_columns: &columns,
                partition_at: &at,
                body_at: 1,
                body_type: &body_type,
                dir: &target,
                separator: b"\n",
                parts: &mut parts,
                created: &mut created,
                zone,
            },
            probe.task_ctx(),
        )
        .await
        .unwrap();
        assert!(
            spilled > 0,
            "the sort must spill under a 16 MiB pool over a 54 MiB tail"
        );
        assert_eq!(leaf_stats(&target), (keys, keys, rows));
    }

    fn leaf_stats(target: &Path) -> (usize, usize, usize) {
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
        assert_eq!(leaf_stats(&target), (300, 300, 1200));
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
        assert_eq!(leaf_stats(&target), (256, 256, 512));
    }
}
