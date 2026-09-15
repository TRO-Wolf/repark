use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{SortOptions, concat_batches};
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryLimit;
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
use futures::{Stream, StreamExt};

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

    fn wrap(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            stream: Mutex::new(Some(stream)),
            properties,
        }
    }
}

struct TailRechunk {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    max_bytes: usize,
    ready: VecDeque<RecordBatch>,
}

impl TailRechunk {
    fn split_batch(&mut self, batch: RecordBatch) -> datafusion::error::Result<()> {
        let rows = batch.num_rows();
        let per_row = (batch.get_array_memory_size() / rows).max(1);
        let max_rows = (self.max_bytes / per_row).max(1);
        let empty = RecordBatch::new_empty(Arc::clone(&self.schema));
        let mut stack = vec![(batch, 0, rows)];
        while let Some((source, offset, len)) = stack.pop() {
            if len <= 1 {
                let slice = source.slice(offset, len);
                self.ready
                    .push_back(concat_batches(&self.schema, [&empty, &slice])?);
                continue;
            }
            let width = max_rows.min(len);
            if width >= len {
                let slice = source.slice(offset, len);
                let fresh = concat_batches(&self.schema, [&empty, &slice])?;
                if get_record_batch_memory_size(&fresh) <= self.max_bytes {
                    self.ready.push_back(fresh);
                    continue;
                }
            }
            let half = len / 2;
            stack.push((source.clone(), offset, half));
            stack.push((source, offset + half, len - half));
        }
        Ok(())
    }
}

impl Stream for TailRechunk {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.ready.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            match ready!(self.inner.as_mut().poll_next(cx)) {
                None => return Poll::Ready(None),
                Some(Err(error)) => return Poll::Ready(Some(Err(error))),
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    if batch.get_array_memory_size() <= self.max_bytes {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    if let Err(error) = self.split_batch(batch) {
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
    }
}

fn fallback_pool_bytes(task_ctx: &TaskContext) -> Option<usize> {
    match task_ctx.memory_pool().memory_limit() {
        MemoryLimit::Finite(bytes) => Some(bytes),
        MemoryLimit::Infinite | MemoryLimit::Unknown => None,
    }
}

fn fallback_sort_context(task_ctx: TaskContext, pool: usize) -> (TaskContext, usize) {
    let configured = task_ctx
        .session_config()
        .options()
        .execution
        .sort_spill_reservation_bytes;
    let reservation = configured.min(pool);
    let max_bytes = (pool.saturating_sub(reservation) / 3).max(1);
    let mut config = task_ctx.session_config().clone();
    config.options_mut().execution.sort_spill_reservation_bytes = reservation;
    (task_ctx.with_session_config(config), max_bytes)
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
    let schema = head.schema();
    let chained = TailSourceExec::chained(head, stream);
    let (task_ctx, max_bytes) = match fallback_pool_bytes(&task_ctx) {
        Some(pool) => fallback_sort_context(task_ctx, pool),
        None => (task_ctx, usize::MAX),
    };
    let rechunked = TailRechunk {
        inner: chained,
        schema: Arc::clone(&schema),
        max_bytes,
        ready: VecDeque::new(),
    };
    let adapted: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        rechunked,
    ));
    let source: Arc<dyn ExecutionPlan> = Arc::new(TailSourceExec::wrap(schema, adapted));
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
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::logical_expr::TableProviderFilterPushDown;

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    fn fat_batch(schema: &SchemaRef, offset: usize, len: usize, keys: usize) -> RecordBatch {
        let names: Vec<String> = (offset..offset + len)
            .map(|index| format!("k{}", index % keys))
            .collect();
        let bodies: Vec<String> = (offset..offset + len)
            .map(|index| format!("{index:016}-{}", "x".repeat(4079)))
            .collect();
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(StringArray::from(bodies)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn peak_rss_mib() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    let tail = line.strip_prefix("VmHWM:")?;
                    tail.split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                        .map(|kib| kib / 1024)
                })
            })
            .unwrap_or(0)
    }

    struct FatRowsExec {
        schema: SchemaRef,
        partitions: usize,
        rows_per_partition: usize,
        batch_rows: usize,
        keys: usize,
        properties: Arc<PlanProperties>,
    }

    impl FatRowsExec {
        fn new(
            partitions: usize,
            rows_per_partition: usize,
            batch_rows: usize,
            keys: usize,
        ) -> Self {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                partitions,
                rows_per_partition,
                batch_rows,
                keys,
                properties,
            }
        }
    }

    impl std::fmt::Debug for FatRowsExec {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("FatRowsExec")
                .field("schema", &self.schema)
                .finish_non_exhaustive()
        }
    }

    impl DisplayAs for FatRowsExec {
        fn fmt_as(
            &self,
            _mode: DisplayFormatType,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(formatter, "FatRowsExec")
        }
    }

    impl ExecutionPlan for FatRowsExec {
        fn name(&self) -> &'static str {
            "FatRowsExec"
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
                    "FatRowsExec takes no children".to_string(),
                ))
            }
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::error::Result<SendableRecordBatchStream> {
            if partition >= self.partitions {
                return Err(DataFusionError::Internal(format!(
                    "FatRowsExec has {} partitions, got {partition}",
                    self.partitions
                )));
            }
            let schema = Arc::clone(&self.schema);
            let start = partition * self.rows_per_partition;
            let end = start + self.rows_per_partition;
            let batch_rows = self.batch_rows;
            let keys = self.keys;
            let stream =
                futures::stream::unfold((start, schema), move |(offset, schema)| async move {
                    if offset >= end {
                        return None;
                    }
                    let len = batch_rows.min(end - offset);
                    let batch = fat_batch(&schema, offset, len, keys);
                    Some((Ok(batch), (offset + len, schema)))
                });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            )))
        }
    }

    #[derive(Debug)]
    struct FatRowsProvider {
        exec: Arc<FatRowsExec>,
    }

    #[async_trait]
    impl TableProvider for FatRowsProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.exec.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&datafusion::logical_expr::Expr],
        ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[datafusion::logical_expr::Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(self.exec.clone())
        }
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
    async fn text_partition_fallback_spills_fat_tail_under_session_pool() {
        for pool in [128usize * 1024 * 1024, 512 * 1024 * 1024] {
            let session = crate::ReparkSession::builder()
                .memory_limit_bytes(pool)
                .batch_size(1024)
                .build()
                .unwrap();
            let rows = 500_000usize;
            let keys = 1000usize;
            let partitions = 8usize;
            let exec = Arc::new(FatRowsExec::new(partitions, rows / partitions, 1024, keys));
            let frame = session
                .context()
                .read_table(Arc::new(FatRowsProvider { exec }))
                .unwrap();
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("out");
            let wall = std::time::Instant::now();
            let spilled = crate::text_partition::write_text_partitioned(
                &frame,
                &target,
                "\n",
                &[String::from("k")],
                "UTC",
            )
            .await
            .unwrap();
            let elapsed = wall.elapsed().as_secs_f64();
            eprintln!(
                "fat tail pool {} MiB: {elapsed:.3}s, peak {} MiB, spill {spilled}",
                pool / 1024 / 1024,
                peak_rss_mib()
            );
            assert!(spilled > 0, "the 2 GiB tail must spill under a pool");
            assert_eq!(leaf_stats(&target), (keys, keys, rows));
            for key in [0, 257, 999] {
                let body = std::fs::read_to_string(
                    target.join(format!("k=k{key}")).join("part-00000.txt"),
                )
                .unwrap();
                assert_eq!(body.lines().count(), rows / keys);
            }
        }
    }

    #[tokio::test]
    async fn text_partition_fallback_splits_pool_busting_batches() {
        let session = crate::ReparkSession::builder()
            .memory_limit_bytes(128 * 1024 * 1024)
            .batch_size(1024)
            .build()
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let rows = 48_000usize;
        let keys = 300usize;
        let batch = fat_batch(&schema, 0, rows, keys);
        let table = datafusion::datasource::memory::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch]],
        )
        .unwrap();
        let frame = session.context().read_table(Arc::new(table)).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        let spilled = crate::text_partition::write_text_partitioned(
            &frame,
            &target,
            "\n",
            &[String::from("k")],
            "UTC",
        )
        .await
        .unwrap();
        assert!(spilled > 0, "the 192 MiB tail must spill under a pool");
        assert_eq!(leaf_stats(&target), (keys, keys, rows));
    }

    #[tokio::test]
    async fn tail_rechunk_splits_only_oversized_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = fat_batch(&schema, 0, 5000, 10);
        eprintln!(
            "whole rows={} arrow_bytes={} df_bytes={}",
            batch.num_rows(),
            batch.get_array_memory_size(),
            get_record_batch_memory_size(&batch)
        );
        let head = batch.slice(100, 4900);
        eprintln!(
            "head rows={} arrow_bytes={} df_bytes={}",
            head.num_rows(),
            head.get_array_memory_size(),
            get_record_batch_memory_size(&head)
        );
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(head)]),
        ));
        let rechunked = TailRechunk {
            inner: stream,
            schema: Arc::clone(&schema),
            max_bytes: 6 * 1024 * 1024,
            ready: VecDeque::new(),
        };
        let adapted: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, rechunked));
        let out: Vec<RecordBatch> = adapted
            .collect::<Vec<datafusion::error::Result<RecordBatch>>>()
            .await
            .into_iter()
            .collect::<datafusion::error::Result<Vec<_>>>()
            .unwrap();
        let mut rows = 0;
        for chunk in &out {
            eprintln!(
                "chunk rows={} arrow_bytes={} df_bytes={}",
                chunk.num_rows(),
                chunk.get_array_memory_size(),
                get_record_batch_memory_size(chunk)
            );
            assert!(get_record_batch_memory_size(chunk) <= 6 * 1024 * 1024);
            rows += chunk.num_rows();
        }
        assert_eq!(rows, 4900);
        assert!(out.len() > 1);
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
