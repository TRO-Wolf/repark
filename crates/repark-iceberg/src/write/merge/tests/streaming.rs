use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

use super::super::*;

/// A one-column `Int32` batch (`id`) — a trivial payload; the interleaving pin's writer discards
fn id_batch(values: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("id batch builds")
}

/// A [`BatchWriter`] that only COUNTS the batches written into it (no IO) — the sink half of the
struct CountingWriter {
    written: Arc<AtomicUsize>,
}

impl BatchWriter for CountingWriter {
    async fn write_batch(&mut self, _batch: RecordBatch) -> Result<()> {
        self.written.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<DataFile>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn write_stream_into_interleaves_writes_with_source_production() {
    // P2 — the DETERMINISTIC interleaving pin.
    let written = Arc::new(AtomicUsize::new(0));
    let observed_written = Arc::new(Mutex::new(Vec::<usize>::new()));

    let items: Vec<Result<RecordBatch>> =
        vec![Ok(id_batch(&[1])), Ok(id_batch(&[2])), Ok(id_batch(&[3]))];
    let observer = Arc::clone(&observed_written);
    let write_probe = Arc::clone(&written);
    let source = futures::stream::iter(items).inspect(move |_batch| {
        observer
            .lock()
            .expect("observation log lock")
            .push(write_probe.load(Ordering::SeqCst));
    });

    let sink = CountingWriter {
        written: Arc::clone(&written),
    };
    let files = write_stream_into(sink, source)
        .await
        .expect("streaming drive succeeds");

    assert!(
        files.is_empty(),
        "the counting writer produces no data files"
    );
    assert_eq!(
        written.load(Ordering::SeqCst),
        3,
        "all three batches were written"
    );
    let observations = observed_written
        .lock()
        .expect("observation log lock")
        .clone();
    assert_eq!(
        observations,
        vec![0, 1, 2],
        "INTERLEAVING: batch k is written before batch k+1 is produced — writing begins before \
         the source is exhausted (a collect-then-write driver would observe [0, 0, 0])"
    );
}

/// A [`BatchWriter`] that sleeps `latency` on every `finish` — models per-file close/upload
struct LatencyFinishWriter {
    latency: std::time::Duration,
    files_to_emit: usize,
}

impl BatchWriter for LatencyFinishWriter {
    async fn write_batch(&mut self, _batch: RecordBatch) -> Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<DataFile>> {
        tokio::time::sleep(self.latency).await;
        // One synthetic "file" marker per finish (paths are unused by the pin).
        Ok(Vec::with_capacity(self.files_to_emit))
    }
}

#[tokio::test]
async fn parallel_finish_is_faster_than_serial_under_injected_latency() {
    // R-PERF-MERGE-S3 proof pin: N finish operations with injected latency L.
    const N: usize = 4;
    const LATENCY_MS: u64 = 80;
    let latency = std::time::Duration::from_millis(LATENCY_MS);

    // Serial: N sequential finish calls on one "logical" path (N writers run one after another).
    let serial_start = std::time::Instant::now();
    for _ in 0..N {
        let mut sink = LatencyFinishWriter {
            latency,
            files_to_emit: 1,
        };
        let _ = sink.finish().await.expect("serial finish");
    }
    let serial_wall = serial_start.elapsed();

    // Parallel: N independent writers, each finishing once (empty stream after open).
    let parallel_start = std::time::Instant::now();
    let worker_futures: Vec<_> = (0..N)
        .map(|_| async move {
            let mut sink = LatencyFinishWriter {
                latency,
                files_to_emit: 1,
            };
            // One empty write path — finish still sleeps L.
            sink.finish().await
        })
        .collect();
    let results = futures::future::join_all(worker_futures).await;
    for result in results {
        result.expect("parallel finish");
    }
    let parallel_wall = parallel_start.elapsed();

    assert!(
        parallel_wall.as_secs_f64() < 0.5 * serial_wall.as_secs_f64(),
        "parallel finish wall {parallel_wall:?} must be < 0.5 × serial {serial_wall:?} \
         (injected {LATENCY_MS}ms × {N} files)"
    );
    // Sanity: serial should be roughly N×L (allow wide CI margin).
    assert!(
        serial_wall >= std::time::Duration::from_millis(LATENCY_MS * (N as u64) * 3 / 4),
        "serial wall {serial_wall:?} should be near {N}×{LATENCY_MS}ms"
    );
}

/// Counts writes and finishes — structural pin for the parallel dispatcher.
struct ProbeWriter {
    writes: Arc<AtomicUsize>,
    finishes: Arc<AtomicUsize>,
}

impl BatchWriter for ProbeWriter {
    async fn write_batch(&mut self, _batch: RecordBatch) -> Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<DataFile>> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn write_stream_into_parallel_round_robin_finishes_all_workers() {
    // Structural pin: K=4 workers + 8 batches → every worker receives ≥1 batch and finishes.
    const K: usize = 4;
    const BATCHES: usize = 8;
    let finishes = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let sinks: Vec<ProbeWriter> = (0..K)
        .map(|_| ProbeWriter {
            writes: Arc::clone(&writes),
            finishes: Arc::clone(&finishes),
        })
        .collect();
    let items: Vec<Result<RecordBatch>> = (0..BATCHES)
        .map(|index| {
            Ok(id_batch(&[
                i32::try_from(index).expect("batch index fits i32")
            ]))
        })
        .collect();
    let files = write_stream_into_parallel_sinks(K, futures::stream::iter(items), sinks)
        .await
        .expect("parallel drive succeeds");
    assert!(files.is_empty());
    assert_eq!(writes.load(Ordering::SeqCst), BATCHES);
    assert_eq!(finishes.load(Ordering::SeqCst), K);
}

/// P1-R1: a mid-stream source error aborts WITHOUT finishing any sink (no orphaned uploads).
#[tokio::test]
async fn parallel_source_error_does_not_finish_sinks() {
    const K: usize = 4;
    let finishes = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let sinks: Vec<ProbeWriter> = (0..K)
        .map(|_| ProbeWriter {
            writes: Arc::clone(&writes),
            finishes: Arc::clone(&finishes),
        })
        .collect();
    let items: Vec<Result<RecordBatch>> = vec![
        Ok(id_batch(&[1])),
        Ok(id_batch(&[2])),
        Err(DataFusionError::Execution("source boom".into())),
        Ok(id_batch(&[3])),
    ];
    let error = write_stream_into_parallel_sinks(K, futures::stream::iter(items), sinks)
        .await
        .expect_err("source error must surface");
    assert!(
        error.to_string().contains("source boom"),
        "surfaced error must be the source root cause, got: {error}"
    );
    assert_eq!(
        finishes.load(Ordering::SeqCst),
        0,
        "no sink may finish() after a source abort (P1-R1 orphan-upload guard)"
    );
}

/// A sink that fails on its first write — used to pin worker-error preference over the
struct FailingWriter {
    finishes: Arc<AtomicUsize>,
}

impl BatchWriter for FailingWriter {
    async fn write_batch(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(DataFusionError::Execution("worker write failed".into()))
    }

    async fn finish(&mut self) -> Result<Vec<DataFile>> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }
}

/// P1-R1: when a worker dies first, the surfaced error is the worker's root cause — not the
#[tokio::test]
async fn parallel_worker_error_preferred_over_channel_closed() {
    const K: usize = 2;
    let finishes = Arc::new(AtomicUsize::new(0));
    let sinks: Vec<FailingWriter> = (0..K)
        .map(|_| FailingWriter {
            finishes: Arc::clone(&finishes),
        })
        .collect();
    // Enough batches that the dispatcher will keep sending after the first worker fails.
    let items: Vec<Result<RecordBatch>> = (0..8).map(|index| Ok(id_batch(&[index]))).collect();
    let error = write_stream_into_parallel_sinks(K, futures::stream::iter(items), sinks)
        .await
        .expect_err("worker write failure must surface");
    let message = error.to_string();
    assert!(
        message.contains("worker write failed"),
        "must prefer worker root cause over channel-closed, got: {message}"
    );
    assert!(
        !message.contains("channel closed before the source"),
        "must not mask worker error with dispatcher secondary message, got: {message}"
    );
    assert_eq!(
        finishes.load(Ordering::SeqCst),
        0,
        "no sink may finish() after a worker write failure"
    );
}
