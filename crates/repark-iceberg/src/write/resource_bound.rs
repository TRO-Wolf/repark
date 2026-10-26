use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryLimit;
use datafusion::prelude::SessionConfig;
use futures::channel::oneshot;
use futures::future::{FutureExt, Shared};
use futures::{Stream, TryStreamExt};

pub const ORDERED_INPUT_FRACTION_KEY: &str = "repark.write.ordered.max-input-fraction";
pub const MOR_UPDATED_FRACTION_KEY: &str = "repark.merge.mor.max-updated-fraction";
pub const CHANGELOG_SOURCE_FRACTION_KEY: &str = "repark.changelog.max-source-fraction";
pub const RESOURCE_BOUND_KEYS: [&str; 3] = [
    ORDERED_INPUT_FRACTION_KEY,
    MOR_UPDATED_FRACTION_KEY,
    CHANGELOG_SOURCE_FRACTION_KEY,
];

const PARTS_PER_MILLION: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceBounds {
    pub ordered_input_ppm: u64,
    pub mor_updated_ppm: u64,
    pub changelog_source_ppm: u64,
    pub pool_bytes: Option<usize>,
}

impl Default for ResourceBounds {
    fn default() -> Self {
        Self {
            ordered_input_ppm: 100_000,
            mor_updated_ppm: 50_000,
            changelog_source_ppm: 62_500,
            pool_bytes: None,
        }
    }
}

impl ResourceBounds {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_config_map<S>(config: &std::collections::HashMap<String, String, S>) -> Result<Self>
    where
        S: std::hash::BuildHasher,
    {
        let defaults = Self::default();
        let read = |key: &str, default: u64| match config.get(key) {
            None => Ok(default),
            Some(raw) => parse_fraction_ppm(key, raw),
        };
        Ok(Self {
            ordered_input_ppm: read(ORDERED_INPUT_FRACTION_KEY, defaults.ordered_input_ppm)?,
            mor_updated_ppm: read(MOR_UPDATED_FRACTION_KEY, defaults.mor_updated_ppm)?,
            changelog_source_ppm: read(
                CHANGELOG_SOURCE_FRACTION_KEY,
                defaults.changelog_source_ppm,
            )?,
            pool_bytes: None,
        })
    }

    #[must_use]
    pub fn from_session_config(config: &SessionConfig) -> Self {
        config
            .get_extension::<Self>()
            .map(|bounds| *bounds)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn at_pool(self, limit: &MemoryLimit) -> Self {
        let pool_bytes = match limit {
            MemoryLimit::Finite(bytes) => Some(*bytes),
            MemoryLimit::Infinite | MemoryLimit::Unknown => None,
        };
        Self { pool_bytes, ..self }
    }

    #[must_use]
    pub fn ordered_input(&self) -> Option<ByteBound> {
        ByteBound::resolve(
            BoundPath::OrderedWrite,
            self.ordered_input_ppm,
            self.pool_bytes,
        )
    }

    #[must_use]
    pub fn mor_updated(&self) -> Option<ByteBound> {
        ByteBound::resolve(
            BoundPath::MergeOnRead,
            self.mor_updated_ppm,
            self.pool_bytes,
        )
    }

    #[must_use]
    pub fn changelog_source(&self) -> Option<ByteBound> {
        ByteBound::resolve(
            BoundPath::Changelog,
            self.changelog_source_ppm,
            self.pool_bytes,
        )
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn parse_fraction_ppm(key: &str, raw: &str) -> Result<u64> {
    let refusal = || {
        DataFusionError::Plan(format!(
            "config `{key}` must be a finite number >= 0 (0 = no bound), got {raw:?}"
        ))
    };
    let value: f64 = raw.trim().parse().map_err(|_| refusal())?;
    if !value.is_finite() || value < 0.0 {
        return Err(refusal());
    }
    let ppm = (value * PARTS_PER_MILLION as f64).round() as u64;
    Ok(if value > 0.0 { ppm.max(1) } else { 0 })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundPath {
    OrderedWrite,
    MergeOnRead,
    Changelog,
}

impl BoundPath {
    fn kind(self) -> &'static str {
        match self {
            Self::OrderedWrite => "ordered write",
            Self::MergeOnRead => "merge-on-read MERGE",
            Self::Changelog => "changelog read",
        }
    }

    fn held(self) -> &'static str {
        match self {
            Self::OrderedWrite => "its input before the sort",
            Self::MergeOnRead => "its updated rows",
            Self::Changelog => "the changelog window",
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::OrderedWrite => ORDERED_INPUT_FRACTION_KEY,
            Self::MergeOnRead => MOR_UPDATED_FRACTION_KEY,
            Self::Changelog => CHANGELOG_SOURCE_FRACTION_KEY,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteBound {
    path: BoundPath,
    fraction_ppm: u64,
    pool_bytes: usize,
    limit_bytes: usize,
}

impl ByteBound {
    fn resolve(path: BoundPath, fraction_ppm: u64, pool_bytes: Option<usize>) -> Option<Self> {
        let pool_bytes = pool_bytes?;
        if fraction_ppm == 0 {
            return None;
        }
        let limit = u128::from(fraction_ppm) * pool_bytes as u128 / u128::from(PARTS_PER_MILLION);
        Some(Self {
            path,
            fraction_ppm,
            pool_bytes,
            limit_bytes: usize::try_from(limit).unwrap_or(usize::MAX),
        })
    }

    #[must_use]
    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn check(&self, held_bytes: usize) -> Result<()> {
        if held_bytes <= self.limit_bytes {
            return Ok(());
        }
        let held = self.path.held();
        Err(DataFusionError::ResourcesExhausted(format!(
            "REPARK: the {kind} holds {held} in memory outside the memory pool; {held} reached \
             at least {held_bytes} bytes, above the bound {fraction} x pool {pool} bytes = \
             {limit} bytes. Raise `{key}` (0 = no bound) or raise the memory pool.",
            kind = self.path.kind(),
            fraction = format_fraction(self.fraction_ppm),
            pool = self.pool_bytes,
            limit = self.limit_bytes,
            key = self.path.key(),
        )))
    }
}

fn format_fraction(fraction_ppm: u64) -> String {
    let whole = fraction_ppm / PARTS_PER_MILLION;
    let part = fraction_ppm % PARTS_PER_MILLION;
    if part == 0 {
        return whole.to_string();
    }
    let digits = format!("{part:06}");
    format!("{whole}.{}", digits.trim_end_matches('0'))
}

#[must_use]
pub fn counted_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| {
            column
                .to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| column.get_array_memory_size())
        })
        .sum()
}

struct DrainBarrier {
    remaining: AtomicUsize,
    release: Mutex<Option<oneshot::Sender<()>>>,
    released: Shared<oneshot::Receiver<()>>,
}

impl DrainBarrier {
    fn new(workers: usize) -> Self {
        let (release, released) = oneshot::channel();
        Self {
            remaining: AtomicUsize::new(workers),
            release: Mutex::new(Some(release)),
            released: released.shared(),
        }
    }

    async fn arrive(&self) {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.release
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
        }
        let _ = self.released.clone().await;
    }
}

pub(crate) struct OrderedInputBudget {
    bound: Option<ByteBound>,
    held: AtomicUsize,
    aborted: Arc<AtomicBool>,
    barrier: Option<DrainBarrier>,
}

impl OrderedInputBudget {
    pub(crate) fn serial(bounds: &ResourceBounds) -> Self {
        Self {
            bound: bounds.ordered_input(),
            held: AtomicUsize::new(0),
            aborted: Arc::new(AtomicBool::new(false)),
            barrier: None,
        }
    }

    pub(crate) fn parallel(bounds: &ResourceBounds, workers: usize) -> Arc<Self> {
        let bound = bounds.ordered_input();
        Arc::new(Self {
            bound,
            held: AtomicUsize::new(0),
            aborted: Arc::new(AtomicBool::new(false)),
            barrier: bound.map(|_| DrainBarrier::new(workers)),
        })
    }

    pub(crate) fn aborted(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.aborted)
    }

    pub(crate) fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    pub(crate) async fn arrive(&self) {
        if let Some(barrier) = &self.barrier {
            barrier.arrive().await;
        }
    }

    fn charge(&self, batch: &RecordBatch) -> Result<()> {
        let Some(bound) = &self.bound else {
            return Ok(());
        };
        let bytes = counted_bytes(batch);
        let held = self.held.fetch_add(bytes, Ordering::SeqCst) + bytes;
        bound.check(held).inspect_err(|_| {
            self.aborted.store(true, Ordering::SeqCst);
        })
    }
}

pub(crate) async fn drain_ordered_input<S>(
    stream: &mut S,
    budget: &OrderedInputBudget,
) -> Result<Vec<RecordBatch>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let mut collected = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        budget.charge(&batch)?;
        collected.push(batch);
    }
    Ok(collected)
}

pub(crate) struct UpdatedRowsBudget {
    bound: Option<ByteBound>,
    held: usize,
}

impl UpdatedRowsBudget {
    pub(crate) fn new(bounds: &ResourceBounds) -> Self {
        Self {
            bound: bounds.mor_updated(),
            held: 0,
        }
    }

    pub(crate) fn charge(&mut self, batches: &[RecordBatch]) -> Result<()> {
        let Some(bound) = &self.bound else {
            return Ok(());
        };
        self.held += batches.iter().map(counted_bytes).sum::<usize>();
        bound.check(self.held)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn batch(rows: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("s", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..rows)
                        .map(|row| format!("s{row:07}"))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn defaults_are_the_ruled_fractions() {
        let bounds = ResourceBounds::default();
        assert_eq!(bounds.ordered_input_ppm, 100_000);
        assert_eq!(bounds.mor_updated_ppm, 50_000);
        assert_eq!(bounds.changelog_source_ppm, 62_500);
        let pooled = bounds.at_pool(&MemoryLimit::Finite(8 * 1024 * 1024 * 1024));
        assert_eq!(pooled.ordered_input().unwrap().limit_bytes(), 858_993_459);
        assert_eq!(pooled.mor_updated().unwrap().limit_bytes(), 429_496_729);
        assert_eq!(
            pooled.changelog_source().unwrap().limit_bytes(),
            536_870_912
        );
    }

    #[test]
    fn zero_and_an_unbounded_pool_mean_no_bound() {
        let mut map = std::collections::HashMap::new();
        map.insert(ORDERED_INPUT_FRACTION_KEY.to_string(), "0".to_string());
        let bounds = ResourceBounds::from_config_map(&map).unwrap();
        assert!(
            bounds
                .at_pool(&MemoryLimit::Finite(1024))
                .ordered_input()
                .is_none()
        );
        assert!(
            ResourceBounds::default()
                .at_pool(&MemoryLimit::Infinite)
                .mor_updated()
                .is_none()
        );
        assert!(ResourceBounds::default().ordered_input().is_none());
    }

    #[test]
    fn invalid_fractions_refuse_naming_the_key() {
        for raw in ["-0.1", "abc", "inf", "NaN", ""] {
            let mut map = std::collections::HashMap::new();
            map.insert(MOR_UPDATED_FRACTION_KEY.to_string(), raw.to_string());
            let error = ResourceBounds::from_config_map(&map).unwrap_err();
            assert!(
                error.to_string().contains(MOR_UPDATED_FRACTION_KEY),
                "{raw}"
            );
        }
        let mut map = std::collections::HashMap::new();
        map.insert(CHANGELOG_SOURCE_FRACTION_KEY.to_string(), "2.5".to_string());
        assert_eq!(
            ResourceBounds::from_config_map(&map)
                .unwrap()
                .changelog_source_ppm,
            2_500_000
        );
    }

    #[test]
    fn the_refusal_names_the_path_the_bound_and_the_key() {
        let bound = ResourceBounds::default()
            .at_pool(&MemoryLimit::Finite(1_000_000))
            .ordered_input()
            .unwrap();
        assert!(bound.check(100_000).is_ok());
        let error = bound.check(100_001).unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(
            error.to_string(),
            "Resources exhausted: REPARK: the ordered write holds its input before the sort in \
             memory outside the memory pool; its input before the sort reached at least 100001 \
             bytes, above the bound 0.1 x pool 1000000 bytes = 100000 bytes. Raise \
             `repark.write.ordered.max-input-fraction` (0 = no bound) or raise the memory pool."
        );
    }

    #[test]
    fn counted_bytes_are_slice_aware() {
        let whole = batch(10_000);
        let slice = whole.slice(0, 100);
        assert!(counted_bytes(&slice) * 50 < counted_bytes(&whole));
        assert!(counted_bytes(&slice) < slice.get_array_memory_size());
    }

    #[tokio::test]
    async fn a_refusal_sets_the_aborted_flag_and_the_barrier_releases_every_worker() {
        let bounds = ResourceBounds::default().at_pool(&MemoryLimit::Finite(1_000));
        let budget = OrderedInputBudget::parallel(&bounds, 2);
        let mut first = futures::stream::iter(vec![Ok(batch(1_000))]);
        let mut second = futures::stream::iter(Vec::<Result<RecordBatch>>::new());
        let (refused, empty) = futures::future::join(
            async {
                let drained = drain_ordered_input(&mut first, &budget).await;
                budget.arrive().await;
                drained
            },
            async {
                let drained = drain_ordered_input(&mut second, &budget).await;
                budget.arrive().await;
                drained
            },
        )
        .await;
        assert!(matches!(
            refused.unwrap_err(),
            DataFusionError::ResourcesExhausted(_)
        ));
        assert!(empty.unwrap().is_empty());
        assert!(budget.is_aborted());
    }
}
