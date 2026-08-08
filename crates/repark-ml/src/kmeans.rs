//! Lloyd k-means over streamed batches (initMode=random only).
//!
//! Default Spark `initMode` is k-means|| — we **do not** fake it. Callers must set
//! `initMode="random"` or fit fails with [`MlError::KMeansInitModeDefault`].
//! Assignment parity vs Spark is up to label permutation when seeds and random init align.

use crate::MAX_FEATURES;
use crate::error::{MlError, Result};

/// ===========================================================================================
/// Validate `KMeans` `initMode` before any streaming work.
///
/// # Errors
/// [`MlError::KMeansInitModeDefault`] or [`MlError::KMeansInitModeUnsupported`].
/// ===========================================================================================
pub fn validate_init_mode(init_mode: &str) -> Result<()> {
    match init_mode {
        // Spark default — refuse so we never silently substitute random.
        "k-means||" | "kmeans||" | "" => Err(MlError::KMeansInitModeDefault),
        "random" => Ok(()),
        other => Err(MlError::KMeansInitModeUnsupported {
            got: other.to_string(),
        }),
    }
}

/// ===========================================================================================
/// Fitted centers (params only).
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq)]
pub struct KMeansSolution {
    /// Cluster centers, each of length `num_features`.
    pub centers: Vec<Vec<f64>>,
    pub num_features: usize,
    pub k: usize,
    pub num_rows: u64,
    pub iterations: usize,
}

/// Small deterministic PRNG (xorshift64*) — no external rand crate.
#[derive(Debug, Clone)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Seed must be non-zero (zero is remapped).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    /// Next u64.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform integer in `0..upper` (upper > 0).
    pub fn next_index(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        // u64 → usize: k / batch sizes are tiny vs 32-bit usize; explicit cast is fine.
        #[allow(clippy::cast_possible_truncation)]
        let bits = self.next_u64() as usize;
        bits % upper
    }
}

/// Cap on rejection-sampling trials per requested center (SAF-004). When `k ≈ n` the
/// random-distinct loop can thrash; after this budget we fall back to sequential fill.
const KMEANS_INIT_MAX_ATTEMPTS_PER_CENTER: u64 = 64;

/// ===========================================================================================
/// Pick `k` distinct row indices as initial centers (needs `num_rows` from a count pass).
///
/// Uses rejection sampling with a hard attempt budget; if the budget is exhausted before `k`
/// distinct indices are found (the `k ≈ n` hang class, audit SAF-004), fills the remainder with
/// sequential unused indices `0, 1, 2, …` so fit always terminates.
///
/// # Errors
/// Empty design, `k == 0`, or `k > num_rows`.
/// ===========================================================================================
pub fn random_center_indices(num_rows: u64, k: usize, seed: u64) -> Result<Vec<u64>> {
    if k == 0 {
        return Err(MlError::IllegalArgument("KMeans k must be ≥ 1".into()));
    }
    if num_rows == 0 {
        return Err(MlError::EmptyDesign("KMeans: zero training rows".into()));
    }
    if (k as u64) > num_rows {
        return Err(MlError::IllegalArgument(format!(
            "KMeans k={k} > num_rows={num_rows}"
        )));
    }
    let mut rng = XorShift64::new(seed);
    let mut chosen = Vec::with_capacity(k);
    // Budget scales with k so small-k stays random-first; large-k cannot hang.
    let max_attempts = (k as u64)
        .saturating_mul(KMEANS_INIT_MAX_ATTEMPTS_PER_CENTER)
        .max(1);
    let mut attempts = 0_u64;
    while chosen.len() < k && attempts < max_attempts {
        attempts += 1;
        let candidate = rng.next_u64() % num_rows;
        if !chosen.contains(&candidate) {
            chosen.push(candidate);
        }
    }
    if chosen.len() < k {
        // SAF-004 fallback: sequential distinct fill of remaining slots (deterministic).
        fill_sequential_distinct_indices(num_rows, k, &mut chosen);
    }
    chosen.sort_unstable();
    Ok(chosen)
}

/// Fill `chosen` up to `k` with the lowest unused indices in `0..num_rows` (SAF-004 fallback).
fn fill_sequential_distinct_indices(num_rows: u64, k: usize, chosen: &mut Vec<u64>) {
    let mut index = 0_u64;
    while chosen.len() < k && index < num_rows {
        if !chosen.contains(&index) {
            chosen.push(index);
        }
        index += 1;
    }
}

/// Squared Euclidean distance. Prefers equal lengths; truncating zip is refused.
///
/// # Errors
/// [`MlError::FeatureWidthMismatch`] when `a.len() != b.len()`.
pub fn squared_distance(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(MlError::FeatureWidthMismatch {
            expected: a.len(),
            actual: b.len(),
        });
    }
    let mut sum = 0.0_f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = x - y;
        sum += d * d;
    }
    Ok(sum)
}

/// Nearest center index (ties → lowest index).
///
/// # Errors
/// Empty `centers`, or a center width mismatch vs `point`.
pub fn nearest_center(point: &[f64], centers: &[Vec<f64>]) -> Result<usize> {
    if centers.is_empty() {
        return Err(MlError::IllegalArgument(
            "nearest_center: centers must be non-empty".into(),
        ));
    }
    let mut best_index = 0;
    let mut best_dist = f64::INFINITY;
    for (index, center) in centers.iter().enumerate() {
        let dist = squared_distance(point, center)?;
        if dist < best_dist {
            best_dist = dist;
            best_index = index;
        }
    }
    Ok(best_index)
}

/// ===========================================================================================
/// One Lloyd assignment+update pass over a stream of dense points.
///
/// Accumulates sum and count per cluster; does not store points.
/// ===========================================================================================
#[derive(Debug, Clone)]
pub struct KMeansPass {
    num_features: usize,
    /// Cluster count (kept for invariants / future diagnostics).
    #[allow(dead_code)]
    k: usize,
    centers: Vec<Vec<f64>>,
    sums: Vec<Vec<f64>>,
    counts: Vec<u64>,
    num_rows: u64,
    row_offset: u64,
}

impl KMeansPass {
    /// Start a pass from current centers.
    ///
    /// # Errors
    /// Empty centers, width mismatch, or non-finite coordinates.
    pub fn new(centers: Vec<Vec<f64>>) -> Result<Self> {
        if centers.is_empty() {
            return Err(MlError::IllegalArgument(
                "KMeans centers must be non-empty".into(),
            ));
        }
        let num_features = centers[0].len();
        if num_features > MAX_FEATURES {
            return Err(MlError::FeatureDimTooLarge {
                actual: num_features,
                limit: MAX_FEATURES,
            });
        }
        if num_features == 0 {
            return Err(MlError::EmptyDesign("KMeans feature dimension is 0".into()));
        }
        for (index, center) in centers.iter().enumerate() {
            if center.len() != num_features {
                return Err(MlError::FeatureWidthMismatch {
                    expected: num_features,
                    actual: center.len(),
                });
            }
            for &value in center {
                if !value.is_finite() {
                    return Err(MlError::IllegalArgument(format!(
                        "KMeans center {index} has non-finite coordinate"
                    )));
                }
            }
        }
        let k = centers.len();
        Ok(Self {
            num_features,
            k,
            centers,
            sums: vec![vec![0.0; num_features]; k],
            counts: vec![0; k],
            num_rows: 0,
            row_offset: 0,
        })
    }

    /// Assign one point and accumulate.
    ///
    /// # Errors
    /// Width mismatch or non-finite feature values.
    pub fn observe_row(&mut self, features: &[f64]) -> Result<()> {
        if features.len() != self.num_features {
            return Err(MlError::FeatureWidthMismatch {
                expected: self.num_features,
                actual: features.len(),
            });
        }
        for &value in features {
            if !value.is_finite() {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset: self.row_offset,
                });
            }
        }
        let assignment = nearest_center(features, &self.centers)?;
        for (dim, &value) in features.iter().enumerate() {
            self.sums[assignment][dim] += value;
        }
        self.counts[assignment] += 1;
        self.num_rows += 1;
        self.row_offset += 1;
        Ok(())
    }

    /// Finish pass → new centers. Empty clusters keep previous center.
    ///
    /// # Errors
    /// Empty stream (zero rows).
    pub fn finish(self) -> Result<(Vec<Vec<f64>>, u64, bool)> {
        if self.num_rows == 0 {
            return Err(MlError::EmptyDesign(
                "KMeans pass saw zero training rows".into(),
            ));
        }
        let mut new_centers = self.centers.clone();
        let mut moved = false;
        for (cluster, (center, (sum, count))) in new_centers
            .iter_mut()
            .zip(self.sums.iter().zip(self.counts.iter()))
            .enumerate()
        {
            let _ = cluster;
            if *count == 0 {
                continue;
            }
            // Counts are tiny cluster sizes in practice; f64 mantissa is enough.
            #[allow(clippy::cast_precision_loss)]
            let inv = 1.0 / (*count as f64);
            for (dim_value, sum_value) in center.iter_mut().zip(sum.iter()) {
                let value = sum_value * inv;
                if (value - *dim_value).abs() > 1e-12 {
                    moved = true;
                }
                *dim_value = value;
            }
        }
        Ok((new_centers, self.num_rows, moved))
    }
}

/// ===========================================================================================
/// Run Lloyd with a caller-provided stream closure (re-executed each iteration).
///
/// # Errors
/// Propagates stream / assignment failures from each pass.
/// ===========================================================================================
pub fn fit_kmeans_lloyd<F>(
    initial_centers: Vec<Vec<f64>>,
    max_iter: usize,
    mut stream_pass: F,
) -> Result<KMeansSolution>
where
    F: FnMut(&mut KMeansPass) -> Result<()>,
{
    let num_features = initial_centers.first().map_or(0, Vec::len);
    let k = initial_centers.len();
    // Validate initial centers even when max_iter == 0 (init-only path).
    let _ = KMeansPass::new(initial_centers.clone())?;
    let mut centers = initial_centers;
    let mut iterations = 0;
    let mut num_rows = 0_u64;

    // max_iter == 0 → return init centers only (Spark: no Lloyd steps). No silent clamp.
    for _ in 0..max_iter {
        let mut pass = KMeansPass::new(centers.clone())?;
        stream_pass(&mut pass)?;
        let (new_centers, rows, moved) = pass.finish()?;
        num_rows = rows;
        centers = new_centers;
        iterations += 1;
        if !moved {
            break;
        }
    }

    Ok(KMeansSolution {
        centers,
        num_features,
        k,
        num_rows,
        iterations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_init_mode_errors() {
        assert!(matches!(
            validate_init_mode("k-means||"),
            Err(MlError::KMeansInitModeDefault)
        ));
        assert!(matches!(
            validate_init_mode(""),
            Err(MlError::KMeansInitModeDefault)
        ));
        assert!(validate_init_mode("random").is_ok());
    }

    #[test]
    fn two_blob_lloyd() {
        // Two clusters around (0,0) and (10,10)
        let points: Vec<[f64; 2]> = vec![
            [0.0, 0.0],
            [0.1, -0.1],
            [-0.1, 0.05],
            [10.0, 10.0],
            [10.1, 9.9],
            [9.9, 10.05],
        ];
        let initial = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let sol = fit_kmeans_lloyd(initial, 20, |pass| {
            for point in &points {
                pass.observe_row(point)?;
            }
            Ok(())
        })
        .expect("lloyd");
        assert_eq!(sol.k, 2);
        // Centers should sit near the two blobs (order preserved from init).
        assert!(sol.centers[0][0].abs() < 0.2);
        assert!((sol.centers[1][0] - 10.0).abs() < 0.2);
    }

    #[test]
    fn random_indices_distinct() {
        let indices = random_center_indices(100, 5, 42).unwrap();
        assert_eq!(indices.len(), 5);
        let mut sorted = indices.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 5);
    }

    #[test]
    fn k_zero_and_k_gt_n_loud() {
        assert!(matches!(
            random_center_indices(10, 0, 1),
            Err(MlError::IllegalArgument(_))
        ));
        assert!(matches!(
            random_center_indices(3, 5, 1),
            Err(MlError::IllegalArgument(_))
        ));
        assert!(matches!(
            random_center_indices(0, 1, 1),
            Err(MlError::EmptyDesign(_))
        ));
    }

    /// SAF-004: `k == n` must terminate (no unbounded rejection sampling hang) with all
    /// distinct indices covering `0..n`.
    #[test]
    fn random_center_indices_k_equals_n_terminates_with_full_cover() {
        for seed in [0_u64, 1, 42, 0xDEAD_BEEF] {
            let indices = random_center_indices(8, 8, seed).expect("k=n must succeed");
            assert_eq!(indices.len(), 8, "seed={seed}");
            let mut sorted = indices.clone();
            sorted.sort_unstable();
            assert_eq!(
                sorted,
                (0..8).collect::<Vec<_>>(),
                "k=n must cover every index, seed={seed}"
            );
        }
        // Larger near-full k also terminates.
        let indices = random_center_indices(100, 100, 7).expect("k=n=100");
        assert_eq!(indices.len(), 100);
        let mut sorted = indices;
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 100);
    }

    #[test]
    fn max_iter_zero_returns_init_centers() {
        let initial = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let sol = fit_kmeans_lloyd(initial.clone(), 0, |_pass| {
            panic!("stream must not run when max_iter=0");
        })
        .expect("init-only");
        assert_eq!(sol.iterations, 0);
        assert_eq!(sol.centers, initial);
        assert_eq!(sol.num_rows, 0);
    }

    #[test]
    fn squared_distance_width_mismatch_loud() {
        let err = squared_distance(&[1.0, 2.0], &[1.0]).unwrap_err();
        assert!(matches!(err, MlError::FeatureWidthMismatch { .. }));
    }

    #[test]
    fn nearest_center_empty_loud() {
        let err = nearest_center(&[1.0], &[]).unwrap_err();
        assert!(matches!(err, MlError::IllegalArgument(_)));
    }

    #[test]
    fn non_finite_feature_loud() {
        let mut pass = KMeansPass::new(vec![vec![0.0], vec![1.0]]).unwrap();
        let err = pass.observe_row(&[f64::INFINITY]).unwrap_err();
        assert!(matches!(
            err,
            MlError::NonFinite {
                what: "feature",
                ..
            }
        ));
    }
}
