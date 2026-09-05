use std::mem::size_of_val;

pub const DEFAULT_COMPRESS_THRESHOLD: usize = 10_000;
pub const DEFAULT_HEAD_SIZE: usize = 50_000;
pub const DEFAULT_ACCURACY: i64 = 10_000;

#[derive(Debug, Clone, Copy)]
pub struct SummaryStats {
    pub value: f64,
    pub rank_jump: i64,
    pub delta: i64,
}

#[derive(Debug, Clone)]
pub struct QuantileSummaries {
    compress_threshold: usize,
    relative_error: f64,
    sampled: Vec<SummaryStats>,
    count: i64,
    head: Vec<f64>,
    compressed: bool,
}

impl QuantileSummaries {
    #[must_use]
    pub fn new(relative_error: f64) -> Self {
        Self {
            compress_threshold: DEFAULT_COMPRESS_THRESHOLD,
            relative_error,
            sampled: Vec::new(),
            count: 0,
            head: Vec::new(),
            compressed: true,
        }
    }

    #[must_use]
    pub fn sampled_count(&self) -> usize {
        self.sampled.len()
    }

    #[must_use]
    pub fn buffered_count(&self) -> usize {
        self.head.len()
    }

    #[must_use]
    pub fn count(&self) -> i64 {
        self.count
    }

    #[must_use]
    pub fn is_compressed(&self) -> bool {
        self.compressed
    }

    #[must_use]
    pub fn relative_error(&self) -> f64 {
        self.relative_error
    }

    #[must_use]
    pub fn stats(&self) -> &[SummaryStats] {
        &self.sampled
    }

    #[must_use]
    pub fn size_bytes(&self) -> usize {
        size_of_val(self)
            + self.sampled.len()
                * size_of_val(&SummaryStats {
                    value: 0.0,
                    rank_jump: 0,
                    delta: 0,
                })
            + self.head.len() * size_of_val(&0.0_f64)
    }

    pub fn insert(&mut self, value: f64) {
        self.head.push(value);
        self.compressed = false;
        if self.head.len() >= DEFAULT_HEAD_SIZE {
            self.flush_head();
            if self.sampled.len() >= self.compress_threshold {
                let merged = compress_sampled(&self.sampled, self.merge_threshold(self.count));
                self.sampled = merged;
                self.compressed = true;
            }
        }
    }

    pub fn compress(&mut self) {
        self.flush_head();
        let merged = compress_sampled(&self.sampled, self.merge_threshold(self.count));
        self.sampled = merged;
        self.compressed = true;
    }

    #[allow(clippy::cast_precision_loss)]
    fn merge_threshold(&self, count: i64) -> f64 {
        let double_error = self.relative_error + self.relative_error;
        let slots = count as f64;
        double_error * slots
    }

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    fn insertion_delta(&self, count: i64) -> i64 {
        let double_error = self.relative_error + self.relative_error;
        let slots = count as f64;
        (double_error * slots).floor() as i64
    }

    fn flush_head(&mut self) {
        if self.head.is_empty() {
            return;
        }
        self.head.sort_by(f64::total_cmp);
        let mut merged = Vec::with_capacity(self.sampled.len() + self.head.len());
        let mut sample_index = 0;
        let mut sorted_index = 0;
        while sorted_index < self.head.len() {
            let current = self.head[sorted_index];
            while sample_index < self.sampled.len() && self.sampled[sample_index].value <= current {
                merged.push(self.sampled[sample_index]);
                sample_index += 1;
            }
            self.count += 1;
            let last_of_batch =
                sample_index == self.sampled.len() && sorted_index + 1 == self.head.len();
            let delta = if merged.is_empty() || last_of_batch {
                0
            } else {
                self.insertion_delta(self.count)
            };
            merged.push(SummaryStats {
                value: current,
                rank_jump: 1,
                delta,
            });
            sorted_index += 1;
        }
        while sample_index < self.sampled.len() {
            merged.push(self.sampled[sample_index]);
            sample_index += 1;
        }
        self.sampled = merged;
        self.head.clear();
    }

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    pub fn merge(&mut self, other: &Self) {
        self.flush_head();
        let mut staged = other.clone();
        staged.flush_head();
        if staged.count == 0 {
            return;
        }
        if self.count == 0 {
            self.sampled = staged.sampled;
            self.count = staged.count;
            self.relative_error = staged.relative_error;
            self.compress_threshold = staged.compress_threshold;
            self.compressed = staged.compressed;
            return;
        }
        let merged_error = self.relative_error.max(staged.relative_error);
        let merged_count = self.count.wrapping_add(staged.count);
        let double_other = staged.relative_error + staged.relative_error;
        let double_self = self.relative_error + self.relative_error;
        let staged_slots = staged.count as f64;
        let self_slots = self.count as f64;
        let additional_self = (double_other * staged_slots).floor() as i64;
        let additional_other = (double_self * self_slots).floor() as i64;
        let mut merged = Vec::with_capacity(self.sampled.len() + staged.sampled.len());
        let mut self_index = 0;
        let mut other_index = 0;
        while self_index < self.sampled.len() && other_index < staged.sampled.len() {
            let self_stats = self.sampled[self_index];
            let other_stats = staged.sampled[other_index];
            if self_stats.value < other_stats.value {
                let extra = if other_index > 0 { additional_self } else { 0 };
                merged.push(SummaryStats {
                    value: self_stats.value,
                    rank_jump: self_stats.rank_jump,
                    delta: self_stats.delta.wrapping_add(extra),
                });
                self_index += 1;
            } else {
                let extra = if self_index > 0 { additional_other } else { 0 };
                merged.push(SummaryStats {
                    value: other_stats.value,
                    rank_jump: other_stats.rank_jump,
                    delta: other_stats.delta.wrapping_add(extra),
                });
                other_index += 1;
            }
        }
        merged.extend_from_slice(&self.sampled[self_index..]);
        merged.extend_from_slice(&staged.sampled[other_index..]);
        let double_merged = merged_error + merged_error;
        let merged_slots = merged_count as f64;
        self.sampled = compress_sampled(&merged, double_merged * merged_slots);
        self.count = merged_count;
        self.relative_error = merged_error;
        self.compress_threshold = staged.compress_threshold;
        self.compressed = true;
    }

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    fn find_approx_quantile(
        &self,
        index: usize,
        min_rank: i64,
        target_error: f64,
        percentile: f64,
    ) -> (usize, i64, f64) {
        let slots = self.count as f64;
        let rank = (percentile * slots).ceil() as i64;
        let rank_float = rank as f64;
        let mut current = self.sampled[index];
        let mut position = index;
        let mut floor = min_rank;
        while position + 1 < self.sampled.len() {
            let ceiling = floor.wrapping_add(current.delta);
            let ceiling_float = ceiling as f64;
            let floor_float = floor as f64;
            if ceiling_float - target_error <= rank_float
                && rank_float <= floor_float + target_error
            {
                return (position, floor, current.value);
            }
            position += 1;
            current = self.sampled[position];
            floor = floor.wrapping_add(current.rank_jump);
        }
        (
            self.sampled.len() - 1,
            0,
            self.sampled[self.sampled.len() - 1].value,
        )
    }

    #[allow(clippy::cast_precision_loss)]
    #[must_use]
    pub fn query(&mut self, percentiles: &[f64]) -> Vec<f64> {
        if !self.compressed {
            self.compress();
        }
        if self.sampled.is_empty() || percentiles.is_empty() {
            return Vec::new();
        }
        let mut worst = i64::MIN;
        for stats in &self.sampled {
            worst = worst.max(stats.delta.wrapping_add(stats.rank_jump));
        }
        let target_error = (worst / 2) as f64;
        let mut order: Vec<usize> = (0..percentiles.len()).collect();
        order.sort_by(|left, right| percentiles[*left].total_cmp(&percentiles[*right]));
        let mut result = vec![0.0; percentiles.len()];
        let mut index = 0;
        let mut min_rank = self.sampled[0].rank_jump;
        let last = self.sampled.len() - 1;
        for position in order {
            let percentile = percentiles[position];
            if percentile <= self.relative_error {
                result[position] = self.sampled[0].value;
            } else if percentile >= 1.0 - self.relative_error {
                result[position] = self.sampled[last].value;
            } else {
                let (next_index, next_rank, approx) =
                    self.find_approx_quantile(index, min_rank, target_error, percentile);
                index = next_index;
                min_rank = next_rank;
                result[position] = approx;
            }
        }
        result
    }

    #[must_use]
    pub fn to_bytes(&mut self) -> Vec<u8> {
        self.compress();
        let mut bytes = Vec::with_capacity(24 + self.sampled.len() * 24);
        let threshold = i32::try_from(self.compress_threshold).unwrap_or(i32::MAX);
        let sampled_len = i32::try_from(self.sampled.len()).unwrap_or(i32::MAX);
        bytes.extend_from_slice(&threshold.to_be_bytes());
        bytes.extend_from_slice(&self.relative_error.to_be_bytes());
        bytes.extend_from_slice(&self.count.to_be_bytes());
        bytes.extend_from_slice(&sampled_len.to_be_bytes());
        for stats in &self.sampled {
            bytes.extend_from_slice(&stats.value.to_be_bytes());
            bytes.extend_from_slice(&stats.rank_jump.to_be_bytes());
            bytes.extend_from_slice(&stats.delta.to_be_bytes());
        }
        bytes
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 24 || !(bytes.len() - 24).is_multiple_of(24) {
            return None;
        }
        let threshold = i32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let relative_error = f64::from_be_bytes(bytes[4..12].try_into().ok()?);
        let count = i64::from_be_bytes(bytes[12..20].try_into().ok()?);
        let stated = i32::from_be_bytes(bytes[20..24].try_into().ok()?);
        if threshold < 0 || count < 0 || stated < 0 {
            return None;
        }
        let recorded = usize::try_from(stated).ok()?;
        if bytes.len() != 24 + recorded * 24 {
            return None;
        }
        let mut sampled = Vec::with_capacity(recorded);
        for record in bytes[24..].chunks_exact(24) {
            let value = f64::from_be_bytes(record[0..8].try_into().ok()?);
            let rank_jump = i64::from_be_bytes(record[8..16].try_into().ok()?);
            let delta = i64::from_be_bytes(record[16..24].try_into().ok()?);
            sampled.push(SummaryStats {
                value,
                rank_jump,
                delta,
            });
        }
        Some(Self {
            compress_threshold: usize::try_from(threshold).ok()?,
            relative_error,
            sampled,
            count,
            head: Vec::new(),
            compressed: true,
        })
    }
}

#[allow(clippy::cast_precision_loss)]
fn compress_sampled(current: &[SummaryStats], threshold: f64) -> Vec<SummaryStats> {
    if current.is_empty() {
        return Vec::new();
    }
    if current.len() == 1 {
        return current.to_vec();
    }
    let mut merged = Vec::with_capacity(current.len());
    let mut pending = current[current.len() - 1];
    for sample in current[1..current.len() - 1].iter().rev() {
        let combined = sample
            .rank_jump
            .wrapping_add(pending.rank_jump)
            .wrapping_add(pending.delta);
        let combined_float = combined as f64;
        if combined_float < threshold {
            pending.rank_jump = pending.rank_jump.wrapping_add(sample.rank_jump);
        } else {
            merged.push(pending);
            pending = *sample;
        }
    }
    merged.push(pending);
    if current[0].value <= pending.value {
        merged.push(current[0]);
    }
    merged.reverse();
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits(values: &[f64]) -> Vec<u64> {
        values.iter().map(|value| value.to_bits()).collect()
    }

    fn sequential(limit: i64) -> QuantileSummaries {
        let mut summary = QuantileSummaries::new(0.0001);
        for value in 1..=limit {
            #[allow(clippy::cast_precision_loss)]
            let slot = value as f64;
            summary.insert(slot);
        }
        summary
    }

    #[test]
    fn insert_buffers_values_until_the_head_fills() {
        let mut summary = QuantileSummaries::new(0.01);
        assert!(summary.is_compressed());
        for value in 0..100 {
            summary.insert(f64::from(value));
        }
        assert_eq!(summary.buffered_count(), 100);
        assert_eq!(summary.sampled_count(), 0);
        assert_eq!(summary.count(), 0);
        assert!(!summary.is_compressed());
    }

    #[test]
    fn flush_keeps_samples_sorted_with_edge_deltas_zero() {
        let mut summary = QuantileSummaries::new(0.01);
        for value in [3.0, 1.0, 2.0] {
            summary.insert(value);
        }
        summary.compress();
        assert_eq!(summary.count(), 3);
        assert_eq!(summary.buffered_count(), 0);
        assert!(summary.is_compressed());
        let values: Vec<f64> = summary.stats().iter().map(|stats| stats.value).collect();
        assert_eq!(bits(&values), bits(&[1.0, 2.0, 3.0]));
        assert_eq!(summary.stats()[0].delta, 0);
        assert_eq!(summary.stats()[2].delta, 0);
    }

    #[test]
    fn small_data_queries_match_discrete_ranks() {
        let mut summary = sequential(200);
        assert_eq!(bits(&summary.query(&[0.5])), bits(&[100.0]));
        assert_eq!(bits(&summary.query(&[0.0])), bits(&[1.0]));
        assert_eq!(bits(&summary.query(&[1.0])), bits(&[200.0]));
        assert_eq!(
            bits(&summary.query(&[0.0, 0.5, 1.0])),
            bits(&[1.0, 100.0, 200.0])
        );
    }

    #[test]
    fn query_returns_percentiles_in_input_order() {
        let mut summary = sequential(200);
        assert_eq!(bits(&summary.query(&[0.75, 0.25])), bits(&[150.0, 50.0]));
    }

    #[test]
    fn accuracy_two_collapses_to_the_minimum() {
        let mut summary = QuantileSummaries::new(0.5);
        for value in 1..=200_i64 {
            #[allow(clippy::cast_precision_loss)]
            let slot = value as f64;
            summary.insert(slot);
        }
        assert_eq!(bits(&summary.query(&[0.5])), bits(&[1.0]));
        assert_eq!(
            bits(&summary.query(&[0.0, 0.5, 1.0])),
            bits(&[1.0, 1.0, 200.0])
        );
    }

    #[test]
    fn edge_percentiles_return_min_and_max() {
        let mut summary = QuantileSummaries::new(0.1);
        for value in [5.0, 1.0, 9.0, 3.0, 7.0] {
            summary.insert(value);
        }
        assert_eq!(bits(&summary.query(&[0.1])), bits(&[1.0]));
        assert_eq!(bits(&summary.query(&[0.9])), bits(&[9.0]));
    }

    #[test]
    fn crafted_scan_picks_the_bounded_sample() {
        let mut summary = QuantileSummaries {
            compress_threshold: DEFAULT_COMPRESS_THRESHOLD,
            relative_error: 0.05,
            sampled: vec![
                SummaryStats {
                    value: 10.0,
                    rank_jump: 1,
                    delta: 0,
                },
                SummaryStats {
                    value: 20.0,
                    rank_jump: 5,
                    delta: 0,
                },
                SummaryStats {
                    value: 30.0,
                    rank_jump: 1,
                    delta: 0,
                },
            ],
            count: 7,
            head: Vec::new(),
            compressed: true,
        };
        assert_eq!(bits(&summary.query(&[0.5])), bits(&[20.0]));
    }

    #[test]
    fn empty_summary_queries_empty() {
        let mut summary = QuantileSummaries::new(0.01);
        assert!(summary.query(&[0.5]).is_empty());
        assert!(summary.query(&[]).is_empty());
    }

    #[test]
    fn compress_keeps_min_max_and_count() {
        let mut summary = QuantileSummaries::new(0.01);
        for value in (1..=200_000_i64).rev() {
            #[allow(clippy::cast_precision_loss)]
            let slot = value as f64;
            summary.insert(slot);
        }
        summary.compress();
        assert_eq!(summary.count(), 200_000);
        let stats = summary.stats();
        assert!(stats.len() < 200_000);
        assert_eq!(stats[0].value.to_bits(), 1.0_f64.to_bits());
        assert_eq!(
            stats[stats.len() - 1].value.to_bits(),
            200_000.0_f64.to_bits()
        );
        for window in stats.windows(2) {
            assert!(window[0].value <= window[1].value);
        }
    }

    #[test]
    fn merge_combines_counts_and_keeps_bounds() {
        let mut left = QuantileSummaries::new(0.01);
        for value in 1..=100 {
            left.insert(f64::from(value));
        }
        let mut right = QuantileSummaries::new(0.01);
        for value in 101..=200 {
            right.insert(f64::from(value));
        }
        left.merge(&right);
        assert_eq!(left.count(), 200);
        assert_eq!(left.relative_error().to_bits(), 0.01_f64.to_bits());
        let stats = left.stats();
        assert_eq!(stats[0].value.to_bits(), 1.0_f64.to_bits());
        assert_eq!(stats[stats.len() - 1].value.to_bits(), 200.0_f64.to_bits());
        let answer = left.query(&[0.5]);
        assert_eq!(bits(&answer), bits(&[101.0]));
        assert!(answer[0] >= 96.0 && answer[0] <= 104.0);
    }

    #[test]
    fn merge_with_empty_is_identity() {
        let mut filled = sequential(200);
        filled.compress();
        let before = filled.to_bytes();
        let empty = QuantileSummaries::new(0.0001);
        filled.merge(&empty);
        assert_eq!(filled.to_bytes(), before);
        let mut other_empty = QuantileSummaries::new(0.0001);
        other_empty.merge(&filled);
        assert_eq!(other_empty.to_bytes(), before);
    }

    #[test]
    fn merge_adopts_the_wider_error() {
        let mut tight = QuantileSummaries::new(0.01);
        tight.insert(1.0);
        let mut loose = QuantileSummaries::new(0.5);
        loose.insert(2.0);
        tight.merge(&loose);
        assert_eq!(tight.relative_error().to_bits(), 0.5_f64.to_bits());
        assert_eq!(tight.count(), 2);
    }

    #[test]
    fn serialization_round_trip_preserves_queries() {
        let mut summary = sequential(200);
        let bytes = summary.to_bytes();
        let mut restored =
            QuantileSummaries::from_bytes(&bytes).expect("a serialized summary deserializes");
        assert_eq!(restored.count(), 200);
        assert_eq!(restored.sampled_count(), summary.sampled_count());
        assert_eq!(
            bits(&restored.query(&[0.0, 0.5, 1.0])),
            bits(&[1.0, 100.0, 200.0])
        );
    }

    #[test]
    fn threshold_value_serializes() {
        let mut summary = QuantileSummaries::new(0.01);
        summary.insert(1.0);
        let bytes = summary.to_bytes();
        let threshold = i32::from_be_bytes(bytes[0..4].try_into().expect("four bytes"));
        assert_eq!(threshold, 10_000);
        assert_eq!(DEFAULT_COMPRESS_THRESHOLD, 10_000);
        assert_eq!(DEFAULT_HEAD_SIZE, 50_000);
    }

    #[test]
    fn from_bytes_rejects_short_and_mismatched() {
        assert!(QuantileSummaries::from_bytes(&[]).is_none());
        assert!(QuantileSummaries::from_bytes(&[0_u8; 23]).is_none());
        assert!(QuantileSummaries::from_bytes(&[0_u8; 25]).is_none());
        let mut header = [0_u8; 24];
        header[0..4].copy_from_slice(&(-1_i32).to_be_bytes());
        assert!(QuantileSummaries::from_bytes(&header).is_none());
        let mut summary = sequential(10);
        let mut bytes = summary.to_bytes();
        bytes.push(0);
        assert!(QuantileSummaries::from_bytes(&bytes).is_none());
    }

    #[test]
    fn million_row_state_stays_small() {
        let mut summary = QuantileSummaries::new(0.0001);
        for value in 1..=1_000_000_i64 {
            #[allow(clippy::cast_precision_loss)]
            let slot = value as f64;
            summary.insert(slot);
        }
        let bytes = summary.to_bytes();
        assert!(
            summary.sampled_count() < 50_000,
            "{}",
            summary.sampled_count()
        );
        assert!(bytes.len() < 2_000_000, "{}", bytes.len());
        assert_eq!(summary.count(), 1_000_000);
    }
}
