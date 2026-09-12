use std::collections::BTreeMap;
use std::collections::BTreeSet;

use iceberg::spec::{PrimitiveType, Type};

const LOW_FRACTION: f64 = 0.25;
const HIGH_MULTIPLE: f64 = 4.0;
pub(super) const DISTINCT_LIMIT: usize = 1000;
pub(super) const BUCKET_WIDTHS: [u32; 5] = [8, 16, 32, 64, 128];
pub(super) const FALLBACK_BYTE_RATIO: f64 = 0.55;

#[derive(Clone, Copy)]
pub(super) enum ColumnKind {
    Timestamp,
    Date,
    Int,
    Text,
}

pub(super) fn column_kind(field_type: &Type) -> Option<ColumnKind> {
    match field_type.as_primitive_type()? {
        PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => Some(ColumnKind::Timestamp),
        PrimitiveType::Date => Some(ColumnKind::Date),
        PrimitiveType::Int | PrimitiveType::Long => Some(ColumnKind::Int),
        PrimitiveType::String => Some(ColumnKind::Text),
        _ => None,
    }
}

pub(super) struct RatedColumn {
    pub(super) name: String,
    pub(super) kind: ColumnKind,
    pub(super) numbers: Vec<Option<(i64, i64)>>,
    pub(super) texts: Vec<Option<(String, String)>>,
}

fn civil_year_month(days_since_epoch: i64) -> (i64, i64) {
    let shifted = days_since_epoch + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era = (day_of_era - day_of_era.div_euclid(1_460) + day_of_era.div_euclid(36_524)
        - day_of_era.div_euclid(146_096))
    .div_euclid(365);
    let year = year_of_era + era * 400;
    let day_of_year =
        day_of_era - (365 * year_of_era + year_of_era.div_euclid(4) - year_of_era.div_euclid(100));
    let month_prime = (5 * day_of_year + 2).div_euclid(153);
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    if month <= 2 {
        (year + 1, month)
    } else {
        (year, month)
    }
}

#[derive(Clone)]
pub(super) enum TemporalGrain {
    Years,
    Months,
    Days,
    Hours,
}

#[derive(Clone)]
pub(super) enum Grain {
    Temporal(TemporalGrain),
    Identity,
    Bucket(u32),
}

fn truncate_epoch(seconds: i64, grain: &TemporalGrain) -> i64 {
    match grain {
        TemporalGrain::Years => civil_year_month(seconds.div_euclid(86_400)).0,
        TemporalGrain::Months => {
            let (year, month) = civil_year_month(seconds.div_euclid(86_400));
            year * 12 + (month - 1)
        }
        TemporalGrain::Days => seconds.div_euclid(86_400),
        TemporalGrain::Hours => seconds.div_euclid(3_600),
    }
}

#[derive(Clone)]
pub(super) struct SpecPart {
    pub(super) column: String,
    pub(super) grain: Grain,
}

pub(super) fn part_label(part: &SpecPart) -> String {
    match &part.grain {
        Grain::Temporal(TemporalGrain::Years) => format!("years({})", part.column),
        Grain::Temporal(TemporalGrain::Months) => format!("months({})", part.column),
        Grain::Temporal(TemporalGrain::Days) => format!("days({})", part.column),
        Grain::Temporal(TemporalGrain::Hours) => format!("hours({})", part.column),
        Grain::Identity => format!("identity({})", part.column),
        Grain::Bucket(width) => format!("bucket({width}, {})", part.column),
    }
}

pub(super) fn spec_label(parts: &[SpecPart]) -> String {
    if parts.is_empty() {
        return "unpartitioned".to_string();
    }
    parts.iter().map(part_label).collect::<Vec<_>>().join("+")
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum ValueKey {
    Number(i64),
    Text(String),
    Pair(Box<ValueKey>, Box<ValueKey>),
}

#[allow(clippy::cast_precision_loss)]
fn penalty(value: f64, target: u64) -> f64 {
    let target = target as f64;
    let low = LOW_FRACTION * target;
    let high = HIGH_MULTIPLE * target;
    (low - value).max(0.0) / low + (value - high).max(0.0) / high
}

#[allow(clippy::cast_precision_loss)]
pub(super) fn accumulate(
    items: &[(f64, Vec<ValueKey>)],
    target: u64,
    byte_ratio: f64,
) -> (f64, usize, f64) {
    let mut totals: BTreeMap<&ValueKey, f64> = BTreeMap::new();
    for (size, values) in items {
        let share = *size / values.len() as f64;
        for value in values {
            *totals.entry(value).or_default() += share;
        }
    }
    let mut score = 0.0;
    let mut projected = 0.0;
    for amount in totals.values() {
        score += penalty(*amount, target);
        projected += (amount * byte_ratio / target as f64).ceil();
    }
    (score, totals.len(), projected)
}

fn truncate_items(
    rated: &RatedColumn,
    grain: &TemporalGrain,
    sizes: &[f64],
) -> (Vec<(f64, Vec<ValueKey>)>, usize) {
    let present: Vec<(i64, i64)> = rated.numbers.iter().filter_map(|pair| *pair).collect();
    if present.is_empty() {
        return (
            sizes
                .iter()
                .map(|size| (*size, vec![ValueKey::Number(0)]))
                .collect(),
            sizes.len(),
        );
    }
    let first = present
        .iter()
        .map(|(low, _)| truncate_epoch(*low, grain))
        .min()
        .unwrap_or(0);
    let last = present
        .iter()
        .map(|(_, high)| truncate_epoch(*high, grain))
        .max()
        .unwrap_or(0);
    let full: Vec<ValueKey> = (first..=last).map(ValueKey::Number).collect();
    let mut items = Vec::with_capacity(sizes.len());
    let mut fallback = 0;
    for (size, pair) in sizes.iter().zip(rated.numbers.iter()) {
        if let Some((low, high)) = pair {
            let start = truncate_epoch(*low, grain).max(first);
            let stop = truncate_epoch(*high, grain).min(last);
            if stop < start {
                items.push((*size, full.clone()));
                fallback += 1;
            } else {
                items.push((*size, (start..=stop).map(ValueKey::Number).collect()));
            }
        } else {
            items.push((*size, full.clone()));
            fallback += 1;
        }
    }
    (items, fallback)
}

fn identity_domain_size(rated: &RatedColumn) -> Option<usize> {
    let present = rated.numbers.iter().flatten().count() + rated.texts.iter().flatten().count();
    if present == 0 {
        return None;
    }
    let endpoints = match rated.kind {
        ColumnKind::Text => text_domain(rated).len(),
        _ => number_domain(rated).len(),
    };
    Some(endpoints)
}

fn number_domain(rated: &RatedColumn) -> Vec<i64> {
    let mut domain = BTreeSet::new();
    for pair in rated.numbers.iter().flatten() {
        domain.insert(pair.0);
        domain.insert(pair.1);
    }
    domain.into_iter().collect()
}

fn text_domain(rated: &RatedColumn) -> Vec<String> {
    let mut domain = BTreeSet::new();
    for pair in rated.texts.iter().flatten() {
        domain.insert(pair.0.clone());
        domain.insert(pair.1.clone());
    }
    domain.into_iter().collect()
}

fn identity_items(
    domain: &[ValueKey],
    rated: &RatedColumn,
    is_text: bool,
    sizes: &[f64],
) -> (Vec<(f64, Vec<ValueKey>)>, usize) {
    let mut items = Vec::with_capacity(sizes.len());
    let mut fallback = 0;
    for (size, index) in sizes.iter().zip(0..) {
        let inside: Vec<ValueKey> = if is_text {
            match &rated.texts[index] {
                Some((low, high)) => domain
                    .iter()
                    .filter(
                        |key| matches!(key, ValueKey::Text(value) if value >= low && value <= high),
                    )
                    .cloned()
                    .collect(),
                None => Vec::new(),
            }
        } else {
            match rated.numbers[index] {
                Some((low, high)) => domain
                    .iter()
                    .filter(|key| {
                        matches!(key, ValueKey::Number(value) if value >= &low && value <= &high)
                    })
                    .cloned()
                    .collect(),
                None => Vec::new(),
            }
        };
        if inside.is_empty() {
            items.push((*size, domain.to_vec()));
            fallback += 1;
        } else {
            items.push((*size, inside));
        }
    }
    (items, fallback)
}

fn bucket_items(width: u32, sizes: &[f64]) -> Vec<(f64, Vec<ValueKey>)> {
    let buckets: Vec<ValueKey> = (0..i64::from(width)).map(ValueKey::Number).collect();
    sizes.iter().map(|size| (*size, buckets.clone())).collect()
}

pub(super) struct ScoredSpec {
    pub(super) parts: Vec<SpecPart>,
    pub(super) label: String,
    pub(super) score: f64,
    pub(super) partitions: usize,
    pub(super) projected: f64,
    pub(super) items: Vec<(f64, Vec<ValueKey>)>,
    pub(super) note: String,
}

pub(super) fn score_single(
    column: &str,
    grain: Grain,
    rated: &RatedColumn,
    sizes: &[f64],
    target: u64,
    byte_ratio: f64,
) -> Option<ScoredSpec> {
    let part = SpecPart {
        column: column.to_string(),
        grain,
    };
    let (items, fallback) = match &part.grain {
        Grain::Temporal(grain) => truncate_items(rated, grain, sizes),
        Grain::Identity => {
            let domain: Vec<ValueKey> = match &rated.kind {
                ColumnKind::Text => text_domain(rated).into_iter().map(ValueKey::Text).collect(),
                _ => number_domain(rated)
                    .into_iter()
                    .map(ValueKey::Number)
                    .collect(),
            };
            if domain.is_empty() {
                return None;
            }
            identity_items(
                &domain,
                rated,
                matches!(rated.kind, ColumnKind::Text),
                sizes,
            )
        }
        Grain::Bucket(width) => (bucket_items(*width, sizes), 0),
    };
    let (score, partitions, projected) = accumulate(&items, target, byte_ratio);
    let note = match &part.grain {
        Grain::Bucket(_) => "uniform-hash spread over N buckets".to_string(),
        _ if fallback > 0 => format!("{fallback} files spread uniformly"),
        _ => String::new(),
    };
    Some(ScoredSpec {
        label: part_label(&part),
        parts: vec![part],
        score,
        partitions,
        projected,
        items,
        note,
    })
}

pub(super) fn score_pair(
    first: &ScoredSpec,
    second: &ScoredSpec,
    target: u64,
    byte_ratio: f64,
) -> ScoredSpec {
    let combined: Vec<(f64, Vec<ValueKey>)> = first
        .items
        .iter()
        .zip(second.items.iter())
        .map(|((size, mine), (_, other))| {
            let mut values = Vec::with_capacity(mine.len() * other.len());
            for left in mine {
                for right in other {
                    values.push(ValueKey::Pair(
                        Box::new(left.clone()),
                        Box::new(right.clone()),
                    ));
                }
            }
            (*size, values)
        })
        .collect();
    let (score, partitions, projected) = accumulate(&combined, target, byte_ratio);
    let mut parts = first.parts.clone();
    parts.extend(second.parts.iter().cloned());
    ScoredSpec {
        label: format!("{}+{}", first.label, second.label),
        parts,
        score,
        partitions,
        projected,
        items: combined,
        note: "cross-product spread".to_string(),
    }
}

pub(super) fn is_better(score: f64, label: &str, best_score: f64, best_label: &str) -> bool {
    score
        .total_cmp(&best_score)
        .then_with(|| label.cmp(best_label))
        == std::cmp::Ordering::Less
}

pub(super) fn identity_choice(rated: &RatedColumn) -> Option<bool> {
    identity_domain_size(rated).map(|count| count <= DISTINCT_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rated_numbers(pairs: Vec<Option<(i64, i64)>>) -> RatedColumn {
        let texts = vec![None; pairs.len()];
        RatedColumn {
            name: "ts".to_string(),
            kind: ColumnKind::Timestamp,
            numbers: pairs,
            texts,
        }
    }

    #[test]
    fn civil_dates_match_the_proleptic_calendar() {
        assert_eq!(civil_year_month(0), (1970, 1));
        assert_eq!(civil_year_month(-1), (1969, 12));
        assert_eq!(civil_year_month(18_628), (2021, 1));
        assert_eq!(civil_year_month(20_513), (2026, 3));
    }

    #[test]
    fn truncate_grains_nest_one_inside_the_other() {
        assert_eq!(truncate_epoch(0, &TemporalGrain::Years), 1970);
        assert_eq!(truncate_epoch(0, &TemporalGrain::Months), 1970 * 12);
        assert_eq!(truncate_epoch(0, &TemporalGrain::Days), 0);
        assert_eq!(truncate_epoch(0, &TemporalGrain::Hours), 0);
        assert_eq!(truncate_epoch(86_400, &TemporalGrain::Days), 1);
        assert_eq!(truncate_epoch(3_599, &TemporalGrain::Hours), 0);
        assert_eq!(truncate_epoch(3_600, &TemporalGrain::Hours), 1);
    }

    #[test]
    fn penalty_is_zero_inside_the_band_and_one_at_the_edges() {
        assert!((penalty(1_000.0, 1_000) - 0.0).abs() < f64::EPSILON);
        assert!((penalty(250.0, 1_000) - 0.0).abs() < f64::EPSILON);
        assert!((penalty(4_000.0, 1_000) - 0.0).abs() < f64::EPSILON);
        assert!((penalty(0.0, 1_000) - 1.0).abs() < f64::EPSILON);
        assert!((penalty(8_000.0, 1_000) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulate_splits_a_spanning_file_one_over_k() {
        let items = vec![(100.0, vec![ValueKey::Number(0), ValueKey::Number(1)])];
        let (score, partitions, projected) = accumulate(&items, 100, 1.0);
        assert!((score - 0.0).abs() < f64::EPSILON);
        assert_eq!(partitions, 2);
        assert!((projected - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulate_counts_one_file_per_value_at_target() {
        let items = vec![
            (100.0, vec![ValueKey::Number(0)]),
            (100.0, vec![ValueKey::Number(0)]),
        ];
        let (score, partitions, projected) = accumulate(&items, 100, 1.0);
        assert!((score - 0.0).abs() < f64::EPSILON);
        assert_eq!(partitions, 1);
        assert!((projected - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rp17_bed_shape_projects_the_uncompressed_sum_once() {
        let items: Vec<(f64, Vec<ValueKey>)> = std::iter::once(36_611.0)
            .chain(std::iter::repeat_n(36_551.0, 205))
            .map(|size| (size, vec![ValueKey::Number(0)]))
            .collect();
        let (_score, partitions, projected) = accumulate(&items, 524_288, 0.376_076);
        assert_eq!(partitions, 1);
        assert!(
            (projected - 6.0).abs() < f64::EPSILON,
            "RP-17 uniform shape projects ceil(2 831 692 / 524 288) = 6, got {projected}"
        );
    }

    #[test]
    fn candidate_labels_use_spark_ddl_spelling() {
        let part = SpecPart {
            column: "ts".to_string(),
            grain: Grain::Temporal(TemporalGrain::Days),
        };
        assert_eq!(part_label(&part), "days(ts)");
        let part = SpecPart {
            column: "id".to_string(),
            grain: Grain::Bucket(16),
        };
        assert_eq!(part_label(&part), "bucket(16, id)");
        let part = SpecPart {
            column: "region".to_string(),
            grain: Grain::Identity,
        };
        assert_eq!(part_label(&part), "identity(region)");
        assert_eq!(spec_label(&[]), "unpartitioned");
    }

    #[test]
    fn bucket_single_splits_bytes_evenly_over_n() {
        let rated = rated_numbers(vec![Some((0, 9)), Some((10, 19))]);
        let candidate = score_single("id", Grain::Bucket(8), &rated, &[800.0, 800.0], 100, 1.0)
            .expect("bucket scores");
        assert_eq!(candidate.label, "bucket(8, id)");
        assert!((candidate.score - 0.0).abs() < f64::EPSILON);
        assert_eq!(candidate.partitions, 8);
        assert_eq!(candidate.note, "uniform-hash spread over N buckets");
    }

    #[test]
    fn pair_single_crosses_parent_values() {
        let rated = rated_numbers(vec![Some((0, 86_400))]);
        let first = score_single(
            "ts",
            Grain::Temporal(TemporalGrain::Days),
            &rated,
            &[100.0],
            100,
            1.0,
        )
        .expect("day scores");
        let second = score_single(
            "ts",
            Grain::Temporal(TemporalGrain::Hours),
            &rated,
            &[100.0],
            100,
            1.0,
        )
        .expect("hour scores");
        let pair = score_pair(&first, &second, 100, 1.0);
        assert_eq!(pair.label, "days(ts)+hours(ts)");
        assert_eq!(pair.partitions, 50);
        assert_eq!(pair.note, "cross-product spread");
    }

    #[test]
    fn truncate_items_spread_missing_bounds_over_the_global_range() {
        let rated = rated_numbers(vec![Some((0, 86_400)), None]);
        let (items, fallback) = truncate_items(&rated, &TemporalGrain::Days, &[1_000.0, 1_000.0]);
        assert_eq!(fallback, 1);
        assert_eq!(items[0].1.len(), 2);
        assert_eq!(items[1].1.len(), 2);
    }

    #[test]
    fn identity_domain_counts_endpoints_not_files() {
        let rated = rated_numbers(vec![Some((3, 3)), Some((3, 7))]);
        assert_eq!(identity_domain_size(&rated), Some(2));
        let empty = rated_numbers(vec![None, None]);
        assert_eq!(identity_domain_size(&empty), None);
    }

    #[test]
    fn column_kinds_cover_the_p2_branches() {
        assert!(matches!(
            column_kind(&Type::Primitive(PrimitiveType::Timestamp)),
            Some(ColumnKind::Timestamp)
        ));
        assert!(matches!(
            column_kind(&Type::Primitive(PrimitiveType::Date)),
            Some(ColumnKind::Date)
        ));
        assert!(matches!(
            column_kind(&Type::Primitive(PrimitiveType::Long)),
            Some(ColumnKind::Int)
        ));
        assert!(matches!(
            column_kind(&Type::Primitive(PrimitiveType::String)),
            Some(ColumnKind::Text)
        ));
        assert!(column_kind(&Type::Primitive(PrimitiveType::Boolean)).is_none());
    }
}
