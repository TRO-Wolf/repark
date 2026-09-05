use std::cmp::Ordering;

use iceberg::spec::{DataFile, Datum, Literal, Struct};

pub(crate) fn ascending_partition_order(mut files: Vec<DataFile>) -> Vec<DataFile> {
    files.sort_by(|left, right| compare_partitions(left.partition(), right.partition()));
    files
}

fn compare_partitions(left: &Struct, right: &Struct) -> Ordering {
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let ordering = match (left_value, right_value) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left_value), Some(right_value)) => compare_values(left_value, right_value),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.iter().len().cmp(&right.iter().len())
}

fn compare_values(left: &Literal, right: &Literal) -> Ordering {
    match (left, right) {
        (Literal::Primitive(left), Literal::Primitive(right)) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

pub(crate) fn stable_commit_order(mut files: Vec<DataFile>) -> Vec<DataFile> {
    files.sort_by(|left, right| {
        compare_partitions(left.partition(), right.partition())
            .then_with(|| compare_bounds(left, right))
            .then_with(|| left.record_count().cmp(&right.record_count()))
            .then_with(|| left.file_size_in_bytes().cmp(&right.file_size_in_bytes()))
            .then_with(|| left.file_path().cmp(right.file_path()))
    });
    files
}

fn compare_bounds(left: &DataFile, right: &DataFile) -> Ordering {
    let mut fields: Vec<i32> = left
        .lower_bounds()
        .keys()
        .chain(right.lower_bounds().keys())
        .chain(left.upper_bounds().keys())
        .chain(right.upper_bounds().keys())
        .copied()
        .collect();
    fields.sort_unstable();
    fields.dedup();
    for field in &fields {
        let ordering = compare_bound(
            left.lower_bounds().get(field),
            right.lower_bounds().get(field),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    for field in &fields {
        let ordering = compare_bound(
            left.upper_bounds().get(field),
            right.upper_bounds().get(field),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_bound(left: Option<&Datum>, right: Option<&Datum>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left
            .literal()
            .partial_cmp(right.literal())
            .unwrap_or_else(|| bound_key(left).cmp(&bound_key(right))),
    }
}

fn bound_key(datum: &Datum) -> Vec<u8> {
    datum
        .to_bytes()
        .map(|bytes| bytes.to_vec())
        .unwrap_or_default()
}
