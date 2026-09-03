use std::cmp::Ordering;

use iceberg::spec::{DataFile, Literal, Struct};

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
