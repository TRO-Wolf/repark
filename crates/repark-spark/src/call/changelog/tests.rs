use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use super::{compute_updates, remove_carryovers};

type Row = (i64, &'static str, &'static str, &'static str, i32, i64);

const SEEDED: [Row; 10] = [
    (1, "a", "x", "INSERT", 0, 100),
    (2, "b", "y", "INSERT", 0, 100),
    (3, "c", "x", "INSERT", 1, 101),
    (4, "d", "y", "INSERT", 2, 102),
    (5, "e", "x", "INSERT", 2, 102),
    (1, "a", "x", "DELETE", 3, 103),
    (2, "b", "y", "DELETE", 3, 103),
    (1, "a", "x", "INSERT", 3, 103),
    (2, "u", "y", "INSERT", 3, 103),
    (3, "c", "x", "DELETE", 4, 104),
];

fn batch(rows: &[Row]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("cat", DataType::Utf8, true),
        Field::new("_change_type", DataType::Utf8, true),
        Field::new("_change_ordinal", DataType::Int32, true),
        Field::new("_commit_snapshot_id", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<i64>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<&str>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<&str>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<&str>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.4).collect::<Vec<i32>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.5).collect::<Vec<i64>>(),
            )),
        ],
    )
    .expect("fixture batch")
}

fn rendered(batch: &RecordBatch) -> Vec<String> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let data = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data");
    let change_type = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("change type");
    let ordinal = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("ordinal");
    let mut rows: Vec<String> = (0..batch.num_rows())
        .map(|row| {
            format!(
                "{},{},{},{}",
                ids.value(row),
                data.value(row),
                change_type.value(row),
                ordinal.value(row)
            )
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn carryover_removal_drops_the_rewritten_but_unchanged_row() {
    let got = remove_carryovers(&batch(&SEEDED), false).expect("carryovers");
    assert_eq!(
        rendered(&got),
        vec![
            "1,a,INSERT,0".to_string(),
            "2,b,DELETE,3".to_string(),
            "2,b,INSERT,0".to_string(),
            "2,u,INSERT,3".to_string(),
            "3,c,DELETE,4".to_string(),
            "3,c,INSERT,1".to_string(),
            "4,d,INSERT,2".to_string(),
            "5,e,INSERT,2".to_string(),
        ]
    );
}

#[test]
fn net_changes_keep_each_surviving_row_once_at_its_last_ordinal() {
    let got = remove_carryovers(&batch(&SEEDED), true).expect("net changes");
    assert_eq!(
        rendered(&got),
        vec![
            "1,a,INSERT,3".to_string(),
            "2,u,INSERT,3".to_string(),
            "4,d,INSERT,2".to_string(),
            "5,e,INSERT,2".to_string(),
        ]
    );
}

#[test]
fn compute_updates_pairs_one_ordinals_delete_and_insert() {
    let got = compute_updates(&batch(&SEEDED), &["id".to_string()]).expect("updates");
    assert_eq!(
        rendered(&got),
        vec![
            "1,a,INSERT,0".to_string(),
            "2,b,INSERT,0".to_string(),
            "2,b,UPDATE_BEFORE,3".to_string(),
            "2,u,UPDATE_AFTER,3".to_string(),
            "3,c,DELETE,4".to_string(),
            "3,c,INSERT,1".to_string(),
            "4,d,INSERT,2".to_string(),
            "5,e,INSERT,2".to_string(),
        ]
    );
}

#[test]
fn compute_updates_over_a_non_unique_identifier_answers_as_spark_does() {
    let got = compute_updates(&batch(&SEEDED), &["cat".to_string()]).expect("updates");
    assert_eq!(
        rendered(&got),
        rendered(&compute_updates(&batch(&SEEDED), &["id".to_string()]).expect("updates"))
    );
}

#[test]
fn compute_updates_refuses_two_deletes_of_one_identifier() {
    let rows = [
        (7, "a", "z", "DELETE", 1, 101),
        (7, "b", "z", "DELETE", 1, 101),
        (8, "c", "z", "INSERT", 1, 101),
    ];
    let error = compute_updates(&batch(&rows), &["id".to_string()]).expect_err("refuses");
    assert!(
        error.to_string().contains(
            "Cannot compute updates because there are multiple rows with the same identifier fields([id]). Please make sure the rows are unique."
        ),
        "{error}"
    );
}

#[test]
fn an_unpaired_delete_at_a_later_ordinal_stays_a_delete() {
    let rows = [
        (3, "c", "x", "INSERT", 1, 101),
        (3, "c", "x", "DELETE", 4, 104),
    ];
    let got = compute_updates(&batch(&rows), &["id".to_string()]).expect("updates");
    assert_eq!(
        rendered(&got),
        vec!["3,c,DELETE,4".to_string(), "3,c,INSERT,1".to_string(),]
    );
}

#[test]
fn carryover_removal_keeps_a_delete_and_insert_from_different_snapshots() {
    let rows = [
        (1, "a", "x", "DELETE", 1, 101),
        (1, "a", "x", "INSERT", 2, 102),
    ];
    let got = remove_carryovers(&batch(&rows), false).expect("carryovers");
    assert_eq!(
        rendered(&got),
        vec!["1,a,DELETE,1".to_string(), "1,a,INSERT,2".to_string(),]
    );
}

#[test]
fn net_changes_drop_a_row_deleted_and_reinserted_inside_the_window() {
    let rows = [
        (1, "a", "x", "DELETE", 1, 101),
        (1, "a", "x", "INSERT", 2, 102),
    ];
    let got = remove_carryovers(&batch(&rows), true).expect("net changes");
    assert!(rendered(&got).is_empty(), "{:?}", rendered(&got));
}

#[test]
fn net_changes_keep_a_row_only_deleted_inside_the_window() {
    let rows = [
        (1, "a", "x", "DELETE", 1, 101),
        (2, "b", "y", "INSERT", 2, 102),
    ];
    let got = remove_carryovers(&batch(&rows), true).expect("net changes");
    assert_eq!(
        rendered(&got),
        vec!["1,a,DELETE,1".to_string(), "2,b,INSERT,2".to_string(),]
    );
}
