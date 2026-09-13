use super::super::cache_budget::distinct_buffer_bytes;
use super::super::*;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

fn sample_batch(rows: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<i32>>()))],
    )
    .unwrap()
}

#[test]
fn distinct_buffer_bytes_ignores_pointers_already_in_the_set() {
    let batch = sample_batch(4);
    let mut seen = HashSet::new();
    let first = distinct_buffer_bytes(std::slice::from_ref(&batch), &mut seen);
    assert!(first > 0);
    assert_eq!(
        distinct_buffer_bytes(std::slice::from_ref(&batch), &mut seen),
        0
    );
}

#[tokio::test]
async fn two_cache_views_sharing_one_buffer_count_it_once() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    session
        .register_record_batches_as_temp_view(
            "__repark_cache_a",
            batch.schema(),
            vec![batch.clone()],
        )
        .unwrap();
    let one_view = session.retained_cache_bytes().await.unwrap();
    assert!(one_view > 0);
    session
        .register_record_batches_as_temp_view("__repark_cache_b", batch.schema(), vec![batch])
        .unwrap();
    assert_eq!(session.retained_cache_bytes().await.unwrap(), one_view);
}

#[tokio::test]
async fn sliced_array_counts_its_parent_buffer_once() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    session
        .register_record_batches_as_temp_view(
            "__repark_cache_a",
            batch.schema(),
            vec![batch.clone()],
        )
        .unwrap();
    let one_view = session.retained_cache_bytes().await.unwrap();
    let sliced = RecordBatch::try_new(batch.schema(), vec![batch.column(0).slice(1, 2)]).unwrap();
    session
        .register_record_batches_as_temp_view("__repark_cache_b", batch.schema(), vec![sliced])
        .unwrap();
    assert_eq!(session.retained_cache_bytes().await.unwrap(), one_view);
}

#[tokio::test]
async fn no_cache_view_returns_zero() {
    let session = ReparkSession::new().unwrap();
    assert_eq!(session.retained_cache_bytes().await.unwrap(), 0);
}

#[tokio::test]
async fn dropped_cache_view_no_longer_counts() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    session
        .register_record_batches_as_temp_view("__repark_cache_a", batch.schema(), vec![batch])
        .unwrap();
    assert!(session.retained_cache_bytes().await.unwrap() > 0);
    assert!(session.drop_temp_view("__repark_cache_a").unwrap());
    assert_eq!(session.retained_cache_bytes().await.unwrap(), 0);
}

#[tokio::test]
async fn non_cache_temp_views_are_ignored() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    for name in ["user_view", "__repark_ckpt_a"] {
        session
            .register_record_batches_as_temp_view(name, batch.schema(), vec![batch.clone()])
            .unwrap();
    }
    assert_eq!(session.retained_cache_bytes().await.unwrap(), 0);
    session
        .register_record_batches_as_temp_view("__repark_cache_a", batch.schema(), vec![batch])
        .unwrap();
    let retained = session.retained_cache_bytes().await.unwrap();
    assert!(retained > 0);
    assert!(session.drop_temp_view("user_view").unwrap());
    assert_eq!(session.retained_cache_bytes().await.unwrap(), retained);
}
