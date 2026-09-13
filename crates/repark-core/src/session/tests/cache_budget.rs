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

#[tokio::test]
async fn total_budget_refuses_mid_stream_before_the_full_result() {
    let session = ReparkSession::new().unwrap();
    let batch_a = sample_batch(4);
    let batch_b = sample_batch(8);
    let batch_c = sample_batch(4);
    let mut seen = HashSet::new();
    let first_batch_bytes = distinct_buffer_bytes(std::slice::from_ref(&batch_a), &mut seen);
    session
        .register_record_batches_as_temp_view(
            "src",
            batch_a.schema(),
            vec![batch_a, batch_b, batch_c],
        )
        .unwrap();
    let frame = session.sql("SELECT * FROM src").await.unwrap();
    let error = session
        .materialize_dataframe_as_cache_view(
            "__repark_cache_x",
            frame,
            (None, Some(first_batch_bytes)),
        )
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("REPARK_CACHE_BUDGET_EXCEEDED"),
        "{message}"
    );
    assert!(
        message.contains(&format!("repark.cache.max_total_bytes={first_batch_bytes}")),
        "{message}"
    );
    assert!(message.contains("retained 0 bytes"), "{message}");
    let admitted: u64 = message
        .split("admitted ")
        .nth(1)
        .and_then(|tail| tail.split(" bytes").next())
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("refusal must report admitted bytes: {message}"));
    assert!(admitted > first_batch_bytes, "{message}");
    let frame = session.sql("SELECT * FROM src").await.unwrap();
    session
        .materialize_dataframe_as_cache_view("__repark_cache_y", frame, (None, Some(u64::MAX)))
        .await
        .unwrap();
    let full = session.retained_cache_bytes().await.unwrap();
    assert!(
        admitted < full,
        "refusal must stop before admitting the full result: admitted {admitted} vs {full}"
    );
    assert!(!session.drop_temp_view("__repark_cache_x").unwrap());
}

#[tokio::test]
async fn total_budget_counts_buffers_shared_with_live_views_once() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    session
        .register_record_batches_as_temp_view("__repark_cache_a", batch.schema(), vec![batch])
        .unwrap();
    let retained = session.retained_cache_bytes().await.unwrap();
    assert!(retained > 0);
    let frame = session.sql("SELECT * FROM __repark_cache_a").await.unwrap();
    session
        .materialize_dataframe_as_cache_view("__repark_cache_b", frame, (None, Some(retained + 1)))
        .await
        .unwrap();
    assert_eq!(session.retained_cache_bytes().await.unwrap(), retained);
    let fresh = sample_batch(4);
    session
        .register_record_batches_as_temp_view("src", fresh.schema(), vec![fresh])
        .unwrap();
    let frame = session.sql("SELECT * FROM src").await.unwrap();
    let error = session
        .materialize_dataframe_as_cache_view("__repark_cache_c", frame, (None, Some(retained)))
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("REPARK_CACHE_BUDGET_EXCEEDED"),
        "{message}"
    );
    assert!(
        message.contains(&format!("retained {retained} bytes")),
        "{message}"
    );
    assert!(!session.drop_temp_view("__repark_cache_c").unwrap());
}

#[tokio::test]
async fn max_bytes_keeps_its_message_and_registers_nothing() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    session
        .register_record_batches_as_temp_view("src", batch.schema(), vec![batch])
        .unwrap();
    let frame = session.sql("SELECT * FROM src").await.unwrap();
    let error = session
        .materialize_dataframe_as_cache_view("__repark_cache_x", frame, (Some(1), None))
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("cache materialize size"), "{message}");
    assert!(
        message.contains("exceeds repark.cache.max_bytes=1"),
        "{message}"
    );
    assert!(
        !message.contains("REPARK_CACHE_BUDGET_EXCEEDED"),
        "{message}"
    );
    assert!(!session.drop_temp_view("__repark_cache_x").unwrap());
}

#[tokio::test]
async fn admitted_cache_registers_and_retained_matches_admitted() {
    let session = ReparkSession::new().unwrap();
    let batch = sample_batch(4);
    let mut seen = HashSet::new();
    let expected = distinct_buffer_bytes(std::slice::from_ref(&batch), &mut seen);
    session
        .register_record_batches_as_temp_view("src", batch.schema(), vec![batch])
        .unwrap();
    let frame = session.sql("SELECT * FROM src").await.unwrap();
    session
        .materialize_dataframe_as_cache_view("__repark_cache_x", frame, (None, Some(u64::MAX)))
        .await
        .unwrap();
    assert_eq!(session.retained_cache_bytes().await.unwrap(), expected);
    assert!(session.drop_temp_view("__repark_cache_x").unwrap());
}
