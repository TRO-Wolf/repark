use super::super::*;
use arrow::array::{Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;

fn source(session: &ReparkSession, name: &str, rows: i32) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<i32>>()))],
    )
    .expect("source fixture batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("source fixture table");
    session
        .context()
        .register_table(
            session.temp_view_ref(name).expect("source ref"),
            Arc::new(table),
        )
        .expect("source registers");
}

async fn lineage(session: &ReparkSession, name: &str) -> DataFrame {
    session
        .context()
        .table(session.temp_view_ref(name).expect("lineage ref"))
        .await
        .expect("lineage frame")
}

async fn filled(session: &ReparkSession, name: &str) -> Option<bool> {
    session
        .lazy_cache_filled(name)
        .await
        .expect("fill state reads")
}

async fn lazy(session: &ReparkSession, budgets: (Option<u64>, Option<u64>)) -> DataFrame {
    source(session, "src", 64);
    let frame = lineage(session, "src").await;
    session
        .register_lazy_cache_view("__repark_cache_t", frame, budgets, None, None)
        .await
        .expect("lazy cache registers");
    assert_eq!(filled(session, "__repark_cache_t").await, Some(false));
    lineage(session, "__repark_cache_t").await
}

#[tokio::test]
async fn planning_and_explain_fill_nothing_and_the_first_execution_fills() {
    let session = ReparkSession::new().unwrap();
    let frame = lazy(&session, (None, None)).await;
    let physical = frame.clone().create_physical_plan().await.expect("plans");
    drop(physical);
    let explained = frame
        .clone()
        .explain(false, false)
        .expect("explain frame")
        .collect()
        .await
        .expect("explain runs");
    assert!(!explained.is_empty());
    assert_eq!(filled(&session, "__repark_cache_t").await, Some(false));
    assert_eq!(frame.clone().count().await.expect("count"), 64);
    assert_eq!(filled(&session, "__repark_cache_t").await, Some(true));
    let rows: usize = frame
        .collect()
        .await
        .expect("collect")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 64);
}

#[tokio::test]
async fn a_refused_fill_keeps_nothing_and_the_next_use_retries() {
    let session = ReparkSession::new().unwrap();
    let frame = lazy(&session, (Some(1), None)).await;
    for _ in 0..2 {
        let error = frame.clone().count().await.expect_err("over the budget");
        let message = engine_err(error).to_string();
        assert!(
            message.contains("repark config error: cache materialize size"),
            "{message}"
        );
        assert!(message.contains("repark.cache.max_bytes=1"), "{message}");
        assert_eq!(filled(&session, "__repark_cache_t").await, Some(false));
    }
    assert_eq!(session.retained_cache_bytes().await.unwrap(), 0);
}

#[tokio::test]
async fn two_concurrent_first_uses_fill_once() {
    let session = ReparkSession::new().unwrap();
    let frame = lazy(&session, (None, None)).await;
    let (first, second) = tokio::join!(frame.clone().collect(), frame.clone().collect());
    let (first, second) = (first.expect("first"), second.expect("second"));
    let pointer = |batches: &[RecordBatch]| batches[0].column(0).to_data().buffers()[0].as_ptr();
    assert_eq!(pointer(&first), pointer(&second));
    assert_eq!(filled(&session, "__repark_cache_t").await, Some(true));
    let again = frame.collect().await.expect("third");
    assert_eq!(pointer(&first), pointer(&again));
}

#[tokio::test]
async fn the_retained_total_counts_a_filled_table() {
    let session = ReparkSession::new().unwrap();
    let frame = lazy(&session, (None, None)).await;
    assert_eq!(session.retained_cache_bytes().await.unwrap(), 0);
    frame.count().await.expect("fills");
    assert!(session.retained_cache_bytes().await.unwrap() > 0);
}

#[tokio::test]
async fn a_released_table_passes_through_and_keeps_nothing() {
    let session = ReparkSession::new().unwrap();
    let frame = lazy(&session, (None, None)).await;
    assert_eq!(frame.clone().count().await.expect("fills"), 64);
    let source = session.temp_view_ref("src").expect("source ref");
    session
        .context()
        .sql(&format!("INSERT INTO {source} VALUES (64)"))
        .await
        .expect("insert plans")
        .collect()
        .await
        .expect("insert runs");
    assert_eq!(frame.clone().count().await.expect("serves the fill"), 64);
    assert!(
        session
            .release_lazy_cache("__repark_cache_t")
            .await
            .expect("releases")
    );
    assert_eq!(filled(&session, "__repark_cache_t").await, Some(false));
    assert_eq!(frame.clone().count().await.expect("passes through"), 65);
    assert_eq!(filled(&session, "__repark_cache_t").await, Some(false));
    assert_eq!(session.retained_cache_bytes().await.unwrap(), 0);
    assert!(!session.release_lazy_cache("src").await.expect("not lazy"));
}

#[tokio::test]
async fn a_limited_read_fills_the_whole_table() {
    let session = ReparkSession::new().unwrap();
    let frame = lazy(&session, (None, None)).await;
    let first = frame
        .clone()
        .limit(0, Some(1))
        .expect("limit")
        .collect()
        .await
        .expect("limit runs");
    assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(filled(&session, "__repark_cache_t").await, Some(true));
    assert_eq!(frame.count().await.expect("count"), 64);
}

#[tokio::test]
async fn the_session_frees_with_a_filled_table_registered() {
    let session = ReparkSession::new().unwrap();
    let weak = Arc::downgrade(&session.context().state_ref());
    let frame = lazy(&session, (None, None)).await;
    frame.clone().count().await.expect("fills");
    drop(frame);
    drop(session);
    assert!(weak.upgrade().is_none());
}
