//! SE-1 declared-sorted temp views: plan-shape elision pins + the verification refusal
//! battery, all through the public [`ReparkSession`] API.
//!
//! Plan pins assert the CONTRACT (`SortExec` count 0 with a declaration, ≥1 without);
//! whether DataFusion also plans a `RepartitionExec` is size/config-dependent and
//! deliberately not pinned (at probe scale 1.2M rows it appears and the elision holds
//! through it — recorded in the unit ledger, not asserted here).

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use repark_core::{Error, ReparkSession, TIGHTEN_NULLS_METADATA_KEY, tightened_field_names};

fn schema(nullable_ts: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, nullable_ts),
        Field::new("close", DataType::Float64, false),
    ]))
}

fn batch(symbols: &[&str], ts: &[Option<i64>], nullable_ts: bool) -> RecordBatch {
    let close: Vec<f64> = (0..symbols.len())
        .map(|i| 100.0 + f64::from(u32::try_from(i).unwrap()))
        .collect();
    RecordBatch::try_new(
        schema(nullable_ts),
        vec![
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(Float64Array::from(close)),
        ],
    )
    .unwrap()
}

fn sorted_rows(per_symbol: i64) -> RecordBatch {
    let mut symbols = Vec::new();
    let mut ts = Vec::new();
    for sym in ["AAA", "BBB"] {
        for i in 0..per_symbol {
            symbols.push(sym);
            ts.push(Some(i));
        }
    }
    batch(&symbols, &ts, false)
}

async fn explain_sortexec_count(session: &ReparkSession, table: &str) -> usize {
    let query = format!(
        "EXPLAIN SELECT symbol, ts, sum(close) OVER (PARTITION BY symbol ORDER BY ts) AS s \
         FROM {table}"
    );
    let batches = session
        .context()
        .sql(&query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
        .matches("SortExec")
        .count()
}

fn keys(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| (*n).to_string()).collect()
}

#[tokio::test]
async fn declaration_elides_the_window_sortexec_tp1() {
    let session = ReparkSession::builder()
        .target_partitions(1)
        .build()
        .unwrap();
    let rows = sorted_rows(50_000);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    assert!(
        explain_sortexec_count(&session, "t").await >= 1,
        "control: plain plan sorts"
    );
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    assert_eq!(
        explain_sortexec_count(&session, "t").await,
        0,
        "declared plan must not sort"
    );
}

#[tokio::test]
async fn declaration_elides_at_default_target_partitions() {
    let session = ReparkSession::new().unwrap();
    let rows = sorted_rows(50_000);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    assert!(explain_sortexec_count(&session, "t").await >= 1);
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    assert_eq!(explain_sortexec_count(&session, "t").await, 0);
}

#[tokio::test]
async fn declared_results_are_identical_to_plain_results() {
    let session = ReparkSession::builder()
        .target_partitions(1)
        .build()
        .unwrap();
    let rows = sorted_rows(5_000);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("declared", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("declared", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    let mut rendered = Vec::new();
    for table in ["plain", "declared"] {
        let batches = session
            .context()
            .sql(&format!(
                "SELECT symbol, ts, sum(close) OVER (PARTITION BY symbol ORDER BY ts) AS s \
                 FROM {table} ORDER BY symbol, ts"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        rendered.push(
            arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string(),
        );
    }
    assert_eq!(
        rendered[0], rendered[1],
        "elision must not change any value"
    );
}

#[tokio::test]
async fn unsorted_data_refuses_loud_and_keeps_the_view() {
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA", "AAA"], &[Some(2), Some(1), Some(3)], false);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    let error = session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap_err();
    match &error {
        Error::Analysis(message) => {
            assert!(
                message.contains("rows 0 and 1"),
                "names the offending pair: {message}"
            );
            assert!(message.contains("symbol, ts"), "names the keys: {message}");
        }
        other => panic!("expected Error::Analysis, got {other:?}"),
    }
    // The failed declaration must not have replaced the registration.
    let count = session
        .context()
        .sql("SELECT count(*) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = arrow::util::pretty::pretty_format_batches(&count)
        .unwrap()
        .to_string();
    assert!(
        text.contains('3'),
        "view still queryable after refusal: {text}"
    );
}

#[tokio::test]
async fn cross_batch_violation_is_caught() {
    let session = ReparkSession::new().unwrap();
    let first = batch(&["AAA", "AAA"], &[Some(5), Some(6)], false);
    let second = batch(&["AAA", "AAA"], &[Some(4), Some(7)], false);
    session
        .register_record_batches_as_temp_view("t", first.schema(), vec![first, second])
        .unwrap();
    let error = session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap_err();
    let Error::Analysis(message) = &error else {
        panic!("expected Error::Analysis, got {error:?}")
    };
    assert!(
        message.contains("rows 1 and 2"),
        "boundary pair named: {message}"
    );
}

#[tokio::test]
async fn equal_keys_and_single_row_and_empty_pass() {
    let session = ReparkSession::new().unwrap();
    let dupes = batch(&["AAA", "AAA"], &[Some(1), Some(1)], false);
    session
        .register_record_batches_as_temp_view("dupes", dupes.schema(), vec![dupes])
        .unwrap();
    session
        .declare_temp_view_sorted("dupes", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();

    let single = batch(&["AAA"], &[Some(1)], false);
    session
        .register_record_batches_as_temp_view("single", single.schema(), vec![single])
        .unwrap();
    session
        .declare_temp_view_sorted("single", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();

    session
        .register_record_batches_as_temp_view("empty", schema(false), vec![])
        .unwrap();
    session
        .declare_temp_view_sorted("empty", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn nulls_last_is_the_declared_ordering() {
    let session = ReparkSession::new().unwrap();
    // Nulls at the tail: consistent with ASC NULLS LAST — accepted.
    let tail_nulls = batch(&["AAA", "AAA", "AAA"], &[Some(1), Some(2), None], true);
    session
        .register_record_batches_as_temp_view("ok", tail_nulls.schema(), vec![tail_nulls])
        .unwrap();
    session
        .declare_temp_view_sorted("ok", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    // A null BEFORE a value contradicts NULLS LAST — refused.
    let head_nulls = batch(&["AAA", "AAA"], &[None, Some(1)], true);
    session
        .register_record_batches_as_temp_view("bad", head_nulls.schema(), vec![head_nulls])
        .unwrap();
    let error = session
        .declare_temp_view_sorted("bad", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Analysis(_)), "got {error:?}");
}

#[tokio::test]
async fn unknown_key_and_empty_keys_and_missing_view_refuse() {
    let session = ReparkSession::new().unwrap();
    let rows = sorted_rows(4);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    let unknown = session
        .declare_temp_view_sorted("t", &keys(&["nope"]), false)
        .await
        .unwrap_err();
    let Error::Analysis(message) = &unknown else {
        panic!("expected Error::Analysis, got {unknown:?}")
    };
    assert!(message.contains("'nope'"), "names the key: {message}");

    let empty = session
        .declare_temp_view_sorted("t", &[], false)
        .await
        .unwrap_err();
    assert!(matches!(empty, Error::Analysis(_)));

    let missing = session
        .declare_temp_view_sorted("ghost", &keys(&["symbol"]), false)
        .await
        .unwrap_err();
    let Error::Analysis(message) = &missing else {
        panic!("expected Error::Analysis, got {missing:?}")
    };
    assert!(message.contains("'ghost'"), "names the view: {message}");
}

#[tokio::test]
async fn non_memtable_provider_refuses() {
    let session = ReparkSession::new().unwrap();
    let rows = sorted_rows(4);
    session
        .register_record_batches_as_temp_view("base", rows.schema(), vec![rows])
        .unwrap();
    session
        .context()
        .sql("CREATE VIEW logical_view AS SELECT * FROM base")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let error = session
        .declare_temp_view_sorted("logical_view", &keys(&["symbol"]), false)
        .await
        .unwrap_err();
    let Error::Analysis(message) = &error else {
        panic!("expected Error::Analysis, got {error:?}")
    };
    assert!(
        message.contains("in-memory"),
        "names the constraint: {message}"
    );
}

#[tokio::test]
async fn redeclaration_is_idempotent() {
    let session = ReparkSession::new().unwrap();
    let rows = sorted_rows(1_000);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    let sort_keys = keys(&["symbol", "ts"]);
    session
        .declare_temp_view_sorted("t", &sort_keys, false)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("t", &sort_keys, false)
        .await
        .unwrap();
    assert_eq!(explain_sortexec_count(&session, "t").await, 0);
}

async fn view_schema(session: &ReparkSession, name: &str) -> SchemaRef {
    session
        .context()
        .table_provider(name)
        .await
        .unwrap()
        .schema()
}

#[tokio::test]
async fn tighten_marks_null_free_keys_and_tags_only_flipped_fields() {
    let session = ReparkSession::new().unwrap();
    // `symbol` is already non-nullable; `ts` is nullable and null-free — only `ts` tags.
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let schema = view_schema(&session, "t").await;
    assert!(!schema.field_with_name("symbol").unwrap().is_nullable());
    assert!(!schema.field_with_name("ts").unwrap().is_nullable());
    assert_eq!(tightened_field_names(&schema), vec!["ts".to_string()]);
    assert!(
        !schema
            .field_with_name("symbol")
            .unwrap()
            .metadata()
            .contains_key(TIGHTEN_NULLS_METADATA_KEY),
        "already-non-nullable keys must not be tagged"
    );
}

#[tokio::test]
async fn tighten_refuses_a_null_in_a_declared_key() {
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), None], true);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    let error = session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap_err();
    let Error::Analysis(message) = &error else {
        panic!("expected Error::Analysis, got {error:?}")
    };
    assert!(message.contains("'ts'"), "names the key: {message}");
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
    // Original registration stays queryable and nullable.
    let schema = view_schema(&session, "t").await;
    assert!(schema.field_with_name("ts").unwrap().is_nullable());
    assert!(tightened_field_names(&schema).is_empty());
}

#[tokio::test]
async fn hint_after_tighten_restores_and_tighten_after_hint_retags() {
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    let after_hint = view_schema(&session, "t").await;
    assert!(
        after_hint.field_with_name("ts").unwrap().is_nullable(),
        "hint after tighten restores original nullability"
    );
    assert!(tightened_field_names(&after_hint).is_empty());
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let after_retighten = view_schema(&session, "t").await;
    assert!(!after_retighten.field_with_name("ts").unwrap().is_nullable());
    assert_eq!(
        tightened_field_names(&after_retighten),
        vec!["ts".to_string()]
    );
}

#[tokio::test]
async fn tighten_metadata_survives_select_star() {
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let planned = session.context().sql("SELECT * FROM t").await.unwrap();
    let arrow = planned.schema().as_arrow().clone();
    assert_eq!(
        tightened_field_names(&arrow),
        vec!["ts".to_string()],
        "CTAS derives from this schema — metadata must survive SELECT *"
    );
    repark_core::refuse_iceberg_create_of_tightened_schema(&arrow).unwrap_err();
}

#[tokio::test]
async fn tighten_and_hint_results_are_bit_identical() {
    let session = ReparkSession::builder()
        .target_partitions(1)
        .build()
        .unwrap();
    let rows = batch(
        &["AAA", "AAA", "BBB", "BBB"],
        &[Some(1), Some(2), Some(1), Some(2)],
        true,
    );
    session
        .register_record_batches_as_temp_view("hint", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("hint", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let mut rendered = Vec::new();
    for table in ["hint", "tight"] {
        let batches = session
            .context()
            .sql(&format!(
                "SELECT symbol, ts, sum(close) OVER (PARTITION BY symbol ORDER BY ts) AS s \
                 FROM {table} ORDER BY symbol, ts"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        rendered.push(
            arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string(),
        );
    }
    assert_eq!(
        rendered[0], rendered[1],
        "tighten must not change any value"
    );
}
