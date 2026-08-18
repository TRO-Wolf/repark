//! SE-1 declared-sorted temp views: plan-shape elision pins + the verification refusal
//! battery, all through the public [`ReparkSession`] API.
//!
//! Plan pins assert the CONTRACT (`SortExec` count 0 with a declaration, ≥1 without);
//! whether DataFusion also plans a `RepartitionExec` is size/config-dependent and
//! deliberately not pinned (at probe scale 1.2M rows it appears and the elision holds
//! through it — recorded in the unit ledger, not asserted here).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use repark_core::{
    Error, ReparkSession, TIGHTEN_NULLS_METADATA_KEY, TIGHTEN_NULLS_METADATA_VALUE,
    refuse_iceberg_create_of_tightened_plan, schema_is_tighten_derived,
    strip_tighten_export_metadata, tightened_field_names,
};

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

#[tokio::test]
async fn tighten_preserves_top_level_schema_metadata() {
    let session = ReparkSession::new().unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("close", DataType::Float64, false),
        ],
        HashMap::from([("owner".to_string(), "se1-d1".to_string())]),
    ));
    let rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["AAA", "AAA"])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .unwrap();
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let tightened = view_schema(&session, "t").await;
    assert_eq!(
        tightened.metadata().get("owner").map(String::as_str),
        Some("se1-d1"),
        "tighten must not drop top-level schema metadata"
    );
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), false)
        .await
        .unwrap();
    let restored = view_schema(&session, "t").await;
    assert_eq!(
        restored.metadata().get("owner").map(String::as_str),
        Some("se1-d1"),
        "hint restore must not drop top-level schema metadata"
    );
}

#[tokio::test]
async fn materialize_of_derived_plan_restamps_tighten_provenance() {
    // Kills: cache/persist/checkpoint collect dropping the tighten tag (R-A).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let derived = session
        .context()
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .unwrap();
    assert!(
        tightened_field_names(derived.schema().as_arrow()).is_empty(),
        "computed columns drop field metadata — the materialize stamp is the seam"
    );
    session
        .materialize_dataframe_as_cache_view("cached", derived, None)
        .await
        .unwrap();
    let cached = view_schema(&session, "cached").await;
    assert!(
        schema_is_tighten_derived(&cached),
        "cache remint must carry tighten provenance"
    );
    let planned = session.context().sql("SELECT * FROM cached").await.unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect_err("cached derived scan must still refuse CREATE");
}

#[tokio::test]
async fn subquery_expression_source_is_visible_to_the_create_walk() {
    // Kills: TreeNode::apply (direct inputs only) missing expression-subquery scans (R-B).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let planned = session
        .context()
        .sql("SELECT 1 AS n FROM plain WHERE EXISTS (SELECT 1 FROM tight)")
        .await
        .unwrap();
    let error = refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect_err("EXISTS subquery over a tightened source must refuse");
    let Error::Analysis(message) = error else {
        panic!("expected Analysis, got {error:?}");
    };
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
}

#[tokio::test]
async fn all_nullable_projection_over_tightened_source_is_allowed() {
    // Kills: refusing a CREATE that would persist no required column (R-D allowed side).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let planned = session
        .context()
        .sql("SELECT CAST(NULL AS BIGINT) AS n FROM tight")
        .await
        .unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect("all-nullable output must be allowed");
}

#[tokio::test]
async fn lazy_view_of_derived_plan_is_visible_to_the_create_walk() {
    // Kills: into_view / createOrReplaceTempView hiding a tightened MemTable (Q-001).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let derived = session
        .context()
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .unwrap();
    session
        .create_or_replace_temp_view_from("d", &derived)
        .unwrap();
    let planned = session.context().sql("SELECT * FROM d").await.unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect_err("lazy view hop must still refuse CREATE");
}

#[tokio::test]
async fn tighten_of_already_non_null_keys_stamps_schema_provenance() {
    // Kills: apply_tighten returning untagged when every key was already required (L-004).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], false);
    session
        .register_record_batches_as_temp_view("t", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("t", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let schema = view_schema(&session, "t").await;
    assert!(
        schema_is_tighten_derived(&schema),
        "successful tighten must stamp the provider even when no field flipped"
    );
    assert!(
        tightened_field_names(&schema).is_empty(),
        "already-required keys stay untagged"
    );
}

#[tokio::test]
async fn remint_hint_restore_does_not_leave_required_untagged_fields() {
    // Kills: remint schema-stamp-only + hint declare dropping the stamp (L-001).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let derived = session
        .context()
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .unwrap();
    session
        .materialize_dataframe_as_cache_view("cached", derived, None)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("cached", &keys(&["ts2"]), false)
        .await
        .unwrap();
    let after_hint = view_schema(&session, "cached").await;
    assert!(
        after_hint.field_with_name("ts2").unwrap().is_nullable(),
        "hint restore after remint must not leave a required untagged column"
    );
    let planned = session.context().sql("SELECT * FROM cached").await.unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect("restored remint must not refuse — no required column remains");
}

#[test]
fn nested_non_null_child_is_treated_as_required_output() {
    // Kills: R-D looking only at top-level nullability (L-002).
    let schema = Schema::new_with_metadata(
        vec![Field::new(
            "wrapper",
            DataType::Struct(Fields::from(vec![Field::new("ts", DataType::Int64, false)])),
            true,
        )],
        HashMap::from([(
            TIGHTEN_NULLS_METADATA_KEY.to_string(),
            TIGHTEN_NULLS_METADATA_VALUE.to_string(),
        )]),
    );
    repark_core::refuse_iceberg_create_of_tightened_schema(&schema)
        .expect_err("nullable struct with a required child must refuse");
}

#[tokio::test]
async fn export_strip_drops_tighten_tags_and_keeps_non_nullability() {
    // Kills: `strip_tighten_export_metadata` no longer removing the tag (or removing the
    // non-null lever with it). UNIT SCOPE — this node calls the helper directly.
    // Y-6 (round 4): the old "Kills: leaking repark.tighten_nulls into user-visible
    // to_arrow()/df.schema export" claim is ~~struck~~ — this node never touched either export
    // path. MEASURED: the binding surface the helper actually guards is
    // `PyDataFrame::analyzed_arrow_schema` (`crates/repark-python/src/dataframe.rs`), and with
    // the helper no-oped that capsule reports `{b"repark.tighten_nulls": b"1"}` on both keys.
    // Coverage extended rather than narrowed: the facade node
    // `test_analyzed_schema_export_carries_no_tighten_tag` pins that boundary and is the node
    // the helper mutant kills there. `to_arrow()` is NOT covered by either — DataFusion drops
    // field metadata across physical execution, so the collected schema is already tag-free
    // with both strip layers no-oped (measured).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let internal = view_schema(&session, "tight").await;
    assert!(
        internal
            .field_with_name("ts")
            .unwrap()
            .metadata()
            .contains_key(TIGHTEN_NULLS_METADATA_KEY),
        "internal provider must keep the tag"
    );
    let exported = strip_tighten_export_metadata(internal);
    assert!(
        !exported
            .field_with_name("ts")
            .unwrap()
            .metadata()
            .contains_key(TIGHTEN_NULLS_METADATA_KEY),
        "export must strip the tag"
    );
    assert!(
        !exported.field_with_name("ts").unwrap().is_nullable(),
        "export must keep the non-null lever"
    );
}

#[tokio::test]
async fn filtered_scan_of_a_view_source_exercises_the_get_logical_plan_recurse() {
    // Y-2 (round 4). Kills: deleting the `TableSource::get_logical_plan` recurse in
    // `collect_tighten_sources`.
    //
    // MEASURED on this tree: with the recurse deleted, ALL FOUR existing Q-001 lazy-view pins
    // (this file's `lazy_view_of_derived_plan_is_visible_to_the_create_walk`, the Spark-door
    // and ANSI-door `*_lazy_view_of_derived_plan_refuses`, and the facade
    // `test_sql_derived_write_and_lazy_view_create_refuse`) stayed GREEN — because
    // DataFusion 54.1 `LogicalPlanBuilder::scan` INLINES a source that has a logical plan, so
    // every SQL-door `SELECT * FROM <view>` puts the tightened `MemTable`'s `TableScan`
    // directly in the outer plan and the walk never needs to recurse.
    //
    // The recurse is NOT dead code: `scan` skips the inline when `table_scan.filters` is
    // non-empty (datafusion-expr 54.1 `builder.rs` L518), which leaves a real `TableScan`
    // whose source still carries the view's plan. That is the shape below, built through the
    // public `LogicalPlanBuilder` API against the public `refuse_iceberg_create_of_tightened_plan`
    // entry point. No SQL-door statement reaches it today (measured above); it is the pin that
    // makes the recurse a live branch instead of a belief about one DataFusion release.
    use std::borrow::Cow;

    use datafusion::catalog::TableProvider;
    use datafusion::datasource::ViewTable;
    use datafusion::logical_expr::{LogicalPlanBuilder, TableSource, lit};

    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    let derived = session
        .context()
        .sql("SELECT ts + 1 AS ts2 FROM tight")
        .await
        .unwrap();
    let view = Arc::new(ViewTable::new(derived.logical_plan().clone(), None));
    assert!(
        TableProvider::get_logical_plan(view.as_ref()).is_some(),
        "the ViewTable must carry the inner plan — otherwise this pin proves nothing"
    );
    let source: Arc<dyn TableSource> =
        datafusion::datasource::provider_as_source(Arc::clone(&view) as Arc<dyn TableProvider>);
    // A non-empty filter list is exactly what makes `scan` keep the `TableScan` node instead of
    // inlining the view — assert the shape before asserting the behavior.
    let plan = LogicalPlanBuilder::scan_with_filters("v", source, None, vec![lit(true)])
        .unwrap()
        .build()
        .unwrap();
    let scan_sources_carry_plans = match &plan {
        datafusion::logical_expr::LogicalPlan::TableScan(scan) => {
            matches!(
                scan.source.get_logical_plan(),
                Some(Cow::Borrowed(_) | Cow::Owned(_))
            )
        }
        other => panic!("expected a retained TableScan, got {other:?}"),
    };
    assert!(
        scan_sources_carry_plans,
        "the retained TableScan's source must still carry the view plan"
    );
    let error = refuse_iceberg_create_of_tightened_plan(&plan)
        .expect_err("a filtered scan of a view over a tightened source must refuse");
    let Error::Analysis(message) = error else {
        panic!("expected Analysis, got {error:?}");
    };
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
}

#[test]
fn list_and_map_child_requiredness_is_seen_by_the_r_d_output_walk() {
    // Y-8 (round 4) — verifier P-5, previously NOT-RUN. Question: does
    // `field_or_child_is_non_nullable` see a REQUIRED child inside a List / Map element field?
    // MEASURED here, through the public schema entry point (the helper is crate-private):
    //
    //   shape                                                   verdict
    //   ------------------------------------------------------  --------
    //   List<item: Int64 NOT NULL>            (outer nullable)   refuses
    //   LargeList<item: Int64 NOT NULL>       (outer nullable)   refuses
    //   FixedSizeList<item: Int64 NOT NULL,2> (outer nullable)   refuses
    //   Map<key NOT NULL, value: Int64 NOT NULL> (outer nullable) refuses
    //   Map<key NOT NULL, value: Int64 NULL>     (outer nullable) ALLOWED
    //   List<item: Int64 NULL>                (outer nullable)   ALLOWED
    //
    // Kills: dropping the List/LargeList/FixedSizeList or the Map arm of
    // `field_or_child_is_non_nullable` (measured: deleting either arm turns the matching rows
    // red). The Map row's accepted scope is deliberate and pinned by the two Map rows together
    // — Iceberg map KEYS are spec-required, so only a required VALUE persists a nested required
    // field; a `Map<key NOT NULL, value NULL>` column must stay allowed.
    fn tighten_derived(field: Field) -> Schema {
        Schema::new_with_metadata(
            vec![field],
            HashMap::from([(
                TIGHTEN_NULLS_METADATA_KEY.to_string(),
                TIGHTEN_NULLS_METADATA_VALUE.to_string(),
            )]),
        )
    }
    fn map_field(value_nullable: bool) -> Field {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, value_nullable),
            ])),
            false,
        );
        Field::new("m", DataType::Map(Arc::new(entries), false), true)
    }
    let required_item = Arc::new(Field::new("item", DataType::Int64, false));
    let nullable_item = Arc::new(Field::new("item", DataType::Int64, true));

    for (label, field) in [
        (
            "List<required>",
            Field::new("l", DataType::List(Arc::clone(&required_item)), true),
        ),
        (
            "LargeList<required>",
            Field::new("l", DataType::LargeList(Arc::clone(&required_item)), true),
        ),
        (
            "FixedSizeList<required>",
            Field::new(
                "l",
                DataType::FixedSizeList(Arc::clone(&required_item), 2),
                true,
            ),
        ),
        ("Map<value required>", map_field(false)),
    ] {
        repark_core::refuse_iceberg_create_of_tightened_schema(&tighten_derived(field)).expect_err(
            &format!("{label} must refuse — it persists a required child"),
        );
    }
    for (label, field) in [
        (
            "List<nullable>",
            Field::new("l", DataType::List(Arc::clone(&nullable_item)), true),
        ),
        ("Map<value nullable>", map_field(true)),
    ] {
        repark_core::refuse_iceberg_create_of_tightened_schema(&tighten_derived(field))
            .unwrap_or_else(|error| {
                panic!("{label} persists no required child and must be allowed: {error:?}")
            });
    }
}
