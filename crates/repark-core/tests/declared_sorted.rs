//! SE-1 declared-sorted temp views: plan-shape elision pins + the verification refusal
//! battery, all through the public [`ReparkSession`] API.
//!
//! Plan pins assert the CONTRACT (`SortExec` count 0 with a declaration, ≥1 without);
//! whether DataFusion also plans a `RepartitionExec` is size/config-dependent and
//! deliberately not pinned because its presence depends on size and configuration.

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
    // Kills: removing `repark.tighten_nulls` from the internal schema or the non-null lever.
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
    // Y-2. Kills deleting the `TableSource::get_logical_plan` recurse. A filtered `TableScan`
    // retains the source plan, so this public shape keeps that branch live.
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
    // Y-8 / P-5. Kills dropping List/LargeList/FixedSizeList or Map handling in
    // `field_or_child_is_non_nullable`; required children refuse, nullable values remain allowed.
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

#[tokio::test]
async fn wide_lazy_view_union_without_tighten_is_not_refused() {
    // Kills: a 64-visit "depth" cap treating width as depth and refusing an
    // innocent CREATE with a tightenNulls message (C1-Q-001).
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA"], &[Some(1)], true);
    let mut parts: Vec<String> = Vec::new();
    for index in 0..65 {
        let source = format!("plain_{index}");
        let view = format!("lazy_{index}");
        session
            .register_record_batches_as_temp_view(&source, rows.schema(), vec![rows.clone()])
            .unwrap();
        let derived = session
            .context()
            .sql(&format!("SELECT * FROM {source}"))
            .await
            .unwrap();
        session
            .create_or_replace_temp_view_from(&view, &derived)
            .unwrap();
        parts.push(format!("SELECT * FROM {view}"));
    }
    let planned = session
        .context()
        .sql(&parts.join(" UNION ALL "))
        .await
        .unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan()).expect(
        "wide non-tighten lazy-view UNION must not refuse and must not mention tightenNulls",
    );
}

#[tokio::test]
async fn remint_hint_unflips_name_colliding_computed_column() {
    // Kills: skip-by-name leaving `ts + 1 AS symbol` required+untagged after
    // hint restore so CREATE persists a tighten-propagated required column
    // (C2-Q-001). Originally-required columns may also widen — conservative.
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
        .sql("SELECT ts + 1 AS symbol FROM tight")
        .await
        .unwrap();
    session
        .materialize_dataframe_as_cache_view("cached", derived, None)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("cached", &keys(&["symbol"]), false)
        .await
        .unwrap();
    let after_hint = view_schema(&session, "cached").await;
    assert!(
        after_hint.field_with_name("symbol").unwrap().is_nullable(),
        "computed column aliased onto an already-required name must unflip"
    );
    let planned = session.context().sql("SELECT * FROM cached").await.unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect("unflipped colliding remint must not refuse");
}

#[tokio::test]
async fn remint_hint_restore_unflips_nested_required_child() {
    // Kills: remint tagging only top-level fields so hint restore leaves a
    // nested required child untagged and CREATE persists it (C1-L-001).
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
        .sql("SELECT ts + 1 AS ts2, named_struct('k', ts + 1) AS wrapper FROM tight")
        .await
        .unwrap_or_else(|_| panic!("named_struct must parse so the nested remint pin can run"));
    session
        .materialize_dataframe_as_cache_view("cached", derived, None)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("cached", &keys(&["ts2"]), false)
        .await
        .unwrap();
    let after_hint = view_schema(&session, "cached").await;
    let wrapper = after_hint.field_with_name("wrapper").unwrap();
    let DataType::Struct(children) = wrapper.data_type() else {
        panic!("wrapper must stay a struct, got {:?}", wrapper.data_type());
    };
    assert!(
        children
            .iter()
            .all(|child| child.is_nullable() || child.name() != "k"),
        "nested reminted required child must unflip on hint restore"
    );
    let planned = session.context().sql("SELECT * FROM cached").await.unwrap();
    refuse_iceberg_create_of_tightened_plan(planned.logical_plan())
        .expect("restored remint with unflipped nested child must not refuse");
}

#[test]
fn export_strip_does_not_stack_overflow_on_deep_struct() {
    // Kills: remint/restore/strip recursing without a depth cap (C2-SAF-001).
    let mut data_type = DataType::Int64;
    for _ in 0..40 {
        data_type = DataType::Struct(Fields::from(vec![Field::new("f", data_type, true)]));
    }
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("wrapper", data_type, true)],
        HashMap::from([(
            TIGHTEN_NULLS_METADATA_KEY.to_string(),
            TIGHTEN_NULLS_METADATA_VALUE.to_string(),
        )]),
    ));
    let exported = strip_tighten_export_metadata(schema);
    assert!(
        !exported.metadata().contains_key(TIGHTEN_NULLS_METADATA_KEY),
        "deep-struct export must still drop the schema stamp"
    );
}

// ===========================================================================================
// Native-door pins cover resolved-catalog refusal, visit budgets, and nested export stripping.
// ===========================================================================================

/// An Iceberg-catalog session on the native door with a tightened temp view `tight` and an
/// untightened `plain`, plus namespace `ice.sales`.
async fn native_ddl_sink_session() -> (tempfile::TempDir, ReparkSession) {
    let warehouse_dir = tempfile::TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = ReparkSession::new().unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
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
    (warehouse_dir, session)
}

#[tokio::test]
async fn native_door_ddl_sink_over_tightened_source_refuses() {
    // Z-2. Kills bypassing `PreExecute::guard` in the native door; tightened Iceberg DDL must
    // refuse before publication.
    let (_dir, session) = native_ddl_sink_session().await;
    for (sql, target) in [
        (
            "CREATE VIEW ice.sales.v_limit AS SELECT * FROM tight LIMIT 0",
            "ice.sales.v_limit",
        ),
        (
            "CREATE VIEW ice.sales.v_false AS SELECT * FROM tight WHERE false",
            "ice.sales.v_false",
        ),
        (
            "SELECT * INTO ice.sales.t_limit FROM tight LIMIT 0",
            "ice.sales.t_limit",
        ),
        (
            "SELECT * INTO ice.sales.t_false FROM tight WHERE false",
            "ice.sales.t_false",
        ),
    ] {
        let error = session
            .sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse on the native door"));
        assert!(
            error.to_string().contains("tightenNulls"),
            "names the flag for `{sql}`: {error}"
        );
        assert!(
            !session.table_exists(target).await.unwrap(),
            "`{sql}` refused but `{target}` was persisted anyway (R6-4 unpublished half)"
        );
    }
}

#[tokio::test]
async fn native_door_session_scoped_and_untightened_ddl_stay_allowed() {
    // Z-2 allowed side. Kills: the belt turning into a blanket DDL refuse — a session-scoped
    // name persists nothing, and an untightened source into the Iceberg catalog is not this
    // rule's business. Both halves must stay green with the guard installed.
    let (_dir, session) = native_ddl_sink_session().await;
    // Iceberg-catalog fixtures use `LIMIT 0` so registration remains a valid test fixture.
    for sql in [
        "CREATE VIEW session_v AS SELECT * FROM tight",
        "SELECT * INTO session_t FROM tight",
        "CREATE VIEW ice.sales.v_plain AS SELECT * FROM plain LIMIT 0",
        "SELECT * INTO ice.sales.t_plain FROM plain LIMIT 0",
    ] {
        session
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("`{sql}` must stay allowed: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("`{sql}` must collect: {error}"));
    }
}

#[tokio::test]
async fn native_door_default_catalog_bare_name_ddl_over_tightened_source_refuses() {
    // Z-1. Kills gating DDL refusal on `TableReference::Full` spelling; bare and partial names
    // resolve through the configured catalog and must refuse with no published table.
    let (_dir, session) = native_ddl_sink_session().await;
    session
        .sql("SET datafusion.catalog.default_catalog = 'ice'")
        .await
        .expect("SET default_catalog");
    session
        .sql("SET datafusion.catalog.default_schema = 'sales'")
        .await
        .expect("SET default_schema");
    for (sql, resolved) in [
        (
            "CREATE VIEW v_bare AS SELECT * FROM datafusion.public.tight LIMIT 0",
            "ice.sales.v_bare",
        ),
        (
            "SELECT * INTO t_bare FROM datafusion.public.tight LIMIT 0",
            "ice.sales.t_bare",
        ),
        // Two-part (Partial) spelling: catalog still comes from the session default.
        (
            "CREATE VIEW sales.v_partial AS SELECT * FROM datafusion.public.tight LIMIT 0",
            "ice.sales.v_partial",
        ),
        (
            "SELECT * INTO sales.t_partial FROM datafusion.public.tight LIMIT 0",
            "ice.sales.t_partial",
        ),
        // Three-part spelling exercises the same resolved-catalog guard.
        (
            "CREATE VIEW ice.sales.v_full AS SELECT * FROM datafusion.public.tight LIMIT 0",
            "ice.sales.v_full",
        ),
    ] {
        let error = session
            .sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse — it resolves into `ice`"));
        assert!(
            error.to_string().contains("tightenNulls"),
            "names the flag for `{sql}`: {error}"
        );
        assert!(
            !session.table_exists(resolved).await.unwrap(),
            "`{sql}` refused but `{resolved}` was persisted anyway (R6-4 unpublished half)"
        );
    }
}

#[tokio::test]
async fn default_catalog_pointing_away_from_iceberg_keeps_session_ddl_allowed() {
    // Z-1 allowed side. Kills: "any Bare name refuses" — the gate is the RESOLVED catalog, so
    // with the default catalog left at `datafusion` (not a registered Iceberg catalog) the
    // session-scoped DDL that the lazy-view pins depend on must keep working.
    let (_dir, session) = native_ddl_sink_session().await;
    session
        .sql("CREATE VIEW still_session_v AS SELECT * FROM tight")
        .await
        .expect("default catalog is not an Iceberg catalog — must stay allowed")
        .collect()
        .await
        .expect("collect");
}

/// Build a chain of `hops` retained `TableScan`s, each over a `ViewTable` wrapping the previous
/// plan (Z-6 helper). A plain filter is what keeps DataFusion 54.1's `scan` from inlining the
/// view — the same shape the Y-2 recurse pin above uses — so each hop costs the walk exactly
/// one inner-plan visit.
fn view_hop_chain(
    base: datafusion::logical_expr::LogicalPlan,
    hops: usize,
) -> datafusion::logical_expr::LogicalPlan {
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::ViewTable;
    use datafusion::logical_expr::{LogicalPlanBuilder, TableSource, lit};

    let mut plan = base;
    for hop in 0..hops {
        let view = Arc::new(ViewTable::new(plan, None));
        let source: Arc<dyn TableSource> =
            datafusion::datasource::provider_as_source(view as Arc<dyn TableProvider>);
        plan = LogicalPlanBuilder::scan_with_filters(
            format!("hop{hop}"),
            source,
            None,
            vec![lit(true)],
        )
        .unwrap()
        .build()
        .unwrap();
    }
    plan
}

#[tokio::test]
async fn view_visit_budget_overflow_is_a_generic_error_not_a_tighten_refusal() {
    // Z-6. Kills an unpinned `MAX_VIEW_VISITS` overflow arm and wrong blame for a non-tighten
    // plan. The retained filtered `TableScan` shape reaches the bounded walk.
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows])
        .unwrap();
    let base = session
        .context()
        .sql("SELECT * FROM plain")
        .await
        .unwrap()
        .logical_plan()
        .clone();
    let plan = view_hop_chain(base, 4100);
    let error = refuse_iceberg_create_of_tightened_plan(&plan)
        .expect_err("a plan past the view-visit budget must fail loud, not silently pass");
    let message = error.to_string();
    assert!(
        message.contains("view-visit budget"),
        "overflow names the budget: {message}"
    );
    assert!(
        !message.contains("tightenNulls"),
        "an untightened deep plan must never be blamed on tightenNulls: {message}"
    );
}

#[tokio::test]
async fn view_hop_chain_under_the_visit_budget_still_walks_clean() {
    // Z-6 allowed side. Kills: a budget so tight that an ordinary retained-scan view chain
    // overflows (the overflow arm is a safety net for cyclic/hostile graphs, not a feature),
    // and a budget counted so loosely that 64 hops already trip it.
    let session = ReparkSession::new().unwrap();
    let rows = batch(&["AAA", "AAA"], &[Some(1), Some(2)], true);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows])
        .unwrap();
    let base = session
        .context()
        .sql("SELECT * FROM plain")
        .await
        .unwrap()
        .logical_plan()
        .clone();
    let plan = view_hop_chain(base, 64);
    refuse_iceberg_create_of_tightened_plan(&plan)
        .expect("a 64-hop untightened view chain must walk clean");
}

#[test]
fn nested_export_strip_covers_every_container_the_tagger_walks() {
    // Z-7. Kills: the export strip missing a nested Arrow container the DETECTOR still sees —
    // that asymmetry leaks the internal `repark.tighten_nulls` key to a user-visible schema.
    // Measured scope: the tagger, the detector, and the strip walk exactly Struct, List,
    // LargeList, FixedSizeList, and the Map VALUE; every other container (Union, Dictionary,
    // RunEndEncoded, the *View list types) is walked by NONE of the three, so nothing can be
    // tagged inside one — recorded, not silently assumed.
    let tagged = |name: &str, nullable: bool| {
        Field::new(name, DataType::Int64, nullable).with_metadata(HashMap::from([(
            TIGHTEN_NULLS_METADATA_KEY.to_string(),
            TIGHTEN_NULLS_METADATA_VALUE.to_string(),
        )]))
    };
    let map_entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            tagged("value", false),
        ])),
        false,
    );
    let containers = vec![
        Field::new(
            "fixed",
            DataType::FixedSizeList(Arc::new(tagged("item", false)), 3),
            true,
        ),
        Field::new(
            "list",
            DataType::List(Arc::new(tagged("item", false))),
            true,
        ),
        Field::new(
            "large",
            DataType::LargeList(Arc::new(tagged("item", false))),
            true,
        ),
        Field::new(
            "strct",
            DataType::Struct(Fields::from(vec![tagged("child", false)])),
            true,
        ),
        Field::new("map", DataType::Map(Arc::new(map_entries), false), true),
    ];
    for field in containers {
        let name = field.name().clone();
        let schema: SchemaRef = Arc::new(Schema::new(vec![field]));
        assert!(
            schema_is_tighten_derived(&schema),
            "`{name}`: the detector must see the nested tag"
        );
        let exported = strip_tighten_export_metadata(Arc::clone(&schema));
        assert!(
            !schema_is_tighten_derived(&exported),
            "`{name}`: the export strip must remove every nested tag the detector sees"
        );
        assert_eq!(
            nested_required_leaf_count(&exported),
            nested_required_leaf_count(&schema),
            "`{name}`: strip drops the tag only — nullability is untouched"
        );
    }
}

/// Count non-nullable leaves under a schema's nested containers (Z-7 helper): the strip must
/// change metadata only, never requiredness.
fn nested_required_leaf_count(schema: &Schema) -> usize {
    fn count(data_type: &DataType) -> usize {
        match data_type {
            DataType::Struct(fields) => fields
                .iter()
                .map(|child| usize::from(!child.is_nullable()) + count(child.data_type()))
                .sum(),
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _)
            | DataType::Map(inner, _) => {
                usize::from(!inner.is_nullable()) + count(inner.data_type())
            }
            _ => 0,
        }
    }
    schema
        .fields()
        .iter()
        .map(|field| usize::from(!field.is_nullable()) + count(field.data_type()))
        .sum()
}
