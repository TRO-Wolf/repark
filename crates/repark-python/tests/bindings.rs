//! Rust integration tests for the PyO3 bindings and Arrow C stream boundary.

use _native::{PyColumn, PyDataFrame, PyReparkSession};
use arrow::array::{
    Array, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchReader,
    StringArray,
};
use arrow::error::ArrowError;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Build a default session (PySpark `SparkSession.builder.getOrCreate()`).
fn session(py: Python<'_>) -> Py<PyReparkSession> {
    Py::new(
        py,
        PyReparkSession::new(py, None, None, None, None).expect("session builds"),
    )
    .expect("pyclass instantiates")
}

#[test]
fn session_constructs_with_builder_knobs() {
    Python::attach(|py| {
        // Exercise configured and default builder paths.
        let s = PyReparkSession::new(py, Some(2), Some(4096), Some(4), None);
        assert!(s.is_ok(), "session with knobs must build");
        let mem0 = PyReparkSession::new(py, Some(0), None, None, None);
        assert!(mem0.is_ok(), "memory_limit_gb=0 must opt out and build");
        let batch0 = PyReparkSession::new(py, None, Some(0), None, None);
        assert!(batch0.is_err(), "batch_size=0 must refuse");
        let parts0 = PyReparkSession::new(py, None, None, Some(0), None);
        assert!(parts0.is_err(), "target_partitions=0 must refuse");
        let _ = py;
    });
}

#[test]
fn config_driven_memory_catalog_registers_through_the_constructor() {
    use std::collections::HashMap;

    Python::attach(|py| {
        let warehouse = std::env::temp_dir().join(format!("repark_cfg_{}", std::process::id()));
        std::fs::create_dir_all(&warehouse).expect("warehouse dir");
        let config = HashMap::from([
            (
                "spark.sql.catalog.glue_alt".to_string(),
                "org.apache.iceberg.spark.SparkCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.glue_alt.type".to_string(),
                "memory".to_string(),
            ),
            (
                "spark.sql.catalog.glue_alt.warehouse".to_string(),
                warehouse.to_string_lossy().into_owned(),
            ),
            (
                "spark.sql.catalog.glue_alt.io-impl".to_string(),
                "org.apache.iceberg.aws.s3.S3FileIO".to_string(),
            ),
        ]);
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, Some(config))
                .expect("config-driven session builds + registers the catalog"),
        )
        .expect("pyclass instantiates");

        session
            .borrow(py)
            .create_namespace(py, "glue_alt", "silver", None)
            .expect("namespace creates on the config-registered catalog");
        session
            .borrow(py)
            .sql(py, "CREATE TABLE glue_alt.silver.t AS SELECT 1 AS id")
            .expect("CTAS runs against the config-registered catalog");
        assert!(
            session
                .borrow(py)
                .table_exists(py, "glue_alt.silver.t")
                .expect("table_exists probes the catalog"),
        );

        let bad = HashMap::from([("spark.sql.catalog.x.type".to_string(), "hive".to_string())]);
        let err = PyReparkSession::new(py, None, None, None, Some(bad))
            .map(|_| ())
            .unwrap_err();
        assert!(
            err.to_string().contains("spark.sql.catalog.x.type"),
            "{err}"
        );

        let _ = std::fs::remove_dir_all(&warehouse);
    });
}

#[test]
fn sql_select_literals_round_trips_and_counts() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(py, "SELECT 1 AS a, 'x' AS b")
            .expect("sql plans + runs");
        let df_cell = Py::new(py, df).expect("dataframe pyclass");

        assert_eq!(
            df_cell.borrow(py).count(py).expect("count runs"),
            1,
            "SELECT one literal row → count 1"
        );

        let table = df_cell.borrow(py).show(py, 20).expect("show renders");
        assert!(
            table.contains('a') && table.contains('b'),
            "header columns present"
        );
        assert!(
            table.contains('1') && table.contains('x'),
            "literal values present"
        );
    });
}

#[test]
fn arrow_c_stream_exports_a_consumable_stream_with_correct_values() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(py, "SELECT 1 AS a, 'x' AS b")
            .expect("sql plans + runs");
        let df_cell: Py<PyDataFrame> = Py::new(py, df).expect("dataframe pyclass");

        let capsule = df_cell
            .borrow(py)
            .__arrow_c_stream__(py, None)
            .expect("stream capsule produced");

        let name = capsule
            .name()
            .expect("capsule has a name")
            .expect("name is not None");
        // SAFETY: the capsule name remains valid for this comparison.
        let name = unsafe { name.as_cstr() };
        assert_eq!(name.to_str().unwrap(), "arrow_array_stream");

        // Re-import and consume the stream.
        let reader = import_stream(&capsule);
        let batches: Vec<_> = reader.map(|b| b.expect("batch decodes")).collect();
        let total: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(total, 1, "one row crossed the boundary");

        let batch = &batches[0];
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("a is int32");
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("b is utf8");
        assert_eq!(a.value(0), 1);
        assert_eq!(b.value(0), "x");
    });
}

#[test]
fn arrow_c_stream_streams_values_and_types_end_to_end() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(
                py,
                "SELECT * FROM (VALUES (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'c')) \
                 AS t(id, amt, label)",
            )
            .expect("sql plans + runs");
        let df_cell: Py<PyDataFrame> = Py::new(py, df).expect("dataframe pyclass");

        let batch = collect_one_batch(py, &df_cell);
        assert_eq!(
            batch.num_rows(),
            3,
            "all three rows cross the streaming boundary"
        );
        assert_eq!(int32_column(&batch, 0), vec![1, 2, 3], "id values");
        let amt = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("amt is Decimal128");
        assert_eq!(amt.precision(), 2);
        assert_eq!(amt.scale(), 1);
        assert_eq!(
            (0..amt.len()).map(|row| amt.value(row)).collect::<Vec<_>>(),
            vec![15, 25, 35],
            "amt i128 scaled at scale 1"
        );
        let label = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label is Utf8");
        assert_eq!(
            (0..label.len())
                .map(|row| label.value(row))
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"],
            "label values"
        );
    });
}

#[test]
fn arrow_c_stream_defers_execution_and_does_not_collect_up_front() {
    // Export must remain lazy while a full drain reports execution errors.
    Python::attach(|py| {
        let s = Py::new(
            py,
            PyReparkSession::new(py, None, None, Some(1), None).expect("session builds"),
        )
        .expect("pyclass instantiates");
        let df = s
            .borrow(py)
            .sql(
                py,
                "SELECT CAST(CASE WHEN id < 4 THEN CAST(id AS VARCHAR) ELSE 'boom' END AS INT) AS v \
                 FROM (VALUES (1), (2), (3), (4), (5), (6)) AS t(id)",
            )
            .expect("plan builds — the CAST error is deferred to execution, not raised at plan time");
        let df_cell: Py<PyDataFrame> = Py::new(py, df).expect("dataframe pyclass");

        let capsule = df_cell.borrow(py).__arrow_c_stream__(py, None).expect(
            "streaming export returns a capsule WITHOUT materializing (no up-front collect)",
        );

        let reader = import_stream(&capsule);
        let drain: Result<Vec<RecordBatch>, ArrowError> = reader.collect();
        let error = drain.expect_err("a full drain surfaces the deferred CAST execution error");
        let message = error.to_string();
        assert!(
            message.to_lowercase().contains("cast"),
            "the engine CAST error text rides through the Arrow stream: {message}"
        );
    });
}

/// Collect a `PyDataFrame` to a single concatenated `RecordBatch` by driving its Arrow C stream
/// export — the same path pyarrow uses. Panics on any FFI/decode failure (test-only).
fn collect_one_batch(py: Python<'_>, df: &Py<PyDataFrame>) -> RecordBatch {
    let capsule = df
        .borrow(py)
        .__arrow_c_stream__(py, None)
        .expect("stream capsule produced");
    let reader = import_stream(&capsule);
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("batch decodes")).collect();
    arrow::compute::concat_batches(&schema, &batches).expect("batches concat")
}

/// Read an `Int64` column as a `Vec<i64>` (test helper; all values assumed non-null).
fn int64_column(batch: &RecordBatch, index: usize) -> Vec<i64> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column is int64");
    (0..array.len()).map(|row| array.value(row)).collect()
}

#[test]
fn with_column_applies_a_column_expression() {
    Python::attach(|py| {
        let s = session(py);
        // `a` = 10; add `b = a + 5` via the PyColumn operator surface.
        let df = s
            .borrow(py)
            .sql(py, "SELECT 10 AS a")
            .expect("sql plans + runs");
        let a = PyColumn::column("a").expect("col builds");
        let five = PyColumn::literal(&5i64.into_pyobject(py).expect("int to py").into_any())
            .expect("literal builds");
        let sum = a.add(&five).expect("add builds");
        let df = df.with_column("b", sum).expect("with_column");
        let df_cell = Py::new(py, df).expect("dataframe pyclass");
        let batch = collect_one_batch(py, &df_cell);
        assert_eq!(int32_column(&batch, 0), vec![10]);
        assert_eq!(int32_column(&batch, 1), vec![15], "b = a + 5");
    });
}

/// Read an `Int32` column as a `Vec<i32>` (test helper; all values assumed non-null).
fn int32_column(batch: &RecordBatch, index: usize) -> Vec<i32> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("column is int32");
    (0..array.len()).map(|row| array.value(row)).collect()
}

#[test]
fn date_function_year_extracts_the_calendar_year() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(py, "SELECT DATE '2024-03-15' AS d")
            .expect("sql plans + runs");
        // `col("d").year()` wires the repark-functions date shim through the PyColumn surface.
        let year = PyColumn::column("d")
            .expect("col builds")
            .year()
            .expect("year builds");
        let df = df.with_column("y", year).expect("with_column");
        let df_cell = Py::new(py, df).expect("dataframe pyclass");
        let batch = collect_one_batch(py, &df_cell);
        // `y` is Int32 (Spark `year` → IntegerType); column 1 is the added `y`.
        assert_eq!(
            int32_column(&batch, 1),
            vec![2024],
            "year(2024-03-15) = 2024"
        );
    });
}

#[test]
fn row_number_over_window_numbers_rows_in_order() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(py, "SELECT * FROM (VALUES (10), (30), (20)) AS t(v)")
            .expect("sql plans + runs");
        // row_number() OVER (ORDER BY v ASC) — no partition; NULLs-first flag is moot (no nulls).
        let numbered = PyColumn::row_number()
            .expect("row_number builds")
            .over(
                vec![],
                vec![PyColumn::column("v").expect("col builds")],
                vec![true],
                vec![true],
                None,
                None,
                None,
            )
            .expect("over builds the window");
        let df = df.with_column("rn", numbered).expect("with_column");
        let df_cell = Py::new(py, df).expect("dataframe pyclass");
        let batch = collect_one_batch(py, &df_cell);
        // Pair each v with its row number; collection order is not guaranteed, so map then check.
        let values = int32_column(&batch, 0);
        let numbers = int32_column(&batch, 1);
        let mut paired: Vec<(i32, i32)> = values.into_iter().zip(numbers).collect();
        paired.sort_unstable();
        assert_eq!(
            paired,
            vec![(10, 1), (20, 2), (30, 3)],
            "row_number follows the ORDER BY v ascending; result is Int32 (Spark parity)"
        );
    });
}

/// Read a `Float64` column as `Vec<f64>` (test helper; NULLs surface as `NaN`).
fn float64_column(batch: &RecordBatch, index: usize) -> Vec<f64> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("column is float64");
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                f64::NAN
            } else {
                array.value(row)
            }
        })
        .collect()
}

#[test]
fn ta_window_ema_over_matches_the_kernel() {
    Python::attach(|py| {
        let s = session(py);
        // A tiny ordered close series; the DataFrame route must equal the ema kernel on it.
        let df = s
            .borrow(py)
            .sql(
                py,
                "SELECT * FROM (VALUES (0, 2.0), (1, 4.0), (2, 6.0), (3, 8.0), (4, 10.0)) AS t(ts, close)",
            )
            .expect("sql plans + runs");
        let period =
            PyColumn::literal(&3i64.into_pyobject(py).expect("int").into_any()).expect("literal");
        let ema_col = PyColumn::ta_window(
            "ta_ema",
            vec![PyColumn::column("close").expect("col builds"), period],
        )
        .expect("ta_window builds")
        .over(
            vec![],
            vec![PyColumn::column("ts").expect("col builds")],
            vec![true],
            vec![true],
            None,
            None,
            None,
        )
        .expect("over builds");
        let df = df.with_column("ema", ema_col).expect("with_column");
        let df_cell = Py::new(py, df).expect("dataframe pyclass");
        let batch = collect_one_batch(py, &df_cell);
        // Pair (ts, ema) and sort by ts, since collection order is not guaranteed.
        let ts = int32_column(&batch, 0);
        let ema = float64_column(&batch, 2);
        let mut paired: Vec<(i32, f64)> = ts.into_iter().zip(ema).collect();
        paired.sort_by_key(|(t, _)| *t);
        let engine: Vec<f64> = paired.into_iter().map(|(_, e)| e).collect();
        let kernel = repark_ta::ema(&[2.0, 4.0, 6.0, 8.0, 10.0], 3).expect("ema");
        assert_eq!(engine.len(), kernel.len());
        for (a, b) in engine.iter().zip(&kernel) {
            assert!(
                a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
                "DataFrame-route ta_ema must be bit-identical to the kernel"
            );
        }
    });
}

#[test]
fn ta_window_rejects_an_unknown_function() {
    Python::attach(|py| {
        let _ = session(py);
        let result = PyColumn::ta_window(
            "ta_not_real",
            vec![PyColumn::column("v").expect("col builds")],
        );
        assert!(result.is_err(), "unknown TA window function is an error");
    });
}

#[test]
fn over_rejects_a_non_window_column() {
    Python::attach(|py| {
        let _ = session(py);
        // A plain column carries no window function → over() must reject it, not silently no-op.
        let result = PyColumn::column("v").expect("col builds").over(
            vec![],
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
        );
        assert!(result.is_err(), "over() on a non-window column is an error");
    });
}

#[test]
fn filter_column_and_filter_sql_keep_matching_rows() {
    Python::attach(|py| {
        let s = session(py);
        let make_df = || {
            s.borrow(py)
                .sql(py, "SELECT * FROM (VALUES (1), (2), (3)) AS t(a)")
                .expect("sql plans + runs")
        };
        // Column predicate: a > 1.
        let predicate = PyColumn::column("a")
            .expect("col builds")
            .gt(&PyColumn::literal(&1i64.into_pyobject(py).expect("int").into_any()).expect("lit"))
            .expect("gt builds");
        let filtered = make_df().filter(predicate).expect("filter");
        let filtered = Py::new(py, filtered).expect("pyclass");
        let mut values = int32_column(&collect_one_batch(py, &filtered), 0);
        values.sort_unstable();
        assert_eq!(values, vec![2, 3]);

        // SQL-string predicate: a <= 2.
        let filtered_sql = make_df().filter_sql("a <= 2").expect("filter_sql");
        let filtered_sql = Py::new(py, filtered_sql).expect("pyclass");
        let mut values = int32_column(&collect_one_batch(py, &filtered_sql), 0);
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
    });
}

#[test]
fn sort_orders_descending_with_nulls_last() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(py, "SELECT * FROM (VALUES (1), (3), (2)) AS t(a)")
            .expect("sql plans + runs");
        // desc → ascending=false, nulls_first=false (Spark's descending default).
        let sorted = df
            .sort(
                vec![PyColumn::column("a").expect("col builds")],
                vec![false],
                vec![false],
            )
            .expect("sort");
        let sorted = Py::new(py, sorted).expect("pyclass");
        assert_eq!(
            int32_column(&collect_one_batch(py, &sorted), 0),
            vec![3, 2, 1]
        );
    });
}

#[test]
fn join_on_names_merges_the_key_column() {
    Python::attach(|py| {
        let s = session(py);
        let left = s
            .borrow(py)
            .sql(py, "SELECT * FROM (VALUES (1, 100), (2, 200)) AS l(k, lv)")
            .expect("left plans");
        let right = s
            .borrow(py)
            .sql(py, "SELECT * FROM (VALUES (1, 11), (2, 22)) AS r(k, rv)")
            .expect("right plans");
        let right_cell = Py::new(py, right).expect("right pyclass");
        let joined = left
            .join_on_names(right_cell.borrow(py), vec!["k".to_string()], "inner")
            .expect("join");
        let joined = Py::new(py, joined).expect("joined pyclass");
        let batch = collect_one_batch(py, &joined);
        // Spark semantics: a single merged key column (k, lv, rv) — not two `k`s.
        assert_eq!(batch.num_columns(), 3, "join key merged to one column");
        assert_eq!(batch.num_rows(), 2);
    });
}

/// Plan `l(k, lv)` LEFT-JOIN-shaped against `r(k, rv)` with the given `how`, collected to one
/// batch. `l` has keys 1, 2 and a NULL-keyed row; `r` has key 1 and a NULL key — so the pair
/// exercises the match, no-match, and `NULL = NULL is unknown` cases together.
fn semi_family_batch(py: Python<'_>, how: &str) -> RecordBatch {
    let s = session(py);
    let left = s
        .borrow(py)
        .sql(
            py,
            "SELECT * FROM (VALUES (1, 100), (2, 200), (NULL, 300)) AS l(k, lv)",
        )
        .expect("left plans");
    let right = s
        .borrow(py)
        .sql(py, "SELECT * FROM (VALUES (1, 11), (NULL, 99)) AS r(k, rv)")
        .expect("right plans");
    let right_cell = Py::new(py, right).expect("right pyclass");
    let joined = left
        .join_on_names(right_cell.borrow(py), vec!["k".to_string()], how)
        .expect("semi-family join plans");
    let joined = Py::new(py, joined).expect("joined pyclass");
    collect_one_batch(py, &joined)
}

#[test]
fn join_on_names_left_semi_keeps_matching_left_rows_only() {
    Python::attach(|py| {
        let batch = semi_family_batch(py, "leftsemi");
        // Left-only schema: the right side is a filter, not a widening (no `rv`, no second `k`).
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            vec!["k".to_string(), "lv".to_string()],
            "LEFT SEMI output is the left side's columns only"
        );
        // k=1 matches; k=2 has no match; k=NULL never matches (NULL = NULL is unknown), so the
        // right side's own NULL key does NOT pull it in.
        assert_eq!(int32_column(&batch, 0), vec![1]);
        assert_eq!(int32_column(&batch, 1), vec![100]);
    });
}

#[test]
fn join_on_names_left_anti_keeps_unmatched_left_rows_including_null_keys() {
    Python::attach(|py| {
        let batch = semi_family_batch(py, "leftanti");
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            vec!["k".to_string(), "lv".to_string()],
            "LEFT ANTI output is the left side's columns only"
        );
        // k=2 is unmatched; the NULL-keyed left row is KEPT (its key never matched anything) —
        // the exact complement of the semi pin above, so neither arm can be vacuous.
        assert_eq!(batch.num_rows(), 2, "k=2 and the NULL-keyed row survive");
        let key_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("k is int32");
        let mut keys: Vec<Option<i32>> = (0..key_column.len())
            .map(|row| {
                if key_column.is_null(row) {
                    None
                } else {
                    Some(key_column.value(row))
                }
            })
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![None, Some(2)]);
    });
}

#[test]
fn join_on_names_semi_family_never_merges_a_key_column() {
    Python::attach(|py| {
        // The no-key-merge invariant, stated against the inner-join baseline it must NOT share:
        // `inner` merges the duplicate right key into one column and carries `rv` through;
        // the semi family carries neither, on every accepted spelling of the keyword.
        let inner = semi_family_batch(py, "inner");
        assert_eq!(
            inner.num_columns(),
            3,
            "baseline: inner merges the key and keeps `rv` (k, lv, rv)"
        );
        for how in [
            "semi",
            "left_semi",
            "leftsemi",
            "anti",
            "left_anti",
            "leftanti",
        ] {
            let batch = semi_family_batch(py, how);
            assert_eq!(
                batch.num_columns(),
                2,
                "how={how}: semi-family output is exactly the left schema (k, lv)"
            );
            assert_eq!(batch.schema().field(0).name(), "k", "how={how}");
            assert_eq!(batch.schema().field(1).name(), "lv", "how={how}");
        }
    });
}

/// Read a `Utf8` column as `Vec<Option<String>>` (test helper; preserves NULLs).
fn string_column(batch: &RecordBatch, index: usize) -> Vec<Option<String>> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column is utf8");
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                None
            } else {
                Some(array.value(row).to_string())
            }
        })
        .collect()
}

/// A `1i64` literal `PyColumn` (the `count(*)` argument).
fn lit_i64(py: Python<'_>, value: i64) -> PyColumn {
    PyColumn::literal(&value.into_pyobject(py).expect("int to py").into_any()).expect("literal")
}

#[test]
fn aggregate_group_by_sum_names_group_first_then_agg() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(
                py,
                "SELECT * FROM (VALUES (1, 10), (1, 40), (2, 30)) AS t(g, x)",
            )
            .expect("sql plans + runs");
        // groupBy(g).agg(sum(x)) — the facade aliases the aggregate to its Spark name.
        let sum_x = PyColumn::column("x")
            .expect("col")
            .aggregate("sum", false)
            .expect("sum agg")
            .alias("sum(x)")
            .expect("alias");
        let grouped = df
            .aggregate(vec![PyColumn::column("g").expect("col")], vec![sum_x])
            .expect("aggregate");
        let grouped = Py::new(py, grouped).expect("pyclass");
        let batch = collect_one_batch(py, &grouped);
        // Spark parity: the group column comes first, then the aggregate.
        assert_eq!(batch.schema().field(0).name(), "g");
        assert_eq!(batch.schema().field(1).name(), "sum(x)");
        let mut paired: Vec<(i32, i64)> = int32_column(&batch, 0)
            .into_iter()
            .zip(int64_column(&batch, 1))
            .collect();
        paired.sort_unstable();
        assert_eq!(paired, vec![(1, 50), (2, 30)], "sum per group");
    });
}

#[test]
fn aggregate_count_star_counts_rows_count_col_skips_nulls() {
    Python::attach(|py| {
        let s = session(py);
        // Group 1 has a NULL x; count(*) counts the row, count(x) skips it.
        let df = s
            .borrow(py)
            .sql(
                py,
                "SELECT * FROM (VALUES (1, 10), (1, CAST(NULL AS INT)), (2, 30)) AS t(g, x)",
            )
            .expect("sql plans + runs");
        let count_star = PyColumn::count_aggregate(vec![lit_i64(py, 1)], false)
            .expect("count(*)")
            .alias("count(1)")
            .expect("alias");
        let count_x = PyColumn::count_aggregate(vec![PyColumn::column("x").expect("col")], false)
            .expect("count(x)")
            .alias("count(x)")
            .expect("alias");
        let grouped = df
            .aggregate(
                vec![PyColumn::column("g").expect("col")],
                vec![count_star, count_x],
            )
            .expect("aggregate");
        let grouped = Py::new(py, grouped).expect("pyclass");
        let batch = collect_one_batch(py, &grouped);
        // Columns: g, count(1), count(x).
        let g = int32_column(&batch, 0);
        let cstar = int64_column(&batch, 1);
        let ccol = int64_column(&batch, 2);
        let mut rows: Vec<(i32, i64, i64)> = g
            .into_iter()
            .zip(cstar)
            .zip(ccol)
            .map(|((a, b), c)| (a, b, c))
            .collect();
        rows.sort_unstable();
        // Group 1: 2 rows, 1 non-null x. Group 2: 1 row, 1 non-null x.
        assert_eq!(
            rows,
            vec![(1, 2, 1), (2, 1, 1)],
            "count(*) counts rows, count(x) skips NULL"
        );
    });
}

#[test]
fn union_positional_keeps_left_names_by_name_resolves() {
    Python::attach(|py| {
        let s = session(py);
        let left = || {
            s.borrow(py)
                .sql(py, "SELECT 1 AS id, 'a' AS name")
                .expect("left plans")
        };
        // Positional union keeps LEFT's names even though the right spells them differently.
        let right_pos = s
            .borrow(py)
            .sql(py, "SELECT 2 AS xid, 'b' AS xname")
            .expect("right plans");
        let right_pos = Py::new(py, right_pos).expect("pyclass");
        let unioned = left().union(right_pos.borrow(py), false).expect("union");
        let unioned = Py::new(py, unioned).expect("pyclass");
        let batch = collect_one_batch(py, &unioned);
        assert_eq!(batch.schema().field(0).name(), "id", "left names win");
        assert_eq!(batch.schema().field(1).name(), "name");
        let mut ids = int32_column(&batch, 0);
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2], "union is UNION ALL — both rows kept");

        // By-name union resolves columns by name regardless of order.
        let right_name = s
            .borrow(py)
            .sql(py, "SELECT 'c' AS name, 3 AS id")
            .expect("right plans");
        let right_name = Py::new(py, right_name).expect("pyclass");
        let by_name = left()
            .union(right_name.borrow(py), true)
            .expect("union_by_name");
        let by_name = Py::new(py, by_name).expect("pyclass");
        let batch = collect_one_batch(py, &by_name);
        let mut pairs: Vec<(i32, Option<String>)> = int32_column(&batch, 0)
            .into_iter()
            .zip(string_column(&batch, 1))
            .collect();
        pairs.sort_by_key(|(id, _)| *id);
        assert_eq!(
            pairs,
            vec![(1, Some("a".to_string())), (3, Some("c".to_string()))],
            "by-name union pairs id↔name despite the reversed right-side order"
        );
    });
}

#[test]
fn distinct_dedups_and_distinct_on_keeps_one_per_key() {
    Python::attach(|py| {
        let s = session(py);
        // distinct() over all columns.
        let df = s
            .borrow(py)
            .sql(
                py,
                "SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'a')) AS t(k, v)",
            )
            .expect("sql plans");
        let distinct = df.distinct().expect("distinct");
        let distinct = Py::new(py, distinct).expect("pyclass");
        assert_eq!(
            collect_one_batch(py, &distinct).num_rows(),
            2,
            "full-row distinct"
        );

        // distinct_on([k]) — one row per key. Non-key values identical per key → deterministic.
        let df2 = s
            .borrow(py)
            .sql(
                py,
                "SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'b')) AS t(k, v)",
            )
            .expect("sql plans");
        let on_k = df2.distinct_on(vec!["k".to_string()]).expect("distinct_on");
        let on_k = Py::new(py, on_k).expect("pyclass");
        let batch = collect_one_batch(py, &on_k);
        assert_eq!(batch.num_rows(), 2, "one survivor per key");
        let mut keys = int32_column(&batch, 0);
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2], "the surviving key set is {{1, 2}}");
    });
}

#[test]
fn with_column_renamed_renames_present_and_noops_absent() {
    Python::attach(|py| {
        let s = session(py);
        let df = s
            .borrow(py)
            .sql(py, "SELECT 1 AS id, 'a' AS name")
            .expect("sql plans");
        let renamed = df.with_column_renamed("name", "label").expect("rename");
        let renamed = Py::new(py, renamed).expect("pyclass");
        let batch = collect_one_batch(py, &renamed);
        assert_eq!(
            batch.schema().field(1).name(),
            "label",
            "present column renamed"
        );

        // Renaming an absent column is a silent no-op (Spark semantics).
        let df2 = s
            .borrow(py)
            .sql(py, "SELECT 1 AS id, 'a' AS name")
            .expect("sql plans");
        let noop = df2
            .with_column_renamed("nope", "label")
            .expect("rename no-op");
        let noop = Py::new(py, noop).expect("pyclass");
        let batch = collect_one_batch(py, &noop);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(
            batch.schema().field(1).name(),
            "name",
            "absent old name → unchanged"
        );
    });
}

/// Move the `FFI_ArrowArrayStream` out of a `PyCapsule` and wrap it in a reader — exactly the
/// consumer half of the Arrow C stream protocol (what pyarrow/polars do internally).
fn import_stream(capsule: &Bound<'_, PyCapsule>) -> ArrowArrayStreamReader {
    let name = c"arrow_array_stream";
    let ptr = capsule
        .pointer_checked(Some(name))
        .expect("capsule pointer is valid for the arrow stream name")
        .as_ptr()
        .cast::<FFI_ArrowArrayStream>();
    // SAFETY: `__arrow_c_stream__` put a valid, initialized `FFI_ArrowArrayStream` at this
    // pointer; `from_raw` moves it out (nulling the producer's release callback so the capsule
    // destructor is a no-op), and we own the moved value for the reader's lifetime.
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(ptr) };
    ArrowArrayStreamReader::try_new(stream).expect("stream is a valid Arrow C stream")
}

/// Keep an explicit reference to `pyo3::ffi` so the linker pulls in libpython under the
/// non-`extension-module` test build (belt-and-suspenders against dead-code stripping).
#[test]
fn libpython_links() {
    Python::attach(|py| {
        // A trivial FFI call proves the interpreter is live and libpython is linked.
        let v = unsafe { ffi::PyEval_GetFrame() };
        let _ = (v, py);
    });
}
