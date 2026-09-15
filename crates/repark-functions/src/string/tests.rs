use super::*;

use datafusion::arrow::array::StringArray;
use datafusion::prelude::SessionContext;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

/// Full registry path: `datafusion-spark` first, then repark shims (name overwrite wins).
fn ctx_register_all() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

async fn one(ctx: &SessionContext, sql: &str) -> Option<String> {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execute `{sql}`: {error}"));
    let column = batches[0].column(0);
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("expected Utf8 for {sql}, got {:?}", column.data_type()));
    strings.is_valid(0).then(|| strings.value(0).to_string())
}

/// Optional release measurement, enabled with `REPARK_PERF_MEASURE=1`.
#[test]
#[allow(clippy::cast_precision_loss)] // ns/row report only
fn perf_measure_substring_char_indices() {
    if std::env::var_os("REPARK_PERF_MEASURE").as_deref() != Some(std::ffi::OsStr::new("1")) {
        eprintln!("PERF-03 skipped (set REPARK_PERF_MEASURE=1 to run 1M-row measurement)");
        return;
    }
    let rows = 1_000_000usize;
    let sample = "αβγδεζηθικλμνξοπρστυφχψωhello世界";
    let start = std::time::Instant::now();
    let mut sink = 0usize;
    for index in 0..rows {
        let out = spark_substring(sample, 2, Some(8));
        sink ^= out.len().wrapping_add(index);
    }
    let elapsed = start.elapsed();
    let ns_new = elapsed.as_nanos() as f64 / rows as f64;
    eprintln!(
        "PERF-03 substring_char_indices rows={rows} total_ms={:.3} ns_per_row={ns_new:.3} sink={sink}",
        elapsed.as_secs_f64() * 1000.0
    );
    let start_baseline = std::time::Instant::now();
    let mut sink_baseline = 0usize;
    for index in 0..rows {
        let chars: Vec<char> = sample.chars().collect();
        let total = i64::try_from(chars.len()).unwrap_or(i64::MAX);
        let start_index = 2_i64 - 1;
        let end = start_index.saturating_add(8);
        let lower = usize::try_from(start_index.clamp(0, total)).unwrap_or(0);
        let upper = usize::try_from(end.clamp(0, total)).unwrap_or(0);
        let out: String = if lower >= upper {
            String::new()
        } else {
            chars[lower..upper].iter().collect()
        };
        sink_baseline ^= out.len().wrapping_add(index);
    }
    let elapsed_baseline = start_baseline.elapsed();
    let ns_old = elapsed_baseline.as_nanos() as f64 / rows as f64;
    eprintln!(
        "PERF-03 substring_vec_char_baseline rows={rows} total_ms={:.3} ns_per_row={ns_old:.3} sink={sink_baseline}",
        elapsed_baseline.as_secs_f64() * 1000.0
    );
    let _ = (sink, sink_baseline, ns_new, ns_old);
}

#[tokio::test]
async fn substring_spark_edge_positions() {
    let ctx = ctx();
    let cases: &[(&str, &str)] = &[
        ("substr('hello', 0, 3)", "hel"),
        ("substring('hello', -3, 2)", "ll"),
        ("substring('hello', -7, 3)", "h"),
        ("substr('hello', 1, 3)", "hel"),
        ("substring('hello', 2, 3)", "ell"),
        ("substr('hello', 2)", "ello"),
        ("substring('hello', -2)", "lo"),
        ("substr('hello', 9, 3)", ""),
        ("substring('hello', 1, 0)", ""),
        ("substr('hello', 1, -1)", ""),
        ("substring('', 1, 2)", ""),
    ];
    for (call, expected) in cases {
        assert_eq!(
            one(&ctx, &format!("SELECT {call}")).await.as_deref(),
            Some(*expected),
            "{call}"
        );
    }
}

#[tokio::test]
async fn substring_nulls_and_multibyte() {
    let ctx = ctx();
    assert_eq!(
        one(&ctx, "SELECT substr(CAST(NULL AS STRING), 1, 2)").await,
        None
    );
    assert_eq!(one(&ctx, "SELECT substr('ab', NULL, 2)").await, None);
    assert_eq!(
        one(&ctx, "SELECT substring('héllo', 2, 3)")
            .await
            .as_deref(),
        Some("éll")
    );
}

#[tokio::test]
async fn concat_coalesce_null_empty_returns_utf8() {
    let ctx = ctx();
    assert_eq!(
        one(
            &ctx,
            "SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'x')"
        )
        .await
        .as_deref(),
        Some("x")
    );
    assert_eq!(
        one(
            &ctx,
            "SELECT concat(concat(coalesce(CAST(NULL AS VARCHAR), ''), ', '), 'Ann')"
        )
        .await
        .as_deref(),
        Some(", Ann")
    );
}

#[tokio::test]
async fn concat_any_null_propagates() {
    let ctx = ctx();
    assert_eq!(
        one(&ctx, "SELECT concat('a', CAST(NULL AS VARCHAR), 'b')").await,
        None
    );
}

#[tokio::test]
async fn concat_basic_and_zero_arg() {
    let ctx = ctx();
    assert_eq!(
        one(&ctx, "SELECT concat('store', 'A')").await.as_deref(),
        Some("storeA")
    );
    assert_eq!(one(&ctx, "SELECT concat()").await.as_deref(), Some(""));
}

#[tokio::test]
async fn concat_result_physical_type_is_utf8() {
    let ctx = ctx();
    let batches = ctx
        .sql("SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'id') AS id")
        .await
        .expect("plan concat coalesce")
        .collect()
        .await
        .expect("execute concat coalesce");
    assert_eq!(batches[0].column(0).data_type(), &DataType::Utf8);
}

#[tokio::test]
async fn concat_array_any_null_propagates_per_row() {
    let ctx = ctx_register_all();
    let batches = ctx
        .sql(
            "SELECT concat(a, b) AS j FROM (VALUES
                ('x', CAST(NULL AS VARCHAR)),
                ('y', 'z'),
                (CAST(NULL AS VARCHAR), 'w')
            ) AS t(a, b)",
        )
        .await
        .expect("plan array concat")
        .collect()
        .await
        .expect("execute array concat");
    assert_eq!(batches[0].num_rows(), 3);
    let column = batches[0].column(0);
    assert_eq!(column.data_type(), &DataType::Utf8);
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 StringArray");
    assert!(!strings.is_valid(0), "row0 any-NULL must be NULL");
    assert_eq!(strings.value(1), "yz");
    assert!(!strings.is_valid(2), "row2 any-NULL must be NULL");
}

#[tokio::test]
async fn concat_binary_stays_binary() {
    let ctx = ctx_register_all();
    let batches = ctx
        .sql("SELECT concat(unbase64('QQ=='), unbase64('Qg==')) AS v")
        .await
        .expect("plan binary concat")
        .collect()
        .await
        .expect("execute binary concat");
    assert_eq!(batches[0].column(0).data_type(), &DataType::Binary);
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryArray>()
        .expect("BinaryArray");
    assert_eq!(values.value(0), b"AB");
}

#[tokio::test]
async fn concat_register_all_overwrites_datafusion_spark() {
    assert!(
        datafusion_spark::all_default_scalar_functions()
            .iter()
            .any(|udf| udf.name() == "concat"),
        "datafusion-spark must still ship concat for this overwrite pin to mean anything"
    );
    let ctx = ctx_register_all();
    let batches = ctx
        .sql("SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'x') AS id")
        .await
        .expect("plan under register_all")
        .collect()
        .await
        .expect("execute under register_all");
    assert_eq!(batches[0].column(0).data_type(), &DataType::Utf8);
    assert_eq!(
        one(
            &ctx,
            "SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'x')"
        )
        .await
        .as_deref(),
        Some("x")
    );
    assert_eq!(
        one(&ctx, "SELECT concat(1, 2)").await.as_deref(),
        Some("12")
    );
}
