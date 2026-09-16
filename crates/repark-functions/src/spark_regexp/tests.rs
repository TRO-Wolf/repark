use super::*;

use datafusion::arrow::array::AsArray;
use datafusion::prelude::SessionContext;

use crate::spark_regex_engine::compile_spark_regex;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_count_udf().as_ref().clone());
    ctx.register_udf(regexp_instr_udf().as_ref().clone());
    ctx
}

fn ctx_register_all() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    ctx
}

async fn one_i32(ctx: &SessionContext, sql: &str) -> Option<i32> {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
    let array = batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    }
}

async fn one_str(ctx: &SessionContext, sql: &str) -> Option<String> {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
    let array = batches[0].column(0).as_string::<i32>();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0).to_owned())
    }
}

#[tokio::test]
async fn regexp_count_null_in_null_out() {
    let ctx = ctx();
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count(CAST(NULL AS VARCHAR), 'ab')").await,
        None
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count('ababab', CAST(NULL AS VARCHAR))").await,
        None
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count('ababab', 'ab')").await,
        Some(3)
    );
}

#[tokio::test]
async fn regexp_instr_ignores_idx_value() {
    let ctx = ctx();
    // Discriminator: group-index would be 3; Spark (and we) return match start 2.
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 1)").await,
        Some(2)
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 0)").await,
        Some(2)
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 3)").await,
        Some(2)
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 99)").await,
        Some(2)
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d')").await,
        Some(2)
    );
    assert_eq!(
        one_i32(
            &ctx,
            "SELECT regexp_instr('abcde', 'b(c)d', CAST(NULL AS INT))"
        )
        .await,
        None
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'zzz')").await,
        Some(0)
    );
}

#[tokio::test]
async fn regexp_instr_is_character_not_byte() {
    let ctx = ctx();
    // 🐈 is one scalar / two UTF-16 units; Spark Matcher.start()+1 is 3.
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('🐈ab', 'ab')").await,
        Some(3)
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('caféx', 'x')").await,
        Some(5)
    );
    // Empty pattern: UTF-16 boundaries (`🐈` is 2 units → count 3).
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count('🐈', '')").await,
        Some(3)
    );
    // Java `\d` is ASCII; ARABIC-INDIC DIGIT THREE must not count.
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count('٣', '\\d')").await,
        Some(0)
    );
}

#[tokio::test]
async fn empty_pattern_matches_spark() {
    let ctx = ctx();
    // Spark: regexp_count('aaa','') = 4 (zero-width at each boundary).
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count('aaa', '')").await,
        Some(4)
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('aaa', '')").await,
        Some(1)
    );
}

#[tokio::test]
async fn overlapping_count_is_non_overlapping() {
    let ctx = ctx();
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count('aaa', 'aa')").await,
        Some(1)
    );
}

#[tokio::test]
async fn register_all_overwrites_datafusion() {
    let ctx = ctx_register_all();
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_count(CAST(NULL AS VARCHAR), 'ab')").await,
        None
    );
    assert_eq!(
        one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 99)").await,
        Some(2)
    );
}

#[test]
fn java_find_loop_matches_spark_zero_width() {
    let digits = compile_spark_regex("[0-9]*", "regexp_count").expect("digits");
    assert_eq!(digits.count_non_overlapping("2026-08-19").expect("c"), 6);
    let stars = compile_spark_regex("b*", "regexp_count").expect("b*");
    assert_eq!(stars.count_non_overlapping("abc").expect("c"), 4);
    let a_star = compile_spark_regex("a*", "regexp_count").expect("a*");
    assert_eq!(a_star.count_non_overlapping("🐈").expect("c"), 3);
    // R4-1: empty `is_match` overcounts start-anchored patterns at a mid-surrogate index.
    let caret = compile_spark_regex("^", "regexp_count").expect("caret");
    assert!(!caret.matches_at_mid_surrogate_index().expect("probe"));
    assert_eq!(caret.count_non_overlapping("🐈").expect("c"), 1);
    let caret_digits = compile_spark_regex(r"^\d*", "regexp_count").expect("caret digits");
    assert_eq!(caret_digits.count_non_overlapping("🐈2026").expect("c"), 1);
    let multiline_caret = compile_spark_regex("(?m)^", "regexp_count").expect("multiline caret");
    assert!(
        !multiline_caret
            .matches_at_mid_surrogate_index()
            .expect("probe")
    );
    assert_eq!(
        multiline_caret.count_non_overlapping("🐈\n🐈").expect("c"),
        2
    );
    assert!(a_star.matches_at_mid_surrogate_index().expect("probe"));
}

#[tokio::test]
async fn dictionary_utf8_column_is_accepted() {
    use datafusion::arrow::array::{DictionaryArray, Int8Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Int8Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    let ctx = ctx();
    let values = StringArray::from(vec!["ababab", "xy"]);
    let keys = Int8Array::from(vec![0_i8, 1, 0]);
    let dict = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).expect("dictionary");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        dict.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).expect("batch");
    ctx.register_batch("dict_strings", batch).expect("register");
    let batches = ctx
        .sql("SELECT regexp_count(s, 'ab') AS c FROM dict_strings")
        .await
        .expect("plan dict")
        .collect()
        .await
        .expect("exec dict");
    let array = batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    assert_eq!(array.value(0), 3);
    assert_eq!(array.value(1), 0);
    assert_eq!(array.value(2), 3);
}

#[tokio::test]
async fn regexp_extract_group_default_and_whole_match() {
    let ctx = ctx_register_all();
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 1)"
        )
        .await,
        Some("100".to_owned())
    );
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 2)"
        )
        .await,
        Some("200".to_owned())
    );
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 0)"
        )
        .await,
        Some("100-200".to_owned())
    );
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)')"
        )
        .await,
        Some("100".to_owned())
    );
    assert_eq!(
        one_str(&ctx, "SELECT regexp_extract('ac', '(a)(b)?', 2)").await,
        Some(String::new())
    );
}

#[tokio::test]
async fn regexp_extract_no_match_is_empty_null_in_null_out() {
    let ctx = ctx_register_all();
    assert_eq!(
        one_str(&ctx, "SELECT regexp_extract('abc', '([0-9]+)', 1)").await,
        Some(String::new())
    );
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract(CAST(NULL AS VARCHAR), '([0-9]+)', 1)"
        )
        .await,
        None
    );
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract('abc', CAST(NULL AS VARCHAR), 1)"
        )
        .await,
        None
    );
    assert_eq!(
        one_str(
            &ctx,
            "SELECT regexp_extract('abc', '([0-9]+)', CAST(NULL AS INT))"
        )
        .await,
        None
    );
}

#[tokio::test]
async fn regexp_extract_bad_group_names_extract() {
    let ctx = ctx_register_all();
    for idx in [3, -1] {
        let query = format!("SELECT regexp_extract('a-b', '(a)-(b)', {idx})");
        let result = ctx.sql(&query).await.expect("plan").collect().await;
        let message = format!("{result:?}");
        assert!(result.is_err(), "idx {idx} must raise; got {message}");
        assert!(
            message.contains("`regexp_extract` is invalid") && message.contains("between 0 and 2"),
            "Spark REGEX_GROUP_INDEX shape; got {message}"
        );
    }
}

#[tokio::test]
async fn regexp_extract_java_union_and_unicode_class() {
    let ctx = ctx_register_all();
    assert_eq!(
        one_str(&ctx, "SELECT regexp_extract('alpha', '([[:alpha:]]+)', 1)").await,
        Some("alpha".to_owned())
    );
    assert_eq!(
        one_str(&ctx, "SELECT regexp_extract('fox', '([[:alpha:]]+)', 1)").await,
        Some(String::new())
    );
    assert_eq!(
        one_str(&ctx, "SELECT regexp_extract('alpha', '(\\p{L}+)', 1)").await,
        Some("alpha".to_owned())
    );
}

#[tokio::test]
async fn malformed_string_idx_is_fail_loud() {
    let ctx = ctx();
    let planned = ctx
        .sql("SELECT regexp_instr('abcde', 'b(c)d', 'i')")
        .await
        .expect("plan");
    let result = planned.collect().await;
    assert!(
        result.is_err(),
        "Spark CAST('i' AS INT) is fail-loud; got {result:?}"
    );
}
