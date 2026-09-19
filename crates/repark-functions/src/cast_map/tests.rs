use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionConfig;

use super::*;
use crate::ansi::with_spark_ansi_config;

fn context(ansi: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(with_spark_ansi_config(SessionConfig::new(), ansi));
    register(&ctx);
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let rewritten = rewrite_map_casts(sql).unwrap_or_else(|| sql.to_owned());
    ctx.sql(&rewritten).await?.collect().await
}

fn first_cell(batches: &[RecordBatch]) -> String {
    array_value_to_string(batches[0].column(0).as_ref(), 0).unwrap()
}

fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", key, false),
                    Field::new("value", value, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

#[test]
fn map_target_parses_any_case_spacing_and_nesting() {
    let flat = map_type(DataType::Utf8, DataType::Int64);
    for text in [
        "MAP<STRING, BIGINT>",
        "map<string,bigint>",
        "Map < String , Long >",
    ] {
        assert_eq!(map_cast_target(text), Some(flat.clone()), "{text}");
    }
    let nested = map_cast_target("MAP<STRING, MAP<STRING, ARRAY<INT>>>").unwrap();
    assert_eq!(
        spark_sql_name(&nested),
        "MAP<STRING, MAP<STRING, ARRAY<INT>>>"
    );
    let structured = map_cast_target("STRUCT<m: MAP<STRING, DECIMAL(10,2)>>").unwrap();
    assert_eq!(
        spark_sql_name(&structured),
        "STRUCT<m: MAP<STRING, DECIMAL(10,2)>>"
    );
    assert_eq!(
        map_cast_token("array<map<string,long>>").as_deref(),
        Some("ARRAY<MAP<STRING, BIGINT>>")
    );
}

#[test]
fn non_map_or_malformed_targets_are_not_map_casts() {
    for text in [
        "ARRAY<INT>",
        "STRUCT<a: INT>",
        "INT",
        "MAP<STRING>",
        "MAP<STRING, INT",
        "MAP<STRING, INT> extra",
        "MAP<STRING, NOPE>",
        "MAP<STRING, DECIMAL(40,2)>",
    ] {
        assert_eq!(map_cast_target(text), None, "{text}");
    }
}

#[test]
fn rewrite_leaves_sql_without_a_map_cast_untouched() {
    for sql in [
        "SELECT 1",
        "SELECT CAST(a AS INT) FROM t",
        "SELECT map('a', 1) AS m",
        "SELECT 'CAST(x AS MAP<STRING, INT>)' AS s",
        "SELECT 1 -- CAST(x AS MAP<STRING, INT>)",
        "SELECT CAST(NULL AS ARRAY<INT>) AS a",
    ] {
        assert_eq!(rewrite_map_casts(sql), None, "{sql}");
    }
}

#[test]
fn rewrite_replaces_only_the_map_cast_and_keeps_the_rest_verbatim() {
    let rewritten =
        rewrite_map_casts("SELECT 'it''s', CAST(m AS MAP<STRING, BIGINT>) AS v, é FROM t").unwrap();
    assert!(rewritten.starts_with("SELECT 'it''s', __repark_cast_map__(m, '"));
    assert!(rewritten.ends_with("', false) AS v, é FROM t"));
    let nested =
        rewrite_map_casts("SELECT try_cast(CAST(m AS MAP<STRING,INT>) AS MAP<STRING,BIGINT>)")
            .unwrap();
    assert_eq!(nested.matches(CAST_MAP_NAME).count(), 2);
    assert!(nested.ends_with("', true)"));
    let empty = rewrite_map_casts("SELECT CAST(map() AS MAP<STRING, INT>)").unwrap();
    assert!(empty.starts_with("SELECT __repark_empty_map_cast__(NULL, '"));
}

#[tokio::test]
async fn null_empty_and_widened_maps_answer() {
    let ctx = context(true);
    let null = run(&ctx, "SELECT CAST(NULL AS MAP<STRING, INT>) AS m")
        .await
        .unwrap();
    assert!(null[0].column(0).is_null(0));
    assert_eq!(
        null[0].schema().field(0).data_type(),
        &map_type(DataType::Utf8, DataType::Int32)
    );
    assert!(null[0].schema().field(0).is_nullable());
    let empty = run(&ctx, "SELECT CAST(map() AS MAP<STRING, INT>) AS m")
        .await
        .unwrap();
    assert_eq!(first_cell(&empty), "{}");
    assert!(!empty[0].schema().field(0).is_nullable());
    let widened = run(&ctx, "SELECT CAST(map('a', 1) AS MAP<STRING, BIGINT>) AS m")
        .await
        .unwrap();
    assert_eq!(first_cell(&widened), "{a: 1}");
    assert_eq!(
        widened[0].schema().field(0).data_type(),
        &map_type(DataType::Utf8, DataType::Int64)
    );
    let keys = run(&ctx, "SELECT CAST(map(1, 'x') AS MAP<STRING, STRING>) AS m")
        .await
        .unwrap();
    assert_eq!(first_cell(&keys), "{1: x}");
}

#[tokio::test]
async fn invalid_value_raises_under_ansi_and_nulls_otherwise() {
    let sql = "SELECT CAST(map('a', 'x') AS MAP<STRING, INT>) AS m";
    let error = run(&context(true), sql).await.unwrap_err().to_string();
    assert!(
        error.contains(
            "[CAST_INVALID_INPUT] The value 'x' of the type \"STRING\" cannot be cast to \"INT\""
        ),
        "{error}"
    );
    let legacy = run(&context(false), sql).await.unwrap();
    assert_eq!(first_cell(&legacy), "{a: }");
    let tried = run(
        &context(true),
        "SELECT try_cast(map('a', 'x') AS MAP<STRING, INT>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&tried), "{a: }");
}

#[tokio::test]
async fn try_cast_and_null_sources_plan_nullable() {
    let ctx = context(true);
    for (sql, nullable) in [
        (
            "SELECT try_cast(map('a', 'x') AS MAP<STRING, INT>) AS m",
            true,
        ),
        ("SELECT CAST(NULL AS MAP<STRING, BIGINT>) AS m", true),
        ("SELECT CAST(map() AS MAP<STRING, INT>) AS m", false),
    ] {
        let rewritten = rewrite_map_casts(sql).unwrap();
        let frame = ctx.sql(&rewritten).await.unwrap();
        assert_eq!(frame.schema().field(0).is_nullable(), nullable, "{sql}");
    }
}

#[tokio::test]
async fn non_map_source_refuses_at_planning() {
    let error = run(&context(true), "SELECT CAST(1 AS MAP<STRING, INT>) AS m")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
            && error.contains("\" to \"MAP<STRING, INT>\". SQLSTATE: 42K09"),
        "{error}"
    );
}

#[tokio::test]
async fn unaliased_cast_keeps_the_spark_projection_name() {
    let batches = run(&context(true), "SELECT CAST(NULL AS MAP<STRING, INT>)")
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).name(),
        "CAST(NULL AS MAP<STRING, INT>)"
    );
}

#[test]
fn comment_hint_is_an_ordinary_comment() {
    for sql in [
        "SELECT 1 /*! CAST(x AS MAP<STRING,INT>) */, 2",
        "SELECT 1 /* CAST(x AS MAP<STRING,INT>) */, 2",
    ] {
        assert_eq!(rewrite_map_casts(sql), None, "{sql}");
    }
}

#[tokio::test]
async fn colliding_keys_after_the_key_cast_stay_as_spark_stores_them() {
    let batches = run(
        &context(true),
        "SELECT CAST(map('2', 'a', '1', 'b', '02', 'c') AS MAP<INT, STRING>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&batches), "{2: a, 1: b, 2: c}");
}

#[tokio::test]
async fn key_legality_follows_the_session_mode_and_try_cast() {
    let refusal = "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]";
    let legacy = run(
        &context(false),
        "SELECT CAST(map('1', 'a') AS MAP<INT, STRING>) AS m",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(legacy.contains(refusal), "{legacy}");
    for ansi in [true, false] {
        let tried = run(
            &context(ansi),
            "SELECT try_cast(map('x', 1) AS MAP<INT, INT>) AS m",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(tried.contains(refusal), "{tried}");
        let widened = run(
            &context(ansi),
            "SELECT try_cast(map(1, 'a') AS MAP<BIGINT, STRING>) AS m",
        )
        .await
        .unwrap();
        assert_eq!(first_cell(&widened), "{1: a}");
    }
}

#[tokio::test]
async fn leaf_casts_follow_spark_overflow_and_trimming() {
    let overflow = "SELECT CAST(map('a', 128) AS MAP<STRING, TINYINT>) AS m";
    let error = run(&context(true), overflow).await.unwrap_err().to_string();
    assert!(
        error.contains("[CAST_OVERFLOW] The value 128")
            && error.contains("cannot be cast to \"TINYINT\" due to an overflow"),
        "{error}"
    );
    let wrapped = run(&context(false), overflow).await.unwrap();
    assert_eq!(first_cell(&wrapped), "{a: -128}");
    for ansi in [true, false] {
        let trimmed = run(
            &context(ansi),
            "SELECT CAST(map('a', ' 1') AS MAP<STRING, INT>) AS m",
        )
        .await
        .unwrap();
        assert_eq!(first_cell(&trimmed), "{a: 1}");
    }
    let legacy_fraction = run(
        &context(false),
        "SELECT CAST(map('a', '1.5') AS MAP<STRING, INT>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&legacy_fraction), "{a: 1}");
    let boolean = run(
        &context(true),
        "SELECT CAST(map('a', ' true ') AS MAP<STRING, BOOLEAN>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&boolean), "{a: true}");
}
