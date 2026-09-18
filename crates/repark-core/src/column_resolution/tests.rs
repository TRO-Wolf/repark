use super::*;

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;

fn repair_state() -> SessionState {
    SessionStateBuilder::new()
        .with_config(datafusion::prelude::SessionConfig::new())
        .with_default_features()
        .build()
}

fn mixed_state() -> SessionState {
    let state = repair_state();
    let schema = Arc::new(Schema::new(vec![
        Field::new("userId", DataType::Int64, true),
        Field::new("eventName", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx.state()
}

async fn plan_names(state: &SessionState, sql: &str, case_insensitive: bool) -> Vec<String> {
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(sql, &dialect).unwrap();
    plan_statement_with_column_repair(state, statement, case_insensitive)
        .await
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

async fn plan_error(state: &SessionState, sql: &str, case_insensitive: bool) -> String {
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(sql, &dialect).unwrap();
    plan_statement_with_column_repair(state, statement, case_insensitive)
        .await
        .unwrap_err()
        .to_string()
}

#[tokio::test]
async fn wrong_case_select_filters_orders_and_reads_rows() {
    let state = mixed_state();
    assert_eq!(
        plan_names(
            &state,
            "SELECT USERID, EVENTNAME FROM t ORDER BY USERID",
            true
        )
        .await,
        vec!["userId".to_string(), "eventName".to_string()]
    );
    let ctx = SessionContext::new_with_state(state);
    let batches = sql_with_column_repair(
        &ctx,
        "SELECT USERID FROM t WHERE EVENTNAME = 'b' ORDER BY USERID",
        true,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(column.len(), 1);
    assert_eq!(column.value(0), 2);
}

#[tokio::test]
async fn quoted_wrong_case_and_qualified_resolve() {
    let state = mixed_state();
    assert_eq!(
        plan_names(&state, "SELECT `USERID` FROM t", true).await,
        vec!["userId".to_string()]
    );
    assert_eq!(
        plan_names(&state, "SELECT T.USERID FROM t AS T", true).await,
        vec!["userId".to_string()]
    );
}

#[tokio::test]
async fn group_by_wrong_case_groups() {
    let state = mixed_state();
    assert_eq!(
        plan_names(
            &state,
            "SELECT EVENTNAME, COUNT(*) AS c FROM t GROUP BY EVENTNAME ORDER BY EVENTNAME",
            true,
        )
        .await,
        vec!["eventName".to_string(), "c".to_string()]
    );
}

#[tokio::test]
async fn case_only_collision_raises_the_spark_sentence() {
    let state = repair_state();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("ID", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(Int32Array::from(vec![Some(2)])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new_with_state(state.clone());
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let error = plan_error(&ctx.state(), "SELECT id FROM t", true).await;
    assert!(
        error.contains(
            "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`t`.`id`]. SQLSTATE: 42702"
        ),
        "unexpected message: {error}"
    );
}

#[tokio::test]
async fn join_collision_on_bare_reference_raises() {
    let state = repair_state();
    let ctx = SessionContext::new_with_state(state);
    for (name, value) in [("amb_l", "a"), ("amb_r", "A")] {
        let schema = Arc::new(Schema::new(vec![Field::new(value, DataType::Int32, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![Some(1)]))]).unwrap();
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    let error = plan_error(
        &ctx.state(),
        "SELECT a FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
        true,
    )
    .await;
    assert!(
        error.contains(
            "[AMBIGUOUS_REFERENCE] Reference `a` is ambiguous, could be: [`amb_l`.`a`, `amb_r`.`a`]. SQLSTATE: 42702"
        ),
        "unexpected message: {error}"
    );
    assert_eq!(
        plan_names(
            &ctx.state(),
            "SELECT amb_l.a FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
            true,
        )
        .await,
        vec!["a".to_string()]
    );
}

#[tokio::test]
async fn sensitive_session_refuses_folded_names_and_keeps_backticks() {
    let state = mixed_state();
    let error = plan_error(&state, "SELECT USERID FROM t", false).await;
    assert!(error.contains("userid"), "unexpected message: {error}");
    let error = plan_error(&state, "SELECT userId FROM t", false).await;
    assert!(error.contains("userid"), "unexpected message: {error}");
    assert_eq!(
        plan_names(&state, "SELECT `userId` FROM t", false).await,
        vec!["userId".to_string()]
    );
}

#[test]
fn fragment_rewrite_resolves_against_known_scopes() {
    let target = vec!["userId".to_string(), "eventName".to_string()];
    let source = vec!["userid".to_string(), "eventname".to_string()];
    let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
    assert_eq!(
        rewrite_fragment_case("USERID = 2", &[scopes[0]], true, None).unwrap(),
        "`userId` = 2".to_string()
    );
    assert_eq!(
        rewrite_fragment_case(
            "t.USERID = s.userid AND t.EVENTNAME = 'x'",
            &scopes,
            true,
            None
        )
        .unwrap(),
        "t.`userId` = s.userid AND t.`eventName` = 'x'".to_string()
    );
    assert_eq!(
        rewrite_fragment_case("USERID = 2", &[scopes[0]], false, None).unwrap(),
        "USERID = 2".to_string()
    );
    assert!(
        rewrite_fragment_case("nope = 2", &[scopes[0]], true, None)
            .unwrap()
            .contains("nope")
    );
}

#[test]
fn fragment_rewrite_skips_subqueries_and_flags_collisions() {
    let target = vec!["userId".to_string()];
    let source = vec!["USERID".to_string(), "userId".to_string()];
    let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
    assert_eq!(
        rewrite_fragment_case("x IN (SELECT USERID FROM other)", &[scopes[0]], true, None).unwrap(),
        "x IN (SELECT USERID FROM other)".to_string()
    );
    let error = rewrite_fragment_case("USERID = s.USERID", &scopes, true, None).unwrap_err();
    assert!(error.to_string().contains("[AMBIGUOUS_REFERENCE]"));
}

#[test]
fn fragment_rewrite_scopes_bare_references_to_one_side() {
    let target = vec!["userId".to_string()];
    let source = vec!["userid".to_string()];
    let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
    assert_eq!(
        rewrite_fragment_case("USERID = 2", &scopes, true, Some("s")).unwrap(),
        "USERID = 2".to_string()
    );
    assert_eq!(
        rewrite_fragment_case("USERID = 2", &scopes, true, Some("t")).unwrap(),
        "`userId` = 2".to_string()
    );
    assert_eq!(
        rewrite_fragment_case("t.USERID = 2", &scopes, true, Some("s")).unwrap(),
        "t.`userId` = 2".to_string()
    );
    let error = rewrite_fragment_case("USERID = 2", &scopes, true, None).unwrap_err();
    assert!(error.to_string().contains("[AMBIGUOUS_REFERENCE]"));
}

#[tokio::test]
async fn dataframe_filter_binds_projection_alias() {
    use datafusion::logical_expr::{col, lit};
    for case_insensitive in [false, true] {
        let ctx = SessionContext::new_with_state(repair_state());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        ctx.register_table(
            "nums",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let frame = sql_with_column_repair(&ctx, "SELECT id AS Id FROM nums", case_insensitive)
            .await
            .unwrap()
            .filter(col("Id").gt(lit(1i64)))
            .unwrap();
        let batches = frame.collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
}

#[tokio::test]
async fn select_alias_does_not_shadow_wrong_case_column() {
    let state = mixed_state();
    assert_eq!(
        plan_names(&state, "SELECT `USERID` AS userid FROM t", true).await,
        vec!["userid".to_string()]
    );
}

#[tokio::test]
async fn missing_column_stays_missing() {
    let state = mixed_state();
    let error = plan_error(&state, "SELECT nope FROM t", true).await;
    assert!(error.contains("nope"), "unexpected message: {error}");
}
