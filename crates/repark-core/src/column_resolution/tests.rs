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
            "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`t`.`id`, `t`.`id`]. SQLSTATE: 42704"
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
            "[AMBIGUOUS_REFERENCE] Reference `a` is ambiguous, could be: [`amb_l`.`a`, `amb_r`.`a`]. SQLSTATE: 42704"
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
    assert!(
        error.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
        "unexpected message: {error}"
    );
    assert!(
        error.contains("SQLSTATE: 42703"),
        "unexpected message: {error}"
    );
}

#[tokio::test]
async fn missing_column_stamps_unresolved_without_fold() {
    let state = mixed_state();
    let error = plan_error(&state, "SELECT nope FROM t", false).await;
    assert!(error.contains("nope"), "unexpected message: {error}");
    assert!(
        error.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
        "unexpected message: {error}"
    );
    assert!(
        error.contains("SQLSTATE: 42703"),
        "unexpected message: {error}"
    );
}

#[tokio::test]
async fn unresolved_stamp_keeps_exact_case_select() {
    let state = mixed_state();
    assert_eq!(
        plan_names(&state, "SELECT userId FROM t", true).await,
        vec!["userId".to_string()]
    );
}

#[tokio::test]
async fn unresolved_stamp_keeps_folded_select() {
    let state = mixed_state();
    assert_eq!(
        plan_names(&state, "SELECT USERID FROM t", true).await,
        vec!["userId".to_string()]
    );
}

#[tokio::test]
async fn unresolved_stamp_skips_ambiguous_references() {
    let ctx = measured_ctx();
    let error = plan_error(&ctx.state(), "SELECT id FROM tw", true).await;
    assert!(
        error.contains("[AMBIGUOUS_REFERENCE]") && error.contains("SQLSTATE: 42704"),
        "unexpected message: {error}"
    );
    assert!(
        !error.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        "unexpected message: {error}"
    );
}

#[tokio::test]
async fn unresolved_stamp_keeps_select_one() {
    let state = mixed_state();
    assert_eq!(plan_names(&state, "SELECT 1", true).await.len(), 1);
}

#[tokio::test]
async fn unresolved_stamp_skips_parse_errors() {
    let ctx = SessionContext::new_with_state(mixed_state());
    let error = sql_with_column_repair(&ctx, "SELECT FROM", true)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        !error.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        "unexpected message: {error}"
    );
}

#[tokio::test]
async fn unresolved_stamp_skips_missing_tables() {
    let state = mixed_state();
    let error = plan_error(&state, "SELECT nope FROM no_such_table", true).await;
    assert!(
        !error.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        "unexpected message: {error}"
    );
}

#[tokio::test]
async fn unresolved_stamp_skips_empty_valid_fields() {
    let state = mixed_state();
    let error = plan_error(&state, "SELECT nope", true).await;
    assert!(error.contains("nope"), "unexpected message: {error}");
    assert!(
        !error.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        "unexpected message: {error}"
    );
}

type ArrayRef = Arc<dyn datafusion::arrow::array::Array>;
type MeasuredTable<'a> = (&'a str, Vec<Field>, Vec<ArrayRef>);

fn measured_ctx() -> SessionContext {
    let ctx = SessionContext::new_with_state(repair_state());
    let int = |name: &str| Field::new(name, DataType::Int32, true);
    let text = |name: &str| Field::new(name, DataType::Utf8, true);
    let ints = |values: Vec<i32>| -> ArrayRef { Arc::new(Int32Array::from(values)) };
    let texts = |values: Vec<&str>| -> ArrayRef { Arc::new(StringArray::from(values)) };
    let tables: Vec<MeasuredTable<'_>> = vec![
        (
            "mc",
            vec![int("userId"), text("eventName")],
            vec![ints(vec![1, 2]), texts(vec!["a", "b"])],
        ),
        ("other", vec![text("name")], vec![texts(vec!["a"])]),
        ("labels", vec![text("eventLabel")], vec![texts(vec!["a"])]),
        ("ids", vec![int("USERID")], vec![ints(vec![2])]),
        (
            "ja",
            vec![int("userId"), int("x")],
            vec![ints(vec![1, 2]), ints(vec![10, 20])],
        ),
        (
            "jb",
            vec![int("userId"), int("y")],
            vec![ints(vec![1]), ints(vec![100])],
        ),
        (
            "jt",
            vec![int("userId"), int("x"), int("y")],
            vec![ints(vec![]), ints(vec![]), ints(vec![])],
        ),
        (
            "tw",
            vec![int("id"), int("ID")],
            vec![ints(vec![1]), ints(vec![0])],
        ),
    ];
    for (name, fields, columns) in tables {
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    ctx
}

async fn measured_rows(ctx: &SessionContext, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    use datafusion::arrow::util::display::array_value_to_string;
    let frame = sql_with_column_repair(ctx, sql, true).await.unwrap();
    let names = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect(),
            );
        }
    }
    rows.sort();
    (names, rows)
}

fn lowered(names: &[String]) -> Vec<String> {
    names.iter().map(|name| name.to_ascii_lowercase()).collect()
}

fn text_rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|value| (*value).to_string()).collect())
        .collect()
}

#[tokio::test]
async fn v01_select_alias_of_the_same_name_folds_the_aliased_column() {
    let ctx = measured_ctx();
    for (sql, rows) in [
        (
            "SELECT userId AS USERID FROM mc",
            text_rows(&[&["1"], &["2"]]),
        ),
        (
            "SELECT userId + 1 AS USERID FROM mc ORDER BY USERID",
            text_rows(&[&["2"], &["3"]]),
        ),
        (
            "SELECT COUNT(userId) AS USERID FROM mc HAVING USERID > 0",
            text_rows(&[&["2"]]),
        ),
        (
            "SELECT USERID AS USERID FROM mc",
            text_rows(&[&["1"], &["2"]]),
        ),
    ] {
        let (names, actual) = measured_rows(&ctx, sql).await;
        assert_eq!(lowered(&names), vec!["userid".to_string()], "{sql}");
        assert_eq!(actual, rows, "{sql}");
    }
}

#[tokio::test]
async fn v02_every_relation_folds_not_only_the_first_miss() {
    let ctx = measured_ctx();
    for sql in [
        "SELECT USERID FROM mc WHERE EVENTNAME IN (SELECT NAME FROM other)",
        "SELECT USERID FROM mc WHERE EVENTNAME IN (SELECT EVENTLABEL FROM labels)",
        "SELECT USERID FROM mc",
    ] {
        let (names, _) = measured_rows(&ctx, sql).await;
        assert_eq!(lowered(&names), vec!["userid".to_string()], "{sql}");
    }
    let (_, rows) = measured_rows(
        &ctx,
        "SELECT USERID FROM mc WHERE EVENTNAME IN (SELECT NAME FROM other)",
    )
    .await;
    assert_eq!(rows, text_rows(&[&["1"]]));
    let (_, rows) = measured_rows(
        &ctx,
        "SELECT USERID FROM mc WHERE EVENTNAME IN (SELECT EVENTLABEL FROM labels)",
    )
    .await;
    assert_eq!(rows, text_rows(&[&["1"]]));
}

#[tokio::test]
async fn v02_outer_spelling_is_not_rewritten_into_an_inner_scope() {
    let ctx = measured_ctx();
    let (names, rows) = measured_rows(
        &ctx,
        "SELECT userid FROM mc WHERE userid IN (SELECT userid FROM ids)",
    )
    .await;
    assert_eq!(names, vec!["userId".to_string()]);
    assert_eq!(rows, text_rows(&[&["2"]]));
}

#[tokio::test]
async fn v04_join_using_folds_inside_insert() {
    let ctx = measured_ctx();
    let (names, rows) =
        measured_rows(&ctx, "SELECT USERID, x, y FROM ja JOIN jb USING (USERID)").await;
    assert_eq!(lowered(&names), vec!["userid", "x", "y"]);
    assert_eq!(rows, text_rows(&[&["1", "10", "100"]]));
    let (_, inserted) = measured_rows(
        &ctx,
        "INSERT INTO jt SELECT USERID, x, y FROM ja JOIN jb USING (USERID)",
    )
    .await;
    assert_eq!(inserted, text_rows(&[&["1"]]));
    let (_, stored) = measured_rows(&ctx, "SELECT userId, x, y FROM jt").await;
    assert_eq!(stored, text_rows(&[&["1", "10", "100"]]));
}

#[tokio::test]
async fn l08_correlated_reference_to_a_case_twin_is_ambiguous() {
    let ctx = measured_ctx();
    for (sql, message) in [
        (
            "SELECT 1 FROM tw t WHERE EXISTS (SELECT 1 FROM other o WHERE o.name = CAST(t.ID AS STRING))",
            "[AMBIGUOUS_REFERENCE] Reference `t`.`ID` is ambiguous, could be: [`t`.`ID`, `t`.`ID`]. SQLSTATE: 42704",
        ),
        (
            "SELECT 1 FROM tw WHERE EXISTS (SELECT 1 FROM other WHERE name = CAST(ID AS STRING))",
            "[AMBIGUOUS_REFERENCE] Reference `ID` is ambiguous, could be: [`tw`.`ID`, `tw`.`ID`]. SQLSTATE: 42704",
        ),
        (
            "SELECT 1 FROM tw t WHERE 'a' IN (SELECT name FROM other WHERE name <> CAST(t.id AS STRING))",
            "[AMBIGUOUS_REFERENCE] Reference `t`.`id` is ambiguous, could be: [`t`.`id`, `t`.`id`]. SQLSTATE: 42704",
        ),
    ] {
        let error = plan_error(&ctx.state(), sql, true).await;
        assert!(error.contains(message), "{sql}: {error}");
    }
}

#[tokio::test]
async fn l08_qualified_reference_is_not_ambiguous_because_a_bare_spelling_appears_elsewhere() {
    let ctx = measured_ctx();
    let (names, rows) = measured_rows(
        &ctx,
        "SELECT i.USERID FROM ids i JOIN ja j ON i.USERID = j.userId WHERE j.userId IN (SELECT userid FROM jb)",
    )
    .await;
    assert_eq!(lowered(&names), ["userid"]);
    assert!(rows.is_empty(), "{rows:?}");
    let (names, rows) = measured_rows(
        &ctx,
        "SELECT i.USERID FROM ids i JOIN ja j ON i.USERID = j.userId WHERE j.userId IN (SELECT userid FROM ja)",
    )
    .await;
    assert_eq!(lowered(&names), ["userid"]);
    assert_eq!(rows, text_rows(&[&["2"]]));
}

#[tokio::test]
async fn l08_bare_twin_options_carry_the_full_relation_name() {
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
    let ctx = measured_ctx();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("ns", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("sc", catalog);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("ID", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![0])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "sc.ns.tw",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    for (sql, message) in [
        (
            "SELECT ID FROM sc.ns.tw",
            "[AMBIGUOUS_REFERENCE] Reference `ID` is ambiguous, could be: [`sc`.`ns`.`tw`.`ID`, `sc`.`ns`.`tw`.`ID`]. SQLSTATE: 42704",
        ),
        (
            "SELECT id FROM sc.ns.tw",
            "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`sc`.`ns`.`tw`.`id`, `sc`.`ns`.`tw`.`id`]. SQLSTATE: 42704",
        ),
        (
            "SELECT ID FROM datafusion.public.tw",
            "[AMBIGUOUS_REFERENCE] Reference `ID` is ambiguous, could be: [`tw`.`ID`, `tw`.`ID`]. SQLSTATE: 42704",
        ),
    ] {
        let error = plan_error(&ctx.state(), sql, true).await;
        assert!(error.contains(message), "{sql}: {error}");
    }
}

#[tokio::test]
async fn v01_order_by_a_select_alias_still_orders_by_the_alias() {
    use datafusion::arrow::util::display::array_value_to_string;
    let ctx = measured_ctx();
    let batches = sql_with_column_repair(
        &ctx,
        "SELECT userId * -1 AS USERID FROM mc ORDER BY USERID",
        true,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| array_value_to_string(batch.column(0), row).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec!["-2".to_string(), "-1".to_string()]);
}

#[tokio::test]
async fn r02_lowercase_only_plans_skip_the_audit() {
    let ctx = measured_ctx();
    let lowercase = ctx
        .sql("SELECT name FROM other")
        .await
        .unwrap()
        .into_unoptimized_plan();
    assert!(!plan_has_upper_ascii_field(&lowercase));
    for sql in [
        "SELECT id FROM tw",
        "SELECT name FROM other WHERE name IN (SELECT `eventName` FROM mc)",
    ] {
        let plan = ctx.sql(sql).await.unwrap().into_unoptimized_plan();
        assert!(plan_has_upper_ascii_field(&plan), "{sql}");
    }
}

#[tokio::test]
async fn n03_star_over_a_case_twin_answers_both_columns_declared() {
    let ctx = measured_ctx();
    for sql in [
        "SELECT * FROM tw",
        "SELECT t.* FROM tw AS t",
        "SELECT * FROM (SELECT * FROM tw) AS s",
    ] {
        let (names, rows) = measured_rows(&ctx, sql).await;
        assert_eq!(names, ["id", "ID"], "{sql}");
        assert_eq!(rows, text_rows(&[&["1", "0"]]), "{sql}");
    }
}

fn plan_on_default_stack(
    state: SessionState,
    sql: String,
    case_insensitive: bool,
) -> Result<Vec<String>> {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                let dialect = state.config().options().sql_parser.dialect;
                let statement = state.sql_to_statement(&sql, &dialect)?;
                let plan =
                    plan_statement_with_column_repair(&state, statement, case_insensitive).await?;
                Ok(plan
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect())
            })
        })
        .unwrap()
        .join()
        .unwrap()
}

fn union_all(branch: &str, count: usize) -> String {
    vec![branch; count].join(" UNION ALL ")
}

#[test]
fn s22b_thousand_branch_union_plans_on_a_two_mebibyte_stack() {
    let sql = format!(
        "SELECT count(*) FROM ({})",
        union_all("SELECT CAST(7.0 AS DOUBLE) AS x", 1000)
    );
    assert_eq!(
        plan_on_default_stack(repair_state(), sql, true).unwrap(),
        ["count(*)"]
    );
}

#[test]
fn s22b_thousand_branch_union_folds_wrong_case_on_a_two_mebibyte_stack() {
    let sql = union_all("SELECT USERID FROM t", 1000);
    assert_eq!(
        plan_on_default_stack(mixed_state(), sql, true).unwrap(),
        ["userId"]
    );
}

#[test]
fn s22b_five_thousand_branch_union_plans_on_a_two_mebibyte_stack() {
    let sql = format!(
        "SELECT count(*) FROM ({})",
        union_all("SELECT CAST(7.0 AS DOUBLE) AS x", 5000)
    );
    assert_eq!(
        plan_on_default_stack(repair_state(), sql, true).unwrap(),
        ["count(*)"]
    );
}

#[test]
fn s22b_stack_estimate_counts_every_union_level_inside_a_subquery() {
    let state = repair_state();
    let dialect = state.config().options().sql_parser.dialect;
    let shallow = state.sql_to_statement("SELECT 1 AS x", &dialect).unwrap();
    let deep_sql = format!(
        "SELECT count(*) FROM ({})",
        union_all("SELECT CAST(7.0 AS DOUBLE) AS x", 1000)
    );
    let deep = state.sql_to_statement(&deep_sql, &dialect).unwrap();
    assert!(stack::stack_bytes_for(&shallow) < 512 * 1024);
    assert!(stack::stack_bytes_for(&deep) >= 1000 * 32 * 1024);
}
