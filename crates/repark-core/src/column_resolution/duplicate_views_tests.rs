use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionContext;

use super::plan_statement_with_column_repair;
use crate::frame_names::rename_duplicate_tolerant;

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn table(fields: &[&str]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(fields[0], DataType::Int64, true),
        Field::new(fields[1], DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

async fn view_state() -> SessionState {
    let state = SessionStateBuilder::new()
        .with_config(datafusion::prelude::SessionConfig::new())
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("l", table(&["id", "s"])).unwrap();
    ctx.register_table("r", table(&["id", "t"])).unwrap();
    ctx.register_table("plain", table(&["id", "w"])).unwrap();
    let both = ctx
        .sql("SELECT a.id, a.s, b.id, b.s FROM l a JOIN l b ON a.id = b.id")
        .await
        .unwrap();
    let both = rename_duplicate_tolerant(both, &names(&["id", "s", "id", "s"])).unwrap();
    ctx.register_table("v1", both.into_view()).unwrap();
    let mixed = ctx
        .sql("SELECT a.id, a.s, b.id, b.t FROM l a JOIN r b ON a.id = b.id")
        .await
        .unwrap();
    let mixed = rename_duplicate_tolerant(mixed, &names(&["id", "s", "id", "t"])).unwrap();
    ctx.register_table("vm", mixed.into_view()).unwrap();
    ctx.state()
}

async fn planned(state: &SessionState, sql: &str, case_insensitive: bool) -> Vec<String> {
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

async fn refused(state: &SessionState, sql: &str, case_insensitive: bool) -> String {
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(sql, &dialect).unwrap();
    plan_statement_with_column_repair(state, statement, case_insensitive)
        .await
        .unwrap_err()
        .to_string()
}

fn ambiguous(reference: &str, options: &str) -> String {
    format!(
        "[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: [{options}]. \
         SQLSTATE: 42704"
    )
}

#[tokio::test]
async fn a_repeated_name_is_ambiguous_under_the_relation_as_written() {
    let state = view_state().await;
    for (sql, reference, option) in [
        ("SELECT id FROM v1", "`id`", "`v1`.`id`"),
        ("SELECT v1.id FROM v1", "`v1`.`id`", "`v1`.`id`"),
        ("SELECT x.id FROM v1 x", "`x`.`id`", "`x`.`id`"),
        ("SELECT s FROM v1", "`s`", "`v1`.`s`"),
        ("SELECT 1 AS one FROM v1 WHERE id = 1", "`id`", "`v1`.`id`"),
        (
            "SELECT id, count(*) FROM v1 GROUP BY id",
            "`id`",
            "`v1`.`id`",
        ),
        ("SELECT max(id) FROM v1", "`id`", "`v1`.`id`"),
        ("SELECT id FROM (SELECT * FROM v1) q", "`id`", "`q`.`id`"),
        ("SELECT id FROM vm", "`id`", "`vm`.`id`"),
    ] {
        for case_insensitive in [true, false] {
            let message = refused(&state, sql, case_insensitive).await;
            assert!(
                message.contains(&ambiguous(reference, &format!("{option}, {option}"))),
                "{sql} ({case_insensitive}): {message}"
            );
        }
    }
}

#[tokio::test]
async fn a_unique_name_and_a_star_plan_over_the_same_view() {
    let state = view_state().await;
    for case_insensitive in [true, false] {
        assert_eq!(
            planned(&state, "SELECT t, s FROM vm", case_insensitive).await,
            names(&["t", "s"])
        );
        assert_eq!(
            planned(&state, "SELECT * FROM v1", case_insensitive).await,
            names(&[
                "__repark_dup_0_id",
                "__repark_dup_1_s",
                "__repark_dup_2_id",
                "__repark_dup_3_s"
            ])
        );
        assert_eq!(
            planned(&state, "SELECT count(*) AS c FROM v1", case_insensitive).await,
            names(&["c"])
        );
    }
}

#[tokio::test]
async fn a_missing_name_suggests_the_display_names() {
    let state = view_state().await;
    for case_insensitive in [true, false] {
        let message = refused(&state, "SELECT nope FROM vm", case_insensitive).await;
        assert!(
            message.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
            "{message}"
        );
        assert!(message.contains("[`id`, `s`, `id`, `t`]"), "{message}");
        assert!(!message.contains("__repark_"), "{message}");
    }
}

#[tokio::test]
async fn the_case_rule_decides_a_respelled_repeat() {
    let state = view_state().await;
    let folded = refused(&state, "SELECT ID FROM v1", true).await;
    assert!(folded.contains("[AMBIGUOUS_REFERENCE]"), "{folded}");
    let exact = refused(&state, "SELECT ID FROM v1", false).await;
    assert!(
        exact.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]"),
        "{exact}"
    );
    assert!(!exact.contains("__repark_"), "{exact}");
}

#[tokio::test]
async fn a_bare_name_shared_with_a_plain_relation_is_ambiguous() {
    let state = view_state().await;
    for case_insensitive in [true, false] {
        let message = refused(
            &state,
            "SELECT id FROM v1 CROSS JOIN plain",
            case_insensitive,
        )
        .await;
        assert!(
            message.contains(&ambiguous("`id`", "`plain`.`id`, `v1`.`id`, `v1`.`id`")),
            "{message}"
        );
        assert_eq!(
            planned(
                &state,
                "SELECT plain.id, w FROM v1 CROSS JOIN plain",
                case_insensitive
            )
            .await,
            names(&["id", "w"])
        );
        assert_eq!(
            planned(&state, "SELECT id FROM plain", case_insensitive).await,
            names(&["id"])
        );
    }
}
