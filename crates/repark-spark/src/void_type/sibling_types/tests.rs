use datafusion::common::Column;
use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::super::select_values_arms::{ArmMap, resolve_insert_arms};
use super::*;

fn arms_of(sql: &str) -> Vec<String> {
    let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    resolve_insert_arms(&source, true)
        .iter()
        .map(|arm| {
            format!(
                "{}:{}",
                arm.positions.len(),
                arm.arm_sql.clone().unwrap_or_default()
            )
        })
        .collect()
}

#[test]
fn stars_over_tables_keep_no_cells_and_carry_the_arm_select() {
    let star = arms_of(
        "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM src",
    );
    assert_eq!(star.len(), 2);
    assert!(star[1].starts_with("0:SELECT * FROM src"), "{star:?}");
    let qualified = arms_of(
        "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT s.* FROM src AS s",
    );
    assert_eq!(qualified.len(), 2);
    assert!(
        qualified[1].starts_with("0:SELECT s.* FROM src AS s"),
        "{qualified:?}"
    );
    let mixed =
        arms_of("INSERT INTO t SELECT * FROM (VALUES (1, 2)) AS v(a, b) JOIN src ON v.a = src.id");
    assert_eq!(mixed.len(), 1);
    assert!(mixed[0].starts_with("0:SELECT * FROM"), "{mixed:?}");
}

#[test]
fn static_typing_reads_literals_and_casts_but_defers_functions() {
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO t VALUES ('x', CAST('x' AS STRING), concat('a', 'b'), \
         TIMESTAMP '2020-01-01 10:00:00', make_timestamp(2020, 1, 1, 0, 0, 0))",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let SetExpr::Values(values) = source.body.as_ref() else {
        panic!("want a VALUES source");
    };
    let typed: Vec<Option<DataType>> = values.rows[0]
        .content
        .iter()
        .map(static_source_type)
        .collect();
    assert_eq!(typed[0], Some(DataType::Utf8), "{typed:?}");
    assert_eq!(typed[1], Some(DataType::Utf8), "{typed:?}");
    assert_eq!(typed[2], None, "{typed:?}");
    assert_eq!(
        typed[3],
        Some(DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into())
        )),
        "{typed:?}"
    );
    assert_eq!(typed[4], None, "{typed:?}");
}

#[test]
fn ambiguity_matches_only_the_typed_error_and_the_tag() {
    let typed = DataFusionError::SchemaError(
        Box::new(SchemaError::AmbiguousReference {
            field: Box::new(Column::from_name("tsc")),
        }),
        Box::new(None),
    );
    assert!(is_ambiguity(&typed));
    let wrapped = DataFusionError::Collection(vec![typed]);
    assert!(is_ambiguity(&wrapped));
    assert!(is_ambiguity(&DataFusionError::Plan(
        "[AMBIGUOUS_REFERENCE] Reference `tsc` is ambiguous, could be: [`t`.`tsc`, `u`.`tsc`]"
            .to_string(),
    )));
    assert!(is_ambiguity(&DataFusionError::Context(
        "context".to_string(),
        Box::new(DataFusionError::Plan(
            "[AMBIGUOUS_REFERENCE] Reference `c` is ambiguous".to_string(),
        )),
    )));
    assert!(is_ambiguity(&DataFusionError::Shared(std::sync::Arc::new(
        DataFusionError::Plan("[AMBIGUOUS_REFERENCE] Reference `c` is ambiguous".to_string()),
    ))));
    assert!(!is_ambiguity(&DataFusionError::Plan(
        "Reference `tsc` is ambiguous, could be expensive".to_string()
    )));
    assert!(!is_ambiguity(&DataFusionError::Plan(
        "Error during planning: [AMBIGUOUS_REFERENCE] Reference `tsc` is ambiguous".to_string(),
    )));
    assert!(!is_ambiguity(&DataFusionError::Plan(
        "table ambiguous_tsc not found".to_string()
    )));
    assert!(!is_ambiguity(&DataFusionError::Plan(
        "type_coercion".to_string()
    )));
}

#[test]
fn resolution_matches_only_typed_and_tagged_failures() {
    let missing = DataFusionError::SchemaError(
        Box::new(SchemaError::FieldNotFound {
            field: Box::new(Column::from_name("nosuch")),
            valid_fields: Vec::new(),
        }),
        Box::new(None),
    );
    assert!(is_resolution_error(&missing));
    assert!(is_resolution_error(&DataFusionError::Collection(vec![
        missing
    ])));
    for payload in [
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] cannot resolve `nosuch`",
        "[UNRESOLVED_ROUTINE] cannot resolve `nosuchfn`",
        "[TABLE_OR_VIEW_NOT_FOUND] cannot find `nosuch`",
        "Invalid function 'nosuchfn'.\nDid you mean 'cosh'?",
        "table 'datafusion.public.missing_table' not found",
    ] {
        let error = DataFusionError::Plan(payload.to_string());
        assert!(is_resolution_error(&error), "{payload}");
    }
    let wrapped = DataFusionError::Diagnostic(
        Box::new(datafusion::common::Diagnostic::new_error(
            "table 'missing_table' not found",
            None,
        )),
        Box::new(DataFusionError::Plan(
            "Invalid function 'nosuchfn'".to_string(),
        )),
    );
    assert!(is_resolution_error(&wrapped));
    for payload in [
        "Cannot coerce arithmetic expression Utf8 + Int64 to valid types",
        "table function 'nosuchtvf' not found",
        "Table function 'nosuchtvf' not found",
        "table ambiguous_tsc not found",
        "Table not found: t",
        "failed to resolve schema: ns",
        "failed to resolve catalog: cat",
        "plan failed: [UNRESOLVED_COLUMN.WITH_SUGGESTION] cannot resolve `nosuch`",
        "plan failed: [UNRESOLVED_ROUTINE] cannot resolve `nosuchfn`",
        "plan failed: [TABLE_OR_VIEW_NOT_FOUND] cannot find `nosuch`",
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] cannot resolve `nosuch`",
    ] {
        let error = DataFusionError::Plan(payload.to_string());
        assert!(!is_resolution_error(&error), "{payload}");
    }
    assert!(!is_resolution_error(&DataFusionError::Execution(
        "INTERVAL expression cannot be Value(Number(\"1\"))".to_string(),
    )));
    let ambiguous = DataFusionError::SchemaError(
        Box::new(SchemaError::AmbiguousReference {
            field: Box::new(Column::from_name("tsc")),
        }),
        Box::new(None),
    );
    assert!(!is_resolution_error(&ambiguous));
}

#[tokio::test]
async fn function_cells_type_through_the_shared_probe() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO t SELECT * FROM (VALUES (1, concat('a', 'b'))) AS v(a, b)",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert_eq!(judge.probed_type(&value).await, Some(DataType::Utf8));
    assert_eq!(judge.probed_type(&value).await, Some(DataType::Utf8));
    assert_eq!(judge.probes.len(), 1);
}

#[tokio::test]
async fn function_string_beside_timestamp_skips_but_beside_bigint_judges() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    for (sql, position, want) in [
        (
            "INSERT INTO t SELECT * FROM (VALUES (1, concat('2020-01-01', ' 10:00:00'))) AS v(a, b) UNION ALL SELECT 2, TIMESTAMP '2021-02-02 02:02:02'",
            1,
            true,
        ),
        (
            "INSERT INTO t SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL SELECT 2, 7L",
            1,
            false,
        ),
    ] {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[position][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert_eq!(judge.skip_string(position, &value).await, want, "{sql}");
    }
}

#[tokio::test]
async fn unresolvable_arms_leave_cells_unjudged_for_the_analyzer() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM missing_table",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    assert_eq!(arms.len(), 2);
    assert!(arms[1].positions.is_empty());
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Unresolved)));
}

#[tokio::test]
async fn arms_without_sql_keep_their_judgement() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b)",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let mut arms = resolve_insert_arms(&source, true);
    arms.push(ArmMap {
        positions: vec![Vec::new(), Vec::new()],
        provenance: vec![Vec::new(), Vec::new()],
        arm_sql: None,
    });
    assert_eq!(arms.len(), 2);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
}

fn mem_ctx() -> SessionContext {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemTable;
    use std::sync::Arc;

    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
        Field::new(
            "t",
            DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            true,
        ),
    ]);
    let table = MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("memory table registers");
    ctx
}

fn mem_ctx_upper() -> SessionContext {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemTable;
    use std::sync::Arc;

    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("C", DataType::Utf8, false),
    ]);
    let table = MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("memory table registers");
    ctx
}

fn mem_ctx_join() -> SessionContext {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemTable;
    use std::sync::Arc;

    let ctx = SessionContext::new();
    for name in ["t", "u"] {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("c", DataType::Utf8, false),
        ]);
        let table =
            MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
        ctx.register_table(name, Arc::new(table))
            .expect("memory table registers");
    }
    ctx
}

#[tokio::test]
async fn quoted_case_in_expressions_resolves_through_column_repair() {
    let ctx = mem_ctx();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(\"S\") FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Typed(_))));
}

#[tokio::test]
async fn genuinely_missing_columns_in_expressions_stay_unresolved() {
    let ctx = mem_ctx();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(nosuch) FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Unresolved)));
}

#[tokio::test]
async fn lowercase_refs_over_uppercase_columns_stay_judged() {
    let ctx = mem_ctx_upper();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(c) FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Typed(_))));
}

#[tokio::test]
async fn case_sensitive_exact_uppercase_refs_stay_judged() {
    let ctx = mem_ctx_upper();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT ID, UPPER(C) FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, false);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, false);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Typed(_))));
}

#[tokio::test]
async fn case_sensitive_missing_uppercase_refs_stay_unresolved() {
    let ctx = mem_ctx();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(S) FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, false);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, false);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Unresolved)));
}

#[tokio::test]
async fn unquoted_interval_window_arms_keep_their_judgement() {
    let ctx = mem_ctx();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, s FROM (SELECT id, s, count(*) OVER (ORDER BY t RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS n FROM t) q",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
    assert!(
        matches!(judge.plans.get(&1), Some(ArmPlan::Typed(types)) if types.get(1) == Some(&DataType::Utf8))
    );
}

#[tokio::test]
async fn coercion_failure_arms_keep_their_judgement() {
    let ctx = mem_ctx();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, s + 1 FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
    assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Failed)));
}

#[tokio::test]
async fn ambiguous_arms_unjudge_values_positions_at_other_positions() {
    let ctx = mem_ctx_join();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO g SELECT v.a, t.c FROM (VALUES ('x', 2)) AS v(a, b) JOIN t ON v.b = t.id JOIN u ON t.id = u.id WHERE c = c",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    assert_eq!(arms.len(), 1);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[0][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(judge.skip_string(0, &value).await);
    assert!(matches!(judge.plans.get(&0), Some(ArmPlan::Ambiguous)));
}

#[test]
fn scoped_arms_carry_their_cte_definitions() {
    let scoped = arms_of(
        "INSERT INTO t WITH xv AS (SELECT * FROM strtab) SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM xv",
    );
    assert_eq!(scoped.len(), 2);
    assert!(scoped[1].starts_with("0:WITH "), "{scoped:?}");
    assert!(scoped[1].contains("xv AS"), "{scoped:?}");
    assert!(scoped[1].ends_with("SELECT * FROM xv"), "{scoped:?}");
    let bare = arms_of(
        "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM src",
    );
    assert_eq!(bare.len(), 2);
    assert!(bare[1].starts_with("0:SELECT * FROM src"), "{bare:?}");
}

#[test]
fn scoped_arms_dedupe_shadowed_names_and_flag_recursion() {
    let shadowed = arms_of(
        "INSERT INTO t WITH v(a, b) AS (VALUES (1, 2)) SELECT * FROM v UNION ALL (WITH v(a, b) AS (VALUES (3, 4)) SELECT * FROM v)",
    );
    assert_eq!(shadowed.len(), 2);
    assert!(shadowed[1].starts_with("2:WITH "), "{shadowed:?}");
    assert!(shadowed[1].contains("VALUES (3, 4)"), "{shadowed:?}");
    assert!(!shadowed[1].contains("VALUES (1, 2)"), "{shadowed:?}");
    let recursive = arms_of("INSERT INTO t WITH RECURSIVE v AS (SELECT * FROM v) SELECT * FROM v");
    assert_eq!(recursive.len(), 1);
    assert!(
        recursive[0].starts_with("0:WITH RECURSIVE "),
        "{recursive:?}"
    );
}

#[test]
fn scoped_arms_with_case_twins_carry_no_sql() {
    let mut statements = Parser::parse_sql(
        &GenericDialect,
        "INSERT INTO t WITH xv AS (VALUES (1)) SELECT * FROM xv UNION ALL (WITH XV AS (VALUES (2)) SELECT * FROM XV)",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, false);
    assert_eq!(arms.len(), 2);
    assert_eq!(arms[1].positions.len(), 1);
    assert!(arms[1].arm_sql.is_none());
    let merged = arms_of(
        "INSERT INTO t WITH xv AS (VALUES (1)) SELECT * FROM xv UNION ALL (WITH XV AS (VALUES (2)) SELECT * FROM XV)",
    );
    assert_eq!(merged.len(), 2);
    assert!(merged[1].starts_with("1:WITH "), "{merged:?}");
    assert!(merged[1].contains("VALUES (2)"), "{merged:?}");
    assert!(!merged[1].contains("VALUES (1)"), "{merged:?}");
}

async fn plan_error(ctx: &SessionContext, sql: &str, case_insensitive: bool) -> DataFusionError {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state
        .sql_to_statement(sql, &dialect)
        .expect("pin SQL parses");
    repark_core::column_resolution::plan_statement_with_column_repair(
        &state,
        statement,
        case_insensitive,
    )
    .await
    .expect_err("pin SQL must fail to plan")
}

#[tokio::test]
async fn real_missing_column_matches_the_anchored_tag() {
    let ctx = mem_ctx();
    let error = plan_error(&ctx, "SELECT id, nosuch FROM t", true).await;
    assert!(is_resolution_error(&error), "{error}");
    assert!(
        matches!(error, DataFusionError::Plan(ref payload) if payload.starts_with("[UNRESOLVED_COLUMN")),
        "{error}"
    );
}

#[tokio::test]
async fn real_unknown_function_matches_invalid_function() {
    let ctx = mem_ctx();
    let error = plan_error(&ctx, "SELECT id, nosuchfn(s) FROM t", true).await;
    assert!(is_resolution_error(&error), "{error}");
}

#[tokio::test]
async fn real_missing_table_matches_unknown_table() {
    let ctx = mem_ctx();
    let error = plan_error(&ctx, "SELECT * FROM missing_table", true).await;
    assert!(is_resolution_error(&error), "{error}");
}

#[tokio::test]
async fn real_unknown_table_function_stays_a_judged_failure() {
    let ctx = mem_ctx();
    let error = plan_error(&ctx, "SELECT * FROM nosuchtvf()", true).await;
    assert!(!is_resolution_error(&error), "{error}");
}

#[tokio::test]
async fn real_ambiguity_tag_matches_the_text_fallback() {
    let ctx = mem_ctx_join();
    let error = plan_error(
        &ctx,
        "SELECT v.a, t.c FROM (VALUES ('x', 2)) AS v(a, b) JOIN t ON v.b = t.id JOIN u ON t.id = u.id WHERE c = c",
        true,
    )
    .await;
    assert!(is_ambiguity(&error), "{error}");
    assert!(
        matches!(error, DataFusionError::Plan(ref payload) if payload.starts_with("[AMBIGUOUS_REFERENCE]")),
        "{error}"
    );
}

#[tokio::test]
async fn real_coercion_failure_stays_a_judged_failure() {
    let ctx = mem_ctx();
    let error = plan_error(&ctx, "SELECT id, s + 1 FROM t", true).await;
    assert!(!is_resolution_error(&error), "{error}");
    assert!(!is_ambiguity(&error), "{error}");
}

fn mem_ctx_nullary() -> SessionContext {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::SessionConfig;
    use std::sync::Arc;

    let config = crate::extension::apply_spark_parser_dialect(SessionConfig::new());
    let ctx = SessionContext::new_with_config(config);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("localtimestamp", DataType::Utf8, false),
    ]);
    let table = MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("memory table registers");
    ctx
}

#[tokio::test]
async fn demoted_nullary_arm_types_as_its_column_not_the_function() {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;

    let ctx = mem_ctx_nullary();
    let catalogs = CatalogRegistry::new();
    let mut statements = Parser::parse_sql(
        &DatabricksDialect {},
        "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, coalesce(localtimestamp, localtimestamp) FROM t",
    )
    .unwrap();
    let Statement::Insert(insert) = statements.swap_remove(0) else {
        panic!("want an INSERT statement");
    };
    let source = insert.source.unwrap();
    let arms = resolve_insert_arms(&source, true);
    let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
    let cell = &arms[0].positions[1][0];
    let value = cell.rows[0].content[cell.column].clone();
    assert!(!judge.skip_string(1, &value).await);
    assert!(
        matches!(judge.plans.get(&1), Some(ArmPlan::Typed(types)) if types.get(1) == Some(&DataType::Utf8))
    );
}
