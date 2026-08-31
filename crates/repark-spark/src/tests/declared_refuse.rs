//! FNP-15 — Spark-door declared-absent function refusals.
//!
//! pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-017

use super::super::*;
#[allow(unused_imports)]
use super::common::*;

fn assert_fnp15_refusal(error: &DataFusionError, name: &str, needle: &str) {
    let text = error.to_string();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{name} must be NotImplemented, got {error:?}"
    );
    assert!(text.contains(name), "must name {name}: {text}");
    assert!(text.contains("unreachable"), "must say unreachable: {text}");
    assert!(text.contains(needle), "must name {needle}: {text}");
    assert!(
        !text.contains("deferred by cost"),
        "FNP-15 is unreachable, not deferred by cost: {text}"
    );
}

#[tokio::test]
async fn java_method_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT java_method(1)")
        .await
        .expect_err("java_method must refuse");
    assert_fnp15_refusal(&error, "java_method", "reflection");
}

#[tokio::test]
async fn reflect_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT reflect(1)")
        .await
        .expect_err("reflect must refuse");
    assert_fnp15_refusal(&error, "reflect", "CallMethodViaReflection");
}

#[tokio::test]
async fn try_reflect_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT try_reflect(1)")
        .await
        .expect_err("try_reflect must refuse");
    assert_fnp15_refusal(&error, "try_reflect", "exception-to-NULL");
}

#[tokio::test]
async fn unwrap_udt_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT unwrap_udt(1)")
        .await
        .expect_err("unwrap_udt must refuse");
    assert_fnp15_refusal(&error, "unwrap_udt", "UserDefinedType");
}

#[tokio::test]
async fn input_file_block_start_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT input_file_block_start()")
        .await
        .expect_err("input_file_block_start must refuse");
    assert_fnp15_refusal(&error, "input_file_block_start", "InputFileBlockHolder");
}

#[tokio::test]
async fn input_file_block_length_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT input_file_block_length()")
        .await
        .expect_err("input_file_block_length must refuse");
    assert_fnp15_refusal(&error, "input_file_block_length", "InputFileBlockHolder");
}

#[tokio::test]
async fn execute_passthrough_attaches_declared_refuse_valve() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = crate::spark_ast::execute_passthrough(&ctx, &catalogs, "SELECT java_method(1)")
        .await
        .expect_err("execute_passthrough must refuse without the router");
    assert_fnp15_refusal(&error, "java_method", "reflection");
}

#[test]
fn spark_ast_source_attaches_declared_refuse_valve() {
    let source = include_str!("../spark_ast.rs");
    let attached = source.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with("crate::refuse_declared_function_in_statement")
            && !trimmed.starts_with("//")
    });
    assert!(
        attached,
        "spark_ast execute_passthrough must call refuse_declared_function_in_statement"
    );
}

#[test]
fn abs_sql_is_not_a_declared_refusal() {
    crate::refuse_declared_function_in_sql("SELECT abs(-1)")
        .expect("abs must not trip the declared-refuse valve");
}

#[tokio::test]
async fn hll_sketch_agg_is_deferred_by_cost() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT hll_sketch_agg(1)")
        .await
        .expect_err("hll_sketch_agg must refuse");
    let text = error.to_string();
    assert!(matches!(error, DataFusionError::NotImplemented(_)));
    assert!(text.contains("deferred by cost"));
    assert!(text.contains("reachable without a JVM"));
    assert!(!text.contains("unreachable"));
}

fn ctx_passthrough() -> (SessionContext, CatalogRegistry) {
    (SessionContext::new(), CatalogRegistry::new())
}
