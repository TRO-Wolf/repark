//! Router-spine tests for passthrough and gate ordering outside the main unit battery.

use datafusion::prelude::SessionContext;
use repark_core::CatalogRegistry;

use crate::execute;

/// A bare context + empty registry — the refuse arms fire before any catalog lookup.
fn ctx() -> (SessionContext, CatalogRegistry) {
    (SessionContext::new(), CatalogRegistry::new())
}

#[test]
fn source_locations_depend_on_rewrite_bytes_not_buffer_ownership() {
    let original = "SELECT '\\n' AS shifted, )";
    let canonical = "SELECT '\n' AS shifted, )";
    let owned_same_bytes = canonical.to_string();
    assert_eq!(
        super::original_sql_for_locations(original, canonical, &owned_same_bytes),
        Some(original)
    );
    assert_eq!(
        super::original_sql_for_locations(original, canonical, "SELECT 1"),
        None
    );
}

#[tokio::test]
async fn truncate_refusal_is_verbatim_v1() {
    // TRUNCATE is a targeted refusal with documented workarounds.
    let (ctx, catalogs) = ctx();
    let error = execute(&ctx, &catalogs, "TRUNCATE TABLE ice.ns.t")
        .await
        .expect_err("TRUNCATE must refuse")
        .to_string();
    assert!(
        error.contains("TRUNCATE TABLE is not supported yet"),
        "{error}"
    );
    assert!(!error.contains("lands in phase-2"), "{error}");
}

#[tokio::test]
async fn select_passthrough_still_executes() {
    // The refuse arms must not swallow the passthrough: a plain SELECT plans and executes.
    let (ctx, catalogs) = ctx();
    let batches = execute(&ctx, &catalogs, "SELECT 1 AS v")
        .await
        .expect("SELECT must pass through")
        .collect()
        .await
        .expect("collect");
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn read_only_set_reaches_p11_refusal() {
    // The positional `read_only_catalogs` argument (the seam's `EngineContext::read_only`
    // field) drives the P11 direction-note on the INSERT passthrough path.
    let (ctx, catalogs) = ctx();
    let read_only: std::collections::HashSet<String> = ["pg".to_string()].into_iter().collect();
    let error = crate::execute_with_read_only(
        &ctx,
        &catalogs,
        "INSERT INTO pg.public.t VALUES (1)",
        &read_only,
    )
    .await
    .expect_err("P11 must refuse")
    .to_string();
    assert!(error.contains("read-only"), "{error}");
}

#[tokio::test]
async fn multi_statement_still_refuses_before_refuse_arms() {
    // The multi-statement gate precedes all statement-specific refusal arms.
    let (ctx, catalogs) = ctx();
    let error = execute(&ctx, &catalogs, "SELECT 1; DROP TABLE ice.ns.t")
        .await
        .expect_err("multi-statement must refuse")
        .to_string();
    assert!(!error.contains("lands in phase-2"), "{error}");
}
