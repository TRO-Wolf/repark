//! Router-spine tests: one refuse test per TEMPORARY PR-2 refuse arm (each arm is restored by
//! the named phase-2 PR), plus passthrough sanity. The v1 lib-root integration battery (200
//! tests) rides PR-3b when the router completes — these tests are PR-2-native and pin only the
//! refuse surface this PR introduces.

use datafusion::prelude::SessionContext;
use repark_core::CatalogRegistry;

use crate::execute;

/// A bare context + empty registry — the refuse arms fire before any catalog lookup.
fn ctx() -> (SessionContext, CatalogRegistry) {
    (SessionContext::new(), CatalogRegistry::new())
}

/// Assert `sql` refuses loudly, naming both the construct fragment and the restoring PR.
async fn assert_refuses(sql: &str, construct_fragment: &str, restoring_pr: &str) {
    let (ctx, catalogs) = ctx();
    let error = execute(&ctx, &catalogs, sql)
        .await
        .expect_err("the PR-2 refuse arm must fire")
        .to_string();
    assert!(
        error.contains(construct_fragment),
        "error must name the construct {construct_fragment:?}: {error}"
    );
    assert!(
        error.contains(restoring_pr),
        "error must name the restoring PR {restoring_pr:?}: {error}"
    );
    assert!(
        error.contains("lands in phase-2"),
        "error must mark the refusal as temporary: {error}"
    );
}

#[tokio::test]
async fn refuses_ctas_until_pr3a() {
    assert_refuses(
        "CREATE TABLE ice.ns.t AS SELECT 1 AS v",
        "CREATE TABLE … AS SELECT (CTAS)",
        "PR-3a",
    )
    .await;
}

#[tokio::test]
async fn refuses_column_def_create_table_until_pr3a() {
    assert_refuses(
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg",
        "column-def CREATE TABLE",
        "PR-3a",
    )
    .await;
}

#[tokio::test]
async fn refuses_drop_table_until_pr3a() {
    assert_refuses("DROP TABLE ice.ns.t", "DROP TABLE", "PR-3a").await;
}

#[tokio::test]
async fn refuses_drop_namespace_until_pr3a() {
    assert_refuses(
        "DROP SCHEMA ice.ns",
        "DROP NAMESPACE | DATABASE | SCHEMA",
        "PR-3a",
    )
    .await;
    assert_refuses(
        "DROP DATABASE ice.ns",
        "DROP NAMESPACE | DATABASE | SCHEMA",
        "PR-3a",
    )
    .await;
}

#[tokio::test]
async fn refuses_create_namespace_spellings_until_pr3a() {
    for sql in [
        "CREATE NAMESPACE ice.ns",
        "CREATE SCHEMA ice.ns",
        "CREATE DATABASE ice.ns",
        "CREATE NAMESPACE IF NOT EXISTS ice.ns LOCATION '/tmp/x'",
    ] {
        assert_refuses(sql, "CREATE NAMESPACE | DATABASE | SCHEMA", "PR-3a").await;
    }
}

#[tokio::test]
async fn refuses_alter_forms_until_pr3a() {
    // Parseable ALTER TABLE, the I7 partition-field form, and an I6 residual all refuse via the
    // one pre-parse ALTER sniff (their v1 recognizers live in the PR-3a `alter` module).
    for sql in [
        "ALTER TABLE ice.ns.t SET TBLPROPERTIES ('k'='v')",
        "ALTER TABLE ice.ns.t ADD PARTITION FIELD month(ts)",
        "ALTER TABLE ice.ns.t WRITE ORDERED BY id",
        "alter table ice.ns.t RENAME TO ice.ns.u",
    ] {
        assert_refuses(sql, "ALTER TABLE", "PR-3a").await;
    }
}

#[tokio::test]
async fn refuses_merge_until_pr3b() {
    // Parseable MERGE hits the statement arm; the star form (unparsable without the PR-3b
    // rewrite) hits the fallthrough — both name the same construct + PR.
    assert_refuses(
        "MERGE INTO ice.ns.t AS t USING ice.ns.s AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.v",
        "MERGE INTO",
        "PR-3b",
    )
    .await;
    assert_refuses(
        "MERGE INTO ice.ns.t AS t USING ice.ns.s AS s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT *",
        "MERGE INTO",
        "PR-3b",
    )
    .await;
}

#[tokio::test]
async fn refuses_insert_overwrite_until_pr3b() {
    assert_refuses(
        "INSERT OVERWRITE ice.ns.t SELECT * FROM ice.ns.s",
        "INSERT OVERWRITE",
        "PR-3b",
    )
    .await;
}

#[tokio::test]
async fn refuses_call_until_pr3b() {
    assert_refuses(
        "CALL ice.system.expire_snapshots(table => 'ns.t')",
        "CALL",
        "PR-3b",
    )
    .await;
}

#[tokio::test]
async fn refuses_branch_tag_ddl_until_pr3b() {
    for sql in [
        "CREATE BRANCH audit IN ice.ns.t",
        "DROP TAG v1 IN ice.ns.t",
        "REPLACE BRANCH audit IN ice.ns.t",
    ] {
        assert_refuses(sql, "BRANCH|TAG", "PR-3b").await;
    }
}

#[tokio::test]
async fn truncate_refusal_is_verbatim_v1() {
    // TRUNCATE is a v1 targeted refuse (C4-L-001), not a PR-2 temporary arm — its message steers
    // to the documented workarounds instead of naming a restoring PR.
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
    // BUG-010 ordering: the multi-statement gate runs before any PR-2 refuse arm, so a script
    // refuses as a parse-class error, not as a NotImplemented construct refusal.
    let (ctx, catalogs) = ctx();
    let error = execute(&ctx, &catalogs, "SELECT 1; DROP TABLE ice.ns.t")
        .await
        .expect_err("multi-statement must refuse")
        .to_string();
    assert!(!error.contains("lands in phase-2"), "{error}");
}
