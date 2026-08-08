//! Guard-set tests. Every guard is a behavior and every REFUSAL is a behavior, so each refusal
//! message class gets its own test alongside the acceptance case that proves the guard is not
//! simply refusing everything.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::Catalog;
use repark_core::{CatalogRegistry, LocationPolicy};
use tempfile::TempDir;

use super::*;
use crate::scan::blank_out_quoted_and_comments;

fn scrub(sql: &str) -> String {
    blank_out_quoted_and_comments(sql)
}

// === Guard 1 — multi-statement ==============================================================

/// A single statement, with any combination of trailing `;`, whitespace, and comments, passes.
#[test]
fn single_statement_with_trailing_noise_is_allowed() {
    for sql in [
        "SELECT 1",
        "SELECT 1;",
        "SELECT 1;   ",
        "SELECT 1; -- done",
        "SELECT 1; /* done */",
        "SELECT 1;;",
        "  -- lead\n SELECT 1 ;\n",
    ] {
        refuse_multi_statement(&scrub(sql))
            .unwrap_or_else(|err| panic!("`{sql}` must pass: {err}"));
    }
}

/// Two real statements refuse, in the Spark error class.
#[test]
fn two_statements_refuse_with_parse_syntax_error_class() {
    let err = refuse_multi_statement(&scrub("SELECT 1; SELECT 2"))
        .expect_err("two statements must refuse");
    let text = err.to_string();
    assert!(
        text.contains("[PARSE_SYNTAX_ERROR]"),
        "must carry Spark's class: {text}"
    );
    assert!(
        text.contains("multiple SQL statements"),
        "must name the class: {text}"
    );
    assert!(
        matches!(err, DataFusionError::SQL(_, _)),
        "must classify as a parse error, got {err:?}"
    );
}

/// Fail-closed: content after `;` refuses even when that content would not parse.
#[test]
fn unparsable_second_statement_still_refuses() {
    refuse_multi_statement(&scrub("SELECT 1; XYZZY 2")).expect_err("fail-closed refuse");
}

/// A `;` inside a string literal or a comment is NOT a statement separator.
#[test]
fn semicolon_inside_literal_or_comment_is_not_multi_statement() {
    for sql in [
        "SELECT 'a; b' AS x",
        "SELECT 1 -- a; b",
        "SELECT /* a; b */ 1",
        r#"SELECT * FROM "we;ird""#,
        "SELECT 'it''s; fine' AS x",
    ] {
        refuse_multi_statement(&scrub(sql))
            .unwrap_or_else(|err| panic!("`{sql}` must not be multi-statement: {err}"));
    }
}

// === Guard 2 — P11 read-only catalog ========================================================

fn read_only_registry(name: &str) -> CatalogRegistry {
    let mut registry = CatalogRegistry::new();
    registry.set_read_only_catalogs(HashSet::from([name.to_string()]));
    registry
}

/// Every DML verb against a read-only catalog refuses, with the generic message.
#[test]
fn read_only_catalog_dml_refuses_generically() {
    let catalogs = read_only_registry("pg");
    for (sql, verb) in [
        ("INSERT INTO pg.public.t SELECT 1", "INSERT"),
        ("INSERT OVERWRITE pg.public.t SELECT 1", "INSERT"),
        ("UPDATE pg.public.t SET a = 1", "UPDATE"),
        ("DELETE FROM pg.public.t WHERE a = 1", "DELETE"),
        ("MERGE INTO pg.public.t USING s ON t.a = s.a", "MERGE"),
    ] {
        let err = refuse_read_only_catalog_dml(&catalogs, &scrub(sql))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`pg`"),
            "must name the catalog for `{sql}`: {err}"
        );
        assert!(
            err.contains("registered read-only"),
            "must name the reason for `{sql}`: {err}"
        );
        assert!(err.contains(verb), "must name the verb for `{sql}`: {err}");
        assert!(
            err.contains("MERGE INTO"),
            "must steer to the supported direction for `{sql}`: {err}"
        );
    }
}

/// The message is GENERIC — it must not name a specific external system (this door is not
/// postgres-flavoured; that wording belongs to the Spark door's ported P11 text).
#[test]
fn read_only_message_is_generic() {
    let message = read_only_catalog_message("pg", "INSERT");
    let lower = message.to_lowercase();
    assert!(
        !lower.contains("postgres"),
        "the ANSI door's P11 message must stay generic: {message}"
    );
}

/// A read from a read-only catalog, and any DML against a writable one, pass.
#[test]
fn reads_and_writable_catalog_dml_pass() {
    let catalogs = read_only_registry("pg");
    for sql in [
        "SELECT * FROM pg.public.t",
        "INSERT INTO ice.sales.t SELECT 1",
        "UPDATE ice.sales.t SET a = 1",
        "CREATE TABLE pg.public.t AS SELECT 1",
    ] {
        refuse_read_only_catalog_dml(&catalogs, &scrub(sql))
            .unwrap_or_else(|err| panic!("`{sql}` must pass this guard: {err}"));
    }
}

/// A read-only catalog NAMED inside a string literal cannot trigger the guard.
#[test]
fn read_only_guard_ignores_string_literals() {
    let catalogs = read_only_registry("pg");
    refuse_read_only_catalog_dml(&catalogs, &scrub("SELECT 'INSERT INTO pg.public.t' AS x"))
        .expect("a literal is not a statement");
}

// === Guard 3 — write-to-branch ==============================================================

async fn ctx_with_table() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.sql("CREATE OR REPLACE VIEW daily AS SELECT 1 AS a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx
}

/// A four-part write target is unambiguously a ref suffix — always refuses.
#[tokio::test]
async fn write_to_branch_refuses() {
    let ctx = SessionContext::new();
    let err = refuse_write_to_branch(
        &ctx,
        &scrub("INSERT INTO ice.sales.t.branch_audit SELECT 1"),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("snapshot ref"), "must name the class: {err}");
    assert!(
        err.contains("`main`"),
        "must explain the silent-main risk: {err}"
    );
    assert!(
        err.contains("ice.sales.t"),
        "must name the table to write instead: {err}"
    );
}

/// A four-part target refuses regardless of the ref-name spelling (a tag has no `branch_`).
#[tokio::test]
async fn four_part_target_refuses_for_tag_spelling_too() {
    let ctx = SessionContext::new();
    refuse_write_to_branch(&ctx, &scrub("INSERT INTO ice.sales.t.release_v1 SELECT 1"))
        .expect_err("four-part target must refuse");
}

/// Two-part `x.branch_y` refuses only when the full name does NOT resolve and the prefix does.
#[tokio::test]
async fn two_part_branch_refuses_only_when_ambiguity_resolves_to_a_ref() {
    let ctx = ctx_with_table().await;
    // `daily` exists, `daily.branch_x` does not → this is the Spark `t.branch_<name>` spelling.
    refuse_write_to_branch(&ctx, &scrub("INSERT INTO daily.branch_x SELECT 1"))
        .expect_err("prefix resolves, full name does not → refuse");

    // Neither resolves → fall through so planning's own "table not found" is the error.
    refuse_write_to_branch(&ctx, &scrub("INSERT INTO nosuch.branch_x SELECT 1"))
        .expect("neither resolves → fall through to planning");
}

/// A REAL two-part table whose leaf is literally named `branch_…` is not a ref write.
#[tokio::test]
async fn real_table_named_branch_something_is_not_refused() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE OR REPLACE VIEW branch_daily AS SELECT 1 AS a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // `public.branch_daily` resolves as a real table → no refuse.
    refuse_write_to_branch(&ctx, &scrub("INSERT INTO public.branch_daily SELECT 1"))
        .expect("a real table named branch_* must not be refused");
}

/// Ordinary writes and reads pass the branch guard.
#[tokio::test]
async fn ordinary_writes_pass_branch_guard() {
    let ctx = SessionContext::new();
    for sql in [
        "INSERT INTO ice.sales.t SELECT 1",
        "SELECT * FROM ice.sales.t.branch_audit",
        "UPDATE ice.sales.t SET a = 1",
    ] {
        refuse_write_to_branch(&ctx, &scrub(sql))
            .unwrap_or_else(|err| panic!("`{sql}` must pass: {err}"));
    }
}

// === Guard 4 — SEC-02 local filesystem ======================================================

async fn plan_of(ctx: &SessionContext, sql: &str) -> LogicalPlan {
    ctx.state().create_logical_plan(sql).await.unwrap()
}

fn registry_with_warehouse(root: &std::path::Path) -> CatalogRegistry {
    let mut catalogs = CatalogRegistry::new();
    catalogs.note_local_warehouse_root(root.to_string_lossy());
    catalogs
}

/// `COPY TO` a local path outside every warehouse root refuses, naming the conf key.
#[tokio::test]
async fn local_filesystem_plan_refuses() {
    let warehouse = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("leak.parquet");

    let ctx = SessionContext::new();
    let catalogs = registry_with_warehouse(warehouse.path());
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
        dest.to_str().unwrap()
    );
    let plan = plan_of(&ctx, &sql).await;
    let err = refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
        "must name the conf: {err}"
    );
    assert!(err.contains("COPY TO"), "must name the surface: {err}");
}

/// The classic sensitive path, and every case variant of the `file:` scheme, refuse.
#[tokio::test]
async fn sensitive_path_and_file_scheme_case_variants_refuse() {
    let warehouse = TempDir::new().unwrap();
    let ctx = SessionContext::new();
    let catalogs = registry_with_warehouse(warehouse.path());
    for location in [
        "/etc/passwd",
        "file:///etc/passwd",
        "FILE:///etc/passwd",
        "File://localhost/etc/passwd",
    ] {
        let sql = format!("COPY (SELECT 1 AS a) TO '{location}' STORED AS PARQUET");
        let plan = plan_of(&ctx, &sql).await;
        let err = refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
            .expect_err("a sensitive local path must refuse")
            .to_string();
        assert!(
            err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
            "`{location}` must name the conf: {err}"
        );
    }
}

/// `CREATE EXTERNAL TABLE … LOCATION` is gated too — not only `COPY TO`.
#[tokio::test]
async fn create_external_table_outside_warehouse_refuses() {
    let warehouse = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let location = outside.path().join("ext");

    let ctx = SessionContext::new();
    let catalogs = registry_with_warehouse(warehouse.path());
    let sql = format!(
        "CREATE EXTERNAL TABLE blocked (a INT) STORED AS PARQUET LOCATION '{}'",
        location.to_str().unwrap()
    );
    let plan = plan_of(&ctx, &sql).await;
    let err = refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("CREATE EXTERNAL TABLE"),
        "must name the surface: {err}"
    );
    assert!(
        err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
        "must name the conf: {err}"
    );
}

/// A path UNDER a registered warehouse root is grandfathered (offline workflows keep working).
#[tokio::test]
async fn path_under_warehouse_root_is_grandfathered() {
    let warehouse = TempDir::new().unwrap();
    let dest = warehouse.path().join("exported");
    let ctx = SessionContext::new();
    let catalogs = registry_with_warehouse(warehouse.path());
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
        dest.to_str().unwrap()
    );
    let plan = plan_of(&ctx, &sql).await;
    refuse_local_filesystem_plan(&ctx, &catalogs, &plan).expect("under warehouse → allowed");
}

/// `..` cannot escape the warehouse root through the grandfather check.
#[tokio::test]
async fn path_traversal_out_of_warehouse_refuses() {
    let warehouse = TempDir::new().unwrap();
    let escape = warehouse.path().join("..").join("escaped.parquet");
    let ctx = SessionContext::new();
    let catalogs = registry_with_warehouse(warehouse.path());
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
        escape.to_str().unwrap()
    );
    let plan = plan_of(&ctx, &sql).await;
    refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
        .expect_err("`..` must not grandfather out of the warehouse");
}

/// Remote schemes are out of this gate's scope.
#[tokio::test]
async fn remote_locations_are_not_gated() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let plan = plan_of(
        &ctx,
        "COPY (SELECT 1 AS a) TO 's3://bucket/key' STORED AS PARQUET",
    )
    .await;
    refuse_local_filesystem_plan(&ctx, &catalogs, &plan).expect("s3:// is out of scope");
}

/// Setting the conf opens the gate — read through `ConfigOptions::entries()`, with no
/// `repark-functions` dependency (the reachability finding recorded in the ledger).
#[tokio::test]
async fn conf_true_opens_the_gate() {
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("ok.parquet");

    let mut config = SessionConfig::new();
    config
        .options_mut()
        .extensions
        .insert(TestAllowConfig { allow: true });
    let ctx = SessionContext::new_with_config(config);
    let catalogs = CatalogRegistry::new();
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
        dest.to_str().unwrap()
    );
    let plan = plan_of(&ctx, &sql).await;
    refuse_local_filesystem_plan(&ctx, &catalogs, &plan).expect("conf true must open the gate");
}

/// A stand-in for the session's `repark.sql` config extension, carrying the SAME prefix + field
/// name the Spark door's `ReparkSqlConfig` registers. If either side renames the knob, this test
/// stops opening the gate and the divergence is caught here rather than in production.
#[derive(Debug, Clone, Default)]
struct TestAllowConfig {
    allow: bool,
}

impl datafusion::common::config::ConfigExtension for TestAllowConfig {
    const PREFIX: &'static str = "repark.sql";
}

impl datafusion::common::config::ExtensionOptions for TestAllowConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn cloned(&self) -> Box<dyn datafusion::common::config::ExtensionOptions> {
        Box::new(self.clone())
    }
    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        if key == "allow_local_filesystem_ddl" {
            self.allow = value.eq_ignore_ascii_case("true");
        }
        Ok(())
    }
    fn entries(&self) -> Vec<datafusion::common::config::ConfigEntry> {
        vec![datafusion::common::config::ConfigEntry {
            key: "repark.sql.allow_local_filesystem_ddl".to_string(),
            value: Some(self.allow.to_string()),
            description: "test stand-in for the session's SEC-02 gate conf",
        }]
    }
}

/// The default (no extension installed at all) is CLOSED — absence must never read as "allow".
#[tokio::test]
async fn missing_conf_extension_is_closed() {
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("leak.parquet");
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let sql = format!(
        "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
        dest.to_str().unwrap()
    );
    let plan = plan_of(&ctx, &sql).await;
    refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
        .expect_err("absent conf must be closed, not open");
}

/// Non-DDL plans are untouched by the gate.
#[tokio::test]
async fn ordinary_select_is_untouched() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let plan = plan_of(&ctx, "SELECT 1 AS a").await;
    refuse_local_filesystem_plan(&ctx, &catalogs, &plan).expect("a SELECT is not gated");
}

// === The guard-set entry point ==============================================================

/// The composed guard set runs in the mandated order: multi-statement first, so a script whose
/// SECOND statement targets a read-only catalog reports the multi-statement class, not P11.
#[tokio::test]
async fn multi_statement_refuses_first_and_quote_aware() {
    let warehouse = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> =
        repark_iceberg::catalog::memory_catalog(warehouse.path().to_str().unwrap())
            .await
            .unwrap();
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        catalog,
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.path().to_path_buf(),
        },
    );
    catalogs.set_read_only_catalogs(HashSet::from(["pg".to_string()]));

    let ctx = SessionContext::new();
    let read_only = HashSet::from(["pg".to_string()]);
    let cx = EngineContext::new(&ctx, &catalogs, &read_only);
    let err = run_text_guards(&cx, "SELECT 1; INSERT INTO pg.public.t SELECT 1")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("[PARSE_SYNTAX_ERROR]"),
        "multi-statement must win the ordering: {err}"
    );

    // Quote-aware: a `;` inside a literal is not a second statement, so the SAME text with the
    // script quoted passes the guard set entirely.
    run_text_guards(
        &cx,
        "SELECT 'SELECT 1; INSERT INTO pg.public.t SELECT 1' AS x",
    )
    .expect("a `;` inside a literal is not a script");
}
