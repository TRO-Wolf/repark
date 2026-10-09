//! Guard-set tests cover each refusal and an acceptance path.

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

/// The message is generic and must not name an external system.
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

// === Guard 4 — BUG-001 MoR valve (resolution wrapper) =======================================

/// The wrapper passes statements outside the `MoR` valve's target scope.
#[tokio::test]
async fn mor_valve_wrapper_passes_what_it_cannot_or_must_not_gate() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let read_only = HashSet::new();
    let cx = repark_core::EngineContext::new(&ctx, &catalogs, &read_only);
    for sql in [
        // Not DML at all.
        "SELECT * FROM ice.sales.t",
        // DML the valve deliberately does not cover.
        "INSERT INTO ice.sales.t VALUES (1)",
        "MERGE INTO ice.sales.t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
        // Short names complete from the session defaults, which name no registered catalog.
        "DELETE FROM t WHERE id = 1",
        "UPDATE sales.t SET a = 1",
        // Three-part name, but no such catalog is registered.
        "DELETE FROM nosuch.sales.t WHERE id = 1",
    ] {
        let statement = datafusion::sql::sqlparser::parser::Parser::parse_sql(
            &datafusion::sql::sqlparser::dialect::GenericDialect {},
            sql,
        )
        .unwrap_or_else(|err| panic!("`{sql}` must parse: {err}"))
        .remove(0);
        refuse_mor_multi_spec_dml(&cx, &statement)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must pass the MoR valve: {err}"));
    }
}

// === Guard 5 — SEC-02 local filesystem ======================================================

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

/// Setting the conf opens the gate. Read it through `ConfigOptions::entries()` without
/// installing the functions extension.
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

/// A stand-in for the session's `repark.sql` config extension with the same prefix and field.
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

/// The composed guard set runs in order, with multi-statement refusal first.
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

    // Quote-aware: a `;` inside a literal is not a second statement; comments are also ignored.
    run_text_guards(
        &cx,
        "SELECT 'SELECT 1; INSERT INTO pg.public.t SELECT 1' AS x",
    )
    .expect("a `;` inside a literal is not a script");
}

// === Guard 6 — G3-E8 subquery-predicate DML valve ===========================================

/// Parse a statement with the same `Generic` dialect the router hands to the valve.
fn parsed(sql: &str) -> Statement {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

/// The valve fires on **every** subquery spelling, at any depth — and on nothing else.
#[test]
fn dml_subquery_valve_fires_on_every_spelling_and_no_other() {
    for sql in [
        "DELETE FROM t WHERE id IN (SELECT id FROM k)",
        "DELETE FROM t WHERE id NOT IN (SELECT id FROM k)",
        "DELETE FROM t WHERE NOT (id IN (SELECT id FROM k))",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM k)",
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM k)",
        "DELETE FROM t WHERE id > ANY (SELECT id FROM k)",
        "DELETE FROM t WHERE id > ALL (SELECT id FROM k)",
        "DELETE FROM t WHERE id = (SELECT max(id) FROM k)",
        "DELETE FROM t WHERE id = 1 OR id IN (SELECT id FROM k)",
        "DELETE FROM t WHERE id > 1 AND id IN (SELECT id FROM k)",
        "DELETE FROM t WHERE abs(id - (SELECT max(id) FROM k)) > 1",
        "DELETE FROM t WHERE CASE WHEN id IN (SELECT id FROM k) THEN true ELSE false END",
        "DELETE FROM t WHERE id IN (SELECT id FROM (SELECT id FROM k) AS x)",
        "UPDATE t SET name = 'z' WHERE id IN (SELECT id FROM k)",
        // Uncorrelated NOT EXISTS, EXISTS over an always-empty subquery, and aggregate predicates.
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM k)",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM k WHERE 1 = 0)",
        "DELETE FROM t WHERE id IN (SELECT max(id) FROM k)",
        // A subquery reached only through a CTE-bearing subquery body.
        "DELETE FROM t WHERE id IN (WITH c AS (SELECT id FROM k) SELECT id FROM c)",
    ] {
        let refusal = refuse_dml_subquery_predicate(&parsed(sql))
            .expect_err(&format!("the valve must fire on {sql:?}"))
            .to_string();
        assert!(
            refusal.contains("subquery predicates are silently mis-executed"),
            "must name the defect class, sql={sql:?}, got {refusal}"
        );
        assert!(
            refusal.contains("G3-E8") && refusal.contains("MERGE INTO"),
            "must name the defect id and the workaround, sql={sql:?}, got {refusal}"
        );
    }

    for sql in [
        "DELETE FROM t WHERE id = 2",
        "DELETE FROM t WHERE id IN (1, 2, 3)",
        "DELETE FROM t WHERE id BETWEEN 2 AND 3",
        "DELETE FROM t WHERE name LIKE 'b%' OR id = 3",
        "DELETE FROM t WHERE abs(id) > 1 AND name IS NOT NULL",
        "UPDATE t SET name = 'z' WHERE id = 2",
        // An assignment subquery is outside this valve: only `selection` is inspected.
        "UPDATE t SET name = (SELECT max(name) FROM k) WHERE id = 2",
        // No WHERE at all is the provider's genuine match-all — never refused.
        "DELETE FROM t",
        // Non-DML statements pass because the valve is statement-shaped.
        "SELECT id FROM t WHERE id IN (SELECT id FROM k)",
        "INSERT INTO t SELECT id FROM k WHERE id IN (SELECT id FROM k2)",
        // Three-part IN / NOT IN / [NOT] EXISTS skip the valve (product hole).
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt t WHERE EXISTS (SELECT 1 FROM ice.sales.keys k WHERE k.id = t.id)",
        "DELETE FROM ice.sales.tgt WHERE NOT EXISTS \
         (SELECT 1 FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
        "DELETE FROM ice.sales.tgt WHERE id IN \
         (SELECT k.id FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM ice.sales.keys)",
    ] {
        refuse_dml_subquery_predicate(&parsed(sql))
            .unwrap_or_else(|err| panic!("the valve must NOT fire on {sql:?}: {err}"));
    }
}

/// The refusal names the verb it refused, so a user is not told to rewrite the wrong statement.
#[test]
fn dml_subquery_refusal_names_its_verb_and_target() {
    let sql = "DELETE FROM ice.sales.t WHERE id = (SELECT max(id) FROM ice.sales.k)";
    let delete = refuse_dml_subquery_predicate(&parsed(sql))
        .unwrap_err()
        .to_string();
    assert!(delete.contains("DELETE with a subquery"), "{delete}");
    assert!(
        delete.contains("ice.sales.t"),
        "must name the target: {delete}"
    );
    assert!(delete.contains("delete EVERY row"), "{delete}");

    let sql = "UPDATE ice.sales.t SET name = 'z' WHERE id = (SELECT max(id) FROM ice.sales.k)";
    let update = refuse_dml_subquery_predicate(&parsed(sql))
        .unwrap_err()
        .to_string();
    assert!(update.contains("UPDATE with a subquery"), "{update}");
    assert!(update.contains("update EVERY row"), "{update}");
    assert!(
        update.contains("WHEN MATCHED THEN UPDATE SET"),
        "the workaround must name the UPDATE arm: {update}"
    );
}

/// The rendered target comes from the PARSED statement, not from the scrubbed text (F-C).
#[test]
fn dml_subquery_refusal_renders_a_usable_target_for_every_spelling() {
    for (sql, expected) in [
        (
            "DELETE FROM \"ice\".\"sales\".\"t\" WHERE id = (SELECT max(id) FROM ice.sales.k)",
            "\"ice\".\"sales\".\"t\"",
        ),
        (
            "UPDATE \"ice\".\"sales\".\"t\" SET name = 'z' \
             WHERE id = (SELECT max(id) FROM ice.sales.k)",
            "\"ice\".\"sales\".\"t\"",
        ),
        // FROM-less DELETE — the Spark spelling the Spark door's own router parse rejects.
        (
            "DELETE ice.sales.t WHERE id = (SELECT max(id) FROM ice.sales.k)",
            "ice.sales.t",
        ),
        // A comment between the verb and the target used to shift the text scan's word cursor.
        (
            "DELETE /* why */ FROM ice.sales.t WHERE id = (SELECT max(id) FROM ice.sales.k)",
            "ice.sales.t",
        ),
    ] {
        let refusal = refuse_dml_subquery_predicate(&parsed(sql))
            .unwrap_err()
            .to_string();
        assert!(
            refusal.contains(&format!("is refused on `{expected}`")),
            "the refusal must name the parsed target {expected}, sql={sql:?}, got {refusal}"
        );
        assert!(
            refusal.contains(&format!("MERGE INTO {expected} AS target")),
            "the workaround must be copy-pasteable, sql={sql:?}, got {refusal}"
        );
    }
}

/// This regression net ensures the router parses with the same dialect as execution.
#[test]
fn router_parse_dialect_matches_the_session_default() {
    let session_default = SessionConfig::new().options().sql_parser.dialect;
    assert_eq!(
        crate::router::PARSER_DIALECT,
        session_default,
        "this door's router parse and the parse `delegate` plans MUST be the same dialect — \
         otherwise a statement can be routed by one parser and executed by another"
    );
}

// === Guard — G15 collation refuse ===========================================================

fn assert_g15_refusal(error: &DataFusionError, requested: &str) {
    let text = error.to_string();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "G15 must be NotImplemented, got {error:?}"
    );
    assert!(
        text.contains(COLLATION_REFUSAL_NEEDLE),
        "must name the unimplemented class: {text}"
    );
    assert!(
        text.contains(requested),
        "must name the requested collation `{requested}`: {text}"
    );
    assert!(
        text.contains("binary/default"),
        "must steer to binary/default ordering: {text}"
    );
}

/// Expression `COLLATE` is detected on the parsed statement (the router's parse).
#[test]
fn collation_valve_fires_on_expression_collate() {
    let error = refuse_collation_in_statement(&parsed("SELECT 'Alice' COLLATE UTF8_LCASE"))
        .expect_err("expression COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// `ORDER BY … COLLATE` is a compare/order-changing path.
#[test]
fn collation_valve_fires_on_order_by_collate() {
    let error = refuse_collation_in_statement(&parsed(
        "SELECT name FROM t ORDER BY name COLLATE UNICODE_CI",
    ))
    .expect_err("ORDER BY COLLATE must refuse");
    assert_g15_refusal(&error, "UNICODE_CI");
}

/// Column-def `STRING COLLATE` is a collation request, not a generic unsupported option.
#[test]
fn collation_valve_fires_on_create_table_column_collate() {
    let error = refuse_collation_in_statement(&parsed(
        "CREATE TABLE ice.sales.t (name STRING COLLATE UTF8_LCASE)",
    ))
    .expect_err("CREATE TABLE column COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// A `COLLATE` token inside a string literal is not a collation request.
#[test]
fn collation_valve_ignores_collate_inside_a_literal() {
    refuse_collation_in_statement(&parsed("SELECT 'COLLATE UTF8_LCASE' AS note"))
        .unwrap_or_else(|error| panic!("literal must pass: {error}"));
}

/// Type-position CAST COLLATE is G15 (sqlparser cannot attach COLLATE inside CAST).
#[test]
fn collation_valve_fires_on_cast_as_string_collate() {
    let error =
        refuse_type_position_collation_in_sql("SELECT CAST('Alice' AS STRING COLLATE UTF8_LCASE)")
            .expect_err("CAST AS STRING COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// SET of a collation `SQLConf` key is a collation request (Q-004).
#[test]
fn collation_valve_fires_on_set_session_key() {
    let error = refuse_collation_in_statement(&parsed(
        "SET spark.sql.collation.objectLevel.enabled = true",
    ))
    .expect_err("SET collation key must refuse");
    assert_g15_refusal(&error, "spark.sql.collation.objectLevel.enabled");
}

/// Parenthesized SET is not discarded (SEC-003). Build it as an AST with dotted names.
#[test]
fn collation_valve_fires_on_parenthesized_set() {
    use datafusion::sql::sqlparser::ast::{Ident, ObjectName, Set};

    let statement = Statement::Set(Set::ParenthesizedAssignments {
        variables: vec![ObjectName::from(vec![
            Ident::new("spark"),
            Ident::new("sql"),
            Ident::new("collation"),
            Ident::new("schemaLevel"),
            Ident::new("enabled"),
        ])],
        values: vec![],
    });
    let error = refuse_collation_in_statement(&statement)
        .expect_err("parenthesized SET collation key must refuse");
    assert_g15_refusal(&error, "spark.sql.collation.schemaLevel.enabled");
}
