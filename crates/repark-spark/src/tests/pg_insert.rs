use std::collections::HashSet;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use repark_core::{CatalogRegistry, ReparkSession, ReparkSessionBuilder};
use tempfile::TempDir;

use crate::write_options::StatementWriteOptions;

const MOUNT: &str = "[default.database.postgres.pg]\nhost = \"203.0.113.1\"\n";

fn spark_context() -> SessionContext {
    let config = repark_functions::cardinality::with_repark_sql_config(
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new()),
        repark_functions::cardinality::ReparkSqlSettings::default(),
    );
    let config = repark_functions::ansi::with_spark_ansi_config(config, true);
    let config = repark_functions::session_time_zone::with_session_time_zone(config, "UTC");
    let config = repark_core::with_session_owner(config, repark_core::session_owner_snapshot());
    let config = repark_functions::case_sensitive::with_spark_case_sensitive_config(config, false);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    ctx.register_udf(crate::spark_as_udf().as_ref().clone());
    ctx.register_udf(crate::suffix_literal_udf().as_ref().clone());
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

fn mounted_session(toml: &str) -> (TempDir, ReparkSession) {
    let directory = TempDir::new().expect("config fixture directory");
    let path = directory.path().join("repark.toml");
    std::fs::write(&path, toml).expect("fixture file");
    let session = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file-built session");
    session
        .register_configured_sources()
        .expect("source registration");
    (directory, session)
}

fn options(pairs: &[(&str, &str)]) -> StatementWriteOptions {
    StatementWriteOptions::validate(
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    )
    .expect("options validate")
}

async fn run(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    read_only: &HashSet<String>,
    options: &StatementWriteOptions,
    sql: &str,
) -> datafusion::error::Result<datafusion::prelude::DataFrame> {
    crate::execute_with_statement_options(ctx, catalogs, sql, read_only, options).await
}

#[tokio::test]
async fn pg_append_routes_to_the_driver_not_the_default_hook() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        "INSERT INTO pg.public.t VALUES (1)",
    )
    .await
    .expect_err("a user-less source refuses in the driver");
    let message = error.to_string();
    assert!(message.contains("database source `pg`"), "{message}");
    assert!(
        !message.contains("not implemented for this table"),
        "{message}"
    );
}

#[tokio::test]
async fn pg_overwrite_names_the_modes_row() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        "INSERT OVERWRITE pg.public.t SELECT 1",
    )
    .await
    .expect_err("overwrite refuses");
    let message = error.to_string();
    assert!(message.contains("CONNECT-DECL-pg-write-modes"), "{message}");
}

#[tokio::test]
async fn pg_update_names_the_upsert_row() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        "UPDATE pg.public.t SET a = 1",
    )
    .await
    .expect_err("update refuses");
    let message = error.to_string();
    assert!(
        message.contains("CONNECT-DECL-pg-write-upsert"),
        "{message}"
    );
}

#[tokio::test]
async fn pg_insert_without_a_source_refuses_like_datafusion() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        "INSERT INTO pg.public.t DEFAULT VALUES",
    )
    .await
    .expect_err("a source-less insert refuses");
    assert!(
        error
            .to_string()
            .contains("Inserts without a source not supported"),
        "{error}"
    );
}

#[tokio::test]
async fn unknown_catalog_insert_keeps_today_refusal() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        "INSERT INTO nosuch.s.t SELECT 1",
    )
    .await
    .expect_err("an unknown catalog refuses");
    assert!(
        error.to_string().contains("table 'nosuch.s.t' not found"),
        "{error}"
    );
}

#[tokio::test]
async fn two_part_pg_name_falls_through_unchanged() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        "INSERT INTO pg.t SELECT 1",
    )
    .await
    .expect_err("a two-part name refuses");
    let message = error.to_string();
    assert!(
        message.contains("table 'datafusion.pg.t' not found"),
        "{message}"
    );
    assert!(!message.contains("database source `pg`"), "{message}");
}

#[tokio::test]
async fn read_only_set_with_specs_lets_insert_through_and_keeps_update_refused() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let read_only: HashSet<String> = ["pg".to_string()].into_iter().collect();
    let insert = run(
        &ctx,
        &catalogs,
        &read_only,
        &StatementWriteOptions::empty(),
        "INSERT INTO pg.public.t VALUES (1)",
    )
    .await
    .expect_err("insert reaches the driver past P11");
    assert!(
        insert.to_string().contains("database source `pg`"),
        "{insert}"
    );
    let update = run(
        &ctx,
        &catalogs,
        &read_only,
        &StatementWriteOptions::empty(),
        "UPDATE pg.public.t SET a = 1",
    )
    .await
    .expect_err("update still refuses at P11");
    let message = update.to_string();
    assert!(message.contains("is read-only"), "{message}");
    assert!(
        !message.contains("CONNECT-DECL-pg-write-upsert"),
        "{message}"
    );
}

#[tokio::test]
async fn write_path_row_still_routes_to_the_driver() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &options(&[("write.path", "row")]),
        "INSERT INTO pg.public.t VALUES (1)",
    )
    .await
    .expect_err("a user-less source refuses in the driver");
    let message = error.to_string();
    assert!(message.contains("database source `pg`"), "{message}");
}

#[tokio::test]
async fn write_path_bad_value_refuses_naming_both_values() {
    let (_directory, session) = mounted_session(MOUNT);
    let catalogs = session.catalogs_snapshot();
    let ctx = spark_context();
    let error = run(
        &ctx,
        &catalogs,
        &HashSet::new(),
        &options(&[("write.path", "columnar")]),
        "INSERT INTO pg.public.t VALUES (1)",
    )
    .await
    .expect_err("a bad write.path refuses");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "{error:?}"
    );
    let message = error.to_string();
    assert!(message.contains("'bulk'"), "{message}");
    assert!(message.contains("'row'"), "{message}");
}
