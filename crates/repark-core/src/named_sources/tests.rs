use std::path::PathBuf;

use tempfile::TempDir;

use crate::ReparkSession;
use crate::session::ReparkSessionBuilder;

const UNROUTABLE_SOURCE: &str = "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\n";

fn staged_config(text: &str) -> (TempDir, PathBuf) {
    let directory = TempDir::new().expect("config fixture directory");
    let path = directory.path().join("repark.toml");
    std::fs::write(&path, text).expect("fixture file");
    (directory, path)
}

fn session_with_source(text: &str) -> (TempDir, ReparkSession) {
    let (directory, path) = staged_config(text);
    let session = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file-built session");
    (directory, session)
}

#[tokio::test]
async fn configured_source_select_refuses_with_connector_message() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    let error = session
        .sql("SELECT * FROM company_db.public.t")
        .await
        .expect_err("using an unimplemented source must refuse");
    let message = error.to_string();
    assert!(message.contains("company_db"), "{message}");
    assert!(message.contains("postgres"), "{message}");
    assert!(message.contains("1.10"), "{message}");
    assert!(!message.contains("not found"), "{message}");
    assert!(!message.contains("does not exist"), "{message}");
}

#[tokio::test]
async fn configured_source_create_table_refuses_with_connector_message() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    for sql in [
        "CREATE TABLE company_db.public.t (a INT)",
        "DROP TABLE company_db.public.t",
    ] {
        let error = session
            .sql(sql)
            .await
            .expect_err("a write shape under a source name must refuse");
        let message = error.to_string();
        assert!(message.contains("company_db"), "{sql}: {message}");
        assert!(message.contains("postgres"), "{sql}: {message}");
        assert!(message.contains("1.10"), "{sql}: {message}");
        assert!(!message.contains("not found"), "{sql}: {message}");
    }
}

#[test]
fn source_registration_opens_no_connection() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("registration over an unroutable host must not connect");
    let rows = session.sources();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "company_db");
}

#[test]
fn source_ping_refuses_until_connector() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    let source = session
        .source("company_db")
        .expect("declared source handle");
    let error = source
        .ping()
        .expect_err("ping must refuse until connectors land");
    let message = error.to_string();
    assert!(message.contains("company_db"), "{message}");
    assert!(message.contains("postgres"), "{message}");
    assert!(message.contains("1.10"), "{message}");
}

#[test]
fn sources_listing_names_kind_profile_and_redacts_secrets() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\npassword = \"s3cret\"\n",
    );
    let rows = session.sources();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.name, "company_db");
    assert_eq!(row.kind, "postgres");
    assert_eq!(row.key_path, "default.database.postgres.company_db");
    assert!(row.auto_register);
    assert_eq!(
        row.properties.get("host").map(String::as_str),
        Some("203.0.113.1")
    );
    assert_eq!(
        row.properties.get("password").map(String::as_str),
        Some("***")
    );
}

#[tokio::test]
async fn auto_register_false_lists_but_does_not_register() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\nauto_register = false\n",
    );
    session
        .register_configured_sources()
        .expect("source registration");
    let rows = session.sources();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].auto_register);
    let error = session
        .sql("SELECT * FROM company_db.public.t")
        .await
        .expect_err("an unregistered name must answer the engine not-found");
    let message = error.to_string();
    assert!(!message.contains("1.10"), "{message}");
    assert!(message.contains("company_db"), "{message}");
}

#[tokio::test]
async fn catalog_named_like_source_refuses_as_duplicate() {
    let warehouse = TempDir::new().expect("warehouse fixture");
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    let error = session
        .register_memory_catalog(
            "company_db",
            warehouse.path().to_str().expect("utf8 warehouse"),
        )
        .await
        .expect_err("a catalog under a source name must refuse as duplicate");
    assert!(error.to_string().contains("already registered"), "{error}");
}

#[test]
fn unknown_source_handle_refuses_naming_declared_sources() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    let error = session
        .source("nope")
        .expect_err("an undeclared name must refuse");
    let message = error.to_string();
    assert!(message.contains("nope"), "{message}");
    assert!(message.contains("company_db"), "{message}");
}
