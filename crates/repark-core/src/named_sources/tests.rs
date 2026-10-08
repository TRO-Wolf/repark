use std::path::PathBuf;

use tempfile::TempDir;

use crate::session::ReparkSessionBuilder;
use crate::{Error, ReparkSession};

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
async fn configured_source_select_resolves_through_the_postgres_mount() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    let error = session
        .sql("SELECT * FROM company_db.public.t")
        .await
        .expect_err("a source without `user` refuses at its first resolution");
    assert!(matches!(error, Error::Config(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        message.contains("database source `company_db`"),
        "{message}"
    );
    assert!(message.contains("`user` is required"), "{message}");
    assert!(!message.contains("1.10"), "{message}");
    assert!(!message.contains("not found"), "{message}");
    assert!(!message.contains("does not exist"), "{message}");
}

#[tokio::test]
async fn configured_source_ddl_refuses_as_read_only() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\n\
         [default.database.sqlserver.ms_db]\nhost = \"203.0.113.1\"\n",
    );
    session
        .register_configured_sources()
        .expect("source registration");
    for sql in [
        "DROP SCHEMA company_db.public",
        "CREATE DATABASE company_db",
        "CREATE SCHEMA company_db.fresh",
        "CREATE SCHEMA IF NOT EXISTS company_db.fresh",
        "CREATE DATABASE company_db.fresh",
        "CREATE DATABASE IF NOT EXISTS company_db.fresh",
    ] {
        let error = session
            .sql(sql)
            .await
            .expect_err("DDL under a Postgres source must refuse");
        let message = error.to_string();
        assert!(
            message.contains("database source `default.database.postgres.company_db` is read-only"),
            "{sql}: {message}"
        );
        assert!(message.contains("CONNECT-DECL-pg-ddl"), "{sql}: {message}");
        assert!(!message.contains("1.10"), "{sql}: {message}");
        assert!(!message.contains("not found"), "{sql}: {message}");
    }
    let error = session
        .sql("DROP SCHEMA ms_db.dbo")
        .await
        .expect_err("DDL under a SQL Server source keeps the pending refusal");
    assert!(error.to_string().contains("1.10"), "{error}");
}

async fn session_defaulting_to_the_mounted_source() -> (TempDir, ReparkSession) {
    let (directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    session
        .sql("SET datafusion.catalog.default_catalog = 'company_db'")
        .await
        .expect("the default catalog moves to the mounted source");
    (directory, session)
}

async fn assert_bare_ddl_refuses_under_a_mounted_default_catalog(sql: &str) {
    let (_directory, session) = session_defaulting_to_the_mounted_source().await;
    let Err(error) = session.sql(sql).await else {
        panic!("{sql}: a bare schema under a mounted default catalog must refuse");
    };
    let message = error.to_string();
    assert!(
        message.contains("database source `default.database.postgres.company_db` is read-only"),
        "{sql}: {message}"
    );
    assert!(message.contains("CONNECT-DECL-pg-ddl"), "{sql}: {message}");
}

#[tokio::test]
async fn bare_create_schema_if_not_exists_refuses_under_a_mounted_default_catalog() {
    assert_bare_ddl_refuses_under_a_mounted_default_catalog("CREATE SCHEMA IF NOT EXISTS bare_x")
        .await;
}

#[tokio::test]
async fn bare_create_schema_refuses_under_a_mounted_default_catalog() {
    assert_bare_ddl_refuses_under_a_mounted_default_catalog("CREATE SCHEMA bare_x").await;
}

#[tokio::test]
async fn bare_create_database_under_a_mounted_default_catalog_makes_an_unrelated_catalog() {
    let (_directory, session) = session_defaulting_to_the_mounted_source().await;
    for sql in [
        "CREATE DATABASE bare_db",
        "CREATE DATABASE IF NOT EXISTS bare_db2",
    ] {
        session
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: a bare database is a new catalog: {error}"));
    }
    let names = session.context().catalog_names();
    for name in ["company_db", "bare_db", "bare_db2"] {
        assert!(
            names.iter().any(|listed| listed == name),
            "{name}: {names:?}"
        );
    }
    session
        .sql("CREATE SCHEMA bare_db.s")
        .await
        .expect("the new catalog is usable");
}

#[tokio::test]
async fn a_dotted_schema_whose_head_is_not_a_source_succeeds_under_a_mounted_default_catalog() {
    let (_directory, session) = session_defaulting_to_the_mounted_source().await;
    session
        .sql("CREATE SCHEMA datafusion.dotted_s")
        .await
        .expect("a dotted name whose head is not a source is not refused");
}

#[tokio::test]
async fn bare_drop_schema_refuses_under_a_mounted_default_catalog() {
    assert_bare_ddl_refuses_under_a_mounted_default_catalog("DROP SCHEMA bare_x").await;
}

#[tokio::test]
async fn bare_schema_ddl_succeeds_under_the_ordinary_default_catalog() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    session
        .sql("CREATE SCHEMA bare_x")
        .await
        .expect("a bare schema under the ordinary default catalog is created");
    session
        .sql("DROP SCHEMA bare_x")
        .await
        .expect("and dropped again");
}

#[tokio::test]
async fn mounted_postgres_sources_are_read_only_catalogs() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\n\
         [default.database.postgres.parked]\nhost = \"203.0.113.1\"\nauto_register = false\n\
         [default.database.sqlserver.ms_db]\nhost = \"203.0.113.1\"\n",
    );
    assert!(session.postgres_catalog_names_snapshot().is_empty());
    session
        .register_configured_sources()
        .expect("source registration");
    let names = session.postgres_catalog_names_snapshot();
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names.contains("company_db"), "{names:?}");
    let catalog = session
        .context()
        .catalog("company_db")
        .expect("the source is mounted");
    assert!(
        catalog
            .downcast_ref::<repark_connect::PostgresCatalog>()
            .is_some()
    );
    assert!(session.context().catalog("parked").is_none());
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

#[tokio::test]
async fn source_ping_resolves_through_the_mount() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\n\
         [default.database.postgres.parked]\nhost = \"203.0.113.1\"\nauto_register = false\n",
    );
    session
        .register_configured_sources()
        .expect("source registration");
    for (name, key_path) in [
        ("company_db", "default.database.postgres.company_db"),
        ("parked", "default.database.postgres.parked"),
    ] {
        let source = session.source(name).expect("declared source handle");
        let error = source
            .ping()
            .await
            .expect_err("a source without `user` refuses before any connection");
        assert!(matches!(error, Error::Config(_)), "{error:?}");
        let message = error.to_string();
        assert!(
            message.contains(&format!("database source `{key_path}`")),
            "{message}"
        );
        assert!(message.contains("`user` is required"), "{message}");
        assert!(!message.contains("1.10"), "{message}");
    }
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

#[test]
fn sources_listing_masks_a_password_inside_a_url_shaped_value() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.acme]\n\
         url = \"postgresql://alice:S3cretPw@db.example.com:5432/sales\"\n\
         dsn = \"host=db.example.com dbname=sales password=S3cretPw\"\n\
         user = \"alice\"\n",
    );
    let rows = session.sources();
    let properties = &rows[0].properties;
    assert_eq!(
        properties.get("url").map(String::as_str),
        Some("postgresql://alice:***@db.example.com:5432/sales")
    );
    assert_eq!(
        properties.get("dsn").map(String::as_str),
        Some("host=db.example.com dbname=sales password=***")
    );
    assert_eq!(properties.get("user").map(String::as_str), Some("alice"));
    let rendered = format!("{rows:?} {:?}", session.source_specs);
    assert!(!rendered.contains("S3cretPw"), "{rendered}");
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

#[test]
fn a_source_named_like_an_engine_catalog_refuses_as_duplicate() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session.context().register_catalog(
        "company_db",
        std::sync::Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );
    let error = session
        .register_configured_sources()
        .expect_err("a source over an engine catalog's name must refuse");
    assert!(
        error
            .to_string()
            .contains("catalog 'company_db' is already registered"),
        "{error}"
    );
    let catalog = session
        .context()
        .catalog("company_db")
        .expect("the engine catalog");
    assert!(
        catalog
            .downcast_ref::<repark_connect::PostgresCatalog>()
            .is_none()
    );
    assert!(session.postgres_catalog_names_snapshot().is_empty());
}

#[test]
fn a_mounted_schema_refuses_table_registration_as_read_only() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    let schema = session
        .context()
        .catalog("company_db")
        .and_then(|catalog| catalog.schema("public"))
        .expect("a mounted schema");
    let table = std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
        std::sync::Arc::new(arrow::datatypes::Schema::empty()),
    ));
    let registered = schema
        .register_table("fresh".to_string(), table)
        .expect_err("registering a table under a Postgres source must refuse");
    let deregistered = schema
        .deregister_table("t")
        .expect_err("deregistering a table under a Postgres source must refuse");
    for error in [registered, deregistered] {
        let message = error.to_string();
        assert!(
            message.contains("database source `company_db` is read-only"),
            "{message}"
        );
        assert!(message.contains("CONNECT-DECL-pg-ddl"), "{message}");
    }
}

#[test]
fn sources_listing_masks_a_percent_encoded_url_password_key() {
    let (_directory, session) = session_with_source(
        "[default.database.postgres.acme]\n\
         url = \"postgresql://T0kenOnly@db.example.com:5432/sales\"\n\
         [default.database.postgres.query]\n\
         url = \"jdbc:postgresql://db.example.com/sales?user=app&pass%77ord=Qu3rySecret\"\n",
    );
    let rows = session.sources();
    let url = |name: &str| {
        rows.iter()
            .find(|row| row.name == name)
            .and_then(|row| row.properties.get("url").cloned())
            .expect("a listed url")
    };
    assert_eq!(url("acme"), "postgresql://***@db.example.com:5432/sales");
    assert_eq!(
        url("query"),
        "jdbc:postgresql://db.example.com/sales?user=app&pass%77ord=***"
    );
    let rendered = format!("{rows:?}");
    assert!(!rendered.contains("T0kenOnly"), "{rendered}");
    assert!(!rendered.contains("Qu3rySecret"), "{rendered}");
}

#[tokio::test]
async fn catalog_apis_resolve_a_mounted_source_instead_of_an_unknown_catalog() {
    let (_directory, session) = session_with_source(UNROUTABLE_SOURCE);
    session
        .register_configured_sources()
        .expect("source registration");
    let error = session
        .table_exists("company_db.public.t")
        .await
        .expect_err("tableExists resolves through the mount, which refuses without `user`");
    assert!(matches!(error, Error::Config(_)), "{error:?}");
    assert!(error.to_string().contains("`user` is required"), "{error}");
    let error = session
        .list_iceberg_table_names("company_db", "public")
        .await
        .expect_err("a catalog operation under a Postgres source refuses read-only");
    assert!(matches!(error, Error::NotImplemented(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        message.contains("database source `default.database.postgres.company_db` is read-only"),
        "{message}"
    );
    assert!(message.contains("CONNECT-DECL-pg-ddl"), "{message}");
    assert!(!message.contains("unknown catalog"), "{message}");
}
