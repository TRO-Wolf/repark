use std::collections::BTreeMap;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::Error;
use crate::session::ReparkSessionBuilder;
use crate::session::write_postgres::{
    PostgresWrite, PostgresWritePath, PostgresWriteReport, PostgresWriteTarget,
    execute_postgres_write, record_postgres_write_report, take_postgres_write_report,
};

const REFUSED_URL: &str = "postgresql://127.0.0.1:1/postgres";

fn frame() -> datafusion::prelude::DataFrame {
    SessionContext::new()
        .read_empty()
        .expect("an empty frame plans")
}

fn properties(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn url_write(dbtable: &str, props: BTreeMap<String, String>) -> PostgresWrite {
    PostgresWrite {
        target: PostgresWriteTarget::Url {
            url: REFUSED_URL.to_string(),
            dbtable: dbtable.to_string(),
            properties: props,
        },
        columns: None,
        case_insensitive: false,
        path: PostgresWritePath::Bulk,
    }
}

fn catalogs() -> crate::catalog_state::CatalogRegistry {
    ReparkSessionBuilder::default()
        .build()
        .expect("a session builds")
        .catalogs_snapshot()
}

#[tokio::test]
async fn an_unknown_mounted_source_refuses_before_any_connection() {
    let write = PostgresWrite {
        target: PostgresWriteTarget::Mounted {
            source: "nosuch".to_string(),
            schema: "public".to_string(),
            table: "t".to_string(),
        },
        columns: None,
        case_insensitive: false,
        path: PostgresWritePath::Bulk,
    };
    let error = execute_postgres_write(&catalogs(), frame(), write, "UTC")
        .await
        .expect_err("an unknown source refuses");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error:?}");
    assert!(
        error
            .to_string()
            .contains("unknown database source `nosuch`")
    );
}

#[tokio::test]
async fn a_non_postgres_mount_refuses() {
    let (directory, path) = {
        let directory = tempfile::TempDir::new().expect("config fixture directory");
        let path = directory.path().join("repark.toml");
        std::fs::write(
            &path,
            "[default.database.sqlserver.ms_db]\nhost = \"203.0.113.1\"\n",
        )
        .expect("fixture file");
        (directory, path)
    };
    let session = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file-built session");
    session
        .register_configured_sources()
        .expect("source registration");
    let _directory = directory;
    let write = PostgresWrite {
        target: PostgresWriteTarget::Mounted {
            source: "ms_db".to_string(),
            schema: "dbo".to_string(),
            table: "t".to_string(),
        },
        columns: None,
        case_insensitive: false,
        path: PostgresWritePath::Bulk,
    };
    let error = execute_postgres_write(&session.catalogs_snapshot(), frame(), write, "UTC")
        .await
        .expect_err("a SQL Server mount refuses");
    assert!(
        error.to_string().contains("not a Postgres source"),
        "{error}"
    );
}

#[tokio::test]
async fn an_unknown_property_refuses_as_config_naming_the_source() {
    let props = properties(&[
        ("user", "postgres"),
        ("password", "secret-test"),
        ("sslmode", "disable"),
        ("bogus", "1"),
    ]);
    let error = execute_postgres_write(&catalogs(), frame(), url_write("public.t", props), "UTC")
        .await
        .expect_err("an unknown key refuses");
    let message = error.to_string();
    assert!(message.contains("bogus"), "{message}");
    assert!(message.contains("database source `jdbc`"), "{message}");
    assert!(!message.contains("secret-test"), "{message}");
    let folded = crate::error_map::engine_err(error);
    assert!(matches!(folded, Error::Config(_)), "{folded:?}");
}

#[tokio::test]
async fn an_unreachable_host_refuses_as_operational_without_the_password() {
    let props = properties(&[
        ("user", "postgres"),
        ("password", "secret-test"),
        ("sslmode", "disable"),
    ]);
    let error = execute_postgres_write(&catalogs(), frame(), url_write("public.t", props), "UTC")
        .await
        .expect_err("a refused port refuses");
    let message = error.to_string();
    assert!(message.contains("database source `jdbc`"), "{message}");
    assert!(!message.contains("secret-test"), "{message}");
    let folded = crate::error_map::engine_err(error);
    assert!(matches!(folded, Error::DataFusion(_)), "{folded:?}");
}

#[tokio::test]
async fn partition_options_on_a_write_are_ignored_not_refused() {
    let props = properties(&[
        ("user", "postgres"),
        ("password", "postgres"),
        ("sslmode", "disable"),
        ("partitionColumn", "id"),
        ("lowerBound", "1"),
        ("upperBound", "9"),
        ("numPartitions", "4"),
        ("predicates", "id > 1"),
    ]);
    let error = execute_postgres_write(&catalogs(), frame(), url_write("public.t", props), "UTC")
        .await
        .expect_err("a refused port refuses");
    let message = error.to_string().to_lowercase();
    assert!(!message.contains("partitioncolumn"), "{message}");
    assert!(!message.contains("predicates"), "{message}");
    assert!(!message.contains("unknown"), "{message}");
    assert!(!message.contains("declared"), "{message}");
}

#[test]
fn write_path_option_defaults_to_bulk_and_names_both_values_on_refusal() {
    use crate::session::write_postgres::{PostgresWritePath, parse_write_path_option};
    assert_eq!(
        parse_write_path_option(None).expect("absent means bulk"),
        PostgresWritePath::Bulk
    );
    assert_eq!(
        parse_write_path_option(Some("bulk")).expect("bulk parses"),
        PostgresWritePath::Bulk
    );
    assert_eq!(
        parse_write_path_option(Some("ROW")).expect("row parses case-insensitively"),
        PostgresWritePath::Row
    );
    let error = parse_write_path_option(Some("columnar")).expect_err("a bad value refuses");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "{error:?}"
    );
    let message = error.to_string();
    assert!(message.contains("'bulk'"), "{message}");
    assert!(message.contains("'row'"), "{message}");
}

#[test]
fn last_write_report_records_on_builder_sessions_and_nowhere_else() {
    let session = ReparkSessionBuilder::default()
        .build()
        .expect("a session builds");
    let context = session.context();
    assert!(take_postgres_write_report(context).is_none());
    record_postgres_write_report(
        context,
        PostgresWriteReport {
            path: PostgresWritePath::Row,
            rows: 7,
        },
    );
    let taken = take_postgres_write_report(context).expect("a recorded report reads back once");
    assert_eq!(taken.path, PostgresWritePath::Row);
    assert_eq!(taken.rows, 7);
    assert!(take_postgres_write_report(context).is_none());
    let bare = SessionContext::new();
    record_postgres_write_report(
        &bare,
        PostgresWriteReport {
            path: PostgresWritePath::Bulk,
            rows: 1,
        },
    );
    assert!(take_postgres_write_report(&bare).is_none());
}
