//! Postgres-target INSERT routing for the ANSI door: appends drive the sink.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{Insert, ObjectName, Statement, TableObject};
use repark_common::SourceKind;
use repark_core::write_postgres::{
    PostgresWrite, PostgresWritePath, PostgresWriteTarget, execute_postgres_write,
    postgres_write_modes_refusal, postgres_write_upsert_refusal, record_postgres_write_report,
};
use repark_core::{CatalogRegistry, EngineContext};

/// Whether `name` is a mounted Postgres source.
#[must_use]
pub(crate) fn is_postgres_source(catalogs: &CatalogRegistry, name: &str) -> bool {
    catalogs.database_source_kind(name) == Some(SourceKind::Postgres)
}

/// The `(source, schema, table)` of a three-part Postgres target, if it is one.
#[must_use]
pub(crate) fn postgres_target_parts(
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Option<(String, String, String)> {
    let mut parts = Vec::new();
    for part in &name.0 {
        parts.push(part.as_ident()?.value.as_str());
    }
    let [source, schema, table] = parts.as_slice() else {
        return None;
    };
    if !is_postgres_source(catalogs, source) {
        return None;
    }
    Some((source.to_string(), schema.to_string(), table.to_string()))
}

/// The upsert refusal when an `UPDATE` targets a Postgres source.
#[must_use]
pub(crate) fn postgres_update_refusal(
    cx: &EngineContext<'_>,
    statement: &Statement,
) -> Option<DataFusionError> {
    let (_, _, catalog, _) = crate::guards::dml_target_ident(cx, statement)?;
    if !is_postgres_source(cx.catalogs, &catalog) {
        return None;
    }
    Some(DataFusionError::NotImplemented(
        postgres_write_upsert_refusal("UPDATE"),
    ))
}

/// Run a Postgres-target INSERT: appends drive the sink, the rest refuse.
pub(crate) async fn execute_postgres_insert(
    cx: &EngineContext<'_>,
    insert: &Insert,
    source: &str,
    schema: &str,
    table: &str,
) -> Result<DataFrame> {
    if insert.overwrite {
        return Err(DataFusionError::NotImplemented(
            postgres_write_modes_refusal("INSERT OVERWRITE"),
        ));
    }
    if insert.replace_into {
        return Err(DataFusionError::NotImplemented(
            postgres_write_upsert_refusal("REPLACE INTO"),
        ));
    }
    let Some(origin) = insert.source.as_ref() else {
        return Err(DataFusionError::Plan(
            "Inserts without a source not supported".to_string(),
        ));
    };
    let plan = crate::router::delegate_plan(cx, &origin.to_string(), None, None).await?;
    let frame = repark_core::PreExecute::from_engine_context(cx)
        .execute(plan)
        .await?;
    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(column_name).collect())
    };
    let state = cx.ctx.state();
    let zone = repark_functions::session_time_zone::session_time_zone_from_options(
        state.config().options(),
    );
    let write = PostgresWrite {
        target: PostgresWriteTarget::Mounted {
            source: source.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        },
        columns,
        case_insensitive: true,
        path: PostgresWritePath::Bulk,
    };
    let report = execute_postgres_write(cx.catalogs, frame, write, zone).await?;
    record_postgres_write_report(cx.ctx, report);
    cx.ctx.read_empty()
}

fn column_name(name: &ObjectName) -> String {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map_or_else(|| name.to_string(), |ident| ident.value.clone()),
        _ => name.to_string(),
    }
}

/// Route one INSERT through the sink when its target is a Postgres source.
pub(crate) async fn route_postgres_insert(
    cx: &EngineContext<'_>,
    insert: &Insert,
) -> Result<Option<DataFrame>> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(None);
    };
    let Some((source, schema, table)) = postgres_target_parts(cx.catalogs, name) else {
        return Ok(None);
    };
    Box::pin(execute_postgres_insert(
        cx, insert, &source, &schema, &table,
    ))
    .await
    .map(Some)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use repark_core::{CatalogRegistry, EngineContext, ReparkSession, ReparkSessionBuilder};

    fn mounted_session(toml: &str) -> (tempfile::TempDir, ReparkSession) {
        let directory = tempfile::TempDir::new().expect("config fixture directory");
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

    async fn run(
        session: &ReparkSession,
        catalogs: &CatalogRegistry,
        read_only: &HashSet<String>,
        sql: &str,
    ) -> datafusion::error::Result<datafusion::prelude::DataFrame> {
        crate::execute(
            EngineContext::new(session.context(), catalogs, read_only),
            sql,
        )
        .await
    }

    const MOUNT: &str = "[default.database.postgres.pg]\nhost = \"203.0.113.1\"\n";

    #[tokio::test]
    async fn pg_append_routes_to_the_driver_not_the_default_hook() {
        let (_directory, session) = mounted_session(MOUNT);
        let catalogs = session.catalogs_snapshot();
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
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
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
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
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
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
    async fn pg_replace_names_the_upsert_row() {
        let (_directory, session) = mounted_session(MOUNT);
        let catalogs = session.catalogs_snapshot();
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
            "REPLACE INTO pg.public.t VALUES (1)",
        )
        .await
        .expect_err("replace refuses");
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
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
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
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
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
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
            "INSERT INTO pg.t SELECT 1",
        )
        .await
        .expect_err("a two-part name refuses");
        let message = error.to_string();
        assert!(!message.contains("database source `pg`"), "{message}");
    }

    #[tokio::test]
    async fn read_only_set_with_specs_lets_insert_through_and_keeps_update_refused() {
        let (_directory, session) = mounted_session(MOUNT);
        let mut catalogs = session.catalogs_snapshot();
        let read_only: HashSet<String> = ["pg".to_string()].into_iter().collect();
        catalogs.set_read_only_catalogs(read_only.clone());
        let insert = run(
            &session,
            &catalogs,
            &read_only,
            "INSERT INTO pg.public.t VALUES (1)",
        )
        .await
        .expect_err("insert reaches the driver past the text guard");
        assert!(
            insert.to_string().contains("database source `pg`"),
            "{insert}"
        );
        let update = run(
            &session,
            &catalogs,
            &read_only,
            "UPDATE pg.public.t SET a = 1",
        )
        .await
        .expect_err("update still refuses at the text guard");
        assert!(
            update.to_string().contains("registered read-only"),
            "{update}"
        );
    }

    static LIVE_SCHEMA_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    struct LiveSchema {
        url: String,
        name: String,
    }

    fn live_schema_name() -> String {
        let id = LIVE_SCHEMA_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("c4ansi_{}_{id}", std::process::id())
    }

    impl LiveSchema {
        fn create(url: &str, name: String, setup: &str) -> Self {
            let schema = Self {
                url: url.to_string(),
                name,
            };
            schema.psql(&format!("CREATE SCHEMA \"{}\"", schema.name));
            schema.psql(setup);
            schema
        }

        fn psql(&self, sql: &str) {
            let output = std::process::Command::new("psql")
                .arg(&self.url)
                .arg("-v")
                .arg("ON_ERROR_STOP=1")
                .arg("-c")
                .arg(sql)
                .output()
                .expect("psql runs");
            assert!(
                output.status.success(),
                "psql failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    impl Drop for LiveSchema {
        fn drop(&mut self) {
            let _ = std::process::Command::new("psql")
                .arg(&self.url)
                .arg("-c")
                .arg(format!("DROP SCHEMA \"{}\" CASCADE", self.name))
                .output();
        }
    }

    fn live_url() -> String {
        std::env::var("REPARK_PG_URL").expect("REPARK_PG_URL names the C-0 container")
    }

    fn live_session(url: &str) -> (tempfile::TempDir, ReparkSession) {
        mounted_session(&format!(
            "[default.database.postgres.pg]\nurl = \"{url}\"\nsslmode = \"disable\"\n"
        ))
    }

    async fn stored_rows(session: &ReparkSession, catalogs: &CatalogRegistry, sql: &str) -> usize {
        let frame = run(session, catalogs, &HashSet::new(), sql)
            .await
            .expect("the read-back select runs");
        frame
            .collect()
            .await
            .expect("the read-back select collects")
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum()
    }

    #[tokio::test]
    #[ignore = "needs REPARK_PG_URL"]
    async fn live_view_target_takes_the_row_path_and_says_why() {
        use repark_core::write_postgres::{PostgresWritePath, take_postgres_write_report};

        let url = live_url();
        let (_directory, session) = live_session(&url);
        let catalogs = session.catalogs_snapshot();
        let name = live_schema_name();
        let setup = format!(
            "CREATE TABLE \"{name}\".t (id bigint PRIMARY KEY, name text); \
             CREATE VIEW \"{name}\".v AS SELECT * FROM \"{name}\".t"
        );
        let schema = LiveSchema::create(&url, name, &setup);
        run(
            &session,
            &catalogs,
            &HashSet::new(),
            &format!(
                "INSERT INTO pg.\"{}\".v (id, name) VALUES (1, 'a')",
                schema.name
            ),
        )
        .await
        .expect("a view target writes through the row path");
        let report =
            take_postgres_write_report(session.context()).expect("the write leaves its report");
        assert_eq!(report.path, PostgresWritePath::Row);
        assert_eq!(report.rows, 1);
        assert_eq!(
            report.fallback.as_deref(),
            Some("the target is a view, which COPY cannot write")
        );
        assert_eq!(
            stored_rows(
                &session,
                &catalogs,
                &format!("SELECT id FROM pg.\"{}\".t", schema.name)
            )
            .await,
            1
        );
    }

    #[tokio::test]
    #[ignore = "needs REPARK_PG_URL"]
    async fn live_insert_rule_target_takes_the_row_path_and_says_why() {
        use repark_core::write_postgres::{PostgresWritePath, take_postgres_write_report};

        let url = live_url();
        let (_directory, session) = live_session(&url);
        let catalogs = session.catalogs_snapshot();
        let name = live_schema_name();
        let setup = format!(
            "CREATE TABLE \"{name}\".t (id bigint PRIMARY KEY, name text); \
             CREATE RULE no_rows AS ON INSERT TO \"{name}\".t DO INSTEAD NOTHING"
        );
        let schema = LiveSchema::create(&url, name, &setup);
        run(
            &session,
            &catalogs,
            &HashSet::new(),
            &format!(
                "INSERT INTO pg.\"{}\".t (id, name) VALUES (1, 'a')",
                schema.name
            ),
        )
        .await
        .expect("a rule target writes through the row path");
        let report =
            take_postgres_write_report(session.context()).expect("the write leaves its report");
        assert_eq!(report.path, PostgresWritePath::Row);
        assert_eq!(
            report.fallback.as_deref(),
            Some("the target has an INSERT rule, which COPY does not fire")
        );
        assert_eq!(
            stored_rows(
                &session,
                &catalogs,
                &format!("SELECT id FROM pg.\"{}\".t", schema.name)
            )
            .await,
            0
        );
    }

    #[tokio::test]
    #[ignore = "needs REPARK_PG_URL"]
    async fn live_named_identity_column_refuses_with_the_core_refusal() {
        let url = live_url();
        let (_directory, session) = live_session(&url);
        let catalogs = session.catalogs_snapshot();
        let name = live_schema_name();
        let setup = format!(
            "CREATE TABLE \"{name}\".g (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text)"
        );
        let schema = LiveSchema::create(&url, name, &setup);
        let error = run(
            &session,
            &catalogs,
            &HashSet::new(),
            &format!(
                "INSERT INTO pg.\"{}\".g (id, name) VALUES (1, 'a')",
                schema.name
            ),
        )
        .await
        .expect_err("a named GENERATED ALWAYS identity column refuses");
        let message = error.to_string();
        assert!(message.contains("GENERATED ALWAYS"), "{message}");
        assert!(message.contains("`id`"), "{message}");
        assert_eq!(
            stored_rows(
                &session,
                &catalogs,
                &format!("SELECT id FROM pg.\"{}\".g", schema.name)
            )
            .await,
            0
        );
    }
}
