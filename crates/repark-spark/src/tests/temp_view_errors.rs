use super::super::*;
use super::common::*;

struct CtxTempViews {
    ctx: SessionContext,
}

impl CtxTempViews {
    fn home_ref(name: &str) -> datafusion::sql::TableReference {
        let table = match name
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
        {
            Some(quoted) => quoted.replace("\"\"", "\""),
            None => name.to_ascii_lowercase(),
        };
        datafusion::sql::TableReference::full("datafusion", "public", table)
    }
}

fn analysis(error: impl std::fmt::Display) -> repark_common::Error {
    repark_common::Error::Analysis(error.to_string())
}

impl repark_core::TempViewSession for CtxTempViews {
    fn create_or_replace_temp_view_from(
        &self,
        name: &str,
        frame: &datafusion::prelude::DataFrame,
    ) -> repark_common::Result<()> {
        let reference = Self::home_ref(name);
        self.ctx
            .deregister_table(reference.clone())
            .map_err(analysis)?;
        self.ctx
            .register_table(reference, frame.clone().into_view())
            .map_err(analysis)?;
        Ok(())
    }

    fn resolve_temp_view_home_ref(&self, name: &str) -> repark_common::Result<Option<Vec<String>>> {
        let reference = Self::home_ref(name);
        let exists = self.ctx.table_exist(reference.clone()).map_err(analysis)?;
        Ok(exists.then(|| {
            vec![
                "datafusion".to_string(),
                "public".to_string(),
                reference.table().to_string(),
            ]
        }))
    }

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["datafusion".to_string(), "public".to_string()])
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Ok(self
            .ctx
            .catalog("datafusion")
            .and_then(|catalog| catalog.schema("public"))
            .map(|schema| schema.table_names())
            .unwrap_or_default())
    }

    fn drop_temp_view(&self, name: &str) -> repark_common::Result<bool> {
        Ok(self
            .ctx
            .deregister_table(Self::home_ref(name))
            .map_err(analysis)?
            .is_some())
    }
}

async fn real_session() -> (TempDir, SessionContext, CatalogRegistry, CtxTempViews) {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let session = CtxTempViews { ctx: ctx.clone() };
    (warehouse, ctx, catalogs, session)
}

async fn real_run(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    session: &CtxTempViews,
    sql: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let frame = crate::router::execute_in_session(
        ctx,
        catalogs,
        sql,
        &std::collections::HashSet::<String>::new(),
        &crate::write_options::StatementWriteOptions::empty(),
        Some(session),
    )
    .await?;
    frame.collect().await
}

async fn real_ok(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    session: &CtxTempViews,
    sql: &str,
) {
    real_run(ctx, catalogs, session, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} must run: {error}"));
}

async fn real_refusal(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    session: &CtxTempViews,
    sql: &str,
) -> DataFusionError {
    real_run(ctx, catalogs, session, sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} must refuse"))
}

const TABLE_OR_VIEW_NOT_FOUND_TAIL: &str = " cannot be found. Verify the spelling and correctness of the schema and catalog.\nIf you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.\nTo tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01";

async fn chain_depth_outcomes() -> (datafusion::error::Result<Vec<RecordBatch>>, DataFusionError) {
    let (_warehouse, ctx, catalogs, session) = real_session().await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW d0 AS SELECT 1 AS id",
    )
    .await;
    for level in 1..=100 {
        let sql = format!(
            "CREATE TEMPORARY VIEW d{level} AS SELECT * FROM d{}",
            level - 1
        );
        real_ok(&ctx, &catalogs, &session, &sql).await;
    }
    let within = real_run(&ctx, &catalogs, &session, "SELECT * FROM d99").await;
    let past = real_refusal(&ctx, &catalogs, &session, "SELECT * FROM d100").await;
    (within, past)
}

#[test]
fn temp_view_chain_depth_refuses_at_the_101st_view() {
    let (within, past) = std::thread::Builder::new()
        .stack_size(512 << 20)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(chain_depth_outcomes())
        })
        .expect("large-stack thread")
        .join()
        .expect("chain depth thread");
    within.unwrap_or_else(|error| panic!("a 100-view chain reads: {error}"));
    assert!(matches!(past, DataFusionError::Plan(_)), "{past:?}");
    assert_eq!(
        past.to_string(),
        "Error during planning: [VIEW_EXCEED_MAX_NESTED_DEPTH] The depth of view `d0` exceeds the maximum view resolution depth (100).\nAnalysis is aborted to avoid errors. If you want to work around this, please try to increase the value of \"spark.sql.view.maxNestedViewDepth\". SQLSTATE: 54K00"
    );
}

#[tokio::test]
async fn temp_view_read_time_refusals_carry_the_full_text() {
    let (_warehouse, ctx, catalogs, session) = real_session().await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW x AS SELECT CAST(1 AS INT) AS id, CAST(2 AS INT) AS k",
    )
    .await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW y AS SELECT * FROM x",
    )
    .await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE OR REPLACE TEMPORARY VIEW x AS SELECT CAST(1 AS INT) AS id",
    )
    .await;
    let error = real_refusal(&ctx, &catalogs, &session, "SELECT * FROM y").await;
    assert_eq!(
        error.to_string(),
        "Error during planning: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `y` has an incompatible schema change and column k cannot be resolved. Expected 1 columns named k but got [].\nPlease try to re-create the view by running: CREATE OR REPLACE TEMPORARY VIEW. SQLSTATE: 51024"
    );
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE OR REPLACE TEMPORARY VIEW x AS SELECT 'a' AS id, CAST(2 AS INT) AS k",
    )
    .await;
    let error = real_refusal(&ctx, &catalogs, &session, "SELECT * FROM y").await;
    assert_eq!(
        error.to_string(),
        "Error during planning: [CANNOT_UP_CAST_DATATYPE] Cannot up cast x.id from \"STRING\" to \"INT\".\nThe type path of the target object is:\n\nYou can either add an explicit cast to the input data or choose a higher precision type of the field in the target object SQLSTATE: 42846"
    );
    real_ok(&ctx, &catalogs, &session, "DROP VIEW x").await;
    let error = real_refusal(&ctx, &catalogs, &session, "SELECT * FROM y").await;
    assert_eq!(
        error.to_string(),
        format!(
            "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `x`{TABLE_OR_VIEW_NOT_FOUND_TAIL}"
        )
    );
}

#[tokio::test]
async fn temp_view_create_time_refusals_carry_the_full_text() {
    let (_warehouse, ctx, catalogs, session) = real_session().await;
    for (sql, expected) in [
        (
            "CREATE TEMPORARY VIEW vbad (i) AS SELECT id, name FROM src",
            "Error during planning: [CREATE_VIEW_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot create view `vbad`, the reason is too many data columns:\nView columns: `i`.\nData columns: `id`, `name`. SQLSTATE: 21S01",
        ),
        (
            "CREATE TEMPORARY VIEW vbad (i, d, x) AS SELECT id, name FROM src",
            "Error during planning: [CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot create view `vbad`, the reason is not enough data columns:\nView columns: `i`, `d`, `x`.\nData columns: `id`, `name`. SQLSTATE: 21S01",
        ),
        (
            "CREATE TEMPORARY VIEW vm AS SELECT 1; SELECT 2",
            "Error during planning: a stored view body must hold exactly one statement",
        ),
        (
            "CREATE TEMPORARY VIEW vb AS SELECT 1 +",
            "Error during planning: could not parse a stored view body: sql parser error: Expected: an expression, found: EOF",
        ),
        (
            "CREATE TEMPORARY VIEW vu AS SELECT * FROM ice.sales.nope",
            "Error during planning: table 'ice.sales.nope' not found",
        ),
    ] {
        let error = real_refusal(&ctx, &catalogs, &session, sql).await;
        assert_eq!(error.to_string(), expected, "{sql}");
    }
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW v AS SELECT id FROM src",
    )
    .await;
    let error = real_refusal(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW v AS SELECT 1 AS id",
    )
    .await;
    assert_eq!(
        error.to_string(),
        "Error during planning: [TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `v` because it already exists.\nChoose a different name, drop or replace the existing view. SQLSTATE: 42P07"
    );
    let error = real_refusal(
        &ctx,
        &catalogs,
        &session,
        "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM v",
    )
    .await;
    assert_eq!(
        error.to_string(),
        "Error during planning: [RECURSIVE_VIEW] Recursive view `v` detected (cycle: `v` -> `v`). SQLSTATE: 42K0H"
    );
}

#[tokio::test]
async fn temp_view_over_a_dropped_catalog_table_refuses_at_read() {
    let (_warehouse, ctx, catalogs, session) = real_session().await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TABLE ice.sales.gone AS SELECT CAST(1 AS INT) AS id",
    )
    .await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW w AS SELECT * FROM ice.sales.gone",
    )
    .await;
    real_ok(&ctx, &catalogs, &session, "DROP TABLE ice.sales.gone").await;
    let error = real_refusal(&ctx, &catalogs, &session, "SELECT * FROM w").await;
    assert_eq!(
        error.to_string(),
        "Error during planning: table 'ice.sales.gone' not found"
    );
}

#[tokio::test]
async fn held_temp_view_frame_refuses_after_a_catalog_replaces_the_home() {
    let (_warehouse, ctx, catalogs, session) = real_session().await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW x AS SELECT 1 AS id",
    )
    .await;
    real_ok(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW y AS SELECT * FROM x",
    )
    .await;
    let held = ctx.table("y").await.expect("held frame");
    ctx.register_catalog(
        "datafusion",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );
    let error = held.collect().await.expect_err("the home schema is gone");
    assert_eq!(
        error.to_string(),
        "Error during planning: failed to resolve schema: public"
    );
}

#[tokio::test]
async fn merge_output_clause_refuses_with_the_full_text() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t USING src s ON t.id = s.id WHEN MATCHED THEN DELETE OUTPUT d.*",
    )
    .await
    .expect_err("MERGE OUTPUT refuses");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "This feature is not implemented: MERGE OUTPUT/RETURNING clauses are not supported"
    );
}
