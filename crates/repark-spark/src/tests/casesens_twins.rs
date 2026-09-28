use super::super::*;
use super::common::*;
use datafusion::arrow::array::ArrayRef;

type AnswerLeg<'a> = (&'a str, &'a [&'a str], &'a [&'a [&'a str]]);

async fn create_probe_tables(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t (id INT, Data STRING) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.u (id INT, Data STRING) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.u VALUES (1, 'x'), (5, 'y')",
    )
    .await;
}

fn cell(column: &ArrayRef, row: usize) -> String {
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values.value(row).to_string();
    }
    panic!("unexpected column type {}", column.data_type());
}

async fn names_and_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Vec<String>, Vec<Vec<String>>) {
    let frame = execute(ctx, catalogs, sql).await.unwrap();
    let names = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let batches = frame.collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::new();
            for column in 0..batch.num_columns() {
                rendered.push(cell(batch.column(column), row));
            }
            rows.push(rendered);
        }
    }
    rows.sort();
    (names, rows)
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect()
}

#[tokio::test]
async fn case_twin_outputs_answer_with_both_spellings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let legs: &[AnswerLeg<'_>] = &[
        (
            "SELECT ID, id FROM ice.sales.t",
            &["ID", "id"],
            &[&["1", "1"], &["2", "2"]],
        ),
        (
            "SELECT id AS Id, ID FROM ice.sales.t",
            &["Id", "ID"],
            &[&["1", "1"], &["2", "2"]],
        ),
        (
            "SELECT *, ID FROM ice.sales.u",
            &["id", "Data", "ID"],
            &[&["1", "x", "1"], &["5", "y", "5"]],
        ),
        ("SELECT 1 AS a, 2 AS A", &["a", "A"], &[&["1", "2"]]),
        (
            "SELECT * FROM (SELECT ID, id FROM ice.sales.t)",
            &["ID", "id"],
            &[&["1", "1"], &["2", "2"]],
        ),
    ];
    for (sql, names, rows) in legs {
        let (actual_names, actual_rows) = names_and_rows(&ctx, &catalogs, sql).await;
        let expected_names = names.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(actual_names, expected_names, "{sql}");
        assert_eq!(actual_rows, strings(rows), "{sql}");
    }
}

#[tokio::test]
async fn reference_to_a_case_twin_is_ambiguous() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let error = execute(&ctx, &catalogs, "SELECT a FROM (SELECT 1 AS a, 2 AS A)")
        .await
        .err()
        .unwrap();
    let message = error.to_string();
    assert!(
        message.contains("[AMBIGUOUS_REFERENCE]"),
        "unexpected message: {message}"
    );
    assert!(
        message.contains("SQLSTATE: 42704"),
        "unexpected message: {message}"
    );
    assert!(
        message.contains("Reference `a` is ambiguous"),
        "unexpected message: {message}"
    );
    enable_case_sensitive(&ctx);
    let (names, rows) =
        names_and_rows(&ctx, &catalogs, "SELECT a FROM (SELECT 1 AS a, 2 AS A)").await;
    assert_eq!(names, vec!["a".to_string()]);
    assert_eq!(rows, strings(&[&["1"]]));
}

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

async fn session_run(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    session: &CtxTempViews,
    sql: &str,
) -> datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
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

async fn assert_column_already_exists(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    name: &str,
) {
    let error = execute(ctx, catalogs, sql).await.err().unwrap();
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "[COLUMN_ALREADY_EXISTS] The column `{name}` already exists. Choose another name or \
             rename the existing column. SQLSTATE: 42711"
        )),
        "{sql}: {message}"
    );
}

async fn assert_not_found(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) {
    let error = execute(ctx, catalogs, sql).await.err().unwrap();
    assert!(error.to_string().contains("not found"), "{sql}: {error}");
}

#[tokio::test]
async fn exact_duplicates_still_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    for sql in [
        "SELECT id, id FROM ice.sales.t",
        "SELECT 1 AS id, 2 AS id",
        "SELECT ID, ID FROM ice.sales.t",
    ] {
        let error = execute(&ctx, &catalogs, sql).await.err().unwrap();
        assert!(
            error.to_string().contains("unique expression names"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn creating_case_twin_columns_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    assert_column_already_exists(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tw AS SELECT 1 AS a, 2 AS A",
        "a",
    )
    .await;
    assert_not_found(&ctx, &catalogs, "SELECT * FROM ice.sales.tw").await;
    assert_column_already_exists(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tw2 (a INT, A INT) USING iceberg",
        "a",
    )
    .await;
    assert_not_found(&ctx, &catalogs, "SELECT * FROM ice.sales.tw2").await;
    assert_column_already_exists(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.vt AS SELECT 1 AS a, 2 AS A",
        "a",
    )
    .await;
    assert_not_found(&ctx, &catalogs, "SELECT * FROM ice.sales.vt").await;
    let session = CtxTempViews { ctx: ctx.clone() };
    let error = session_run(
        &ctx,
        &catalogs,
        &session,
        "CREATE TEMPORARY VIEW tvt AS SELECT 1 AS a, 2 AS A",
    )
    .await
    .err()
    .unwrap();
    assert!(
        error.to_string().contains(
            "[COLUMN_ALREADY_EXISTS] The column `a` already exists. Choose another name or \
             rename the existing column. SQLSTATE: 42711"
        ),
        "{error}"
    );
    let error = session_run(&ctx, &catalogs, &session, "SELECT * FROM tvt")
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("not found"), "{error}");
}

#[tokio::test]
async fn positional_insert_from_case_twins_answers() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tw8 (a INT, b INT) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tw8 SELECT 1 AS a, 2 AS A",
    )
    .await;
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.tw8").await;
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(rows, strings(&[&["1", "2"]]));
}
