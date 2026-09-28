use super::super::*;
use super::common::*;
use datafusion::arrow::array::ArrayRef;

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

#[tokio::test]
async fn derived_and_cte_projections_keep_the_written_spelling() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT ID, DATA FROM ice.sales.t)",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "DATA".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH c AS (SELECT ID, DATA FROM ice.sales.t) SELECT * FROM c",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "DATA".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH c AS (SELECT Id FROM ice.sales.t) SELECT * FROM c WHERE ID > 1",
    )
    .await;
    assert_eq!(names, vec!["Id".to_string()]);
    assert_eq!(rows, vec![vec!["2".to_string()]]);
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT ID FROM ice.sales.t UNION ALL SELECT id FROM ice.sales.u)",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string()],
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["5".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT * FROM (SELECT ID FROM ice.sales.t))",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string()]);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["2".to_string()]]);
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT ID FROM ice.sales.t) x \
         JOIN (SELECT id FROM ice.sales.t) y ON x.id = y.ID",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "id".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["2".to_string(), "2".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM (SELECT 1 AS ID)").await;
    assert_eq!(names, vec!["ID".to_string()]);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

#[tokio::test]
async fn values_derived_table_first_keeps_later_spellings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (VALUES (7)) AS v(x) \
         CROSS JOIN (SELECT id FROM ice.sales.t) a \
         CROSS JOIN (SELECT Data FROM ice.sales.u) b",
    )
    .await;
    assert_eq!(
        names,
        vec!["x".to_string(), "id".to_string(), "Data".to_string()]
    );
    assert_eq!(
        rows,
        vec![
            vec!["7".to_string(), "1".to_string(), "x".to_string()],
            vec!["7".to_string(), "1".to_string(), "y".to_string()],
            vec!["7".to_string(), "2".to_string(), "x".to_string()],
            vec!["7".to_string(), "2".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn values_cte_keeps_later_spellings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH v(x) AS (VALUES (7)), a AS (SELECT id FROM ice.sales.t), \
         b AS (SELECT Data FROM ice.sales.u) SELECT * FROM v, a, b",
    )
    .await;
    assert_eq!(
        names,
        vec!["x".to_string(), "id".to_string(), "Data".to_string()]
    );
    assert_eq!(
        rows,
        vec![
            vec!["7".to_string(), "1".to_string(), "x".to_string()],
            vec!["7".to_string(), "1".to_string(), "y".to_string()],
            vec!["7".to_string(), "2".to_string(), "x".to_string()],
            vec!["7".to_string(), "2".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn values_body_in_the_middle_keeps_later_spellings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT ID FROM ice.sales.t) a \
         CROSS JOIN (VALUES (7)) AS v(x) \
         CROSS JOIN (SELECT DATA FROM ice.sales.u) b",
    )
    .await;
    assert_eq!(
        names,
        vec!["ID".to_string(), "x".to_string(), "DATA".to_string()]
    );
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "7".to_string(), "x".to_string()],
            vec!["1".to_string(), "7".to_string(), "y".to_string()],
            vec!["2".to_string(), "7".to_string(), "x".to_string()],
            vec!["2".to_string(), "7".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn values_left_set_operation_keeps_later_spellings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (VALUES (7) UNION ALL SELECT id FROM ice.sales.t) AS v(x) \
         CROSS JOIN (SELECT ID FROM ice.sales.u) b",
    )
    .await;
    assert_eq!(names, vec!["x".to_string(), "ID".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["1".to_string(), "5".to_string()],
            vec!["2".to_string(), "1".to_string()],
            vec!["2".to_string(), "5".to_string()],
            vec!["7".to_string(), "1".to_string()],
            vec!["7".to_string(), "5".to_string()],
        ]
    );
}

#[tokio::test]
async fn natural_join_over_case_differing_derived_outputs_cross_joins() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT ID FROM ice.sales.t) a NATURAL JOIN (SELECT id FROM ice.sales.u) b",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "id".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["1".to_string(), "5".to_string()],
            vec!["2".to_string(), "1".to_string()],
            vec!["2".to_string(), "5".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT count(*) AS c FROM (SELECT ID FROM ice.sales.t) a NATURAL JOIN (SELECT id FROM ice.sales.u) b",
    )
    .await;
    assert_eq!(names, vec!["c".to_string()]);
    assert_eq!(rows, vec![vec!["4".to_string()]]);
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH a AS (SELECT ID FROM ice.sales.t), b AS (SELECT id FROM ice.sales.u) SELECT * FROM a NATURAL JOIN b",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "id".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["1".to_string(), "5".to_string()],
            vec!["2".to_string(), "1".to_string()],
            vec!["2".to_string(), "5".to_string()],
        ]
    );
}

#[tokio::test]
async fn swapped_spellings_stay_with_their_scope() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (VALUES (7)) AS v(x) \
         CROSS JOIN (SELECT id FROM ice.sales.t) a \
         CROSS JOIN (SELECT ID FROM ice.sales.u) b",
    )
    .await;
    assert_eq!(
        names,
        vec!["x".to_string(), "id".to_string(), "ID".to_string()]
    );
    assert_eq!(
        rows,
        vec![
            vec!["7".to_string(), "1".to_string(), "1".to_string()],
            vec!["7".to_string(), "1".to_string(), "5".to_string()],
            vec!["7".to_string(), "2".to_string(), "1".to_string()],
            vec!["7".to_string(), "2".to_string(), "5".to_string()],
        ]
    );
}

#[tokio::test]
async fn column_alias_list_keeps_its_spelling() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT * FROM (SELECT id FROM ice.sales.t) AS x(Kay)",
    )
    .await;
    assert_eq!(names, vec!["Kay".to_string()]);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["2".to_string()]]);
}

#[tokio::test]
async fn already_equal_nested_shapes_stay() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    for sql in [
        "SELECT ID FROM (SELECT id FROM ice.sales.t)",
        "SELECT ID FROM (SELECT ID FROM ice.sales.t)",
        "SELECT x.ID FROM (SELECT id FROM ice.sales.t) x",
        "SELECT ID FROM ice.sales.t GROUP BY ID HAVING count(*) > 0",
        "SELECT ID FROM ice.sales.t t WHERE EXISTS (SELECT 1 FROM ice.sales.t u WHERE u.ID = t.id)",
    ] {
        let (names, rows) = names_and_rows(&ctx, &catalogs, sql).await;
        assert_eq!(names, vec!["ID".to_string()], "{sql}");
        assert_eq!(
            rows,
            vec![vec!["1".to_string()], vec!["2".to_string()]],
            "{sql}"
        );
    }
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "SELECT Data FROM ice.sales.t WHERE ID IN (SELECT ID FROM ice.sales.u)",
    )
    .await;
    assert_eq!(names, vec!["Data".to_string()]);
    assert_eq!(rows, vec![vec!["a".to_string()]]);
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH c AS (SELECT * FROM ice.sales.t) SELECT DATA FROM c",
    )
    .await;
    assert_eq!(names, vec!["DATA".to_string()]);
    assert_eq!(rows, vec![vec!["a".to_string()], vec!["b".to_string()]]);
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH c AS (SELECT id, Data FROM ice.sales.t) SELECT ID, DATA FROM c",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "DATA".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
}

#[tokio::test]
async fn merge_with_a_derived_source_spelled_in_another_case() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m (id INT, Data STRING) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.m VALUES (1, 'p')").await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.m t USING (SELECT id + 1 AS id, Data FROM ice.sales.u) AS src \
         ON t.id = src.id WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.m").await;
    assert_eq!(names, vec!["id".to_string(), "Data".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "p".to_string()],
            vec!["2".to_string(), "x".to_string()],
            vec!["6".to_string(), "y".to_string()],
        ]
    );
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.m t USING (SELECT 1 AS ID, 'Q' AS DATA) AS src ON t.ID = src.ID \
         WHEN MATCHED THEN UPDATE SET t.DATA = src.DATA",
    )
    .await;
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.m").await;
    assert_eq!(names, vec!["id".to_string(), "Data".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "Q".to_string()],
            vec!["2".to_string(), "x".to_string()],
            vec!["6".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn cte_outer_reference_in_another_case_binds() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let (names, rows) = names_and_rows(
        &ctx,
        &catalogs,
        "WITH c AS (SELECT id, Data FROM ice.sales.t) SELECT ID, DATA FROM c",
    )
    .await;
    assert_eq!(names, vec!["ID".to_string(), "DATA".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
}

#[tokio::test]
async fn catalog_view_body_keeps_its_spelling() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT ID, DATA FROM ice.sales.t",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v2 AS SELECT id, Data FROM ice.sales.t",
    )
    .await;
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.v").await;
    assert_eq!(names, vec!["ID".to_string(), "DATA".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT id, data FROM ice.sales.v").await;
    assert_eq!(names, vec!["id".to_string(), "data".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(&ctx, &catalogs, "SELECT ID, DATA FROM ice.sales.v2").await;
    assert_eq!(names, vec!["ID".to_string(), "DATA".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ]
    );
    let (names, rows) = names_and_rows(&ctx, &catalogs, "DESCRIBE ice.sales.v").await;
    assert_eq!(
        names,
        vec![
            "col_name".to_string(),
            "data_type".to_string(),
            "comment".to_string()
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec!["DATA".to_string(), "string".to_string(), String::new()],
            vec!["ID".to_string(), "int".to_string(), String::new()],
        ]
    );
}

#[tokio::test]
async fn twin_cte_outputs_still_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    let error = execute(
        &ctx,
        &catalogs,
        "WITH c AS (SELECT 1 AS a, 2 AS A) SELECT A FROM c",
    )
    .await
    .err()
    .unwrap();
    let message = error.to_string();
    assert!(
        message.contains("[AMBIGUOUS_REFERENCE]")
            && message.contains("Reference `A` is ambiguous")
            && message.contains("SQLSTATE: 42704"),
        "{message}"
    );
}

#[tokio::test]
async fn dml_spelled_in_another_case_answers() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_probe_tables(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m (id INT, Data STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.m VALUES (1, 'p'), (2, 'x'), (6, 'y')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.m SET DATA = 'U' WHERE ID = 1",
    )
    .await;
    let (_, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.m").await;
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "U".to_string()],
            vec!["2".to_string(), "x".to_string()],
            vec!["6".to_string(), "y".to_string()],
        ]
    );
    run(&ctx, &catalogs, "DELETE FROM ice.sales.m WHERE DATA = 'U'").await;
    let (_, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.m").await;
    assert_eq!(
        rows,
        vec![
            vec!["2".to_string(), "x".to_string()],
            vec!["6".to_string(), "y".to_string()],
        ]
    );
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.m (ID, DATA) VALUES (8, 'i')",
    )
    .await;
    let (_, rows) = names_and_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.m").await;
    assert_eq!(
        rows,
        vec![
            vec!["2".to_string(), "x".to_string()],
            vec!["6".to_string(), "y".to_string()],
            vec!["8".to_string(), "i".to_string()],
        ]
    );
}
