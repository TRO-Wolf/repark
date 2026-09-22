use super::super::*;
use super::common::*;
use datafusion::arrow::array::AsArray;

async fn seed_schema_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> (i64, i64) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id INT, data STRING, cat STRING) USING iceberg"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} SELECT 1 AS id, 'a' AS data, 'x' AS cat"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} SELECT 2 AS id, 'b' AS data, 'y' AS cat"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} CREATE BRANCH b0"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} CREATE TAG t0"),
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), table.into());
    let live = catalogs["ice"].load_table(&ident).await.unwrap();
    let snapshot = live.metadata().current_snapshot_id().unwrap();
    let stamp = live
        .metadata()
        .snapshot_by_id(snapshot)
        .unwrap()
        .timestamp_ms();
    (snapshot, stamp)
}

fn render_cells(batch: &RecordBatch) -> Vec<Vec<String>> {
    let mut rendered = Vec::new();
    for row in 0..batch.num_rows() {
        let mut cells = Vec::new();
        for column in batch.columns() {
            match column.data_type() {
                DataType::Int32 => {
                    let values = column.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
                    if values.is_null(row) {
                        cells.push("NULL".to_string());
                    } else {
                        cells.push(values.value(row).to_string());
                    }
                }
                DataType::Utf8 => {
                    let values = column.as_string::<i32>();
                    if values.is_null(row) {
                        cells.push("NULL".to_string());
                    } else {
                        cells.push(values.value(row).to_string());
                    }
                }
                other => panic!("unexpected branch-schema cell type {other:?}"),
            }
        }
        rendered.push(cells);
    }
    rendered
}

async fn star_shape(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Vec<(String, String)>, Vec<Vec<String>>) {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("query {sql:?} failed: {error}"))
        .collect()
        .await
        .unwrap();
    assert!(!batches.is_empty(), "query {sql:?} returned no batches");
    let names: Vec<(String, String)> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), format!("{:?}", field.data_type())))
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        rows.extend(render_cells(batch));
    }
    rows.sort();
    (names, rows)
}

async fn add_column_seed(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> (i64, i64) {
    let seeded = seed_schema_table(ctx, catalogs, table).await;
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} ADD COLUMN z INT"),
    )
    .await;
    seeded
}

#[tokio::test]
async fn branch_selector_projects_current_schema_after_add_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    add_column_seed(&ctx, &catalogs, "bs_add").await;
    let (names, rows) =
        star_shape(&ctx, &catalogs, "SELECT * FROM ice.sales.bs_add.branch_b0").await;
    assert_eq!(
        names,
        vec![
            ("id".to_string(), "Int32".to_string()),
            ("data".to_string(), "Utf8".to_string()),
            ("cat".to_string(), "Utf8".to_string()),
            ("z".to_string(), "Int32".to_string()),
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                "1".to_string(),
                "a".to_string(),
                "x".to_string(),
                "NULL".to_string()
            ],
            vec![
                "2".to_string(),
                "b".to_string(),
                "y".to_string(),
                "NULL".to_string()
            ],
        ]
    );
}

#[tokio::test]
async fn branch_selector_filters_on_the_added_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    add_column_seed(&ctx, &catalogs, "bs_where").await;
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.bs_where.branch_b0 WHERE z IS NULL"
        )
        .await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn version_as_of_branch_projects_current_schema() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    add_column_seed(&ctx, &catalogs, "bs_version").await;
    let (names, rows) = star_shape(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.bs_version VERSION AS OF 'b0'",
    )
    .await;
    assert_eq!(
        names,
        vec![
            ("id".to_string(), "Int32".to_string()),
            ("data".to_string(), "Utf8".to_string()),
            ("cat".to_string(), "Utf8".to_string()),
            ("z".to_string(), "Int32".to_string()),
        ]
    );
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row[3] == "NULL"));
}

#[tokio::test]
async fn branch_selector_projects_current_schema_after_drop_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_schema_table(&ctx, &catalogs, "bs_drop").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.bs_drop DROP COLUMN data",
    )
    .await;
    let (names, rows) =
        star_shape(&ctx, &catalogs, "SELECT * FROM ice.sales.bs_drop.branch_b0").await;
    assert_eq!(
        names,
        vec![
            ("id".to_string(), "Int32".to_string()),
            ("cat".to_string(), "Utf8".to_string())
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "y".to_string()]
        ]
    );
}

#[tokio::test]
async fn branch_selector_projects_current_schema_after_rename_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_schema_table(&ctx, &catalogs, "bs_rename").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.bs_rename RENAME COLUMN data TO payload",
    )
    .await;
    let (names, rows) = star_shape(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.bs_rename.branch_b0",
    )
    .await;
    assert_eq!(
        names,
        vec![
            ("id".to_string(), "Int32".to_string()),
            ("payload".to_string(), "Utf8".to_string()),
            ("cat".to_string(), "Utf8".to_string()),
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "a".to_string(), "x".to_string()],
            vec!["2".to_string(), "b".to_string(), "y".to_string()],
        ]
    );
}

#[tokio::test]
async fn tag_snapshot_and_timestamp_keep_the_snapshot_schema() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let (snapshot, stamp_ms) = add_column_seed(&ctx, &catalogs, "bs_near").await;
    let stamp_secs = (stamp_ms + 999) / 1000;
    let expected_names = vec![
        ("id".to_string(), "Int32".to_string()),
        ("data".to_string(), "Utf8".to_string()),
        ("cat".to_string(), "Utf8".to_string()),
    ];
    let expected_rows = vec![
        vec!["1".to_string(), "a".to_string(), "x".to_string()],
        vec!["2".to_string(), "b".to_string(), "y".to_string()],
    ];
    let queries = vec![
        "SELECT * FROM ice.sales.bs_near.tag_t0".to_string(),
        "SELECT * FROM ice.sales.bs_near VERSION AS OF 't0'".to_string(),
        format!("SELECT * FROM ice.sales.bs_near.snapshot_id_{snapshot}"),
        format!("SELECT * FROM ice.sales.bs_near VERSION AS OF {snapshot}"),
        format!("SELECT * FROM ice.sales.bs_near TIMESTAMP AS OF {stamp_secs}"),
    ];
    for sql in &queries {
        let (names, rows) = star_shape(&ctx, &catalogs, sql).await;
        assert_eq!(names, expected_names, "{sql}");
        assert_eq!(rows, expected_rows, "{sql}");
    }
}

#[tokio::test]
async fn unknown_ref_keeps_the_exact_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    add_column_seed(&ctx, &catalogs, "bs_unknown").await;
    let selector_err = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.bs_unknown.branch_nope",
    )
    .await
    .expect_err("an unknown branch selector must refuse")
    .to_string();
    let version_err = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.bs_unknown VERSION AS OF 'nope'",
    )
    .await
    .expect_err("an unknown VERSION AS OF ref must refuse")
    .to_string();
    assert_eq!(selector_err, version_err);
    assert_eq!(
        selector_err,
        "External error: Cannot find matching snapshot ID or reference name for version nope"
    );
}

#[tokio::test]
async fn reader_options_branch_projects_current_schema() {
    use repark_core::time_travel::{TimeTravelSpec, read_table_at};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    add_column_seed(&ctx, &catalogs, "bs_reader").await;
    let zone = repark_core::SessionTimeZone::default();
    let parts = vec![
        "ice".to_string(),
        "sales".to_string(),
        "bs_reader".to_string(),
    ];
    let branch_batches = read_table_at(
        &ctx,
        &catalogs,
        &parts,
        &TimeTravelSpec::VersionRef("b0".to_string()),
        &zone,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let branch_names: Vec<String> = branch_batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        branch_names,
        vec![
            "id".to_string(),
            "data".to_string(),
            "cat".to_string(),
            "z".to_string()
        ]
    );
    let tag_batches = read_table_at(
        &ctx,
        &catalogs,
        &parts,
        &TimeTravelSpec::VersionRef("t0".to_string()),
        &zone,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let tag_names: Vec<String> = tag_batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        tag_names,
        vec!["id".to_string(), "data".to_string(), "cat".to_string()]
    );
}
