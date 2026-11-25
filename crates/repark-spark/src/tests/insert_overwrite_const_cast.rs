use super::super::*;
use super::common::*;

const CARD: &str = "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE 1 = 0";

fn card(target: &str, value: &str) -> String {
    CARD.replace("{T}", target).replace("{V}", value)
}

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> datafusion::error::Result<()> {
    execute(ctx, catalogs, sql).await?.collect().await?;
    Ok(())
}

async fn seeded(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &str,
    column: &str,
    value: &str,
) -> String {
    let table = format!("ice.sales.{name}");
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE {table} (x {column}, p INT) USING iceberg PARTITIONED BY (p)"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO {table} SELECT {value}, 1"),
    )
    .await;
    table
}

#[tokio::test]
async fn the_session_plans_each_card_cast_to_one_guarded_cast() {
    let wh = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&wh).await;
    for target in ["DOUBLE", "FLOAT", "TIMESTAMP"] {
        let frame = crate::spark_ast::execute_passthrough(&ctx, &catalogs, &card(target, "x"))
            .await
            .expect("plan");
        let casts = crate::insert_overwrite::const_cast::constant_casts(frame.logical_plan());
        assert_eq!(
            casts.len(),
            1,
            "{target}: {}",
            frame.logical_plan().display_indent()
        );
        assert_eq!(casts[0].0, "x", "{target}");
    }
}

#[tokio::test]
async fn overwrite_doors_refuse_the_card_cells_and_keep_the_rows() {
    let wh = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&wh).await;
    let cells = [
        ("DOUBLE", "CAST(1.5 AS DOUBLE)"),
        ("TIMESTAMP", "TIMESTAMP'2024-01-02 03:04:05'"),
    ];
    for (index, (column, value)) in cells.into_iter().enumerate() {
        let table = seeded(&ctx, &catalogs, &format!("ioc_{index}"), column, value).await;
        let source = card(column, "x");
        for statement in [
            format!("INSERT OVERWRITE {table} SELECT *, 1 FROM ({source}) q"),
            format!("INSERT OVERWRITE TABLE {table} SELECT *, 1 FROM ({source}) q"),
            format!("INSERT OVERWRITE {table} PARTITION (p = 1) {source}"),
        ] {
            let error = outcome(&ctx, &catalogs, &statement)
                .await
                .expect_err(&statement);
            assert!(
                error.to_string().contains("[CAST_INVALID_INPUT]"),
                "{statement}: {error}"
            );
            assert_eq!(
                rows(&ctx, &catalogs, &format!("SELECT * FROM {table}")).await,
                1
            );
        }
    }
}

#[tokio::test]
async fn values_outside_the_table_and_ansi_off_still_overwrite() {
    let wh = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&wh).await;
    let table = seeded(&ctx, &catalogs, "ioc_nan", "DOUBLE", "CAST(1.5 AS DOUBLE)").await;
    let statement = format!(
        "INSERT OVERWRITE {table} SELECT *, 1 FROM ({}) q",
        card("DOUBLE", "NaN")
    );
    outcome(&ctx, &catalogs, &statement)
        .await
        .expect("NaN is a DOUBLE");
    assert_eq!(
        rows(&ctx, &catalogs, &format!("SELECT * FROM {table}")).await,
        0
    );
    let wh_off = TempDir::new().expect("warehouse");
    let (ctx_off, catalogs_off) = setup_with_ansi(&wh_off, false).await;
    let column = "TIMESTAMP";
    let off = seeded(
        &ctx_off,
        &catalogs_off,
        "ioc_off",
        column,
        "TIMESTAMP'2024-01-02 03:04:05'",
    )
    .await;
    let statement = format!(
        "INSERT OVERWRITE {off} SELECT *, 1 FROM ({}) q",
        card(column, "x")
    );
    outcome(&ctx_off, &catalogs_off, &statement)
        .await
        .expect("ANSI off writes");
    assert_eq!(
        rows(&ctx_off, &catalogs_off, &format!("SELECT * FROM {off}")).await,
        0
    );
}
