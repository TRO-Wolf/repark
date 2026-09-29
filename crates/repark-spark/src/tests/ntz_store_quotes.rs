use datafusion::arrow::array::AsArray;

use super::super::*;
use super::common::*;
use super::ntz_store::setup_ntz;

async fn id_strings(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(i32, String)> {
    let out = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let ids = out[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    let texts = out[0].column(1).as_string::<i32>();
    (0..out[0].num_rows())
        .map(|row| (ids.value(row), texts.value(row).to_string()))
        .collect()
}

#[tokio::test]
async fn backslash_quote_strings_store_their_exact_values() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bs (id INT, s STRING, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    let wall = "TIMESTAMP_NTZ'2024-01-02 03:04:05'";
    for (id, cell) in [
        (1, r#"'{"msg": "it\\''s"}'"#),
        (2, r"'C:\\dir\\''s file'"),
        (3, r"'end\\'''"),
        (4, r"'no quote'"),
        (101, r"'end\\'"),
        (102, r"'a\\\\''b'"),
        (103, r"'a\nb\tc'"),
        (104, r"'\u00e9'"),
        (106, r"'-- x\\''y'"),
        (109, r"'a\nb\\''c'"),
        (1051, r"'a\\''b'"),
        (1054, r"'\\'''"),
    ] {
        let sql = format!("INSERT INTO ice.sales.bs VALUES ({id}, {cell}, {wall})");
        run(&ctx, &catalogs, &sql).await;
    }
    run(
        &ctx,
        &catalogs,
        &format!(
            "INSERT INTO ice.sales.bs (c, id, s) VALUES ({wall}, 5, {})",
            r"'x\\''y'"
        ),
    )
    .await;
    for (id, cell) in [
        (
            10,
            r"CAST(replace('2024-01-02 03:04:05\\''', '\\''', '') AS TIMESTAMP_NTZ)",
        ),
        (
            107,
            r"CAST(replace('-- x\\''y2024-01-02 03:04:05', '-- x\\''y', '') AS TIMESTAMP_NTZ)",
        ),
    ] {
        let sql = format!("INSERT INTO ice.sales.bs VALUES ({id}, 'x', {cell})");
        run(&ctx, &catalogs, &sql).await;
    }
    assert_eq!(
        id_strings(
            &ctx,
            &catalogs,
            "SELECT id, s FROM ice.sales.bs ORDER BY id"
        )
        .await,
        vec![
            (1, "{\"msg\": \"it\\'s\"}".to_string()),
            (2, "C:\\dir\\'s file".to_string()),
            (3, "end\\'".to_string()),
            (4, "no quote".to_string()),
            (5, "x\\'y".to_string()),
            (10, "x".to_string()),
            (101, "end\\".to_string()),
            (102, "a\\\\'b".to_string()),
            (103, "a\nb\tc".to_string()),
            (104, "é".to_string()),
            (106, "-- x\\'y".to_string()),
            (107, "x".to_string()),
            (109, "a\nb\\'c".to_string()),
            (1051, "a\\'b".to_string()),
            (1054, "\\'".to_string()),
        ]
    );
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.bs").await,
        [
            1, 2, 3, 4, 5, 10, 101, 102, 103, 104, 106, 107, 109, 1051, 1054
        ]
        .map(|id| (id, Some("2024-01-02 03:04:05".to_string())))
        .to_vec()
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bsm (id INT, m MAP<STRING, STRING>, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    for (id, cell) in [(6, r"map('k', 'v\\''w')"), (401, r"map('k', '-- x\\''y')")] {
        let sql = format!("INSERT INTO ice.sales.bsm VALUES ({id}, {cell}, {wall})");
        run(&ctx, &catalogs, &sql).await;
    }
    assert_eq!(
        id_strings(
            &ctx,
            &catalogs,
            "SELECT id, m['k'] AS v FROM ice.sales.bsm ORDER BY id"
        )
        .await,
        vec![(6, "v\\'w".to_string()), (401, "-- x\\'y".to_string()),]
    );
}
