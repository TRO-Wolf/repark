use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int32Type};

use super::super::*;
use super::common::*;

const NTZ_FIRST: &str = "VALUES (1, TIMESTAMP_NTZ '2024-07-01 12:00:00'), (2, TIMESTAMP \
                         '2024-01-15 23:30:00'), (3, TIMESTAMP_NTZ '2024-03-10 02:30:00'), (4, \
                         NULL)";

const LTZ_FIRST: &str = "VALUES (1, TIMESTAMP '2024-07-01 12:00:00'), (2, TIMESTAMP_NTZ \
                         '2024-01-15 23:30:00'), (3, TIMESTAMP '2024-03-10 02:30:00'), (4, NULL)";

const SPARK_WALLS: [(&str, [&str; 3]); 2] = [
    (
        "America/New_York",
        [
            "2024-07-01 12:00:00",
            "2024-01-15 23:30:00",
            "2024-03-10 03:30:00",
        ],
    ),
    (
        "Asia/Kolkata",
        [
            "2024-07-01 12:00:00",
            "2024-01-15 23:30:00",
            "2024-03-10 02:30:00",
        ],
    ),
];

async fn session_at(warehouse: &TempDir, zone: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup_at_zone(warehouse, zone).await;
    repark_functions::register_all(&ctx);
    (ctx, catalogs)
}

async fn text_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<Vec<Option<String>>> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let columns: Vec<_> = (1..batch.num_columns())
            .map(|column| cast(batch.column(column), &DataType::Utf8).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let mut cells = vec![Some(ids.value(row).to_string())];
            for column in &columns {
                let texts = column.as_string::<i32>();
                cells.push((!texts.is_null(row)).then(|| texts.value(row).to_string()));
            }
            rows.push(cells);
        }
    }
    rows.sort();
    rows
}

fn door_sql(door: &str, source: &str) -> (bool, String) {
    let inline = format!("SELECT * FROM {source} AS s(id, c)");
    let (seeded, sql) = match door {
        "insert_values" => (false, format!("INSERT INTO {{t}} {source}")),
        "insert_select" => (false, format!("INSERT INTO {{t}} {inline}")),
        "overwrite_values" => (false, format!("INSERT OVERWRITE {{t}} {source}")),
        "overwrite_select" => (false, format!("INSERT OVERWRITE {{t}} {inline}")),
        "by_name" => (false, format!("INSERT INTO {{t}} BY NAME {inline}")),
        "overwrite_by_name" => (false, format!("INSERT OVERWRITE {{t}} BY NAME {inline}")),
        "merge_insert_star" => (
            false,
            format!(
                "MERGE INTO {{t}} t USING ({inline}) s ON t.id = s.id WHEN NOT MATCHED THEN \
                 INSERT *"
            ),
        ),
        "merge_insert" => (
            false,
            format!(
                "MERGE INTO {{t}} t USING ({inline}) s ON t.id = s.id WHEN NOT MATCHED THEN \
                 INSERT (id, c) VALUES (s.id, s.c)"
            ),
        ),
        "merge_update" => (
            true,
            format!(
                "MERGE INTO {{t}} t USING ({inline}) s ON t.id = s.id WHEN MATCHED THEN UPDATE \
                 SET c = s.c"
            ),
        ),
        "merge_update_star" => (
            true,
            format!(
                "MERGE INTO {{t}} t USING ({inline}) s ON t.id = s.id WHEN MATCHED THEN UPDATE \
                 SET *"
            ),
        ),
        other => panic!("unknown door {other}"),
    };
    (seeded, sql.replace("{t}", "ice.sales.mix"))
}

async fn assert_door_stores_sparks_walls(door: &str) {
    for (zone, walls) in SPARK_WALLS {
        for column_type in ["TIMESTAMP", "TIMESTAMP_NTZ"] {
            for (order, source) in [("ntz first", NTZ_FIRST), ("ltz first", LTZ_FIRST)] {
                let warehouse = TempDir::new().unwrap();
                let (ctx, catalogs) = session_at(&warehouse, zone).await;
                run(
                    &ctx,
                    &catalogs,
                    &format!("CREATE TABLE ice.sales.mix (id INT, c {column_type}) USING iceberg"),
                )
                .await;
                let (seeded, sql) = door_sql(door, source);
                if seeded {
                    run(
                        &ctx,
                        &catalogs,
                        "INSERT INTO ice.sales.mix VALUES (1, NULL), (2, NULL), (3, NULL), (4, \
                         NULL)",
                    )
                    .await;
                }
                run(&ctx, &catalogs, &sql).await;
                let got = text_rows(
                    &ctx,
                    &catalogs,
                    "SELECT id, CAST(c AS STRING) FROM ice.sales.mix",
                )
                .await;
                let mut want: Vec<Vec<Option<String>>> = (1..=3)
                    .zip(walls)
                    .map(|(id, wall)| vec![Some(id.to_string()), Some(wall.to_string())])
                    .collect();
                want.push(vec![Some("4".to_string()), None]);
                assert_eq!(got, want, "{door} {zone} {column_type} {order}");
            }
        }
    }
}

#[tokio::test]
async fn mixed_values_type_the_column_timestamp_through_the_session_zone() {
    for (zone, walls) in SPARK_WALLS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = session_at(&warehouse, zone).await;
        for source in [NTZ_FIRST, LTZ_FIRST] {
            let got = text_rows(
                &ctx,
                &catalogs,
                &format!(
                    "SELECT CAST(id AS INT), typeof(c), CAST(c AS STRING) FROM {source} AS s(id, c)"
                ),
            )
            .await;
            let mut want: Vec<Vec<Option<String>>> = (1..=3)
                .zip(walls)
                .map(|(id, wall)| {
                    vec![
                        Some(id.to_string()),
                        Some("timestamp".to_string()),
                        Some(wall.to_string()),
                    ]
                })
                .collect();
            want.push(vec![
                Some("4".to_string()),
                Some("timestamp".to_string()),
                None,
            ]);
            assert_eq!(got, want, "{zone} {source}");
        }
    }
}

#[tokio::test]
async fn unmixed_values_keep_their_timestamp_type() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = session_at(&warehouse, "America/New_York").await;
    let got = text_rows(
        &ctx,
        &catalogs,
        "SELECT CAST(id AS INT), typeof(a), CAST(a AS STRING), typeof(b), CAST(b AS STRING) FROM \
         VALUES (1, TIMESTAMP_NTZ '2024-07-01 12:00:00', TIMESTAMP '2024-07-01 12:00:00'), (2, \
         TIMESTAMP_NTZ '2024-03-10 02:30:00', NULL) AS s(id, a, b)",
    )
    .await;
    let text = |value: &str| Some(value.to_string());
    assert_eq!(
        got,
        vec![
            vec![
                text("1"),
                text("timestamp_ntz"),
                text("2024-07-01 12:00:00"),
                text("timestamp"),
                text("2024-07-01 12:00:00"),
            ],
            vec![
                text("2"),
                text("timestamp_ntz"),
                text("2024-03-10 02:30:00"),
                text("timestamp"),
                None,
            ],
        ]
    );
}

#[tokio::test]
async fn insert_values_stores_sparks_walls() {
    assert_door_stores_sparks_walls("insert_values").await;
}

#[tokio::test]
async fn insert_select_stores_sparks_walls() {
    assert_door_stores_sparks_walls("insert_select").await;
}

#[tokio::test]
async fn overwrite_values_stores_sparks_walls() {
    assert_door_stores_sparks_walls("overwrite_values").await;
}

#[tokio::test]
async fn overwrite_select_stores_sparks_walls() {
    assert_door_stores_sparks_walls("overwrite_select").await;
}

#[tokio::test]
async fn by_name_stores_sparks_walls() {
    assert_door_stores_sparks_walls("by_name").await;
}

#[tokio::test]
async fn overwrite_by_name_stores_sparks_walls() {
    assert_door_stores_sparks_walls("overwrite_by_name").await;
}

#[tokio::test]
async fn merge_insert_star_stores_sparks_walls() {
    assert_door_stores_sparks_walls("merge_insert_star").await;
}

#[tokio::test]
async fn merge_insert_stores_sparks_walls() {
    assert_door_stores_sparks_walls("merge_insert").await;
}

#[tokio::test]
async fn merge_update_stores_sparks_walls() {
    assert_door_stores_sparks_walls("merge_update").await;
}

#[tokio::test]
async fn merge_update_star_stores_sparks_walls() {
    assert_door_stores_sparks_walls("merge_update_star").await;
}
