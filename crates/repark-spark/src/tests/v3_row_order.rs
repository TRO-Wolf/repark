use super::super::*;
use super::common::*;

const V3_MOR: &str = "'format-version' = '3', \
     'write.delete.mode' = 'merge-on-read', \
     'write.update.mode' = 'merge-on-read', \
     'write.merge.mode' = 'merge-on-read'";

async fn lineage(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i64, Option<i64>, Option<i64>)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "SELECT CAST(id AS BIGINT) AS row_key, _row_id, _last_updated_sequence_number \
             FROM ice.sales.{table} ORDER BY id"
        ),
    )
    .await
    .unwrap_or_else(|err| panic!("lineage select on {table}: {err}"))
    .collect()
    .await
    .unwrap_or_else(|err| panic!("lineage collect on {table}: {err}"));
    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("id casts to BIGINT");
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("_row_id is BIGINT");
        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("_last_updated_sequence_number is BIGINT");
        for row in 0..batch.num_rows() {
            out.push((
                ids.value(row),
                (!row_ids.is_null(row)).then(|| row_ids.value(row)),
                (!seqs.is_null(row)).then(|| seqs.value(row)),
            ));
        }
    }
    out
}

async fn live_v3_merge_cell(table: &str) -> Vec<(i64, Option<i64>, Option<i64>)> {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} USING iceberg PARTITIONED BY (part) \
             TBLPROPERTIES ({V3_MOR}) AS \
             SELECT * FROM (VALUES (1, 'n1', 1)) AS t(id, name, part)"
        ),
    )
    .await;
    for id in 2..=10 {
        run(
            &ctx,
            &catalogs,
            &format!(
                "INSERT INTO ice.sales.{table} VALUES ({id}, 'n{id}', {})",
                id % 2
            ),
        )
        .await;
    }
    run(
        &ctx,
        &catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 3"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "MERGE INTO ice.sales.{table} AS t USING \
             (SELECT 2 AS id, 'm2' AS name, 0 AS part \
              UNION ALL SELECT 11, 'n11', 1) AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name, part) VALUES (s.id, s.name, s.part)"
        ),
    )
    .await;
    lineage(&ctx, &catalogs, table).await
}

#[tokio::test]
async fn mor_merge_insert_takes_sparks_row_id_in_ten_consecutive_runs() {
    let _: &str = "pins: v3-11-row-id-determinism/C-002, C-003";
    let survivors: Vec<(i64, Option<i64>, Option<i64>)> = vec![
        (1, Some(0), Some(1)),
        (2, Some(1), Some(12)),
        (4, Some(3), Some(4)),
        (5, Some(4), Some(5)),
        (6, Some(5), Some(6)),
        (7, Some(6), Some(7)),
        (8, Some(7), Some(8)),
        (9, Some(8), Some(9)),
        (10, Some(9), Some(10)),
        (11, Some(11), Some(12)),
    ];
    let mut observed = Vec::new();
    let mut full = Vec::new();
    for attempt in 0..10 {
        let rows = live_v3_merge_cell(&format!("rowid_{attempt}")).await;
        observed.push(
            rows.iter()
                .find(|(id, _, _)| *id == 11)
                .unwrap_or_else(|| panic!("run {attempt} lost the inserted row: {rows:?}"))
                .1,
        );
        full.push(rows);
    }
    assert_eq!(
        observed,
        vec![Some(11); 10],
        "Spark reads 11 in 10 of 10; this engine read {observed:?}"
    );
    assert_eq!(full, vec![survivors; 10]);
}

#[tokio::test]
async fn partitioned_ctas_numbers_files_ascending_by_partition_value() {
    let _: &str = "pins: v3-11-row-id-determinism/C-003";
    for attempt in 0..5 {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        let table = format!("ctasorder_{attempt}");
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} USING iceberg PARTITIONED BY (part) \
                 TBLPROPERTIES ('format-version' = '3') AS \
                 SELECT * FROM (VALUES (1, 2), (2, 0), (3, 1), (4, 0), (5, 2)) AS t(id, part)"
            ),
        )
        .await;
        assert_eq!(
            lineage(&ctx, &catalogs, &table).await,
            vec![
                (1, Some(3), Some(1)),
                (2, Some(0), Some(1)),
                (3, Some(2), Some(1)),
                (4, Some(1), Some(1)),
                (5, Some(4), Some(1)),
            ],
            "run {attempt}"
        );
    }
}

#[tokio::test]
async fn mor_merge_across_three_partitions_numbers_files_ascending_by_partition_value() {
    let _: &str = "pins: v3-11-row-id-determinism/C-003";
    for attempt in 0..5 {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        let table = format!("mergeorder_{attempt}");
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id INT, name STRING, part INT) USING iceberg \
                 PARTITIONED BY (part) TBLPROPERTIES ({V3_MOR})"
            ),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a', 2), (2, 'b', 2)"),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "MERGE INTO ice.sales.{table} AS t USING \
                 (SELECT 2 AS id, 'm' AS name, 2 AS part \
                  UNION ALL SELECT 7, 'g', 1 UNION ALL SELECT 8, 'h', 0) AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.name = s.name \
                 WHEN NOT MATCHED THEN INSERT (id, name, part) VALUES (s.id, s.name, s.part)"
            ),
        )
        .await;
        assert_eq!(
            lineage(&ctx, &catalogs, &table).await,
            vec![
                (1, Some(0), Some(1)),
                (2, Some(1), Some(2)),
                (7, Some(3), Some(2)),
                (8, Some(2), Some(2)),
            ],
            "run {attempt}"
        );
    }
}

async fn ctas_partition_order(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    partitioning: &str,
    columns: &str,
    values: &str,
) -> Vec<(i64, Option<i64>)> {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} USING iceberg PARTITIONED BY ({partitioning}) \
             TBLPROPERTIES ('format-version' = '3') AS \
             SELECT * FROM (VALUES {values}) AS t({columns})"
        ),
    )
    .await;
    lineage(ctx, catalogs, table)
        .await
        .into_iter()
        .map(|(id, row_id, _)| (id, row_id))
        .collect()
}

#[tokio::test]
async fn a_null_partition_slot_is_numbered_first_whatever_order_it_arrives_in() {
    let _: &str = "pins: v3-11-row-id-determinism/C-007";
    for (attempt, values) in [
        "(1, 0), (2, CAST(NULL AS INT)), (3, 1)",
        "(1, CAST(NULL AS INT)), (2, 0), (3, 1)",
        "(1, 1), (2, CAST(NULL AS INT)), (3, 0)",
    ]
    .into_iter()
    .enumerate()
    {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        let table = format!("nullpart_{attempt}");
        let rows = ctas_partition_order(&ctx, &catalogs, &table, "part", "id, part", values).await;
        let expected = match attempt {
            0 => vec![(1, Some(1)), (2, Some(0)), (3, Some(2))],
            1 => vec![(1, Some(0)), (2, Some(1)), (3, Some(2))],
            _ => vec![(1, Some(2)), (2, Some(0)), (3, Some(1))],
        };
        assert_eq!(rows, expected, "arrival {attempt}");
    }
}

#[tokio::test]
async fn a_two_field_spec_orders_lexicographically_in_spec_field_order() {
    let _: &str = "pins: v3-11-row-id-determinism/C-007";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    let rows = ctas_partition_order(
        &ctx,
        &catalogs,
        "twofield",
        "a, b",
        "id, a, b",
        "(1, 0, 1), (2, 0, 0), (3, 1, 1), (4, 1, 0), (5, 2, 0)",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, Some(1)),
            (2, Some(0)),
            (3, Some(3)),
            (4, Some(2)),
            (5, Some(4)),
        ]
    );
}

#[tokio::test]
async fn transform_partitions_order_by_the_transformed_value_ascending() {
    let _: &str = "pins: v3-11-row-id-determinism/C-007";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    assert_eq!(
        ctas_partition_order(
            &ctx,
            &catalogs,
            "trunc",
            "truncate(1, part)",
            "id, part",
            "(1, 'aa'), (2, 'bb'), (3, 'cc'), (4, 'dd'), (5, 'ee')",
        )
        .await,
        vec![
            (1, Some(0)),
            (2, Some(1)),
            (3, Some(2)),
            (4, Some(3)),
            (5, Some(4)),
        ],
        "truncate"
    );
    assert_eq!(
        ctas_partition_order(
            &ctx,
            &catalogs,
            "bkt",
            "bucket(4, part)",
            "id, part",
            "(1, 0), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5), (7, 6), (8, 7)",
        )
        .await,
        vec![
            (1, Some(0)),
            (2, Some(1)),
            (3, Some(2)),
            (4, Some(5)),
            (5, Some(4)),
            (6, Some(6)),
            (7, Some(3)),
            (8, Some(7)),
        ],
        "bucket"
    );
    assert_eq!(
        ctas_partition_order(
            &ctx,
            &catalogs,
            "dayt",
            "days(d)",
            "id, d",
            "(1, DATE '2026-01-01'), (2, DATE '2026-01-02'), (3, DATE '2026-01-03'), \
             (4, DATE '2026-01-04'), (5, DATE '2026-01-05')",
        )
        .await,
        vec![
            (1, Some(0)),
            (2, Some(1)),
            (3, Some(2)),
            (4, Some(3)),
            (5, Some(4)),
        ],
        "days"
    );
}
