use super::super::*;
use super::common::*;
use super::v3_cow::{self, Lineage};

use iceberg::spec::FormatVersion;

const COW_V3: &str = "'format-version' = '3', \
     'write.delete.mode' = 'copy-on-write', \
     'write.update.mode' = 'copy-on-write', \
     'write.merge.mode' = 'copy-on-write'";

const MOR_V3: &str = "'format-version' = '3', \
     'write.delete.mode' = 'merge-on-read', \
     'write.update.mode' = 'merge-on-read'";

async fn create_v3(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, props: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ({props})"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} SELECT * FROM src"),
    )
    .await;
    let ident = TableIdent::from_strs(["sales", table]).expect("ident");
    let table_loaded = catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load");
    assert_eq!(table_loaded.metadata().format_version(), FormatVersion::V3);
}

#[tokio::test]
async fn created_v3_cow_update_keeps_row_id() {
    let _: &str = "pins: rp-6-fork-repin/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    create_v3(&ctx, &catalogs, "created_upd", COW_V3).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.created_upd SET name = 'x' WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.created_upd").await,
        vec![(1, "a".into()), (2, "x".into()), (3, "c".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "created_upd").await,
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "created_upd").await,
        Lineage {
            next_row_id: 6,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(3),
        }
    );
}

#[tokio::test]
async fn created_v3_mor_update_keeps_row_id() {
    let _: &str = "pins: rp-6-fork-repin/C-003";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    create_v3(&ctx, &catalogs, "created_mor", MOR_V3).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.created_mor SET name = 'x' WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.created_mor").await,
        vec![(1, "a".into()), (2, "x".into()), (3, "c".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "created_mor").await,
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "created_mor").await,
        Lineage {
            next_row_id: 4,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(1),
        }
    );
    assert_eq!(
        v3_cow::live_data_file_count(&catalogs, "created_mor").await,
        2
    );
    assert_eq!(
        v3_cow::live_delete_file_count(&catalogs, "created_mor").await,
        1
    );
    assert_eq!(
        v3_cow::live_manifest_count(&catalogs, "created_mor").await,
        3
    );
}

#[tokio::test]
async fn adopted_v3_cow_update_then_delete_keeps_survivor_row_ids() {
    let _: &str = "pins: rp-6-fork-repin/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_ud", "adopt_ud").await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.adopt_ud SET name = 'x' WHERE id = 2",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_ud WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_ud").await,
        vec![(1, "a".into()), (3, "c".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_ud").await,
        vec![(1, 0, 1), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_ud").await,
        Lineage {
            next_row_id: 8,
            snapshot_first_row_id: Some(6),
            snapshot_added_rows: Some(2),
        }
    );
}

#[tokio::test]
async fn adopted_v3_cow_delete_then_update_keeps_row_id() {
    let _: &str = "pins: rp-6-fork-repin/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_du", "adopt_du").await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_du WHERE id = 2",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.adopt_du SET name = 'x' WHERE id = 3",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_du").await,
        vec![(1, "a".into()), (3, "x".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_du").await,
        vec![(1, 0, 1), (3, 2, 3)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_du").await,
        Lineage {
            next_row_id: 7,
            snapshot_first_row_id: Some(5),
            snapshot_added_rows: Some(2),
        }
    );
}

#[tokio::test]
async fn adopted_v3_cow_overwrite_then_delete_matches_spark_new_row_ids() {
    let _: &str = "pins: rp-6-fork-repin/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_ow", "adopt_ow").await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.adopt_ow VALUES (1, 'a'), (3, 'c')",
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_ow").await,
        vec![(1, 3, 2), (3, 4, 2)]
    );
    assert_eq!(v3_cow::lineage(&catalogs, "adopt_ow").await.next_row_id, 5);
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_ow WHERE id = 3",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_ow").await,
        vec![(1, "a".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_ow").await,
        vec![(1, 3, 2)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_ow").await,
        Lineage {
            next_row_id: 6,
            snapshot_first_row_id: Some(5),
            snapshot_added_rows: Some(1),
        }
    );
}

#[tokio::test]
async fn adopted_v3_cow_delete_each_position_keeps_other_row_ids() {
    let _: &str = "pins: rp-6-fork-repin/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    for (table, deleted, expected) in [
        ("pos1", 1, vec![(2, 1, 1), (3, 2, 1)]),
        ("pos3", 3, vec![(1, 0, 1), (2, 1, 1)]),
    ] {
        v3_cow::adopt_cow_v3(&ctx, &catalogs, &format!("seed_{table}"), table).await;
        run(
            &ctx,
            &catalogs,
            &format!("DELETE FROM ice.sales.{table} WHERE id = {deleted}"),
        )
        .await;
        assert_eq!(
            v3_cow::lineage_triples(&ctx, &catalogs, table).await,
            expected
        );
        assert_eq!(v3_cow::lineage(&catalogs, table).await.next_row_id, 5);
    }
}

#[tokio::test]
async fn adopted_v3_mor_update_then_delete_keeps_survivor_row_ids() {
    let _: &str = "pins: rp-6-fork-repin/C-003";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_v3(&ctx, &catalogs, "seed_mud", "adopt_mud", MOR_V3).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.adopt_mud SET name = 'x' WHERE id = 2",
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_mud").await,
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    );
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_mud WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_mud").await,
        vec![(1, "a".into()), (3, "c".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_mud").await,
        vec![(1, 0, 1), (3, 2, 1)]
    );
}

fn merge_sql(table: &str, body: &str) -> String {
    format!("MERGE INTO ice.sales.{table} AS t USING {body}")
}

#[tokio::test]
async fn created_v3_cow_merge_matched_update_keeps_row_id() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    create_v3(&ctx, &catalogs, "created_mrg", COW_V3).await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "created_mrg",
            "(SELECT 2 AS id, 'm' AS name) AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "created_mrg").await,
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "created_mrg").await,
        Lineage {
            next_row_id: 6,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(3),
        }
    );
}

#[tokio::test]
async fn created_v3_mor_merge_matched_update_keeps_row_id() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    create_v3(
        &ctx,
        &catalogs,
        "created_mmrg",
        "'format-version' = '3', 'write.merge.mode' = 'merge-on-read'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "created_mmrg",
            "(SELECT 2 AS id, 'm' AS name) AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "created_mmrg").await,
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "created_mmrg").await,
        Lineage {
            next_row_id: 4,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(1),
        }
    );
    assert_eq!(
        v3_cow::live_data_file_count(&catalogs, "created_mmrg").await,
        2
    );
    assert_eq!(
        v3_cow::live_delete_file_count(&catalogs, "created_mmrg").await,
        1
    );
}

#[tokio::test]
async fn adopted_v3_cow_merge_matched_delete_keeps_survivor_row_id() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_md", "adopt_md").await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "adopt_md",
            "(SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
        ),
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_md").await,
        vec![(1, "a".into()), (3, "c".into())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_md").await,
        vec![(1, 0, 1), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_md").await,
        Lineage {
            next_row_id: 5,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(2),
        }
    );
}

#[tokio::test]
async fn adopted_v3_cow_merge_not_matched_insert_assigns_new_row_id() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_ins", "adopt_ins").await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "adopt_ins",
            "(SELECT 4 AS id, 'd' AS name) AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_ins").await,
        vec![(1, 0, 1), (2, 1, 1), (3, 2, 1), (4, 3, 2)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_ins").await,
        Lineage {
            next_row_id: 4,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(1),
        }
    );
    assert_eq!(
        v3_cow::live_data_file_count(&catalogs, "adopt_ins").await,
        2
    );
}

#[tokio::test]
async fn adopted_v3_cow_merge_nmbs_delete_keeps_survivor_row_id() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_nmbs", "adopt_nmbs").await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "adopt_nmbs",
            "(SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name) AS s \
             ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_nmbs").await,
        vec![(1, 0, 1), (2, 1, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_nmbs").await,
        Lineage {
            next_row_id: 5,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(2),
        }
    );
}

#[tokio::test]
async fn adopted_v3_cow_merge_mixed_keeps_updated_row_id_and_assigns_insert() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, "seed_mix", "adopt_mix").await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "adopt_mix",
            "(SELECT 2 AS id, 'm' AS name, 'upd' AS op UNION ALL \
             SELECT 3 AS id, 'x' AS name, 'del' AS op UNION ALL \
             SELECT 4 AS id, 'd' AS name, 'ins' AS op) AS s ON t.id = s.id \
             WHEN MATCHED AND s.op = 'del' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET t.name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_mix").await,
        vec![(2, 1, 2), (4, 4, 2)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_mix").await,
        Lineage {
            next_row_id: 5,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(2),
        }
    );
}

#[tokio::test]
async fn adopted_v3_mor_merge_matched_delete_keeps_survivor_row_id() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_v3(
        &ctx,
        &catalogs,
        "seed_mmd",
        "adopt_mmd",
        "'format-version' = '3', 'write.merge.mode' = 'merge-on-read'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "adopt_mmd",
            "(SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_mmd").await,
        vec![(1, 0, 1), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_mmd").await,
        Lineage {
            next_row_id: 3,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(0),
        }
    );
    assert_eq!(
        v3_cow::live_delete_file_count(&catalogs, "adopt_mmd").await,
        1
    );
}

#[tokio::test]
async fn adopted_v3_mor_merge_mixed_keeps_updated_row_id_and_assigns_insert() {
    let _: &str = "pins: v3-7-merge-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    v3_cow::adopt_v3(
        &ctx,
        &catalogs,
        "seed_mmix",
        "adopt_mmix",
        "'format-version' = '3', 'write.merge.mode' = 'merge-on-read'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &merge_sql(
            "adopt_mmix",
            "(SELECT 2 AS id, 'm' AS name, 'upd' AS op UNION ALL \
             SELECT 3 AS id, 'x' AS name, 'del' AS op UNION ALL \
             SELECT 4 AS id, 'd' AS name, 'ins' AS op) AS s ON t.id = s.id \
             WHEN MATCHED AND s.op = 'del' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET t.name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ),
    )
    .await;
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "adopt_mmix").await,
        vec![(2, 1, 2), (4, 4, 2)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "adopt_mmix").await,
        Lineage {
            next_row_id: 5,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(2),
        }
    );
    assert_eq!(
        v3_cow::live_delete_file_count(&catalogs, "adopt_mmix").await,
        1
    );
}
