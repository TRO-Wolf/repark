use std::collections::HashSet;

use super::super::*;
use super::common::*;

use super::v3_upgrade::{lineage, merge_delete_sql, seed_mor_four, upgrade, walk_puffin};

async fn live_delete_files(
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(String, u64, Option<String>)> {
    use iceberg::spec::ManifestContentType;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = loaded.metadata();
    let mut files = Vec::new();
    let Some(snapshot) = metadata.current_snapshot() else {
        return files;
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .unwrap();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(loaded.file_io()).await.unwrap();
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            files.push((
                format!("{:?}", file.file_format()),
                file.record_count(),
                file.referenced_data_file()
                    .map(|path| path.rsplit('/').next().unwrap_or_default().to_string()),
            ));
        }
    }
    files.sort();
    files
}

async fn seed_delete_source(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id INT) USING iceberg"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (3)"),
    )
    .await;
}

async fn seed_upgraded_legacy(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> String {
    seed_mor_four(ctx, catalogs, table, "merge-on-read").await;
    run(ctx, catalogs, &merge_delete_sql(table, 2)).await;
    assert_eq!(
        live_delete_files(catalogs, table).await,
        vec![("Parquet".to_string(), 1, None)],
        "the v2 arm leaves ONE file-scoped parquet position delete"
    );
    upgrade(ctx, catalogs, table).await;
    let data_files = live_data_file_paths(catalogs, table).await;
    assert_eq!(data_files.len(), 1, "the seed is one data file");
    data_files
        .into_iter()
        .next()
        .map(|path| path.rsplit('/').next().unwrap_or_default().to_string())
        .expect("one data file")
}

#[tokio::test]
async fn merge_on_read_delete_merges_a_legacy_parquet_position_delete_into_the_dv() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    let data_file = seed_upgraded_legacy(&ctx, &catalogs, "legacy").await;

    run(&ctx, &catalogs, &merge_delete_sql("legacy", 3)).await;

    let after = load_sales_table(&catalogs, "legacy").await;
    assert_eq!(after.metadata().next_row_id(), 4);
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacy").await,
        vec![(1, "a".to_string()), (4, "d".to_string())]
    );
    assert_eq!(
        lineage(&ctx, &catalogs, "legacy").await,
        vec![(1, Some(0), Some(1)), (4, Some(3), Some(1))]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacy").await,
        vec![("Puffin".to_string(), 2, Some(data_file))],
        "Spark leaves ONE Puffin of record_count 2 and no parquet delete file"
    );
    let mut puffins = 0;
    walk_puffin(wh.path(), &mut puffins);
    assert_eq!(puffins, 1);
}

#[tokio::test]
async fn merge_on_read_delete_over_a_legacy_delete_then_appends_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_upgraded_legacy(&ctx, &catalogs, "legacyap").await;
    run(&ctx, &catalogs, &merge_delete_sql("legacyap", 3)).await;

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.legacyap VALUES (9, 'z')",
    )
    .await;

    let after = load_sales_table(&catalogs, "legacyap").await;
    assert_eq!(after.metadata().next_row_id(), 5);
    assert_eq!(
        lineage(&ctx, &catalogs, "legacyap").await,
        vec![
            (1, Some(0), Some(1)),
            (4, Some(3), Some(1)),
            (9, Some(4), Some(4))
        ]
    );
}

#[tokio::test]
async fn merge_on_read_update_over_a_legacy_parquet_position_delete_merges_into_the_dv() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    let data_file = seed_upgraded_legacy(&ctx, &catalogs, "legacyup").await;

    seed_delete_source(&ctx, &catalogs, "upsrc").await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.legacyup SET name = 'Z' WHERE id IN (SELECT id FROM ice.sales.upsrc)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacyup").await,
        vec![
            (1, "a".to_string()),
            (3, "Z".to_string()),
            (4, "d".to_string())
        ]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacyup").await,
        vec![("Puffin".to_string(), 2, Some(data_file))],
        "the UPDATE arm merges the legacy positions too"
    );
}

#[tokio::test]
async fn delete_where_over_a_legacy_parquet_position_delete_merges_into_the_dv() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    let data_file = seed_upgraded_legacy(&ctx, &catalogs, "legacydw").await;

    seed_delete_source(&ctx, &catalogs, "dwsrc").await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.legacydw WHERE id IN (SELECT id FROM ice.sales.dwsrc)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacydw").await,
        vec![(1, "a".to_string()), (4, "d".to_string())]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacydw").await,
        vec![("Puffin".to_string(), 2, Some(data_file))]
    );
}

#[tokio::test]
async fn two_legacy_parquet_deletes_on_one_data_file_both_merge_and_both_go() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_mor_four(&ctx, &catalogs, "legacy2", "merge-on-read").await;
    run(&ctx, &catalogs, &merge_delete_sql("legacy2", 1)).await;
    run(&ctx, &catalogs, &merge_delete_sql("legacy2", 2)).await;
    assert_eq!(
        live_delete_files(&catalogs, "legacy2").await,
        vec![
            ("Parquet".to_string(), 1, None),
            ("Parquet".to_string(), 1, None)
        ],
        "this engine's v2 arm leaves TWO live file-scoped deletes where Spark rewrites one"
    );
    let data_file = {
        upgrade(&ctx, &catalogs, "legacy2").await;
        live_data_file_paths(&catalogs, "legacy2")
            .await
            .into_iter()
            .next()
            .map(|path| path.rsplit('/').next().unwrap_or_default().to_string())
            .expect("one data file")
    };

    run(&ctx, &catalogs, &merge_delete_sql("legacy2", 3)).await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacy2").await,
        vec![(4, "d".to_string())]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacy2").await,
        vec![("Puffin".to_string(), 3, Some(data_file))],
        "both superseded parquet deletes go in the same RowDelta"
    );
}

#[tokio::test]
async fn a_sibling_data_files_legacy_delete_stays_live_when_it_is_not_touched() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_mor_four(&ctx, &catalogs, "legacysib", "merge-on-read").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.legacysib VALUES (5, 'e'), (6, 'f')",
    )
    .await;
    run(&ctx, &catalogs, &merge_delete_sql("legacysib", 2)).await;
    run(&ctx, &catalogs, &merge_delete_sql("legacysib", 5)).await;
    upgrade(&ctx, &catalogs, "legacysib").await;

    run(&ctx, &catalogs, &merge_delete_sql("legacysib", 3)).await;

    let files = live_delete_files(&catalogs, "legacysib").await;
    assert_eq!(
        files
            .iter()
            .filter(|(format, _, _)| format == "Parquet")
            .count(),
        1,
        "the untouched data file keeps its own legacy delete live: {files:?}"
    );
    assert_eq!(
        files
            .iter()
            .filter(|(format, _, _)| format == "Puffin")
            .count(),
        1,
        "only the touched data file gets a DV: {files:?}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacysib").await,
        vec![
            (1, "a".to_string()),
            (4, "d".to_string()),
            (6, "f".to_string())
        ],
        "the sibling's legacy delete keeps id 5 deleted"
    );
}

#[tokio::test]
async fn a_plain_where_merge_on_read_delete_over_a_legacy_delete_merges_into_the_dv() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    let data_file = seed_upgraded_legacy(&ctx, &catalogs, "legacyplain").await;

    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.legacyplain WHERE id = 3",
    )
    .await;

    let after = load_sales_table(&catalogs, "legacyplain").await;
    assert_eq!(after.metadata().next_row_id(), 4);
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacyplain").await,
        vec![(1, "a".to_string()), (4, "d".to_string())]
    );
    assert_eq!(
        lineage(&ctx, &catalogs, "legacyplain").await,
        vec![(1, Some(0), Some(1)), (4, Some(3), Some(1))]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacyplain").await,
        vec![("Puffin".to_string(), 2, Some(data_file))],
        "the plain-WHERE arm now merges in the fork's own delete exec: ONE Puffin of record_count \
         2 and no parquet delete file"
    );
    let mut puffins = 0;
    walk_puffin(wh.path(), &mut puffins);
    assert_eq!(puffins, 1);
}

#[tokio::test]
async fn a_plain_where_merge_on_read_update_over_a_legacy_delete_merges_into_the_dv() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    let data_file = seed_upgraded_legacy(&ctx, &catalogs, "legacyplanup").await;

    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.legacyplanup SET name = 'Z' WHERE id = 3",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacyplanup").await,
        vec![
            (1, "a".to_string()),
            (3, "Z".to_string()),
            (4, "d".to_string())
        ]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacyplanup").await,
        vec![("Puffin".to_string(), 2, Some(data_file))],
        "the plain-WHERE UPDATE arm merges the legacy positions too"
    );
}

async fn seed_partition_scoped_legacy(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.legacypart (id INT, name STRING) USING iceberg TBLPROPERTIES \
         ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', \
          'write.delete.granularity' = 'partition')",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.legacypart VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.legacypart VALUES (3, 'c'), (4, 'd')",
    )
    .await;
    run(
        ctx,
        catalogs,
        "MERGE INTO ice.sales.legacypart AS t USING (SELECT 1 AS id UNION ALL SELECT 3) AS s \
         ON t.id = s.id WHEN MATCHED THEN DELETE",
    )
    .await;
    assert_eq!(
        live_delete_files(catalogs, "legacypart").await,
        vec![("Parquet".to_string(), 2, None)],
        "partition granularity leaves ONE delete file covering BOTH data files"
    );
    upgrade(ctx, catalogs, "legacypart").await;
}

#[tokio::test]
async fn a_partition_scoped_legacy_delete_merges_and_keeps_the_parquet_live() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_partition_scoped_legacy(&ctx, &catalogs).await;
    let data_files: HashSet<String> = live_data_file_paths(&catalogs, "legacypart")
        .await
        .into_iter()
        .map(|path| path.rsplit('/').next().unwrap_or_default().to_string())
        .collect();
    assert_eq!(data_files.len(), 2, "the seed is two data files");

    run(&ctx, &catalogs, &merge_delete_sql("legacypart", 2)).await;

    let after = load_sales_table(&catalogs, "legacypart").await;
    assert_eq!(after.metadata().next_row_id(), 4);
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacypart").await,
        vec![(4, "d".to_string())]
    );
    let mut files = live_delete_files(&catalogs, "legacypart").await;
    assert_eq!(
        files
            .iter()
            .filter(|(format, count, referenced)| format == "Parquet"
                && *count == 2
                && referenced.is_none())
            .count(),
        1,
        "Spark leaves the partition-scoped parquet delete LIVE: {files:?}"
    );
    let first_dv: Vec<&(String, u64, Option<String>)> = files
        .iter()
        .filter(|(format, _, _)| format == "Puffin")
        .collect();
    assert_eq!(
        first_dv.len(),
        1,
        "only the touched file gets a DV: {files:?}"
    );
    assert_eq!(first_dv[0].1, 2, "the DV unions the legacy position");
    let first = first_dv[0]
        .2
        .clone()
        .expect("the DV references a data file");
    assert!(data_files.contains(&first));

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.legacypart VALUES (9, 'z')",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacypart").await,
        vec![(4, "d".to_string()), (9, "z".to_string())]
    );

    run(&ctx, &catalogs, &merge_delete_sql("legacypart", 4)).await;

    files = live_delete_files(&catalogs, "legacypart").await;
    assert_eq!(
        files
            .iter()
            .filter(|(format, _, _)| format == "Parquet")
            .count(),
        1,
        "the parquet delete is STILL live once every data file it covers carries a DV: {files:?}"
    );
    let puffins: HashSet<String> = files
        .iter()
        .filter(|(format, count, _)| format == "Puffin" && *count == 2)
        .filter_map(|(_, _, referenced)| referenced.clone())
        .collect();
    assert_eq!(
        puffins, data_files,
        "the second touched data file gets its own DV of record_count 2: {files:?}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacypart").await,
        vec![(9, "z".to_string())]
    );
}

#[tokio::test]
async fn copy_on_write_over_a_legacy_parquet_position_delete_leaves_it_alone() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_upgraded_legacy(&ctx, &catalogs, "legacycow").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.legacycow SET TBLPROPERTIES ('write.merge.mode' = 'copy-on-write')",
    )
    .await;

    run(&ctx, &catalogs, &merge_delete_sql("legacycow", 3)).await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.legacycow").await,
        vec![(1, "a".to_string()), (4, "d".to_string())]
    );
    assert_eq!(
        live_delete_files(&catalogs, "legacycow").await,
        vec![("Parquet".to_string(), 1, None)],
        "copy-on-write never writes a DV, so the legacy delete is left exactly as it was"
    );
    let mut puffins = 0;
    walk_puffin(wh.path(), &mut puffins);
    assert_eq!(puffins, 0);
}

async fn branch_delete_files(
    catalogs: &CatalogRegistry,
    table: &str,
    branch: &str,
) -> Vec<(String, u64, Option<String>)> {
    use iceberg::spec::ManifestContentType;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = loaded.metadata();
    let mut files = Vec::new();
    let Some(snapshot) = metadata.snapshot_for_ref(branch) else {
        return files;
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .unwrap();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(loaded.file_io()).await.unwrap();
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            files.push((
                format!("{:?}", file.file_format()),
                file.record_count(),
                file.referenced_data_file()
                    .map(|path| path.rsplit('/').next().unwrap_or_default().to_string()),
            ));
        }
    }
    files.sort();
    files
}

fn branch_merge_delete_sql(table: &str, branch: &str, id: i32) -> String {
    format!(
        "MERGE INTO ice.sales.{table}.branch_{branch} AS t USING (SELECT {id} AS id) AS s \
         ON t.id = s.id WHEN MATCHED THEN DELETE"
    )
}

#[tokio::test]
async fn a_second_merge_on_read_delete_on_a_diverged_branch_merges_the_branch_only_dv() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_mor_four(&ctx, &catalogs, "brdv", "merge-on-read").await;
    upgrade(&ctx, &catalogs, "brdv").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.brdv CREATE BRANCH b",
    )
    .await;
    let main_before = load_sales_table(&catalogs, "brdv")
        .await
        .metadata()
        .current_snapshot_id();

    run(&ctx, &catalogs, &branch_merge_delete_sql("brdv", "b", 2)).await;
    assert_eq!(
        branch_delete_files(&catalogs, "brdv", "b").await.len(),
        1,
        "the first branch DELETE lands one DV on the branch and nothing on main"
    );
    assert_eq!(
        live_delete_files(&catalogs, "brdv").await,
        Vec::new(),
        "main carries no delete file at all, so a close against main sees no DV to merge"
    );

    run(&ctx, &catalogs, &branch_merge_delete_sql("brdv", "b", 3)).await;

    let after = load_sales_table(&catalogs, "brdv").await;
    assert_eq!(
        after.metadata().current_snapshot_id(),
        main_before,
        "main never moves"
    );
    let branch_files = branch_delete_files(&catalogs, "brdv", "b").await;
    assert_eq!(
        branch_files.len(),
        1,
        "the second branch DELETE must MERGE into the branch's own DV, not add a second: \
         {branch_files:?}"
    );
    assert_eq!(
        branch_files[0].1, 2,
        "the merged branch DV carries both positions: {branch_files:?}"
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.brdv.branch_b").await,
        vec![1, 4]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.brdv").await,
        vec![1, 2, 3, 4],
        "main still reads every row"
    );
}

#[tokio::test]
async fn a_legacy_parquet_delete_that_exists_only_on_a_branch_merges_on_that_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_mor_four(&ctx, &catalogs, "brlg", "merge-on-read").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.brlg CREATE BRANCH b",
    )
    .await;
    run(&ctx, &catalogs, &branch_merge_delete_sql("brlg", "b", 2)).await;
    assert_eq!(
        branch_delete_files(&catalogs, "brlg", "b").await,
        vec![("Parquet".to_string(), 1, None)],
        "the legacy parquet delete exists on the branch only"
    );
    assert_eq!(
        live_delete_files(&catalogs, "brlg").await,
        Vec::new(),
        "main carries none of it"
    );
    upgrade(&ctx, &catalogs, "brlg").await;

    run(&ctx, &catalogs, &branch_merge_delete_sql("brlg", "b", 3)).await;

    let branch_files = branch_delete_files(&catalogs, "brlg", "b").await;
    assert_eq!(
        branch_files.len(),
        1,
        "the branch's legacy delete leaves in the same RowDelta the DV arrives in: {branch_files:?}"
    );
    assert_eq!(branch_files[0].0, "Puffin");
    assert_eq!(branch_files[0].1, 2);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.brlg.branch_b").await,
        vec![1, 4]
    );
}

#[tokio::test]
#[ignore = "measurement: prints the legacy-walk cost at 8 and 48 delete manifests"]
async fn measure_legacy_walk_cost() {
    for manifests in [8usize, 48] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
        run(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.walk (id INT, name STRING, part INT) USING iceberg \
             PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3', \
             'write.delete.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read', \
             'commit.manifest-merge.enabled' = 'false')",
        )
        .await;
        for part in 0..manifests {
            run(
                &ctx,
                &catalogs,
                &format!(
                    "INSERT INTO ice.sales.walk VALUES ({}, 'a', {part}), ({}, 'b', {part})",
                    part * 2,
                    part * 2 + 1
                ),
            )
            .await;
        }
        for part in 0..manifests {
            run(
                &ctx,
                &catalogs,
                &format!(
                    "MERGE INTO ice.sales.walk AS t USING (SELECT {} AS id) AS s \
                     ON t.id = s.id WHEN MATCHED THEN DELETE",
                    part * 2
                ),
            )
            .await;
        }
        let delete_manifests = delete_manifest_count(&catalogs, "walk").await;
        let started = std::time::Instant::now();
        run(
            &ctx,
            &catalogs,
            "MERGE INTO ice.sales.walk AS t USING (SELECT 1 AS id) AS s \
             ON t.id = s.id WHEN MATCHED THEN DELETE",
        )
        .await;
        let elapsed = started.elapsed();
        println!(
            "MEASURE legacy-walk: {manifests} commits -> {delete_manifests} delete manifests, \
             one more MoR DELETE took {elapsed:?}"
        );
    }
}

#[tokio::test]
#[ignore = "measurement: prints the pure-DV close wall at 8, 48 and 192 data manifests"]
async fn measure_pure_dv_close_cost() {
    for manifests in [8usize, 48, 192] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
        run(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.puredv (id INT, name STRING, part INT) USING iceberg \
             PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3', \
             'write.delete.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read', \
             'commit.manifest-merge.enabled' = 'false')",
        )
        .await;
        for part in 0..manifests {
            run(
                &ctx,
                &catalogs,
                &format!("INSERT INTO ice.sales.puredv VALUES ({part}, 'a', {part})"),
            )
            .await;
        }
        let data_manifests = data_manifest_count(&catalogs, "puredv").await;
        let started = std::time::Instant::now();
        run(&ctx, &catalogs, "DELETE FROM ice.sales.puredv WHERE id = 0").await;
        let elapsed = started.elapsed();
        println!(
            "MEASURE pure-dv-close: {manifests} commits -> {data_manifests} data manifests, \
             one MoR DELETE took {elapsed:?}"
        );
    }
}

async fn data_manifest_count(catalogs: &CatalogRegistry, table: &str) -> usize {
    use iceberg::spec::ManifestContentType;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = loaded.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return 0;
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .unwrap();
    manifest_list
        .entries()
        .iter()
        .filter(|entry| entry.content == ManifestContentType::Data)
        .count()
}

async fn delete_manifest_count(catalogs: &CatalogRegistry, table: &str) -> usize {
    use iceberg::spec::ManifestContentType;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = loaded.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return 0;
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .unwrap();
    manifest_list
        .entries()
        .iter()
        .filter(|entry| entry.content == ManifestContentType::Deletes)
        .count()
}
