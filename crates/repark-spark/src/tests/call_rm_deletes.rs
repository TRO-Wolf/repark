use iceberg::spec::{ManifestContentType, Operation};

use super::super::*;
use super::common::*;

async fn replay(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    part: &str,
    version: &str,
    dml: &[&str],
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, cat STRING, v STRING) USING iceberg {part} \
             TBLPROPERTIES ('format-version' = '{version}', 'write.delete.mode' = 'merge-on-read')"
        ),
    )
    .await;
    for seed in [
        "INSERT INTO ice.sales.{table} VALUES (1, 'x', 'a'), (2, 'y', 'b')",
        "INSERT INTO ice.sales.{table} VALUES (3, 'x', 'c'), (4, 'y', 'd')",
        "INSERT INTO ice.sales.{table} VALUES (5, 'x', 'e'), (6, 'z', 'f')",
    ] {
        run(ctx, catalogs, &seed.replace("{table}", table)).await;
    }
    for stmt in dml {
        run(ctx, catalogs, &stmt.replace("{table}", table)).await;
    }
}

fn sales(table: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".into()), table.into())
}

async fn layout(catalogs: &CatalogRegistry, table: &str) -> Vec<(u8, i32, usize, usize)> {
    let catalog = &catalogs["ice"];
    let ident = sales(table);
    let loaded = catalog.load_table(&ident).await.expect("load table");
    let metadata = loaded.metadata();
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    let listed = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut shaped = Vec::new();
    for manifest in listed.entries() {
        let loaded_manifest = manifest
            .load_manifest(loaded.file_io())
            .await
            .expect("load manifest");
        let live = loaded_manifest
            .entries()
            .iter()
            .filter(|entry| entry.is_alive())
            .count();
        let content = match manifest.content {
            ManifestContentType::Data => 0,
            ManifestContentType::Deletes => 1,
        };
        let (data, deletes) = match manifest.content {
            ManifestContentType::Data => (live, 0),
            ManifestContentType::Deletes => (0, live),
        };
        shaped.push((content, manifest.partition_spec_id, data, deletes));
    }
    shaped.sort();
    shaped
}

async fn call_counts(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> (i64, i64) {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("rewrite_manifests CALL")
        .collect()
        .await
        .expect("collect result");
    let batch = &batches[0];
    let index = batch
        .schema()
        .index_of("rewritten_manifests_count")
        .expect("column");
    let rewritten = i64::from(
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32")
            .value(0),
    );
    let index = batch
        .schema()
        .index_of("added_manifests_count")
        .expect("column");
    let added = i64::from(
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32")
            .value(0),
    );
    (rewritten, added)
}

async fn live_ids(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> Vec<i32> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id FROM ice.sales.{table} ORDER BY id"),
    )
    .await
    .expect("select ids")
    .collect()
    .await
    .expect("collect ids");
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 ids");
        for index in 0..column.len() {
            ids.push(column.value(index));
        }
    }
    ids
}

async fn current_summary(
    catalogs: &CatalogRegistry,
    table: &str,
) -> (Operation, usize, usize, usize) {
    let loaded = catalogs["ice"]
        .load_table(&sales(table))
        .await
        .expect("load");
    let snapshot = loaded
        .metadata()
        .current_snapshot()
        .expect("current snapshot");
    let props = &snapshot.summary().additional_properties;
    let count = |key: &str| {
        props
            .get(key)
            .expect("summary key")
            .parse::<usize>()
            .expect("count")
    };
    (
        snapshot.summary().operation.clone(),
        count("manifests-created"),
        count("manifests-kept"),
        count("manifests-replaced"),
    )
}

async fn snapshot_total(catalogs: &CatalogRegistry, table: &str) -> usize {
    catalogs["ice"]
        .load_table(&sales(table))
        .await
        .expect("load")
        .metadata()
        .snapshots()
        .count()
}

const DELETE_1: &str = "DELETE FROM ice.sales.{table} WHERE id = 1";
const DELETE_4: &str = "DELETE FROM ice.sales.{table} WHERE id = 4";
const DELETE_5: &str = "DELETE FROM ice.sales.{table} WHERE id = 5";
const DELETE_7: &str = "DELETE FROM ice.sales.{table} WHERE id = 7";
const DELETE_8: &str = "DELETE FROM ice.sales.{table} WHERE id = 8";
const DELETE_9: &str = "DELETE FROM ice.sales.{table} WHERE id = 9";
const INSERT_4: &str = "INSERT INTO ice.sales.{table} VALUES (7, 'x', 'g'), (8, 'x', 'h'), (9, 'y', 'i'), (10, 'y', 'j')";
const INSERT_9: &str = "INSERT INTO ice.sales.{table} VALUES (9, 'x', 'q')";
const EVOLVE: &str = "ALTER TABLE ice.sales.{table} ADD PARTITION FIELD bucket(2, id)";
const PART: &str = "PARTITIONED BY (cat)";

async fn setup_version(wh: &TempDir, version: &str) -> (SessionContext, CatalogRegistry) {
    if version == "3" {
        setup_allow_create_format_version_3(wh).await
    } else {
        setup(wh).await
    }
}

#[tokio::test]
async fn rm_deletes_unpart_mor_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(&ctx, &catalogs, "rm_u2", "", "2", &[DELETE_1, DELETE_4]).await;
    assert_eq!(
        layout(&catalogs, "rm_u2").await,
        vec![
            (0, 0, 1, 0),
            (0, 0, 1, 0),
            (0, 0, 1, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_u2')"
        )
        .await,
        (5, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_u2").await,
        vec![(0, 0, 3, 0), (1, 0, 0, 2)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_u2").await, vec![2, 3, 5, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_u2").await,
        (Operation::Replace, 2, 0, 5)
    );
}

#[tokio::test]
async fn rm_deletes_unpart_mor_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(&ctx, &catalogs, "rm_u3", "", "3", &[DELETE_1, DELETE_4]).await;
    assert_eq!(
        layout(&catalogs, "rm_u3").await,
        vec![
            (0, 0, 1, 0),
            (0, 0, 1, 0),
            (0, 0, 1, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_u3')"
        )
        .await,
        (5, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_u3").await,
        vec![(0, 0, 3, 0), (1, 0, 0, 2)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_u3").await, vec![2, 3, 5, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_u3").await,
        (Operation::Replace, 2, 0, 5)
    );
}

#[tokio::test]
async fn rm_deletes_no_deletes_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(&ctx, &catalogs, "rm_n2", PART, "2", &[]).await;
    assert_eq!(
        layout(&catalogs, "rm_n2").await,
        vec![(0, 0, 2, 0), (0, 0, 2, 0), (0, 0, 2, 0)]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_n2')"
        )
        .await,
        (3, 1)
    );
    assert_eq!(layout(&catalogs, "rm_n2").await, vec![(0, 0, 6, 0)]);
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_n2").await,
        vec![1, 2, 3, 4, 5, 6]
    );
    assert_eq!(
        current_summary(&catalogs, "rm_n2").await,
        (Operation::Replace, 1, 0, 3)
    );
}

#[tokio::test]
async fn rm_deletes_no_deletes_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(&ctx, &catalogs, "rm_n3", PART, "3", &[]).await;
    assert_eq!(
        layout(&catalogs, "rm_n3").await,
        vec![(0, 0, 2, 0), (0, 0, 2, 0), (0, 0, 2, 0)]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_n3')"
        )
        .await,
        (3, 1)
    );
    assert_eq!(layout(&catalogs, "rm_n3").await, vec![(0, 0, 6, 0)]);
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_n3").await,
        vec![1, 2, 3, 4, 5, 6]
    );
    assert_eq!(
        current_summary(&catalogs, "rm_n3").await,
        (Operation::Replace, 1, 0, 3)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(
        &ctx,
        &catalogs,
        "rm_p2",
        PART,
        "2",
        &[DELETE_1, DELETE_4, DELETE_5],
    )
    .await;
    assert_eq!(
        layout(&catalogs, "rm_p2").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_p2')"
        )
        .await,
        (6, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_p2").await,
        vec![(0, 0, 6, 0), (1, 0, 0, 3)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_p2").await, vec![2, 3, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_p2").await,
        (Operation::Replace, 2, 0, 6)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(
        &ctx,
        &catalogs,
        "rm_p3",
        PART,
        "3",
        &[DELETE_1, DELETE_4, DELETE_5],
    )
    .await;
    assert_eq!(
        layout(&catalogs, "rm_p3").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_p3')"
        )
        .await,
        (6, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_p3").await,
        vec![(0, 0, 6, 0), (1, 0, 0, 3)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_p3").await, vec![2, 3, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_p3").await,
        (Operation::Replace, 2, 0, 6)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_spec_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(&ctx, &catalogs, "rm_s2", PART, "2", &[DELETE_1, DELETE_4]).await;
    assert_eq!(
        layout(&catalogs, "rm_s2").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_s2', spec_id => 0)"
        )
        .await,
        (5, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_s2").await,
        vec![(0, 0, 6, 0), (1, 0, 0, 2)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_s2").await, vec![2, 3, 5, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_s2").await,
        (Operation::Replace, 2, 0, 5)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_spec_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(&ctx, &catalogs, "rm_s3", PART, "3", &[DELETE_1, DELETE_4]).await;
    assert_eq!(
        layout(&catalogs, "rm_s3").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_s3', spec_id => 0)"
        )
        .await,
        (5, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_s3").await,
        vec![(0, 0, 6, 0), (1, 0, 0, 2)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_s3").await, vec![2, 3, 5, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_s3").await,
        (Operation::Replace, 2, 0, 5)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_nocache_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(&ctx, &catalogs, "rm_c2", PART, "2", &[DELETE_1, DELETE_4]).await;
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_c2', use_caching => false)"
        )
        .await,
        (5, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_c2").await,
        vec![(0, 0, 6, 0), (1, 0, 0, 2)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_c2").await, vec![2, 3, 5, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_c2").await,
        (Operation::Replace, 2, 0, 5)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_nocache_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(&ctx, &catalogs, "rm_c3", PART, "3", &[DELETE_1, DELETE_4]).await;
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_c3', use_caching => false)"
        )
        .await,
        (5, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_c3").await,
        vec![(0, 0, 6, 0), (1, 0, 0, 2)]
    );
    assert_eq!(live_ids(&ctx, &catalogs, "rm_c3").await, vec![2, 3, 5, 6]);
    assert_eq!(
        current_summary(&catalogs, "rm_c3").await,
        (Operation::Replace, 2, 0, 5)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_real_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(
        &ctx,
        &catalogs,
        "rm_r2",
        PART,
        "2",
        &[INSERT_4, DELETE_7, DELETE_9, DELETE_8],
    )
    .await;
    assert_eq!(
        layout(&catalogs, "rm_r2").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_r2')"
        )
        .await,
        (7, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_r2").await,
        vec![(0, 0, 8, 0), (1, 0, 0, 3)]
    );
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_r2").await,
        vec![1, 2, 3, 4, 5, 6, 10]
    );
    assert_eq!(
        current_summary(&catalogs, "rm_r2").await,
        (Operation::Replace, 2, 0, 7)
    );
}

#[tokio::test]
async fn rm_deletes_part_mor_real_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(
        &ctx,
        &catalogs,
        "rm_r3",
        PART,
        "3",
        &[INSERT_4, DELETE_7, DELETE_9, DELETE_8],
    )
    .await;
    assert_eq!(
        layout(&catalogs, "rm_r3").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (1, 0, 0, 0),
            (1, 0, 0, 1),
            (1, 0, 0, 1)
        ]
    );
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_r3')"
        )
        .await,
        (7, 2)
    );
    assert_eq!(
        layout(&catalogs, "rm_r3").await,
        vec![(0, 0, 8, 0), (1, 0, 0, 2)]
    );
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_r3").await,
        vec![1, 2, 3, 4, 5, 6, 10]
    );
    assert_eq!(
        current_summary(&catalogs, "rm_r3").await,
        (Operation::Replace, 2, 0, 7)
    );
}

#[tokio::test]
async fn rm_deletes_evolved_spec_v2() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(
        &ctx,
        &catalogs,
        "rm_e2",
        PART,
        "2",
        &[DELETE_1, EVOLVE, INSERT_9, DELETE_9],
    )
    .await;
    assert_eq!(
        layout(&catalogs, "rm_e2").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 1, 1, 0),
            (1, 0, 0, 1),
            (1, 1, 0, 1)
        ]
    );
    let snapshots_before = snapshot_total(&catalogs, "rm_e2").await;
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_e2')"
        )
        .await,
        (0, 0)
    );
    assert_eq!(snapshot_total(&catalogs, "rm_e2").await, snapshots_before);
    assert_eq!(
        layout(&catalogs, "rm_e2").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 1, 1, 0),
            (1, 0, 0, 1),
            (1, 1, 0, 1)
        ]
    );
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_e2").await,
        vec![2, 3, 4, 5, 6]
    );
}

#[tokio::test]
async fn rm_deletes_evolved_spec_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "3").await;
    replay(
        &ctx,
        &catalogs,
        "rm_e3",
        PART,
        "3",
        &[DELETE_1, EVOLVE, INSERT_9, DELETE_9],
    )
    .await;
    assert_eq!(
        layout(&catalogs, "rm_e3").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 1, 1, 0),
            (1, 0, 0, 1),
            (1, 1, 0, 1)
        ]
    );
    let snapshots_before = snapshot_total(&catalogs, "rm_e3").await;
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_e3')"
        )
        .await,
        (0, 0)
    );
    assert_eq!(snapshot_total(&catalogs, "rm_e3").await, snapshots_before);
    assert_eq!(
        layout(&catalogs, "rm_e3").await,
        vec![
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 0, 2, 0),
            (0, 1, 1, 0),
            (1, 0, 0, 1),
            (1, 1, 0, 1)
        ]
    );
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_e3").await,
        vec![2, 3, 4, 5, 6]
    );
}

#[tokio::test]
async fn rm_deletes_non_current_spec_rewrites_that_spec() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(
        &ctx,
        &catalogs,
        "rm_o2",
        PART,
        "2",
        &[DELETE_1, EVOLVE, INSERT_9, DELETE_9],
    )
    .await;
    assert_eq!(
        call_counts(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.rm_o2', spec_id => 0)"
        )
        .await,
        (3, 1)
    );
    assert_eq!(
        layout(&catalogs, "rm_o2").await,
        vec![(0, 0, 6, 0), (0, 1, 1, 0), (1, 0, 0, 1), (1, 1, 0, 1)]
    );
    assert_eq!(
        live_ids(&ctx, &catalogs, "rm_o2").await,
        vec![2, 3, 4, 5, 6]
    );
    assert_eq!(
        current_summary(&catalogs, "rm_o2").await,
        (Operation::Replace, 1, 3, 3)
    );
}

#[tokio::test]
async fn rm_deletes_unknown_spec_id_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_version(&wh, "2").await;
    replay(&ctx, &catalogs, "rm_x2", PART, "2", &[DELETE_1, DELETE_4]).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.rm_x2', spec_id => 99)",
    )
    .await
    .expect_err("an unknown spec id must refuse");
    assert!(
        error.to_string().contains("Invalid spec id 99"),
        "the refusal must name the unknown spec, got: {error}"
    );
}
