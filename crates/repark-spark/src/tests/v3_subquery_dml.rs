use super::super::*;
use super::common::*;
use super::v3_cow::{self, Lineage};

use iceberg::spec::FormatVersion;

const COW_V3: &str = "'format-version' = '3', \
     'write.delete.mode' = 'copy-on-write', \
     'write.update.mode' = 'copy-on-write', \
     'write.merge.mode' = 'copy-on-write'";

const DELETE_IN: &str = "DELETE FROM ice.sales.{t} WHERE id IN (SELECT id FROM ice.sales.srcids)";
const DELETE_NOT_IN: &str =
    "DELETE FROM ice.sales.{t} WHERE id NOT IN (SELECT id FROM ice.sales.srcids)";
const DELETE_EXISTS: &str = "DELETE FROM ice.sales.{t} AS tgt WHERE EXISTS \
     (SELECT 1 FROM ice.sales.srcids AS s WHERE s.id = tgt.id)";
const DELETE_NOT_EXISTS: &str = "DELETE FROM ice.sales.{t} AS tgt WHERE NOT EXISTS \
     (SELECT 1 FROM ice.sales.srcids AS s WHERE s.id = tgt.id)";
const UPDATE_IN: &str =
    "UPDATE ice.sales.{t} SET name = 'm' WHERE id IN (SELECT id FROM ice.sales.srcids)";
const DELETE_CORRELATED_IN: &str = "DELETE FROM ice.sales.{t} AS tgt WHERE tgt.id IN \
     (SELECT s.id FROM ice.sales.srcids AS s WHERE s.id = tgt.id)";
const DELETE_CORRELATED_IN_OFFSET: &str = "DELETE FROM ice.sales.{t} AS tgt WHERE tgt.id IN \
     (SELECT s.id FROM ice.sales.srcids AS s WHERE s.id = tgt.id + 1)";

const MOR_V3_DELETE: &str = "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'";
const MOR_V3_UPDATE: &str = "'format-version' = '3', 'write.update.mode' = 'merge-on-read'";

async fn seed_source(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.srcids (id INT) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(ctx, catalogs, "INSERT INTO ice.sales.srcids VALUES (2)").await;
}

async fn created_v3(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, props: &str) {
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
    let loaded = catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load");
    assert_eq!(loaded.metadata().format_version(), FormatVersion::V3);
}

async fn snapshot_id(catalogs: &CatalogRegistry, table: &str) -> Option<i64> {
    let ident = TableIdent::from_strs(["sales", table]).expect("ident");
    catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load")
        .metadata()
        .current_snapshot_id()
}

fn seed_rows() -> Vec<(i32, String)> {
    SEED_ROWS
        .iter()
        .map(|(id, name)| (*id, (*name).to_string()))
        .collect()
}

async fn created_cow_v3(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ({COW_V3})"
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
    let loaded = catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load");
    assert_eq!(loaded.metadata().format_version(), FormatVersion::V3);
}

async fn data_file_count(catalogs: &CatalogRegistry, table: &str) -> usize {
    let ident = TableIdent::from_strs(["sales", table]).expect("ident");
    let loaded = catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load");
    let Some(snapshot) = loaded.metadata().current_snapshot() else {
        return 0;
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), loaded.metadata())
        .await
        .expect("manifest list");
    let mut files = 0;
    for entry in manifest_list.entries() {
        if entry.content != iceberg::spec::ManifestContentType::Data {
            continue;
        }
        let manifest = entry
            .load_manifest(loaded.file_io())
            .await
            .expect("manifest");
        files += manifest
            .entries()
            .iter()
            .filter(|entry| entry.is_alive())
            .count();
    }
    files
}

const F_V3_8_UPDATE_FILES: usize = 2;

const SEED_ROWS: [(i32, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
const SEED_TRIPLES: [(i32, i64, i64); 3] = [(1, 0, 1), (2, 1, 1), (3, 2, 1)];

struct Cell {
    rows: Vec<(i32, String)>,
    triples: Vec<(i32, i64, i64)>,
    lineage: Lineage,
    data_files: usize,
}

fn delete_hit_cell() -> Cell {
    Cell {
        rows: vec![(1, "a".into()), (3, "c".into())],
        triples: vec![(1, 0, 1), (3, 2, 1)],
        lineage: Lineage {
            next_row_id: 5,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(2),
        },
        data_files: 1,
    }
}

fn delete_miss_cell() -> Cell {
    Cell {
        rows: vec![(2, "b".into())],
        triples: vec![(2, 1, 1)],
        lineage: Lineage {
            next_row_id: 4,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(1),
        },
        data_files: 1,
    }
}

fn update_cell() -> Cell {
    Cell {
        rows: vec![(1, "a".into()), (2, "m".into()), (3, "c".into())],
        triples: vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)],
        lineage: Lineage {
            next_row_id: 6,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(3),
        },
        data_files: F_V3_8_UPDATE_FILES,
    }
}

async fn assert_created(table: &str, statement: &str, expected: Cell) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    created_cow_v3(&ctx, &catalogs, table).await;
    run(&ctx, &catalogs, &statement.replace("{t}", table)).await;
    assert_cell(&ctx, &catalogs, table, expected).await;
}

async fn assert_adopted(seed: &str, table: &str, statement: &str, expected: Cell) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    v3_cow::adopt_cow_v3(&ctx, &catalogs, seed, table).await;
    run(&ctx, &catalogs, &statement.replace("{t}", table)).await;
    assert_cell(&ctx, &catalogs, table, expected).await;
}

async fn assert_cell(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    expected: Cell,
) {
    assert_eq!(
        table_rows(ctx, catalogs, &format!("ice.sales.{table}")).await,
        expected.rows
    );
    assert_eq!(
        v3_cow::lineage_triples(ctx, catalogs, table).await,
        expected.triples
    );
    assert_eq!(v3_cow::lineage(catalogs, table).await, expected.lineage);
    assert_eq!(data_file_count(catalogs, table).await, expected.data_files);
}

#[tokio::test]
async fn created_v3_cow_subquery_in_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_created("sub_in", DELETE_IN, delete_hit_cell()).await;
}

#[tokio::test]
async fn created_v3_cow_subquery_not_in_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_created("sub_notin", DELETE_NOT_IN, delete_miss_cell()).await;
}

#[tokio::test]
async fn created_v3_cow_subquery_exists_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_created("sub_ex", DELETE_EXISTS, delete_hit_cell()).await;
}

#[tokio::test]
async fn created_v3_cow_subquery_not_exists_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_created("sub_nex", DELETE_NOT_EXISTS, delete_miss_cell()).await;
}

#[tokio::test]
async fn created_v3_cow_subquery_in_update_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_created("sub_upd", UPDATE_IN, update_cell()).await;
}

#[tokio::test]
async fn adopted_v3_cow_subquery_in_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_adopted("seed_ain", "adopt_ain", DELETE_IN, delete_hit_cell()).await;
}

#[tokio::test]
async fn adopted_v3_cow_subquery_not_in_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_adopted(
        "seed_anotin",
        "adopt_anotin",
        DELETE_NOT_IN,
        delete_miss_cell(),
    )
    .await;
}

#[tokio::test]
async fn adopted_v3_cow_subquery_exists_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_adopted("seed_aex", "adopt_aex", DELETE_EXISTS, delete_hit_cell()).await;
}

#[tokio::test]
async fn adopted_v3_cow_subquery_not_exists_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_adopted(
        "seed_anex",
        "adopt_anex",
        DELETE_NOT_EXISTS,
        delete_miss_cell(),
    )
    .await;
}

#[tokio::test]
async fn adopted_v3_cow_subquery_in_update_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_adopted("seed_aupd", "adopt_aupd", UPDATE_IN, update_cell()).await;
}

#[tokio::test]
async fn created_v3_cow_correlated_subquery_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_created("sub_correlated", DELETE_CORRELATED_IN, delete_hit_cell()).await;
}

#[tokio::test]
async fn adopted_v3_cow_correlated_subquery_delete_keeps_row_lineage() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    assert_adopted(
        "seed_acorrelated",
        "adopt_acorrelated",
        DELETE_CORRELATED_IN,
        delete_hit_cell(),
    )
    .await;
}

#[tokio::test]
async fn created_v3_cow_correlated_subquery_delete_matching_nothing_leaves_the_table_unmoved() {
    let _: &str = "pins: v3-8-subquery-where-lineage/C-002";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    created_cow_v3(&ctx, &catalogs, "sub_zero").await;
    let before = snapshot_id(&catalogs, "sub_zero").await;
    run(
        &ctx,
        &catalogs,
        &DELETE_CORRELATED_IN_OFFSET.replace("{t}", "sub_zero"),
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.sub_zero").await,
        seed_rows(),
        "`s.id = tgt.id + 1` matches no target row"
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "sub_zero").await,
        SEED_TRIPLES.to_vec()
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "sub_zero").await.next_row_id,
        3,
        "next-row-id stays at the seed value, as Spark's does"
    );
    assert_eq!(data_file_count(&catalogs, "sub_zero").await, 1);
    assert_eq!(
        snapshot_id(&catalogs, "sub_zero").await,
        before,
        "F-v3-8-empty-delete-snapshot: the engine commits nothing where Spark commits an \
         empty overwrite"
    );
}

#[tokio::test]
async fn created_v3_merge_on_read_subquery_dml_commits_deletion_vectors() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-001, C-003";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    created_v3(&ctx, &catalogs, "sub_mor_del", MOR_V3_DELETE).await;
    created_v3(&ctx, &catalogs, "sub_mor_upd", MOR_V3_UPDATE).await;
    let before_delete = snapshot_id(&catalogs, "sub_mor_del").await;
    let before_update = snapshot_id(&catalogs, "sub_mor_upd").await;
    run(&ctx, &catalogs, &DELETE_IN.replace("{t}", "sub_mor_del")).await;
    run(&ctx, &catalogs, &UPDATE_IN.replace("{t}", "sub_mor_upd")).await;
    assert_ne!(snapshot_id(&catalogs, "sub_mor_del").await, before_delete);
    assert_ne!(snapshot_id(&catalogs, "sub_mor_upd").await, before_update);
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.sub_mor_del").await,
        vec![(1, "a".to_string()), (3, "c".to_string())]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "sub_mor_del").await,
        vec![(1, 0, 1), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "sub_mor_del").await.next_row_id,
        3
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.sub_mor_upd").await,
        vec![
            (1, "a".to_string()),
            (2, "m".to_string()),
            (3, "c".to_string())
        ]
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "sub_mor_upd").await,
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)]
    );
    assert_eq!(
        v3_cow::lineage(&catalogs, "sub_mor_upd").await.next_row_id,
        4
    );
}
