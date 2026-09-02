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
