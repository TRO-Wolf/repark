use super::super::*;
use super::common::*;
use super::v3_cow::{self, Lineage};

use iceberg::spec::{DataContentType, FormatVersion, ManifestContentType};

const MOR_V3: &str = "'format-version' = '3', \
     'write.delete.mode' = 'merge-on-read', \
     'write.update.mode' = 'merge-on-read', \
     'write.merge.mode' = 'merge-on-read'";

const MOR_V2: &str = "'format-version' = '2', \
     'write.delete.mode' = 'merge-on-read', \
     'write.update.mode' = 'merge-on-read'";

const DELETE_PLAIN: &str = "DELETE FROM ice.sales.{t} WHERE id = 2";
const UPDATE_PLAIN: &str = "UPDATE ice.sales.{t} SET name = 'm' WHERE id = 2";
const DELETE_IN: &str = "DELETE FROM ice.sales.{t} WHERE id IN (SELECT id FROM ice.sales.srcids)";
const DELETE_NOT_IN: &str =
    "DELETE FROM ice.sales.{t} WHERE id NOT IN (SELECT id FROM ice.sales.srcids)";
const DELETE_EXISTS: &str = "DELETE FROM ice.sales.{t} AS tgt WHERE EXISTS \
     (SELECT 1 FROM ice.sales.srcids AS s WHERE s.id = tgt.id)";
const DELETE_NOT_EXISTS: &str = "DELETE FROM ice.sales.{t} AS tgt WHERE NOT EXISTS \
     (SELECT 1 FROM ice.sales.srcids AS s WHERE s.id = tgt.id)";
const UPDATE_IN: &str =
    "UPDATE ice.sales.{t} SET name = 'm' WHERE id IN (SELECT id FROM ice.sales.srcids)";

const SEED_ROWS: [(i32, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
const SEED_TRIPLES: [(i32, i64, i64); 3] = [(1, 0, 1), (2, 1, 1), (3, 2, 1)];

struct DeleteEntry {
    format: String,
    content: DataContentType,
    referenced: Option<String>,
    records: u64,
}

struct Cell {
    rows: Vec<(i32, String)>,
    triples: Vec<(i32, i64, i64)>,
    lineage: Lineage,
    data_files: usize,
    delete_records: u64,
}

fn delete_hit_cell() -> Cell {
    Cell {
        rows: vec![(1, "a".into()), (3, "c".into())],
        triples: vec![(1, 0, 1), (3, 2, 1)],
        lineage: Lineage {
            next_row_id: 3,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(0),
        },
        data_files: 1,
        delete_records: 1,
    }
}

fn delete_miss_cell() -> Cell {
    Cell {
        rows: vec![(2, "b".into())],
        triples: vec![(2, 1, 1)],
        lineage: Lineage {
            next_row_id: 3,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(0),
        },
        data_files: 1,
        delete_records: 2,
    }
}

fn update_cell() -> Cell {
    Cell {
        rows: vec![(1, "a".into()), (2, "m".into()), (3, "c".into())],
        triples: vec![(1, 0, 1), (2, 1, 2), (3, 2, 1)],
        lineage: Lineage {
            next_row_id: 4,
            snapshot_first_row_id: Some(3),
            snapshot_added_rows: Some(1),
        },
        data_files: 2,
        delete_records: 1,
    }
}

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

async fn created_mor(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, props: &str) {
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
}

async fn load_sales(catalogs: &CatalogRegistry, table: &str) -> iceberg::table::Table {
    let ident = TableIdent::from_strs(["sales", table]).expect("ident");
    catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load")
}

async fn live_data_file_paths(catalogs: &CatalogRegistry, table: &str) -> Vec<String> {
    content_file_paths(catalogs, table, ManifestContentType::Data).await
}

async fn content_file_paths(
    catalogs: &CatalogRegistry,
    table: &str,
    content: ManifestContentType,
) -> Vec<String> {
    let loaded = load_sales(catalogs, table).await;
    let Some(snapshot) = loaded.metadata().current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), loaded.metadata())
        .await
        .expect("manifest list");
    let mut paths = Vec::new();
    for entry in manifest_list.entries() {
        if entry.content != content {
            continue;
        }
        let manifest = entry
            .load_manifest(loaded.file_io())
            .await
            .expect("manifest");
        for item in manifest.entries() {
            if item.is_alive() {
                paths.push(item.data_file().file_path().to_string());
            }
        }
    }
    paths.sort();
    paths
}

async fn live_delete_entries(catalogs: &CatalogRegistry, table: &str) -> Vec<DeleteEntry> {
    let loaded = load_sales(catalogs, table).await;
    let Some(snapshot) = loaded.metadata().current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), loaded.metadata())
        .await
        .expect("manifest list");
    let mut entries = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(loaded.file_io())
            .await
            .expect("manifest");
        for item in manifest.entries() {
            if !item.is_alive() {
                continue;
            }
            let data_file = item.data_file();
            entries.push(DeleteEntry {
                format: format!("{:?}", data_file.file_format()),
                content: data_file.content_type(),
                referenced: data_file.referenced_data_file(),
                records: data_file.record_count(),
            });
        }
    }
    entries
}

async fn assert_v3_cell(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    cell: &Cell,
) {
    let loaded = load_sales(catalogs, table).await;
    assert_eq!(loaded.metadata().format_version(), FormatVersion::V3);
    assert_eq!(
        table_rows(ctx, catalogs, &format!("ice.sales.{table}")).await,
        cell.rows
    );
    assert_eq!(
        v3_cow::lineage_triples(ctx, catalogs, table).await,
        cell.triples
    );
    assert_eq!(v3_cow::lineage(catalogs, table).await, cell.lineage);
    let data_files = live_data_file_paths(catalogs, table).await;
    assert_eq!(data_files.len(), cell.data_files);
    let deletes = live_delete_entries(catalogs, table).await;
    assert_eq!(deletes.len(), 1, "exactly one live deletion vector");
    assert_eq!(deletes[0].format, "Puffin");
    assert_eq!(deletes[0].content, DataContentType::PositionDeletes);
    assert_eq!(deletes[0].records, cell.delete_records);
    let referenced = deletes[0]
        .referenced
        .as_ref()
        .expect("a v3 deletion vector is file-scoped");
    assert!(
        data_files.contains(referenced),
        "referenced_data_file names a live data file: {referenced} not in {data_files:?}"
    );
}

async fn assert_created(table: &str, statement: &str, cell: &Cell) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    created_mor(&ctx, &catalogs, table, MOR_V3).await;
    run(&ctx, &catalogs, &statement.replace("{t}", table)).await;
    assert_v3_cell(&ctx, &catalogs, table, cell).await;
}

async fn assert_adopted(seed: &str, table: &str, statement: &str, cell: &Cell) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    v3_cow::adopt_v3(&ctx, &catalogs, seed, table, MOR_V3).await;
    run(&ctx, &catalogs, &statement.replace("{t}", table)).await;
    assert_v3_cell(&ctx, &catalogs, table, cell).await;
}

#[tokio::test]
async fn created_v3_mor_subquery_in_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_created("mor_in", DELETE_IN, &delete_hit_cell()).await;
}

#[tokio::test]
async fn created_v3_mor_subquery_not_in_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_created("mor_notin", DELETE_NOT_IN, &delete_miss_cell()).await;
}

#[tokio::test]
async fn created_v3_mor_subquery_exists_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_created("mor_ex", DELETE_EXISTS, &delete_hit_cell()).await;
}

#[tokio::test]
async fn created_v3_mor_subquery_not_exists_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_created("mor_notex", DELETE_NOT_EXISTS, &delete_miss_cell()).await;
}

#[tokio::test]
async fn created_v3_mor_subquery_in_update_keeps_row_id_and_advances_next_row_id_by_one() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_created("mor_upd", UPDATE_IN, &update_cell()).await;
}

#[tokio::test]
async fn adopted_v3_mor_subquery_in_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_adopted("seed_ain", "amor_in", DELETE_IN, &delete_hit_cell()).await;
}

#[tokio::test]
async fn adopted_v3_mor_subquery_not_in_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_adopted(
        "seed_anotin",
        "amor_notin",
        DELETE_NOT_IN,
        &delete_miss_cell(),
    )
    .await;
}

#[tokio::test]
async fn adopted_v3_mor_subquery_exists_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_adopted("seed_aex", "amor_ex", DELETE_EXISTS, &delete_hit_cell()).await;
}

#[tokio::test]
async fn adopted_v3_mor_subquery_not_exists_delete_writes_one_file_scoped_deletion_vector() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_adopted(
        "seed_anotex",
        "amor_notex",
        DELETE_NOT_EXISTS,
        &delete_miss_cell(),
    )
    .await;
}

#[tokio::test]
async fn adopted_v3_mor_subquery_in_update_keeps_row_id_and_advances_next_row_id_by_one() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-003";
    assert_adopted("seed_aupd", "amor_upd", UPDATE_IN, &update_cell()).await;
}

#[tokio::test]
async fn created_v3_mor_plain_where_dml_matches_the_subquery_cell() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-004";
    assert_created("mor_plain_del", DELETE_PLAIN, &delete_hit_cell()).await;
    assert_created("mor_plain_upd", UPDATE_PLAIN, &update_cell()).await;
}

#[tokio::test]
async fn v3_mor_subquery_delete_granularity_partition_still_writes_one_file_scoped_dv() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-004";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    created_mor(
        &ctx,
        &catalogs,
        "mor_gran",
        &format!("{MOR_V3}, 'write.delete.granularity' = 'partition'"),
    )
    .await;
    run(&ctx, &catalogs, &DELETE_IN.replace("{t}", "mor_gran")).await;
    assert_v3_cell(&ctx, &catalogs, "mor_gran", &delete_hit_cell()).await;
}

#[tokio::test]
async fn v2_mor_subquery_dml_still_writes_parquet_position_deletes() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-004";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_source(&ctx, &catalogs).await;
    for (table, statement) in [("v2_mor_del", DELETE_IN), ("v2_mor_upd", UPDATE_IN)] {
        created_mor(&ctx, &catalogs, table, MOR_V2).await;
        run(&ctx, &catalogs, &statement.replace("{t}", table)).await;
        let loaded = load_sales(&catalogs, table).await;
        assert_eq!(loaded.metadata().format_version(), FormatVersion::V2);
        let deletes = live_delete_entries(&catalogs, table).await;
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].format, "Parquet");
        assert_eq!(deletes[0].content, DataContentType::PositionDeletes);
        assert_eq!(deletes[0].referenced, None);
    }
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.v2_mor_del").await,
        vec![(1, "a".to_string()), (3, "c".to_string())]
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.v2_mor_upd").await,
        vec![
            (1, "a".to_string()),
            (2, "m".to_string()),
            (3, "c".to_string())
        ]
    );
}

#[tokio::test]
async fn v3_mor_subquery_delete_matching_nothing_leaves_the_table_at_the_seed() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-004";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.srcids (id INT) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.srcids VALUES (9)").await;
    created_mor(&ctx, &catalogs, "mor_empty", MOR_V3).await;
    run(&ctx, &catalogs, &DELETE_IN.replace("{t}", "mor_empty")).await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.mor_empty").await,
        SEED_ROWS
            .iter()
            .map(|(id, name)| (*id, (*name).to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        v3_cow::lineage_triples(&ctx, &catalogs, "mor_empty").await,
        SEED_TRIPLES.to_vec()
    );
    assert!(live_delete_entries(&catalogs, "mor_empty").await.is_empty());
}

#[tokio::test]
async fn the_v3_create_opt_in_refusal_no_longer_claims_merge_on_read_is_unserved() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-006";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for statement in [
        "CREATE TABLE ice.sales.v3_gate (id INT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
        "CREATE TABLE ice.sales.v3ctas_gate USING iceberg \
         TBLPROPERTIES ('format-version' = '3') AS SELECT * FROM src",
    ] {
        let error = execute(&ctx, &catalogs, statement)
            .await
            .expect_err("format-version 3 without the opt-in must refuse")
            .to_string();
        assert!(
            error.contains("repark.sql.allowCreateFormatVersion3")
                && error.contains("format-version")
                && !error.contains("merge-on-read"),
            "opt-in refusal names the conf, not a false merge-on-read limit: {error}"
        );
    }
}
