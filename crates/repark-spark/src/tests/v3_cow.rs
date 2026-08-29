//! V3R-1 (2026-08-25): copy-on-write UPDATE / MERGE and the resolver seat refuse a v3 table
//! before any write (`V3-COW-1`). RP-2 (2026-08-27, fork `ce92a7bf`) measured the plain-`WHERE`
//! DELETE on v3 Spark-clean in both modes and lifted it: MOR commits Puffin DVs, COW preserves
//! survivor lineage. No native `DataFrame` DML (C-012).
//!
//! Model: Claude Fable 5
//!
//! V3E-2: [`V3_MAINTENANCE_ORACLE`] is the dated maintenance-oracle pair (charter §5).
//! pins: v3r-1-rulings/C-001, C-002, C-003, C-004, C-005, C-012
//! pins: v3e-1-2-cow-oracle/C-009, C-010
//! pins: rp-2-fork-repin/C-002, C-003, C-005
//! pins: rp-2-fork-repin/C-001, C-004, C-007, C-008

use super::super::*;
use super::common::*;

use std::path::PathBuf;

use iceberg::spec::FormatVersion;

/// Dated V3E-2 decision (2026-08-24): Spark 4.1.2 + Iceberg 1.11.0 runs v3
/// `rewrite_data_files` / `expire_snapshots`. Aligns the nightly pin. Transcript in the unit ledger.
pub(super) const V3_MAINTENANCE_ORACLE: &str = "pyspark-4.1.2+iceberg-1.11.0";

const COW_V3: &str = "'format-version' = '3', \
     'write.delete.mode' = 'copy-on-write', \
     'write.update.mode' = 'copy-on-write', \
     'write.merge.mode' = 'copy-on-write'";

/// C-003: v3 with no `write.merge.mode` (engine default is copy-on-write).
const UNSET_MERGE_V3: &str = "'format-version' = '3'";

/// Iceberg table property that selects the table master key (spec: table encryption).
const ENCRYPTION_KEY_ID_PROP: &str = "encryption.key-id";

#[derive(Debug, PartialEq, Eq)]
struct Lineage {
    next_row_id: u64,
    snapshot_first_row_id: Option<u64>,
    snapshot_added_rows: Option<u64>,
}

/// CREATE v3 (opt-in) + seed + `register_table` under `adopted`.
///
/// Returns the metadata pointer the CALL adopted.
async fn adopt_v3(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    seed: &str,
    adopted: &str,
    tblproperties: &str,
) -> String {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{seed} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ({tblproperties})"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{seed} SELECT * FROM src"),
    )
    .await;
    let ident = TableIdent::from_strs(["sales", seed]).expect("seed ident");
    let catalog = catalogs.get("ice").expect("ice");
    let table = catalog.load_table(&ident).await.expect("load seed");
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let metadata_file = table
        .metadata_location()
        .expect("engine-created v3 has a version-uuid pointer")
        .to_string();
    run(
        ctx,
        catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.{adopted}', \
             metadata_file => '{metadata_file}')"
        ),
    )
    .await;
    let adopted_ident = TableIdent::from_strs(["sales", adopted]).expect("adopted ident");
    let adopted_table = catalog
        .load_table(&adopted_ident)
        .await
        .expect("load adopted");
    assert_eq!(
        adopted_table.metadata().format_version(),
        FormatVersion::V3,
        "register_table must keep format v3"
    );
    metadata_file
}

async fn adopt_cow_v3(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    seed: &str,
    adopted: &str,
) -> String {
    adopt_v3(ctx, catalogs, seed, adopted, COW_V3).await
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

async fn current_snapshot_id(catalogs: &CatalogRegistry, table: &str) -> Option<i64> {
    load_sales(catalogs, table)
        .await
        .metadata()
        .current_snapshot_id()
}

async fn lineage(catalogs: &CatalogRegistry, table: &str) -> Lineage {
    let loaded = load_sales(catalogs, table).await;
    let metadata = loaded.metadata();
    let (snapshot_first_row_id, snapshot_added_rows) = metadata
        .current_snapshot()
        .map_or((None, None), |snapshot| {
            (snapshot.first_row_id(), snapshot.added_rows_count())
        });
    Lineage {
        next_row_id: metadata.next_row_id(),
        snapshot_first_row_id,
        snapshot_added_rows,
    }
}

fn assert_still_v3(table: &iceberg::table::Table) {
    assert_eq!(
        table.metadata().format_version(),
        FormatVersion::V3,
        "COW DML must not change format version"
    );
}

/// pins: v3e-1-2-cow-oracle/C-010
#[test]
fn v3_maintenance_oracle_is_the_recorded_pair() {
    assert_eq!(
        V3_MAINTENANCE_ORACLE, "pyspark-4.1.2+iceberg-1.11.0",
        "V3E-2 named pair must match the live transcript"
    );
}

/// The refusal names the row, the verb and row lineage; the table is untouched and still v3.
async fn assert_cow_refused_untouched(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    sql: &str,
    verb: &str,
) {
    let before_snapshot = current_snapshot_id(catalogs, table).await;
    let before = lineage(catalogs, table).await;
    let qualified = format!("ice.sales.{table}");
    let before_rows = table_rows(ctx, catalogs, &qualified).await;
    let err = execute(ctx, catalogs, sql)
        .await
        .expect_err("copy-on-write DML on a v3 table must refuse")
        .to_string();
    assert!(
        err.contains("V3-COW-1") && err.contains("row lineage") && err.contains(verb),
        "refusal must name the row, row lineage and `{verb}`: {err}"
    );
    assert_eq!(
        current_snapshot_id(catalogs, table).await,
        before_snapshot,
        "a refused {verb} must not commit"
    );
    assert_eq!(
        lineage(catalogs, table).await,
        before,
        "a refused {verb} must not touch lineage counters"
    );
    assert_eq!(
        table_rows(ctx, catalogs, &qualified).await,
        before_rows,
        "a refused {verb} must not touch rows"
    );
    assert_still_v3(&load_sales(catalogs, table).await);
}

/// RP-2: plain-`WHERE` COW DELETE on v3 runs; every survivor keeps its lineage counters.
/// pins: rp-2-fork-repin/C-005
#[tokio::test]
async fn adopted_v3_cow_delete_carries_survivor_row_lineage() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_cow_v3(&ctx, &catalogs, "seed_del", "adopt_del").await;
    assert_eq!(
        lineage(&catalogs, "adopt_del").await.next_row_id,
        3,
        "seed INSERT of 3 rows assigns 0..2 — append stays open"
    );
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_del WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_del").await,
        vec![(1, "a".into()), (3, "c".into())],
        "the delete commits the right rows"
    );
    assert_eq!(
        lineage(&catalogs, "adopt_del").await.next_row_id,
        5,
        "next_row_id matches Spark's own v3 COW DELETE exactly (live oracle 2026-08-27: \\
         Spark 4.1.2 + Iceberg 1.11.0 leaves next-row-id = 5 on the same recipe, allocating \\
         then suppressing — #226); Spark reads every survivor's _row_id unchanged"
    );
    assert_still_v3(&load_sales(&catalogs, "adopt_del").await);
}

/// pins: v3r-1-rulings/C-002
#[tokio::test]
async fn adopted_v3_cow_update_refuses_rather_than_reassign_row_lineage() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_cow_v3(&ctx, &catalogs, "seed_upd", "adopt_upd").await;
    assert_cow_refused_untouched(
        &ctx,
        &catalogs,
        "adopt_upd",
        "UPDATE ice.sales.adopt_upd SET name = 'x' WHERE id = 2",
        "UPDATE",
    )
    .await;
}

/// pins: v3r-1-rulings/C-003
#[tokio::test]
async fn adopted_v3_cow_merge_refuses_with_unset_and_explicit_mode() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_v3(&ctx, &catalogs, "seed_mrg", "adopt_mrg", UNSET_MERGE_V3).await;
    assert!(
        load_sales(&catalogs, "adopt_mrg")
            .await
            .metadata()
            .properties()
            .get("write.merge.mode")
            .is_none(),
        "the unset write.merge.mode hole must be covered, not only explicit copy-on-write"
    );
    adopt_cow_v3(&ctx, &catalogs, "seed_mrg2", "adopt_mrg2").await;
    for table in ["adopt_mrg", "adopt_mrg2"] {
        assert_cow_refused_untouched(
            &ctx,
            &catalogs,
            table,
            &format!(
                "MERGE INTO ice.sales.{table} AS t USING (SELECT 2 AS id, 'm' AS name \
                 UNION ALL SELECT 4 AS id, 'n' AS name) AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.name = s.name \
                 WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
            ),
            "MERGE INTO",
        )
        .await;
    }
}

/// pins: v3r-1-rulings/C-005
#[tokio::test]
async fn v2_cow_delete_still_commits_control() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v2del (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.v2del SELECT * FROM src",
    )
    .await;
    assert_eq!(
        load_sales(&catalogs, "v2del")
            .await
            .metadata()
            .format_version(),
        FormatVersion::V2,
        "the control is a v2 table"
    );
    run(&ctx, &catalogs, "DELETE FROM ice.sales.v2del WHERE id = 2").await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.v2del").await,
        vec![(1, "a".into()), (3, "c".into())],
        "the guard must not reach v2"
    );
}

/// pins: v3r-1-rulings/C-004
#[tokio::test]
async fn adopted_v3_mor_merge_still_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.morv3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.morv3 SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::from_strs(["sales", "morv3"]).unwrap();
    let catalog = catalogs.get("ice").expect("ice");
    let metadata_file = catalog
        .load_table(&ident)
        .await
        .unwrap()
        .metadata_location()
        .expect("pointer")
        .to_string();
    run(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.adopt_mor', \
             metadata_file => '{metadata_file}')"
        ),
    )
    .await;
    assert_eq!(
        load_sales(&catalogs, "adopt_mor")
            .await
            .metadata()
            .format_version(),
        FormatVersion::V3
    );
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.adopt_mor AS t USING (SELECT 1 AS id, 'z' AS name) AS s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name",
    )
    .await
    .expect_err("MoR MERGE on v3 must still refuse")
    .to_string();
    assert!(
        err.contains("this table is V3") && err.contains("deletion vectors"),
        "MoR refuse must name format V3: {err}"
    );
}

/// pins: v3e-1-2-cow-oracle/C-009
#[tokio::test]
async fn v3_create_with_encryption_key_id_still_scans_without_a_kms() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.enc (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '3', '{ENCRYPTION_KEY_ID_PROP}' = 'not-a-key')"
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.enc SELECT * FROM src",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
        "this engine does not implement table encryption: rows must stay readable"
    );
    let table = load_sales(&catalogs, "enc").await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    assert!(
        table.metadata().encryption_keys_iter().next().is_none(),
        "setting {ENCRYPTION_KEY_ID_PROP} must not populate table-metadata encryption-keys"
    );
    assert_eq!(
        table
            .metadata()
            .properties()
            .get(ENCRYPTION_KEY_ID_PROP)
            .map(String::as_str),
        Some("not-a-key"),
        "the property is stored; encryption is not performed"
    );
}

/// RP-2: merge-on-read plain-`WHERE` DELETE on v3 commits a Puffin deletion vector.
/// pins: rp-2-fork-repin/C-003
#[tokio::test]
async fn adopted_v3_mor_delete_commits_a_puffin_deletion_vector() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_v3(
        &ctx,
        &catalogs,
        "seed_mordel",
        "adopt_mordel",
        "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'",
    )
    .await;
    let before_snapshot = current_snapshot_id(&catalogs, "adopt_mordel").await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_mordel WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_mordel").await,
        vec![(1, "a".into()), (3, "c".into())],
        "the delete commits the right rows"
    );
    assert_ne!(
        current_snapshot_id(&catalogs, "adopt_mordel").await,
        before_snapshot,
        "the MOR DELETE commits"
    );
    let kinds = live_delete_file_kinds(&catalogs, "adopt_mordel").await;
    assert!(
        kinds.iter().all(|kind| kind == "Puffin"),
        "v3 forbids new position-delete files; got {kinds:?}"
    );
    assert!(
        !kinds.is_empty(),
        "the MOR DELETE must commit a delete file"
    );
}

#[tokio::test]
async fn adopted_v3_mor_second_delete_refuses_while_a_deletion_vector_is_live() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_v3(
        &ctx,
        &catalogs,
        "seed_mor2",
        "adopt_mor2",
        "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_mor2 WHERE id = 2",
    )
    .await;
    let first = load_sales(&catalogs, "adopt_mor2").await;
    let kinds = live_delete_file_kinds(&catalogs, "adopt_mor2").await;
    assert_eq!(
        kinds,
        vec!["Puffin".to_string()],
        "one live deletion vector after the first DELETE"
    );
    let location = PathBuf::from(first.metadata().location());
    let objects = count_objects(&location);
    let before = lineage(&catalogs, "adopt_mor2").await;
    let survivors = vec![(1, "a".to_string()), (3, "c".to_string())];
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_mor2").await,
        survivors
    );
    let err = execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_mor2 WHERE id = 3",
    )
    .await
    .expect_err("a second DELETE must refuse while a deletion vector is live")
    .to_string();
    assert!(
        err.contains("V3-COW-1") && err.contains("1 live deletion vector"),
        "the refusal must name the live vector: {err}"
    );
    let second = load_sales(&catalogs, "adopt_mor2").await;
    assert_eq!(
        second.metadata().current_snapshot_id(),
        first.metadata().current_snapshot_id(),
        "a refused DELETE must not commit"
    );
    assert_eq!(
        second.metadata_location(),
        first.metadata_location(),
        "the metadata pointer is unchanged"
    );
    assert_eq!(lineage(&catalogs, "adopt_mor2").await, before);
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_mor2").await,
        survivors
    );
    assert_eq!(live_delete_file_kinds(&catalogs, "adopt_mor2").await, kinds);
    assert_eq!(
        count_objects(&location),
        objects,
        "no object was written under the table location"
    );
    assert_still_v3(&second);
}

/// Live delete files in the CURRENT snapshot, as debug-formatted file formats.
async fn live_delete_file_kinds(catalogs: &CatalogRegistry, table: &str) -> Vec<String> {
    let loaded = load_sales(catalogs, table).await;
    let mut kinds = Vec::new();
    if let Some(snapshot) = loaded.metadata().current_snapshot() {
        let manifest_list = snapshot
            .load_manifest_list(loaded.file_io(), loaded.metadata())
            .await
            .expect("manifest list");
        for entry in manifest_list.entries() {
            if entry.content != iceberg::spec::ManifestContentType::Deletes {
                continue;
            }
            let manifest = entry
                .load_manifest(loaded.file_io())
                .await
                .expect("manifest");
            for entry in manifest.entries() {
                if entry.is_alive() {
                    kinds.push(format!("{:?}", entry.data_file().file_format()));
                }
            }
        }
    }
    kinds
}

/// SEC-001: two-part and bare names under a session default catalog / schema resolve the same
/// guard seats: the UPDATE refuses, the DELETE (RP-2 lift) commits the right rows.
/// pins: rp-2-fork-repin/C-003, C-005
#[tokio::test]
async fn adopted_v3_cow_dml_with_default_catalog_short_names_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_cow_v3(&ctx, &catalogs, "seed_short", "adopt_short").await;
    ctx.sql("SET datafusion.catalog.default_catalog = 'ice'")
        .await
        .expect("set default catalog");
    ctx.sql("SET datafusion.catalog.default_schema = 'sales'")
        .await
        .expect("set default schema");
    assert_cow_refused_untouched(
        &ctx,
        &catalogs,
        "adopt_short",
        "UPDATE adopt_short SET name = 'x' WHERE id = 2",
        "UPDATE",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM sales.adopt_short WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_short").await,
        vec![(1, "a".into()), (3, "c".into())],
        "the short-name DELETE resolves and commits"
    );
}

/// SEC-002: a padded `' Merge-On-Read '` (copy-on-write to the fork) still refuses on v3 —
/// the UPDATE refusal never consults the property (RP-2), so the spelling cannot slip past.
/// pins: v3r-1-rulings/C-004
/// pins: rp-2-fork-repin/C-005
#[tokio::test]
async fn adopted_v3_padded_merge_on_read_spelling_still_refuses_update() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_v3(
        &ctx,
        &catalogs,
        "seed_pad",
        "adopt_pad",
        "'format-version' = '3', 'write.delete.mode' = ' Merge-On-Read '",
    )
    .await;
    let before = lineage(&catalogs, "adopt_pad").await;
    let err = execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.adopt_pad SET name = 'x' WHERE id = 2",
    )
    .await
    .expect_err("a padded merge-on-read spelling must not slip past the UPDATE refusal")
    .to_string();
    assert!(
        err.contains("V3-COW-1") && err.contains("row lineage"),
        "the copy-on-write arm's reason: {err}"
    );
    assert_eq!(lineage(&catalogs, "adopt_pad").await, before, "no commit");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_pad").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );
}
