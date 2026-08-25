//! V3E-1 — copy-on-write DML on a `register_table`-adopted format-v3 table.
//!
//! Model: Grok 4.6 xHigh
//! CodeQuality:S
//!
//! The merge-on-read arms refuse v3. The copy-on-write arms never read the format version
//! (registry `V3-COW-1`). This leaf measures that hole: CREATE v3 (opt-in) + seed, then
//! `CALL system.register_table` under a second ident (the Glue drop-in shape). Memory-catalog
//! `DROP TABLE` deletes the metadata pointer (`FileIO::delete`), so the seed ident is left in
//! place; DML runs against the adopted ident. Lineage columns are not plannable (`V3-ROWID-1`);
//! the engine-observable half is `next_row_id` + the new snapshot's `first_row_id` /
//! `added_rows`. Spark `_row_id` numbers live in the unit ledger.
//!
//! Native `DataFrame` has no Iceberg DML write surface (C-008).
//!
//! V3E-2: [`V3_MAINTENANCE_ORACLE`] is the dated maintenance-oracle pair (charter §5).
//!
//! pins: v3e-1-2-cow-oracle/C-001, C-002, C-003, C-004, C-005, C-006, C-008, C-010

use super::super::*;
use super::common::*;

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

/// pins: v3e-1-2-cow-oracle/C-001, C-005
#[tokio::test]
async fn adopted_v3_cow_delete_commits_and_drops_the_matched_row() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_cow_v3(&ctx, &catalogs, "seed_del", "adopt_del").await;
    let before_snapshot = current_snapshot_id(&catalogs, "adopt_del").await;
    let before = lineage(&catalogs, "adopt_del").await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_del WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_del").await,
        vec![(1, "a".into()), (3, "c".into())]
    );
    let after_snapshot = current_snapshot_id(&catalogs, "adopt_del").await;
    assert_ne!(
        before_snapshot, after_snapshot,
        "COW DELETE must commit a new snapshot"
    );
    let after_table = load_sales(&catalogs, "adopt_del").await;
    assert_still_v3(&after_table);
    let after = lineage(&catalogs, "adopt_del").await;
    // C-005 engine-observable half. Exact numbers filled after the first Actor run.
    assert!(
        before.next_row_id > 0,
        "seed INSERT must assign row lineage; next_row_id={}",
        before.next_row_id
    );
    // Reassigns: remaining 2 rows get new ids starting at 3 (V3-LINEAGE-1 class on DML).
    assert_eq!(before.next_row_id, 3, "seed of 3 rows assigns 0..2");
    assert_eq!(after.next_row_id, 5, "COW DELETE reassigns the 2 survivors");
    assert_eq!(after.snapshot_first_row_id, Some(3));
    assert_eq!(after.snapshot_added_rows, Some(2));
}

/// pins: v3e-1-2-cow-oracle/C-002, C-005
#[tokio::test]
async fn adopted_v3_cow_update_commits_and_rewrites_matched_values() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_cow_v3(&ctx, &catalogs, "seed_upd", "adopt_upd").await;
    let before = lineage(&catalogs, "adopt_upd").await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.adopt_upd SET name = 'x' WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_upd").await,
        vec![(1, "a".into()), (2, "x".into()), (3, "c".into())]
    );
    assert_still_v3(&load_sales(&catalogs, "adopt_upd").await);
    let after = lineage(&catalogs, "adopt_upd").await;
    // Reassigns all 3 live rows (V3-LINEAGE-1 class on DML).
    assert_eq!(before.next_row_id, 3);
    assert_eq!(after.next_row_id, 6, "COW UPDATE reassigns every live row");
    assert_eq!(after.snapshot_first_row_id, Some(3));
    assert_eq!(after.snapshot_added_rows, Some(3));
}

/// pins: v3e-1-2-cow-oracle/C-003, C-005
#[tokio::test]
async fn adopted_v3_cow_merge_commits_matched_and_not_matched() {
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
        "C-003 is the unset write.merge.mode hole, not explicit copy-on-write"
    );
    let before = lineage(&catalogs, "adopt_mrg").await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.adopt_mrg AS t USING (SELECT 2 AS id, 'm' AS name \
         UNION ALL SELECT 4 AS id, 'n' AS name) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.name = s.name \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_mrg").await,
        vec![
            (1, "a".into()),
            (2, "m".into()),
            (3, "c".into()),
            (4, "n".into())
        ]
    );
    assert_still_v3(&load_sales(&catalogs, "adopt_mrg").await);
    let after = lineage(&catalogs, "adopt_mrg").await;
    // 3 rewritten + 1 inserted = 4 new ids (V3-LINEAGE-1 class on DML).
    assert_eq!(before.next_row_id, 3);
    assert_eq!(
        after.next_row_id, 7,
        "COW MERGE reassigns survivors and assigns the insert"
    );
    assert_eq!(after.snapshot_first_row_id, Some(3));
    assert_eq!(after.snapshot_added_rows, Some(4));
}

/// pins: v3e-1-2-cow-oracle/C-004
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
