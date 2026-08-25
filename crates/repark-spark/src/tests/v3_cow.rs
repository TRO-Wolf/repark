//! V3R-1 — copy-on-write DML on a format-v3 table refuses (registry `V3-COW-1`).
//!
//! Model: Claude Fable 5
//! CodeQuality:S
//!
//! V3E-1 measured the copy-on-write arms committing on an adopted v3 table while reassigning
//! row lineage (`next_row_id` 3 → 5 / 6 / 7 for DELETE / UPDATE / MERGE). The owner ruled the
//! path guarded (2026-08-25): every copy-on-write arm now refuses a v3 table at write-mode
//! resolution, before any data write, naming the row, the verb and row lineage; the table keeps
//! its snapshot, its rows and its lineage counters. The merge-on-read arms still refuse v3
//! (R113), so a v3 table is append-only here until fork F-7. Adoption uses the V3E-1 shape:
//! CREATE v3 (opt-in) + seed, then `CALL system.register_table` under a second ident.
//!
//! Native `DataFrame` has no Iceberg DML write surface (C-012).
//!
//! V3E-2: [`V3_MAINTENANCE_ORACLE`] is the dated maintenance-oracle pair (charter §5).
//!
//! pins: v3r-1-rulings/C-001, C-002, C-003, C-004, C-005, C-012
//! pins: v3e-1-2-cow-oracle/C-009, C-010

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

/// Every copy-on-write arm refuses the same way: the error names the registry row, the verb
/// and row lineage; afterwards the table has the same snapshot, the same live rows and the same
/// lineage counters, and is still v3.
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

/// pins: v3r-1-rulings/C-001
#[tokio::test]
async fn adopted_v3_cow_delete_refuses_rather_than_reassign_row_lineage() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    adopt_cow_v3(&ctx, &catalogs, "seed_del", "adopt_del").await;
    assert_eq!(
        lineage(&catalogs, "adopt_del").await.next_row_id,
        3,
        "seed INSERT of 3 rows assigns 0..2 — append stays open"
    );
    assert_cow_refused_untouched(
        &ctx,
        &catalogs,
        "adopt_del",
        "DELETE FROM ice.sales.adopt_del WHERE id = 2",
        "DELETE",
    )
    .await;
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

/// V3R-1 measurement: the merge-on-read plain-`WHERE` `DELETE` on an adopted v3 table takes the
/// passthrough path (the fork's `TableProvider`), where the V3-COW-1 seat deliberately steps
/// aside. Pins that this path refuses too, naming format v3 — a v3 table is append-only.
///
/// pins: v3r-1-rulings/C-004
#[tokio::test]
async fn adopted_v3_mor_delete_still_refuses() {
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
    let before_rows = table_rows(&ctx, &catalogs, "ice.sales.adopt_mordel").await;
    let err = execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.adopt_mordel WHERE id = 2",
    )
    .await
    .expect_err("MoR DELETE on v3 must refuse")
    .to_string();
    assert!(
        err.contains("V3") || err.contains("v3") || err.contains("deletion vector"),
        "MoR DELETE refuse must name format v3: {err}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_mordel").await,
        before_rows
    );
}

/// CCC SEC-001 regression: with the session's `datafusion.catalog.default_catalog` /
/// `default_schema` set, DataFusion resolves two-part and bare names — and before the fix the
/// valve stepped aside below three parts, so `DELETE FROM sales.t` committed a v3 rewrite.
/// Both short forms now refuse, and the table is untouched.
///
/// pins: v3r-1-rulings/C-001, C-002
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
        "DELETE FROM sales.adopt_short WHERE id = 2",
        "DELETE",
    )
    .await;
    assert_cow_refused_untouched(
        &ctx,
        &catalogs,
        "adopt_short",
        "UPDATE adopt_short SET name = 'x' WHERE id = 2",
        "UPDATE",
    )
    .await;
}

/// CCC SEC-002 regression: a padded `' Merge-On-Read '` was merge-on-read to the valve's
/// trim-and-case-fold check (which stepped aside) and copy-on-write to the fork, which
/// committed the rewrite. The valve now refuses every v3 table whatever the mode says.
///
/// pins: v3r-1-rulings/C-004
#[tokio::test]
async fn adopted_v3_padded_merge_on_read_spelling_still_refuses() {
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
        "DELETE FROM ice.sales.adopt_pad WHERE id = 2",
    )
    .await
    .expect_err("a padded merge-on-read spelling on v3 must still refuse")
    .to_string();
    assert!(
        err.contains("V3") && err.contains("deletion vectors"),
        "the merge-on-read arm's reason: {err}"
    );
    assert_eq!(lineage(&catalogs, "adopt_pad").await, before, "no commit");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.adopt_pad").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );
}
