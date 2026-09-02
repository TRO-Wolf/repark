//! Pins format-v3 maintenance behavior, including lineage and dangling-delete guards.
//! pins: v3-6-v3-types/C-006

use super::super::*;
use super::common::*;

use iceberg::spec::FormatVersion;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

/// Upgrade an engine-created v2 table to v3 through the fork's transaction action.
async fn upgrade_to_v3(catalog: &Arc<dyn Catalog>, ident: &TableIdent) {
    let table = catalog.load_table(ident).await.expect("load for upgrade");
    let transaction = Transaction::new(&table);
    let action = transaction
        .upgrade_table_version()
        .set_format_version(FormatVersion::V3);
    let transaction = action.apply(transaction).expect("apply upgrade");
    transaction
        .commit(catalog.as_ref())
        .await
        .expect("commit upgrade");
}

/// Six single-row data files, which is one over Spark's `min-input-files` floor of five.
async fn seed_six_files(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} AS SELECT 1 AS id, 'a' AS name"),
    )
    .await;
    for index in 2..=6 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
}

/// The fixture is genuinely v3, asserted before anything is concluded from it.
#[tokio::test]
async fn v3_fixture_really_is_format_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_six_files(&ctx, &catalogs, "fixture_check").await;
    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "fixture_check"]).unwrap();

    let before = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        before.metadata().format_version(),
        FormatVersion::V2,
        "the engine creates v2 tables; if this ever changes the guard's domain changes with it"
    );

    upgrade_to_v3(catalog, &ident).await;

    let after = catalog.load_table(&ident).await.unwrap();
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
}

/// pins: rp-2-fork-repin/C-004
/// pins: rp-3-fork-repin/C-005
/// pins: rp-4-fork-repin/C-003
/// Public CALL on a 12-file v3 table keeps `_row_id` / seq Spark-equal.
#[tokio::test]
async fn call_rewrite_data_files_on_v3_preserves_row_lineage() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3_rewrite (id BIGINT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    for index in 1..=12 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.v3_rewrite SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "v3_rewrite"]).unwrap();
    let table = catalog.load_table(&ident).await.expect("load v3");
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let before = scan_lineage_triples(&table).await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v3_rewrite')",
    )
    .await
    .expect("v3 rewrite must run now that fork #243 carries lineage")
    .collect()
    .await
    .expect("collect v3 rewrite");
    let rewritten = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("rewritten_data_files_count is Int32")
        .value(0);
    assert_eq!(rewritten, 12, "twelve single-row files must compact");

    let after_table = catalog.load_table(&ident).await.expect("reload");
    let after = scan_lineage_triples(&after_table).await;
    assert_eq!(
        after, before,
        "CALL rewrite_data_files must keep _row_id and last_updated_seq"
    );
}

/// The incidental control: v2 still compacts, and still reports five columns.
#[tokio::test]
async fn call_rewrite_data_files_still_compacts_a_v2_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_six_files(&ctx, &catalogs, "v2_rewrite").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v2_rewrite')",
    )
    .await
    .expect("v2 rewrite must still run")
    .collect()
    .await
    .expect("collect v2 rewrite");
    let batch = &batches[0];

    assert_eq!(batch.schema().fields().len(), 5, "Spark's five columns");
    let rewritten = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("rewritten_data_files_count is Int32")
        .value(0);
    assert_eq!(
        rewritten, 6,
        "six single-row files must still be compacted on v2"
    );
}

/// The guard's blast-radius argument, pinned instead of asserted.
// pins: v3-2-create-v3-opt-in/C-004, C-008
#[tokio::test]
async fn the_engine_still_cannot_produce_a_v3_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v2only AS SELECT 1 AS id, 'a' AS name",
    )
    .await;

    for sql in [
        "CREATE TABLE ice.sales.c3 (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
        "CREATE TABLE ice.sales.c4 USING iceberg TBLPROPERTIES ('format-version' = '3') \
         AS SELECT 1 AS id",
        "ALTER TABLE ice.sales.v2only SET TBLPROPERTIES ('format-version' = '3')",
        "ALTER TABLE ice.sales.v2only SET TBLPROPERTIES ('format-version' = '3', 'k' = 'v')",
    ] {
        let outcome = match execute(&ctx, &catalogs, sql).await {
            Ok(frame) => frame.collect().await.err().map(|err| err.to_string()),
            Err(err) => Some(err.to_string()),
        };
        assert!(
            outcome.is_some(),
            "this door produced a v3 table, so the guard's blast-radius claim is wrong: {sql}"
        );
    }

    // And the table it did create is still v2.
    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "v2only"]).unwrap();
    assert_eq!(
        catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .format_version(),
        FormatVersion::V2
    );
}

/// pins: v3-2-create-v3-opt-in/C-005, C-011, C-015
/// pins: rp-4-fork-repin/C-003
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn opt_in_create_produces_v3_and_rewrite_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3opt (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    for index in 1..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.v3opt SELECT {index} AS id"),
        )
        .await;
    }

    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "v3opt"]).unwrap();
    let table = catalog.load_table(&ident).await.unwrap();
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let before = scan_lineage_triples(&table).await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v3opt')",
    )
    .await
    .expect("opt-in v3 rewrite must run")
    .collect()
    .await
    .expect("collect opt-in v3 rewrite");
    let rewritten = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("rewritten_data_files_count is Int32")
        .value(0);
    assert_eq!(rewritten, 6, "six single-row files must compact");
    let after = scan_lineage_triples(&catalog.load_table(&ident).await.unwrap()).await;
    assert_eq!(
        after, before,
        "opt-in CREATE rewrite must keep _row_id and last_updated_seq"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3mor (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.v3mor SELECT 1 AS id",
    )
    .await;
    let merge = match execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.v3mor AS t USING (SELECT 1 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
    )
    .await
    {
        Ok(frame) => frame.collect().await.err().map(|err| err.to_string()),
        Err(err) => Some(err.to_string()),
    };
    let merge = merge.expect("merge-on-read MERGE must still refuse a v3 table");
    assert!(
        merge.contains("V3"),
        "non-v2 MoR MERGE guard must still fire: {merge}"
    );
}

async fn scan_lineage_triples(table: &iceberg::table::Table) -> Vec<(i64, i64, i64)> {
    use futures::TryStreamExt;
    use iceberg::metadata_columns::{
        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    };

    let stream = table
        .scan()
        .select([
            "id",
            RESERVED_COL_NAME_ROW_ID,
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
        ])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow");
    let batches: Vec<_> = stream.try_collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch.column_by_name("id").expect("id");
        let row_ids = batch
            .column_by_name(RESERVED_COL_NAME_ROW_ID)
            .expect("_row_id")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_row_id Int64");
        let seqs = batch
            .column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
            .expect("seq")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seq Int64");
        for index in 0..batch.num_rows() {
            let id = if let Some(array) = ids.as_any().downcast_ref::<Int64Array>() {
                array.value(index)
            } else if let Some(array) = ids.as_any().downcast_ref::<Int32Array>() {
                i64::from(array.value(index))
            } else {
                panic!("id type {:?}", ids.data_type());
            };
            assert!(!row_ids.is_null(index), "missing _row_id at {index}");
            assert!(!seqs.is_null(index), "missing seq at {index}");
            rows.push((id, row_ids.value(index), seqs.value(index)));
        }
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn rewrite_after_same_arity_spec_evolution_stamps_current_spec() {
    let _: &str = "pins: rp-6-fork-repin/C-005";
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.evo (id INT, x INT, y INT) USING iceberg \
         PARTITIONED BY (x) TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    for index in 1..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.evo VALUES ({index}, 10, {index})"),
        )
        .await;
    }
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.evo REPLACE PARTITION FIELD x WITH y",
    )
    .await;
    let catalog = catalogs.get("ice").expect("ice");
    let ident = TableIdent::from_strs(["sales", "evo"]).unwrap();
    let before = catalog
        .load_table(&ident)
        .await
        .expect("load before rewrite");
    let current_spec = before.metadata().default_partition_spec_id();
    assert_ne!(current_spec, 0, "REPLACE must mint a new spec");
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.evo')",
    )
    .await
    .expect("rewrite after evolution must run")
    .collect()
    .await
    .expect("collect rewrite");
    let rewritten = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("rewritten_data_files_count")
        .value(0);
    assert!(
        rewritten > 0,
        "compaction must rewrite at least one file, got {rewritten}"
    );
    let after = catalog
        .load_table(&ident)
        .await
        .expect("load after rewrite");
    assert_eq!(after.metadata().default_partition_spec_id(), current_spec);
    let snapshot = after.metadata().current_snapshot().expect("snapshot");
    let manifest_list = snapshot
        .load_manifest_list(after.file_io(), after.metadata())
        .await
        .expect("manifest list");
    let mut spec_ids = Vec::new();
    for entry in manifest_list.entries() {
        if entry.content != iceberg::spec::ManifestContentType::Data {
            continue;
        }
        let manifest = entry
            .load_manifest(after.file_io())
            .await
            .expect("manifest");
        for item in manifest.entries() {
            if item.is_alive() {
                spec_ids.push(item.data_file().partition_spec_id());
            }
        }
    }
    assert!(!spec_ids.is_empty(), "rewrite must leave live data files");
    assert!(
        spec_ids.iter().all(|spec_id| *spec_id == current_spec),
        "every rewritten file must stamp the current spec {current_spec}, got {spec_ids:?}"
    );
}
