//! pins: v3-5-dv-compaction/C-001, C-002, C-003, C-004
//! Model: Grok 4.6
//! V3-5: v3 `rewrite_data_files` drops in-scope Puffin DVs and reports the true count.
//! MUTATION: `removed_delete_files_count == 0` → this REDs.

use super::super::*;
use super::call::call_count;
use super::common::*;

use iceberg::spec::FormatVersion;

async fn live_ids(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> Vec<i32> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id FROM {table} ORDER BY id"),
    )
    .await
    .expect("select live ids")
    .collect()
    .await
    .expect("collect live ids");
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch.column(0);
        if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
            ids.extend(array.iter().flatten());
        } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
            ids.extend(
                array
                    .iter()
                    .flatten()
                    .map(|value| i32::try_from(value).expect("id fits i32")),
            );
        } else {
            panic!("id must be Int32 or Int64, got {:?}", batch.schema());
        }
    }
    ids
}

async fn seed_six_file_v3_mor_with_one_dv_per_file(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.v3dv (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read', \
         'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    for index in 1..=6 {
        run(
            ctx,
            catalogs,
            &format!(
                "INSERT INTO ice.sales.v3dv VALUES ({index}, 'a'), ({}, 'b')",
                index + 10
            ),
        )
        .await;
    }
    run(ctx, catalogs, "DELETE FROM ice.sales.v3dv WHERE id <= 6").await;
}

#[tokio::test]
async fn call_rewrite_position_delete_files_still_refuses_engine_written_v3_dvs() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_six_file_v3_mor_with_one_dv_per_file(&ctx, &catalogs).await;
    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.v3dv')",
    )
    .await
    .expect_err("B-MOR-3: live Puffin vectors must refuse")
    .to_string();
    assert!(
        err.contains("live Puffin deletion vector"),
        "refusal must name Puffin vectors: {err}"
    );
    assert!(
        err.contains("6 live Puffin"),
        "refusal must name the live count: {err}"
    );
}

#[tokio::test]
async fn call_rewrite_data_files_on_v3_drops_scoped_deletion_vectors() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_six_file_v3_mor_with_one_dv_per_file(&ctx, &catalogs).await;

    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "v3dv"]).unwrap();
    let table = catalog.load_table(&ident).await.expect("load v3dv");
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let before_vectors = crate::call::count_live_deletion_vectors(&table)
        .await
        .expect("count DVs before rewrite");
    let before_ids = live_ids(&ctx, &catalogs, "ice.sales.v3dv").await;
    let before_lineage = scan_lineage_id_rowid_seq(&table).await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v3dv')",
    )
    .await
    .expect("v3 rewrite with live DVs must run")
    .collect()
    .await
    .expect("collect rewrite result");
    let batch = &batches[0];
    let rewritten = call_count(batch, "rewritten_data_files_count");
    let added = call_count(batch, "added_data_files_count");
    let removed = call_count(batch, "removed_delete_files_count");

    let after_table = catalog.load_table(&ident).await.expect("reload");
    let after_vectors = crate::call::count_live_deletion_vectors(&after_table)
        .await
        .expect("count DVs after rewrite");
    let after_ids = live_ids(&ctx, &catalogs, "ice.sales.v3dv").await;
    let after_lineage = scan_lineage_id_rowid_seq(&after_table).await;

    let summary = format!(
        "C-001 before_dvs={before_vectors} live_ids={before_ids:?} \
         rewritten={rewritten} added={added} removed_delete_files_count={removed} \
         after_dvs={after_vectors} after_ids={after_ids:?}"
    );
    assert_eq!(
        before_vectors, 6,
        "fixture must seed one Puffin DV per data file: {summary}"
    );
    assert_eq!(
        before_ids,
        vec![11, 12, 13, 14, 15, 16],
        "fixture live set after DELETE: {summary}"
    );
    assert_eq!(rewritten, 6, "six small files must compact: {summary}");
    assert_eq!(added, 1, "binpack writes one file: {summary}");
    assert_eq!(
        after_ids, before_ids,
        "live rows must survive compaction: {summary}"
    );
    assert_eq!(
        after_lineage, before_lineage,
        "V3-LINEAGE-1 must stay green on a DV fixture: {summary}"
    );
    assert_eq!(
        after_vectors, 0,
        "DVs scoped to rewritten files must drop: {summary}"
    );
    assert_eq!(
        removed, 6,
        "removed_delete_files_count must be the six dropped DVs: {summary}"
    );
}

async fn scan_lineage_id_rowid_seq(table: &iceberg::table::Table) -> Vec<(i64, i64, i64)> {
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
                panic!("id Int32 or Int64");
            };
            rows.push((id, row_ids.value(index), seqs.value(index)));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}
