//! pins: v3-5-dv-compaction/C-001, C-002, C-003, C-004
//! pins: b-mor-3-rewrite-position-deletes-v3/C-002, C-003
//! Model: Grok 4.6
//! V3-5: v3 `rewrite_data_files` drops in-scope Puffin DVs and reports the true count.
//! MUTATION: `removed_delete_files_count == 0` → this REDs.

use super::super::*;
use super::call::call_count;
use super::common::*;

use iceberg::spec::FormatVersion;

fn assert_rewrite_count_columns_are_int32(batch: &RecordBatch) {
    let schema = batch.schema();
    for name in [
        "rewritten_data_files_count",
        "added_data_files_count",
        "failed_data_files_count",
        "removed_delete_files_count",
    ] {
        let index = schema.index_of(name).expect(name);
        let field = schema.field(index);
        assert_eq!(
            field.data_type(),
            &DataType::Int32,
            "{name} must be Arrow Int32 (C-004)"
        );
        assert!(!field.is_nullable(), "{name} is non-nullable");
    }
    let bytes_index = schema
        .index_of("rewritten_bytes_count")
        .expect("rewritten_bytes_count");
    let bytes = schema.field(bytes_index);
    assert_eq!(bytes.data_type(), &DataType::Int64);
    assert!(!bytes.is_nullable());
}

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

async fn delete_file_formats(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(String, i64)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT file_format, record_count FROM {table}.delete_files"),
    )
    .await
    .expect("select delete_files")
    .collect()
    .await
    .expect("collect delete_files");
    let mut rows = Vec::new();
    for batch in &batches {
        let formats = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file_format Utf8");
        let counts = batch.column(1);
        for index in 0..batch.num_rows() {
            let count = counts
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|array| array.value(index))
                .or_else(|| {
                    counts
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .map(|array| i64::from(array.value(index)))
                })
                .expect("record_count Int32 or Int64");
            rows.push((formats.value(index).to_ascii_uppercase(), count));
        }
    }
    rows.sort();
    rows
}

fn assert_four_zeros(batch: &RecordBatch) {
    assert_eq!(call_count(batch, "rewritten_delete_files_count"), 0);
    assert_eq!(call_count(batch, "added_delete_files_count"), 0);
    assert_eq!(call_count(batch, "rewritten_bytes_count"), 0);
    assert_eq!(call_count(batch, "added_bytes_count"), 0);
}

#[tokio::test]
async fn call_rewrite_position_delete_files_on_engine_written_v3_dvs_returns_zeros() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_six_file_v3_mor_with_one_dv_per_file(&ctx, &catalogs).await;
    let ident = TableIdent::from_strs(["sales", "v3dv"]).unwrap();
    let catalog = catalogs.get("ice").expect("ice catalog");
    let table = catalog.load_table(&ident).await.expect("load");
    let before_vectors = live_deletion_vector_count(&table).await;
    let before_ids = live_ids(&ctx, &catalogs, "ice.sales.v3dv").await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.v3dv')",
    )
    .await
    .expect("DV-only rewrite returns zeros")
    .collect()
    .await
    .expect("collect");
    assert_four_zeros(&batches[0]);
    let after = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(live_deletion_vector_count(&after).await, before_vectors);
    assert_eq!(before_vectors, 6);
    assert_eq!(
        live_ids(&ctx, &catalogs, "ice.sales.v3dv").await,
        before_ids
    );
    let second = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.v3dv')",
    )
    .await
    .expect("second run")
    .collect()
    .await
    .expect("collect second");
    assert_four_zeros(&second[0]);
}

async fn seed_upgraded_file_scoped_parquet_deletes(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    files: i32,
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', \
             'write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', \
             'write.delete.granularity' = 'file')"
        ),
    )
    .await;
    for ident in 1..=files {
        run(
            ctx,
            catalogs,
            &format!(
                "INSERT INTO ice.sales.{table} VALUES ({ident}, 'a'), ({}, 'b')",
                ident + 100
            ),
        )
        .await;
    }
    for ident in 1..=files {
        run(
            ctx,
            catalogs,
            &format!("DELETE FROM ice.sales.{table} WHERE id = {ident}"),
        )
        .await;
    }
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} SET TBLPROPERTIES ('format-version' = '3')"),
    )
    .await;
}

#[tokio::test]
async fn call_rewrite_position_delete_files_converts_five_upgraded_parquet_deletes_to_puffin() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_upgraded_file_scoped_parquet_deletes(&ctx, &catalogs, "cellb", 5).await;
    let before = delete_file_formats(&ctx, &catalogs, "ice.sales.cellb").await;
    assert_eq!(before.len(), 5);
    assert!(before.iter().all(|row| row.0 == "PARQUET" && row.1 == 1));
    let before_ids = live_ids(&ctx, &catalogs, "ice.sales.cellb").await;
    let ident = TableIdent::from_strs(["sales", "cellb"]).unwrap();
    let catalog = catalogs.get("ice").expect("ice catalog");
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.cellb')",
    )
    .await
    .expect("admitted parquet-to-DV rewrite")
    .collect()
    .await
    .expect("collect");
    let batch = &batches[0];
    assert_eq!(call_count(batch, "rewritten_delete_files_count"), 5);
    assert_eq!(call_count(batch, "added_delete_files_count"), 5);
    assert!(call_count(batch, "rewritten_bytes_count") > 0);
    assert!(call_count(batch, "added_bytes_count") > 0);
    let after = delete_file_formats(&ctx, &catalogs, "ice.sales.cellb").await;
    assert_eq!(after.len(), 5);
    assert!(after.iter().all(|row| row.0 == "PUFFIN" && row.1 == 1));
    assert_eq!(
        live_ids(&ctx, &catalogs, "ice.sales.cellb").await,
        before_ids
    );
    assert_eq!(before_ids, vec![101, 102, 103, 104, 105]);
    let after_table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(after_table.metadata().next_row_id(), 10);
    let second = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.cellb')",
    )
    .await
    .expect("second run")
    .collect()
    .await
    .expect("collect second");
    assert_four_zeros(&second[0]);
}

#[tokio::test]
async fn call_rewrite_position_delete_files_converts_two_upgraded_parquet_deletes_below_spark_floor()
 {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_upgraded_file_scoped_parquet_deletes(&ctx, &catalogs, "cellb2", 2).await;
    let before = delete_file_formats(&ctx, &catalogs, "ice.sales.cellb2").await;
    assert_eq!(before, vec![("PARQUET".into(), 1), ("PARQUET".into(), 1)]);
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.cellb2')",
    )
    .await
    .expect("fork v3 arm converts below Spark min-input-files")
    .collect()
    .await
    .expect("collect");
    let batch = &batches[0];
    assert_eq!(call_count(batch, "rewritten_delete_files_count"), 2);
    assert_eq!(call_count(batch, "added_delete_files_count"), 2);
    let after = delete_file_formats(&ctx, &catalogs, "ice.sales.cellb2").await;
    assert_eq!(after, vec![("PUFFIN".into(), 1), ("PUFFIN".into(), 1)]);
    assert_eq!(
        live_ids(&ctx, &catalogs, "ice.sales.cellb2").await,
        vec![101, 102]
    );
}

#[tokio::test]
async fn call_rewrite_position_delete_files_converts_mixed_remaining_parquet_below_spark_floor() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed_upgraded_file_scoped_parquet_deletes(&ctx, &catalogs, "cellc", 2).await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.cellc WHERE id = 102",
    )
    .await;
    let mixed = delete_file_formats(&ctx, &catalogs, "ice.sales.cellc").await;
    assert!(
        mixed.iter().any(|row| row.0 == "PARQUET") && mixed.iter().any(|row| row.0 == "PUFFIN"),
        "mixed parquet + DV before CALL: {mixed:?}"
    );
    let before_ids = live_ids(&ctx, &catalogs, "ice.sales.cellc").await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.cellc')",
    )
    .await
    .expect("fork converts remaining parquet")
    .collect()
    .await
    .expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 1);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 1);
    let after = delete_file_formats(&ctx, &catalogs, "ice.sales.cellc").await;
    assert!(after.iter().all(|row| row.0 == "PUFFIN"), "{after:?}");
    assert_eq!(after.len(), 2);
    assert_eq!(
        live_ids(&ctx, &catalogs, "ice.sales.cellc").await,
        before_ids
    );
}

#[tokio::test]
async fn call_rewrite_position_delete_files_splits_partition_parquet_below_spark_floor() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.celld (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', \
         'write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', \
         'write.delete.granularity' = 'partition')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.celld VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.celld VALUES (3, 'c'), (4, 'd')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.celld WHERE id IN (1, 3)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.celld SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    let before = delete_file_formats(&ctx, &catalogs, "ice.sales.celld").await;
    assert_eq!(before, vec![("PARQUET".into(), 2)]);
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.celld')",
    )
    .await
    .expect("fork splits partition parquet into one DV per data file")
    .collect()
    .await
    .expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 1);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 2);
    let after = delete_file_formats(&ctx, &catalogs, "ice.sales.celld").await;
    assert_eq!(after, vec![("PUFFIN".into(), 1), ("PUFFIN".into(), 1)]);
    assert_eq!(
        live_ids(&ctx, &catalogs, "ice.sales.celld").await,
        vec![2, 4]
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
    let before_vectors = live_deletion_vector_count(&table).await;
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
    assert_rewrite_count_columns_are_int32(batch);
    let rewritten = call_count(batch, "rewritten_data_files_count");
    let added = call_count(batch, "added_data_files_count");
    let removed = call_count(batch, "removed_delete_files_count");

    let after_table = catalog.load_table(&ident).await.expect("reload");
    let after_vectors = live_deletion_vector_count(&after_table).await;
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
