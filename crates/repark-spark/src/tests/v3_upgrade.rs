use super::super::*;
use super::common::*;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use iceberg::spec::FormatVersion;

async fn seed_v2(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '2')"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
    )
    .await;
}

pub(super) async fn upgrade(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} SET TBLPROPERTIES ('format-version' = '3')"),
    )
    .await;
}

pub(super) async fn refuse(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("`{sql}` must refuse"),
            Err(err) => err.to_string(),
        },
        Err(err) => err.to_string(),
    }
}

pub(super) async fn lineage(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, Option<i64>, Option<i64>)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.{table} ORDER BY id"
        ),
    )
    .await
    .unwrap_or_else(|err| panic!("lineage select on {table}: {err}"))
    .collect()
    .await
    .unwrap_or_else(|err| panic!("lineage collect on {table}: {err}"));
    let schema = batches[0].schema();
    assert_eq!(schema.field(1).name(), "_row_id");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id Int32");
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_row_id Int64");
        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_last_updated_sequence_number Int64");
        for index in 0..batch.num_rows() {
            rows.push((
                ids.value(index),
                (!row_ids.is_null(index)).then(|| row_ids.value(index)),
                (!seqs.is_null(index)).then(|| seqs.value(index)),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn alter_upgrades_v2_to_v3_in_place_with_the_opt_in() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "up").await;
    let before = load_sales_table(&catalogs, "up").await;
    assert_eq!(before.metadata().format_version(), FormatVersion::V2);
    let snapshots_before = before.metadata().snapshots().count();

    upgrade(&ctx, &catalogs, "up").await;

    let after = load_sales_table(&catalogs, "up").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(
        after.metadata().snapshots().count(),
        snapshots_before,
        "the upgrade commits metadata only — Spark adds no snapshot"
    );
    assert_eq!(after.metadata().next_row_id(), 0);
    assert!(
        !after.metadata().properties().contains_key("format-version"),
        "the reserved key must not land in the persisted property map"
    );
    assert_eq!(
        lineage(&ctx, &catalogs, "up").await,
        vec![(1, None, None), (2, None, None), (3, None, None)],
        "pre-upgrade rows carry no lineage until a later v3 commit assigns it"
    );
}

#[tokio::test]
async fn alter_upgrade_refuses_without_the_opt_in() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_v2(&ctx, &catalogs, "up").await;
    let message = refuse(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.up SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    assert!(
        message.contains("repark.sql.allowCreateFormatVersion3")
            && message.contains("format-version"),
        "the refusal must name the conf and the key: {message}"
    );
    assert_eq!(
        load_sales_table(&catalogs, "up")
            .await
            .metadata()
            .format_version(),
        FormatVersion::V2
    );
}

#[tokio::test]
async fn alter_downgrade_and_unsupported_versions_refuse_naming_both_versions() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "dg").await;
    upgrade(&ctx, &catalogs, "dg").await;

    let down = refuse(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.dg SET TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    assert!(
        down.contains("format-version") && down.contains("v3") && down.contains("v2"),
        "downgrade must name the key and both versions: {down}"
    );

    seed_v2(&ctx, &catalogs, "bad").await;
    for (value, needle) in [
        ("1", "v1"),
        ("-1", "v-1"),
        ("0", "v0"),
        ("4", "v1 through v3"),
        ("x", "not an Iceberg format version"),
        ("", "not an Iceberg format version"),
        ("3.0", "not an Iceberg format version"),
        (" 3 ", "not an Iceberg format version"),
    ] {
        let message = refuse(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE ice.sales.bad SET TBLPROPERTIES ('format-version' = '{value}')"),
        )
        .await;
        assert!(
            message.contains("format-version") && message.contains(needle),
            "`{value}` must refuse naming `{needle}`: {message}"
        );
    }
    assert_eq!(
        load_sales_table(&catalogs, "bad")
            .await
            .metadata()
            .format_version(),
        FormatVersion::V2
    );
}

#[tokio::test]
async fn alter_to_the_current_version_is_a_no_op() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "noop").await;
    let uuid_before = load_sales_table(&catalogs, "noop")
        .await
        .metadata()
        .metadata_log()
        .len();
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.noop SET TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    let after = load_sales_table(&catalogs, "noop").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V2);
    assert_eq!(
        after.metadata().metadata_log().len(),
        uuid_before,
        "a same-version request writes no metadata file, as Spark writes none"
    );

    upgrade(&ctx, &catalogs, "noop").await;
    let log_after_upgrade = load_sales_table(&catalogs, "noop")
        .await
        .metadata()
        .metadata_log()
        .len();
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.noop SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    assert_eq!(
        load_sales_table(&catalogs, "noop")
            .await
            .metadata()
            .metadata_log()
            .len(),
        log_after_upgrade
    );
}

#[tokio::test]
async fn alter_carrying_the_upgrade_and_another_key_lands_both() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "both").await;
    let log_before = load_sales_table(&catalogs, "both")
        .await
        .metadata()
        .metadata_log()
        .len();
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.both SET TBLPROPERTIES ('format-version' = '3', 'k' = 'v')",
    )
    .await;
    let after = load_sales_table(&catalogs, "both").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(
        after.metadata().properties().get("k").map(String::as_str),
        Some("v")
    );
    assert!(!after.metadata().properties().contains_key("format-version"));
    assert_eq!(
        after.metadata().metadata_log().len() - log_before,
        1,
        "Spark lands the upgrade and the key in ONE metadata commit"
    );
}

#[tokio::test]
async fn alter_of_another_key_alone_still_leaves_the_version_alone() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "other").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.other SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')",
    )
    .await;
    let after = load_sales_table(&catalogs, "other").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V2);
    assert_eq!(
        after
            .metadata()
            .properties()
            .get("write.delete.mode")
            .map(String::as_str),
        Some("merge-on-read")
    );
}

#[tokio::test]
async fn append_after_an_engine_upgrade_assigns_lineage_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "app").await;
    upgrade(&ctx, &catalogs, "app").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.app VALUES (4, 'd'), (5, 'e')",
    )
    .await;
    let after = load_sales_table(&catalogs, "app").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(after.metadata().next_row_id(), 5);
    assert_eq!(
        lineage(&ctx, &catalogs, "app").await,
        vec![
            (1, Some(2), Some(1)),
            (2, Some(3), Some(1)),
            (3, Some(4), Some(1)),
            (4, Some(0), Some(2)),
            (5, Some(1), Some(2)),
        ]
    );
}

pub(super) async fn seed_mor_four(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    mode: &str,
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg TBLPROPERTIES \
             ('format-version' = '2', 'write.merge.mode' = '{mode}', \
              'write.delete.mode' = '{mode}', 'write.update.mode' = '{mode}')"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"),
    )
    .await;
}

pub(super) fn merge_delete_sql(table: &str, id: i32) -> String {
    format!(
        "MERGE INTO ice.sales.{table} AS t USING (SELECT {id} AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE"
    )
}

pub(super) fn walk_puffin(dir: &std::path::Path, count: &mut usize) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_puffin(&path, count);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("puffin") {
                *count += 1;
            }
        }
    }
}

#[tokio::test]
async fn merge_on_read_delete_after_an_engine_upgrade_writes_a_deletion_vector() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_mor_four(&ctx, &catalogs, "dv", "merge-on-read").await;
    upgrade(&ctx, &catalogs, "dv").await;
    assert_eq!(
        lineage(&ctx, &catalogs, "dv").await,
        vec![
            (1, None, None),
            (2, None, None),
            (3, None, None),
            (4, None, None)
        ]
    );

    run(&ctx, &catalogs, &merge_delete_sql("dv", 2)).await;

    let after = load_sales_table(&catalogs, "dv").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(after.metadata().next_row_id(), 4);
    assert_eq!(
        lineage(&ctx, &catalogs, "dv").await,
        vec![
            (1, Some(0), Some(1)),
            (3, Some(2), Some(1)),
            (4, Some(3), Some(1))
        ]
    );
    assert_eq!(delete_file_count(&catalogs, "dv").await, 1);
    let mut puffins = 0;
    walk_puffin(wh.path(), &mut puffins);
    assert_eq!(puffins, 1, "the v3 MoR delete lands as ONE Puffin DV");
}

#[tokio::test]
async fn copy_on_write_dml_after_an_engine_upgrade_matches_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_mor_four(&ctx, &catalogs, "cow", "copy-on-write").await;
    upgrade(&ctx, &catalogs, "cow").await;

    run(&ctx, &catalogs, "DELETE FROM ice.sales.cow WHERE id = 2").await;
    let after_delete = load_sales_table(&catalogs, "cow").await;
    assert_eq!(after_delete.metadata().next_row_id(), 3);
    assert_eq!(
        lineage(&ctx, &catalogs, "cow").await,
        vec![
            (1, Some(0), Some(2)),
            (3, Some(1), Some(2)),
            (4, Some(2), Some(2))
        ]
    );

    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.cow SET name = 'z' WHERE id = 3",
    )
    .await;
    let after_update = load_sales_table(&catalogs, "cow").await;
    assert_eq!(after_update.metadata().next_row_id(), 6);
    assert_eq!(
        lineage(&ctx, &catalogs, "cow").await,
        vec![
            (1, Some(0), Some(2)),
            (3, Some(1), Some(3)),
            (4, Some(2), Some(2))
        ]
    );
}

#[tokio::test]
async fn rewrite_data_files_after_an_engine_upgrade_assigns_lineage_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rw (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    for index in 1..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.rw VALUES ({index}, 'x')"),
        )
        .await;
    }
    upgrade(&ctx, &catalogs, "rw").await;
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rw')",
    )
    .await;

    let after = load_sales_table(&catalogs, "rw").await;
    assert_eq!(after.metadata().next_row_id(), 6);
    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "rw"]).unwrap();
    assert_eq!(count_planned_data_files(catalog.as_ref(), &ident).await, 1);
    let rows = lineage(&ctx, &catalogs, "rw").await;
    let mut row_ids: Vec<i64> = rows.iter().filter_map(|(_, row_id, _)| *row_id).collect();
    row_ids.sort_unstable();
    assert_eq!(
        row_ids,
        vec![0, 1, 2, 3, 4, 5],
        "the rewrite assigns one distinct row id per rewritten row"
    );
    assert!(
        rows.iter().all(|(_, _, seq)| *seq == Some(7)),
        "every rewritten row carries the rewrite snapshot's sequence number: {rows:?}"
    );
}

#[tokio::test]
async fn register_table_of_an_engine_upgraded_table_reads_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    seed_v2(&ctx, &catalogs, "reg").await;
    upgrade(&ctx, &catalogs, "reg").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.reg VALUES (4, 'd'), (5, 'e')",
    )
    .await;
    let source = load_sales_table(&catalogs, "reg").await;
    let metadata_file = source
        .metadata_location()
        .expect("engine metadata pointer")
        .to_string();

    let fresh = TempDir::new().unwrap();
    let (ctx2, catalogs2) = setup_allow_create_format_version_3(&fresh).await;
    run(
        &ctx2,
        &catalogs2,
        &format!(
            "CALL ice.system.register_table(table => 'sales.adopted', \
             metadata_file => '{metadata_file}')"
        ),
    )
    .await;
    let adopted = load_sales_table(&catalogs2, "adopted").await;
    assert_eq!(adopted.metadata().format_version(), FormatVersion::V3);
    assert_eq!(adopted.metadata().next_row_id(), 5);
    assert_eq!(
        lineage(&ctx2, &catalogs2, "adopted").await,
        vec![
            (1, Some(2), Some(1)),
            (2, Some(3), Some(1)),
            (3, Some(4), Some(1)),
            (4, Some(0), Some(2)),
            (5, Some(1), Some(2)),
        ]
    );
}

async fn create_table_at(
    catalogs: &CatalogRegistry,
    table: &str,
    format_version: FormatVersion,
) -> TableIdent {
    let catalog = catalogs.get("ice").expect("ice catalog");
    let schema = iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            iceberg::spec::NestedField::optional(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            )
            .into(),
            iceberg::spec::NestedField::optional(
                2,
                "name",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
            .into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name(table.to_string())
        .schema(schema)
        .format_version(format_version)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create table at the requested format version");
    TableIdent::from_strs(["sales", table]).unwrap()
}

#[tokio::test]
async fn alter_upgrades_a_v1_table_straight_to_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    let ident = create_table_at(&catalogs, "v1", FormatVersion::V1).await;
    let catalog = catalogs.get("ice").expect("ice catalog");
    assert_eq!(
        catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .format_version(),
        FormatVersion::V1
    );

    upgrade(&ctx, &catalogs, "v1").await;

    let after = catalog.load_table(&ident).await.unwrap();
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(after.metadata().next_row_id(), 0);
    assert_eq!(
        after.metadata().snapshots().count(),
        0,
        "the upgrade commits metadata only"
    );

    let mut without = TempDir::new().unwrap();
    let (ctx2, catalogs2) = setup(&without).await;
    let ident2 = create_table_at(&catalogs2, "v1", FormatVersion::V1).await;
    let message = refuse(
        &ctx2,
        &catalogs2,
        "ALTER TABLE ice.sales.v1 SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    assert!(
        message.contains("repark.sql.allowCreateFormatVersion3"),
        "v1 to v3 is behind the same opt-in: {message}"
    );
    assert_eq!(
        catalogs2
            .get("ice")
            .expect("ice catalog")
            .load_table(&ident2)
            .await
            .unwrap()
            .metadata()
            .format_version(),
        FormatVersion::V1
    );
    without.disable_cleanup(false);
}

#[tokio::test]
async fn partitioned_table_upgrade_and_append_match_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.part (id INT, name STRING, part INT) USING iceberg \
         PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.part VALUES (1, 'a', 0), (2, 'b', 0), (3, 'c', 1)",
    )
    .await;

    upgrade(&ctx, &catalogs, "part").await;

    let after = load_sales_table(&catalogs, "part").await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(after.metadata().next_row_id(), 0);
    assert_eq!(
        lineage(&ctx, &catalogs, "part").await,
        vec![(1, None, None), (2, None, None), (3, None, None)]
    );

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.part VALUES (4, 'd', 0), (5, 'e', 1)",
    )
    .await;
    let appended = load_sales_table(&catalogs, "part").await;
    assert_eq!(appended.metadata().next_row_id(), 5);
    assert_eq!(
        lineage(&ctx, &catalogs, "part").await,
        vec![
            (1, Some(2), Some(1)),
            (2, Some(3), Some(1)),
            (3, Some(4), Some(1)),
            (4, Some(0), Some(2)),
            (5, Some(1), Some(2))
        ],
        "the fork's FanoutWriter drains ascending (F-20), so a partitioned plain INSERT INTO \
         takes Spark's exact id -> _row_id map on the two-value identity-int set"
    );
}
