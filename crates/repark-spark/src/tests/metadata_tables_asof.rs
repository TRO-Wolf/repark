use super::super::*;
use super::common::*;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn metadata_asof_served_per_type() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ddl = "CREATE TABLE ice.sales.a (id INT, name STRING) USING iceberg \
        TBLPROPERTIES ('write.delete.mode'='merge-on-read')";
    run(&ctx, &catalogs, ddl).await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.a SELECT * FROM src").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "a".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.a SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table.metadata().current_snapshot_id().expect("s2");
    let s2_ts = table.metadata().snapshot_by_id(s2).unwrap().timestamp_ms();
    run(&ctx, &catalogs, "DELETE FROM ice.sales.a WHERE id = 1").await;
    let tx = Transaction::new(&table);
    let action = tx
        .manage_snapshots()
        .create_tag("tag_s1", s1)
        .create_branch("branch_s2", s2);
    let tx = action.apply(tx).expect("apply create ref");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit refs");
    let s2_text = chrono::DateTime::from_timestamp_millis(s2_ts)
        .expect("s2 commit time")
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string();
    for (suffix, expected) in [
        ("files", 1),
        ("data_files", 1),
        ("delete_files", 0),
        ("entries", 1),
        ("manifests", 1),
        ("partitions", 1),
        ("position_deletes", 0),
    ] {
        let sql = format!("SELECT count(*) FROM ice.sales.a.{suffix} VERSION AS OF {s1}");
        assert_eq!(
            asof_scalar(&ctx, &catalogs, &sql).await,
            expected,
            "{suffix}"
        );
    }
    let sum_sql = format!("SELECT sum(record_count) FROM ice.sales.a.files VERSION AS OF {s1}");
    assert_eq!(asof_scalar(&ctx, &catalogs, &sum_sql).await, 3);
    let sql = format!("SELECT count(*) FROM ice.sales.a.files FOR SYSTEM_VERSION AS OF {s1}");
    assert_eq!(asof_scalar(&ctx, &catalogs, &sql).await, 1);
    for (suffix, floor) in [("files", 1), ("delete_files", 0), ("position_deletes", 0)] {
        let sql = format!("SELECT count(*) FROM ice.sales.a.{suffix}");
        assert!(asof_scalar(&ctx, &catalogs, &sql).await > floor, "{suffix}");
    }
    for (suffix, columns, order) in [
        (
            "snapshots",
            "snapshot_id, parent_id, operation",
            "snapshot_id",
        ),
        (
            "history",
            "snapshot_id, parent_id, is_current_ancestor",
            "snapshot_id",
        ),
        ("refs", "name, type, snapshot_id", "name"),
        (
            "metadata_log_entries",
            "file, latest_sequence_number",
            "latest_sequence_number",
        ),
    ] {
        let plain = format!("SELECT {columns} FROM ice.sales.a.{suffix} ORDER BY {order}");
        let pinned = format!(
            "SELECT {columns} FROM ice.sales.a.{suffix} VERSION AS OF {s1} ORDER BY {order}"
        );
        let plain_rows = asof_rendered(&ctx, &catalogs, &plain).await;
        let pinned_rows = asof_rendered(&ctx, &catalogs, &pinned).await;
        assert_eq!(pinned_rows, plain_rows, "{suffix}");
    }
    for (version, expected) in [("tag_s1", 1), ("branch_s2", 2)] {
        let sql = format!("SELECT count(*) FROM ice.sales.a.files VERSION AS OF '{version}'");
        assert_eq!(
            asof_scalar(&ctx, &catalogs, &sql).await,
            expected,
            "{version}"
        );
    }
    let ts_sql = format!("SELECT count(*) FROM ice.sales.a.files TIMESTAMP AS OF '{s2_text}'");
    assert_eq!(asof_scalar(&ctx, &catalogs, &ts_sql).await, 2);
    for suffix in [
        "files",
        "data_files",
        "delete_files",
        "entries",
        "manifests",
        "partitions",
        "position_deletes",
    ] {
        let sql = format!("SELECT count(*) FROM ice.sales.a.{suffix} VERSION AS OF 999");
        assert_eq!(asof_scalar(&ctx, &catalogs, &sql).await, 0, "{suffix}");
    }
    let empty_fields: Vec<(String, DataType, bool)> = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.a.files VERSION AS OF 999 LIMIT 0",
    )
    .await
    .unwrap()
    .schema()
    .fields()
    .iter()
    .map(|field| {
        (
            field.name().clone(),
            field.data_type().clone(),
            field.is_nullable(),
        )
    })
    .collect();
    let full_fields: Vec<(String, DataType, bool)> =
        execute(&ctx, &catalogs, "SELECT * FROM ice.sales.a.files LIMIT 0")
            .await
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
    assert_eq!(empty_fields, full_fields);
    let mapped = repark_core::engine_err(
        execute(
            &ctx,
            &catalogs,
            "SELECT count(*) FROM ice.sales.a.files VERSION AS OF 'nope'",
        )
        .await
        .expect_err("unknown ref must fail"),
    );
    assert!(
        matches!(mapped, repark_common::Error::IllegalArgument(ref message)
            if message.as_str()
                == "Cannot find matching snapshot ID or reference name for version nope"),
        "got: {mapped}"
    );
    let mapped = repark_core::engine_err(
        execute(
            &ctx,
            &catalogs,
            "SELECT count(*) FROM ice.sales.a.snapshots VERSION AS OF 'nope'",
        )
        .await
        .expect_err("unknown ref on a rule-1 type must fail"),
    );
    assert!(
        matches!(mapped, repark_common::Error::IllegalArgument(ref message)
            if message.as_str()
                == "Cannot find matching snapshot ID or reference name for version nope"),
        "got: {mapped}"
    );
    let mapped = repark_core::engine_err(
        execute(
            &ctx,
            &catalogs,
            "SELECT count(*) FROM ice.sales.a.files TIMESTAMP AS OF '2000-01-01 00:00:00'",
        )
        .await
        .expect_err("old ts must fail"),
    );
    assert!(
        matches!(mapped, repark_common::Error::IllegalArgument(ref message)
            if message.as_str() == "Cannot find a snapshot older than 2000-01-01T00:00:00+00:00"),
        "got: {mapped}"
    );
    let mapped = repark_core::engine_err(
        execute(
            &ctx,
            &catalogs,
            "SELECT count(*) FROM ice.sales.a.snapshots VERSION AS OF 999",
        )
        .await
        .expect_err("unknown id on a rule-1 type must fail loud"),
    );
    assert!(
        matches!(mapped, repark_common::Error::IllegalArgument(_)),
        "got: {mapped:?}"
    );
    assert_eq!(mapped.to_string(), "Cannot find snapshot with ID 999");
    for (suffix, upper) in [
        ("all_manifests", "ALL_MANIFESTS"),
        ("all_files", "ALL_FILES"),
        ("all_data_files", "ALL_DATA_FILES"),
        ("all_delete_files", "ALL_DELETE_FILES"),
        ("all_entries", "ALL_ENTRIES"),
    ] {
        let sql = format!("SELECT count(*) FROM ice.sales.a.{suffix} VERSION AS OF 1");
        let mapped = repark_core::engine_err(
            execute(&ctx, &catalogs, &sql)
                .await
                .expect_err("all_* must refuse"),
        );
        assert!(
            matches!(mapped, repark_common::Error::Analysis(_)),
            "{suffix} mapped class, got: {mapped:?}"
        );
        let err = mapped.to_string();
        assert!(
            err.contains(&format!("Cannot select snapshot in table: {upper}")),
            "got: {err}"
        );
    }
}

#[tokio::test]
async fn reader_metadata_path_matches_sql_door() {
    use repark_core::time_travel::{TimeTravelSpec, read_table_at};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m (id INT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.m SELECT 1 AS id").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "m".into());
    let first = catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("first snapshot");
    run(&ctx, &catalogs, "INSERT INTO ice.sales.m SELECT 2 AS id").await;
    let zone = repark_core::SessionTimeZone::default();
    let parts = ["ice", "sales", "m", "files"]
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let spec = TimeTravelSpec::SnapshotId(first);
    let reader = read_table_at(&ctx, &catalogs, &parts, &spec, &zone)
        .await
        .unwrap();
    let sql = execute(
        &ctx,
        &catalogs,
        &format!("SELECT * FROM ice.sales.m.files VERSION AS OF {first}"),
    )
    .await
    .unwrap();
    let reader_names: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let sql_names: Vec<String> = sql
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(reader_names, sql_names);
    let reader_batches = reader.collect().await.unwrap();
    let sql_batches = sql.collect().await.unwrap();
    let reader_rows: usize = reader_batches.iter().map(|batch| batch.num_rows()).sum();
    let sql_rows: usize = sql_batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(reader_rows, 1);
    assert_eq!(sql_rows, 1);
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&reader_batches).unwrap();
    let expected = datafusion::arrow::util::pretty::pretty_format_batches(&sql_batches).unwrap();
    assert_eq!(rendered.to_string(), expected.to_string());
    let missing = TimeTravelSpec::SnapshotId(999);
    let reader = read_table_at(&ctx, &catalogs, &parts, &missing, &zone)
        .await
        .unwrap();
    let sql = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.m.files VERSION AS OF 999",
    )
    .await
    .unwrap();
    let reader_names: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let sql_names: Vec<String> = sql
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(reader_names, sql_names);
    let reader_batches = reader.collect().await.unwrap();
    let sql_batches = sql.collect().await.unwrap();
    let reader_rows: usize = reader_batches.iter().map(|batch| batch.num_rows()).sum();
    let sql_rows: usize = sql_batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(reader_rows, 0);
    assert_eq!(sql_rows, 0);
}
