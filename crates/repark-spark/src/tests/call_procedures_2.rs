use super::super::*;
use super::call::call_count;
use super::common::*;

fn column_names(batch: &datafusion::arrow::array::RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn plan_message(error: datafusion::error::DataFusionError) -> String {
    let datafusion::error::DataFusionError::Plan(message) = error else {
        panic!("expected a Plan error, got {error}");
    };
    message
}

fn external_message(error: datafusion::error::DataFusionError) -> String {
    let datafusion::error::DataFusionError::External(source) = error else {
        panic!("expected an External error, got {error}");
    };
    source.to_string()
}

fn assert_duplicate_refusal(error: datafusion::error::DataFusionError) {
    let message = external_message(error);
    assert!(
        message.contains(
            "Cannot complete import because data files to be imported already exist within the \
             target table"
        ),
        "duplicate refusal must carry the shared message body, got {message}"
    );
    assert!(
        message.contains("you may set 'check_duplicate_files' to false to force the import"),
        "duplicate refusal must carry the shared force-import tail, got {message}"
    );
}

async fn seed_partitioned_mor_with_deletes(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, cat STRING) USING iceberg PARTITIONED BY \
             (cat) TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', \
             'write.merge.mode' = 'merge-on-read')"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'x'), (2, 'x')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (3, 'y'), (4, 'y')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 1"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 3"),
    )
    .await;
}

#[tokio::test]
async fn call_rpd_where_restricts_to_matching_partition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_partitioned_mor_with_deletes(&ctx, &catalogs, "rwf").await;
    seed_partitioned_mor_with_deletes(&ctx, &catalogs, "rwu").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.rwf', where => 'cat \
         = \"x\"', options => map('rewrite-all', 'true'))",
    )
    .await
    .expect("filtered rewrite must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        column_names(&batches[0]),
        vec![
            "rewritten_delete_files_count",
            "added_delete_files_count",
            "rewritten_bytes_count",
            "added_bytes_count",
        ]
    );
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 1);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 1);
    assert!(call_count(&batches[0], "rewritten_bytes_count") > 0);
    assert!(call_count(&batches[0], "added_bytes_count") > 0);
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.rwu', options => \
         map('rewrite-all', 'true'))",
    )
    .await
    .expect("unfiltered rewrite must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 2);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 2);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.rwf").await,
        vec![2, 4]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.rwu").await,
        vec![2, 4]
    );
}

async fn snapshot_ids_ordered(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<i64> {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "SELECT snapshot_id FROM ice.sales.{table}.snapshots ORDER BY committed_at, snapshot_id"
        ),
    )
    .await
    .expect("read snapshot log")
    .collect()
    .await
    .expect("collect snapshot log");
    let mut ids = Vec::new();
    for batch in &batches {
        let values = batch
            .column_by_name("snapshot_id")
            .expect("snapshot_id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id is bigint");
        for index in 0..values.len() {
            ids.push(values.value(index));
        }
    }
    ids
}

fn expire_row(batch: &datafusion::arrow::array::RecordBatch) -> Vec<i64> {
    vec![
        call_count(batch, "deleted_data_files_count"),
        call_count(batch, "deleted_position_delete_files_count"),
        call_count(batch, "deleted_equality_delete_files_count"),
        call_count(batch, "deleted_manifest_files_count"),
        call_count(batch, "deleted_manifest_lists_count"),
        call_count(batch, "deleted_statistics_files_count"),
    ]
}

fn expire_columns() -> Vec<String> {
    vec![
        "deleted_data_files_count".to_string(),
        "deleted_position_delete_files_count".to_string(),
        "deleted_equality_delete_files_count".to_string(),
        "deleted_manifest_files_count".to_string(),
        "deleted_manifest_lists_count".to_string(),
        "deleted_statistics_files_count".to_string(),
    ]
}

async fn seed_four_snapshots(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg"),
    )
    .await;
    for index in 1..=3 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({index}, 'v{index}')"),
        )
        .await;
    }
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 1"),
    )
    .await;
}

#[tokio::test]
async fn call_expire_snapshot_ids_expires_exactly_those() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_four_snapshots(&ctx, &catalogs, "exn").await;
    seed_four_snapshots(&ctx, &catalogs, "exp").await;
    let named_ids = snapshot_ids_ordered(&ctx, &catalogs, "exn").await;
    assert_eq!(named_ids.len(), 4);
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(table => 'sales.exn', snapshot_ids => array({}, {}))",
            named_ids[0], named_ids[1]
        ),
    )
    .await
    .expect("snapshot_ids must expire");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), expire_columns());
    assert_eq!(expire_row(&batches[0]), vec![0, 0, 0, 0, 2, 0]);
    let remaining = snapshot_ids_ordered(&ctx, &catalogs, "exn").await;
    assert_eq!(remaining, named_ids[2..]);
    let positional_ids = snapshot_ids_ordered(&ctx, &catalogs, "exp").await;
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots('sales.exp', NULL, NULL, NULL, NULL, array({}, {}))",
            positional_ids[0], positional_ids[1]
        ),
    )
    .await
    .expect("positional snapshot_ids must bind in declared order");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(expire_row(&batches[0]), vec![0, 0, 0, 0, 2, 0]);
    let remaining = snapshot_ids_ordered(&ctx, &catalogs, "exp").await;
    assert_eq!(remaining, positional_ids[2..]);
}

#[tokio::test]
async fn call_expire_accept_and_ignore_trio_equals_plain() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_four_snapshots(&ctx, &catalogs, "ex0").await;
    seed_four_snapshots(&ctx, &catalogs, "ex1").await;
    seed_four_snapshots(&ctx, &catalogs, "ex2").await;
    seed_four_snapshots(&ctx, &catalogs, "ex3").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.expire_snapshots(table => 'sales.ex0', older_than => TIMESTAMP '2999-01-01 00:00:00')",
    )
    .await
    .expect("plain expiry must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), expire_columns());
    let plain = expire_row(&batches[0]);
    for (table, argument) in [
        ("ex1", "stream_results => true"),
        ("ex2", "max_concurrent_deletes => 2"),
        ("ex3", "clean_expired_metadata => true"),
    ] {
        let frame = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.expire_snapshots(table => 'sales.{table}', older_than => \
                 TIMESTAMP '2999-01-01 00:00:00', {argument})"
            ),
        )
        .await
        .expect("ignored argument must be accepted");
        let batches = frame.collect().await.expect("collect");
        assert_eq!(column_names(&batches[0]), expire_columns());
        assert_eq!(expire_row(&batches[0]), plain);
        assert_eq!(snapshot_ids_ordered(&ctx, &catalogs, table).await.len(), 1);
    }
    assert_eq!(snapshot_ids_ordered(&ctx, &catalogs, "ex0").await.len(), 1);
}

#[tokio::test]
async fn call_expire_ignored_args_type_check() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ext AS SELECT * FROM src",
    )
    .await;
    for (argument, needle) in [
        ("stream_results => 1", "must be a boolean literal"),
        (
            "clean_expired_metadata => 'yes'",
            "must be a boolean literal",
        ),
        ("max_concurrent_deletes => 'many'", "is not an integer"),
        ("snapshot_ids => 1", "must be an array of integer literals"),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.expire_snapshots(table => 'sales.ext', {argument})"),
        )
        .await
        .expect_err("mistyped argument must refuse");
        assert!(
            plan_message(error).contains(needle),
            "mistyped {argument} must name its type"
        );
    }
}

#[tokio::test]
async fn call_rm_sort_by_stays_a_loud_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.x', sort_by => array('id'))",
    )
    .await
    .expect_err("sort_by must refuse");
    assert_eq!(
        plan_message(error),
        "unknown CALL argument `sort_by`; allowed: table, use_caching, spec_id"
    );
}

fn add_files_columns() -> Vec<String> {
    vec![
        "added_files_count".to_string(),
        "changed_partition_count".to_string(),
    ]
}

fn added_files_count(batch: &datafusion::arrow::array::RecordBatch) -> i64 {
    call_count(batch, "added_files_count")
}

fn changed_partition_count_is_null(batch: &datafusion::arrow::array::RecordBatch) -> bool {
    batch
        .column_by_name("changed_partition_count")
        .expect("changed column")
        .is_null(0)
}

async fn copy_source_files(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    select: &str,
    dest: &std::path::Path,
) -> Vec<std::path::PathBuf> {
    run(
        ctx,
        catalogs,
        &format!("COPY ({select}) TO '{}' STORED AS PARQUET", dest.display()),
    )
    .await;
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dest)
        .expect("list staged copy")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    files.sort();
    assert_eq!(files.len(), 1, "one parquet file per staged copy");
    files
}

fn move_into_partition_dir(
    files: Vec<std::path::PathBuf>,
    root: &std::path::Path,
    partition: &str,
) {
    let dir = root.join(partition);
    std::fs::create_dir_all(&dir).expect("create partition dir");
    for file in files {
        let name = file.file_name().expect("staged file name");
        std::fs::rename(&file, dir.join(name)).expect("move into partition dir");
    }
}

async fn name_mapping_property(
    catalogs: &CatalogRegistry,
    namespace: &str,
    table: &str,
) -> Option<String> {
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table.to_string(),
    );
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    loaded
        .metadata()
        .properties()
        .get("schema.name-mapping.default")
        .cloned()
}

fn expected_name_mapping_json() -> String {
    "[ {\n  \"field-id\" : 1,\n  \"names\" : [ \"id\" ]\n}, {\n  \"field-id\" : 2,\n  \"names\" : [ \"data\" ]\n}, {\n  \"field-id\" : 3,\n  \"names\" : [ \"cat\" ]\n} ]".to_string()
}

async fn live_file_paths(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<String> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT file_path FROM ice.sales.{table}.all_files"),
    )
    .await
    .expect("read all_files")
    .collect()
    .await
    .expect("collect all_files");
    let mut paths = Vec::new();
    for batch in &batches {
        let values = batch
            .column_by_name("file_path")
            .expect("file_path column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file_path is utf8");
        for index in 0..values.len() {
            paths.push(values.value(index).to_string());
        }
    }
    paths.sort();
    paths
}

#[tokio::test]
async fn call_add_files_partitioned_imports_two_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let root = wh.path().join("addsrc").join("parted");
    let stage_x = wh.path().join("stage_x");
    let stage_y = wh.path().join("stage_y");
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT id, name AS data FROM src WHERE id <= 2",
            &stage_x,
        )
        .await,
        &root,
        "cat=x",
    );
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT id, name AS data FROM src WHERE id = 3",
            &stage_y,
        )
        .await,
        &root,
        "cat=y",
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.afx (id INT, data STRING, cat STRING) USING iceberg \
         PARTITIONED BY (cat)",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afx', source_table => '`parquet`.`{}`')",
            root.display()
        ),
    )
    .await
    .expect("partitioned import must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), add_files_columns());
    assert_eq!(added_files_count(&batches[0]), 2);
    assert!(changed_partition_count_is_null(&batches[0]));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.afx").await,
        vec![1, 2, 3]
    );
    assert_eq!(
        name_mapping_property(&catalogs, "sales", "afx").await,
        Some(expected_name_mapping_json())
    );
    let paths = live_file_paths(&ctx, &catalogs, "afx").await;
    assert_eq!(paths.len(), 2);
    let root_str = root.display().to_string();
    for path in &paths {
        assert!(
            path.starts_with(root_str.as_str()),
            "imported file stays under the source directory, got {path}"
        );
    }
}

#[tokio::test]
async fn call_add_files_unpartitioned_imports_one_file() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let root = wh.path().join("addsrc").join("flat");
    copy_source_files(
        &ctx,
        &catalogs,
        "SELECT id, name AS data, 'w' AS cat FROM src",
        &root,
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.afu (id INT, data STRING, cat STRING) USING iceberg",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afu', source_table => '`parquet`.`{}`')",
            root.display()
        ),
    )
    .await
    .expect("flat import must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), add_files_columns());
    assert_eq!(added_files_count(&batches[0]), 1);
    assert!(changed_partition_count_is_null(&batches[0]));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.afu").await,
        vec![1, 2, 3]
    );
    assert_eq!(
        name_mapping_property(&catalogs, "sales", "afu").await,
        Some(expected_name_mapping_json())
    );
}

#[tokio::test]
async fn call_add_files_partition_filter_restricts() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let root = wh.path().join("addsrc").join("filtered");
    let stage_x = wh.path().join("stage_fx");
    let stage_y = wh.path().join("stage_fy");
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT id, name AS data FROM src WHERE id <= 2",
            &stage_x,
        )
        .await,
        &root,
        "cat=x",
    );
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT id, name AS data FROM src WHERE id = 3",
            &stage_y,
        )
        .await,
        &root,
        "cat=y",
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.aff (id INT, data STRING, cat STRING) USING iceberg \
         PARTITIONED BY (cat)",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.aff', source_table => '`parquet`.`{}`', \
             partition_filter => map('cat', 'x'))",
            root.display()
        ),
    )
    .await
    .expect("filtered import must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(added_files_count(&batches[0]), 1);
    assert!(changed_partition_count_is_null(&batches[0]));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.aff").await,
        vec![1, 2]
    );
    assert_eq!(
        name_mapping_property(&catalogs, "sales", "aff").await,
        Some(expected_name_mapping_json())
    );
}

#[tokio::test]
async fn call_add_files_parallelism_matches_serial() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let root = wh.path().join("addsrc").join("par");
    let stage_x = wh.path().join("stage_px");
    let stage_y = wh.path().join("stage_py");
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT id, name AS data FROM src WHERE id <= 2",
            &stage_x,
        )
        .await,
        &root,
        "cat=x",
    );
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT id, name AS data FROM src WHERE id = 3",
            &stage_y,
        )
        .await,
        &root,
        "cat=y",
    );
    for table in ["afp1", "afp2"] {
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id INT, data STRING, cat STRING) USING iceberg \
                 PARTITIONED BY (cat)"
            ),
        )
        .await;
    }
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afp1', source_table => '`parquet`.`{}`')",
            root.display()
        ),
    )
    .await
    .expect("serial import must run");
    let serial = frame.collect().await.expect("collect");
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afp2', source_table => '`parquet`.`{}`', \
             parallelism => 2)",
            root.display()
        ),
    )
    .await
    .expect("parallel import must run");
    let parallel = frame.collect().await.expect("collect");
    assert_eq!(added_files_count(&serial[0]), 2);
    assert_eq!(added_files_count(&parallel[0]), 2);
    assert!(changed_partition_count_is_null(&parallel[0]));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.afp1").await,
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.afp2").await
    );
}

#[tokio::test]
async fn call_add_files_check_duplicate_files_raises() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let root = wh.path().join("addsrc").join("dup");
    copy_source_files(
        &ctx,
        &catalogs,
        "SELECT id, name AS data, 'w' AS cat FROM src",
        &root,
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.afd (id INT, data STRING, cat STRING) USING iceberg",
    )
    .await;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afd', source_table => '`parquet`.`{}`')",
            root.display()
        ),
    )
    .await
    .expect("first import must run");
    let error = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afd', source_table => '`parquet`.`{}`', \
             check_duplicate_files => true)",
            root.display()
        ),
    )
    .await
    .expect_err("duplicate import must raise");
    assert_duplicate_refusal(error);
    let error = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afd', source_table => '`parquet`.`{}`')",
            root.display()
        ),
    )
    .await
    .expect_err("duplicate import with the check omitted must raise");
    assert_duplicate_refusal(error);
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afd', source_table => '`parquet`.`{}`', \
             check_duplicate_files => false)",
            root.display()
        ),
    )
    .await
    .expect("forced import must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(added_files_count(&batches[0]), 1);
}

#[tokio::test]
async fn call_add_files_commits_name_mapping_and_binds_by_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let root = wh.path().join("addsrc").join("reordered");
    let stage = wh.path().join("stage_reo");
    move_into_partition_dir(
        copy_source_files(
            &ctx,
            &catalogs,
            "SELECT name AS data, id FROM src WHERE id <= 2",
            &stage,
        )
        .await,
        &root,
        "cat=x",
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.afo (id INT, data STRING, cat STRING) USING iceberg \
         PARTITIONED BY (cat)",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afo', source_table => '`parquet`.`{}`')",
            root.display()
        ),
    )
    .await
    .expect("reordered import must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(added_files_count(&batches[0]), 1);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.afo").await,
        vec![1, 2]
    );
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT data FROM ice.sales.afo ORDER BY id",
    )
    .await
    .expect("read imported strings")
    .collect()
    .await
    .expect("collect strings");
    let values = batches[0]
        .column_by_name("data")
        .expect("data column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data is utf8");
    assert_eq!(values.value(0), "a");
    assert_eq!(values.value(1), "b");
    let mapping = name_mapping_property(&catalogs, "sales", "afo").await;
    assert_eq!(mapping, Some(expected_name_mapping_json()));
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.afo', source_table => '`parquet`.`{}`', \
             check_duplicate_files => false)",
            root.display()
        ),
    )
    .await
    .expect("second import must run");
    assert_eq!(
        name_mapping_property(&catalogs, "sales", "afo").await,
        mapping
    );
}

#[tokio::test]
async fn call_add_files_refuses_unsupported_sources() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.afr (id INT, data STRING, cat STRING) USING iceberg",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.add_files(table => 'sales.afr', source_table => '`orc`.`/tmp/x`')",
    )
    .await
    .expect_err("orc source must refuse");
    let datafusion::error::DataFusionError::NotImplemented(message) = error else {
        panic!("expected a NotImplemented refusal, got {error}");
    };
    assert_eq!(
        message,
        "CALL add_files source format `orc` is not supported (only `parquet` directory imports \
         are supported)"
    );
    for source in ["`db`.`table`", "db.table"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.add_files(table => 'sales.afr', source_table => {source})"),
        )
        .await
        .expect_err("catalog source must refuse");
        assert!(
            error
                .to_string()
                .contains("catalog table sources are not supported"),
            "catalog refusal must name the gap, got {error}"
        );
    }
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.add_files(table => 'sales.afr', source_table => 'plain-string')",
    )
    .await
    .expect_err("bare string source must refuse");
    assert_eq!(
        plan_message(error),
        "CALL add_files argument `source_table` must be a parquet directory reference \
         (`parquet`.`<directory>`), got 'plain-string'"
    );
}

#[tokio::test]
async fn call_rpd_where_malformed_refuses_with_parse_text() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.wm AS SELECT * FROM src",
    )
    .await;
    for where_sql in ["id =", "nope = 1"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_position_delete_files(table => 'sales.wm', where => \
                 '{where_sql}')"
            ),
        )
        .await
        .expect_err("malformed where must refuse");
        assert_eq!(
            plan_message(error),
            format!("Cannot parse predicates in where option: {where_sql}")
        );
    }
}
