use super::super::*;
use super::common::*;

fn procs_ident(table: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".into()), table.into())
}

async fn seed_three(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id BIGINT, data STRING, cat STRING) USING iceberg \
             PARTITIONED BY (cat) TBLPROPERTIES ('format-version' = '2')"
        ),
    )
    .await;
    for values in [
        "(1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')",
        "(3, 'c', 'x'), (7, 'g', 'x')",
        "(4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')",
    ] {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES {values}"),
        )
        .await;
    }
}

async fn snapshot_ids(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> Vec<i64> {
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
        let column = batch
            .column_by_name("snapshot_id")
            .expect("snapshot_id column");
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id is bigint");
        for index in 0..values.len() {
            ids.push(values.value(index));
        }
    }
    ids
}

fn int64_column(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let column = batch.column_by_name(name).expect("result column");
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("bigint column");
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn int32_column(batch: &RecordBatch, name: &str) -> Vec<i32> {
    let column = batch.column_by_name(name).expect("result column");
    let values = column
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int column");
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch.column_by_name(name).expect("result column");
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    (0..values.len())
        .map(|index| values.value(index).to_string())
        .collect()
}

fn schema_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

#[tokio::test]
async fn call_ancestors_of_walks_newest_first_with_snapshot_timestamps() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "anc").await;
    let ids = snapshot_ids(&ctx, &catalogs, "anc").await;
    assert_eq!(ids.len(), 3, "seed must commit three snapshots");

    let batches = execute(&ctx, &catalogs, "CALL ice.system.ancestors_of('sales.anc')")
        .await
        .expect("ancestors_of CALL")
        .collect()
        .await
        .expect("collect ancestors");
    let batch = &batches[0];
    assert_eq!(schema_names(batch), vec!["snapshot_id", "timestamp"]);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>(),
        vec![DataType::Int64, DataType::Int64]
    );
    assert_eq!(
        int64_column(batch, "snapshot_id"),
        vec![ids[2], ids[1], ids[0]]
    );

    let table = catalogs["ice"]
        .load_table(&procs_ident("anc"))
        .await
        .expect("load table");
    let stamps: std::collections::HashMap<i64, i64> = table
        .metadata()
        .snapshots()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot.timestamp_ms()))
        .collect();
    let rows = int64_column(batch, "snapshot_id");
    let times = int64_column(batch, "timestamp");
    for (id, stamp) in rows.iter().zip(times.iter()) {
        assert_eq!(
            stamps.get(id),
            Some(stamp),
            "timestamp must be the snapshot's own"
        );
    }
}

#[tokio::test]
async fn call_ancestors_of_honors_older_snapshot_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "ancold").await;
    let ids = snapshot_ids(&ctx, &catalogs, "ancold").await;
    assert_eq!(ids.len(), 3, "seed must commit three snapshots");
    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.ancestors_of(table => 'sales.ancold', snapshot_id => {})",
            ids[1]
        ),
    )
    .await
    .expect("ancestors_of CALL with older id")
    .collect()
    .await
    .expect("collect ancestors");
    assert_eq!(
        int64_column(&batches[0], "snapshot_id"),
        vec![ids[1], ids[0]],
        "the walk starts at the requested snapshot, not the current head"
    );
}

#[tokio::test]
async fn call_ancestors_of_names_missing_snapshots_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ancempty (id BIGINT) USING iceberg",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.ancestors_of('sales.ancempty')",
    )
    .await
    .expect_err("empty table must refuse");
    assert!(
        error.to_string().contains("Cannot find snapshot: -1"),
        "Spark's empty-table text, got: {error}"
    );

    seed_three(&ctx, &catalogs, "ancmiss").await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.ancestors_of(table => 'sales.ancmiss', snapshot_id => 12345)",
    )
    .await
    .expect_err("unknown id must refuse");
    assert!(
        error.to_string().contains("Cannot find snapshot: 12345"),
        "Spark's unknown-id text, got: {error}"
    );
}

#[tokio::test]
async fn call_compute_table_stats_registers_blobs_in_caller_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "cts").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.cts', columns => array('data', 'id'))",
    )
    .await
    .expect("compute_table_stats CALL")
    .collect()
    .await
    .expect("collect stats result");
    let batch = &batches[0];
    assert_eq!(schema_names(batch), vec!["statistics_file"]);
    let paths = string_column(batch, "statistics_file");
    assert_eq!(paths.len(), 1);

    let table = catalogs["ice"]
        .load_table(&procs_ident("cts"))
        .await
        .expect("load table");
    let statistics: Vec<_> = table.metadata().statistics_iter().collect();
    assert_eq!(statistics.len(), 1, "one statistics entry per run");
    assert_eq!(statistics[0].statistics_path, paths[0]);
    let fields: Vec<_> = statistics[0]
        .blob_metadata
        .iter()
        .map(|blob| blob.fields.clone())
        .collect();
    assert_eq!(
        fields,
        vec![vec![2], vec![1]],
        "Spark keeps caller order after dedup, not schema order"
    );
    for blob in &statistics[0].blob_metadata {
        assert_eq!(blob.r#type, "apache-datasketches-theta-v1");
    }
}

#[tokio::test]
async fn call_compute_table_stats_registers_older_snapshot_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "ctsold").await;
    let ids = snapshot_ids(&ctx, &catalogs, "ctsold").await;
    assert_eq!(ids.len(), 3, "seed must commit three snapshots");
    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.compute_table_stats(table => 'sales.ctsold', snapshot_id => {})",
            ids[0]
        ),
    )
    .await
    .expect("compute_table_stats CALL with older id")
    .collect()
    .await
    .expect("collect stats result");
    assert_eq!(batches[0].num_rows(), 1);
    let table = catalogs["ice"]
        .load_table(&procs_ident("ctsold"))
        .await
        .expect("load table");
    let statistics: Vec<_> = table.metadata().statistics_iter().collect();
    assert_eq!(statistics.len(), 1, "one statistics entry per run");
    assert_eq!(
        statistics[0].snapshot_id, ids[0],
        "the entry pins the requested snapshot, not the current head"
    );
    for blob in &statistics[0].blob_metadata {
        assert_eq!(
            blob.snapshot_id, ids[0],
            "every blob pins the requested snapshot"
        );
    }
    let id_blob = statistics[0]
        .blob_metadata
        .iter()
        .find(|blob| blob.fields == vec![1])
        .expect("one blob on the id field");
    assert_eq!(
        id_blob.properties.get("ndv").map(String::as_str),
        Some("3"),
        "the first snapshot holds three distinct ids, the current head eight"
    );
    assert_eq!(
        snapshot_ids(&ctx, &catalogs, "ctsold").await,
        ids,
        "the stats run commits no data snapshot"
    );
}

#[tokio::test]
async fn call_compute_table_stats_refuses_empty_columns_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "ctsempty").await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.ctsempty', columns => array())",
    )
    .await
    .expect_err("empty columns must refuse");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "Spark's IllegalArgumentException class, got: {error}"
    );
    assert!(
        error.to_string().contains("Columns cannot be null/empty"),
        "Spark's empty-columns text, got: {error}"
    );
    let table = catalogs["ice"]
        .load_table(&procs_ident("ctsempty"))
        .await
        .expect("load table");
    assert_eq!(
        table.metadata().statistics_iter().count(),
        0,
        "the refusal registers no statistics file"
    );
}

#[tokio::test]
async fn call_compute_table_stats_resolves_nested_names_and_dedupes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ctsnest (id BIGINT, st STRUCT<a: INT, b: STRING>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ctsnest VALUES (1, named_struct('a', 1, 'b', 'x')), (2, \
         named_struct('a', 2, 'b', 'y'))",
    )
    .await;

    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.ctsnest', columns => array('st.a'))",
    )
    .await
    .expect_err("nested column reaches the fork");
    assert!(
        error.to_string().contains("Column a not found in table"),
        "the name passes through to the fork scan instead of collapsing to empty stats, got: \
         {error}"
    );
    let table = catalogs["ice"]
        .load_table(&procs_ident("ctsnest"))
        .await
        .expect("load table");
    assert_eq!(
        table.metadata().statistics_iter().count(),
        0,
        "no silent empty-stats commit for the nested name"
    );

    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.ctsnest', columns => \
         array('st'))",
    )
    .await
    .expect_err("struct column must refuse");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "Spark's IllegalArgumentException class, got: {error}"
    );
    assert!(
        error.to_string().contains(
            "Can't compute stats on non-primitive type column: st (struct<3: a: optional int, 4: \
             b: optional string>)"
        ),
        "Spark's non-primitive text, got: {error}"
    );

    seed_three(&ctx, &catalogs, "ctsdup").await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.ctsdup', columns => array('id', \
         'id'))",
    )
    .await
    .expect("duplicate column CALL")
    .collect()
    .await
    .expect("collect duplicate stats result");
    assert_eq!(batches[0].num_rows(), 1);
    let table = catalogs["ice"]
        .load_table(&procs_ident("ctsdup"))
        .await
        .expect("load table");
    let statistics: Vec<_> = table.metadata().statistics_iter().collect();
    assert_eq!(statistics.len(), 1);
    assert_eq!(
        statistics[0].blob_metadata.len(),
        1,
        "a duplicate resolves to one blob"
    );
}

#[tokio::test]
async fn call_compute_table_stats_refuses_unknown_column_and_empty_answers_zero_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "ctsneg").await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.ctsneg', columns => array('nope'))",
    )
    .await
    .expect_err("unknown column must refuse");
    assert!(
        error
            .to_string()
            .contains("Can't find column nope in table"),
        "Spark's unknown-column text, got: {error}"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ctsempty (id BIGINT) USING iceberg",
    )
    .await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_table_stats(table => 'sales.ctsempty')",
    )
    .await
    .expect("empty table answers")
    .collect()
    .await
    .expect("collect empty result");
    assert_eq!(schema_names(&batches[0]), vec!["statistics_file"]);
    assert_eq!(
        batches[0].num_rows(),
        0,
        "Spark answers zero rows with no statistics"
    );
}

#[tokio::test]
async fn call_compute_partition_stats_registers_entry_and_refuses_unpartitioned() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "cps").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_partition_stats(table => 'sales.cps')",
    )
    .await
    .expect("compute_partition_stats CALL")
    .collect()
    .await
    .expect("collect partition stats result");
    let batch = &batches[0];
    assert_eq!(schema_names(batch), vec!["partition_statistics_file"]);
    let paths = string_column(batch, "partition_statistics_file");
    assert_eq!(paths.len(), 1);

    let table = catalogs["ice"]
        .load_table(&procs_ident("cps"))
        .await
        .expect("load table");
    let entries: Vec<_> = table.metadata().partition_statistics_iter().collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].statistics_path, paths[0]);

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cpsflat (id BIGINT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.cpsflat VALUES (1)").await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.compute_partition_stats(table => 'sales.cpsflat')",
    )
    .await
    .expect_err("unpartitioned table must refuse");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "Spark's IllegalArgumentException class, got: {error}"
    );
    assert!(
        error.to_string().contains("Table must be partitioned"),
        "Spark's unpartitioned text, got: {error}"
    );
    assert!(
        !error.to_string().contains("External") && !error.to_string().contains("DataInvalid"),
        "the router guard raises, not the fork External shape, got: {error}"
    );
}

#[tokio::test]
async fn call_compute_partition_stats_registers_older_snapshot_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "cpsold").await;
    let ids = snapshot_ids(&ctx, &catalogs, "cpsold").await;
    assert_eq!(ids.len(), 3, "seed must commit three snapshots");
    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.compute_partition_stats(table => 'sales.cpsold', snapshot_id => \
             {})",
            ids[0]
        ),
    )
    .await
    .expect("compute_partition_stats CALL with older id")
    .collect()
    .await
    .expect("collect partition stats result");
    assert_eq!(batches[0].num_rows(), 1);
    let table = catalogs["ice"]
        .load_table(&procs_ident("cpsold"))
        .await
        .expect("load table");
    let entries: Vec<_> = table.metadata().partition_statistics_iter().collect();
    assert_eq!(entries.len(), 1, "one partition-statistics entry per run");
    assert_eq!(
        entries[0].snapshot_id, ids[0],
        "the entry pins the requested snapshot, not the current head"
    );
    assert_eq!(
        snapshot_ids(&ctx, &catalogs, "cpsold").await,
        ids,
        "the stats run commits no data snapshot"
    );
}

#[tokio::test]
async fn call_rewrite_table_path_stages_manifests_lists_and_answers_sparks_counts() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "rtp").await;
    let table = catalogs["ice"]
        .load_table(&procs_ident("rtp"))
        .await
        .expect("load table");
    let source = table.metadata().location().to_string();
    let target = format!("{}/dst", wh.path().to_str().unwrap());

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rewrite_table_path(table => 'sales.rtp', source_prefix => \
             '{source}', target_prefix => '{target}')"
        ),
    )
    .await
    .expect("rewrite_table_path CALL")
    .collect()
    .await
    .expect("collect rewrite result");
    let batch = &batches[0];
    assert_eq!(
        schema_names(batch),
        vec![
            "latest_version",
            "file_list_location",
            "rewritten_manifest_file_paths_count",
            "rewritten_delete_file_paths_count",
        ]
    );
    assert_eq!(
        int32_column(batch, "rewritten_manifest_file_paths_count"),
        vec![3]
    );
    assert_eq!(
        int32_column(batch, "rewritten_delete_file_paths_count"),
        vec![0]
    );

    let listed = string_column(batch, "file_list_location");
    assert_eq!(listed.len(), 1);
    assert!(
        listed[0].starts_with(&format!("{source}/metadata/copy-table-staging-")),
        "default staging sits under the table metadata dir, got: {}",
        listed[0]
    );
    let text = std::fs::read_to_string(&listed[0]).expect("file list exists");
    let lines: Vec<&str> = text.lines().collect();
    assert!(!lines.is_empty());
    assert!(
        lines.iter().any(|line| line.ends_with(".metadata.json")),
        "the staged rewritten metadata joins the copy plan like Spark's list"
    );
    for line in &lines {
        let parts: Vec<&str> = line.split(',').collect();
        assert_eq!(
            parts.len(),
            2,
            "each file-list line pairs source and target"
        );
        assert!(
            parts[0].starts_with(&source),
            "from-side under the source: {line}"
        );
        assert!(
            parts[1].starts_with(&target),
            "to-side under the target: {line}"
        );
    }

    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_table_path(table => 'sales.rtp', source_prefix => '/nope', \
         target_prefix => '/x')",
    )
    .await
    .expect_err("wrong prefix must refuse");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "the router guard raises IllegalArgumentException, got: {error}"
    );
    let metadata_file = catalogs["ice"]
        .load_table(&procs_ident("rtp"))
        .await
        .expect("load table")
        .metadata_location()
        .expect("metadata location")
        .to_string();
    assert!(
        error
            .to_string()
            .contains(&format!("Path {metadata_file}/ does not start with /nope/")),
        "the RePark-owned guard text, not the fork relativize error, got: {error}"
    );
    assert!(
        !error.to_string().contains("RewriteTablePath:"),
        "the fork error must not satisfy this pin, got: {error}"
    );

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rewrite_table_path(table => 'sales.rtp', source_prefix => \
             '{source}', target_prefix => '{target}', create_file_list => false)"
        ),
    )
    .await
    .expect("no-file-list CALL")
    .collect()
    .await
    .expect("collect no-file-list result");
    assert_eq!(
        string_column(&batches[0], "file_list_location"),
        vec!["N/A"]
    );
}

#[tokio::test]
async fn call_rewrite_table_path_version_range_refuses_naming_the_fork_gap() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three(&ctx, &catalogs, "rtpver").await;
    let table = catalogs["ice"]
        .load_table(&procs_ident("rtpver"))
        .await
        .expect("load table");
    let source = table.metadata().location().to_string();
    let error = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rewrite_table_path(table => 'sales.rtpver', source_prefix => \
             '{source}', target_prefix => '/dst', end_version => 'v3.metadata.json')"
        ),
    )
    .await
    .expect_err("version range must refuse");
    assert!(
        error.to_string().contains("start_version / end_version"),
        "refusal names the unsupported range, got: {error}"
    );
}

#[tokio::test]
async fn call_unknown_procedure_lists_the_four_new_names() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.not_a_real_proc(table => 'sales.t')",
    )
    .await
    .expect_err("unknown CALL must fail loud");
    let message = error.to_string();
    for name in [
        "ancestors_of",
        "compute_table_stats",
        "compute_partition_stats",
        "rewrite_table_path",
    ] {
        assert!(
            message.contains(name),
            "supported set names {name}, got: {message}"
        );
    }
}
