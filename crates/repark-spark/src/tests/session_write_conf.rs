use super::super::*;
use super::common::*;

fn set_session_conf(ctx: &SessionContext, key: &str, value: &str) {
    let state = ctx.state_ref();
    let mut guard = state.write();
    assert!(repark_iceberg::write::apply_session_write_key(
        guard.config_mut().options_mut(),
        key,
        value
    ));
}

fn unset_session_conf(ctx: &SessionContext, key: &str) {
    let state = ctx.state_ref();
    let mut guard = state.write();
    assert!(repark_iceberg::write::unset_session_write_key(
        guard.config_mut().options_mut(),
        key
    ));
}

fn team_of(table: &iceberg::table::Table) -> Option<String> {
    table
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .summary()
        .additional_properties
        .get("team")
        .cloned()
}

#[tokio::test]
async fn session_team_stamps_plain_insert() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sess (id INT) USING iceberg",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    run(&ctx, &catalogs, "INSERT INTO ice.sales.sess VALUES (1)").await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    let table = load_sales_table(&catalogs, "sess").await;
    assert_eq!(team_of(&table).as_deref(), Some("a"));
}

#[tokio::test]
async fn session_team_stamps_cow_delete_overwrite() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sdel (id INT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.sdel VALUES (1)").await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    run(&ctx, &catalogs, "DELETE FROM ice.sales.sdel WHERE id = 1").await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    let table = load_sales_table(&catalogs, "sdel").await;
    assert_eq!(team_of(&table).as_deref(), Some("a"));
}

#[tokio::test]
async fn bogus_session_codec_refuses_naming_the_codec() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sbog (id INT) USING iceberg",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.compression-codec", "bogus");
    let error = execute(&ctx, &catalogs, "INSERT INTO ice.sales.sbog VALUES (1)")
        .await
        .expect_err("bogus session codec refuses");
    unset_session_conf(&ctx, "spark.sql.iceberg.compression-codec");
    assert!(error.to_string().contains("bogus"), "{error}");
    let table = load_sales_table(&catalogs, "sbog").await;
    assert!(table.metadata().current_snapshot().is_none());
}

#[tokio::test]
async fn unset_session_conf_restores_unstamped_writes() {
    let _: &str = "pins: ice-session-write-conf-1/C-034";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.suns (id INT) USING iceberg",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    run(&ctx, &catalogs, "INSERT INTO ice.sales.suns VALUES (1)").await;
    let table = load_sales_table(&catalogs, "suns").await;
    assert_eq!(team_of(&table), None);
}

const NONCE_KEY: &str = "engine.operation-id";
const SIZE_KEYS: [&str; 3] = ["added-files-size", "total-files-size", "removed-files-size"];

type SummaryPairs = Vec<(String, String)>;
type Summaries = Vec<(String, SummaryPairs)>;
type StampedSummary = (i64, String, SummaryPairs);
type Layout = (usize, Vec<u64>, Summaries);

async fn live_data_files(table: &iceberg::table::Table) -> (usize, Vec<u64>) {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return (0, Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("manifest list");
    let mut counts = Vec::new();
    for entry in manifest_list.entries() {
        if entry.content != iceberg::spec::ManifestContentType::Data {
            continue;
        }
        let manifest = entry
            .load_manifest(table.file_io())
            .await
            .expect("manifest");
        for alive in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            counts.push(alive.data_file().record_count());
        }
    }
    counts.sort_unstable();
    (counts.len(), counts)
}

fn summaries_of(table: &iceberg::table::Table, drop: &[&str]) -> Summaries {
    let mut rows: Vec<StampedSummary> = table
        .metadata()
        .snapshots()
        .map(|snapshot| {
            let summary = snapshot.summary();
            let mut pairs: SummaryPairs = summary
                .additional_properties
                .iter()
                .filter(|(key, _)| !drop.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            pairs.sort();
            (
                snapshot.timestamp_ms(),
                format!("{:?}", summary.operation),
                pairs,
            )
        })
        .collect();
    rows.sort_by_key(|(stamp, _, _)| *stamp);
    rows.into_iter()
        .map(|(_, operation, pairs)| (operation, pairs))
        .collect()
}

async fn layout_under(
    table_name: &str,
    statement: &str,
    conf: Option<(&str, &str)>,
    drop: &[&str],
) -> Layout {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.{table_name} (id BIGINT, data STRING) USING iceberg"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.{table_name} VALUES (1,'a'),(2,'b'),(3,'c')"),
    )
    .await;
    if let Some((key, value)) = conf {
        set_session_conf(&ctx, key, value);
    }
    run(&ctx, &catalogs, &statement.replace("{t}", table_name)).await;
    let table = load_sales_table(&catalogs, table_name).await;
    let (count, records) = live_data_files(&table).await;
    (count, records, summaries_of(&table, drop))
}

async fn assert_session_conf_keeps_the_layout(tag: &str, statement: &str) {
    let base_for_property = layout_under(&format!("{tag}pb"), statement, None, &[NONCE_KEY]).await;
    let under_property = layout_under(
        &format!("{tag}pp"),
        statement,
        Some(("spark.sql.iceberg.snapshot-property.team", "a")),
        &[NONCE_KEY, "team"],
    )
    .await;
    assert_eq!(
        base_for_property, under_property,
        "a snapshot-property-only session conf changed more than the stamp on {statement}"
    );

    let mut codec_drop = vec![NONCE_KEY];
    codec_drop.extend_from_slice(&SIZE_KEYS);
    let base_for_codec = layout_under(&format!("{tag}cb"), statement, None, &codec_drop).await;
    let under_codec = layout_under(
        &format!("{tag}cc"),
        statement,
        Some(("spark.sql.iceberg.compression-codec", "gzip")),
        &codec_drop,
    )
    .await;
    assert_eq!(
        base_for_codec, under_codec,
        "a codec-only session conf changed more than the written bytes on {statement}"
    );
}

#[tokio::test]
async fn a_session_conf_keeps_a_plain_update_layout() {
    let _: &str = "pins: ice-session-write-conf-1/C-045";
    Box::pin(assert_session_conf_keeps_the_layout(
        "u",
        "UPDATE ice.sales.{t} SET data = 'z' WHERE id = 1",
    ))
    .await;
}

#[tokio::test]
async fn a_session_conf_keeps_a_plain_insert_layout() {
    let _: &str = "pins: ice-session-write-conf-1/C-045";
    Box::pin(assert_session_conf_keeps_the_layout(
        "i",
        "INSERT INTO ice.sales.{t} VALUES (9,'i')",
    ))
    .await;
}

#[tokio::test]
async fn a_session_conf_keeps_a_plain_delete_layout() {
    let _: &str = "pins: ice-session-write-conf-1/C-045";
    Box::pin(assert_session_conf_keeps_the_layout(
        "d",
        "DELETE FROM ice.sales.{t} WHERE id = 2",
    ))
    .await;
}

fn set_dynamic_overwrite(ctx: &SessionContext) {
    let state = ctx.state_ref();
    let mut guard = state.write();
    let config = guard.config_mut();
    let updated = repark_core::with_partition_overwrite_mode(
        config.clone(),
        repark_core::PartitionOverwriteMode::Dynamic,
    );
    *config = updated;
}

async fn replace_partitions_with_deleted_records(
    table_name: &str,
    overwrite: &str,
) -> Result<(), DataFusionError> {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    set_dynamic_overwrite(&ctx);
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table_name} (id BIGINT, cat STRING) USING iceberg \
             PARTITIONED BY (cat)"
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.{table_name} VALUES (1,'x'),(2,'x'),(3,'y')"),
    )
    .await;
    set_session_conf(
        &ctx,
        "spark.sql.iceberg.snapshot-property.deleted-records",
        "5",
    );
    let outcome = execute(&ctx, &catalogs, overwrite).await.map(|_| ());
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.deleted-records");
    outcome
}

#[tokio::test]
async fn replace_partitions_into_a_new_partition_stamps_deleted_records() {
    let _: &str = "pins: ice-session-write-conf-1/C-047";
    replace_partitions_with_deleted_records(
        "rpnew",
        "INSERT OVERWRITE ice.sales.rpnew SELECT 9, 'w'",
    )
    .await
    .expect("a partition the overwrite does not replace produces no deleted-records to collide");
}

#[tokio::test]
async fn replace_partitions_into_an_existing_partition_names_the_engine_value() {
    let _: &str = "pins: ice-session-write-conf-1/C-047";
    let error = replace_partitions_with_deleted_records(
        "rpold",
        "INSERT OVERWRITE ice.sales.rpold SELECT 9, 'x'",
    )
    .await
    .expect_err("replacing a live partition collides on deleted-records");
    assert_eq!(
        error.strip_backtrace(),
        "External error: Multiple entries with same key: deleted-records=2 and \
         deleted-records=5",
        "the refusal must name Spark's computed engine value, not `<resolved at commit>`"
    );
}

async fn static_partition_overwrite_under(
    table_name: &str,
    key: &str,
    value: &str,
    overwrite: &str,
) -> Result<Summaries, DataFusionError> {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table_name} (id BIGINT, cat STRING) USING iceberg \
             PARTITIONED BY (cat)"
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.{table_name} VALUES (1,'x'),(2,'x'),(3,'y')"),
    )
    .await;
    set_session_conf(&ctx, key, value);
    let outcome = execute(&ctx, &catalogs, overwrite).await.map(|_| ());
    unset_session_conf(&ctx, key);
    outcome?;
    let table = load_sales_table(&catalogs, table_name).await;
    Ok(summaries_of(&table, &[NONCE_KEY]))
}

#[tokio::test]
async fn static_partition_overwrite_of_a_new_partition_stamps_deleted_records() {
    let _: &str = "pins: ice-session-write-conf-1/C-054";
    let summaries = static_partition_overwrite_under(
        "sonew",
        "spark.sql.iceberg.snapshot-property.deleted-records",
        "5",
        "INSERT OVERWRITE ice.sales.sonew PARTITION (cat = 'w') VALUES (9)",
    )
    .await
    .expect("a partition the row filter does not reach removes nothing to collide with");
    let (operation, pairs) = summaries.last().expect("an overwrite snapshot");
    assert_eq!(operation, "Overwrite");
    assert_eq!(
        pairs
            .iter()
            .find(|(key, _)| key == "deleted-records")
            .map(|(_, value)| value.as_str()),
        Some("5"),
        "the free key is stamped: {pairs:?}"
    );
}

#[tokio::test]
async fn static_partition_overwrite_of_a_live_partition_names_the_engine_value() {
    let _: &str = "pins: ice-session-write-conf-1/C-054";
    let error = static_partition_overwrite_under(
        "soold",
        "spark.sql.iceberg.snapshot-property.deleted-records",
        "5",
        "INSERT OVERWRITE ice.sales.soold PARTITION (cat = 'x') VALUES (9)",
    )
    .await
    .expect_err("overwriting a live partition collides on deleted-records");
    assert_eq!(
        error.strip_backtrace(),
        "External error: Multiple entries with same key: deleted-records=2 and \
         deleted-records=5",
        "the refusal must name Spark's computed engine value, not `<resolved at commit>`"
    );
}

#[tokio::test]
async fn static_partition_overwrite_names_the_total_it_would_have_written() {
    let _: &str = "pins: ice-session-write-conf-1/C-054";
    let error = static_partition_overwrite_under(
        "sotot",
        "spark.sql.iceberg.snapshot-property.total-records",
        "77",
        "INSERT OVERWRITE ice.sales.sotot PARTITION (cat = 'w') VALUES (9)",
    )
    .await
    .expect_err("a total is always produced, so it always collides");
    assert_eq!(
        error.strip_backtrace(),
        "External error: Multiple entries with same key: total-records=4 and total-records=77",
        "the total counts the three live rows plus the added one, with nothing removed"
    );
}

async fn rewrite_manifests_under(key: &str, value: &str, table_name: &str) -> Summaries {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.{table_name} (id BIGINT) USING iceberg"),
    )
    .await;
    for id in 1..=2 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.{table_name} VALUES ({id})"),
        )
        .await;
    }
    set_session_conf(&ctx, key, value);
    run(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.rewrite_manifests(table => 'sales.{table_name}')"),
    )
    .await;
    unset_session_conf(&ctx, key);
    let table = load_sales_table(&catalogs, table_name).await;
    summaries_of(&table, &[NONCE_KEY])
}

#[tokio::test]
async fn rewrite_manifests_does_not_stamp_the_session_snapshot_property() {
    let _: &str = "pins: ice-session-write-conf-1/C-049";
    let summaries =
        rewrite_manifests_under("spark.sql.iceberg.snapshot-property.team", "a", "rmteam").await;
    let (operation, pairs) = summaries.last().expect("a replace snapshot");
    assert_eq!(operation, "Replace");
    assert!(
        !pairs.iter().any(|(key, _)| key == "team"),
        "Spark's rewrite_manifests replace snapshot carries no session property: {pairs:?}"
    );
}

#[tokio::test]
async fn rewrite_manifests_does_not_refuse_a_colliding_session_key() {
    let _: &str = "pins: ice-session-write-conf-1/C-049";
    let summaries = rewrite_manifests_under(
        "spark.sql.iceberg.snapshot-property.total-records",
        "77",
        "rmcoll",
    )
    .await;
    let (operation, pairs) = summaries.last().expect("a replace snapshot");
    assert_eq!(operation, "Replace");
    assert_eq!(
        pairs
            .iter()
            .find(|(key, _)| key == "total-records")
            .map(|(_, value)| value.as_str()),
        Some("2"),
        "Spark commits the engine total and refuses nothing here: {pairs:?}"
    );
}

fn branch_head_summary(table: &iceberg::table::Table, branch: &str) -> SummaryPairs {
    let snapshot = table
        .metadata()
        .snapshot_for_ref(branch)
        .unwrap_or_else(|| panic!("branch {branch} must have a head"));
    let mut pairs: SummaryPairs = snapshot
        .summary()
        .additional_properties
        .iter()
        .filter(|(key, _)| *key != NONCE_KEY)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    pairs.sort();
    pairs
}

async fn branch_write_stamp(table_name: &str, statement: &str) -> SummaryPairs {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.{table_name} (id BIGINT, data STRING) USING iceberg"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.{table_name} VALUES (1,'a'),(2,'b')"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.{table_name} CREATE BRANCH b"),
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    run(&ctx, &catalogs, &statement.replace("{t}", table_name)).await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    let table = load_sales_table(&catalogs, table_name).await;
    branch_head_summary(&table, "b")
}

#[tokio::test]
async fn a_branch_insert_stamps_the_session_snapshot_property() {
    let _: &str = "pins: ice-session-write-conf-1/C-051";
    let pairs =
        branch_write_stamp("bins", "INSERT INTO ice.sales.{t}.branch_b VALUES (3,'c')").await;
    assert!(
        pairs.contains(&("team".to_string(), "a".to_string())),
        "the branch head an INSERT commits must carry the session property: {pairs:?}"
    );
    assert_eq!(
        pairs
            .iter()
            .find(|(key, _)| key == "added-records")
            .map(|(_, value)| value.as_str()),
        Some("1"),
        "the INSERT must have committed through the append arm: {pairs:?}"
    );
}

#[tokio::test]
async fn a_branch_delete_stamps_the_session_snapshot_property() {
    let _: &str = "pins: ice-session-write-conf-1/C-051";
    let pairs = branch_write_stamp("bdel", "DELETE FROM ice.sales.{t}.branch_b WHERE id = 1").await;
    assert!(
        pairs.contains(&("team".to_string(), "a".to_string())),
        "the branch head a DELETE commits must carry the session property: {pairs:?}"
    );
}

#[tokio::test]
async fn truncate_does_not_stamp_and_does_not_refuse_a_colliding_key() {
    let _: &str = "pins: ice-session-write-conf-1/C-052";
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.strunc (id BIGINT) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.strunc VALUES (1),(2),(3)",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team", "a");
    set_session_conf(
        &ctx,
        "spark.sql.iceberg.snapshot-property.deleted-records",
        "5",
    );
    run(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.strunc").await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.team");
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.deleted-records");
    let table = load_sales_table(&catalogs, "strunc").await;
    let summaries = summaries_of(&table, &[NONCE_KEY]);
    let (operation, pairs) = summaries.last().expect("a delete snapshot");
    assert_eq!(operation, "Delete");
    assert!(
        !pairs.iter().any(|(key, _)| key == "team"),
        "Spark's TRUNCATE stamps nothing: {pairs:?}"
    );
    assert!(
        !pairs
            .iter()
            .any(|(key, value)| key == "deleted-records" && value == "5"),
        "and it takes no colliding session value either: {pairs:?}"
    );
}
