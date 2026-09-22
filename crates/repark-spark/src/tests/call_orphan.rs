//! Pins the destructive `CALL system.remove_orphan_files` surface and its safety defaults.

use super::super::*;
use super::common::*;

/// The procedure executes with an explicit cutoff and returns its result schema.
#[tokio::test]
async fn call_remove_orphan_files_is_no_longer_an_unsupported_procedure() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.wired AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'sales.wired', older_than => {})",
            older_than_two_days_ago_ms()
        ),
    )
    .await
    .expect("MW-3 wired this procedure; it must no longer refuse as unsupported");
}

/// Every non-hidden file under the table's directory, relative paths, sorted.
fn files_under(dir: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, base, out);
            } else if let Ok(relative) = path.strip_prefix(base) {
                out.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// Plant `count` orphan files, aged `age_days` days, in the table's `data` directory.
fn plant_orphans(table_dir: &std::path::Path, count: usize, age_days: u64) -> Vec<String> {
    let data_dir = table_dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let stamp = std::time::SystemTime::now() - std::time::Duration::from_secs(age_days * 86_400);
    let mut planted = Vec::new();
    for index in 0..count {
        let name = format!("orphan-{index}.parquet");
        let path = data_dir.join(&name);
        std::fs::write(&path, b"not really parquet").expect("write orphan");
        // The fork cuts on the LISTED file's `created_at_millis`.
        let handle = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("reopen orphan to age it");
        handle
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(stamp)
                    .set_accessed(stamp),
            )
            .expect("age the orphan");
        planted.push(name);
    }
    planted
}

/// A cutoff safely past the 24-hour floor, in epoch millis.
fn older_than_two_days_ago_ms() -> i64 {
    chrono::Utc::now().timestamp_millis() - 2 * 24 * 60 * 60 * 1000
}

fn orphan_locations(batches: &[datafusion::arrow::array::RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("string column");
        for index in 0..column.len() {
            out.push(column.value(index).to_string());
        }
    }
    out
}

/// MW-3: the dry run lists every orphan and deletes nothing.
#[tokio::test]
async fn call_remove_orphan_files_dry_run_lists_without_deleting() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orphans AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("orphans");
    let planted = plant_orphans(&table_dir, 2, 10);
    let before = files_under(&table_dir);

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.remove_orphan_files(table => 'sales.orphans', dry_run => true)",
    )
    .await
    .expect("dry run CALL");
    let batches = result.collect().await.expect("collect orphan result");
    let batch = &batches[0];

    // Spark's schema, measured: one column, string, NON-nullable.
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "orphan_file_location");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!batch.schema().field(0).is_nullable());

    let listed: Vec<String> = {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("string column");
        (0..column.len())
            .map(|i| column.value(i).to_string())
            .collect()
    };
    assert_eq!(listed.len(), 2, "one row per orphan, got {listed:?}");
    for name in &planted {
        assert!(
            listed.iter().any(|location| location.ends_with(name)),
            "dry run must list {name}, got {listed:?}"
        );
    }

    assert_eq!(
        files_under(&table_dir),
        before,
        "dry_run => true must not remove one file"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orphans").await,
        1
    );
}

/// MW-3: the armed run deletes the orphans **and provably not one live file**.
#[tokio::test]
async fn call_remove_orphan_files_armed_deletes_orphans_and_nothing_else() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.armed AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    for index in 2..=4 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.armed SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
    let table_dir = wh.path().join("sales").join("armed");
    let planted = plant_orphans(&table_dir, 3, 10);
    let before = files_under(&table_dir);
    assert!(
        before.len() > planted.len(),
        "fixture must hold live files too, else 'nothing else' proves nothing"
    );

    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.armed', older_than => {}, dry_run => false)",
            older_than_two_days_ago_ms()
        ),
    )
    .await
    .expect("armed CALL");
    assert_eq!(
        result
            .collect()
            .await
            .expect("collect")
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        3,
        "three orphans reported"
    );

    let after = files_under(&table_dir);
    let removed: Vec<&String> = before.iter().filter(|f| !after.contains(f)).collect();
    let mut removed_names: Vec<String> = removed
        .iter()
        .map(|path| {
            std::path::Path::new(path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    removed_names.sort();
    let mut expected = planted.clone();
    expected.sort();
    assert_eq!(
        removed_names, expected,
        "the armed run must remove EXACTLY the orphans; removed {removed_names:?}"
    );
    // And the table still reads, which is the point of "not one live file".
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.armed").await,
        4
    );
}

#[tokio::test]
async fn call_remove_orphan_files_reads_location_positionally() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.scoped AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("scoped");
    let scan_root = table_dir.join("data").join("sub");
    plant_orphans(&scan_root, 1, 10);
    let inside = scan_root.join("data").join("orphan-0.parquet");
    plant_orphans(&table_dir, 1, 10);
    let outside = table_dir.join("data").join("orphan-0.parquet");

    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.remove_orphan_files('sales.scoped', {}, '{}', false)",
            older_than_two_days_ago_ms(),
            scan_root.display()
        ),
    )
    .await
    .expect("a positional `location` must scope the sweep");
    let batches = result.collect().await.expect("collect orphan result");
    let locations = orphan_locations(&batches);
    assert_eq!(
        locations.len(),
        1,
        "the sweep lists only what `location` covers, got {locations:?}"
    );
    assert!(
        locations[0].contains("/sub/"),
        "the listed orphan sits under `location`, got {locations:?}"
    );
    assert!(
        !inside.exists(),
        "the orphan under `location` is deleted, got {inside:?}"
    );
    assert!(
        outside.exists(),
        "the orphan outside `location` must stay, got {outside:?}"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_bare_call_deletes_with_sparks_three_day_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bare AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("bare");
    plant_orphans(&table_dir, 1, 10);
    let data_dir = table_dir.join("data");
    std::fs::rename(
        data_dir.join("orphan-0.parquet"),
        data_dir.join("orphan-old.parquet"),
    )
    .expect("rename the aged orphan aside");
    plant_orphans(&table_dir, 1, 1);
    let before = files_under(&table_dir);

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.remove_orphan_files(table => 'sales.bare')",
    )
    .await
    .expect("a bare call deletes with Spark's now-minus-3-days default");
    let batches = result.collect().await.expect("collect orphan result");
    let locations = orphan_locations(&batches);
    assert_eq!(locations.len(), 1, "one row, got {locations:?}");
    assert!(
        locations[0].ends_with("orphan-old.parquet"),
        "the row must be the 10-day-old orphan, got {locations:?}"
    );

    let after = files_under(&table_dir);
    let removed: Vec<&String> = before.iter().filter(|file| !after.contains(file)).collect();
    assert_eq!(
        removed.len(),
        1,
        "exactly one file removed, got {removed:?}"
    );
    assert!(
        removed[0].ends_with("orphan-old.parquet"),
        "only the 10-day-old orphan goes, got {removed:?}"
    );
    assert!(
        after.iter().any(|file| file.ends_with("orphan-0.parquet")),
        "the 1-day-old orphan stays, got {after:?}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.bare").await,
        1
    );
}

/// MW-3: the 24-hour floor, which is PARITY with Spark rather than stricter.
#[tokio::test]
async fn call_remove_orphan_files_enforces_sparks_twenty_four_hour_floor() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.floor AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("floor");
    plant_orphans(&table_dir, 1, 10);
    let before = files_under(&table_dir);
    let now_ms = chrono::Utc::now().timestamp_millis();
    let hour_ms = 60 * 60 * 1000;

    // Inside the floor: refused.
    for (label, older_than) in [("now", now_ms), ("now-23h", now_ms - 23 * hour_ms)] {
        let err = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.remove_orphan_files(\
                     table => 'sales.floor', older_than => {older_than}, dry_run => false)"
            ),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("less than 24 hours"),
            "{label} must trip the floor, got: {err}"
        );
        assert_eq!(
            files_under(&table_dir),
            before,
            "{label}: a floor refusal must delete nothing"
        );
    }

    // Outside it: runs.
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.floor', older_than => {})",
            now_ms - 25 * hour_ms
        ),
    )
    .await
    .expect("now-25h is outside the floor and must run, as it does on Spark");
}

#[tokio::test]
async fn call_remove_orphan_files_accepts_sparks_optional_arguments() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.args AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("args");
    plant_orphans(&table_dir, 1, 10);
    let before = files_under(&table_dir);
    for argument in [
        "max_concurrent_deletes => 2",
        "stream_results => true",
        "prefix_listing => true",
        "prefix_mismatch_mode => 'IGNORE'",
        "equal_schemes => map('file','file'), equal_authorities => map('a','a')",
    ] {
        let result = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.remove_orphan_files(\
                     table => 'sales.args', dry_run => true, {argument})"
            ),
        )
        .await
        .expect("optional argument must be accepted");
        let batches = result.collect().await.expect("collect");
        let locations = orphan_locations(&batches);
        assert_eq!(
            locations.len(),
            1,
            "{argument} must list the orphan, got {locations:?}"
        );
        assert!(
            locations[0].ends_with("orphan-0.parquet"),
            "{argument} must list the planted orphan, got {locations:?}"
        );
        assert_eq!(
            files_under(&table_dir),
            before,
            "{argument} runs dry, nothing moves"
        );
    }
    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.remove_orphan_files(\
             table => 'sales.args', dry_run => true, file_list_view => 'v')",
    )
    .await
    .expect_err("file_list_view still refuses");
    assert!(
        matches!(err, DataFusionError::NotImplemented(_)),
        "file_list_view refuses NotImplemented, got: {err}"
    );
    assert!(
        err.to_string().contains("is not supported in v1"),
        "refusal names the deferral, got: {err}"
    );
    assert_eq!(files_under(&table_dir), before);
}

#[tokio::test]
async fn call_remove_orphan_files_near_misses_still_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.near AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("near");
    plant_orphans(&table_dir, 1, 10);
    let before = files_under(&table_dir);
    let hour_ago = chrono::Utc::now().timestamp_millis() - 3_600_000;
    let cases = [
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.near', prefix_mismatch_mode => 'bogus')"
                .to_string(),
            "Error during planning: Invalid mode: bogus".to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.near', equal_schemes => 'file')"
                .to_string(),
            "Error during planning: CALL remove_orphan_files argument `equal_schemes` must be \
             map(k, v, …), got 'file'"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.near', stream_results => 'yes')"
                .to_string(),
            "Error during planning: CALL argument `stream_results` must be a boolean literal \
             (true / false), got `'yes'`"
                .to_string(),
        ),
        (
            format!(
                "CALL ice.system.remove_orphan_files(\
                     table => 'sales.near', older_than => {hour_ago})"
            ),
            "Error during planning: CALL remove_orphan_files refuses an `older_than` less than \
             24 hours in the past. A short interval can delete files an in-flight commit has \
             written but not yet referenced, which corrupts the table. This matches Apache \
             Spark's own floor."
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(table => 'sales.near', dry_run => 'false')"
                .to_string(),
            "Error during planning: CALL argument `dry_run` must be a boolean literal \
             (true / false), got `'false'`"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(table => 'sales.near', foo => 1)".to_string(),
            "Error during planning: unknown CALL argument `foo`; allowed: table, older_than, \
             location, dry_run, max_concurrent_deletes, file_list_view, equal_schemes, \
             equal_authorities, prefix_mismatch_mode, prefix_listing, stream_results"
                .to_string(),
        ),
    ];
    for (call, expected) in cases {
        let err = execute(&ctx, &catalogs, &call)
            .await
            .expect_err("near miss must refuse");
        assert_eq!(err.to_string(), expected);
        assert_eq!(
            files_under(&table_dir),
            before,
            "a refused call must not have touched the table"
        );
    }
}

/// MW-3: `dry_run` takes a boolean literal, not a quoted string.
#[tokio::test]
async fn call_remove_orphan_files_refuses_a_quoted_dry_run() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.quoted AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("quoted");
    plant_orphans(&table_dir, 1, 10);
    let before = files_under(&table_dir);

    let err = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.quoted', older_than => {}, dry_run => 'false')",
            older_than_two_days_ago_ms()
        ),
    )
    .await
    .expect_err("a quoted boolean must refuse rather than arm the deletion");
    assert!(
        err.to_string().contains("must be a boolean literal"),
        "got: {err}"
    );
    assert_eq!(files_under(&table_dir), before);
}

/// MW-3: the shared-CTAS-root rule, pinned directly.
#[test]
fn call_orphan_shared_ctas_root_rule() {
    use repark_core::LocationPolicy;

    use crate::call::refuse_shared_temp_fallback_location;

    let policy = LocationPolicy::TempFallbackAllowed {
        root: std::path::PathBuf::from("/scratch"),
    };

    // Under the fallback root: refused, and the message names the hazard rather than the symptom.
    let err = refuse_shared_temp_fallback_location(
        Some(&policy),
        "/scratch/repark_ctas/mem/ns/events",
        "ns.events",
    )
    .expect_err("a table in the shared CTAS root must refuse");
    let message = err.to_string();
    assert!(message.contains("shared CTAS fallback root"), "{message}");
    assert!(
        message.contains("CREATE NAMESPACE"),
        "the refusal must tell the caller how to get out of it: {message}"
    );

    // A namespace that owns its location is untouched, even under the SAME temp root.
    refuse_shared_temp_fallback_location(Some(&policy), "/scratch/my-warehouse/ns/events", "x")
        .expect("an owned location under the same root is fine");

    // A sibling directory that merely starts with the same characters is not under the root.
    refuse_shared_temp_fallback_location(Some(&policy), "/scratch/repark_ctas_other/t", "x")
        .expect("prefix similarity is not containment");

    // The remote policies never reach the fallback at all.
    for remote in [
        LocationPolicy::RequireExplicitLocation,
        LocationPolicy::ServiceManagedLocation,
    ] {
        refuse_shared_temp_fallback_location(Some(&remote), "/scratch/repark_ctas/mem/ns/t", "x")
            .expect("a remote catalog assigns real locations; the rule must not fire");
    }
    refuse_shared_temp_fallback_location(None, "/scratch/repark_ctas/mem/ns/t", "x")
        .expect("no policy, no rule");

    // Parent of the fallback tree (the warehouse itself) would list repark_ctas recursively.
    let err = refuse_shared_temp_fallback_location(Some(&policy), "/scratch", "owned.t")
        .expect_err("a parent of the fallback root must refuse");
    assert!(
        err.to_string().contains("shared CTAS fallback root"),
        "{err}"
    );

    refuse_shared_temp_fallback_location(
        Some(&policy),
        "file:///scratch/repark_ctas/mem/ns/events",
        "x",
    )
    .expect_err("file:// aliases must refuse");
    refuse_shared_temp_fallback_location(
        Some(&policy),
        "/scratch/owned/../repark_ctas/mem/ns/events",
        "x",
    )
    .expect_err("lexically equivalent .. paths must refuse");
    refuse_shared_temp_fallback_location(Some(&policy), "/scratch/repark_ansi_ctas/ice/ns/t", "x")
        .expect_err("the ANSI fallback prefix must refuse");
    refuse_shared_temp_fallback_location(
        Some(&policy),
        "file:/scratch/repark_ctas/mem/ns/events",
        "x",
    )
    .expect_err("file:/ (one slash) must refuse — FileIO lists it as /scratch/repark_ctas");
    refuse_shared_temp_fallback_location(
        Some(&policy),
        "file://scratch/repark_ctas/mem/ns/events",
        "x",
    )
    .expect_err("hostless file://path must refuse — FileIO treats it as absolute");
}

/// A13: CALL `location` pointing at the fallback tree must refuse on the execute path.
#[tokio::test]
async fn call_remove_orphan_files_refuses_a_location_arg_under_the_fallback_root() {
    use std::sync::Arc;

    use repark_core::ReparkSession;

    use crate::{SparkDialect, SparkExtension};

    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    let owned = format!("{warehouse}/owned");
    session
        .create_namespace(
            "ice",
            "owned",
            HashMap::from([("location".to_string(), owned)]),
        )
        .await
        .unwrap();
    session
        .sql("CREATE TABLE ice.owned.t USING iceberg AS SELECT 1 AS id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let fallback = warehouse_dir.path().join("repark_ctas");
    let err = session
        .sql(&format!(
            "CALL ice.system.remove_orphan_files(table => 'owned.t', older_than => {}, \
             location => '{}')",
            older_than_two_days_ago_ms(),
            fallback.display()
        ))
        .await
        .expect_err("CALL location under the fallback root must refuse");
    assert!(
        err.to_string().contains("shared CTAS fallback root"),
        "{err}"
    );
}

async fn s3_tables_registry(warehouse: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, mut catalogs) = setup(warehouse).await;
    let s3_tables = repark_iceberg::catalog::s3tables_catalog(&HashMap::from([
        (
            "table_bucket_arn".to_string(),
            "arn:aws:s3tables:us-east-2:123456789012:bucket/example".to_string(),
        ),
        ("region_name".to_string(), "us-east-2".to_string()),
    ]))
    .await
    .expect("s3tables catalog constructs offline");
    catalogs.insert(
        "s3t".to_string(),
        s3_tables,
        LocationPolicy::ServiceManagedLocation,
    );
    (ctx, catalogs)
}

fn assert_orphan_s3_tables_refusal(err: &DataFusionError, table_arg: &str) {
    let message = err.to_string();
    assert!(
        message.contains(table_arg),
        "the refusal must name the table, got: {message}"
    );
    assert!(
        message.contains("table buckets do not support listing") && message.contains("405"),
        "the refusal must name the reason, got: {message}"
    );
    assert!(
        message.contains("unreferencedFileRemoval")
            && message.contains("maintenance configuration"),
        "the refusal must name the service's own remedy, got: {message}"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_on_s3_tables_refuses_before_any_io() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = s3_tables_registry(&warehouse).await;
    let err = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL s3t.system.remove_orphan_files(table => 'ns.t', older_than => {}, \
             dry_run => false)",
            older_than_two_days_ago_ms()
        ),
    )
    .await
    .expect_err("a table bucket cannot be listed; the CALL must refuse before any IO");
    assert_orphan_s3_tables_refusal(&err, "ns.t");
}

#[tokio::test]
async fn call_remove_orphan_files_on_s3_tables_dry_run_refuses_the_same_way() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = s3_tables_registry(&warehouse).await;
    let older_than = older_than_two_days_ago_ms();
    let armed = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL s3t.system.remove_orphan_files(table => 'ns.t', older_than => {older_than}, \
             dry_run => false)"
        ),
    )
    .await
    .expect_err("armed call refuses")
    .to_string();
    for spelling in [
        format!(
            "CALL s3t.system.remove_orphan_files(table => 'ns.t', older_than => {older_than}, \
             dry_run => true)"
        ),
        format!("CALL s3t.system.remove_orphan_files(table => 'ns.t', older_than => {older_than})"),
    ] {
        let err = execute(&ctx, &catalogs, &spelling)
            .await
            .expect_err("dry run refuses the same way");
        assert_eq!(
            err.to_string(),
            armed,
            "the dry-run spelling must refuse identically: {spelling}"
        );
        assert_orphan_s3_tables_refusal(&err, "ns.t");
    }
}
