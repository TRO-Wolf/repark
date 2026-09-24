//! Pins the destructive `CALL system.remove_orphan_files` surface and its safety defaults.

use super::super::*;
use super::call_orphan_scope::{call_rows, ctas, fallback_session, plant, register_file_list};
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

    let mut listed: Vec<String> = {
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
    listed.sort();
    let mut expected: Vec<String> = planted
        .iter()
        .map(|name| format!("file:{}", table_dir.join("data").join(name).display()))
        .collect();
    expected.sort();
    assert_eq!(
        listed, expected,
        "the dry run lists each orphan in its file: form"
    );

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
    let mut reported = orphan_locations(&result.collect().await.expect("collect"));
    reported.sort();
    let mut expected: Vec<String> = planted
        .iter()
        .map(|name| format!("file:{}", table_dir.join("data").join(name).display()))
        .collect();
    expected.sort();
    assert_eq!(
        reported, expected,
        "three orphans reported in their file: form"
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
    assert_eq!(
        locations,
        vec![format!("file:{}", inside.display())],
        "the listed orphan sits under `location`"
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
    assert_eq!(
        locations,
        vec![format!(
            "file:{}",
            data_dir.join("orphan-old.parquet").display()
        )],
        "the row must be the 10-day-old orphan"
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
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.near', equal_schemes => map('k', NULL))"
                .to_string(),
            "Error during planning: CALL remove_orphan_files argument `equal_schemes` value \
             for `k` must be a string literal, got NULL"
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

#[tokio::test]
async fn call_remove_orphan_files_mistyped_arguments_still_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mistyped AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("mistyped");
    plant_orphans(&table_dir, 1, 10);
    let before = files_under(&table_dir);
    let cases = [
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.mistyped', equal_authorities => 'a')"
                .to_string(),
            "Error during planning: CALL remove_orphan_files argument `equal_authorities` \
             must be map(k, v, …), got 'a'"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.mistyped', equal_schemes => foo('a', 'b'))"
                .to_string(),
            "Error during planning: CALL remove_orphan_files argument `equal_schemes` must be \
             map(k, v, …), got foo('a', 'b')"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.mistyped', equal_schemes => map('k'))"
                .to_string(),
            "Error during planning: CALL remove_orphan_files argument `equal_schemes` must \
             list key/value pairs (got 1 arguments)"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.mistyped', max_concurrent_deletes => 'x')"
                .to_string(),
            "Error during planning: CALL argument `max_concurrent_deletes` string is not an \
             integer: x"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.mistyped', prefix_listing => 'yes')"
                .to_string(),
            "Error during planning: CALL argument `prefix_listing` must be a boolean literal \
             (true / false), got `'yes'`"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.mistyped', prefix_mismatch_mode => 7)"
                .to_string(),
            "Error during planning: CALL argument `prefix_mismatch_mode` must be a string \
             literal, got 7"
                .to_string(),
        ),
        (
            "CALL ice.system.remove_orphan_files(table => 'sales.mistyped', location => 5)"
                .to_string(),
            "Error during planning: CALL argument `location` must be a string literal, got 5"
                .to_string(),
        ),
    ];
    for (call, expected) in cases {
        let err = execute(&ctx, &catalogs, &call)
            .await
            .expect_err("mistyped argument must refuse");
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

#[test]
fn call_orphan_shared_ctas_root_rule() {
    use repark_core::LocationPolicy;

    use super::call_orphan_scope::shared_root_refusal;
    use crate::call::remove_orphan_files::refuse_shared_temp_fallback_location;

    let ctas_root = std::path::Path::new("/scratch/repark_ctas");
    let ansi_root = std::path::Path::new("/scratch/repark_ansi_ctas");
    let plan = |err: DataFusionError| match err {
        DataFusionError::Plan(message) => message,
        other => panic!("expected DataFusionError::Plan, got {other:?}"),
    };
    let policy = LocationPolicy::TempFallbackAllowed {
        root: std::path::PathBuf::from("/scratch"),
    };

    let err =
        refuse_shared_temp_fallback_location(Some(&policy), "/scratch/repark_ctas", "ns.events")
            .expect_err("the shared CTAS root itself must refuse");
    assert_eq!(
        plan(err),
        shared_root_refusal("ns.events", "/scratch/repark_ctas", ctas_root)
    );

    for own in [
        "/scratch/repark_ctas/mem/ns/events",
        "/scratch/repark_ctas/mem/ns/events/data",
        "file:///scratch/repark_ctas/mem/ns/events",
        "/scratch/repark_ansi_ctas/ice/ns/t",
    ] {
        refuse_shared_temp_fallback_location(Some(&policy), own, "ns.events")
            .unwrap_or_else(|err| panic!("a table's own directory is sweepable: {own}: {err}"));
    }

    refuse_shared_temp_fallback_location(Some(&policy), "/scratch/my-warehouse/ns/events", "x")
        .expect("an owned location under the same root is fine");
    refuse_shared_temp_fallback_location(Some(&policy), "/scratch/repark_ctas_other/t", "x")
        .expect("prefix similarity is not containment");

    for remote in [
        LocationPolicy::RequireExplicitLocation,
        LocationPolicy::ServiceManagedLocation,
    ] {
        refuse_shared_temp_fallback_location(Some(&remote), "/scratch/repark_ctas", "x")
            .expect("a remote catalog assigns real locations; the rule must not fire");
    }
    refuse_shared_temp_fallback_location(None, "/scratch/repark_ctas", "x")
        .expect("no policy, no rule");

    for root_alias in [
        "/scratch",
        "/",
        "/scratch/repark_ctas/",
        "file:///scratch/repark_ctas",
        "/scratch/owned/../repark_ctas",
        "/scratch/repark_ansi_ctas",
        "file:/scratch/repark_ctas",
        "file://scratch/repark_ctas",
        "/scratch/repark_ctas/mem/..",
        "/scratch/./repark_ctas",
        "/scratch//repark_ctas",
    ] {
        let err = refuse_shared_temp_fallback_location(Some(&policy), root_alias, "owned.t")
            .expect_err("the fallback root, its aliases and its parents must refuse");
        let root = if root_alias == "/scratch/repark_ansi_ctas" {
            ansi_root
        } else {
            ctas_root
        };
        assert_eq!(
            plan(err),
            shared_root_refusal("owned.t", root_alias, root),
            "{root_alias}"
        );
    }

    let aliased_root = LocationPolicy::TempFallbackAllowed {
        root: std::path::PathBuf::from("/scratch/detour/.."),
    };
    let err = refuse_shared_temp_fallback_location(
        Some(&aliased_root),
        "/scratch/repark_ctas",
        "owned.t",
    )
    .expect_err("a warehouse registered through `..` still guards its fallback root");
    assert_eq!(
        plan(err),
        shared_root_refusal("owned.t", "/scratch/repark_ctas", ctas_root)
    );
}

#[test]
fn call_remove_orphan_files_listing_path_table_location_normal_form_rule() {
    use crate::call::remove_orphan_files::table_location_is_normal;
    for normal in [
        "/wh/t",
        "/wh/t/",
        "file:///wh/t",
        "file:///wh/t/",
        "s3://bucket/t",
        "s3://bucket/",
        "s3://bucket",
    ] {
        assert!(table_location_is_normal(normal), "{normal}");
    }
    for aliased in [
        "/wh/detour/../t",
        "/wh/./t",
        "/wh//t",
        "/wh/t//",
        "file:///wh/detour/../t",
        "s3://bucket/x/../t",
    ] {
        assert!(!table_location_is_normal(aliased), "{aliased}");
    }
}

#[tokio::test]
async fn call_remove_orphan_files_refuses_a_location_arg_at_the_fallback_root() {
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
    let scan = fallback.display().to_string();
    let err = super::call_orphan_scope::call_error(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'owned.t', older_than => {}, \
             location => '{scan}')",
            older_than_two_days_ago_ms(),
        ),
    )
    .await;
    assert_eq!(
        super::call_orphan_scope::plan_message(err),
        super::call_orphan_scope::shared_root_refusal("owned.t", &scan, &fallback)
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

#[test]
fn call_remove_orphan_files_qualifies_only_a_location_starting_with_slash() {
    use crate::call::remove_orphan_files::qualify_local_path;
    for (location, qualified) in [
        ("/tmp/a", "file:/tmp/a"),
        ("/var/a", "file:/var/a"),
        ("/", "file:/"),
        ("//host/a", "file://host/a"),
        ("/tmp/../a/", "file:/tmp/../a/"),
    ] {
        assert_eq!(qualify_local_path(location), qualified);
    }
    for unchanged in [
        "file:/tmp/a",
        "file:///tmp/a",
        "s3://b/a",
        "memory:/a",
        "a/relative",
        "",
        "C:\\tmp\\a",
        "C:/tmp/a",
        "\\\\host\\a",
        " /tmp/a",
        "./a",
    ] {
        assert_eq!(qualify_local_path(unchanged), unchanged);
    }
}

#[tokio::test]
async fn call_remove_orphan_files_listing_rows_qualify_only_slash_rooted_locations() {
    use crate::call::remove_orphan_files::listed_orphan_dataframe;
    let cases = [
        ("/tmp/a/x.parquet", "file:/tmp/a/x.parquet"),
        ("s3://b/k.parquet", "s3://b/k.parquet"),
        ("file:/tmp/y.parquet", "file:/tmp/y.parquet"),
        ("file:///tmp/v.parquet", "file:///tmp/v.parquet"),
        ("memory:/m.parquet", "memory:/m.parquet"),
        ("rel/z.parquet", "rel/z.parquet"),
        ("", ""),
        ("C:\\w\\d.parquet", "C:\\w\\d.parquet"),
        ("C:/w/d.parquet", "C:/w/d.parquet"),
        ("\\\\srv\\share\\d.parquet", "\\\\srv\\share\\d.parquet"),
    ];
    let locations: Vec<String> = cases.iter().map(|(input, _)| input.to_string()).collect();
    let batches = listed_orphan_dataframe(&SessionContext::new(), &locations)
        .expect("listing rows")
        .collect()
        .await
        .expect("collect listing rows");
    let field = Field::new("orphan_file_location", DataType::Utf8, false);
    assert_eq!(batches[0].schema().as_ref(), &Schema::new(vec![field]));
    let expected: Vec<&str> = cases.iter().map(|(_, output)| *output).collect();
    assert_eq!(orphan_locations(&batches), expected);
}

#[tokio::test]
async fn call_remove_orphan_files_listing_prints_file_scheme_and_deletes_the_bare_path() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', dry_run => false)",
    )
    .await
    .expect("armed listing");
    assert_eq!(listed, vec![format!("file:{}", orphan.display())]);
    assert!(!orphan.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_prints_the_bare_path_unqualified() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);
    register_file_list(&session, &[(orphan.display().to_string(), 0)]).await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(\
             table => 'ns.t', dry_run => false, file_list_view => 'v')",
    )
    .await
    .expect("armed view sweep");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(!orphan.exists());
}
