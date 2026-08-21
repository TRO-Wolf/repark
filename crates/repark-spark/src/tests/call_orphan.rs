//! MW-3 — `CALL system.remove_orphan_files`, the one maintenance procedure that destroys data.
//!
//! Split out of `call.rs` when that module crossed the 1500-line ceiling. These tests share a
//! subject rather than a mechanism: every one of them is about the blast radius of a deletion,
//! which is why the fixture helpers below live here and not in `common.rs`.
//!
//! Ledger: `task/mw-3-remove-orphan-files-ledger.md`. Registry rows `ORPHAN-1` / `ORPHAN-2`.

use super::super::*;
use super::common::*;

/// **Retired by MW-3.** This pinned `remove_orphan_files` refusing as a fork-queue residual. The
/// fork surface it was waiting on is now wired, so the refusal it asserted is gone and the
/// procedure's real behaviour is pinned by the `call_remove_orphan_files_*` and `call_orphan*`
/// tests below. What survives here is the part still true and still worth guarding: the procedure
/// no longer refuses as unsupported.
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
///
/// The orphan pins compare the WHOLE directory before and after, not just the orphan set. That is
/// the difference between proving the armed run deleted the orphans and proving it deleted the
/// orphans *and nothing else*.
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
///
/// Aged deliberately: an orphan younger than the `older_than` cutoff is not an orphan yet, and a
/// fixture whose files are all seconds old would pass whatever cutoff the test happens to pick.
fn plant_orphans(table_dir: &std::path::Path, count: usize, age_days: u64) -> Vec<String> {
    let data_dir = table_dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let stamp = std::time::SystemTime::now() - std::time::Duration::from_secs(age_days * 86_400);
    let mut planted = Vec::new();
    for index in 0..count {
        let name = format!("orphan-{index}.parquet");
        let path = data_dir.join(&name);
        std::fs::write(&path, b"not really parquet").expect("write orphan");
        // The fork cuts on the LISTED file's `created_at_millis`, which for local storage is
        // opendal's `last_modified`. A freshly written orphan is therefore newer than any legal
        // cutoff (the floor forbids one under 24 hours old), so the fixture has to age the file
        // or the armed path is untestable.
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

/// MW-3: the dry run lists every orphan and deletes nothing.
///
/// Oracle — live Spark 4.0.1 + Iceberg 1.10.0: `dry_run => true` on a table with two orphans
/// returned **2 rows** and left all five files (3 data + 2 orphans) on disk. Same result shape as
/// the armed run, one row per orphan, `orphan_file_location` a non-nullable string.
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
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'sales.orphans', older_than => {})",
            older_than_two_days_ago_ms()
        ),
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

    // The half that matters: dry-run is the DEFAULT, so nothing moved.
    assert_eq!(
        files_under(&table_dir),
        before,
        "the default is a dry run — not one file may be removed"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orphans").await,
        1
    );
}

/// MW-3: the armed run deletes the orphans **and provably not one live file**.
///
/// "It deleted the orphans" is half a test. This compares the entire table directory before and
/// after, and asserts the difference is exactly the planted set.
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

/// Registry row `ORPHAN-1` — `older_than` is required here and defaulted in Spark.
///
/// Oracle — live Spark 4.0.1: `CALL … remove_orphan_files(table => 't')` with no `older_than`
/// runs, defaulting to `now - 3 days`, and DELETES. This engine refuses.
#[tokio::test]
async fn call_orphan1_requires_an_explicit_older_than() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.need AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let table_dir = wh.path().join("sales").join("need");
    plant_orphans(&table_dir, 1, 10);
    let before = files_under(&table_dir);

    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.remove_orphan_files(table => 'sales.need')",
    )
    .await
    .expect_err("ORPHAN-1: a defaulted cutoff is refused where Spark supplies one");
    let message = err.to_string();
    assert!(
        message.contains("requires an explicit `older_than`"),
        "refusal must name the argument, got: {message}"
    );
    assert_eq!(
        files_under(&table_dir),
        before,
        "a refused call must not have touched the table"
    );
}

/// MW-3: the 24-hour floor, which is PARITY with Spark rather than stricter.
///
/// Oracle — live Spark 4.0.1 + Iceberg 1.10.0, measured across the boundary:
/// `older_than = now` refuses, `now - 23h` refuses, `now - 25h` runs and deletes. Java's floor
/// lives in `RemoveOrphanFilesProcedure`, not the Action API — its own message points callers at
/// the Action API to bypass it — so the fork has no floor and this router carries it.
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

    // Inside the floor: refused. Both `now` and `now - 23h`, the two Spark was measured on.
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

    // Outside it: runs. The control that makes the refusals above mean something.
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

/// MW-3: deferred arguments refuse by name rather than being ignored.
///
/// On a procedure that deletes files, silently ignoring `location` narrowing or an
/// `equal_schemes` mapping would widen the blast radius past what the caller asked for.
#[tokio::test]
async fn call_remove_orphan_files_refuses_deferred_arguments() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.args AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    for argument in [
        "max_concurrent_deletes => 4",
        "file_list_view => 'v'",
        "equal_schemes => map('s3a', 's3')",
        "equal_authorities => map('a', 'b')",
        "prefix_mismatch_mode => 'DELETE'",
        "prefix_listing => true",
    ] {
        let err = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.remove_orphan_files(\
                     table => 'sales.args', older_than => {}, {argument})",
                older_than_two_days_ago_ms()
            ),
        )
        .await
        .expect_err("deferred argument must refuse");
        assert!(
            err.to_string().contains("is not supported in v1"),
            "refusal must name the argument, got: {err}"
        );
    }
}

/// MW-3: `dry_run` takes a boolean literal, not a quoted string.
///
/// Coercing `'false'` would mean a typo silently arming the only procedure here that destroys
/// data. It refuses instead.
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
///
/// Found while writing the pins above: the facade's `mem.ns.events` fixture resolved into
/// `<temp>/repark_ctas/mem/ns/events`, and a dry run there listed **139,179** files left behind by
/// unrelated runs. That path is derived from the catalog, namespace and table NAME alone, so two
/// sessions using the same names share a directory — and orphan removal deletes what one table's
/// metadata does not reference.
///
/// Pinned as a table because the interesting cases are about paths, not about catalogs: the rule
/// must fire on the fallback root and must NOT fire on a namespace that owns its location, which
/// is what every other test in this module relies on.
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

    // A namespace that owns its location is untouched, even under the SAME temp root — this is
    // the case every other pin in this module runs in, so a rule that caught it would be useless.
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
}
