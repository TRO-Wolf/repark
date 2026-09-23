use std::os::unix::fs::PermissionsExt;

use super::super::*;
use super::call_orphan_scope::{
    call_rows, ctas, fallback_session, file_scheme_table, plant, prefix_conflict_message,
    referenced_data_file, register_file_list, submit,
};
use super::common::*;

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_returns_each_spelling_verbatim_sorted_once() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let live = referenced_data_file(&table_dir);
    let bare = plant(&table_dir, "orphan-bare.parquet", 10);
    let spelled = plant(&table_dir, "orphan-spelled.parquet", 10);
    let spelled_uri = format!("file://{}", spelled.display());
    register_file_list(
        &session,
        &[
            (spelled_uri.clone(), 0),
            (bare.display().to_string(), 0),
            (bare.display().to_string(), 0),
            (format!("file:{}", live.display()), 0),
        ],
    )
    .await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(\
             table => 'ns.t', dry_run => true, file_list_view => 'v')",
    )
    .await
    .expect("dry run");
    assert_eq!(
        listed,
        vec![bare.display().to_string(), spelled_uri],
        "each orphan once, sorted, in the view's own spelling; a `file:` spelling of the live \
         file is still the live file"
    );
    assert!(bare.exists() && spelled.exists() && live.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_gc_refusals_match_the_listing_path() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);
    register_file_list(&session, &[(orphan.display().to_string(), 0)]).await;

    for (value, expected) in [
        (
            "false",
            "DataInvalid => Cannot delete orphan files: GC is disabled (deleting files may \
             corrupt other tables)",
        ),
        (
            "maybe",
            "DataInvalid => Invalid boolean value 'maybe' for table property 'gc.enabled'",
        ),
    ] {
        submit(
            &session,
            &format!("ALTER TABLE ice.ns.t SET TBLPROPERTIES ('gc.enabled' = '{value}')"),
        )
        .await;
        let view = call_rows(
            &session,
            "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
        )
        .await
        .expect_err("gc.enabled gates the view path");
        let listing = call_rows(
            &session,
            "CALL ice.system.remove_orphan_files(table => 'ns.t')",
        )
        .await
        .expect_err("gc.enabled gates the listing path");
        assert!(view.starts_with(expected), "{value}: {view}");
        assert_eq!(view, listing, "{value}: both doors answer with one string");
    }
    assert!(orphan.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_refuses_a_malformed_view() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);

    for (view, select, expected) in [
        (
            "no_stamp",
            format!("SELECT '{}' AS file_path", orphan.display()),
            "CALL remove_orphan_files `file_list_view` has no `last_modified` column; the view \
             must carry file_path STRING and last_modified TIMESTAMP",
        ),
        (
            "int_path",
            "SELECT 7 AS file_path, CAST(from_unixtime(0) AS TIMESTAMP) AS last_modified"
                .to_string(),
            "Invalid file_path column: Int32 is not a string",
        ),
    ] {
        let frame = session.sql(&select).await.unwrap();
        session
            .create_or_replace_temp_view_from(view, &frame)
            .unwrap();
        let err = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => '{view}')"
            ),
        )
        .await
        .expect_err("a malformed view refuses");
        assert!(err.contains(expected), "{view}: {err}");
    }
    assert!(orphan.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_skips_a_null_last_modified() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let untimed = plant(&table_dir, "orphan-untimed.parquet", 10);
    let timed = plant(&table_dir, "orphan-timed.parquet", 10);
    let frame = session
        .sql(&format!(
            "SELECT '{}' AS file_path, CAST(NULL AS TIMESTAMP) AS last_modified \
             UNION ALL SELECT '{}' AS file_path, CAST(from_unixtime(0) AS TIMESTAMP) AS \
             last_modified",
            untimed.display(),
            timed.display()
        ))
        .await
        .unwrap();
    session
        .create_or_replace_temp_view_from("v", &frame)
        .unwrap();

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
    )
    .await
    .expect("armed file_list_view runs");
    assert_eq!(listed, vec![timed.display().to_string()]);
    assert!(!timed.exists());
    assert!(
        untimed.exists(),
        "a row with no last_modified is never a candidate"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_honours_the_prefix_mismatch_mode() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let (_, live) = file_scheme_table(&session, &warehouse).await;
    register_file_list(&session, &[(live.display().to_string(), 0)]).await;

    for (mode, expected) in [
        ("IGNORE", Vec::new()),
        ("DELETE", vec![live.display().to_string()]),
    ] {
        let listed = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'fq.t', dry_run => true, \
                 file_list_view => 'v', prefix_mismatch_mode => '{mode}')"
            ),
        )
        .await
        .expect("a non-error mode answers rows");
        assert_eq!(listed, expected, "{mode}");
    }
    assert!(live.exists(), "dry_run deletes nothing");
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_applies_equal_schemes() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let (table_dir, live) = file_scheme_table(&session, &warehouse).await;
    register_file_list(&session, &[(live.display().to_string(), 0)]).await;

    let mapped = ", equal_schemes => map('s3, file', 'x')";
    let view = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'fq.t', file_list_view => 'v'{mapped})"
        ),
    )
    .await
    .expect_err("the mapped valid scheme still conflicts with a bare path");
    let listing = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'fq.t', location => '{}'{mapped})",
            table_dir.display()
        ),
    )
    .await
    .expect_err("the listing path maps the same way");
    assert_eq!(view, prefix_conflict_message("(x, )"));
    assert_eq!(view, listing);

    for scheme in ["s3a", "s3n"] {
        register_file_list(
            &session,
            &[(format!("{scheme}://bucket{}", live.display()), 0)],
        )
        .await;
        let err = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'fq.t', dry_run => true, \
                 file_list_view => 'v', location => '{scheme}://bucket{}')",
                table_dir.display()
            ),
        )
        .await
        .expect_err("the scheme folds to s3 by default and still differs from file");
        assert_eq!(err, prefix_conflict_message("(file, s3)"), "{scheme}");
    }
    assert!(live.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_reports_a_failed_delete() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir.join("locked"), "orphan-file.parquet", 10);
    let locked = orphan.parent().unwrap().to_path_buf();
    register_file_list(&session, &[(orphan.display().to_string(), 0)]).await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

    let err = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
    )
    .await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = err.expect_err("a delete the filesystem refuses is reported, not swallowed");
    assert!(
        err.contains(&format!(
            "CALL remove_orphan_files deleted 0 of 1 orphan files; 1 could not be removed. \
             First failure: `{}`",
            orphan.display()
        )),
        "{err}"
    );
    assert!(orphan.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_other_table_walk_is_scoped_to_the_fallback_policy() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs, root) = setup_strict_catalog(&warehouse).await;
    let namespace_dir = format!("{root}/ns");
    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.ns LOCATION '{namespace_dir}'"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut orphans = Vec::new();
    for table in ["a", "b"] {
        execute(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE glue_like.ns.{table} AS SELECT 1 AS id"),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let table_dir = std::path::Path::new(&namespace_dir).join(table);
        orphans.push(plant(&table_dir, "orphan-file.parquet", 10));
    }

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL glue_like.system.remove_orphan_files(\
                 table => 'ns.a', location => '{namespace_dir}', dry_run => true)"
        ),
    )
    .await
    .expect("a RequireExplicitLocation catalog does not run the other-table walk")
    .collect()
    .await
    .unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 2, "both tables' orphans are listed");
    assert!(orphans.iter().all(|orphan| orphan.exists()));
}
