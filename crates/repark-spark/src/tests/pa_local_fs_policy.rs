use super::super::*;
use super::common::*;

fn copy_to(location: &str) -> String {
    format!("COPY (SELECT * FROM src) TO '{location}' STORED AS PARQUET")
}

fn linked_outside(wh: &TempDir) -> TempDir {
    let outside = TempDir::new().unwrap();
    std::os::unix::fs::symlink(outside.path(), wh.path().join("link")).unwrap();
    outside
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("must refuse, but answered: {sql}"),
            Err(error) => panic!("must refuse before execution, but failed at collect: {error}"),
        },
        Err(error) => error.to_string(),
    }
}

fn assert_policy_refusal(err: &str, surface: &str) {
    assert!(
        err.contains(repark_functions::cardinality::ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
        "must name the conf: {err}"
    );
    assert!(err.contains(surface), "must name the surface: {err}");
}

async fn answer(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) {
    execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[tokio::test]
async fn pa02_explain_analyze_copy_outside_refuses_and_writes_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("wrapped");
    let sql = format!("EXPLAIN ANALYZE {}", copy_to(dest.to_str().unwrap()));
    let err = refusal(&ctx, &catalogs, &sql).await;
    assert_policy_refusal(&err, "COPY TO");
    assert_eq!(count_objects(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa02_explain_analyze_copy_under_the_warehouse_still_writes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let dest = wh.path().join("wrapped_inside");
    let sql = format!("EXPLAIN ANALYZE {}", copy_to(dest.to_str().unwrap()));
    answer(&ctx, &catalogs, &sql).await;
    assert!(count_objects(&dest) > 0, "the warehouse control must write");
}

#[tokio::test]
async fn pa02_explain_analyze_copy_outside_writes_under_the_opt_in() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_local_fs_ddl(&wh).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("wrapped");
    let sql = format!("EXPLAIN ANALYZE {}", copy_to(dest.to_str().unwrap()));
    answer(&ctx, &catalogs, &sql).await;
    assert!(count_objects(&dest) > 0, "the opt-in control must write");
}

#[tokio::test]
async fn pa02_plain_explain_copy_outside_still_answers_without_writing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("explained");
    let sql = format!("EXPLAIN {}", copy_to(dest.to_str().unwrap()));
    answer(&ctx, &catalogs, &sql).await;
    assert_eq!(
        count_objects(outside.path()),
        0,
        "plain EXPLAIN must not write"
    );
}

#[tokio::test]
async fn pa03_one_missing_parent_through_a_warehouse_symlink_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = linked_outside(&wh);
    let dest = wh.path().join("link").join("new").join("export");
    let err = refusal(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert_policy_refusal(&err, "COPY TO");
    assert_eq!(count_objects(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa03_three_missing_parents_through_a_warehouse_symlink_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = linked_outside(&wh);
    let dest = wh.path().join("link/m1/m2/m3/export");
    let err = refusal(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert_policy_refusal(&err, "COPY TO");
    assert_eq!(count_objects(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa03_create_external_table_through_a_warehouse_symlink_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = linked_outside(&wh);
    let location = wh.path().join("link/m1/m2/ext/");
    let sql = format!(
        "CREATE EXTERNAL TABLE pa03_ext (a INT) STORED AS PARQUET LOCATION '{}'",
        location.to_str().unwrap()
    );
    let err = refusal(&ctx, &catalogs, &sql).await;
    assert_policy_refusal(&err, "CREATE EXTERNAL TABLE");
    assert_eq!(count_objects(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa03_a_symlink_that_stays_inside_the_warehouse_still_writes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    std::fs::create_dir(wh.path().join("real")).unwrap();
    std::os::unix::fs::symlink(wh.path().join("real"), wh.path().join("inner")).unwrap();
    let dest = wh.path().join("inner/m1/m2/m3/export");
    answer(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert!(
        count_objects(&wh.path().join("real/m1/m2/m3/export")) > 0,
        "the in-warehouse symlink control must write"
    );
}

#[tokio::test]
async fn pa03_missing_parents_under_the_warehouse_still_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let dest = wh.path().join("m1/m2/m3/export");
    answer(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert!(count_objects(&dest) > 0, "the warehouse control must write");
}

#[tokio::test]
async fn pa03_dot_dot_after_a_missing_parent_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let dest = format!("{}/missing/../../escaped", wh.path().to_str().unwrap());
    let err = refusal(&ctx, &catalogs, &copy_to(&dest)).await;
    assert_policy_refusal(&err, "COPY TO");
}

#[tokio::test]
async fn pa03_dangling_symlink_in_the_warehouse_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = TempDir::new().unwrap();
    std::os::unix::fs::symlink(outside.path().join("not_yet"), wh.path().join("dangling")).unwrap();
    let dest = wh.path().join("dangling");
    let err = refusal(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert_policy_refusal(&err, "COPY TO");
    assert_eq!(count_objects(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa03_missing_parents_through_a_symlink_write_under_the_opt_in() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_local_fs_ddl(&wh).await;
    let outside = linked_outside(&wh);
    let dest = wh.path().join("link/m1/m2/m3/export");
    answer(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert!(
        count_objects(&outside.path().join("m1/m2/m3/export")) > 0,
        "the opt-in control must write"
    );
}

#[tokio::test]
async fn pa04_non_ascii_relative_locations_refuse_with_the_policy_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for location in ["aaaaé", "aaaaaaé", "file://aaaaaaaaé", "file:aaaé"] {
        let err = refusal(&ctx, &catalogs, &copy_to(location)).await;
        assert_policy_refusal(&err, "COPY TO");
    }
    assert!(!std::path::Path::new("aaaaé").exists());
    assert!(!std::path::Path::new("aaaaaaé").exists());
    assert!(!std::path::Path::new("aaaaaaaaé").exists());
}

#[tokio::test]
async fn pa04_non_ascii_locations_outside_refuse_and_under_the_warehouse_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let outside = TempDir::new().unwrap();
    let out = outside.path().to_str().unwrap();
    let inside = wh.path().to_str().unwrap();
    for location in [
        format!("{out}/é"),
        format!("file://localhost{out}/aaé"),
        format!("FILE://LOCALHOST{out}/日本"),
    ] {
        let err = refusal(&ctx, &catalogs, &copy_to(&location)).await;
        assert_policy_refusal(&err, "COPY TO");
    }
    assert_eq!(count_objects(outside.path()), 0, "nothing may land outside");
    for (location, landed) in [
        (format!("{inside}/é"), wh.path().join("é")),
        (
            format!("file://localhost{inside}/aaé"),
            wh.path().join("aaé"),
        ),
        (format!("file://{inside}/日本"), wh.path().join("日本")),
    ] {
        answer(&ctx, &catalogs, &copy_to(&location)).await;
        assert!(count_objects(&landed) > 0, "{location} must write");
    }
}

#[tokio::test]
async fn pa04_non_ascii_locations_outside_write_under_the_opt_in() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_local_fs_ddl(&wh).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("aaaaaaé");
    answer(
        &ctx,
        &catalogs,
        &copy_to(&format!("file://localhost{}", dest.to_str().unwrap())),
    )
    .await;
    assert!(count_objects(&dest) > 0, "the opt-in control must write");
}
