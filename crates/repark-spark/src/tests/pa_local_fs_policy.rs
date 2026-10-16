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
    let plan = ctx.state().create_logical_plan(&sql).await.unwrap();
    let err = crate::local_fs_ddl::refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
        .unwrap_err()
        .to_string();
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

const CHILD_ENV: &str = "REPARK_PA_LOCAL_FS_CHILD";

fn runs_here(test: &str) -> bool {
    if std::env::var(CHILD_ENV).as_deref() == Ok(test) {
        return true;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--test-threads=1"])
        .env(CHILD_ENV, test)
        .status()
        .unwrap();
    assert!(status.success(), "{test} failed in its own process");
    false
}

struct CwdGuard(std::path::PathBuf);

impl CwdGuard {
    fn enter(dir: &std::path::Path) -> Self {
        let home = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();
        Self(home)
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

fn nested_warehouse() -> (TempDir, TempDir) {
    let base = TempDir::new().unwrap();
    let wh = TempDir::new_in(base.path()).unwrap();
    std::fs::create_dir_all(wh.path().join("a/b")).unwrap();
    std::os::unix::fs::symlink(wh.path().join("a/b"), wh.path().join("deep")).unwrap();
    (base, wh)
}

#[tokio::test]
async fn v01_symlink_then_dot_dot_writes_outside_on_todays_head_and_now_refuses() {
    let (base, wh) = nested_warehouse();
    let (ctx, catalogs) = setup(&wh).await;
    let w = wh.path().to_str().unwrap();
    for sql in [
        copy_to(&format!("{w}/deep/../../out/bare")),
        copy_to(&format!("file://{w}/deep/../../out/url")),
        format!(
            "EXPLAIN ANALYZE {}",
            copy_to(&format!("{w}/deep/../../out/ea"))
        ),
    ] {
        let err = refusal(&ctx, &catalogs, &sql).await;
        assert_policy_refusal(&err, "COPY TO");
    }
    assert_eq!(
        count_objects(&base.path().join("out")),
        0,
        "nothing may land outside"
    );
}

#[tokio::test]
async fn v02_percent_encoded_dot_segments_write_outside_on_todays_head_and_now_refuse() {
    let (base, wh) = nested_warehouse();
    let (ctx, catalogs) = setup(&wh).await;
    let w = wh.path().to_str().unwrap();
    for sql in [
        copy_to(&format!("file://{w}/%2e%2e/out/p1")),
        copy_to(&format!("file://{w}/%2E%2E/out/p2")),
        copy_to(&format!("file://{w}/.%2e/out/p3")),
        copy_to(&format!("file:{w}/%2e%2e/out/p4")),
        copy_to(&format!("file://localhost{w}/%2e%2e/out/p5")),
        format!(
            "EXPLAIN ANALYZE {}",
            copy_to(&format!("file://{w}/%2e%2e/out/p6"))
        ),
    ] {
        let err = refusal(&ctx, &catalogs, &sql).await;
        assert_policy_refusal(&err, "COPY TO");
    }
    let create = format!(
        "CREATE EXTERNAL TABLE v02_ext (a INT) STORED AS PARQUET LOCATION 'file://{w}/%2e%2e/out/pc/'"
    );
    let plan = ctx.state().create_logical_plan(&create).await.unwrap();
    let err = crate::local_fs_ddl::refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
        .unwrap_err()
        .to_string();
    assert_policy_refusal(&err, "CREATE EXTERNAL TABLE");
    assert_eq!(
        count_objects(&base.path().join("out")),
        0,
        "nothing may land outside"
    );
}

#[tokio::test]
async fn v03_file_relative_with_the_cwd_in_a_warehouse_writes_outside_on_todays_head() {
    if !runs_here(
        "tests::pa_local_fs_policy::v03_file_relative_with_the_cwd_in_a_warehouse_writes_outside_on_todays_head",
    ) {
        return;
    }
    let (base, wh) = nested_warehouse();
    let (ctx, catalogs) = setup(&wh).await;
    let relative = base.path().to_str().unwrap().trim_start_matches('/');
    let home = std::env::current_dir().unwrap();
    {
        let _cwd = CwdGuard::enter(wh.path());
        for location in [
            format!("file:{relative}/out/fc"),
            format!("file:./{relative}/out/fc4"),
        ] {
            let err = refusal(&ctx, &catalogs, &copy_to(&location)).await;
            assert_policy_refusal(&err, "COPY TO");
        }
    }
    assert_eq!(
        std::env::current_dir().unwrap(),
        home,
        "the cwd is restored"
    );
    assert_eq!(
        count_objects(&base.path().join("out")),
        0,
        "nothing may land outside"
    );
}

#[tokio::test]
async fn a_leading_space_is_read_from_the_cwd_and_writes_outside_on_todays_head() {
    if !runs_here(
        "tests::pa_local_fs_policy::a_leading_space_is_read_from_the_cwd_and_writes_outside_on_todays_head",
    ) {
        return;
    }
    let (base, wh) = nested_warehouse();
    let (ctx, catalogs) = setup(&wh).await;
    let cwd = base.path().join("cwd");
    std::fs::create_dir(&cwd).unwrap();
    let home = std::env::current_dir().unwrap();
    {
        let _cwd = CwdGuard::enter(&cwd);
        let location = format!(" {}/trap", wh.path().to_str().unwrap());
        let err = refusal(&ctx, &catalogs, &copy_to(&location)).await;
        assert_policy_refusal(&err, "COPY TO");
    }
    assert_eq!(
        std::env::current_dir().unwrap(),
        home,
        "the cwd is restored"
    );
    assert_eq!(count_objects(&cwd), 0, "nothing may land outside");
}

#[tokio::test]
async fn a_warehouse_registered_through_a_symlink_still_writes_inside_it() {
    let scratch = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&scratch).await;
    let base = TempDir::new().unwrap();
    let real = base.path().join("real");
    std::fs::create_dir_all(real.join("r2")).unwrap();
    let linked = base.path().join("whl");
    std::os::unix::fs::symlink(&real, &linked).unwrap();
    std::os::unix::fs::symlink(real.join("r2"), real.join("inner")).unwrap();
    catalogs.note_local_warehouse_root(linked.to_str().unwrap());
    for (location, landed) in [
        (linked.join("direct"), real.join("direct")),
        (linked.join("inner/x"), real.join("r2/x")),
        (linked.join("m1/m2/m3/x"), real.join("m1/m2/m3/x")),
    ] {
        answer(&ctx, &catalogs, &copy_to(location.to_str().unwrap())).await;
        assert!(
            count_objects(&landed) > 0,
            "{} must write",
            location.display()
        );
    }
}

#[tokio::test]
async fn an_ancestor_whose_metadata_read_fails_refuses_with_the_policy_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    std::fs::write(wh.path().join("plain"), b"x").unwrap();
    let dest = wh.path().join("plain/child/x");
    let err = refusal(&ctx, &catalogs, &copy_to(dest.to_str().unwrap())).await;
    assert_policy_refusal(&err, "COPY TO");
}

#[tokio::test]
async fn rv01_a_glob_character_in_the_warehouse_path_writes_outside_on_todays_head_and_now_refuses()
{
    for name in ["wh[1]", "wh?", "wh*"] {
        let scratch = TempDir::new().unwrap();
        let (ctx, mut catalogs) = setup(&scratch).await;
        let base = TempDir::new().unwrap();
        let root = base.path().join(name);
        std::fs::create_dir(&root).unwrap();
        catalogs.note_local_warehouse_root(root.to_str().unwrap());
        let b = base.path().to_str().unwrap();
        for sql in [
            copy_to(&format!("{b}/out/gr")),
            copy_to(&format!("{b}/sib/gr2")),
            format!(
                "EXPLAIN ANALYZE {}",
                copy_to(&format!("file://{b}/out/gea"))
            ),
            copy_to(root.join("gin").to_str().unwrap()),
        ] {
            let err = refusal(&ctx, &catalogs, &sql).await;
            assert_policy_refusal(&err, "COPY TO");
        }
        assert_eq!(
            count_objects(base.path()),
            0,
            "{name}: nothing anywhere under the base"
        );
        let encoded = format!("file://{b}/{}/gin2", name.replace('?', "%3F"));
        answer(&ctx, &catalogs, &copy_to(&encoded)).await;
        assert!(
            count_objects(&root.join("gin2")) > 0,
            "{name}: the root itself writes"
        );
    }
}

#[tokio::test]
async fn rv02_a_dot_dot_over_a_symlink_above_the_warehouse_writes_inside() {
    let base = TempDir::new().unwrap();
    std::fs::create_dir_all(base.path().join("o1/o2")).unwrap();
    std::os::unix::fs::symlink(base.path().join("o1/o2"), base.path().join("lnk")).unwrap();
    let wh = TempDir::new_in(base.path()).unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let b = base.path().to_str().unwrap();
    let name = wh.path().file_name().unwrap().to_str().unwrap();
    for (location, landed) in [
        (
            format!("{b}/lnk/../{name}/anc_in"),
            wh.path().join("anc_in"),
        ),
        (
            format!("file://{b}/lnk/../{name}/anc_in2"),
            wh.path().join("anc_in2"),
        ),
    ] {
        answer(&ctx, &catalogs, &copy_to(&location)).await;
        assert!(
            count_objects(&landed) > 0,
            "{location} must land at {}",
            landed.display()
        );
    }
    assert_eq!(count_objects(&base.path().join("o1")), 0, "nothing in o1");
}

#[tokio::test]
async fn rv03_private_use_characters_in_a_file_url_write_inside() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let w = wh.path().to_str().unwrap();
    for (location, landed) in [
        (
            format!("file://{w}/%EE%80%80%EE%80%80/pua"),
            wh.path().join("\u{E000}\u{E000}/pua"),
        ),
        (
            format!("file://{w}/\u{E000}\u{E001}\u{E002}/pua3"),
            wh.path().join("\u{E000}\u{E001}\u{E002}/pua3"),
        ),
    ] {
        answer(&ctx, &catalogs, &copy_to(&location)).await;
        assert!(
            count_objects(&landed) > 0,
            "{location} must land at {}",
            landed.display()
        );
    }
}

#[tokio::test]
async fn the_cells_of_ruling_114_write_where_the_written_path_lands() {
    let (_base, wh) = nested_warehouse();
    let (ctx, catalogs) = setup(&wh).await;
    std::fs::create_dir(wh.path().join("sib")).unwrap();
    std::os::unix::fs::symlink(wh.path().join("sib"), wh.path().join("s2")).unwrap();
    let w = wh.path().to_str().unwrap();
    for (location, landed) in [
        (format!("{w}/deep/../dd_in"), wh.path().join("dd_in")),
        (
            format!("{w}/deep/../m1/dd_in2"),
            wh.path().join("m1/dd_in2"),
        ),
        (format!("{w}/s2/../sb_in"), wh.path().join("sb_in")),
        (format!("file://{w}/x/%2e%2e/pin2"), wh.path().join("pin2")),
        (format!("{w}/x/../pin_bare"), wh.path().join("pin_bare")),
        (
            format!("file://{w}/x/../pin_url"),
            wh.path().join("pin_url"),
        ),
    ] {
        answer(&ctx, &catalogs, &copy_to(&location)).await;
        assert!(
            count_objects(&landed) > 0,
            "{location} must land at {}",
            landed.display()
        );
    }
    assert_eq!(
        count_objects(&wh.path().join("a")),
        0,
        "nothing through the symlink target"
    );
    assert!(
        !wh.path().join("x").exists(),
        "the absent directory stays absent"
    );
}
