use std::path::Path;
use std::sync::Arc;

use repark_core::{ReparkSession, SqlDialect};
use repark_spark::SparkExtension;
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const ALLOW_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

struct Door {
    session: ReparkSession,
    warehouse: TempDir,
}

async fn ansi_door(allow: bool) -> Door {
    let warehouse = TempDir::new().unwrap();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let mut builder = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect);
    if allow {
        builder = builder.config(ALLOW_KEY, "true");
    }
    let session = builder.build().unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    Door { session, warehouse }
}

impl Door {
    fn wh(&self) -> &Path {
        self.warehouse.path()
    }

    async fn answer(&self, sql: &str) {
        self.session
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    async fn refusal(&self, sql: &str, surface: &str) {
        let err = match self.session.sql(sql).await {
            Ok(frame) => match frame.collect().await {
                Ok(_) => panic!("must refuse, but answered: {sql}"),
                Err(error) => {
                    panic!("must refuse before execution, but failed at collect: {error}")
                }
            },
            Err(error) => error.to_string(),
        };
        assert!(err.contains(ALLOW_KEY), "must name the conf: {err}");
        assert!(err.contains(surface), "must name the surface: {err}");
    }
}

fn copy_to(location: &str) -> String {
    format!("COPY (SELECT 1 AS a) TO '{location}' STORED AS PARQUET")
}

fn files_under(root: &Path) -> usize {
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0;
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                count += 1;
            }
        }
    }
    count
}

fn linked_outside(door: &Door) -> TempDir {
    let outside = TempDir::new().unwrap();
    std::os::unix::fs::symlink(outside.path(), door.wh().join("link")).unwrap();
    outside
}

#[tokio::test]
async fn pa02_ansi_explain_analyze_copy_outside_refuses_and_writes_nothing() {
    let door = ansi_door(false).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("wrapped");
    let sql = format!("EXPLAIN ANALYZE {}", copy_to(dest.to_str().unwrap()));
    door.refusal(&sql, "COPY TO").await;
    assert_eq!(files_under(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa02_ansi_explain_analyze_copy_controls_still_write() {
    let door = ansi_door(false).await;
    let inside = door.wh().join("wrapped_inside");
    door.answer(&format!(
        "EXPLAIN ANALYZE {}",
        copy_to(inside.to_str().unwrap())
    ))
    .await;
    assert!(files_under(&inside) > 0, "the warehouse control must write");

    let open = ansi_door(true).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("wrapped");
    open.answer(&format!(
        "EXPLAIN ANALYZE {}",
        copy_to(dest.to_str().unwrap())
    ))
    .await;
    assert!(files_under(&dest) > 0, "the opt-in control must write");
}

#[tokio::test]
async fn pa02_ansi_plain_explain_copy_outside_still_answers_without_writing() {
    let door = ansi_door(false).await;
    let outside = TempDir::new().unwrap();
    let dest = outside.path().join("explained");
    door.answer(&format!("EXPLAIN {}", copy_to(dest.to_str().unwrap())))
        .await;
    assert_eq!(
        files_under(outside.path()),
        0,
        "plain EXPLAIN must not write"
    );
}

#[tokio::test]
async fn pa03_ansi_missing_parents_through_a_warehouse_symlink_refuse() {
    let door = ansi_door(false).await;
    let outside = linked_outside(&door);
    for tail in ["link/new/export", "link/m1/m2/m3/export"] {
        let dest = door.wh().join(tail);
        door.refusal(&copy_to(dest.to_str().unwrap()), "COPY TO")
            .await;
    }
    let location = door.wh().join("link/m1/m2/ext/");
    door.refusal(
        &format!(
            "CREATE EXTERNAL TABLE pa03_ext (a INT) STORED AS PARQUET LOCATION '{}'",
            location.to_str().unwrap()
        ),
        "CREATE EXTERNAL TABLE",
    )
    .await;
    assert_eq!(files_under(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa03_ansi_dot_dot_and_dangling_symlink_refuse() {
    let door = ansi_door(false).await;
    let outside = TempDir::new().unwrap();
    std::os::unix::fs::symlink(outside.path().join("not_yet"), door.wh().join("dangling")).unwrap();
    let escaped = format!("{}/missing/../../escaped", door.wh().to_str().unwrap());
    door.refusal(&copy_to(&escaped), "COPY TO").await;
    door.refusal(
        &copy_to(door.wh().join("dangling").to_str().unwrap()),
        "COPY TO",
    )
    .await;
    assert_eq!(files_under(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn pa03_ansi_controls_still_write() {
    let door = ansi_door(false).await;
    std::fs::create_dir(door.wh().join("real")).unwrap();
    std::os::unix::fs::symlink(door.wh().join("real"), door.wh().join("inner")).unwrap();
    door.answer(&copy_to(
        door.wh().join("inner/m1/m2/m3/export").to_str().unwrap(),
    ))
    .await;
    assert!(files_under(&door.wh().join("real/m1/m2/m3/export")) > 0);
    door.answer(&copy_to(door.wh().join("m1/m2/m3/plain").to_str().unwrap()))
        .await;
    assert!(files_under(&door.wh().join("m1/m2/m3/plain")) > 0);

    let open = ansi_door(true).await;
    let outside = linked_outside(&open);
    open.answer(&copy_to(
        open.wh().join("link/m1/m2/m3/export").to_str().unwrap(),
    ))
    .await;
    assert!(files_under(&outside.path().join("m1/m2/m3/export")) > 0);
}

#[tokio::test]
async fn pa04_ansi_non_ascii_relative_locations_refuse_with_the_policy_error() {
    let door = ansi_door(false).await;
    for location in ["aaaaé", "aaaaaaé", "file://aaaaaaaaé"] {
        door.refusal(&copy_to(location), "COPY TO").await;
    }
    assert!(!Path::new("aaaaé").exists());
    assert!(!Path::new("aaaaaaé").exists());
    assert!(!Path::new("aaaaaaaaé").exists());
}

#[tokio::test]
async fn pa04_ansi_non_ascii_controls() {
    let door = ansi_door(false).await;
    let outside = TempDir::new().unwrap();
    let out = outside.path().to_str().unwrap();
    for location in [format!("{out}/é"), format!("file://localhost{out}/aaé")] {
        door.refusal(&copy_to(&location), "COPY TO").await;
    }
    assert_eq!(files_under(outside.path()), 0, "nothing may land outside");
    let inside = door.wh().to_str().unwrap().to_string();
    for (location, landed) in [
        (format!("{inside}/é"), door.wh().join("é")),
        (
            format!("file://localhost{inside}/aaé"),
            door.wh().join("aaé"),
        ),
    ] {
        door.answer(&copy_to(&location)).await;
        assert!(files_under(&landed) > 0, "{location} must write");
    }

    let open = ansi_door(true).await;
    let opened = TempDir::new().unwrap();
    let dest = opened.path().join("aaaaaaé");
    open.answer(&copy_to(&format!(
        "file://localhost{}",
        dest.to_str().unwrap()
    )))
    .await;
    assert!(files_under(&dest) > 0, "the opt-in control must write");
}
