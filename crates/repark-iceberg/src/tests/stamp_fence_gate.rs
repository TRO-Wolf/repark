use std::path::{Path, PathBuf};

const CLAIM: &str = "SiteStamp::claim";
const FENCED_COMMIT: &str = ".commit(tx, catalog)";
const STAMPED_SUMMARY: &str = "stamp.summary(";
const CLAIMING_ARMS: &[&str] = &[
    "repark-iceberg/src/write/merge/snapshot_commit.rs",
    "repark-iceberg/src/write/write_options.rs",
];
const STAMP_OWNERS: &[&str] = &[
    "repark-iceberg/src/write/sink_offsets.rs",
    "repark-iceberg/src/write/sink_offsets/append_fence.rs",
    "repark-iceberg/src/write/sink_offsets/lineage.rs",
];
const OWNED: &[&str] = &[
    ".summary_entries()",
    ".stamp_transaction(",
    "AppendFence::install(",
    "AppendFence::for_starting_mark(",
    "ClaimedStamp {",
];

fn crates_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate sits under crates/")
        .to_path_buf()
}

fn is_test_source(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    name.ends_with("tests.rs")
        || name.ends_with("_pins.rs")
        || name.ends_with("_gate.rs")
        || path.components().any(|part| part.as_os_str() == "tests")
}

fn product_sources(root: &Path) -> Vec<(String, String)> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("a readable source directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                let skipped = path
                    .file_name()
                    .is_some_and(|name| name == "target" || name == "benches");
                if !skipped {
                    pending.push(path);
                }
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && !is_test_source(&path)
            {
                let relative = path
                    .strip_prefix(root)
                    .expect("under the crates root")
                    .to_string_lossy()
                    .replace('\\', "/");
                let text = std::fs::read_to_string(&path).expect("a readable source file");
                found.push((relative, text));
            }
        }
    }
    found.sort();
    found
}

fn violations(sources: &[(String, String)]) -> Vec<String> {
    let mut found = Vec::new();
    for (path, text) in sources {
        let claims = text.matches(CLAIM).count();
        let arm = CLAIMING_ARMS.contains(&path.as_str());
        if claims > 0 && !arm && !STAMP_OWNERS.contains(&path.as_str()) {
            found.push(format!("{path} claims a stamp and is not a listed arm"));
        }
        if arm {
            let commits = text.matches(FENCED_COMMIT).count();
            let summaries = text.matches(STAMPED_SUMMARY).count();
            if claims == 0 || claims != commits || claims != summaries {
                found.push(format!(
                    "{path}: {claims} claims, {summaries} stamped summaries, {commits} fenced commits"
                ));
            }
        }
        if STAMP_OWNERS.contains(&path.as_str()) {
            for line in text.lines() {
                if line.contains(".commit(") && !line.contains("fenced") {
                    found.push(format!("{path} commits past the fence: {}", line.trim()));
                }
            }
        } else {
            for token in OWNED {
                if text.contains(token) {
                    found.push(format!("{path} uses {token} outside the stamp's module"));
                }
            }
        }
    }
    found
}

#[test]
fn every_arm_that_takes_a_stamp_commits_through_the_fence() {
    let sources = product_sources(&crates_root());
    for arm in CLAIMING_ARMS {
        assert!(
            sources.iter().any(|(path, _)| path == arm),
            "{arm} is gone; update the list of arms"
        );
    }
    let found = violations(&sources);
    assert!(found.is_empty(), "{found:#?}");
}

#[test]
fn the_gate_sees_an_arm_that_stamps_its_summary_and_commits_on_the_bare_catalog() {
    let arm = String::from(CLAIMING_ARMS[0]);
    let honest = format!(
        "let stamp = {CLAIM}_isolated(table);\nlet s = {STAMPED_SUMMARY}extra);\nstamp{FENCED_COMMIT}.await"
    );
    assert!(violations(&[(arm.clone(), honest)]).is_empty());
    let bare = format!(
        "let stamp = {CLAIM}_isolated(table);\nlet s = {STAMPED_SUMMARY}extra);\ntx.commit(catalog.as_ref()).await"
    );
    assert_eq!(violations(&[(arm, bare)]).len(), 1);
    let stranger = (
        String::from("repark-spark/src/new_arm.rs"),
        format!("let stamp = {CLAIM}(table);"),
    );
    assert_eq!(violations(&[stranger]).len(), 1);
    let borrowed = (
        String::from("repark-iceberg/src/write/new_arm.rs"),
        String::from("let entries = claimed.summary_entries()?;"),
    );
    assert_eq!(violations(&[borrowed]).len(), 1);
    let unfenced = (
        String::from(STAMP_OWNERS[0]),
        String::from("let committed = tx.commit(catalog.as_ref()).await;"),
    );
    assert_eq!(violations(&[unfenced]).len(), 1);
}
