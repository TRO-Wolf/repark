mod identity;
mod parse;

use std::fs;
use std::path::{Path, PathBuf};

use super::SilverPlan;
use super::SilverRefusal;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/silver/fixtures")
}

fn read_fixture(relative: &str) -> String {
    let path = fixture_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn parse_fixture(relative: &str) -> Result<SilverPlan, SilverRefusal> {
    SilverPlan::parse(&read_fixture(relative))
}

fn toml_files(directory: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
        .map(|entry| entry.expect("fixture entry").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("toml"))
        .collect();
    files.sort();
    files
}

#[test]
fn fixture_counts_meet_the_floor() {
    let positive = toml_files(&fixture_root().join("positive"));
    let negative = toml_files(&fixture_root().join("negative"));
    assert!(
        positive.len() >= 6,
        "need at least 6 positive fixtures, found {}",
        positive.len()
    );
    assert!(
        negative.len() >= 20,
        "need at least 20 negative fixtures, found {}",
        negative.len()
    );
}

#[test]
fn silver_rust_sources_do_not_name_datafusion_iceberg_or_pyo3() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let files = [
        root.join("silver.rs"),
        root.join("silver/plan.rs"),
        root.join("silver/policy.rs"),
        root.join("silver/identity.rs"),
        root.join("silver/explain.rs"),
        root.join("silver/refusal.rs"),
    ];
    for path in files {
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        for needle in ["datafusion", "iceberg", "pyo3", "PyO3"] {
            assert!(!text.contains(needle), "{} names {needle}", path.display());
        }
    }
}
