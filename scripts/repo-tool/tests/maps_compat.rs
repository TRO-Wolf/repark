use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

fn git(root: &Path, arguments: &[&str]) -> Output {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn source_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned()
}

fn fixture(text: &str) -> TempDir {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    fs::write(root.path().join("map.md"), text).unwrap();
    fs::write(root.path().join("real.md"), "# Real\n").unwrap();
    git(root.path(), &["add", "."]);
    root
}

fn python(root: &Path) -> Output {
    Command::new("python3").arg("-c").arg(
        "import sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); import sync_map_md; sys.exit(sync_map_md.run(Path(sys.argv[2]), fix=False, strict=False))"
    ).arg(source_root().join("scripts")).arg(root).output().unwrap()
}

fn rust(root: &Path, snapshot: &str, command: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_repark-repo"))
        .arg("--repo")
        .arg(root)
        .arg("--snapshot")
        .arg(snapshot)
        .args(command)
        .output()
        .unwrap()
}

fn assert_agree(root: &Path, expected: i32) {
    let python = python(root);
    assert_eq!(
        python.status.code(),
        Some(expected),
        "{}",
        String::from_utf8_lossy(&python.stderr)
    );
    for snapshot in ["worktree", "index"] {
        let rust = rust(root, snapshot, &["maps", "--check"]);
        assert_eq!(
            rust.status.code(),
            python.status.code(),
            "{snapshot}: {} {}",
            String::from_utf8_lossy(&rust.stdout),
            String::from_utf8_lossy(&rust.stderr)
        );
    }
}

#[test]
fn legacy_link_classes_match_python() {
    let cases = [
        ("html", "<details>\n[x](missing.md)\n</details>\n"),
        ("comment", "<!-- [x](missing.md) -->\n"),
        ("line_separator", "`open\u{85}[x](missing.md)`close\n"),
        ("indented", "    [x](missing.md)\n"),
        ("escaped", "\\[x](missing.md)\n"),
        ("double_ticks", "``[x](missing.md)``\n"),
        ("missing_parent", "[x](missing/../real.md)\n"),
        ("file_parent", "[x](real.md/../real.md)\n"),
        ("html_absolute", "<table>\n[x](/real.md)\n</table>\n"),
        (
            "html_duplicate",
            "<div>\n- [a](real.md)\n- [b](real.md)\n</div>\n",
        ),
        (
            "wrapped_duplicate",
            "<div>\n- first\n  [a](real.md)\n- second\n  [b](real.md)\n</div>\n",
        ),
        (
            "table",
            "| First | Second |\n| --- | --- |\n| text \\`foo\\` | value |\n| next | [x](/missing.md) |\n",
        ),
    ];
    let mut mismatches = Vec::new();
    for (name, text) in cases {
        let root = fixture(text);
        let python = python(root.path());
        assert_eq!(
            python.status.code(),
            Some(1),
            "Python fixture {name}: {}",
            String::from_utf8_lossy(&python.stderr)
        );
        for snapshot in ["worktree", "index"] {
            let rust = rust(root.path(), snapshot, &["maps", "--check"]);
            if rust.status.code() != Some(1) {
                mismatches.push(format!(
                    "{name}/{snapshot}: {} {}",
                    String::from_utf8_lossy(&rust.stdout),
                    String::from_utf8_lossy(&rust.stderr)
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn real_map_corpus_and_reported_table_link_match_python() {
    let source = source_root();
    let baseline = python(&source);
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    let current = rust(&source, "worktree", &["maps", "--check"]);
    assert_eq!(
        current.status.code(),
        baseline.status.code(),
        "{}",
        String::from_utf8_lossy(&current.stdout)
    );
    let root = fixture("# Root\n");
    let tracked = git(&source, &["ls-files", "-z"]);
    for relative in String::from_utf8(tracked.stdout)
        .unwrap()
        .split('\0')
        .filter(|path| !path.is_empty())
    {
        let destination = root.path().join(relative);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        let bytes = if relative == "map.md" || relative.ends_with("/map.md") {
            fs::read(source.join(relative)).unwrap()
        } else {
            Vec::new()
        };
        fs::write(destination, bytes).unwrap();
    }
    git(root.path(), &["add", "."]);
    assert_agree(root.path(), 0);
    let path = root.path().join("scripts/map.md");
    let original = fs::read_to_string(&path).unwrap();
    assert!(original.contains("(../docs/port/census.md)"));
    for target in ["(/docs/port/census.md)", "(../docs/port/c874-missing.md)"] {
        fs::write(
            &path,
            original
                .lines()
                .map(|line| {
                    if line.starts_with("| A census cohort") {
                        line.replace("(../docs/port/census.md)", target)
                    } else {
                        line.to_owned()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        git(root.path(), &["add", "scripts/map.md"]);
        assert_agree(root.path(), 1);
    }
}

#[cfg(unix)]
#[test]
fn tracked_backslash_does_not_break_any_checker() {
    let root = fixture("# Root\n");
    fs::write(root.path().join("back\\slash.txt"), "tracked").unwrap();
    fs::create_dir_all(root.path().join("task/ledgers/staging")).unwrap();
    let baselines: std::collections::BTreeMap<String, (usize, bool)> =
        serde_json::from_str(include_str!("../../ledger_grammar_exceptions.json")).unwrap();
    for (name, (ceiling, _)) in baselines {
        let mut rows = String::new();
        for number in 1..=ceiling {
            writeln!(rows, "| C-{number:03} | legacy | PROVEN | proof |").unwrap();
        }
        fs::write(root.path().join("task/ledgers/staging").join(name), rows).unwrap();
    }
    git(root.path(), &["add", "."]);
    assert_agree(root.path(), 0);
    for (command, script) in [
        ("docs-links", "check_docs_links.py"),
        ("ledger-grammar", "check_ledger_grammar.py"),
    ] {
        let expected = Command::new("python3")
            .arg(source_root().join("scripts").join(script))
            .arg("--repo")
            .arg(root.path())
            .output()
            .unwrap();
        assert!(
            expected.status.success(),
            "{}",
            String::from_utf8_lossy(&expected.stderr)
        );
        let output = rust(root.path(), "worktree", &["checks", command]);
        assert_eq!(
            output.status.code(),
            expected.status.code(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn legacy_valid_links_code_and_real_parent_agree() {
    for text in [
        "[x](real.md)\n",
        "`[x](missing.md)`\n",
        "```markdown\n[x](missing.md)\n```\n",
        "[x](child/../real.md)\n",
    ] {
        let root = fixture(text);
        fs::create_dir(root.path().join("child")).unwrap();
        fs::write(root.path().join("child/file.txt"), "tracked").unwrap();
        git(root.path(), &["add", "."]);
        assert_agree(root.path(), 0);
    }
}
