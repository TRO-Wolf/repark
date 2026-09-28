use serde_json::Value;
use std::fmt::Write as _;
use std::{fs, path::Path, process::Command};

const LEDGER: &str = "task/ledgers/staging/demo-ledger.md";
const ROWS: &str = "| C-002 | second | PROVEN | proof |\n| C-001 | first | **PROVEN** (measured) | proof |\n| C-003 | third | OPEN | waiting |\n";

fn git(root: &Path, arguments: &[&str]) {
    let result = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn write(root: &Path, path: &str, text: &str) {
    let file = root.join(path);
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(file, text).unwrap();
}

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    let policy: std::collections::BTreeMap<String, (usize, bool)> =
        serde_json::from_str(include_str!("../../ledger_grammar_exceptions.json")).unwrap();
    for (name, (ceiling, _)) in policy {
        let mut rows = String::new();
        for number in 1..=ceiling {
            writeln!(rows, "| C-{number:03} | legacy | PROVEN | proof |").unwrap();
        }
        write(root.path(), &format!("task/ledgers/staging/{name}"), &rows);
    }
    write(root.path(), LEDGER, ROWS);
    write(
        root.path(),
        "crates/pins.rs",
        "pins\u{3a} demo/C-001, C-002\n",
    );
    root
}

fn compare(root: &Path) -> Value {
    git(root, &["add", "-A"]);
    let reference = Path::new(env!("CARGO_MANIFEST_DIR")).join("../check_ledger_grammar.py");
    let python = Command::new("python3")
        .arg(reference)
        .arg("--repo")
        .arg(root)
        .output()
        .unwrap();
    let rust = Command::new(env!("CARGO_BIN_EXE_repark-repo"))
        .arg("--repo")
        .arg(root)
        .args(["checks", "ledger-grammar"])
        .output()
        .unwrap();
    assert_eq!(
        rust.status.code(),
        python.status.code(),
        "python={}\nrust={}",
        String::from_utf8_lossy(&python.stderr),
        String::from_utf8_lossy(&rust.stderr)
    );
    let output: Value = serde_json::from_slice(&rust.stdout).unwrap();
    let report = output["checks"]["ledger-grammar"].clone();
    let expected: Vec<_> = String::from_utf8(python.stderr)
        .unwrap()
        .lines()
        .filter(|line| !line.starts_with("ledger-grammar: FAIL"))
        .map(str::to_owned)
        .collect();
    assert_eq!(report["diagnostics"], serde_json::json!(expected));
    report
}

fn attestation() -> String {
    let mut entries = String::new();
    for number in 1..=10 {
        writeln!(
            entries,
            "- id: AT-{number}\n  status: N/A\n  justification: no surface"
        )
        .unwrap();
    }
    format!("```yaml\nCOVERAGE_ATTESTATION:\n{entries}complete: true\n```\n")
}

#[test]
fn valid_rows_counts_and_citations_match_python() {
    let root = fixture();
    let result = compare(root.path());
    assert_eq!(result["ok"], true);
    assert_eq!(result["clauses"], 15);
    assert_eq!(result["pinned_clauses"], 2);
    assert_eq!(result["live_ledgers"], 3);
}

#[test]
fn malformed_rows_and_unpinned_order_match_python() {
    let root = fixture();
    write(root.path(), "crates/pins.rs", "");
    write(
        root.path(),
        LEDGER,
        &format!(
            "{ROWS}| C-004 | no verdict | proof |\n| C-002 | duplicate | REJECTED | proof |\n| C-005 | PROVEN | only text |\n| C-006 | OPEN | PROVEN | proof |\n"
        ),
    );
    let result = compare(root.path());
    assert_eq!(result["ok"], false);
    assert!(result["diagnostics"].as_array().unwrap().len() >= 5);
    write(
        root.path(),
        LEDGER,
        "| C-009 | invalid |\n| C-002 | second | PROVEN | proof |\n| C-009 | now valid | PROVEN | proof |\n| C-003 | open | OPEN | proof |\n",
    );
    let result = compare(root.path());
    assert!(result["diagnostics"].to_string().contains("C-002, C-009"));
}

#[test]
fn reading_header_window_and_code_spans_match_python() {
    let root = fixture();
    write(root.path(), "crates/pins.rs", "");
    for (header, expected) in [
        ("**Path:** READING\n".to_owned(), true),
        ("`**Path:** READING`\n".to_owned(), false),
        (format!("{}**Path:** READING\n", "\n".repeat(39)), true),
        (format!("{}**Path:** READING\n", "\n".repeat(40)), false),
    ] {
        write(root.path(), LEDGER, &format!("{header}{ROWS}"));
        assert_eq!(compare(root.path())["ok"], expected);
    }
}

#[test]
fn attestation_missing_invalid_and_duplicate_categories_match_python() {
    let root = fixture();
    let closed = ROWS.replace("OPEN", "REJECTED");
    for suffix in [
        String::new(),
        attestation(),
        attestation().replace("status: N/A", "status: ATTACKED"),
        attestation().replace("justification: no surface", "justification:"),
        attestation().replace("AT-10", "AT-11"),
        attestation().replace("AT-10", "AT-1"),
        attestation().replace("complete: true", "complete: false"),
        attestation().replace("complete: true", ""),
        attestation().replace("status: N/A", "status: maybe"),
        attestation().replace("status: N/A", "status: ATTA\u{200d}CKED"),
        attestation().replace("status: N/A", "status: ATT\u{1f}ACKED"),
        format!("{}{}", attestation(), attestation()),
    ] {
        write(root.path(), LEDGER, &format!("{closed}{suffix}"));
        compare(root.path());
    }
    write(root.path(), LEDGER, &format!("{closed}{}", attestation()));
    assert_eq!(compare(root.path())["ok"], true);
}

#[test]
fn finding_required_fields_and_disposition_prefix_match_python() {
    let root = fixture();
    let valid = "```yaml\nFINDING:\nid: F-1\nseverity: S1\ncategory: AT-2\nclause: [C-001]\ndisposition: REMEDIATED with pin\n```\n";
    for finding in [
        valid.to_owned(),
        "```\nFINDING:\n```\n".to_owned(),
        valid
            .replace("S1", "S9")
            .replace("AT-2", "AT-20")
            .replace("C-001", "none")
            .replace("REMEDIATED", "WITHDRAWN"),
    ] {
        write(root.path(), LEDGER, &format!("{ROWS}{finding}"));
        compare(root.path());
    }
}

#[test]
fn archive_rows_and_fenced_multiline_pins_match_python() {
    let root = fixture();
    write(
        root.path(),
        "task/ledgers/archive/2026-09/2026-09-01-old-ledger.md",
        "| C-008 | archive | invalid |\n",
    );
    write(
        root.path(),
        "scripts/pins.md",
        "```\npins\u{3a} old/C-008\npins\u{3a} demo/C-001,\n C-002\n```\n",
    );
    assert_eq!(compare(root.path())["ok"], true);
    write(root.path(), "scripts/pins.md", "pins\u{3a} unknown/C-999\n");
    assert_eq!(compare(root.path())["ok"], false);
}

#[test]
fn stale_exceptions_and_exact_ratchet_match_python() {
    let root = fixture();
    fs::remove_file(
        root.path()
            .join("task/ledgers/staging/v3-0-charter-ledger.md"),
    )
    .unwrap();
    assert_eq!(compare(root.path())["ok"], false);
    write(
        root.path(),
        "task/ledgers/staging/fnp-0-charter-ledger.md",
        "",
    );
    let result = compare(root.path());
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("ratchet it down")
    );
}

#[test]
fn legacy_line_breaks_and_fence_toggling_match_python() {
    let root = fixture();
    for newline in ["\r", "\r\n", "\u{b}", "\u{85}", "\u{2028}"] {
        write(root.path(), LEDGER, &ROWS.replace('\n', newline));
        assert_eq!(compare(root.path())["ok"], true);
    }
    write(
        root.path(),
        LEDGER,
        &format!("```\n| C-999 | ignored |\n~~~\n{ROWS}"),
    );
    assert_eq!(compare(root.path())["ok"], true);
}

#[test]
fn python_control_whitespace_in_reading_and_citations_matches() {
    let root = fixture();
    write(root.path(), "crates/pins.rs", "");
    write(
        root.path(),
        LEDGER,
        &format!("**Path:**\u{1f}READING\n{ROWS}"),
    );
    assert_eq!(compare(root.path())["ok"], true);
    write(root.path(), LEDGER, ROWS);
    write(
        root.path(),
        "crates/pins.rs",
        "pins\u{3a}\u{1f}demo/C-001,\u{1f}C-002\n",
    );
    assert_eq!(compare(root.path())["ok"], true);
}
