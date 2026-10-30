"""MAP-GOVERNANCE-1: the union binding model, explicit-id disclaimers, own-bin floors, and the
multi-line definition shapes."""

from __future__ import annotations

from pathlib import Path

import pytest
from test_map_governance_1 import (
    _HEADER,
    _LEDGER,
    _MAP,
    _ROWS,
    _SIDECAR,
    _SIDECAR_TEXT,
    _TEST,
    _UNIT,
    _grammar,
    _migrate,
    _scratch_map_repo,
    _write,
    seed_repo,
)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return seed_repo(tmp_path)


def test_an_id_less_disclaimer_keeps_the_binding_and_is_reported(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            f"- `pins.rs` — the battery, red then green. pins: {_UNIT}/C-001",
            "  The measurement of C-002 (two reads; then success) is cited here so the grammar"
            " gate counts it; this file pins none of it.",
            f"  pins: {_UNIT}/C-002",
            "  Their clause citations below stay so the grammar gate counts the merged ledger.",
            f"  pins: {_UNIT}/C-003",
            "",
        ]
    )
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn demo_holds() {}\n"})
    report = _migrate(repo, "--check", "--report", "hand-candidates")
    assert "hand candidates: 1" in report.stdout
    assert f"  {_UNIT}/C-003\t{_TEST}\t{_MAP}:7\t" in report.stdout
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        "C-002: none (records)",
        f"C-003: {_TEST}",
    ]


def test_a_row_named_test_file_is_added_beside_the_citations_file(tmp_path: Path) -> None:
    other = "crates/demo/tests/other_battery.rs"
    rows = _ROWS.replace(
        "| C-002 | The other thing holds. | A pin. | PROVEN | the battery |",
        "| C-002 | The other thing holds. | A pin. | PROVEN | `src/engine.rs`, "
        "`other_battery.rs::other_holds` and `demo_holds` |",
    )
    text = f"# map\n\n- `pins.rs` — the battery. pins: {_UNIT}/C-001, C-002\n"
    files = {
        _TEST: "#[test]\nfn demo_holds() {}\n",
        other: "#[test]\nfn other_holds() {}\n",
        "crates/demo/src/engine.rs": "pub fn run() {}\n",
    }
    repo = _scratch_map_repo(tmp_path, text, files)
    _write(repo, _LEDGER, rows)
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        f"C-002: {other}::other_holds, {_TEST}::demo_holds",
    ]


def test_a_sidecar_in_another_bin_does_not_satisfy_the_floor(repo: Path) -> None:
    completed = f"task/ledgers/completed/{_UNIT}"
    _write(repo, f"{completed}-ledger.md", _ROWS)
    _write(repo, f"{completed}-pins.md", _SIDECAR_TEXT)
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_LEDGER}: 1 PROVEN clause(s) with no binding (ceiling 0): C-002" in result.stderr


def test_more_shell_rust_and_decorator_shapes_define_nothing(repo: Path) -> None:
    script = "scripts/check_more.sh"
    body = (
        "cat <<\\EOF\nphantom_bs() {\nEOF\n"
        "cat <<'Q'\nphantom_sq() {\nQ\n"
        'cat <<"D"\nphantom_dq() {\nD\n'
        'echo "multi\nphantom_ml() {\n"\n'
        "x=$((1<<3))\nreal_after() {\n  :\n}\n"
    )
    _write(repo, script, body)
    rust = "crates/demo/tests/nested.rs"
    _write(repo, rust, "/* outer\n/* inner */\nfn phantom_nested() {}\n*/\n#[test]\nfn real() {}\n")
    for path, name in (
        (script, "phantom_bs"),
        (script, "phantom_sq"),
        (script, "phantom_dq"),
        (script, "phantom_ml"),
        (rust, "phantom_nested"),
    ):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, name
        assert f"C-001 names `{name}`, which {path} does not define" in result.stderr
    _write(repo, _SIDECAR, _HEADER + f"C-001: {script}::real_after, {rust}::real\nC-002: {_TEST}\n")
    assert _grammar(repo).returncode == 0
    used = "crates/demo/tests/test_used.py"
    _write(
        repo, used, "import pytest\n\n\n@pytest.mark.usefixtures('db')\ndef helper():\n    pass\n"
    )
    _write(repo, _SIDECAR, _HEADER + f"C-001: {used}::helper\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 binds {used}, which is not a test file or a script" in result.stderr
