"""MAP-GOVERNANCE-1: the pin binding lives in ledger sidecars the grammar gate verifies."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_GRAMMAR = _REPO / "scripts" / "check_ledger_grammar.py"
_STAGING = "task/ledgers/staging"
_UNIT = "mg-demo-charter"
_LEDGER = f"{_STAGING}/{_UNIT}-ledger.md"
_SIDECAR = f"{_STAGING}/{_UNIT}-pins.md"
_TEST = "crates/demo/tests/pins.rs"
_HEADER = f"# {_UNIT} pin binding, read by scripts/check_ledger_grammar.py rule B\n\n"

_ROWS = """# Charter

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The thing holds. | A pin. | PROVEN | `pins.rs` |
| C-002 | The other thing holds. | A pin. | PROVEN | `pins.rs` |
| C-003 | Not yet known. | Needs the module. | OPEN | waiting |
"""


def _exceptions() -> dict[str, tuple[int, bool]]:
    spec = importlib.util.spec_from_file_location("check_ledger_grammar", _GRAMMAR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return dict(module.EXCEPTIONS)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, check=True, text=True
    ).stdout


def _write(repo: Path, path: str, text: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _grammar(repo: Path) -> subprocess.CompletedProcess[str]:
    _git(repo, "add", "-A")
    return subprocess.run(
        [sys.executable, str(_GRAMMAR), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q", "-b", "main")
    for name, (ceiling, _governed) in _exceptions().items():
        rows = "".join(f"| C-{n:03d} | legacy | x | PROVEN | x |\n" for n in range(1, ceiling + 1))
        _write(tmp_path, f"{_STAGING}/{name}", "# legacy\n\n" + rows)
    _write(tmp_path, _LEDGER, _ROWS)
    _write(tmp_path, _TEST, "fn demo_holds() {}\nfn other_holds() {}\n")
    _write(
        tmp_path,
        _SIDECAR,
        _HEADER + f"C-001: {_TEST}::demo_holds\nC-002: {_TEST}::other_holds, {_TEST}\n",
    )
    return tmp_path


def test_sidecar_binding_pins_proven_clauses(repo: Path) -> None:
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr
    assert "2 pinned clause ids" in result.stdout


def test_sidecar_path_only_binding_counts(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_unbound_proven_clause_goes_red(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"1 PROVEN clause(s) with no `pins: {_UNIT}/C-NNN` citation (ceiling 0): C-002" in (
        result.stderr
    )


def test_name_missing_from_the_file_goes_red(repo: Path) -> None:
    _write(repo, _TEST, "fn demo_holds() {}\nfn other_holds_renamed() {}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:4: C-002 names `other_holds`, which is not a token in {_TEST}" in (
        result.stderr
    )


def test_name_must_be_a_whole_token(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo\nC-002: {_TEST}::other_holds\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 names `demo`, which is not a token in {_TEST}" in result.stderr


def test_untracked_path_goes_red(repo: Path) -> None:
    gone = "crates/demo/tests/gone.rs"
    _write(repo, _SIDECAR, _HEADER + f"C-001: {gone}::demo_holds\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:3: C-001 binds {gone}, which is not a tracked file" in result.stderr


def test_missing_clause_goes_red(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\nC-009: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:5: C-009 names no clause of {_UNIT} in any ledger" in result.stderr


def test_malformed_and_duplicate_lines_go_red(repo: Path) -> None:
    _write(
        repo,
        _SIDECAR,
        _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\nC-002: {_TEST}\nloose prose\nC-001:\n",
    )
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:5: C-002 is bound twice" in result.stderr
    assert f"{_SIDECAR}:6: not a binding line" in result.stderr
    assert f"{_SIDECAR}:7: not a binding line" in result.stderr


def test_malformed_name_goes_red(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::not a name\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:3: C-001 entry `{_TEST}::not a name` is not <path> or <path>::<name>" in (
        result.stderr
    )


def test_archive_sidecar_strips_the_date_prefix(repo: Path) -> None:
    archived = "task/ledgers/archive/2026-08/2026-08-20-old-unit"
    _write(repo, f"{archived}-ledger.md", "| C-001 | Old. | x | PROVEN | x |\n")
    _write(repo, f"{archived}-pins.md", f"# old-unit pin binding\n\nC-001: {_TEST}\n")
    assert _grammar(repo).returncode == 0
    _write(repo, f"{archived}-pins.md", f"# old-unit pin binding\n\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert "C-002 names no clause of old-unit in any ledger" in result.stderr


def test_floor_keeps_its_meaning(repo: Path) -> None:
    for name, (ceiling, _governed) in _exceptions().items():
        if not ceiling:
            continue
        sidecar = f"{_STAGING}/{name.removesuffix('-ledger.md')}-pins.md"
        _write(repo, sidecar, f"# pins\n\nC-001: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1
        assert f"ceiling {ceiling} is above the measured {ceiling - 1}" in result.stderr


def test_a_citation_in_source_still_counts(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    _write(repo, _TEST, f"// pins: {_UNIT}/C-002\nfn demo_holds() {{}}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_a_citation_in_a_map_still_counts_during_the_transition(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    _write(repo, "crates/demo/tests/map.md", f"# map\n\n- `pins.rs` pins: {_UNIT}/C-002\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr
