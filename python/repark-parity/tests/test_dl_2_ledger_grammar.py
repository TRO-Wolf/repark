"""DL-2: the ledger grammar gate — every rule red on a plant, green on the clean shape."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "check_ledger_grammar.py"
_STAGING = "task/ledgers/staging"
_UNIT = "u9-demo-charter"
_LEDGER = f"{_STAGING}/{_UNIT}-ledger.md"
_TEST = "crates/demo/tests/pins.rs"

_ROWS = """# Charter — U9

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The thing holds. | A pin. | PROVEN | `pins.rs` |
| C-002 | The other thing holds. | A pin. | **PROVEN** (measured) | `pins.rs` |
| C-003 | Not yet known. | Needs the module. | OPEN | waiting on the ceiling |
"""

_ATTESTATION_ROWS = "\n".join(
    f"    - id: AT-{n}\n      status: N/A\n      justification: no surface on a docs unit"
    for n in range(2, 11)
)
_ATTESTATION = f"""
```yaml
COVERAGE_ATTESTATION:
  pr_unit: U9
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: walked the clauses
      artifacts: [crates/demo/tests/pins.rs]
{_ATTESTATION_ROWS}
  reattested: []
  complete: true
```
"""


def _exceptions() -> dict[str, tuple[int, bool]]:
    """The script's real EXCEPTIONS table, so the fixture mirrors its rows."""
    spec = importlib.util.spec_from_file_location("check_ledger_grammar", _SCRIPT)
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


def _run(repo: Path) -> subprocess.CompletedProcess[str]:
    _git(repo, "add", "-A")
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A scratch tree: one staging ledger (two PROVEN, one OPEN), both PROVEN clauses pinned."""
    _git(tmp_path, "init", "-q", "-b", "main")
    # The real table's rows must name ledgers that exist and sit exactly at their ceilings.
    for name, (ceiling, _governed) in _exceptions().items():
        rows = "".join(f"| C-{n:03d} | legacy | — | PROVEN | x |\n" for n in range(1, ceiling + 1))
        _write(tmp_path, f"{_STAGING}/{name}", "# legacy\n\n" + rows)
    _write(tmp_path, _LEDGER, _ROWS)
    _write(tmp_path, _TEST, f"// pins: {_UNIT}/C-001, C-002\nfn demo() {{}}\n")
    return tmp_path


def test_clean_shape_is_green_and_counts(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-002
    result = _run(repo)
    assert result.returncode == 0, result.stderr
    legacy = sum(ceiling for ceiling, _governed in _exceptions().values())
    assert (
        f"{len(_exceptions()) + 1} staging ledgers clean ({3 + legacy} clauses, 2 pinned clause ids"
        in result.stdout
    )


def test_verdict_cell_and_duplicate_id_go_red(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-001
    _write(
        repo,
        _LEDGER,
        _ROWS + "| C-004 | Done-ish. | — | DONE | x |\n| C-001 | Again. | — | PROVEN | x |\n",
    )
    result = _run(repo)
    assert result.returncode == 1
    assert "C-004 needs exactly one verdict cell" in result.stderr
    assert "C-001 duplicate clause id" in result.stderr


def test_row_without_evidence_goes_red(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-001
    _write(repo, _LEDGER, _ROWS + "| C-005 | Bare. | PROVEN |\n")
    result = _run(repo)
    assert result.returncode == 1
    assert "C-005 needs the clause text and at least one evidence cell" in result.stderr


def test_unpinned_proven_clause_and_dead_citation_go_red(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-003
    _write(repo, _TEST, f"// pins: {_UNIT}/C-001, C-099\n")
    result = _run(repo)
    assert result.returncode == 1
    assert f"`pins: {_UNIT}/C-099` names no clause in any ledger" in result.stderr
    assert (
        f"1 PROVEN clause(s) with no `pins: {_UNIT}/C-NNN` citation (ceiling 0): C-002"
        in result.stderr
    )


def test_archived_clause_can_be_cited(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-003
    _write(
        repo,
        "task/ledgers/archive/2026-08/2026-08-20-old-unit-ledger.md",
        "| C-001 | Old. | — | PROVEN | x |\n",
    )
    old = "old-unit"  # built, not literal: the real tree's scan must not see a citation here
    _write(repo, _TEST, f"// pins: {_UNIT}/C-001, C-002\n// pins: {old}/C-001\n")
    assert _run(repo).returncode == 0


def test_attestation_required_once_no_clause_is_open(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-004
    converged = _ROWS.replace("| OPEN | waiting on the ceiling |", "| PROVEN | `pins.rs` |")
    _write(repo, _TEST, f"// pins: {_UNIT}/C-001, C-002, C-003\n")
    _write(repo, _LEDGER, converged)
    result = _run(repo)
    assert result.returncode == 1
    assert "no COVERAGE_ATTESTATION block" in result.stderr
    _write(repo, _LEDGER, converged + _ATTESTATION)
    assert _run(repo).returncode == 0


def test_attestation_shape_defects_go_red(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-004
    broken = _ATTESTATION.replace("      artifacts: [crates/demo/tests/pins.rs]\n", "")
    broken = broken.replace(
        "    - id: AT-10\n      status: N/A\n      justification: no surface on a docs unit", ""
    )
    broken = broken.replace(
        "    - id: AT-5\n      status: N/A\n      justification: no surface on a docs unit",
        "    - id: AT-5\n      status: N/A",
    )
    _write(repo, _LEDGER, _ROWS + broken)
    result = _run(repo)
    assert result.returncode == 1
    assert "AT-1 ATTACKED without artifacts" in result.stderr
    assert "AT-5 N/A without justification" in result.stderr
    assert "attestation lacks AT-10" in result.stderr
    assert "says complete: true but the categories say false" in result.stderr


def test_ledger_without_a_clause_table_goes_red(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-004
    _write(repo, f"{_STAGING}/u10-prose-ledger.md", "# U10\n\nJust prose.\n")
    result = _run(repo)
    assert result.returncode == 1
    assert "u10-prose-ledger.md: no clause table" in result.stderr


def test_finding_record_fields_are_checked(repo: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-004
    finding = (
        "\n```yaml\nFINDING:\n  id: F-U9-1\n  severity: S9\n  category: AT-1\n"
        "  claim: x\n  disposition: FIXED\n```\n"
    )
    _write(repo, _LEDGER, _ROWS + finding)
    result = _run(repo)
    assert result.returncode == 1
    assert "FINDING F-U9-1 severity must be S0..S3" in result.stderr
    assert "FINDING F-U9-1 names no charter clause" in result.stderr
    assert "FINDING F-U9-1 disposition must start with" in result.stderr


def test_exceptions_table_ratchets_down_only(tmp_path: Path) -> None:
    # pins: dl-2-ledger-grammar-charter/C-005
    # The real table against the real tree: a ceiling above the measured count, or a row for a
    # ledger no longer in staging, is a finding — provoked by editing a copy of the script.
    source = _SCRIPT.read_text(encoding="utf-8")
    assert '"fnp-0-charter-ledger.md": (12, False)' in source
    raised = source.replace(
        '"fnp-0-charter-ledger.md": (12, False)', '"fnp-0-charter-ledger.md": (13, False)'
    )
    raised = raised.replace(
        '"v3-0-charter-ledger.md": (0, False),',
        '"v3-0-charter-ledger.md": (0, False),\n    "gone-ledger.md": (0, False),',
    )
    copy = tmp_path / "raised_ceilings.py"
    copy.write_text(raised, encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(copy), "--repo", str(_REPO)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "ceiling 13 is above the measured 12" in result.stderr
    assert "EXCEPTIONS names gone-ledger.md, which is not in task/ledgers/staging/" in result.stderr
