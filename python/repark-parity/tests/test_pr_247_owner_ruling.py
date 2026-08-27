from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts/check_owner_ruling.py"


def _load_checker() -> ModuleType:
    specification = importlib.util.spec_from_file_location("check_owner_ruling", _SCRIPT)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _write_contracts(tree: Path, checker: ModuleType) -> None:
    agents = (
        f"{checker.expected_prefix(checker.DOCUMENT_TITLES['AGENTS.md'])}"
        f"{checker.EXPECTED_BOUNDARY}\n\n"
    )
    claude = checker.expected_prefix(checker.DOCUMENT_TITLES["CLAUDE.md"])
    (tree / "AGENTS.md").write_text(agents, encoding="utf-8")
    (tree / "CLAUDE.md").write_text(claude, encoding="utf-8")


def test_live_contract_keeps_the_ruling_and_review_boundary_byte_exact() -> None:
    """pins: pr-247-revalidation/C-001, C-003, C-004."""
    checker = _load_checker()
    assert checker.findings(_REPO) == []


def test_one_byte_owner_ruling_mutation_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "CLAUDE.md"
    text = path.read_text(encoding="utf-8").replace("FABLE", "Fable", 1)
    path.write_text(text, encoding="utf-8")
    assert checker.findings(tmp_path) == [
        "CLAUDE.md: owner ruling is not byte-exact at the document start"
    ]


def test_missing_contract_file_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    (tmp_path / "CLAUDE.md").unlink()
    found = checker.findings(tmp_path)
    assert len(found) == 1
    assert found[0].startswith("CLAUDE.md: cannot read:")


def test_malformed_contract_file_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    (tmp_path / "CLAUDE.md").write_bytes(b"\xff")
    found = checker.findings(tmp_path)
    assert len(found) == 1
    assert found[0].startswith("CLAUDE.md: cannot read:")


def test_contract_symlink_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "CLAUDE.md"
    target = tmp_path / "alternate-adapter.md"
    path.rename(target)
    path.symlink_to(target)
    assert checker.findings(tmp_path) == ["CLAUDE.md: must be a regular file, not a symlink"]


def test_relocated_owner_ruling_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "CLAUDE.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(f"adapter preamble\n{text}", encoding="utf-8")
    assert checker.findings(tmp_path) == [
        "CLAUDE.md: owner ruling is not byte-exact at the document start"
    ]


def test_duplicated_owner_ruling_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "CLAUDE.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(f"{text}{checker.EXPECTED_RULING}\n", encoding="utf-8")
    assert checker.findings(tmp_path) == ["CLAUDE.md: owner ruling must appear exactly once"]


def test_enforcement_boundary_mutation_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "AGENTS.md"
    text = path.read_text(encoding="utf-8").replace("review holds this rule", "automation", 1)
    path.write_text(text, encoding="utf-8")
    assert checker.findings(tmp_path) == ["AGENTS.md: enforcement boundary is missing or changed"]


def test_relocated_enforcement_boundary_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "AGENTS.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace(f"{checker.EXPECTED_BOUNDARY}\n\n", "contract body\n\n")
        + f"{checker.EXPECTED_BOUNDARY}\n",
        encoding="utf-8",
    )
    assert checker.findings(tmp_path) == ["AGENTS.md: enforcement boundary is missing or changed"]


def test_duplicated_enforcement_boundary_fails_closed(tmp_path: Path) -> None:
    checker = _load_checker()
    _write_contracts(tmp_path, checker)
    path = tmp_path / "AGENTS.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(f"{text}{checker.EXPECTED_BOUNDARY}\n", encoding="utf-8")
    assert checker.findings(tmp_path) == [
        "AGENTS.md: enforcement boundary must appear exactly once"
    ]


def test_immediate_gate_is_narrow_and_cap_1_remains_wired() -> None:
    """pins: pr-247-revalidation/C-002, C-005, C-007."""
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ci_line = next(line for line in makefile.splitlines() if line.startswith("ci: "))
    workflow = (_REPO / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "check-owner-ruling" in ci_line
    assert "run: python3 scripts/check_owner_ruling.py" in workflow
    assert "check-comment-density" not in makefile
    assert not (_REPO / "scripts/check_comment_density.py").exists()
    assert not (_REPO / "scripts/comment_ceilings.json").exists()
    assert "check-rust-file-size" in ci_line
    assert "check-lib-py" in ci_line
    rust_gate = (_REPO / "scripts/check_rust_file_size.py").read_text(encoding="utf-8")
    python_gate = (_REPO / "scripts/check_lib_py.py").read_text(encoding="utf-8")
    declaration = r"DEFAULT_CEILING(?:: int)? = 1_?000"
    assert re.search(declaration, rust_gate) is not None
    assert re.search(declaration, python_gate) is not None


def test_pr247_navigation_names_every_enforcement_artifact() -> None:
    """pins: pr-247-revalidation/C-006."""
    required = {
        "map.md": "check-owner-ruling",
        ".github/workflows/map.md": "check_owner_ruling.py",
        "scripts/map.md": "check_owner_ruling.py",
        "python/repark-parity/tests/map.md": "test_pr_247_owner_ruling.py",
        "task/ledgers/completed/map.md": "pr-247-revalidation-ledger.md",
    }
    for relative_path, artifact in required.items():
        text = (_REPO / relative_path).read_text(encoding="utf-8")
        assert artifact in text, relative_path
