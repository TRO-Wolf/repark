"""MAP-PR-GATE-1: the map.md lockstep unit is the pull request; staged mode warns."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_GUARD = _REPO / "scripts" / "check_map_md.sh"
_SYNC = _REPO / "scripts" / "sync_map_md.py"
_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, check=True, text=True, env=_ENV
    ).stdout


def _guard(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(_GUARD), *args],
        capture_output=True,
        text=True,
        check=False,
        cwd=repo,
        env=_ENV,
    )


def _sync(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    script_dir = repo / "scripts"
    script_dir.mkdir(exist_ok=True)
    shutil.copy2(_SYNC, script_dir / "sync_map_md.py")
    return subprocess.run(
        [sys.executable, str(script_dir / "sync_map_md.py"), *(args or ("--check",))],
        capture_output=True,
        text=True,
        check=False,
        cwd=repo,
        env=_ENV,
    )


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", message)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "pkg").mkdir()
    (repo / "pkg" / "mod.rs").write_text("fn a() {}\n", encoding="utf-8")
    (repo / "pkg" / "map.md").write_text("# pkg\n\n- [mod.rs](mod.rs)\n", encoding="utf-8")
    (repo / "map.md").write_text("# root\n\n- [pkg/](pkg/map.md)\n", encoding="utf-8")
    _commit(repo, "base")
    return repo


def test_branch_mode_passes_when_code_and_map_change(repo: Path) -> None:
    """pins: map-pr-gate-1/C-001."""
    base = _git(repo, "rev-parse", "HEAD").strip()
    (repo / "pkg" / "mod.rs").write_text("fn b() {}\n", encoding="utf-8")
    (repo / "pkg" / "map.md").write_text("# pkg\n\n- [mod.rs](mod.rs) — edited\n", encoding="utf-8")
    _commit(repo, "work")
    result = _guard(repo, "--base", base)
    assert result.returncode == 0, result.stderr


def test_branch_mode_fails_when_only_code_changes(repo: Path) -> None:
    """pins: map-pr-gate-1/C-002."""
    base = _git(repo, "rev-parse", "HEAD").strip()
    (repo / "pkg" / "mod.rs").write_text("fn b() {}\n", encoding="utf-8")
    _commit(repo, "work")
    result = _guard(repo, "--base", base)
    assert result.returncode == 1
    assert "pkg/map.md" in result.stderr
    assert "ERROR" in result.stderr


def test_branch_mode_fails_for_a_new_directory_without_map(repo: Path) -> None:
    """pins: map-pr-gate-1/C-003."""
    base = _git(repo, "rev-parse", "HEAD").strip()
    (repo / "newdir").mkdir()
    (repo / "newdir" / "x.rs").write_text("fn x() {}\n", encoding="utf-8")
    _commit(repo, "work")
    result = _guard(repo, "--base", base)
    assert result.returncode == 1
    assert "newdir has changed code but no map.md" in result.stderr


def test_branch_mode_ignores_deletion_only_changes(repo: Path) -> None:
    """pins: map-pr-gate-1/C-004."""
    base = _git(repo, "rev-parse", "HEAD").strip()
    _git(repo, "rm", "pkg/mod.rs")
    _git(repo, "commit", "-m", "delete only")
    result = _guard(repo, "--base", base)
    assert result.returncode == 0, result.stderr


def test_branch_mode_handles_a_root_level_file(repo: Path) -> None:
    """pins: map-pr-gate-1/C-005 — a root file's map is `map.md`, not `./map.md`."""
    base = _git(repo, "rev-parse", "HEAD").strip()
    (repo / "top.rs").write_text("fn t() {}\n", encoding="utf-8")
    _commit(repo, "work")
    failed = _guard(repo, "--base", base)
    assert failed.returncode == 1
    assert "map.md was not updated on this branch" in failed.stderr
    (repo / "map.md").write_text("# root\n\n- [top.rs](top.rs)\n", encoding="utf-8")
    _commit(repo, "map")
    passed = _guard(repo, "--base", base)
    assert passed.returncode == 0, passed.stderr


def test_staged_mode_warns_and_exits_zero(repo: Path) -> None:
    """pins: map-pr-gate-1/C-006 — the hook warns; ci.yml's map.md guard holds the rule."""
    (repo / "pkg" / "mod.rs").write_text("fn b() {}\n", encoding="utf-8")
    _git(repo, "add", "pkg/mod.rs")
    result = _guard(repo)
    assert result.returncode == 0
    assert "WARNING:" in result.stderr
    assert "map.md guard" in result.stderr


def test_duplicate_row_rule_fires_on_two_rows(tmp_path: Path) -> None:
    """pins: map-pr-gate-1/C-007 — two rows, same first link, one finding."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "a.py").write_text("a = 1\n", encoding="utf-8")
    (repo / "map.md").write_text(
        "# m\n\n- [a.py](a.py) — one\n- [a.py](a.py) — two\n", encoding="utf-8"
    )
    _commit(repo, "base")
    result = _sync(repo)
    assert result.returncode == 1
    assert "duplicate row" in result.stderr


def test_duplicate_row_rule_survives_fix(tmp_path: Path) -> None:
    """pins: map-pr-gate-1/C-007 — `--fix` never resolves it; the row choice is a hand call."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "a.py").write_text("a = 1\n", encoding="utf-8")
    (repo / "map.md").write_text(
        "# m\n\n- [a.py](a.py) — one\n- [a.py](a.py) — two\n", encoding="utf-8"
    )
    _commit(repo, "base")
    result = _sync(repo, "--fix")
    assert result.returncode == 1
    assert "duplicate row" in result.stderr
    assert (repo / "map.md").read_text(encoding="utf-8").count("[a.py](a.py)") == 2


def test_duplicate_row_rule_quiet_when_one_row_mentions_twice(tmp_path: Path) -> None:
    """pins: map-pr-gate-1/C-008 — the comparison is between rows, on first links only."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "a.py").write_text("a = 1\n", encoding="utf-8")
    (repo / "map.md").write_text(
        "# m\n\n- [a.py](a.py) — one row, [a.py](a.py) named twice\n", encoding="utf-8"
    )
    _commit(repo, "base")
    result = _sync(repo)
    assert result.returncode == 0, result.stderr
