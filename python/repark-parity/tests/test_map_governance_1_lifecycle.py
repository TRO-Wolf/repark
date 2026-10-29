"""MAP-GOVERNANCE-1: the ledger lifecycle carries a ledger's pin sidecar and never freezes it."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "ledger_lifecycle.py"
_DATE = "2026-08-20T12:00:00+00:00"
_LATER = "2026-09-03T12:00:00+00:00"
_IDENTITY = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}
_ENV = {**os.environ, **_IDENTITY, "GIT_AUTHOR_DATE": _DATE, "GIT_COMMITTER_DATE": _DATE}
_LATER_ENV = {**os.environ, **_IDENTITY, "GIT_AUTHOR_DATE": _LATER, "GIT_COMMITTER_DATE": _LATER}

_COMPLETED = "task/ledgers/completed"
_LEDGER = f"{_COMPLETED}/x1-demo-ledger.md"
_SIDECAR = f"{_COMPLETED}/x1-demo-pins.md"
_MONTH = "task/ledgers/archive/2026-08"
_ARCHIVED_LEDGER = f"{_MONTH}/2026-08-20-x1-demo-ledger.md"
_ARCHIVED_SIDECAR = f"{_MONTH}/2026-08-20-x1-demo-pins.md"
_SIDECAR_TEXT = "# x1-demo pin binding\n\nC-001: crates/demo/tests/pins.rs::demo_holds\n"


def _git(repo: Path, *args: str, env: dict[str, str] | None = None) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=True,
        text=True,
        env=env or _ENV,
    ).stdout


def _write(repo: Path, path: str, text: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _commit(repo: Path, message: str, env: dict[str, str] | None = None) -> None:
    _git(repo, "add", "-A", env=env)
    _git(repo, "commit", "-q", "-m", message, env=env)


def _run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        env=_ENV,
        check=False,
    )


def _bin_map(title: str, rows: str) -> str:
    return f"# map — {title}\n\n## Contents\n{rows}\n## Pointers\n- Up: [../map.md](../map.md)\n"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q", "-b", "main")
    _write(tmp_path, "AGENTS.md", "# AGENTS\n")
    _write(tmp_path, "task/map.md", "# map\n\n## Contents\n- [ledgers/](ledgers/map.md) — bins\n")
    _write(
        tmp_path,
        "task/ledgers/map.md",
        "# map\n\n## Contents\n- [completed/](completed/map.md) — done\n",
    )
    _write(tmp_path, "task/ledgers/staging/map.md", _bin_map("staging", ""))
    _write(
        tmp_path,
        f"{_COMPLETED}/map.md",
        _bin_map("completed", "- [x1-demo-ledger.md](x1-demo-ledger.md) — **X1:** the demo.\n"),
    )
    _write(tmp_path, _LEDGER, "# Ledger — X1\n\n| C-001 | It holds. | x | PROVEN | x |\n")
    _write(tmp_path, "crates/demo/tests/pins.rs", "fn demo_holds() {}\n")
    _commit(tmp_path, "seed")
    _write(tmp_path, _SIDECAR, _SIDECAR_TEXT)
    _commit(tmp_path, "sidecar, a later day", env=_LATER_ENV)
    return tmp_path


def test_a_sidecar_in_a_frozen_bin_may_be_edited_and_a_ledger_may_not(repo: Path) -> None:
    base = _git(repo, "rev-parse", "HEAD").strip()
    _write(repo, _SIDECAR, _SIDECAR_TEXT.replace("::demo_holds", ""))
    result = _run(repo, "check", "--base", base)
    assert result.returncode == 0, result.stderr
    _write(repo, _LEDGER, "# Ledger — X1\n\n| C-001 | It holds less. | x | PROVEN | x |\n")
    result = _run(repo, "check", "--base", base)
    assert result.returncode == 1
    assert f"{_LEDGER}: frozen ledger edited beyond a link repair" in result.stderr
    assert "pins.md" not in result.stderr


def test_an_archived_sidecar_may_be_edited(repo: Path) -> None:
    assert _run(repo, "archive").returncode == 0
    _commit(repo, "archived")
    base = _git(repo, "rev-parse", "HEAD").strip()
    assert (repo / _ARCHIVED_SIDECAR).read_text(encoding="utf-8") == _SIDECAR_TEXT
    _write(repo, _ARCHIVED_SIDECAR, _SIDECAR_TEXT.replace("::demo_holds", ""))
    result = _run(repo, "check", "--base", base)
    assert result.returncode == 0, result.stderr


def test_move_carries_the_sidecar_without_a_map_row(repo: Path) -> None:
    staged = "task/ledgers/staging/z3-live"
    _write(repo, f"{staged}-ledger.md", "# Z3\n")
    _write(repo, f"{staged}-pins.md", "# z3-live pin binding\n")
    _commit(repo, "z3")
    result = _run(repo, "move", f"{staged}-ledger.md", "completed")
    assert result.returncode == 0, result.stderr
    assert (repo / f"{_COMPLETED}/z3-live-ledger.md").is_file()
    assert (repo / f"{_COMPLETED}/z3-live-pins.md").is_file()
    assert not (repo / f"{staged}-pins.md").exists()
    completed_map = (repo / f"{_COMPLETED}/map.md").read_text(encoding="utf-8")
    assert "- [z3-live-ledger.md](z3-live-ledger.md) — Z3" in completed_map
    assert "z3-live-pins" not in completed_map


def test_archive_carries_the_sidecar_under_the_ledger_date(repo: Path) -> None:
    result = _run(repo, "archive")
    assert result.returncode == 0, result.stderr
    assert (repo / _ARCHIVED_LEDGER).is_file()
    assert (repo / _ARCHIVED_SIDECAR).read_text(encoding="utf-8") == _SIDECAR_TEXT
    assert not (repo / _SIDECAR).exists()
    assert not (repo / "task/ledgers/archive/2026-09").exists()
    month_map = (repo / f"{_MONTH}/map.md").read_text(encoding="utf-8")
    assert "2026-08-20-x1-demo-ledger.md" in month_map
    assert "pins" not in month_map
    archive_map = (repo / "task/ledgers/archive/map.md").read_text(encoding="utf-8")
    assert "- [2026-08/](2026-08/map.md) — 1 ledger\n" in archive_map
    _commit(repo, "archived")
    assert _run(repo, "check").returncode == 0


def test_archive_of_a_named_ledger_carries_its_sidecar(repo: Path) -> None:
    result = _run(repo, "archive", _LEDGER)
    assert result.returncode == 0, result.stderr
    assert (repo / _ARCHIVED_SIDECAR).is_file()
    assert not (repo / _SIDECAR).exists()


def _size_check(repo: Path) -> subprocess.CompletedProcess[str]:
    source = (_REPO / "scripts" / "check_map_size.py").read_text(encoding="utf-8")
    start = source.index("\nEXCEPTIONS: dict[str, tuple[int, int]] = {") + 1
    end = source.index("\n}\n", start) + 3
    empty = "EXCEPTIONS: dict[str, tuple[int, int]] = {\n}\n"
    _write(repo, "scratch/check_map_size.py", source[:start] + empty + source[end:])
    _git(repo, "add", "-A")
    return subprocess.run(
        [sys.executable, str(repo / "scratch/check_map_size.py"), "--repo", str(repo)],
        capture_output=True,
        text=True,
        env=_ENV,
        check=False,
    )


def test_a_move_then_the_size_ratchet_stays_green(repo: Path) -> None:
    rows = "".join(f"- [r{n}-ledger.md](r{n}-ledger.md) — **R{n}:** row.\n" for n in range(250))
    for n in range(250):
        _write(repo, f"{_COMPLETED}/r{n}-ledger.md", f"# R{n}\n")
    _write(repo, f"{_COMPLETED}/map.md", _bin_map("completed", rows))
    _write(repo, "task/ledgers/archive/map.md", "# map — archive\n")
    _write(repo, "task/ledgers/staging/z3-live-ledger.md", "# Z3\n")
    _commit(repo, "a full completed bin")
    assert _size_check(repo).returncode == 0
    moved = _run(repo, "move", "task/ledgers/staging/z3-live-ledger.md", "completed")
    assert moved.returncode == 0, moved.stderr
    completed_map = (repo / f"{_COMPLETED}/map.md").read_text(encoding="utf-8")
    assert len(completed_map.splitlines()) > 250
    result = _size_check(repo)
    assert result.returncode == 0, result.stderr
