"""MAP-GOVERNANCE-1: the ledger lifecycle carries a ledger's pin sidecar and never freezes it."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_map_governance_1 import _big_bytes, _size, _size_repo, _table

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


def test_archive_moves_an_excepted_map_row_by_exactly_its_link_growth(repo: Path) -> None:
    big = "docs/big/map.md"
    link = "[x1](../../task/ledgers/completed/x1-demo-ledger.md)"
    body = "".join(f"- row {n}\n" for n in range(250)) + f"- {link} — the demo.\n"
    _write(repo, big, body)
    _write(repo, "docs/still/map.md", "".join(f"- still {n}\n" for n in range(240)))
    _write(repo, "task/ledgers/archive/map.md", "# map — archive\n")
    source = (_REPO / "scripts" / "check_map_size.py").read_text(encoding="utf-8")
    start = source.index("\nEXCEPTIONS: dict[str, tuple[int, int]] = {") + 1
    end = source.index("\n}\n", start) + 3
    before = (251, len(body.encode()))
    still = (240, len("".join(f"- still {n}\n" for n in range(240)).encode()))
    table = (
        "EXCEPTIONS: dict[str, tuple[int, int]] = {\n"
        f'    "{big}": {before},\n    "docs/still/map.md": {still},\n}}\n'
    )
    _write(repo, "scripts/check_map_size.py", source[:start] + table + source[end:])
    _commit(repo, "an excepted map links the ledger")
    result = _run(repo, "archive")
    assert result.returncode == 0, result.stderr
    assert "moved 1 map-size baseline row(s)" in result.stdout
    growth = len("archive/2026-08/2026-08-20-x1-demo-ledger.md") - len(
        "completed/x1-demo-ledger.md"
    )
    after = (repo / big).read_text(encoding="utf-8")
    assert len(after.encode()) == before[1] + growth
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    assert f'    "{big}": (251, {before[1] + growth}),\n' in script
    assert f'    "docs/still/map.md": {still},\n' in script
    size = subprocess.run(
        [sys.executable, str(repo / "scripts/check_map_size.py"), "--repo", str(repo)],
        capture_output=True,
        text=True,
        env=_ENV,
        check=False,
    )
    assert size.returncode == 0, size.stderr


def test_one_unit_in_two_bins_is_refused(repo: Path) -> None:
    _write(repo, "task/ledgers/staging/x1-demo-ledger.md", "# Ledger — X1 again\n")
    _commit(repo, "a copy in staging")
    result = _run(repo, "check")
    assert result.returncode == 1
    assert "x1-demo: one unit, ledgers in two bins" in result.stderr


def _rows(count: int) -> str:
    return "".join(f"- row {n}\n" for n in range(count))


def _main_grew(tmp_path: Path) -> Path:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "seed")
    _write(repo, "big/map.md", _rows(260))
    _git(repo, "commit", "-q", "-am", "main grows the map")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    _git(repo, "checkout", "-q", "-b", "feature")
    return repo


def test_growth_from_main_reds_the_check_until_an_update_reseeds_it(tmp_path: Path) -> None:
    repo = _main_grew(tmp_path)
    result = _size(repo)
    assert result.returncode == 1
    assert "ERROR: big/map.md grew to 260 lines" in result.stderr
    assert _size(repo, "--update-baselines").returncode == 0
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    assert f'    "big/map.md": (260, {len(_rows(260).encode())}),' in script
    assert _size(repo).returncode == 0, _size(repo).stderr


def test_without_a_merge_base_the_update_refuses_growth(tmp_path: Path) -> None:
    repo = _main_grew(tmp_path)
    _git(repo, "update-ref", "-d", "refs/remotes/origin/main")
    result = _size(repo, "--update-baselines")
    assert result.returncode == 1
    assert "REFUSED: big/map.md: grew to 260 lines" in result.stderr


def test_a_map_grown_on_the_branch_past_main_goes_red(tmp_path: Path) -> None:
    repo = _main_grew(tmp_path)
    _write(repo, "big/map.md", _rows(261))
    result = _size(repo)
    assert result.returncode == 1
    assert "ERROR: big/map.md grew to 261 lines" in result.stderr
    result = _size(repo, "--update-baselines")
    assert result.returncode == 1
    assert f"past (260, {len(_rows(260).encode())})" in result.stderr


def test_update_baselines_raises_a_row_to_the_merge_base_size(tmp_path: Path) -> None:
    repo = _main_grew(tmp_path)
    size = len(_rows(260).encode())
    assert _size(repo, "--update-baselines").returncode == 0
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    assert f'    "big/map.md": (260, {size}),' in script
    _write(repo, "big/map.md", _rows(255))
    assert _size(repo, "--update-baselines").returncode == 0
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    assert f'    "big/map.md": (255, {len(_rows(255).encode())}),' in script
    assert _size(repo).returncode == 0


def test_a_branch_lowered_map_growing_back_toward_the_base_goes_red(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "seed")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    _git(repo, "checkout", "-q", "-b", "feature")
    _write(repo, "big/map.md", _rows(240))
    assert _size(repo, "--update-baselines").returncode == 0
    _git(repo, "commit", "-q", "-am", "the branch lowers the map")
    _write(repo, "big/map.md", _rows(248))
    result = _size(repo)
    assert result.returncode == 1
    assert "ERROR: big/map.md grew to 248 lines" in result.stderr
    result = _size(repo, "--update-baselines")
    assert result.returncode == 1
    assert "REFUSED: big/map.md: grew to 248 lines" in result.stderr
