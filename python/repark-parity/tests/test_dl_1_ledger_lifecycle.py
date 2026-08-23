"""DL-1: the ledger lifecycle script — archive/move rewrite links; check goes red on plants."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "ledger_lifecycle.py"
_DATE = "2026-08-20T12:00:00+00:00"
_ENV = {
    **os.environ,
    "GIT_AUTHOR_DATE": _DATE,
    "GIT_COMMITTER_DATE": _DATE,
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}

_LEDGER = "task/ledgers/completed/x1-demo-ledger.md"
_ARCHIVED = "task/ledgers/archive/2026-08/2026-08-20-x1-demo-ledger.md"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, check=True, text=True, env=_ENV
    ).stdout


def _write(repo: Path, path: str, text: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


def _run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        env=_ENV,
        check=False,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A tiny repository on `main`: one completed ledger, linked from three places."""
    _git(tmp_path, "init", "-q", "-b", "main")
    _write(tmp_path, "AGENTS.md", "# AGENTS\n")
    _write(
        tmp_path, "STATUS.md", "# STATUS\n\nSee [x1](task/ledgers/completed/x1-demo-ledger.md).\n"
    )
    _write(tmp_path, "docs/map.md", "# map\n\n## Contents\n- [note.md](note.md) — a note\n")
    _write(
        tmp_path,
        "docs/note.md",
        "# note\n\n[x1](../task/ledgers/completed/x1-demo-ledger.md#s1) and `[x](../task/x.md)`.\n",
    )
    _write(tmp_path, "task/map.md", "# map\n\n## Contents\n- [ledgers/](ledgers/map.md) — bins\n")
    _write(
        tmp_path,
        "task/ledgers/map.md",
        "# map\n\n## Contents\n- [completed/](completed/map.md) — done\n",
    )
    _write(
        tmp_path,
        "task/ledgers/staging/map.md",
        "# map\n\n## Contents\n\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(
        tmp_path,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [x1-demo-ledger.md](x1-demo-ledger.md) — **X1:** the demo\n"
        "  unit, wrapped.\n\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(
        tmp_path,
        _LEDGER,
        "# Ledger — X1\n\nPolicy: [AGENTS](../../../AGENTS.md). Same name elsewhere:\n"
        "[other](../../../docs/note.md).\n",
    )
    _commit(tmp_path, "seed")
    return tmp_path


def test_archive_moves_rewrites_and_relocates_the_map_row(repo: Path) -> None:
    result = _run(repo, "archive")
    assert result.returncode == 0, result.stderr
    assert (repo / _ARCHIVED).is_file()
    assert not (repo / _LEDGER).exists()
    # Links into the ledger are rewritten, fragments kept, code spans untouched.
    assert (
        "(task/ledgers/archive/2026-08/2026-08-20-x1-demo-ledger.md)"
        in (repo / "STATUS.md").read_text()
    )
    note = (repo / "docs/note.md").read_text()
    assert "(../task/ledgers/archive/2026-08/2026-08-20-x1-demo-ledger.md#s1)" in note
    assert "`[x](../task/x.md)`" in note
    # The ledger's own outgoing links are re-expressed from its new directory.
    archived = (repo / _ARCHIVED).read_text()
    assert "(../../../../AGENTS.md)" in archived
    assert "(../../../../docs/note.md)" in archived
    # The row left completed/map.md for the month map, condensed to one line (DL-3).
    assert "x1-demo-ledger" not in (repo / "task/ledgers/completed/map.md").read_text()
    month_map = (repo / "task/ledgers/archive/2026-08/map.md").read_text()
    assert (
        "- [2026-08-20-x1-demo-ledger.md](2026-08-20-x1-demo-ledger.md) — "
        "**X1:** the demo unit, wrapped.\n" in month_map
    )
    assert (
        "- [2026-08/](2026-08/map.md) — 1 ledger"
        in (repo / "task/ledgers/archive/map.md").read_text()
    )
    # Everything is staged as one operation: no untracked, no unstaged lines.
    status = _git(repo, "status", "--porcelain", "--untracked-files=all").splitlines()
    assert status and all(line[1] == " " for line in status), status
    assert _run(repo, "check").returncode == 0


def test_archive_is_idempotent_and_leaves_or_refuses_a_ledger_not_on_main(repo: Path) -> None:
    assert _run(repo, "archive").returncode == 0
    _commit(repo, "archived")
    again = _run(repo, "archive")
    assert again.returncode == 0 and "nothing to archive" in again.stdout
    _git(repo, "checkout", "-q", "-b", "unit")
    _write(repo, "task/ledgers/completed/y2-new-ledger.md", "# Y2\n")
    _commit(repo, "finish y2 on the branch")
    # Unnamed (the pickup's `make ledger-archive`): the unit's own ledger is left, exit 0.
    left = _run(repo, "archive")
    assert left.returncode == 0, left.stderr
    assert "y2-new-ledger.md is not on main yet — left for the next pickup" in left.stdout
    assert (repo / "task/ledgers/completed/y2-new-ledger.md").is_file()
    # Named explicitly: refused, nothing changed.
    refused = _run(repo, "archive", "task/ledgers/completed/y2-new-ledger.md")
    assert refused.returncode == 1
    assert "not on main yet" in refused.stderr
    assert (repo / "task/ledgers/completed/y2-new-ledger.md").is_file()


def test_move_to_completed_and_archive_is_not_a_move_target(repo: Path) -> None:
    _write(repo, "task/ledgers/staging/z3-live-ledger.md", "# Z3\n")
    _write(repo, "briefs.md", "[z3](task/ledgers/staging/z3-live-ledger.md)\n")
    _commit(repo, "z3")
    result = _run(repo, "move", "task/ledgers/staging/z3-live-ledger.md", "completed")
    assert result.returncode == 0, result.stderr
    assert (repo / "task/ledgers/completed/z3-live-ledger.md").is_file()
    assert "(task/ledgers/completed/z3-live-ledger.md)" in (repo / "briefs.md").read_text()
    completed_map = (repo / "task/ledgers/completed/map.md").read_text()
    assert "- [z3-live-ledger.md](z3-live-ledger.md) — Z3" in completed_map
    assert _run(repo, "move", "task/ledgers/completed/z3-live-ledger.md", "archive").returncode == 2


def test_check_red_on_a_ledger_outside_the_bins(repo: Path) -> None:
    _write(repo, "task/stray-ledger.md", "# stray\n")
    _commit(repo, "plant")
    result = _run(repo, "check")
    assert result.returncode == 1
    assert "task/stray-ledger.md: ledger outside the bins" in result.stderr


def test_check_red_on_an_archive_prefix_that_disagrees_with_its_month(repo: Path) -> None:
    _write(repo, "task/ledgers/archive/2026-07/2026-08-02-w-ledger.md", "# w\n")
    _write(repo, "task/ledgers/archive/loose-ledger.md", "# loose\n")
    _commit(repo, "plant")
    result = _run(repo, "check")
    assert result.returncode == 1
    assert "prefix 2026-08 disagrees with directory 2026-07" in result.stderr
    assert "task/ledgers/archive/loose-ledger.md: archive files are" in result.stderr


def test_check_red_on_a_dead_ledger_link_anywhere(repo: Path) -> None:
    _write(repo, "docs/note.md", "[gone](../task/ledgers/completed/gone-ledger.md)\n")
    _commit(repo, "plant")
    result = _run(repo, "check")
    assert result.returncode == 1
    assert "docs/note.md: dead ledger link `task/ledgers/completed/gone-ledger.md`" in result.stderr


def test_check_frozen_rule_allows_link_repair_and_errata_only(repo: Path) -> None:
    base = _git(repo, "rev-parse", "HEAD").strip()
    ledger = repo / _LEDGER
    ledger.write_text(ledger.read_text().replace("(../../../AGENTS.md)", "(../../../../AGENTS.md)"))
    assert _run(repo, "check", "--base", base).returncode == 0
    ledger.write_text("**Errata (2026-08-21):** the count was 3.\n\n" + ledger.read_text())
    assert _run(repo, "check", "--base", base).returncode == 0
    ledger.write_text(ledger.read_text().replace("Same name", "A different claim"))
    result = _run(repo, "check", "--base", base)
    assert result.returncode == 1
    assert f"{_LEDGER}: frozen ledger edited beyond a link repair" in result.stderr
    ledger.unlink()
    deleted = _run(repo, "check", "--base", base)
    assert deleted.returncode == 1
    assert f"{_LEDGER}: deleted" in deleted.stderr


def test_a_moved_ledger_keeps_its_sibling_links_true(repo: Path) -> None:
    ledger = repo / _LEDGER
    ledger.write_text(ledger.read_text() + "\n[sibling](map.md)\n")
    _commit(repo, "sibling link")
    result = _run(repo, "archive")
    assert result.returncode == 0, result.stderr
    assert "(../../completed/map.md)" in (repo / _ARCHIVED).read_text()


def test_a_plus_continuation_is_joined_not_split(repo: Path) -> None:
    # Review finding (2026-08-23): `task/map.md` wraps three descriptions onto lines that
    # begin with `+ `, which a list-item regex reads as a nested bullet. Under DL-3 the
    # archive destination condenses to the first sentence — the `+ ` line must be IN it,
    # joined, never orphaned in the source map. Live bins carry rows whole
    # (test_a_move_to_a_live_bin_still_carries_the_whole_row).
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [x1-demo-ledger.md](x1-demo-ledger.md) — shipped a/b\n"
        "  + c/d and the rest.\n  - [sub](../../../docs/note.md) — a child row\n\n"
        "- [keep.md](map.md) — stays\n\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _commit(repo, "wrapped row")
    assert _run(repo, "archive").returncode == 0
    month_map = (repo / "task/ledgers/archive/2026-08/map.md").read_text()
    assert (
        "- [2026-08-20-x1-demo-ledger.md](2026-08-20-x1-demo-ledger.md) — "
        "shipped a/b + c/d and the rest.\n" in month_map
    )
    completed_map = (repo / "task/ledgers/completed/map.md").read_text()
    assert "c/d" not in completed_map and "child row" not in completed_map
    assert "\n\n\n" not in completed_map
    assert "## Contents\n- [keep.md](map.md) — stays\n\n## Pointers" in completed_map


def test_check_refuses_to_pass_closed_without_a_base(repo: Path) -> None:
    result = _run(repo, "check", "--base", "no/such/ref")
    assert result.returncode == 2
    assert "refuse to pass closed" in result.stderr


def test_frozen_rule_sees_prose_smuggled_into_a_link_target(repo: Path) -> None:
    base = _git(repo, "rev-parse", "HEAD").strip()
    ledger = repo / _LEDGER
    ledger.write_text(
        ledger.read_text().replace("(../../../AGENTS.md)", "(../../../AGENTS.md THE RUN FAILED)")
    )
    result = _run(repo, "check", "--base", base)
    assert result.returncode == 1
    assert "frozen ledger edited beyond" in result.stderr


def test_an_archive_month_map_row_is_one_line_first_sentence(repo: Path) -> None:
    # pins: dl-3-archive-map-compaction-charter/C-001, C-003
    # (C-003's migration ran this same function over the real 2026-08 map; the
    # tree-level evidence is the migration commit's recorded gate runs.)
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [x1-demo-ledger.md](x1-demo-ledger.md) — **X1:** shipped the\n"
        "  demo end to end. Second sentence with detail.\n  + wrapped tail on a bullet-like line.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _commit(repo, "wrapped multi-sentence row")
    assert _run(repo, "archive").returncode == 0
    month_map = (repo / "task/ledgers/archive/2026-08/map.md").read_text()
    row = (
        "- [2026-08-20-x1-demo-ledger.md](2026-08-20-x1-demo-ledger.md) — "
        "**X1:** shipped the demo end to end.\n"
    )
    assert row in month_map
    assert "Second sentence" not in month_map and "wrapped tail" not in month_map
    # pins: dl-3-archive-map-compaction-charter/C-004
    assert "do not read this file whole" in month_map


def test_a_move_to_a_live_bin_still_carries_the_whole_row(repo: Path) -> None:
    # pins: dl-3-archive-map-compaction-charter/C-002
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Contents\n- [z3-live-ledger.md](z3-live-ledger.md) — first. Second\n"
        "  sentence, wrapped.\n\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/staging/z3-live-ledger.md", "# Z3\n")
    _commit(repo, "z3")
    assert _run(repo, "move", "task/ledgers/staging/z3-live-ledger.md", "completed").returncode == 0
    completed_map = (repo / "task/ledgers/completed/map.md").read_text()
    assert "— first. Second\n  sentence, wrapped.\n" in completed_map
