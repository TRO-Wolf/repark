"""The ledger lifecycle script — archive/move rewrite links; check goes red on plants."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

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
PINS_LABEL = "pins:"


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
    # `task/map.md` wraps descriptions onto lines that begin with `+ `, which a
    # list-item regex reads as a nested bullet. Under DL-3 the archive destination
    # condenses to the first sentence — the `+ ` line must be IN it, joined, never
    # orphaned in the source map. Live bins carry rows whole
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


def _tail_repo(repo: Path, row: str, ledger: str, ledger_text: str) -> None:
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        "- [zzz-old-ledger.md](../completed/zzz-old-ledger.md) — **ZZZ-OLD:** filed.\n" + row,
    )
    _write(repo, ledger, ledger_text)
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [aaa-old-ledger.md](aaa-old-ledger.md) — **AAA-OLD:** first.\n"
        "- [zzz-old-ledger.md](zzz-old-ledger.md) — **ZZZ-OLD:** last.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/completed/zzz-old-ledger.md", "# Ledger — ZZZ-OLD\n")
    _commit(repo, "ledger with a row past the Pointers section")


def test_move_cuts_a_tail_row_with_pins_and_pastes_it_in_order(repo: Path) -> None:
    _tail_repo(
        repo,
        "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the demo unit, wrapped\n"
        "  over two lines with detail.\n"
        "  " + PINS_LABEL + " m1-demo/C-001, C-002\n",
        "task/ledgers/staging/m1-demo-ledger.md",
        "# Ledger — M1\n",
    )
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert "m1-demo-ledger" not in staging
    assert (
        "- [zzz-old-ledger.md](../completed/zzz-old-ledger.md) — **ZZZ-OLD:** filed.\n" in staging
    )
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the demo unit, wrapped\n  over two lines with detail.\n" in completed
    assert "  " + PINS_LABEL + " m1-demo/C-001, C-002\n" in completed
    assert "Ledger — M1" not in completed
    assert completed.index("aaa-old-ledger") < completed.index("m1-demo-ledger")
    assert completed.index("m1-demo-ledger") < completed.index("zzz-old-ledger")


def test_move_cuts_a_tail_one_line_row(repo: Path) -> None:
    _tail_repo(
        repo,
        "- [m2-demo-ledger.md](m2-demo-ledger.md) — **M2:** one line, like rp-64.\n",
        "task/ledgers/staging/m2-demo-ledger.md",
        "# Ledger — M2-DEMO\n",
    )
    assert _run(repo, "move", "task/ledgers/staging/m2-demo-ledger.md", "completed").returncode == 0
    assert "m2-demo-ledger" not in (repo / "task/ledgers/staging/map.md").read_text()
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m2-demo-ledger.md") == 2
    assert "— **M2:** one line, like rp-64.\n" in completed
    assert "M2-DEMO" not in completed
    assert completed.index("aaa-old-ledger") < completed.index("m2-demo-ledger")
    assert completed.index("m2-demo-ledger") < completed.index("zzz-old-ledger")


def test_move_carries_the_sidecar_without_a_row(repo: Path) -> None:
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Contents\n"
        "- [m3-demo-ledger.md](m3-demo-ledger.md) — **M3:** the demo unit.\n"
        "  " + PINS_LABEL + " m3-demo/C-001\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/staging/m3-demo-ledger.md", "# Ledger — M3\n")
    _write(repo, "task/ledgers/staging/m3-demo-pins.md", "C-001: none (records)\n")
    _commit(repo, "m3 with a sidecar")
    assert _run(repo, "move", "task/ledgers/staging/m3-demo-ledger.md", "completed").returncode == 0
    assert (repo / "task/ledgers/completed/m3-demo-pins.md").is_file()
    assert not (repo / "task/ledgers/staging/m3-demo-pins.md").exists()
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert "m3-demo-pins" not in completed
    assert completed.count("m3-demo-ledger.md") == 2


def test_move_leaves_a_history_file_frozen_and_names_it(repo: Path) -> None:
    history = "docs/history/facade/file.md"
    _write(repo, "task/ledgers/staging/m5-demo-ledger.md", "# Ledger — M5\n")
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Contents\n- [m5-demo-ledger.md](m5-demo-ledger.md) — **M5:** the demo.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "notes.md", "[m5](task/ledgers/staging/m5-demo-ledger.md)\n")
    _write(
        repo,
        history,
        "# frozen\n\nSee [m5](../../../task/ledgers/staging/m5-demo-ledger.md).\n",
    )
    _commit(repo, "m5 with a history link")
    before = (repo / history).read_text()
    result = _run(repo, "move", "task/ledgers/staging/m5-demo-ledger.md", "completed")
    assert result.returncode == 0, result.stderr
    assert (repo / history).read_text() == before
    assert "(task/ledgers/completed/m5-demo-ledger.md)" in (repo / "notes.md").read_text()
    assert f"{history} left frozen (1 link(s) to moved files)" in result.stdout


def test_archive_leaves_a_history_file_frozen(repo: Path) -> None:
    history = "docs/history/facade/file.md"
    _write(repo, "task/ledgers/completed/m6-demo-ledger.md", "# Ledger — M6\n")
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [m6-demo-ledger.md](m6-demo-ledger.md) — **M6:** the demo.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(
        repo,
        history,
        "# frozen\n\nSee [m6](../../../task/ledgers/completed/m6-demo-ledger.md).\n",
    )
    _commit(repo, "m6 with a history link")
    before = (repo / history).read_text()
    result = _run(repo, "archive", "task/ledgers/completed/m6-demo-ledger.md")
    assert result.returncode == 0, result.stderr
    assert (repo / history).read_text() == before
    assert f"{history} left frozen (1 link(s) to moved files)" in result.stdout


def test_check_ignores_a_dead_ledger_link_in_history_only(repo: Path) -> None:
    _write(
        repo,
        "docs/history/facade/file.md",
        "# frozen\n\nSee [gone](../../../task/ledgers/staging/gone-ledger.md).\n",
    )
    _commit(repo, "stale history link")
    assert _run(repo, "check").returncode == 0
    _write(repo, "notes.md", "[gone](task/ledgers/staging/gone-ledger.md)\n")
    _commit(repo, "same dead link in a live file")
    result = _run(repo, "check")
    assert result.returncode == 1
    assert "notes.md: dead ledger link `task/ledgers/staging/gone-ledger.md`" in result.stderr


def test_move_without_a_source_row_synthesises_exactly_one_row(repo: Path) -> None:
    _write(repo, "task/ledgers/staging/m4-demo-ledger.md", "# Ledger — M4-DEMO\n")
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [aaa-old-ledger.md](aaa-old-ledger.md) — **AAA-OLD:** first.\n"
        "- [zzz-old-ledger.md](zzz-old-ledger.md) — **ZZZ-OLD:** last.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _commit(repo, "m4 with no map row")
    assert _run(repo, "move", "task/ledgers/staging/m4-demo-ledger.md", "completed").returncode == 0
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert "[m4-demo-ledger.md](m4-demo-ledger.md) — Ledger — M4-DEMO\n" in completed
    assert completed.count("m4-demo-ledger.md") == 2


def _commented_repo(repo: Path, tail: str) -> str:
    comment = "<!--\n- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** inside a comment.\n-->\n"
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n" + comment + tail,
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [aaa-old-ledger.md](aaa-old-ledger.md) — **AAA-OLD:** first.\n"
        "- [zzz-old-ledger.md](zzz-old-ledger.md) — **ZZZ-OLD:** last.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/completed/zzz-old-ledger.md", "# Ledger — ZZZ-OLD\n")
    _commit(repo, "m1 with a commented bullet")
    return comment


def test_move_skips_a_commented_bullet_and_cuts_the_real_row(repo: Path) -> None:
    comment = _commented_repo(
        repo,
        "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n"
        "  " + PINS_LABEL + " m1-demo/C-001\n",
    )
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert comment in staging
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the real row.\n" in completed
    assert "  " + PINS_LABEL + " m1-demo/C-001\n" in completed
    assert "inside a comment" not in completed


def test_move_with_only_a_commented_bullet_synthesises(repo: Path) -> None:
    comment = _commented_repo(repo, "")
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    assert comment in (repo / "task/ledgers/staging/map.md").read_text()
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert "[m1-demo-ledger.md](m1-demo-ledger.md) — Ledger — M1\n" in completed
    assert completed.count("m1-demo-ledger.md") == 2


def test_move_skips_a_prose_bullet_and_cuts_the_real_row(repo: Path) -> None:
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        "\n## Notes\n- See [m1-demo-ledger.md](m1-demo-ledger.md) for the background.\n"
        "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _commit(repo, "m1 with a prose bullet")
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert (
        "- See [m1-demo-ledger.md](../completed/m1-demo-ledger.md) for the background.\n" in staging
    )
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the real row.\n" in completed


def test_move_skips_a_fenced_bullet_and_cuts_the_real_row(repo: Path) -> None:
    fence = (
        "```md\n- [m1-demo-ledger.md](../completed/m1-demo-ledger.md) — **M1:** filed form.\n```\n"
    )
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        + fence
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _commit(repo, "m1 with a fenced bullet")
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert fence in staging
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the real row.\n" in completed
    assert "filed form" not in completed


def test_move_and_archive_refuse_a_history_source(repo: Path) -> None:
    history = "docs/history/facade/file.md"
    _write(repo, history, "# frozen\n")
    _commit(repo, "history file")
    moved = _run(repo, "move", history, "completed")
    assert moved.returncode == 2
    assert "ERROR:" in moved.stderr
    assert "history is frozen" in moved.stderr
    archived = _run(repo, "archive", history)
    assert archived.returncode == 2
    assert "ERROR:" in archived.stderr
    assert "history is frozen" in archived.stderr
    assert _git(repo, "status", "--short").strip() == ""


def _contents_repo(repo: Path, head: str) -> None:
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        + head
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n"
        "  " + PINS_LABEL + " m1-demo/C-001\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [aaa-old-ledger.md](aaa-old-ledger.md) — **AAA-OLD:** first.\n"
        "- [zzz-old-ledger.md](zzz-old-ledger.md) — **ZZZ-OLD:** last.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/completed/zzz-old-ledger.md", "# Ledger — ZZZ-OLD\n")
    _commit(repo, "m1 with a decoy bullet")


def _moved_contents_row(repo: Path) -> None:
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the real row.\n" in completed
    assert "  " + PINS_LABEL + " m1-demo/C-001\n" in completed


def test_contents_prose_bullet_stays_and_real_row_moves(repo: Path) -> None:
    _contents_repo(repo, "- See [m1-demo-ledger.md](m1-demo-ledger.md) for the background.\n")
    _moved_contents_row(repo)
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert (
        "- See [m1-demo-ledger.md](../completed/m1-demo-ledger.md) for the background.\n" in staging
    )


def test_contents_commented_bullet_stays_and_real_row_moves(repo: Path) -> None:
    comment = "<!--\n- [m1-demo-ledger.md](../completed/m1-demo-ledger.md) — **M1:** filed.\n-->\n"
    _contents_repo(repo, comment)
    _moved_contents_row(repo)
    assert comment in (repo / "task/ledgers/staging/map.md").read_text()
    assert "filed" not in (repo / "task/ledgers/completed/map.md").read_text()


def test_contents_fenced_bullet_stays_and_real_row_moves(repo: Path) -> None:
    fence = "```md\n- [m1-demo-ledger.md](../completed/m1-demo-ledger.md) — **M1:** filed.\n```\n"
    _contents_repo(repo, fence)
    _moved_contents_row(repo)
    assert fence in (repo / "task/ledgers/staging/map.md").read_text()
    assert "filed" not in (repo / "task/ledgers/completed/map.md").read_text()


def test_comment_holding_a_fence_line_leaves_the_row_visible(repo: Path) -> None:
    comment = "<!--\n```\n-->\n"
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        + comment
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n"
        "  " + PINS_LABEL + " m1-demo/C-001\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _commit(repo, "m1 under a comment holding a fence")
    result = _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed")
    assert result.returncode == 0, result.stderr
    assert "rewrote 1 link(s)" in result.stdout
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert comment in staging
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert "— **M1:** the real row.\n" in completed


def test_fence_holding_a_comment_opener_leaves_the_row_visible(repo: Path) -> None:
    fence = "```md\n<!--\n```\n"
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        + fence
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n"
        "  " + PINS_LABEL + " m1-demo/C-001\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _commit(repo, "m1 under a fence holding a comment opener")
    result = _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed")
    assert result.returncode == 0, result.stderr
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert fence in staging
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert "— **M1:** the real row.\n" in completed


def _load_lifecycle() -> ModuleType:
    specification = importlib.util.spec_from_file_location("ledger_lifecycle", _SCRIPT)
    if specification is None or specification.loader is None:
        raise RuntimeError("cannot load the lifecycle script")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def test_commented_links_rewrite_without_shifting_positions(repo: Path) -> None:
    _write(repo, "task/ledgers/staging/x9-demo-ledger.md", "# Ledger — X9\n")
    _write(repo, "task/ledgers/staging/y9-demo-ledger.md", "# Ledger — Y9\n")
    _write(
        repo,
        "notes.md",
        "<!-- note --> - [x9-demo-ledger.md](task/ledgers/staging/x9-demo-ledger.md)\n"
        "<!-- a --> <!-- b --> - [y9-demo-ledger.md](task/ledgers/staging/y9-demo-ledger.md)\n",
    )
    _commit(repo, "x9 y9 with commented links")
    assert _run(repo, "move", "task/ledgers/staging/x9-demo-ledger.md", "completed").returncode == 0
    assert _run(repo, "move", "task/ledgers/staging/y9-demo-ledger.md", "completed").returncode == 0
    lines = (repo / "notes.md").read_text().splitlines()
    assert (
        lines[0] == "<!-- note --> - [x9-demo-ledger.md](task/ledgers/completed/x9-demo-ledger.md)"
    )
    assert (
        lines[1]
        == "<!-- a --> <!-- b --> - [y9-demo-ledger.md](task/ledgers/completed/y9-demo-ledger.md)"
    )


def test_links_around_a_comment_both_rewrite_exactly(repo: Path) -> None:
    _write(repo, "task/ledgers/staging/x9-demo-ledger.md", "# Ledger — X9\n")
    _write(repo, "task/ledgers/staging/y9-demo-ledger.md", "# Ledger — Y9\n")
    _write(
        repo,
        "notes.md",
        "- [x9-demo-ledger.md](task/ledgers/staging/x9-demo-ledger.md) <!-- note -->"
        " - [y9-demo-ledger.md](task/ledgers/staging/y9-demo-ledger.md)\n",
    )
    _commit(repo, "x9 y9 around a comment")
    assert _run(repo, "move", "task/ledgers/staging/x9-demo-ledger.md", "completed").returncode == 0
    assert _run(repo, "move", "task/ledgers/staging/y9-demo-ledger.md", "completed").returncode == 0
    assert (repo / "notes.md").read_text() == (
        "- [x9-demo-ledger.md](task/ledgers/completed/x9-demo-ledger.md) <!-- note -->"
        " - [y9-demo-ledger.md](task/ledgers/completed/y9-demo-ledger.md)\n"
    )


def _fenced_decoy_repo(repo: Path, fence: str) -> None:
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        + fence
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _commit(repo, "m1 under a long fence")


def _moved_fenced_row(repo: Path, fence: str) -> None:
    result = _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed")
    assert result.returncode == 0, result.stderr
    assert "rewrote 1 link(s)" in result.stdout
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert fence in staging
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the real row.\n" in completed
    assert "filed" not in completed


def test_long_fence_hides_inner_marker_and_decoy(repo: Path) -> None:
    fence = (
        "````\n```\n- [m1-demo-ledger.md](../completed/m1-demo-ledger.md) — **M1:** filed.\n````\n"
    )
    _fenced_decoy_repo(repo, fence)
    _moved_fenced_row(repo, fence)


def test_tilde_fence_hides_inner_marker_and_decoy(repo: Path) -> None:
    fence = (
        "~~~~\n```\n- [m1-demo-ledger.md](../completed/m1-demo-ledger.md) — **M1:** filed.\n~~~~\n"
    )
    _fenced_decoy_repo(repo, fence)
    _moved_fenced_row(repo, fence)


def test_code_span_opener_leaves_the_row_visible(repo: Path) -> None:
    line = "`<!--` opens nothing, see [m1-demo-ledger.md](m1-demo-ledger.md).\n"
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        + line
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _commit(repo, "m1 under a code span opener")
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert (
        "`<!--` opens nothing, see [m1-demo-ledger.md](../completed/m1-demo-ledger.md).\n"
        in staging
    )
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert "— **M1:** the real row.\n" in completed


def _indented_repo(repo: Path, tail: str) -> None:
    _write(
        repo,
        "task/ledgers/staging/map.md",
        "# map\n\n## Purpose\nStaging.\n\n## Contents\n"
        "- [aaa-ledger.md](aaa-ledger.md) — **AAA:** stays.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        + tail
        + "- [m1-demo-ledger.md](m1-demo-ledger.md) — **M1:** the real row.\n",
    )
    _write(repo, "task/ledgers/staging/m1-demo-ledger.md", "# Ledger — M1\n")
    _write(
        repo,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [aaa-old-ledger.md](aaa-old-ledger.md) — **AAA-OLD:** first.\n"
        "- [zzz-old-ledger.md](zzz-old-ledger.md) — **ZZZ-OLD:** last.\n"
        "\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(repo, "task/ledgers/completed/zzz-old-ledger.md", "# Ledger — ZZZ-OLD\n")
    _commit(repo, "m1 under an indented marker")


def _moved_indented_row(repo: Path, head: str) -> None:
    assert _run(repo, "move", "task/ledgers/staging/m1-demo-ledger.md", "completed").returncode == 0
    staging = (repo / "task/ledgers/staging/map.md").read_text()
    assert head in staging
    assert "**M1:** the real row." not in staging
    completed = (repo / "task/ledgers/completed/map.md").read_text()
    assert completed.count("m1-demo-ledger.md") == 2
    assert "— **M1:** the real row.\n" in completed


def test_indented_marker_is_text_and_row_moves(repo: Path) -> None:
    _indented_repo(repo, "    ```\n")
    _moved_indented_row(repo, "    ```\n")


def test_indented_marker_inside_a_fence_is_text(repo: Path) -> None:
    fence = (
        "```md\n    ```\n- [m1-demo-ledger.md](../completed/m1-demo-ledger.md)"
        " — **M1:** filed.\n```\n"
    )
    _indented_repo(repo, fence)
    _moved_indented_row(repo, fence)
    assert "filed" not in (repo / "task/ledgers/completed/map.md").read_text()


def test_triple_span_hides_the_opener_and_row_moves(repo: Path) -> None:
    line = "```<!--``` tail.\n"
    _indented_repo(repo, line)
    _moved_indented_row(repo, line)


def test_double_span_hides_the_opener_and_row_moves(repo: Path) -> None:
    line = "``` `` `<!--` `` tail.\n"
    _indented_repo(repo, line)
    _moved_indented_row(repo, line)


def test_info_backtick_line_is_text_and_row_moves(repo: Path) -> None:
    line = "```a`b\n"
    _indented_repo(repo, line)
    _moved_indented_row(repo, line)


def test_masked_lines_keep_every_position() -> None:
    module = _load_lifecycle()
    text = (
        "plain line with [a](b) link\n"
        "<!-- same-line comment --> after [c](d)\n"
        "<!-- open\n"
        "still comment [e](f)\n"
        "--> closed [g](h)\n"
        "```md\n"
        "fenced [i](j)\n"
        "```\n"
        "````\n"
        "``` inner\n"
        "- [k](../completed/k) decoy\n"
        "````\n"
        "~~~~\n"
        "``` tilde inner\n"
        "~~~~\n"
        "`<!--` code span opener [l](m)\n"
        "trailing `code` span [n](o)\n"
        "    ``` four-space marker [p](q)\n"
        "```a`b info backtick [r](s)\n"
        "```<!--``` span fence [t](u)\n"
        "``` `` `<!--` `` span parts [v](w)\n"
        "~~~~\n"
        "tilde content [x](y)\n"
        "~~~~\n"
    )
    for line, masked in module._masked_lines(text):
        assert len(masked) == len(line)
