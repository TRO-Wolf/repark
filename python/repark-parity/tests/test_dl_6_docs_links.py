from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "check_docs_links.py"
_LEDGER = "task/ledgers/staging/u1-ledger.md"
_ALLOWLIST = "scripts/docs_links_allowlist.txt"
_HEADER = "# allowlist: one path:line:link entry per line; entries only shrink\n"
_GOOD_PAGE = "# Scratch page\n\n## Usage\n\nbody text.\n"
_README = (
    "Scratch\n"
    "\n"
    "[guide](GUIDE.md) and [usage](GUIDE.md#usage) and [site](https://example.com) and\n"
    "[fragment](#usage) and `[span](SKIPPED.md)` links.\n"
    "\n"
    "```text\n"
    "[fenced](FENCED.md)\n"
    "```\n"
    "\n"
    "[deep](docs/guide.md) and [deep-anchor](docs/guide.md#usage)\n"
)
_LEDGER_TEXT = (
    "Charter u1\n"
    "\n"
    "| Clause | Proposition | Verdict | Evidence |\n"
    "|---|---|---|---|\n"
    "| C-001 | The section exists. | PROVEN | docs: docs/guide.md#usage |\n"
)


def _write(repo: Path, path: str, text: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _run(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(tmp_path)], capture_output=True, check=True
    )
    _write(tmp_path, "README.md", _README)
    _write(tmp_path, "GUIDE.md", _GOOD_PAGE)
    _write(tmp_path, "docs/guide.md", _GOOD_PAGE)
    _write(tmp_path, _LEDGER, _LEDGER_TEXT)
    _write(tmp_path, _ALLOWLIST, _HEADER)
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], capture_output=True, check=True)
    return tmp_path


def test_clean_fixture_counts_files_and_links(repo: Path) -> None:
    result = _run(repo)
    assert result.returncode == 0, result.stderr
    assert "4 files, 5 links checked" in result.stdout


def test_missing_target_reds_with_path_line_and_reason(repo: Path) -> None:
    _write(repo, "README.md", _README + "[gone](MISSING.md)\n")
    result = _run(repo)
    assert result.returncode == 1
    assert "README.md:11: MISSING.md -> does not exist" in result.stderr


def test_untracked_target_reds(repo: Path) -> None:
    _write(repo, "EXTRA.md", "extra page\n")
    _write(repo, "README.md", _README + "[extra](EXTRA.md)\n")
    result = _run(repo)
    assert result.returncode == 1
    assert "README.md:11: EXTRA.md -> exists but is not tracked" in result.stderr


def test_bad_anchor_reds(repo: Path) -> None:
    _write(repo, "README.md", _README + "[bad](GUIDE.md#nope)\n")
    result = _run(repo)
    assert result.returncode == 1
    assert "README.md:11: GUIDE.md#nope -> anchor #nope does not match a heading" in result.stderr


def test_bad_docs_cell_reds(repo: Path) -> None:
    _write(repo, _LEDGER, _LEDGER_TEXT.replace("docs/guide.md#usage", "docs/guide.md#nope"))
    result = _run(repo)
    assert result.returncode == 1
    assert (
        "task/ledgers/staging/u1-ledger.md:5: docs/guide.md#nope -> "
        "anchor #nope does not match a heading" in result.stderr
    )


def test_allowlist_drops_its_entries_only(repo: Path) -> None:
    _write(repo, "README.md", _README + "[gone](MISSING.md)\n[other](GONE_TOO.md)\n")
    _write(repo, _ALLOWLIST, _HEADER + "README.md:11:MISSING.md\n")
    result = _run(repo)
    assert result.returncode == 1
    assert "MISSING.md" not in result.stderr
    assert "README.md:12: GONE_TOO.md -> does not exist" in result.stderr
    _write(repo, _ALLOWLIST, _HEADER + "README.md:11:MISSING.md\nREADME.md:12:GONE_TOO.md\n")
    assert _run(repo).returncode == 0


def test_malformed_allowlist_entry_fails_closed(repo: Path) -> None:
    _write(repo, _ALLOWLIST, _HEADER + "not-an-entry\n")
    result = _run(repo)
    assert result.returncode == 2
    assert "malformed" in result.stderr


def test_stale_allowlist_entry_fails(repo: Path) -> None:
    _write(repo, _ALLOWLIST, _HEADER + "README.md:3:GUIDE.md\n")
    result = _run(repo)
    assert result.returncode == 1
    assert (
        "scripts/docs_links_allowlist.txt: stale entry README.md:3:GUIDE.md — "
        "the link is no longer broken; remove this row" in result.stderr
    )
    assert "docs-links: FAIL — 0 broken link(s), 1 stale allowlist entry" in result.stderr


def test_real_tree_is_green_under_the_seeded_allowlist() -> None:
    result = subprocess.run(
        [sys.executable, str(_SCRIPT)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
