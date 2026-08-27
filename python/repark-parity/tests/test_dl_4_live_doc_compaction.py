"""DL-4: the live documents carry only live state — compact, the grammar, and the gate."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPTS = _REPO / "scripts"
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
_RECORD = "docs/history/beta/status-record.md"

_STATUS = """# STATUS

## Active workstreams

The queue is [the slate](briefs/next-sequence.md).

<!-- ws id=alpha ledgers=alpha- state=open -->
- **Alpha (A)** (open). Its record: [x1](task/ledgers/completed/x1-demo-ledger.md).
<!-- /ws -->

<!-- ws id=beta ledgers=beta- state=closed closed=2026-08-19 by=#7 history=docs/history/beta -->
- **Beta wave (B)** (closed by B-2). Design: [design](docs/design/beta.md).
  - **Delivered:** B-1, B-2. Its `design` sits under [docs/](docs/map.md).
<!-- /ws -->

**Closed campaigns** — records in [docs/history/](docs/history/map.md):
<!-- closed-campaigns -->
- **Gamma** — closed 2026-08-01 by #1; record:
  [other](docs/history/other/README.md)

## Known correctness issues

None.
"""

_SLATE = """# Slate

## The order

| # | Unit | Size |
|---|---|---|
| 1 | **X1** — the demo | S <!-- unit id=x1 --> |
| 2 | **X2** — the next | M <!-- unit id=x2 --> |

<!-- unit id=x1 -->
**Why X1 first.** Because.
<!-- /unit -->

<!-- unit id=x2 -->
**Why X2 second.** Also because.
<!-- /unit -->

## Standing rules

1. Rule.
"""


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
        [sys.executable, str(_SCRIPTS / "ledger_lifecycle.py"), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        env=_ENV,
        check=False,
    )


def _gate(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPTS / "check_docs_compaction.py"), "--repo", str(repo)],
        capture_output=True,
        text=True,
        env=_ENV,
        check=False,
    )


def _load(name: str) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A tiny repository on `main`: one completed ledger, a closed campaign, a two-row slate."""
    _git(tmp_path, "init", "-q", "-b", "main")
    _write(tmp_path, "AGENTS.md", "# AGENTS\n")
    _write(tmp_path, ".agents/skills/engineering-method/SKILL.md", "# method\n")
    _write(tmp_path, ".agents/skills/sepmo/unit-runbook.md", "# runbook\n")  # PROC-1 CEILINGS key
    _write(tmp_path, "STATUS.md", _STATUS)
    _write(tmp_path, "briefs/next-sequence.md", _SLATE)
    _write(
        tmp_path,
        "briefs/map.md",
        "# map\n\n## Contents\n- [next-sequence.md](next-sequence.md) — the slate\n",
    )
    _write(
        tmp_path,
        "docs/map.md",
        "# map\n\n## Contents\n- [design/](design/map.md) — designs\n"
        "- [history/](history/map.md) — the archive\n",
    )
    _write(tmp_path, "docs/design/map.md", "# map\n\n## Contents\n- [beta.md](beta.md) — beta\n")
    _write(tmp_path, "docs/design/beta.md", "# beta\n")
    _write(
        tmp_path,
        "docs/history/map.md",
        "# map\n\n## Contents\n- [other/](other/map.md) — another campaign\n\n"
        "## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(
        tmp_path,
        "docs/history/other/map.md",
        "# map\n\n## Contents\n- [README.md](README.md) — other\n",
    )
    _write(tmp_path, "docs/history/other/README.md", "# other\n")
    _write(tmp_path, "task/map.md", "# map\n\n## Contents\n- [ledgers/](ledgers/map.md) — bins\n")
    _write(
        tmp_path,
        "task/ledgers/map.md",
        "# map\n\n## Contents\n- [completed/](completed/map.md) — done\n"
        "- [staging/](staging/map.md) — live\n",
    )
    _write(
        tmp_path,
        "task/ledgers/staging/map.md",
        "# map\n\n## Contents\n\n## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(
        tmp_path,
        "task/ledgers/completed/map.md",
        "# map\n\n## Contents\n- [x1-demo-ledger.md](x1-demo-ledger.md) — **X1:** the demo\n\n"
        "## Pointers\n- Up: [../map.md](../map.md)\n",
    )
    _write(tmp_path, _LEDGER, "# Ledger — X1\n\nPolicy: [AGENTS](../../../AGENTS.md).\n")
    _commit(tmp_path, "seed")
    return tmp_path


def test_archive_makes_a_merged_unit_leave_the_slate_whole(repo: Path) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-002, C-007 — row and prose go; idempotent."""
    first = _run(repo, "archive")
    assert first.returncode == 0, first.stderr
    slate = (repo / "briefs/next-sequence.md").read_text()
    assert "x1" not in slate and "X1" not in slate and "Why X1" not in slate
    assert "| 1 | **X2** — the next | M <!-- unit id=x2 --> |" in slate
    assert "<!-- unit id=x2 -->\n**Why X2 second.**" in slate
    assert "\n\n\n" not in slate
    _commit(repo, "pickup")
    second = _run(repo, "archive")
    assert second.returncode == 0 and "nothing to archive" in second.stdout
    assert _git(repo, "status", "--porcelain") == ""


def test_a_closed_campaign_leaves_status_for_its_history_bin(repo: Path) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-003 — cut, filed, links rewritten, one line kept."""
    result = _run(repo, "compact")
    assert result.returncode == 0, result.stderr
    status = (repo / "STATUS.md").read_text()
    assert "id=beta" not in status and "Beta wave" in status
    beta_row = f"- **Beta wave (B)** — closed 2026-08-19 by #7; record: [{_RECORD}]({_RECORD})"
    assert beta_row in status
    # after the wrapped Gamma row, never inside it: a continuation line belongs to its row
    assert status.index("  [other](docs/history/other/README.md)") < status.index(beta_row)
    assert "<!-- ws id=alpha" in status  # the open block is untouched
    record = (repo / _RECORD).read_text()
    assert "## Cut from STATUS.md — closed 2026-08-19 by #7" in record
    assert "[design](../../design/beta.md)" in record and "[docs/](../../map.md)" in record
    assert "<!-- ws" not in record
    assert "status-record.md" in (repo / "docs/history/beta/map.md").read_text()
    history_map = (repo / "docs/history/map.md").read_text()
    assert "[beta/](beta/map.md) — **Beta wave (B)**" in history_map
    sync = subprocess.run(
        [sys.executable, str(_SCRIPTS / "sync_map_md.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
        cwd=repo,
    )
    assert sync.returncode == 0, sync.stdout + sync.stderr


def test_compact_touches_only_the_two_live_files_and_the_campaign_bin(repo: Path) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-004 — ledgers, other history: never written."""
    before = (repo / _LEDGER).read_text()
    result = _run(repo, "compact")
    assert result.returncode == 0, result.stderr
    staged = set(_git(repo, "diff", "--cached", "--name-only").splitlines())
    assert staged == {
        "STATUS.md",
        "briefs/next-sequence.md",
        "docs/history/beta/map.md",
        "docs/history/beta/status-record.md",
        "docs/history/map.md",
    }
    assert (repo / _LEDGER).read_text() == before
    assert (repo / "docs/history/other/README.md").read_text() == "# other\n"


@pytest.mark.parametrize(
    ("text", "phrase"),
    [
        ("<!-- ws id=a state=open -->\n- **A**\n", "never closes"),
        ("- **A**\n<!-- /ws -->\n", "closes nothing"),
        ("<!-- ws id=a state=open -->\n<!-- ws id=b state=open -->\n<!-- /ws -->\n", "no nesting"),
        ("<!-- ws state=open -->\n<!-- /ws -->\n", "has no id"),
        ("<!-- ws id=a state=closed -->\n<!-- /ws -->\n", "needs `closed=`"),
        ("<!-- ws id=a state=shut -->\n<!-- /ws -->\n", "state must be one of"),
        (
            "<!-- ws id=a state=closed closed=2026-08-01 by=#1 history=../out -->\n<!-- /ws -->\n",
            "must be a campaign bin",
        ),
        (
            "<!-- ws id=a state=closed closed=2026-08-01 by=#1 history=docs/history -->\n"
            "<!-- /ws -->\n",
            "must be a campaign bin",
        ),
    ],
)
def test_the_parser_refuses_a_malformed_document(text: str, phrase: str) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-001 — each refusal names the file and line."""
    blocks = _load("doc_blocks")
    parsed = blocks.parse(text, "STATUS.md")
    assert any(
        phrase in finding and finding.startswith("STATUS.md:") for finding in parsed.findings
    ), parsed.findings


def test_a_marker_inside_code_is_prose_not_a_marker() -> None:
    """pins: dl-4-live-doc-compaction-charter/C-001 — code spans and fences are masked."""
    blocks = _load("doc_blocks")
    text = (
        "Rows carry a `<!-- unit id=x -->` marker.\n\n```\n<!-- ws id=y state=open -->\n```\n"
        "<!-- unit id=z -->\nreal\n<!-- /unit -->\n"
    )
    parsed = blocks.parse(text, "briefs/next-sequence.md")
    assert parsed.findings == [] and [b.id for b in parsed.blocks] == ["z"]


def test_a_bullet_outside_any_block_is_found_by_line() -> None:
    """pins: dl-4-live-doc-compaction-charter/C-001 — coverage is a finding, not a convention."""
    blocks = _load("doc_blocks")
    text = (
        "## Active workstreams\n\n<!-- ws id=a state=open -->\n- **A**\n<!-- /ws -->\n"
        "- **B** unmarked\n\n## Next\n- not a workstream\n"
    )
    parsed = blocks.parse(text, "STATUS.md")
    assert parsed.findings == []
    assert blocks.uncovered_bullets(text, parsed) == [6]


def test_the_gate_is_red_on_each_class_and_green_on_the_compacted_tree(repo: Path) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-005 — (a) closed (b) merged (c) cover (d) size."""
    red = _gate(repo)
    assert red.returncode == 1
    assert "closed campaign `beta` is still here" in red.stderr  # (a)
    assert "unit `x1` merged and is still on the slate" in red.stderr  # (b)
    assert _run(repo, "archive").returncode == 0
    _commit(repo, "pickup")
    green = _gate(repo)
    assert green.returncode == 0, green.stderr
    status = repo / "STATUS.md"
    status.write_text(status.read_text().replace("## Known", "- **Delta** unmarked\n\n## Known"))
    _commit(repo, "plant an unmarked workstream")
    coverage = _gate(repo)
    assert coverage.returncode == 1 and "outside any `ws` block" in coverage.stderr  # (c)
    gate = _load("check_docs_compaction")
    found = gate.findings(repo, {"STATUS.md": 10, "briefs/next-sequence.md": 10})
    assert any("exceeds its ceiling" in line for line in found)  # (d)


def test_compact_refuses_cleanly_without_the_closed_campaigns_marker(repo: Path) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-003 — a refusal names the cause; no traceback."""
    status = repo / "STATUS.md"
    status.write_text(status.read_text().replace("<!-- closed-campaigns -->\n", ""))
    _commit(repo, "drop the marker")
    result = _run(repo, "compact")
    assert result.returncode == 1
    assert "has no `<!-- closed-campaigns -->` line" in result.stderr
    assert "Traceback" not in result.stderr
    assert _git(repo, "status", "--porcelain") == ""


def test_two_closed_campaigns_sharing_a_bin_get_one_map_row(repo: Path) -> None:
    """pins: dl-4-live-doc-compaction-charter/C-003 — a second cut appends a record, not a row."""
    status = repo / "STATUS.md"
    status.write_text(
        status.read_text().replace(
            "<!-- ws id=alpha ledgers=alpha- state=open -->",
            "<!-- ws id=alpha ledgers=alpha- state=closed closed=2026-08-20 by=#8 "
            "history=docs/history/beta -->",
        )
    )
    _commit(repo, "close alpha into beta's bin")
    assert _run(repo, "compact").returncode == 0
    bin_map = (repo / "docs/history/beta/map.md").read_text()
    assert bin_map.count("](status-record.md)") == 1
    assert (repo / _RECORD).read_text().count("## Cut from STATUS.md") == 2


def test_the_tree_is_migrated() -> None:
    """pins: dl-4-live-doc-compaction-charter/C-006 — the real STATUS and slate are compacted."""
    blocks = _load("doc_blocks")
    status = (_REPO / "STATUS.md").read_text()
    parsed = blocks.parse(status, "STATUS.md")
    assert parsed.findings == []
    assert not [b for b in parsed.blocks if b.attrs.get("state") == "closed"]
    assert blocks.uncovered_bullets(status, parsed) == []
    for campaign in ("pyc", "lrs", "iceberg-maintenance-wave"):
        assert (_REPO / "docs/history" / campaign / "status-record.md").is_file()
    assert (_REPO / "docs/history/hardening-h1/increments-2026-08-15.md").is_file()
    assert "## 2026-08-15" not in status
    slate = (_REPO / "briefs/next-sequence.md").read_text()
    assert "and left this file" not in slate and "## PYC" not in slate
    gate = _load("check_docs_compaction")
    assert gate.findings(_REPO, gate.CEILINGS) == []


def test_the_rule_text_is_in_place() -> None:
    """pins: dl-4-live-doc-compaction-charter/C-008 — each document states the rule once."""
    agents = (_REPO / "AGENTS.md").read_text()
    assert agents.count("**A live document carries no obituary.**") == 1
    assert "make check-docs-compaction" in agents
    skill = (_REPO / ".agents/skills/compact-context-docs/SKILL.md").read_text()
    assert "**Delete,\n   don't narrate:**" in skill or "Delete, don't narrate" in skill.replace(
        "\n   ", " "
    )
    slate = (_REPO / "briefs/next-sequence.md").read_text()
    assert "No departure line for the unit, here or anywhere." in slate
    manifest = (_REPO / ".agents/skills/sepmo/binding-manifest.md").read_text()
    assert manifest.count("`make check-docs-compaction`") == 1
