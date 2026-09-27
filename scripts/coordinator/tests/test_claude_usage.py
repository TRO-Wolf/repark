"""Tests for the coordinator transcript token-accounting script."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "claude_usage.py"


def write_lines(path: Path, lines: list[dict[str, object]]) -> None:
    """Write records to path as JSONL."""
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")


def usage_line(stamp: str, values: dict[str, int]) -> dict[str, object]:
    """Build one assistant transcript line carrying message usage."""
    return {"timestamp": stamp, "message": {"role": "assistant", "usage": values}}


def content_line(
    stamp: str, values: dict[str, int], content: list[dict[str, object]]
) -> dict[str, object]:
    """Build one assistant transcript line carrying content and usage."""
    return {
        "timestamp": stamp,
        "message": {"role": "assistant", "content": content, "usage": values},
    }


def make_fixture(root: Path) -> None:
    """Build a two-file fixture with one idle and one non-idle turn."""
    write_lines(
        root / "abcdef0123456789.jsonl",
        [
            usage_line(
                "2026-09-26T10:00:00+00:00",
                {
                    "input_tokens": 100,
                    "cache_read_input_tokens": 1000,
                    "cache_creation_input_tokens": 50,
                    "output_tokens": 200,
                },
            ),
            usage_line(
                "2026-09-26T12:00:00+00:00",
                {
                    "input_tokens": 10,
                    "cache_read_input_tokens": 20,
                    "cache_creation_input_tokens": 30,
                    "output_tokens": 40,
                },
            ),
            content_line(
                "2026-09-26T13:00:00+00:00",
                {
                    "input_tokens": 1,
                    "cache_read_input_tokens": 2,
                    "cache_creation_input_tokens": 3,
                    "output_tokens": 4,
                },
                [
                    {"type": "text", "text": "Checking state."},
                    {
                        "type": "tool_use",
                        "name": "Read",
                        "input": {"file_path": "/tmp/state.md"},
                    },
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "input": {"command": "git status --short"},
                    },
                ],
            ),
            content_line(
                "2026-09-26T14:00:00+00:00",
                {
                    "input_tokens": 7,
                    "cache_read_input_tokens": 8,
                    "cache_creation_input_tokens": 9,
                    "output_tokens": 10,
                },
                [
                    {"type": "text", "text": "Writing the fix."},
                    {
                        "type": "tool_use",
                        "name": "Write",
                        "input": {"file_path": "/tmp/state.md"},
                    },
                ],
            ),
            {
                "timestamp": "2026-09-26T12:30:00+00:00",
                "message": {"role": "user"},
            },
        ],
    )
    agent_dir = root / "abcdef0123456789" / "subagents"
    agent_dir.mkdir(parents=True)
    write_lines(
        agent_dir / "agent-987654321.jsonl",
        [
            usage_line(
                "2026-09-26T11:00:00+00:00",
                {
                    "input_tokens": 5,
                    "cache_read_input_tokens": 6,
                    "cache_creation_input_tokens": 7,
                    "output_tokens": 8,
                },
            ),
        ],
    )


def run_json(root: Path, extra: list[str]) -> dict[str, dict[str, object]]:
    """Run the script with --json over root and return rows keyed by label."""
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), str(root), "--json", *extra],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    decoded = json.loads(completed.stdout)
    assert isinstance(decoded, list)
    rows: dict[str, dict[str, object]] = {row["label"]: row for row in decoded}
    return rows


def test_sums_group_by_file(tmp_path: Path) -> None:
    """Usage sums group by file with turns counted and a total row."""
    make_fixture(tmp_path)
    rows = run_json(tmp_path, [])
    assert rows["abcdef01"]["turns"] == 4
    assert rows["abcdef01"]["input_tokens"] == 118
    assert rows["abcdef01"]["cache_read_input_tokens"] == 1030
    assert rows["abcdef01"]["cache_creation_input_tokens"] == 92
    assert rows["abcdef01"]["output_tokens"] == 254
    assert rows["agent-98765432"]["turns"] == 1
    assert rows["agent-98765432"]["input_tokens"] == 5
    assert rows["total"]["turns"] == 5
    assert rows["total"]["input_tokens"] == 123
    assert rows["total"]["cache_read_input_tokens"] == 1036
    assert rows["total"]["cache_creation_input_tokens"] == 99
    assert rows["total"]["output_tokens"] == 262


def test_since_filters_old_lines(tmp_path: Path) -> None:
    """The --since filter drops lines with older stamps from every row."""
    make_fixture(tmp_path)
    rows = run_json(tmp_path, ["--since", "2026-09-26T11:30:00+00:00"])
    assert rows["abcdef01"]["turns"] == 3
    assert rows["abcdef01"]["input_tokens"] == 18
    assert rows["abcdef01"]["idle_turns"] == 1
    assert rows["abcdef01"]["idle_pct"] == pytest.approx(1 / 3)
    assert "agent-98765432" not in rows
    assert rows["total"]["turns"] == 3
    assert rows["total"]["output_tokens"] == 54
    assert rows["total"]["idle_turns"] == 1


def test_table_mode_prints_rows_and_total(tmp_path: Path) -> None:
    """The default table mode prints one row per file plus a total row."""
    make_fixture(tmp_path)
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    assert "abcdef01 4 118 1030 92 254 1 0.25" in completed.stdout
    assert "agent-98765432 1 5 6 7 8 0 0.00" in completed.stdout
    assert "total 5 123 1036 99 262 1 0.20" in completed.stdout


def test_idle_turns_count_read_only_short_turns(tmp_path: Path) -> None:
    """Idle turns are read-only calls with short text; bare lines are not idle."""
    make_fixture(tmp_path)
    rows = run_json(tmp_path, [])
    assert rows["abcdef01"]["idle_turns"] == 1
    assert rows["abcdef01"]["idle_pct"] == 0.25
    assert rows["agent-98765432"]["idle_turns"] == 0
    assert rows["agent-98765432"]["idle_pct"] == 0.0
    assert rows["total"]["idle_turns"] == 1
    assert rows["total"]["idle_pct"] == pytest.approx(0.2)
