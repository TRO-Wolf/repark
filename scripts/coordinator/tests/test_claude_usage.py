"""Tests for the coordinator transcript token-accounting script."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "claude_usage.py"


def write_lines(path: Path, lines: list[dict[str, object]]) -> None:
    """Write records to path as JSONL."""
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")


def usage_line(stamp: str, values: dict[str, int]) -> dict[str, object]:
    """Build one assistant transcript line carrying message usage."""
    return {"timestamp": stamp, "message": {"role": "assistant", "usage": values}}


def make_fixture(root: Path) -> None:
    """Build a two-file transcript fixture: one main session, one sub-agent."""
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
    assert rows["abcdef01"]["turns"] == 2
    assert rows["abcdef01"]["input_tokens"] == 110
    assert rows["abcdef01"]["cache_read_input_tokens"] == 1020
    assert rows["abcdef01"]["cache_creation_input_tokens"] == 80
    assert rows["abcdef01"]["output_tokens"] == 240
    assert rows["agent-98765432"]["turns"] == 1
    assert rows["agent-98765432"]["input_tokens"] == 5
    assert rows["total"]["turns"] == 3
    assert rows["total"]["input_tokens"] == 115
    assert rows["total"]["cache_read_input_tokens"] == 1026
    assert rows["total"]["cache_creation_input_tokens"] == 87
    assert rows["total"]["output_tokens"] == 248


def test_since_filters_old_lines(tmp_path: Path) -> None:
    """The --since filter drops lines with older stamps from every row."""
    make_fixture(tmp_path)
    rows = run_json(tmp_path, ["--since", "2026-09-26T11:30:00+00:00"])
    assert rows["abcdef01"]["turns"] == 1
    assert rows["abcdef01"]["input_tokens"] == 10
    assert "agent-98765432" not in rows
    assert rows["total"]["turns"] == 1
    assert rows["total"]["output_tokens"] == 40


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
    assert "abcdef01 2 110 1020 80 240" in completed.stdout
    assert "agent-98765432 1 5 6 7 8" in completed.stdout
    assert "total 3 115 1026 87 248" in completed.stdout
