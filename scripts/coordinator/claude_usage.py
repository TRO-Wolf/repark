#!/usr/bin/env python3
"""Per-file Claude token accounting over transcript JSONL.

Main sessions are ``*.jsonl``; sub-agents are ``<session>/subagents/agent-*.jsonl``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

TOKEN_FIELDS: tuple[str, ...] = (
    "input_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse the transcript-directory CLI arguments."""
    parser = argparse.ArgumentParser(description="Sum Claude transcript tokens per file.")
    parser.add_argument("transcript_dir", type=Path, help="Transcript root directory.")
    parser.add_argument("--since", default=None, help="Keep stamps at or after this stamp.")
    parser.add_argument("--json", action="store_true", help="Emit rows as JSON.")
    return parser.parse_args(argv)


def decode_line(line: str) -> dict[str, object] | None:
    """Decode one transcript line, or None when it is not a JSON object."""
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None
    return record if isinstance(record, dict) else None


def transcript_files(root: Path) -> list[Path]:
    """List main-session and sub-agent transcript files under root, sorted."""
    files = sorted(root.glob("*.jsonl"))
    for child in sorted(root.iterdir()):
        if child.is_dir():
            files.extend(sorted((child / "subagents").glob("agent-*.jsonl")))
    return files


def label_for(path: Path) -> str:
    """Label one transcript: session-stem prefix, or agent- plus agent prefix."""
    stem = path.stem
    if path.parent.name == "subagents":
        return f"agent-{stem.removeprefix('agent-')[:8]}"
    return stem[:8]


def summarize(path: Path, since: str | None) -> dict[str, int]:
    """Sum usage tokens in one file; turns count assistant lines with usage."""
    totals: dict[str, int] = dict.fromkeys(["turns", *TOKEN_FIELDS], 0)
    for line in path.read_text(encoding="utf-8").splitlines():
        record = decode_line(line)
        if record is None:
            continue
        stamp = record.get("timestamp")
        if since is not None and isinstance(stamp, str) and stamp < since:
            continue
        message = record.get("message")
        if not isinstance(message, dict):
            continue
        usage = message.get("usage")
        if not isinstance(usage, dict):
            continue
        if message.get("role") == "assistant":
            totals["turns"] += 1
        for field in TOKEN_FIELDS:
            value = usage.get(field)
            if isinstance(value, int):
                totals[field] += value
    return totals


def total_of(rows: list[tuple[str, dict[str, int]]]) -> dict[str, int]:
    """Sum one totals row over collected rows."""
    totals: dict[str, int] = dict.fromkeys(["turns", *TOKEN_FIELDS], 0)
    for _, row in rows:
        for key in totals:
            totals[key] += row[key]
    return totals


def format_table(rows: list[tuple[str, dict[str, int]]], total: dict[str, int]) -> str:
    """Format collected rows plus a total row as a plain table."""
    header = ["label", "turns", *TOKEN_FIELDS]
    lines = [" ".join(header)]
    for label, row in [*rows, ("total", total)]:
        lines.append(" ".join([label, *[str(row[key]) for key in header[1:]]]))
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Run the token-accounting CLI; return the process exit code."""
    args = parse_args(argv)
    root: Path = args.transcript_dir
    if not root.is_dir():
        print(f"error: transcript dir not found: {root}", file=sys.stderr)
        return 2
    rows = [(label_for(path), summarize(path, args.since)) for path in transcript_files(root)]
    total = total_of(rows)
    if args.json:
        payload = [{"label": label, **row} for label, row in [*rows, ("total", total)]]
        print(json.dumps(payload, indent=2))
    else:
        print(format_table(rows, total), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
