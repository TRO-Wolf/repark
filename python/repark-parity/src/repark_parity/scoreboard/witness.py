from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from repark_parity.scoreboard.encoding import encode_line, read_records

SHOWN_FIELDS = (
    "outcome",
    "value",
    "schema",
    "sqlstate",
    "error_class",
    "message",
    "physical_write",
    "reason",
)


def witnesses(
    directory: Path, match: str = "", kind: str | None = None, limit: int = 20
) -> Iterator[str]:
    """Yield joined cell, verdict and both answers for cells of a class, name or id fragment."""
    cases, spark, repark, diff = (
        read_records(directory / (name + ".jsonl")) for name in ("cases", "spark", "repark", "diff")
    )
    shown = 0
    for key, verdict in diff.items():
        if kind and verdict["class"] != kind:
            continue
        if match.lower() not in encode_line([key, cases[key].get("names")]).lower():
            continue
        answers = [
            {field: item[field] for field in SHOWN_FIELDS if field in item}
            for item in (spark[key], repark[key])
        ]
        yield encode_line(
            {"cell": cases[key], "verdict": verdict, "spark": answers[0], "repark": answers[1]}
        )
        shown += 1
        if shown >= limit:
            return
