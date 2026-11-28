from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from functools import cache
from pathlib import Path

from repark_parity.scoreboard.encoding import write_lines

ZONES = ("UTC", "America/Los_Angeles", "Asia/Kolkata")
API_REVIEW = Path(__file__).resolve().parents[5] / "docs/design/v1-0-api-review-2026-09-02.json"


@cache
def api_rows() -> tuple[dict, ...]:
    """Return the dated API review rows that map a public name to its freeze row."""
    return tuple(json.loads(API_REVIEW.read_text())["rows"])


def write_cells(path: Path, cells: Iterable[dict]) -> None:
    """Write a case file, failing on a duplicate id."""
    rows = list(cells)
    seen: set[str] = set()
    for cell in rows:
        if cell["id"] in seen:
            raise ValueError(f"duplicate id {cell['id']}")
        seen.add(cell["id"])
    write_lines(path, rows)


def surface_for(name: str, fallback: str) -> str:
    """Return the freeze row that lists a public name, or the fallback row."""
    for row in api_rows():
        if name in (row.get("member_names") or []):
            return row["id"]
    return fallback


def scalar_grid(
    key: str,
    name: str,
    expressions: tuple[str, str, str],
    seed: str,
    predicate: str,
    zones: tuple[str, ...] = ("UTC",),
    modes: tuple[bool, ...] = (True, False),
    fallback: str = "J1",
) -> Iterator[dict]:
    """Expand one scalar expression into six seats per ANSI mode and zone."""
    literal, column, typed = expressions
    surface = surface_for(name, fallback)
    setup = [f"df = fixture({seed})", "df.createOrReplaceTempView('audit_input')"]
    for ansi in modes:
        for zone in zones:
            base = {"ansi": ansi, "zone": zone, "names": [name], "surface": surface}
            prefix = f"{key}.{'on' if ansi else 'off'}.{zone.split('/')[-1]}"
            seats = [
                ("sql-literal", {"sql": f"SELECT {literal} AS v"}),
                ("sql-column", {"sql": f"SELECT {column} AS v FROM audit_input", "setup": setup}),
                (
                    "expr-column",
                    {"py": f"df.select(F.expr({column!r}).alias('v'))", "setup": setup},
                ),
                ("typed-column", {"py": f"df.select(({typed}).alias('v'))", "setup": setup}),
                ("expr-literal", {"py": f"s.range(1).select(F.expr({literal!r}).alias('v'))"}),
                (
                    "sql-filter",
                    {
                        "sql": "SELECT rid FROM audit_input WHERE "
                        + predicate.format(v=f"({column})"),
                        "setup": setup,
                    },
                ),
            ]
            for seat, options in seats:
                yield {**base, **options, "id": f"{prefix}.{seat}", "seat": seat}
