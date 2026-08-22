"""Bank minimized divergence repros under ``repros/<seed>-<n>.sql``.

Each banked file is a self-contained artifact:

- header comments: seed, query index, compare message, row snippets
- CREATE-style comments describing the fixture tables (``-- TABLE`` + ``--   ROW`` JSON)
- the minimized SQL

Empty corpus is a valid outcome — do not pad.

Replay pins **must** restore the minimized TABLE rows via
``load_minimized_database`` (C1-Q-002) — not the full seed fixture alone.
"""

from __future__ import annotations

import ast
import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from .datagen import FuzzDatabase, FuzzTable
from .minimizer import MinimizedRepro

REPROS_DIR_NAME = "repros"


class BankedRepro(BaseModel):
    """Index entry for one banked repro."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    seed: int
    query_index: int
    path: str
    sql: str
    compare_message: str
    pin_id: str


def default_repros_dir() -> Path:
    return Path(__file__).resolve().parent / REPROS_DIR_NAME


def next_bank_sequence(repros_dir: Path | None = None, *, seed: int) -> int:
    """Return ``max(existing sequences for seed) + 1``, or 1 if none (C4-L-001)."""
    out_dir = repros_dir if repros_dir is not None else default_repros_dir()
    if not out_dir.is_dir():
        return 1
    pattern = re.compile(rf"^{re.escape(str(seed))}-(?P<seq>\d+)\.sql$")
    highest = 0
    for path in out_dir.glob(f"{seed}-*.sql"):
        match = pattern.match(path.name)
        if match is None:
            continue
        highest = max(highest, int(match.group("seq")))
    return highest + 1


def bank_repro(
    repro: MinimizedRepro,
    *,
    repros_dir: Path | None = None,
    sequence: int,
) -> BankedRepro:
    """Write ``<seed>-<sequence>.sql`` and return the index entry."""
    out_dir = repros_dir if repros_dir is not None else default_repros_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    pin_id = f"fuzz-{repro.seed}-{sequence}"
    file_name = f"{repro.seed}-{sequence}.sql"
    path = out_dir / file_name
    if path.exists():
        # Refuse silent overwrite of an existing banked witness (C4-L-001).
        msg = f"bank path already exists (refusing overwrite): {path}"
        raise FileExistsError(msg)

    repark_snip = _snip_rows(repro.repark_rows)
    duck_snip = _snip_rows(repro.duckdb_rows)
    # Single-line comments only — newlines would break header parsers (C3-Q-001).
    compare_one_line = " ".join(str(repro.compare_message).splitlines()).strip()
    lines: list[str] = [
        f"-- R-SQL-FUZZER banked repro pin_id={pin_id}",
        f"-- seed={repro.seed}",
        f"-- query_index={repro.query_index}",
        f"-- minimizer_steps={repro.steps}",
        f"-- has_order_by={int(repro.spec.has_order_by)}",
        f"-- compare: {compare_one_line}",
        f"-- repark_rows: {repark_snip}",
        f"-- duckdb_rows: {duck_snip}",
        "--",
        "-- Fixture tables (seeded subset after minimization):",
    ]
    for table_name, table in sorted(repro.database.tables.items()):
        cols = ", ".join(f"{name}:{col_type}" for name, col_type in table.columns)
        lines.append(f"-- TABLE {table_name} ({cols})")
        for row in table.rows:
            # JSON row encoding (not Python repr) so Decimal/date/datetime round-trip
            # via load_minimized_database without eval (C1-Q-002).
            lines.append(f"--   ROW {_encode_row_json(row)}")
    lines.append("--")
    lines.append(repro.sql.rstrip() + "\n")
    path.write_text("\n".join(lines), encoding="utf-8")

    return BankedRepro(
        seed=repro.seed,
        query_index=repro.query_index,
        path=str(path),
        sql=repro.sql,
        compare_message=repro.compare_message,
        pin_id=pin_id,
    )


def list_banked_repros(repros_dir: Path | None = None) -> list[BankedRepro]:
    """Scan the repros directory for ``<seed>-<n>.sql`` files."""
    out_dir = repros_dir if repros_dir is not None else default_repros_dir()
    if not out_dir.is_dir():
        return []
    found: list[BankedRepro] = []
    pattern = re.compile(r"^(?P<seed>\d+)-(?P<seq>\d+)\.sql$")
    for path in sorted(out_dir.glob("*.sql")):
        match = pattern.match(path.name)
        if match is None:
            continue
        text = path.read_text(encoding="utf-8")
        seed = int(match.group("seed"))
        sequence = int(match.group("seq"))
        pin_id = f"fuzz-{seed}-{sequence}"
        query_index = _comment_int(text, "query_index") or -1
        compare_message = _comment_str(text, "compare") or ""
        sql = _extract_sql(text)
        found.append(
            BankedRepro(
                seed=seed,
                query_index=query_index,
                path=str(path),
                sql=sql,
                compare_message=compare_message,
                pin_id=pin_id,
            )
        )
    return found


def write_corpus_index(
    banked: list[BankedRepro],
    *,
    path: Path,
) -> None:
    """Write a JSON index of banked repros (for the ledger / xfail pins)."""
    payload = {
        "corpus_count": len(banked),
        "empty": len(banked) == 0,
        "repros": [item.model_dump() for item in banked],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _snip_rows(rows: list[tuple[Any, ...]], *, limit: int = 5) -> str:
    head = rows[:limit]
    suffix = "" if len(rows) <= limit else f" …(+{len(rows) - limit} more)"
    return repr(head) + suffix


def _comment_int(text: str, key: str) -> int | None:
    match = re.search(rf"^-- {re.escape(key)}=(-?\d+)\s*$", text, flags=re.MULTILINE)
    if match is None:
        return None
    return int(match.group(1))


def _comment_str(text: str, key: str) -> str | None:
    match = re.search(rf"^-- {re.escape(key)}:\s*(.*)$", text, flags=re.MULTILINE)
    if match is None:
        return None
    return match.group(1).strip()


def _extract_sql(text: str) -> str:
    lines = text.splitlines()
    sql_lines = [line for line in lines if not line.startswith("--") and line.strip()]
    return "\n".join(sql_lines).strip()


_TABLE_HEADER = re.compile(
    r"^-- TABLE (?P<name>\w+) \((?P<cols>.+)\)\s*$",
)
_TABLE_ROW_JSON = re.compile(r"^--   ROW (?P<row>\[.*\])\s*$")
# Legacy Python-repr rows (pre-octo C1) — best-effort via literal_eval for ints/str/None only.
_TABLE_ROW_LEGACY = re.compile(r"^--   (?P<row>\(.*\))\s*$")


def _encode_row_json(row: tuple[Any, ...]) -> str:
    """Encode one fixture row as a JSON array with typed objects for Decimal/date/ts."""
    encoded: list[Any] = []
    for cell in row:
        if cell is None or isinstance(cell, (bool, int, str)):
            encoded.append(cell)
        elif isinstance(cell, float):
            if math.isnan(cell) or math.isinf(cell):
                encoded.append({"__float__": str(cell)})
            else:
                encoded.append(cell)
        elif isinstance(cell, Decimal):
            encoded.append({"__decimal__": str(cell)})
        elif isinstance(cell, datetime):
            encoded.append({"__datetime__": cell.isoformat()})
        elif isinstance(cell, date):
            encoded.append({"__date__": cell.isoformat()})
        else:
            encoded.append({"__repr__": repr(cell)})
    return json.dumps(encoded, separators=(",", ":"), ensure_ascii=False)


def _decode_row_json(raw: str) -> tuple[Any, ...]:
    payload = json.loads(raw)
    if not isinstance(payload, list):
        msg = f"bank row JSON must be an array; got {type(payload).__name__}"
        raise ValueError(msg)
    cells: list[Any] = []
    for item in payload:
        if isinstance(item, dict):
            if "__decimal__" in item:
                cells.append(Decimal(item["__decimal__"]))
            elif "__datetime__" in item:
                cells.append(datetime.fromisoformat(item["__datetime__"]))
            elif "__date__" in item:
                cells.append(date.fromisoformat(item["__date__"]))
            elif "__float__" in item:
                cells.append(float(item["__float__"]))
            elif "__repr__" in item:
                cells.append(item["__repr__"])
            else:
                msg = f"unknown bank cell tag: {item!r}"
                raise ValueError(msg)
        else:
            cells.append(item)
    return tuple(cells)


def _flush_parsed_table(
    tables: dict[str, FuzzTable],
    current_name: str | None,
    current_cols: list[tuple[str, str]],
    current_rows: list[tuple[Any, ...]],
) -> tuple[str | None, list[tuple[str, str]], list[tuple[Any, ...]]]:
    """Commit one TABLE block into ``tables`` and reset the current buffers."""
    if current_name is None:
        return None, current_cols, current_rows
    tables[current_name] = FuzzTable(
        name=current_name,
        columns=tuple(current_cols),
        rows=tuple(current_rows),
    )
    return None, [], []


def parse_minimized_tables(text: str) -> dict[str, FuzzTable]:
    """Parse ``-- TABLE`` / ``--   ROW […]`` comments into ``FuzzTable``s.

    Returns an empty dict when the bank file has no fixture section.
    """
    tables: dict[str, FuzzTable] = {}
    current_name: str | None = None
    current_cols: list[tuple[str, str]] = []
    current_rows: list[tuple[Any, ...]] = []

    for line in text.splitlines():
        header = _TABLE_HEADER.match(line)
        if header is not None:
            current_name, current_cols, current_rows = _flush_parsed_table(
                tables, current_name, current_cols, current_rows
            )
            current_name = header.group("name")
            current_cols = []
            for part in header.group("cols").split(","):
                part = part.strip()
                if not part:
                    continue
                if ":" not in part:
                    msg = f"bank table column missing type: {part!r}"
                    raise ValueError(msg)
                col_name, col_type = part.split(":", maxsplit=1)
                current_cols.append((col_name.strip(), col_type.strip()))
            current_rows = []
            continue
        row_json = _TABLE_ROW_JSON.match(line)
        if row_json is not None and current_name is not None:
            current_rows.append(_decode_row_json(row_json.group("row")))
            continue
        row_legacy = _TABLE_ROW_LEGACY.match(line)
        if row_legacy is not None and current_name is not None:
            parsed = ast.literal_eval(row_legacy.group("row"))
            if not isinstance(parsed, tuple):
                parsed = (parsed,)
            current_rows.append(tuple(parsed))
    _flush_parsed_table(tables, current_name, current_cols, current_rows)
    return tables


def load_minimized_database(text: str, *, seed: int) -> FuzzDatabase | None:
    """Build a ``FuzzDatabase`` from banked TABLE comments, or None if absent."""
    tables = parse_minimized_tables(text)
    if not tables:
        return None
    return FuzzDatabase(seed=seed, tables=tables)
