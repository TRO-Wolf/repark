"""Parse `EXPLAIN ANALYZE` text into per-operator-class metric totals."""

from __future__ import annotations

import re

COUNT_COUNTERS: tuple[str, ...] = (
    "spill_count",
    "spilled_rows",
    "output_rows",
    "skipped_aggregation_rows",
)
SIZE_COUNTERS: tuple[str, ...] = ("spilled_bytes", "peak_mem_used", "output_bytes")
COUNTERS: tuple[str, ...] = COUNT_COUNTERS + SIZE_COUNTERS

COUNT_UNITS: dict[str, int] = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 10**12}
SIZE_UNITS: dict[str, int] = {"B": 1, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}

_NODE = re.compile(r"^(?P<name>[A-Za-z][A-Za-z0-9_]*)\b")
_METRIC = re.compile(
    r"\b(?P<key>[a-z_]+)=(?P<num>\d+(?:\.\d+)?)\s?(?P<unit>TB|GB|MB|KB|[KMBT]|B)?(?=[,\]\s]|$)"
)


def parse_value(key: str, number: str, unit: str | None) -> int:
    """Undo DataFusion's human display: counts are 1000-based, sizes 1024-based."""
    table = SIZE_UNITS if key in SIZE_COUNTERS else COUNT_UNITS
    multiplier = table.get(unit or "", 1)
    return round(float(number) * multiplier)


def _strip_frame(line: str) -> str:
    """Drop the tree drawing and row frame around one plan line."""
    return line.strip().lstrip("+-> |").strip()


def plan_text_from_rows(rows: list[object]) -> str:
    """Join the `plan` column of an `EXPLAIN ANALYZE` result into one text."""
    chunks: list[str] = []
    for row in rows:
        mapping = row.asDict(recursive=False) if hasattr(row, "asDict") else None
        if mapping is not None and "plan" in mapping:
            chunks.append(str(mapping["plan"]))
        elif hasattr(row, "__getitem__") and hasattr(row, "__len__") and len(row) > 1:
            chunks.append(str(row[1]))
        else:
            chunks.append(str(row))
    return "\n".join(chunks)


def parse_nodes(plan_text: str) -> dict[str, dict[str, int]]:
    """Total every counter in COUNTERS per physical-operator class name."""
    totals: dict[str, dict[str, int]] = {}
    for raw in plan_text.splitlines():
        text = _strip_frame(raw)
        match = _NODE.match(text)
        if match is None or "metrics=[" not in text:
            continue
        name = match.group("name")
        if not name.endswith("Exec"):
            continue
        bucket = totals.setdefault(name, dict.fromkeys(COUNTERS, 0))
        bucket["instances"] = bucket.get("instances", 0) + 1
        body = text.split("metrics=[", 1)[1]
        for hit in _METRIC.finditer(body):
            key = hit.group("key")
            if key in COUNTERS:
                bucket[key] += parse_value(key, hit.group("num"), hit.group("unit"))
    return totals


def total_counter(totals: dict[str, dict[str, int]], counter: str) -> int:
    """Sum one counter over every operator class in `totals`."""
    return sum(bucket.get(counter, 0) for bucket in totals.values())
