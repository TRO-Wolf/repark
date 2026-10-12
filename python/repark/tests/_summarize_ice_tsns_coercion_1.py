"""Build the twin summary from the fixture and the Spark lattice.

The markdown lists, per sibling and door, where the twin stores the plain column,
where it does not, where the twin's kind differs from Spark, and the range cells.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

CARD_SIBLINGS = (
    "untyped_null",
    "typed_null_ts",
    "typed_null_ntz",
    "us_column_ts",
    "us_column_ntz",
    "us_literal_ts",
    "us_literal_ntz",
    "date_literal",
)
NANOSECOND_TARGETS = ("ts_ns", "tz_ns")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def kind_of(type_name: str) -> str:
    """Return naive, zoned, or the type family of ``type_name``."""
    text = type_name.strip().lower()
    if not text:
        return "error"
    if text.startswith("timestamp_ntz"):
        return "naive"
    if "tz=" in text or ", tz" in text:
        return "zoned"
    if text.startswith("timestamp["):
        return "naive"
    if text.startswith("timestamp"):
        return "zoned"
    if text.startswith("date"):
        return "date"
    if "string" in text or "utf8" in text:
        return "string"
    if text in {"void", "null"} or text.startswith("null"):
        return "null"
    return text


def outcome(fixture: dict[str, Any], slot: int) -> dict[str, Any]:
    """Return one packed outcome, or a missing marker."""
    if slot < 0:
        return {"missing": True, "stored": []}
    return fixture["outcomes"][slot]


def same(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Return whether two stored cells are the same result, including error text."""
    fields = ("skip", "error", "stored", "parquet_type", "missing")
    return all(left.get(field) == right.get(field) for field in fields)


def value_same(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Return whether the stored values, Parquet type and skip flag match."""
    return (
        left.get("stored") == right.get("stored")
        and left.get("parquet_type") == right.get("parquet_type")
        and bool(left.get("skip")) == bool(right.get("skip"))
        and bool(left.get("missing")) == bool(right.get("missing"))
    )


def show(cell: dict[str, Any]) -> str:
    """Return one line of text for a stored cell."""
    if cell.get("missing"):
        return "missing"
    if cell.get("skip"):
        return "skip"
    parts: list[str] = []
    if "error" in cell:
        parts.append("ERR " + str(cell["error"]))
    if cell.get("parquet_type"):
        parts.append(str(cell["parquet_type"]))
    parts.append(json.dumps(cell.get("stored"), separators=(",", ":")))
    return " ".join(parts)


def iter_role(fixture: dict[str, Any], role: str) -> list[tuple[str, str, dict[str, Any]]]:
    """Return ``(row key, door, outcome)`` for ``role``."""
    found: list[tuple[str, str, dict[str, Any]]] = []
    doors = fixture["doors"]
    for key, slots in fixture[role].items():
        for door, slot in zip(doors, slots, strict=True):
            found.append((key, door, outcome(fixture, slot)))
    return found


def plain_index(fixture: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """Index plain outcomes by ``(zone|target|source, door)``."""
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for key, door, cell in iter_role(fixture, "plain"):
        indexed[(key, door)] = cell
    return indexed


def twin_rows(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one record per twin cell, with the plain cell beside it."""
    plains = plain_index(fixture)
    rows: list[dict[str, Any]] = []
    for key, door, cell in iter_role(fixture, "twin"):
        zone, target, source, form, sibling = key.split("|")
        plain = plains.get((f"{zone}|{target}|{source}", door), {"missing": True, "stored": []})
        rows.append(
            {
                "zone": zone,
                "target": target,
                "source": source,
                "form": form,
                "sibling": sibling,
                "door": door,
                "twin": cell,
                "plain": plain,
                "equal": same(cell, plain),
                "value_equal": value_same(cell, plain),
            }
        )
    return rows


def read_kind(cell: dict[str, Any]) -> str:
    """Return the kind of a read or a Spark cell."""
    if "error" in cell and "type" not in cell:
        return "error"
    return kind_of(str(cell.get("type", "")))


def micros_of(tick: int, type_name: str) -> int | None:
    """Return epoch microseconds, truncating nanoseconds toward zero."""
    if "[ns]" in type_name:
        if tick < 0:
            return -((-tick) // 1000)
        return tick // 1000
    if "[us]" in type_name:
        return tick
    return None


def wall_of(tick: int, type_name: str) -> str:
    """Return the UTC wall of a naive timestamp tick."""
    if "[ns]" in type_name:
        scale, digits = 1_000_000_000, 9
    elif "[us]" in type_name:
        scale, digits = 1_000_000, 6
    else:
        return str(tick)
    seconds = tick // scale
    frac = tick - seconds * scale
    moment = EPOCH + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%d %H:%M:%S") + f".{frac:0{digits}d}"


def values_by_id(rows: list[dict[str, Any]], field: str) -> dict[Any, list[Any]]:
    """Group non-null ``field`` values by id."""
    grouped: dict[Any, list[Any]] = defaultdict(list)
    for row in rows:
        value = row.get(field)
        if value is None:
            continue
        grouped[row.get("id")].append(value)
    return {key: sorted(values) for key, values in grouped.items()}


def placement_diff(ours: dict[str, Any], spark: dict[str, Any], kind: str) -> str:
    """Return a placement mismatch, or an empty string when the values agree."""
    if kind == "zoned":
        type_name = str(ours.get("type", ""))
        left: dict[Any, list[Any]] = defaultdict(list)
        for row in ours.get("rows", []):
            tick = row.get("tick")
            if tick is None:
                continue
            micros = micros_of(int(tick), type_name)
            if micros is not None:
                left[row.get("id")].append(micros)
        right = values_by_id(spark.get("rows", []), "micros")
        if not right:
            return "spark has no unix_micros"
        if {key: sorted(value) for key, value in left.items()} != right:
            return f"ours {dict(left)} spark {right}"
        return ""
    if kind != "naive":
        return ""
    type_name = str(ours.get("type", ""))
    our_walls = []
    for row in ours.get("rows", []):
        tick = row.get("tick")
        if tick is None:
            continue
        our_walls.append(wall_of(int(tick), type_name))
    spark_walls = [
        str(row.get("text")) for row in spark.get("rows", []) if row.get("text") is not None
    ]
    if len(our_walls) != len(spark_walls):
        return f"walls {our_walls} vs {spark_walls}"
    for ours_wall, spark_wall in zip(sorted(our_walls), sorted(spark_walls), strict=True):
        spark_text = spark_wall.replace("T", " ").split("+", 1)[0].strip()
        head, _, frac = ours_wall.partition(".")
        other_head, _, other_frac = spark_text.partition(".")
        if head != other_head:
            return f"walls {our_walls} vs {spark_walls}"
        if not (frac.startswith(other_frac) or other_frac.startswith(frac)):
            return f"walls {our_walls} vs {spark_walls}"
    return ""


def kind_rows(fixture: dict[str, Any], lattice: dict[str, Any]) -> list[dict[str, Any]]:
    """Compare each twin read with Spark at the fixture's ANSI mode."""
    ansi = str(fixture.get("ansi", "true")).lower()
    if ansi in {"1", "on"}:
        ansi = "true"
    if ansi in {"0", "off"}:
        ansi = "false"
    found: list[dict[str, Any]] = []
    reads = fixture.get("reads", {})
    cells = lattice.get("cells", {})
    for key, ours in reads.items():
        zone, source, form, sibling, role = key.split("|")
        if role != "twin" or sibling not in CARD_SIBLINGS:
            continue
        spark = cells.get(f"{zone}|{ansi}|{source}|{form}|{sibling}")
        if spark is None:
            found.append(
                {
                    "key": key,
                    "ours": read_kind(ours),
                    "spark": "missing",
                    "placement": "",
                }
            )
            continue
        our_kind = read_kind(ours)
        spark_kind = read_kind(spark)
        placement = ""
        if our_kind == spark_kind:
            placement = placement_diff(ours, spark, our_kind)
        found.append(
            {
                "key": key,
                "ours": our_kind,
                "spark": spark_kind,
                "placement": placement,
                "our_type": ours.get("type", ours.get("error", "")),
                "spark_type": spark.get("type", spark.get("error", "")),
            }
        )
    return found


def count_table(rows: list[dict[str, Any]]) -> list[str]:
    """Return per-sibling, per-door equal and differ counts."""
    counts: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        slot = counts[(row["sibling"], row["door"])]
        slot[0 if row["equal"] else 1] += 1
    lines = ["| sibling | door | equal | differ |", "|---|---|---:|---:|"]
    for sibling, door in sorted(counts):
        equal, differ = counts[(sibling, door)]
        lines.append(f"| {sibling} | {door} | {equal} | {differ} |")
    return lines


def diff_lines(rows: list[dict[str, Any]]) -> list[str]:
    """Group differing cells and list both values."""
    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for row in rows:
        if row["equal"]:
            continue
        signature = (
            row["sibling"],
            row["form"],
            row["source"],
            row["target"],
            show(row["twin"]),
            show(row["plain"]),
        )
        groups[signature].append(f"{row['zone']}/{row['door']}")
    lines = [
        "| sibling | form | source | target | twin | plain | cells |",
        "|---|---|---|---|---|---|---|",
    ]
    for signature in sorted(groups):
        sibling, form, source, target, twin, plain = signature
        cells = ", ".join(sorted(groups[signature]))
        lines.append(
            f"| {sibling} | {form} | {source} | {target} | `{twin}` | `{plain}` | {cells} |"
        )
    if len(lines) == 2:
        return ["No differing cells."]
    return lines


def range_lines(fixture: dict[str, Any]) -> list[str]:
    """Group the range reads."""
    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for key, cell in sorted(fixture.get("range", {}).items()):
        zone, ansi, source, form, sibling, role = key.split("|")
        shown = cell.get("type") or ("ERR " + str(cell.get("error", "")))
        values = json.dumps(cell.get("rows"), separators=(",", ":"))
        signature = (ansi, source, form, sibling, role, str(shown), values)
        groups[signature].append(zone)
    lines = [
        "| ansi | source | form | sibling | role | result | rows | zones |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for signature in sorted(groups):
        ansi, source, form, sibling, role, shown, values = signature
        zones = ", ".join(groups[signature])
        line = (
            f"| {ansi} | {source} | {form} | {sibling} | {role} "
            f"| `{shown}` | `{values}` | {zones} |"
        )
        lines.append(line)
    if len(lines) == 2:
        return ["No range cells."]
    return lines


def build_summary(fixture: dict[str, Any], lattice: dict[str, Any]) -> str:
    """Return the summary markdown."""
    rows = twin_rows(fixture)
    kinds = kind_rows(fixture, lattice)
    untyped = [row for row in rows if row["sibling"] == "untyped_null"]
    ns_untyped = [row for row in untyped if row["target"] in NANOSECOND_TARGETS]
    ns_value_differ = [row for row in ns_untyped if not row["value_equal"]]
    kind_bad = [row for row in kinds if row["ours"] != row["spark"]]
    place_bad = [row for row in kinds if row["placement"]]
    lines = [
        "# Twins summary — ICE-TSNS-COERCION-1 slice 0",
        "",
        "Generated by `_summarize_ice_tsns_coercion_1.py`. Do not edit by hand.",
        "",
        f"- Commit: `{fixture.get('commit', '')}`",
        f"- ANSI: `{fixture.get('ansi', '')}`",
        f"- Statement rows: {len(fixture.get('statement', {}))}",
        f"- Twin rows: {len(fixture.get('twin', {}))}",
        f"- Plain rows: {len(fixture.get('plain', {}))}",
        f"- Twin cells: {len(rows)}",
        f"- Twin cells equal to the plain column: {sum(row['equal'] for row in rows)}",
        f"- Twin cells different from the plain column: {sum(not row['equal'] for row in rows)}",
        f"- Untyped NULL on nanosecond targets: {len(ns_untyped)} cells, "
        f"{len(ns_value_differ)} differ in stored value or Parquet type",
        f"- Twin reads whose kind differs from Spark: {len(kind_bad)}",
        f"- Twin reads whose zone placement differs from Spark: {len(place_bad)}",
        "",
        "## Counts by sibling and door",
        "",
        *count_table(rows),
        "",
        "## Cells where the twin differs from the plain column",
        "",
        *diff_lines(rows),
        "",
        "## Kind against Spark",
        "",
    ]
    if kind_bad:
        lines.append("| read | ours | spark | our type | spark type |")
        lines.append("|---|---|---|---|---|")
        for row in kind_bad:
            lines.append(
                f"| {row['key']} | {row['ours']} | {row['spark']} | "
                f"`{row.get('our_type', '')}` | `{row.get('spark_type', '')}` |"
            )
    else:
        lines.append("Every card-spelling twin read matches Spark's kind.")
    lines.extend(["", "## Zone placement against Spark", ""])
    if place_bad:
        lines.append("| read | kind | detail |")
        lines.append("|---|---|---|")
        for row in place_bad:
            lines.append(f"| {row['key']} | {row['ours']} | `{row['placement']}` |")
    else:
        lines.append("No zone-placement difference on a kind match.")
    lines.extend(["", "## Range cells", "", *range_lines(fixture), ""])
    return "\n".join(lines)


def main() -> None:
    """Write the summary for the fixture and lattice paths."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--lattice", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    lattice = json.loads(args.lattice.read_text(encoding="utf-8"))
    args.output.write_text(build_summary(fixture, lattice), encoding="utf-8")


if __name__ == "__main__":
    main()
