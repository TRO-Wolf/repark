from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

NAN_IDS = (1, 2, 3, 4, 5, 6, 7, 8)
CONTROL_IDS = (9, 10, 11, 12, 13, 14)


def load(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def by_table(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"files": [], "door": None, "checks": None}
    )
    for record in records:
        if record["record"] == "door":
            out[record["table"]]["door"] = record
        elif record["record"] == "checks":
            out[record["table"]]["checks"] = record
        elif record["record"] == "file":
            out[record["table"]]["files"].append(record)
    return out


def stored(entry: dict[str, Any], column: str) -> dict[int, Any]:
    out: dict[int, Any] = {}
    for item in entry["files"]:
        for row in item.get("rows", []):
            if "id" in row and column in row:
                out[row["id"]] = row[column]
    return out


def nan_bits(entry: dict[str, Any], column: str) -> str:
    values = stored(entry, column)
    found = sorted({values[i] for i in NAN_IDS if i in values and values[i] is not None})
    return " ".join(found) if found else "-"


def controls(entry: dict[str, Any], column: str) -> dict[int, Any]:
    values = stored(entry, column)
    return {i: values.get(i) for i in CONTROL_IDS}


def nan_partitions(entry: dict[str, Any]) -> tuple[str, int]:
    values = set()
    files = 0
    for item in entry["files"]:
        partition = item.get("partition") or {}
        d = partition.get("d")
        if isinstance(d, str) and d.startswith("f64:") and _is_nan64(d[4:]):
            values.add(d[4:])
            files += 1
    return (" ".join(sorted(values)) or "-", files)


def _is_nan64(text: str) -> bool:
    bits = int(text, 16)
    return (bits & 0x7FFF_FFFF_FFFF_FFFF) > 0x7FF0_0000_0000_0000


def check(entry: dict[str, Any], name: str) -> Any:
    checks = entry["checks"] or {}
    item = checks.get(name) or {}
    if item.get("outcome") != "ok":
        return item.get("error_type", "-")
    value = item["value"]
    return ",".join(str(v) for v in value) if isinstance(value, list) else value


def nan_count(entry: dict[str, Any], field: str) -> int:
    return sum(
        int(item.get("nan_value_counts", {}).get(field, 0))
        for item in entry["files"]
        if item.get("content", 0) == 0
    )


def render(
    repark: dict[str, dict[str, Any]], spark: dict[str, dict[str, Any]], label: str
) -> list[str]:
    lines = [
        f"| table | {label} NaN bits d / f | Spark NaN bits d / f | controls d, f equal | "
        f"NaN partition value {label} / Spark | NaN-partition files {label} / Spark | "
        f"`= NaN` d {label} / Spark | `isnan(d)` {label} / Spark | NaN count d {label} / Spark |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for name in sorted(set(repark) | set(spark)):
        r, s = repark.get(name), spark.get(name)
        if r is None or s is None:
            continue
        if r["door"] and r["door"]["outcome"] != "ok":
            lines.append(f"| {name} | refused: {r['door'].get('error', '')[:90]} | | | | | | | |")
            continue
        same = controls(r, "d") == controls(s, "d") and controls(r, "f") == controls(s, "f")
        rp, rf = nan_partitions(r)
        sp, sf = nan_partitions(s)
        lines.append(
            f"| {name} | {nan_bits(r, 'd')} / {nan_bits(r, 'f')} | "
            f"{nan_bits(s, 'd')} / {nan_bits(s, 'f')} | {'yes' if same else 'NO'} | "
            f"{rp} / {sp} | {rf} / {sf} | {check(r, 'eq_nan_d')} / {check(s, 'eq_nan_d')} | "
            f"{check(r, 'isnan_d')} / {check(s, 'isnan_d')} | "
            f"{nan_count(r, '3')} / {nan_count(s, '3')} |"
        )
    return lines


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repark", required=True)
    parser.add_argument("--spark", required=True)
    parser.add_argument("--label", default="main")
    arguments = parser.parse_args()
    repark = by_table(load(Path(arguments.repark)))
    spark = by_table(load(Path(arguments.spark)))
    native = {name: entry for name, entry in repark.items() if "native" in name}
    print("\n".join(render(repark, spark, arguments.label)))
    print()
    print(f"| native table | {arguments.label} NaN bits d / f | controls d | outcome |")
    print("|---|---|---|---|")
    for name, entry in sorted(native.items()):
        door = entry["door"] or {}
        print(
            f"| {name} | {nan_bits(entry, 'd')} / {nan_bits(entry, 'f')} | "
            f"{controls(entry, 'd')} | {door.get('outcome')} {door.get('error', '')[:90]} |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
