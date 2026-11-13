from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def is_nan(text: str) -> bool:
    bits = int(text, 16)
    if len(text) == 16:
        return (bits & 0x7FFF_FFFF_FFFF_FFFF) > 0x7FF0_0000_0000_0000
    return (bits & 0x7FFF_FFFF) > 0x7F80_0000


def hexes(value: Any, out: set[str], controls: set[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key not in ("ids", "id", "label", "k"):
                hexes(item, out, controls)
        return
    if isinstance(value, list):
        for item in value:
            hexes(item, out, controls)
        return
    if not isinstance(value, str):
        return
    text = value.split(":", 1)[1] if value[:4] in ("f64:", "f32:") else value
    if len(text) in (8, 16) and all(c in "0123456789abcdef" for c in text):
        (out if is_nan(text) else controls).add(text)


def summary(files: list[dict[str, Any]]) -> dict[str, Any]:
    data: set[str] = set()
    controls: set[str] = set()
    partitions: set[str] = set()
    nan_files = 0
    for item in files:
        hexes(item.get("data", []), data, controls)
        found: set[str] = set()
        hexes(item.get("partition") or {}, found, set())
        if found:
            nan_files += 1
            partitions |= found
    return {
        "data": " ".join(sorted(data)) or "-",
        "controls": " ".join(sorted(controls)) or "-",
        "partitions": " ".join(sorted(partitions)) or "-",
        "nan_files": nan_files,
    }


def cells(path: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record["record"] == "cell":
            if record["outcome"] != "ok":
                out[record["table"]] = {"error": record["error"][:80]}
            else:
                out[record["table"]] = summary(record["files"])
        if record["record"] == "rewrite" and record["phase"] in ("before", "after"):
            out[f"{record['table']} {record['phase']} rewrite"] = summary(record["files"])
        if (
            record["record"] == "rewrite"
            and record["phase"] == "call"
            and record["outcome"] != "ok"
        ):
            out[f"{record['table']} call"] = {"error": record["error"][:80]}
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repark", required=True)
    parser.add_argument("--spark", required=True)
    parser.add_argument("--label", default="main")
    arguments = parser.parse_args()
    repark = cells(Path(arguments.repark))
    spark = cells(Path(arguments.spark))
    print(
        f"| table | {arguments.label} NaN data bits | Spark NaN data bits | "
        f"{arguments.label} NaN partition values (files) | Spark NaN partition values (files) | "
        "non-NaN bits equal |"
    )
    print("|---|---|---|---|---|---|")
    for name in sorted(set(repark) | set(spark)):
        r = repark.get(name, {"error": "absent"})
        s = spark.get(name, {"error": "absent"})
        if "error" in r or "error" in s:
            left = r.get("error", r.get("data"))
            right = s.get("error", s.get("data"))
            print(f"| {name} | {left} | {right} | | | |")
            continue
        same = "yes"
        if r["controls"] != s["controls"]:
            same = f"NO ({r['controls']} / {s['controls']})"
        print(
            f"| {name} | {r['data']} | {s['data']} | {r['partitions']} ({r['nan_files']}) | "
            f"{s['partitions']} ({s['nan_files']}) | {same} |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
