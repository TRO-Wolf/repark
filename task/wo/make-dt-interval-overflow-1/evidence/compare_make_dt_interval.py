from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def short(cell: dict[str, Any] | None) -> str:
    if cell is None:
        return "-"
    if cell["kind"] == "error":
        label = cell.get("cls") or cell.get("first", "")[:90]
        return f"raises `{label}`"
    if "readback" in cell:
        return "ok; " + short(cell["readback"])
    if "stored" in cell:
        return "stored " + ", ".join(cell["stored"])
    if "tree" in cell:
        return cell["tree"].replace("\n", " ")
    if "types" in cell:
        return f"{cell['rows']} `{cell['types']}` nulls {cell['nulls']}"
    rows = cell["rows"]
    types = ",".join(t for _, t in cell["dtypes"])
    return f"{rows} `{types}` nulls {cell['nulls']}"


def same(spark: dict[str, Any] | None, other: dict[str, Any] | None) -> bool:
    if spark is None or other is None:
        return False
    if spark["kind"] == "error" or other["kind"] == "error":
        if spark["kind"] != other["kind"]:
            return False
        wanted = spark.get("cls", "")
        return bool(wanted) and wanted in (
            other.get("cls", "") + other.get("first", "")
        )
    for key in ("dtypes", "nulls"):
        if (spark.get(key) or [None])[-1] != (other.get(key) or [None])[-1]:
            return False
    for key in ("rows", "stored"):
        if spark.get(key) != other.get(key):
            return False
    if spark.get("tree", "").splitlines()[-1:] != other.get("tree", "").splitlines()[-1:]:
        return False
    if "readback" in spark:
        return same(spark["readback"], other.get("readback"))
    return True


def verdict(
    spark: dict[str, Any] | None, main: dict[str, Any] | None, head: Any
) -> str:
    main_ok = same(spark, main)
    if head is None:
        return "=" if main_ok else "≠"
    head_ok = same(spark, head)
    if main_ok and head_ok:
        return "= / ="
    if main_ok:
        return "**MOVED OFF**"
    if head_ok:
        return "fixed"
    if main is not None and head == main:
        return "≠ / ≠ (unmoved)"
    return "≠ / ≠ (moved)"


def load(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text())["cells"]


def door_and_statement(key: str) -> tuple[str, str]:
    door, source, expr = (key.split("|", 2) + ["", ""])[:3]
    return door, f"{source} · `{expr}`" if expr else source


def build(args: argparse.Namespace) -> tuple[list[str], dict[str, int]]:
    lines = [
        "| Door | Statement | ANSI | Spark 4.1.2 | Main | Head | Verdict |",
        "|---|---|---|---|---|---|---|",
    ]
    counts: dict[str, int] = {}
    for mode in ("false", "true"):
        spark = load(args.dir / f"{args.prefix_spark}_{mode}.json") or {}
        main = load(args.dir / f"{args.prefix_main}_{mode}.json") or {}
        head = (
            load(args.dir / f"{args.prefix_head}_{mode}.json")
            if args.prefix_head
            else None
        )
        keys = [k for k in main if not k.startswith(("native|", "write.parquet|"))]
        for key in keys:
            head_cell = head.get(key) if head is not None else None
            mark = verdict(
                spark.get(key), main.get(key), head_cell if head is not None else None
            )
            counts[mark] = counts.get(mark, 0) + 1
            door, stmt = door_and_statement(key)
            head_text = short(head_cell) if head is not None else "-"
            lines.append(
                f"| {door} | {stmt} | {mode} | {short(spark.get(key))} | "
                f"{short(main.get(key))} | {head_text} | {mark} |"
            )
    return lines, counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--prefix-spark", default="spark")
    parser.add_argument("--prefix-main", default="main")
    parser.add_argument("--prefix-head")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--only-diff", action="store_true")
    args = parser.parse_args()
    lines, counts = build(args)
    if args.only_diff:
        lines = lines[:2] + [
            ln for ln in lines[2:] if not ln.endswith(("| = |", "| = / = |"))
        ]
    args.out.write_text("\n".join(lines) + "\n")
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
