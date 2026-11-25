from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

UNWIRED = {"default_load", "sql", "view"}
DOOR_TEXT = {
    "reader": "spark.read.parquet(p)",
    "format_load": 'spark.read.format("parquet").load(p)',
    "default_load": "spark.read.load(p)",
    "sql": "SELECT * FROM parquet.`p`",
    "view": "CREATE TEMPORARY VIEW v USING parquet OPTIONS (path p)",
    "native": "native read_parquet(p)",
}


def cell_text(entry: dict[str, Any] | None) -> str:
    if entry is None:
        return "-"
    if "files_on_disk" in entry:
        return "on disk: " + ", ".join(f"`{name}`" for name in entry["files_on_disk"])
    err = entry.get("err") or entry.get("rows_err")
    if err is not None:
        cond = err.get("cond") or ""
        first = err["first"]
        for marker in ("/tmp/", "file:///tmp/"):
            if marker in first:
                first = first.split(marker)[0] + "…"
        label = cond if cond else first[:60]
        return f"refuses: {err['type']} {label}".strip()
    rows = entry.get("rows") or []
    ids = ", ".join(str(row[0]) if len(row) == 1 else str(tuple(row)) for row in rows)
    files = ", ".join(entry.get("files", []))
    return f"ids [{ids}]; n={entry.get('count')}; files [{files}]; {entry.get('schema', '')}"


def ids_of(entry: dict[str, Any] | None) -> str:
    if entry is None:
        return "-"
    err = entry.get("err") or entry.get("rows_err")
    if err is not None:
        return f"refuses: {err['type']}"
    rows = entry.get("rows") or []
    return "ids [" + ", ".join(str(row[0]) for row in rows) + f"]; n={entry.get('count')}"


def same(left: dict[str, Any] | None, right: dict[str, Any] | None) -> bool:
    if left is None or right is None:
        return left is right
    left_err = "err" in left or "rows_err" in left
    right_err = "err" in right or "rows_err" in right
    if left_err or right_err:
        return left_err and right_err
    if "files_on_disk" in left or "files_on_disk" in right:
        return left.get("files_on_disk") == right.get("files_on_disk")
    return (
        left.get("rows") == right.get("rows")
        and left.get("count") == right.get("count")
        and left.get("files") == right.get("files")
    )


def spark_entry(spark: dict[str, Any], key: str) -> dict[str, Any] | None:
    if key.endswith("|native"):
        return spark.get(key.split("|")[0] + "|reader")
    return spark.get(key)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", required=True)
    parser.add_argument("--spark-cross", required=True)
    parser.add_argument("--main", required=True)
    parser.add_argument("--head", default="")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    spark = json.loads(Path(args.spark).read_text())["modes"]
    for mode, cells in json.loads(Path(args.spark_cross).read_text())["modes"].items():
        for key, value in cells.items():
            spark[mode][f"spark-reads-repark/{key}"] = value
    main_ = json.loads(Path(args.main).read_text())["modes"]
    head = json.loads(Path(args.head).read_text())["modes"] if args.head else None
    lines = []
    header = "| cell | door | ANSI | Spark 4.1.2 | main | "
    header += "head | moved |" if head else "main = Spark |"
    lines.append(header)
    lines.append("|---|---|---|---|---|---|---|" if head else "|---|---|---|---|---|---|")
    counts = {"fixed": 0, "off": 0, "same": 0, "unlike": 0}
    unwired: dict[str, int] = {}
    unwired_moved: list[str] = []
    for mode in ("false", "true"):
        keys = sorted(set(spark[mode]) | set(main_[mode]))
        for key in keys:
            if key.startswith(("spark-reads-repark/", "cross/", "write/")):
                continue
            cell, _, door = key.partition("|")
            s_entry = spark_entry(spark[mode], key)
            m_entry = main_[mode].get(key)
            if door in UNWIRED:
                h_refuses = head is None or "err" in (head[mode].get(key) or {})
                unwired[door] = unwired.get(door, 0) + 1
                if "err" not in (m_entry or {}) or not h_refuses:
                    unwired_moved.append(f"{key} ({mode})")
                continue
            door_text = DOOR_TEXT.get(door, door or "-")
            m_same = same(s_entry, m_entry)
            row = (
                f"| `{cell}` | {door_text} | {mode} | {cell_text(s_entry)} | {cell_text(m_entry)} |"
            )
            if head:
                h_entry = head[mode].get(key)
                h_same = same(s_entry, h_entry)
                if m_same and h_same:
                    moved = "no (= Spark)"
                    counts["same"] += 1
                elif not m_same and h_same:
                    moved = "**fixed**"
                    counts["fixed"] += 1
                elif m_same and not h_same:
                    moved = "**OFF SPARK**"
                    counts["off"] += 1
                elif same(m_entry, h_entry):
                    moved = "no (unlike Spark)"
                    counts["unlike"] += 1
                else:
                    moved = "moved, still unlike Spark"
                    counts["unlike"] += 1
                row += f" {cell_text(h_entry)} | {moved} |"
            else:
                row += f" {'yes' if m_same else '**no**'} |"
            lines.append(row)
    lines.append("")
    for door, count in sorted(unwired.items()):
        lines.append(
            f"- {DOOR_TEXT[door]}: refuses on main"
            + (" and on head" if head else "")
            + f" in all {count} cells (both modes); Spark answers as `spark.read.parquet`."
        )
    if unwired_moved:
        lines.append("- unwired cells that did not refuse: " + ", ".join(unwired_moved))
    lines.append("")
    lines.append("Writes and round trips (ids only; Spark's own part names differ per run):")
    lines.append("")
    lines.append("| case | ANSI | Spark | main | head |")
    lines.append("|---|---|---|---|---|")
    cases = [
        ("append beside `b`, read by the same engine", "write/append/self", "write/append/self"),
        (
            "overwrite beside `b`, read by the same engine",
            "write/overwrite/self",
            "write/overwrite/self",
        ),
        ("Spark's append dir read by RePark", "write/append/self", "cross/write-append"),
        ("Spark's overwrite dir read by RePark", "write/overwrite/self", "cross/write-overwrite"),
        ("RePark's append dir read by Spark", "spark-reads-repark/cross/write-append", ""),
        ("RePark's overwrite dir read by Spark", "spark-reads-repark/cross/write-overwrite", ""),
        ("Spark-written flat dir (markers) read by RePark", "", "cross/written-flat"),
        ("Spark-written partitioned dir read by RePark", "", "cross/written-partitioned"),
    ]
    for mode in ("false", "true"):
        for label, s_key, r_key in cases:
            s_text = ids_of(spark[mode].get(s_key)) if s_key else "ids [1, 2, 3] written"
            m_text = ids_of(main_[mode].get(r_key)) if r_key else "-"
            h_text = ids_of(head[mode].get(r_key)) if head and r_key else "-"
            lines.append(f"| {label} | {mode} | {s_text} | {m_text} | {h_text} |")
    if head:
        lines.append("")
        lines.append(
            f"Totals: fixed {counts['fixed']}, moved off Spark {counts['off']}, "
            f"equal on both {counts['same']}, unlike Spark on head {counts['unlike']}."
        )
    Path(args.out).write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
