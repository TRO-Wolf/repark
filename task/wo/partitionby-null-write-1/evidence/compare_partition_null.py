from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_text(result: dict[str, Any]) -> str:
    write = result.get("write", {})
    if write.get("ok"):
        return "ok"
    return "ERR " + write.get("first", "")[:90]


def dirs_of(result: dict[str, Any]) -> list[str]:
    listing = result.get("listing", {})
    if not listing.get("exists"):
        return []
    return listing.get("dirs", [])


def read_key(read: dict[str, Any] | None) -> Any:
    if not read:
        return None
    if not read.get("ok"):
        return ["ERR", read.get("first", "")[:120]]
    return [read.get("schema"), read.get("rows")]


def cell_row(
    key: str, spark: dict[str, Any], engine: dict[str, Any], cross: dict[str, Any] | None
) -> dict[str, Any]:
    spark_dirs = dirs_of(spark)
    engine_dirs = dirs_of(engine)
    spark_ok = bool(spark.get("write", {}).get("ok"))
    engine_ok = bool(engine.get("write", {}).get("ok"))
    spark_own = read_key(spark.get("read_own"))
    spark_reads_engine = read_key(cross)
    engine_own = read_key(engine.get("read_own"))
    engine_reads_spark = read_key(engine.get("read_spark_dir"))
    if spark_ok and engine_ok:
        same_dirs = spark_dirs == engine_dirs
        same_spark_read = spark_own == spark_reads_engine
        verdict = "EQUAL" if same_dirs and same_spark_read else "DIFF"
    elif not spark_ok and not engine_ok:
        verdict = "BOTH-REFUSE"
    elif spark_ok:
        verdict = "ENGINE-REFUSES"
    else:
        verdict = "ENGINE-ACCEPTS-SPARK-REFUSES"
    return {
        "key": key,
        "verdict": verdict,
        "spark_write": write_text(spark),
        "engine_write": write_text(engine),
        "spark_dirs": spark_dirs,
        "engine_dirs": engine_dirs,
        "dirs_equal": spark_dirs == engine_dirs,
        "spark_reads_engine_equals_spark_own": spark_own == spark_reads_engine,
        "engine_own_read": engine_own,
        "engine_reads_spark_dir": engine_reads_spark,
        "spark_own_read": spark_own,
        "spark_reads_engine": spark_reads_engine,
        "engine_contents": engine.get("contents"),
    }


def build(
    spark: dict[str, Any], engine: dict[str, Any], cross: dict[str, Any], label: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ansi in ("false", "true"):
        spark_cells = spark["modes"][ansi]
        engine_cells = engine["modes"][ansi]
        cross_cells = cross.get("cross", {}).get(label, {}).get(ansi, {})
        for key in spark_cells:
            row = cell_row(key, spark_cells[key], engine_cells.get(key, {}), cross_cells.get(key))
            row["ansi"] = ansi
            rows.append(row)
    return rows


def short(items: list[str], limit: int = 140) -> str:
    text = ", ".join(items)
    if len(text) > limit:
        return text[:limit] + f" … ({len(items)} dirs)"
    return text


def markdown(rows: list[dict[str, Any]], label: str) -> str:
    lines = [
        f"| ANSI | Cell (type/format/door/mode) | Verdict vs Spark | Spark dirs | {label} dirs |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        spark_dirs = short(row["spark_dirs"]) if row["spark_write"] == "ok" else row["spark_write"]
        engine_dirs = (
            short(row["engine_dirs"]) if row["engine_write"] == "ok" else row["engine_write"]
        )
        lines.append(
            f"| {row['ansi']} | `{row['key']}` | {row['verdict']} | "
            f"{spark_dirs.replace('|', '¦')} | {engine_dirs.replace('|', '¦')} |"
        )
    return "\n".join(lines) + "\n"


HEADER_LINES = ("id",)
LOCAL_PATH = re.compile(r"(?:file:)?/(?:tmp|home)/\S*")


def error_class(row: dict[str, Any], side: str) -> str:
    text = row[f"{side}_write"]
    return text.split(" ", 2)[1] if text.startswith("ERR ") and " " in text[4:] else text


def flat_contents(contents: dict[str, Any] | None) -> Any:
    if not contents:
        return []
    schemas = sorted({json.dumps(entry.get("schema")) for entry in contents.values()})
    rows = sorted(
        json.dumps(row)
        for entry in contents.values()
        for row in entry.get("rows", [])
        if row not in HEADER_LINES
    )
    return [schemas, rows]


def without_headers(read: Any) -> Any:
    if not isinstance(read, list) or len(read) != 2 or read[0] == "ERR":
        return read
    return [read[0], [row for row in read[1] if row != list(HEADER_LINES)]]


def cell_text(row: dict[str, Any], side: str) -> str:
    if row[f"{side}_write"] != "ok":
        return LOCAL_PATH.sub("<dir>", row[f"{side}_write"]).replace("|", "¦")
    return ", ".join(row[f"{side}_dirs"]).replace("|", "¦") or "(none)"


def domain_rows(main_rows: list[dict[str, Any]], head_rows: list[dict[str, Any]]) -> list[Any]:
    out = []
    head_by = {(row["ansi"], row["key"]): row for row in head_rows}
    for main in main_rows:
        head = head_by[(main["ansi"], main["key"])]
        same_files = flat_contents(main["engine_contents"]) == flat_contents(
            head["engine_contents"]
        )
        same_reads = (
            without_headers(main["engine_own_read"]) == without_headers(head["engine_own_read"])
            and main["engine_reads_spark_dir"] == head["engine_reads_spark_dir"]
        )
        both_wrote = main["engine_write"] == "ok" and head["engine_write"] == "ok"
        moved = (
            main["verdict"] != head["verdict"]
            or (both_wrote and main["engine_dirs"] != head["engine_dirs"])
            or (not both_wrote and error_class(main, "engine") != error_class(head, "engine"))
        )
        out.append(
            {
                "ansi": main["ansi"],
                "key": main["key"],
                "spark": cell_text(main, "spark"),
                "main": cell_text(main, "engine"),
                "head": cell_text(head, "engine"),
                "main_verdict": main["verdict"],
                "head_verdict": head["verdict"],
                "moved": moved,
                "same_files": same_files,
                "same_repark_reads": same_reads,
            }
        )
    return out


def domain_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| ANSI | Cell (type/format/door/mode) | Spark dirs | main dirs | head dirs | main | head "
        "| moved | rows same | RePark reads same |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['ansi']} | `{row['key']}` | {row['spark']} | {row['main']} | {row['head']} "
            f"| {row['main_verdict']} | {row['head_verdict']} | {'yes' if row['moved'] else 'no'} "
            f"| {'yes' if row['same_files'] else 'NO'} "
            f"| {'yes' if row['same_repark_reads'] else 'NO'} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", type=Path, required=True)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--cross", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--out-json", type=Path, required=True)
    parser.add_argument("--out-md", type=Path, required=True)
    parser.add_argument("--main-table", type=Path)
    parser.add_argument("--domain-md", type=Path)
    args = parser.parse_args()
    rows = build(load(args.spark), load(args.engine), load(args.cross), args.label)
    args.out_json.write_text(json.dumps(rows, indent=1, ensure_ascii=False))
    args.out_md.write_text(markdown(rows, args.label))
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
    print(json.dumps(counts, sort_keys=True))
    if args.main_table and args.domain_md:
        domain = domain_rows(json.loads(args.main_table.read_text()), rows)
        args.domain_md.write_text(domain_markdown(domain))
        moved = [row for row in domain if row["moved"]]
        print(
            json.dumps(
                {
                    "cells": len(domain),
                    "moved": len(moved),
                    "moved_off_spark": sum(
                        1 for row in moved if row["main_verdict"] in {"EQUAL", "BOTH-REFUSE"}
                    ),
                    "moved_to_equal": sum(1 for row in moved if row["head_verdict"] == "EQUAL"),
                    "files_differ": sum(1 for row in domain if not row["same_files"]),
                    "reads_differ": sum(1 for row in domain if not row["same_repark_reads"]),
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
