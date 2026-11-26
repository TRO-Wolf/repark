from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def answer(entry: dict[str, Any]) -> tuple[str, str]:
    write = "ok" if entry["write"] == "ok" else "refuses"
    read = entry["read"]
    read_text = "refuses" if isinstance(read, str) else "[" + ", ".join(read) + "]"
    return write, read_text


def text(entry: dict[str, Any]) -> str:
    write, read = answer(entry)
    detail = "" if entry["write"] == "ok" else f" ({entry['write'].split(': ')[1]})"
    return f"write {write}{detail}; read {read}"


def verdict(spark: Any, main: Any, head: Any) -> str:
    m_same = answer(spark) == answer(main)
    h_same = answer(spark) == answer(head)
    if m_same and h_same:
        return "no (= Spark)"
    if h_same:
        return "**fixed**"
    if m_same:
        return "**OFF SPARK**"
    if answer(main) == answer(head):
        return "no (unlike Spark)"
    return "moved, still unlike Spark"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", required=True)
    parser.add_argument("--main", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    spark = json.loads(Path(args.spark).read_text())["modes"]
    main_ = json.loads(Path(args.main).read_text())["modes"]
    head = json.loads(Path(args.head).read_text())["modes"]
    lines = [
        "# FILE-EXTENSION-FILTER-1 local write paths beside hidden names",
        "",
        "Rendered by `render_write_hidden.py` from `probe_write_hidden.py` on Spark 4.1.2,",
        "main and head; each cell writes one row (`id` 20) into a fresh directory, then",
        "reads it back with the same engine.",
        "",
        "| cell | ANSI | Spark 4.1.2 | main | head | moved |",
        "|---|---|---|---|---|---|",
    ]
    for mode in ("false", "true"):
        for key in spark[mode]:
            s_entry, m_entry, h_entry = spark[mode][key], main_[mode][key], head[mode][key]
            lines.append(
                f"| `{key}` | {mode} | {text(s_entry)} | {text(m_entry)} | {text(h_entry)} | "
                f"{verdict(s_entry, m_entry, h_entry)} |"
            )
    Path(args.out).write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
