from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import probe_step0 as probe


def short(cell: dict[str, Any] | None) -> str:
    if cell is None:
        return "-"
    if cell["kind"] == "error":
        if cell["class"] == "OTHER":
            return "ERR " + cell["first"][:70].replace("|", "/")
        return f"ERR [{cell['class']}]"
    rows = json.dumps(cell["rows"]).replace("|", "/")
    return rows if len(rows) <= 70 else rows[:67] + "..."


def verdict(spark: dict[str, Any] | None, other: dict[str, Any] | None) -> str:
    if spark is None or other is None:
        return "n/a"
    if spark["kind"] == "ok" and other["kind"] == "ok":
        return "same" if spark["rows"] == other["rows"] else "WRONG"
    if spark["kind"] == "error" and other["kind"] == "error":
        return "same" if spark["class"] == other["class"] else "refuse"
    if spark["kind"] == "error":
        return "SILENT"
    return "OVER-RAISE"


def write_verdict(spark: list[dict[str, Any]] | None, other: list[dict[str, Any]] | None) -> str:
    if not spark or not other:
        return "n/a"
    return verdict(spark[-1], other[-1])


TABLES = {
    "insert_into": "ins",
    "insert_overwrite": "ow",
    "ctas": "ctas",
    "update": "upd",
    "merge": "mrg",
}


def flatten_writes(payload: dict[str, Any], readback: dict[str, Any] | None) -> dict[str, Any]:
    out = {}
    for key, steps in payload.get("writes", {}).items():
        last = steps[-1]
        kind, label = key.split("/")
        state = (readback or {}).get(f"{TABLES[kind]}_{label}")
        if last["kind"] == "error":
            out[key] = last
        else:
            out[key] = state or {"kind": "ok", "rows": ["<no readback>"]}
        if state is not None:
            out[key + "#readback"] = state
    return out


def describe(key: str) -> tuple[str, str]:
    base = key.split("#")[0]
    parts = base.split("/")
    head = parts[0]
    if head == "kernel":
        column, target = parts[2].split("->")
        precision, scale = target.split(",")
        ident = parts[3][2:]
        cast = f"DECIMAL({precision},{scale})"
        if parts[1] == "lit":
            row = next(r for r in probe.ROWS if str(r[0]) == ident)
            value = row[probe.COLUMNS.index(column)]
            literal = probe.literal_text(value, probe.SOURCE_TYPES[column])
            return "spark.sql literal", f"SELECT CAST({literal} AS {cast})"
        return "spark.sql column", f"SELECT CAST({column} AS {cast}) FROM t WHERE id = {ident}"
    if head == "place":
        if parts[2] in probe.PLACEMENTS:
            return "spark.sql", probe.PLACEMENTS[parts[2]][0].format(t=parts[1])
        extra = {
            "tiny_scale_eq": "SELECT id FROM t WHERE CAST(ti AS DECIMAL(3,1)) = 1",
            "int_digit_lt": "SELECT id FROM t WHERE CAST(ii AS DECIMAL(9,0)) < 0",
            "small_digit_in": "SELECT id FROM t WHERE CAST(si AS DECIMAL(4,0)) IN (1, 123)",
        }
        return "spark.sql", extra[parts[2]]
    if head == "card":
        return "spark.sql", (
            "WHERE CAST(b AS DECIMAL(5,0)) = 1"
            if parts[1] == "filter"
            else "SELECT CAST(b AS DECIMAL(5,0))"
        ) + " over (SELECT CAST(123456 AS BIGINT) AS b)"
    if head == "door":
        view, name, door = parts[1], parts[2], parts[3]
        if name == "projection":
            return door, f"{probe.CAST} projected over {view}"
        return door, f"filter {probe.DOOR_FILTERS[name][0]} over {view}"
    if head == "neighbour":
        expr = probe.NEIGHBOURS[parts[1]]
        if parts[2].startswith("id"):
            return "spark.sql", f"SELECT {expr} FROM t WHERE id = {parts[2][2:]}"
        op = "=" if parts[2] == "where_eq" else "<"
        return "spark.sql", f"SELECT id FROM t WHERE {expr} {op} 1"
    if head == "conj":
        expr = probe.CONJUNCTION_CASTS[parts[1]]
        shapes = {
            "view_eq": f"SELECT id FROM t_fit WHERE {expr} = 1",
            "and_eq": f"SELECT id FROM t WHERE id IN (1, 5, 7) AND {expr} = 1",
            "and_one_row": f"SELECT id FROM t WHERE id = 1 AND {expr} = 1",
        }
        return "spark.sql", shapes[parts[2]]
    if head == "null":
        return "spark.sql", "nullability of a cast over a NOT NULL source"
    if head == "pushdown":
        if parts[1] == "accepted":
            return "spark.conf.set", f"{probe.PUSHDOWN_KEY} = true (RePark only)"
        return "spark.sql on pq, pushdown_filters on", probe.PRUNE_STATEMENTS[parts[2]].format(
            t="pq"
        )
    if head == "prune":
        return f"spark.sql on {parts[1]}", probe.PRUNE_STATEMENTS[parts[2]].format(t=parts[1])
    readback = " (Spark readback)" if "#" in key else ""
    return (
        "spark.sql write",
        f"{parts[0]} from {probe.WRITE_SELECT}{probe.WRITE_SOURCES[parts[1]]}{readback}",
    )


def load(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--head", default="head")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    lines = [
        "| cell | door | statement | ANSI | Spark 4.1.2 | main | main vs Spark | head "
        "| head vs Spark | moved |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    totals: dict[str, int] = {}
    moved: list[str] = []
    for ansi in ("true", "false"):
        spark = load(args.dir / f"spark-{ansi}.json")
        main_run = load(args.dir / f"main-{ansi}.json")
        head_run = load(args.dir / f"{args.head}-{ansi}.json")
        if spark is None or main_run is None:
            continue
        spark_rb = load(args.dir / f"readback-spark-{ansi}.json")
        main_rb = load(args.dir / f"readback-main-{ansi}.json")
        head_rb = load(args.dir / f"readback-{args.head}-{ansi}.json")
        spark_cells = {**spark["cells"], **flatten_writes(spark, spark_rb)}
        main_cells = {**main_run["cells"], **flatten_writes(main_run, main_rb)}
        head_cells = {**head_run["cells"], **flatten_writes(head_run, head_rb)} if head_run else {}
        for key in sorted(spark_cells):
            s_cell = spark_cells.get(key)
            m_cell = main_cells.get(key)
            h_cell = head_cells.get(key)
            mv = verdict(s_cell, m_cell)
            hv = verdict(s_cell, h_cell) if head_run else "n/a"
            mark = ""
            if (
                head_run
                and m_cell is not None
                and h_cell is not None
                and short(m_cell) != short(h_cell)
            ):
                mark = f"{mv}->{hv}"
                moved.append(f"{key} ANSI {ansi}: {mv} -> {hv}")
            totals[f"{ansi}/main/{mv}"] = totals.get(f"{ansi}/main/{mv}", 0) + 1
            totals[f"{ansi}/head/{hv}"] = totals.get(f"{ansi}/head/{hv}", 0) + 1
            door, statement = describe(key)
            statement = statement.replace("|", "/")
            lines.append(
                f"| `{key}` | {door} | `{statement}` | {ansi} | {short(s_cell)} | "
                f"{short(m_cell)} | {mv} | {short(h_cell)} | {hv} | {mark} |"
            )
    summary = ["", "## Totals", ""]
    summary += [f"- {key}: {value}" for key, value in sorted(totals.items())]
    summary += ["", "## Moved", ""] + [f"- {item}" for item in moved]
    args.out.write_text("\n".join(lines + summary) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
