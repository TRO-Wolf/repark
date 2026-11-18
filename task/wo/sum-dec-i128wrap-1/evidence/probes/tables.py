import json
import sys
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path

with Path(sys.argv[1]).open() as fh:
    rows = [json.loads(line) for line in fh]
SILENT = ("SPARK_RAISES", "VALUE")
LOUD = ("REPARK_RAISES", "BOTH_RAISE_OTHER")
KINDS = [
    "EQUAL",
    "BOTH_RAISE",
    "SPARK_RAISES",
    "VALUE",
    "REPARK_RAISES",
    "BOTH_RAISE_OTHER",
    "TYPE",
    "FORMAT",
    "READ_ERR",
]


def bucket(r: dict) -> str:
    pos = r["pos"] or ""
    if r["door"] == "iceberg":
        return "iceberg"
    if r["door"] == "dataframe":
        return "dataframe"
    if pos.startswith(("win", "try_win")):
        return "window"
    if "distinct" in pos:
        return "distinct"
    if pos in (
        "beside",
        "having",
        "having_all",
        "plus1",
        "as_string",
        "linear",
        "cte",
        "scalar_sub",
        "temp_view",
    ):
        return "expression"
    return "aggregate"


def family(r: dict) -> str:
    return r["family"]


def table(title: str, keyf: Callable, filt: Callable) -> None:
    grid = defaultdict(Counter)
    for r in rows:
        if filt(r):
            grid[keyf(r)][r["verdict"]] += 1
    print(f"### {title}\n")
    print("| key | cells | " + " | ".join(KINDS) + " | silent | loud other |")
    print("|" + "---|" * (len(KINDS) + 4))
    for k in sorted(grid, key=str):
        c = grid[k]
        silent = sum(c[x] for x in SILENT)
        loud = sum(c[x] for x in LOUD)
        print(
            f"| {k} | {sum(c.values())} | "
            + " | ".join(str(c[x]) for x in KINDS)
            + f" | {silent} | {loud} |"
        )
    print()


def tags(title: str, filt: Callable) -> None:
    grid = defaultdict(Counter)
    for r in rows:
        if filt(r) and r["verdict"] in SILENT:
            for t in r["tags"] or ["untagged"]:
                grid[(r["ansi"], bucket(r))][t] += 1
    print(f"### {title}\n")
    for k in sorted(grid):
        print(f"- {k[0]} / {k[1]}: " + ", ".join(f"{t} {n}" for t, n in sorted(grid[k].items())))
    print()


def prec(r: dict) -> str:
    c = r["col"] or ""
    return c.split("_")[0] if c.startswith("c") else c


table("Totals by run, ANSI and family", lambda r: (r["run"], r["ansi"], family(r)), lambda r: True)
table(
    "Spark door, decimal sum and try_sum, by position bucket",
    lambda r: (r["ansi"], family(r), bucket(r)),
    lambda r: r["run"] == "spark_door" and r["family"] in ("sum", "try_sum"),
)
table(
    "Spark door, decimal sum, by input precision",
    lambda r: (r["ansi"], prec(r)),
    lambda r: r["run"] == "spark_door" and r["family"] == "sum",
)
table(
    "Native door, by family and bucket",
    lambda r: (r["ansi"], family(r), bucket(r)),
    lambda r: r["run"] == "native",
)
tags(
    "Spark door, decimal sum: what RePark answered in the silent cells",
    lambda r: r["run"] == "spark_door" and r["family"] == "sum",
)
tags(
    "Native door, decimal sum: what RePark answered in the silent cells",
    lambda r: r["run"] == "native" and r["family"] == "sum",
)
