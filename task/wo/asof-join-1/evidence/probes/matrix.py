import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from cmp import load

ORDER = ("EQUAL", "TIE", "WRONG", "WRONG-NAMES", "WRONG-TYPE", "RAISE", "MISSING")


def answer(rec: dict[str, Any] | None) -> str:
    if rec is None:
        return "-"
    if "error" in rec:
        return rec["error"].split(" ")[0] + " " + " ".join(rec["error"].split(" ")[1:])[:70]
    return f"{len(rec['rows'])} rows {','.join(rec['types'][3:4] + rec['types'][8:9])}"


def table(title: str, head: list[str], rows: list[list[str]]) -> None:
    print("\n## " + title + "\n")
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for row in rows:
        print("| " + " | ".join(row) + " |")


def counts_line(counter: Counter) -> list[str]:
    return [str(counter.get(v, 0)) for v in ORDER]


def main() -> None:
    raw, verdicts = Path(sys.argv[1]), Path(sys.argv[2])
    data = load(raw)
    spark = data["spark"]
    rows = [json.loads(line) for line in verdicts.open()]
    by_pair: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    for r in rows:
        by_pair[(r["key"]["g"], r["key"]["ansi"], r["pair"])][r["v"]] += 1
    table(
        "Totals by group, mode and pair",
        ["group", "ansi", "pair", *ORDER],
        [[g, a, p, *counts_line(c)] for (g, a, p), c in sorted(by_pair.items())],
    )
    grid: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    for r in rows:
        k = r["key"]
        if k["g"] == "G" and r["pair"] in ("main-W1", "main-W2"):
            grid[(k["typ"], k["dir"], r["pair"])][r["v"]] += 1
    table(
        "Grid: main's workarounds against Spark's _joinAsOf, by type and direction (both modes)",
        ["type", "direction", "pair", *ORDER],
        [[t, d, p, *counts_line(c)] for (t, d, p), c in sorted(grid.items())],
    )
    sel: dict[str, list[list[str]]] = defaultdict(list)
    for (key, surf), rec in sorted(spark.items()):
        k = json.loads(key)
        if surf not in ("D", "SQL", "F.expr", "F", "DataFrame", "P"):
            continue
        g = k["g"]
        if g in ("E", "C", "S"):
            other = dict(k, ansi="false" if k["ansi"] == "true" else "true")
            if k["ansi"] != "true":
                continue
            off = spark.get((json.dumps(other, sort_keys=True), surf))
            main = data["main"].get((key, surf))
            sel[g].append([k["case"], answer(rec), answer(off), answer(main)])
        if g in ("M", "O", "N") and k["ansi"] == "true":
            other = dict(k, ansi="false")
            off = spark.get((json.dumps(other, sort_keys=True), surf))
            name = f"{k['typ']} {k['dir']} exact={k['exact']} tol={k['tol']}"
            sel[g].append([name, answer(rec), answer(off)])
    for g, title in (
        ("E", "Errors (BIGINT unless named; on=k1, how=left, backward unless named)"),
        ("C", "Call forms"),
        ("S", "SQL and helper availability"),
    ):
        table(title, ["case", "Spark ANSI on", "Spark ANSI off", "main ANSI on"], sel[g])
    for g, title in (
        ("M", "Mixed as-of types (left>right), on=k1, how=left"),
        ("O", "Overflow at the integer bounds, on=k1, how=left"),
        ("N", "DOUBLE NaN, -0.0 and infinities, on=k1, how=left"),
    ):
        table(title, ["cell", "Spark ANSI on", "Spark ANSI off"], sel[g])
    detail: dict[tuple[str, str], Counter] = defaultdict(Counter)
    for r in rows:
        k = r["key"]
        if k["g"] in ("M", "O", "N", "T", "P") and r["pair"] != "main-D":
            detail[(k["g"], r["pair"])][r["v"]] += 1
    table(
        "Non-grid verdicts by pair (both modes)",
        ["group", "pair", *ORDER],
        [[g, p, *counts_line(c)] for (g, p), c in sorted(detail.items())],
    )
    wrong = [
        r
        for r in rows
        if r["v"] in ("WRONG", "WRONG-NAMES", "WRONG-TYPE", "RAISE")
        and r["pair"] in ("main-W1", "main-W2", "spark-W1", "spark-W2", "spark-PD")
    ]
    print("\n## Every WRONG, WRONG-NAMES, WRONG-TYPE and RAISE cell\n")
    for r in wrong:
        k = {key: value for key, value in r["key"].items() if key not in ("g",)}
        print("-", r["key"]["g"], r["pair"], r["v"], json.dumps(k, sort_keys=True))


if __name__ == "__main__":
    main()
