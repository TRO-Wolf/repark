from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
import analyze as an

CODE = {
    "EQUAL": "=",
    "WRONG": "W",
    "ZERO": "z",
    "R-ERR": "E",
    "S-ERR": "S",
    "BOTH-ERR": "x",
    "NA": ".",
}
B = an.BUILDS


def code(entry: dict[str, Any], prefix: str = "v_") -> str:
    return "".join(CODE[entry.get(f"{prefix}{b}", "NA")] for b in B)


def short(value: Any, width: int = 60) -> str:
    text = json.dumps(value, separators=(",", ":")) if not isinstance(value, str) else value
    text = text.replace("|", "/")
    return text if len(text) <= width else text[: width - 1] + "…"


def table(head: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return out


def totals(entries: list[dict[str, Any]]) -> list[str]:
    tally: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for e in entries:
        for b in B:
            tally[(b, e["ansi"])][e[f"v_{b}"]] += 1
    head = ["build", "ANSI", "cells", *an_order()]
    rows = []
    for (b, ansi), counter in sorted(
        tally.items(), key=lambda kv: (kv[0][1] != "true", B.index(kv[0][0]))
    ):
        rows.append([b, ansi, sum(counter.values()), *[counter.get(k, 0) for k in an_order()]])
    return table(head, rows)


def an_order() -> list[str]:
    return ["EQUAL", "WRONG", "ZERO", "R-ERR", "S-ERR", "BOTH-ERR", "NA"]


def shape_matrix(entries: list[dict[str, Any]], door: str) -> list[str]:
    pick = [
        e
        for e in entries
        if e["ansi"] == "true" and e["door"] == door and e["src"] in ("created", "computed")
    ]
    columns = [
        (s, c)
        for s in ("created", "computed")
        for c in ("dc", "ds", "dp", "dq", "fc", "fs", "fp", "fq")
    ]
    grid: dict[str, dict[tuple[str, str], str]] = defaultdict(dict)
    order: list[str] = []
    for e in pick:
        if e["col"] == "mix":
            continue
        if e["op"] not in order:
            order.append(e["op"])
        grid[e["op"]][(e["src"], e["col"])] = code(e)
    labels = [an.shape_of(s, c).replace(" DOUBLE", "").replace(" FLOAT", " F") for s, c in columns]
    head = [
        "op",
        *[
            f"{'mem' if s == 'created' else 'sql'} {label}"
            for (s, _), label in zip(columns, labels, strict=True)
        ],
    ]
    fam_order = {op: i for i, op in enumerate(op for ops in an.FAMILIES.values() for op in ops)}
    rows = []
    for op in sorted(order, key=lambda o: fam_order.get(o, 999)):
        rows.append([op, *[grid[op].get(col, "") for col in columns]])
    return table(head, rows)


def key_answers(entries: list[dict[str, Any]]) -> list[str]:
    wanted = [
        "agg_max",
        "agg_min",
        "grp_max",
        "grp_min",
        "greatest",
        "least",
        "ord_asc",
        "ord_desc",
        "f_eq_nan",
        "f_gt_inf",
        "f_lt_zero",
        "arr_sort",
        "cnt_distinct",
    ]
    rows = []
    for e in entries:
        if (
            e["ansi"] == "true"
            and e["door"] == "sql"
            and e["src"] == "created"
            and e["col"] in ("dc", "ds")
            and e["op"] in wanted
        ):
            rows.append(
                [
                    e["op"],
                    an.shape_of(e["src"], e["col"]),
                    short(e["spark"], 46),
                    short(e.get("a_base"), 46),
                    short(e.get("a_1325"), 30),
                    short(e.get("a_head3"), 46),
                ]
            )
    rows.sort(key=lambda r: (wanted.index(r[0]), r[1]))
    return table(["op", "shape", "Spark", "base", "1325", "head3"], rows)


def door_matrix(entries: list[dict[str, Any]]) -> list[str]:
    tally: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0, 0])
    for e in entries:
        if e["ansi"] != "true":
            continue
        key = (e["family"], e["door"])
        tally[key][0] += 1
        for i, b in enumerate(B, start=1):
            tally[key][i] += e[f"v_{b}"] in ("WRONG",)
    doors = ["sql", "frame", "fexpr", "native"]
    rows = []
    for fam in an.FAMILIES:
        row = [fam]
        for door in doors:
            n, *w = tally.get((fam, door), [0, 0, 0, 0])
            row.append(f"{n}: {'/'.join(str(x) for x in w)}" if n else "")
        rows.append(row)
    return table(["family", *[f"{d} (cells: wrong base/1325/head3)" for d in doors]], rows)


def storage_matrix(entries: list[dict[str, Any]]) -> list[str]:
    tally: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0, 0])
    bits: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for e in entries:
        if e["ansi"] != "true" or e["door"] not in ("sql", "native") or e["col"] in ("mix",):
            continue
        if e["src"] == "nat_parquet_l5" and e["door"] != "native":
            continue
        if e["src"] != "nat_parquet_l5" and e["door"] != "sql":
            continue
        key = (e["src"], e["col"])
        tally[key][0] += 1
        for i, b in enumerate(B, start=1):
            tally[key][i] += e[f"v_{b}"] == "WRONG"
        if e["op"] == "arr_sort":
            for b in B:
                got = e.get(f"bits_{b}")
                bits[key][b] = ",".join(
                    x[:4] + "…" + x[-4:] if len(x) > 8 else x for x in got or []
                )
            got = e.get("spark_bits") or []
            bits[key]["spark"] = ",".join(x[:4] + "…" + x[-4:] if len(x) > 8 else x for x in got)
    rows = []
    for (src, col), (n, *w) in sorted(tally.items()):
        if col[0] not in ("d", "p", "v"):
            continue
        rows.append(
            [
                src,
                col,
                n,
                "/".join(str(x) for x in w),
                bits[(src, col)].get("spark", ""),
                bits[(src, col)].get("base", ""),
                bits[(src, col)].get("1325", ""),
                bits[(src, col)].get("head3", ""),
            ]
        )
    return table(
        [
            "source",
            "column",
            "cells",
            "wrong base/1325/head3",
            "NaN bits Spark reads",
            "base reads",
            "1325 reads",
            "head3 reads",
        ],
        rows,
    )


def pruning(entries: list[dict[str, Any]]) -> list[str]:
    ops = ("f_gt_half", "f_gt_inf", "f_eq_nan", "f_in_nan", "agg_max")
    rows = []
    for e in entries:
        if e["ansi"] != "true" or e["op"] not in ops:
            continue
        if not (e["src"].endswith(("_l5", "_part")) or e["src"].startswith("pq_")):
            continue
        if e["col"] not in ("dc", "ds", "pk"):
            continue
        if e["door"] != ("native" if e["src"] == "nat_parquet_l5" else "sql"):
            continue
        plan = e.get("plan_base") or []
        scan = ""
        for line in plan:
            if "PlaceholderRowExec" in line:
                scan = "stats"
            elif "N=" in line:
                scan = "N=" + line.split("N=")[1].split(",")[0]
            elif "PlaceholderRowExec" in line:
                scan = "stats"
            elif "files_ranges_pruned_statistics" in line or "row_groups_pruned_statistics" in line:
                part = [p for p in line.split(", ") if "pruned_statistics" in p]
                scan = ";".join(
                    p.split("=")[0].replace("_pruned_statistics", "") + "=" + p.split("=")[1][:24]
                    for p in part[:2]
                )
        rows.append(
            [
                e["src"],
                e["col"],
                e["op"],
                short(e.get("spark"), 26),
                short(e.get("spark_semantic"), 26),
                short(e.get("a_base"), 26),
                short(e.get("a_1325"), 26),
                short(e.get("a_head3"), 26),
                scan[:40],
            ]
        )
    rows.sort()
    return table(
        [
            "source",
            "col",
            "op",
            "Spark on the source",
            "Spark in memory",
            "base",
            "1325",
            "head3",
            "base scan",
        ],
        rows,
    )


def producers(path: Path) -> list[str]:
    grid: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for line in path.open():
        p = json.loads(line)
        key = (p["op"], p["form"])
        tag = f"{p['engine']}/{p['door']}/{'on' if p['ansi'] == 'true' else 'off'}"
        if not p["ok"]:
            value = "ERR " + (p.get("error") or "")[:28]
        elif p["form"] == "jvm_raw":
            value = ",".join(str(r[0])[:4] for r in p["rows"])
        else:
            value = ",".join(str(r[0]) for r in p["rows"]) + (
                " " + ",".join(b[:4] for b in p["nan_bits"]) if p["nan_bits"] else ""
            )
        grid[key][tag] = value
    tags = [
        f"{e}/{d}/{a}"
        for e in ("spark", "base", "1325", "head3")
        for d in ("sql", "native")
        for a in ("on", "off")
        if not (e == "spark" and d == "native")
    ]
    rows = [[op, form, *[grid[(op, form)].get(t, "") for t in tags]] for (op, form) in sorted(grid)]
    return table(["producer", "form", *tags], rows)


def ansi_delta(entries: list[dict[str, Any]]) -> list[str]:
    by_key = defaultdict(dict)
    for e in entries:
        by_key[(e["door"], e["src"], e["col"], e["op"])][e["ansi"]] = e
    counts = Counter()
    rows = []
    for key, pair in sorted(by_key.items()):
        if "false" not in pair or "true" not in pair:
            continue
        counts["pairs"] += 1
        for b in B:
            if pair["true"][f"v_{b}"] != pair["false"][f"v_{b}"]:
                counts[f"verdict moved {b}"] += 1
                if len(rows) < 12:
                    rows.append([*key, b, pair["true"][f"v_{b}"], pair["false"][f"v_{b}"]])
    return [
        f"Cells measured in both modes: {counts['pairs']}; verdicts that move with ANSI: "
        + ", ".join(f"{b} {counts[f'verdict moved {b}']}" for b in B)
        + "."
    ] + (
        ["", *table(["door", "source", "col", "op", "build", "ANSI on", "ANSI off"], rows)]
        if rows
        else []
    )


def main() -> int:
    compare = Path(sys.argv[1])
    out = Path(sys.argv[2])
    entries = [json.loads(line) for line in compare.open()]
    sections = {
        "totals": totals(entries),
        "shape-sql": shape_matrix(entries, "sql"),
        "shape-native": shape_matrix(entries, "native"),
        "key-answers": key_answers(entries),
        "doors": door_matrix(entries),
        "storage": storage_matrix(entries),
        "pruning": pruning(entries),
        "producers": producers(compare.with_name("producers.jsonl")),
        "ansi": ansi_delta(entries),
    }
    with out.open("w") as sink:
        for name, lines in sections.items():
            sink.write(f"<!-- section {name} -->\n" + "\n".join(lines) + "\n\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
