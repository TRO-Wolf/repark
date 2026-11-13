from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

BUILDS = ("base", "1325", "head3")
SHAPE_NAMES = {
    "created": {"c": "canonical", "s": "sign-set", "p": "payload", "q": "sign-payload"},
    "computed": {"c": "CAST('NaN')", "s": "sqrt(-1)", "p": "inf - inf", "q": "-x"},
}
FAMILIES = {
    "agg": ("agg_max", "agg_min", "grp_max", "grp_min", "grp_max_one", "grp_min_one"),
    "window": ("win_max_all", "win_max_run", "win_min_run", "win_max_slide", "win_min_slide"),
    "greatest/least": ("greatest", "least"),
    "sort": (
        "ord_asc",
        "ord_desc",
        "ord_asc_nl",
        "ord_desc_nf",
        "topk_asc",
        "topk_desc",
        "win_rank",
    ),
    "compare-literal": (
        "f_eq_nan",
        "f_ne_nan",
        "f_lt_nan",
        "f_le_nan",
        "f_gt_nan",
        "f_ge_nan",
        "f_nseq_nan",
        "f_in_nan",
        "f_notin_nan",
        "f_between",
        "f_gt_inf",
        "f_ge_inf",
        "f_gt_half",
        "f_lt_zero",
        "f_le_neginf",
        "p_eq_nan",
        "p_gt_inf",
        "p_lt_nan",
        "case_gt",
        "case_eq",
    ),
    "compare-column": ("f_eq_col", "p_eq_col", "p_lt_col"),
    "control": ("f_isnan",),
    "array": ("arr_sort", "arr_min", "arr_max", "sort_arr_asc", "sort_arr_desc"),
    "keys": ("cnt_distinct", "approx_distinct", "group_by", "join_col"),
    "mix": ("mix_distinct", "mix_max", "mix_group", "mix_order", "mix_eq_nan"),
}
OP_FAMILY = {op: fam for fam, names in FAMILIES.items() for op in names}


def load(path: Path) -> dict[tuple[str, ...], dict[str, Any]]:
    out = {}
    if not path.exists():
        return out
    for line in path.open():
        record = json.loads(line)
        if record["record"] == "cell":
            out[(record["door"], record["src"], record["col"], record["op"])] = record
        elif record["record"] == "producer":
            out[("producer", record["door"], record["op"], record["form"])] = record
    return out


def zero_blind(rows: Any) -> str:
    return json.dumps(rows).replace('"-0.0"', '"0.0"')


def verdict(mine: dict[str, Any] | None, truth: dict[str, Any] | None) -> str:
    if mine is None or truth is None:
        return "NA"
    if mine["ok"] and truth["ok"]:
        if mine["rows"] == truth["rows"]:
            return "EQUAL"
        if zero_blind(mine["rows"]) == zero_blind(truth["rows"]):
            return "ZERO"
        return "WRONG"
    if not mine["ok"] and not truth["ok"]:
        return "BOTH-ERR"
    return "R-ERR" if not mine["ok"] else "S-ERR"


def shape_of(src: str, col: str) -> str:
    if col == "mix":
        return "all four"
    if col in ("pk", "v"):
        return "partition" if col == "pk" else "data"
    names = SHAPE_NAMES["computed"] if src == "computed" else SHAPE_NAMES["created"]
    kind = "DOUBLE" if col[0] == "d" else "FLOAT"
    return f"{names[col[1]]} {kind}"


def semantic_source(src: str) -> str | None:
    if src in ("created", "computed", "part_created"):
        return None
    return "part_created" if src.endswith("_part") else "created"


def main() -> int:
    runs = Path(sys.argv[1])
    out = Path(sys.argv[2])
    spark = {a: load(runs / f"cells-spark-ansi-{a}.jsonl") for a in ("true", "false")}
    for ansi in ("true",):
        spark[ansi].update(load(runs / f"extra-part-spark-ansi-{ansi}.jsonl"))
        spark[ansi].update(load(runs / f"extra-grp-spark-ansi-{ansi}.jsonl"))
    legs = {}
    for build in BUILDS:
        for ansi in ("true", "false"):
            data = load(runs / f"cells-{build}-ansi-{ansi}.jsonl")
            if ansi == "true":
                data.update(load(runs / f"extra-part-{build}-ansi-{ansi}.jsonl"))
                data.update(load(runs / f"extra-grp-{build}-ansi-{ansi}.jsonl"))
            legs[(build, ansi)] = data
    rows = []
    for ansi in ("true", "false"):
        keys = set()
        for build in BUILDS:
            keys |= {k for k in legs[(build, ansi)] if k[0] != "producer"}
        for key in sorted(keys):
            door, src, col, op = key
            truth_door = "sql" if door == "native" else door
            truth = spark[ansi].get((truth_door, src, col, op))
            if truth is None and door in ("frame", "fexpr"):
                truth = spark[ansi].get(("sql", src, col, op))
            sem_src = semantic_source(src)
            semantic = spark[ansi].get(("sql", sem_src, col, op)) if sem_src else None
            if src == "nat_parquet_l5":
                truth = spark[ansi].get(("sql", "created", col, op))
            entry = {
                "ansi": ansi,
                "door": door,
                "src": src,
                "col": col,
                "op": op,
                "family": OP_FAMILY.get(op, "other"),
                "shape": shape_of(src, col),
                "spark": None
                if truth is None
                else (truth["rows"] if truth["ok"] else "ERR " + truth["error"][:120]),
                "spark_semantic": None
                if semantic is None
                else (semantic["rows"] if semantic["ok"] else "ERR " + semantic["error"][:120]),
                "spark_bits": None if truth is None else truth.get("nan_bits"),
            }
            for build in BUILDS:
                mine = legs[(build, ansi)].get(key)
                entry[f"v_{build}"] = verdict(mine, truth)
                if semantic is not None:
                    entry[f"sem_{build}"] = verdict(mine, semantic)
                if mine is not None:
                    entry[f"a_{build}"] = (
                        mine["rows"] if mine["ok"] else "ERR " + mine["error"][:120]
                    )
                    entry[f"bits_{build}"] = mine.get("nan_bits")
                    if mine.get("plan"):
                        entry[f"plan_{build}"] = mine["plan"]
            if truth is not None and semantic is not None and truth["ok"] and semantic["ok"]:
                entry["spark_exact_vs_semantic"] = verdict(truth, semantic)
            rows.append(entry)
    producers = []
    for ansi in ("true", "false"):
        for key, record in sorted(spark[ansi].items()):
            if key[0] == "producer":
                producers.append(
                    {
                        "engine": "spark",
                        "ansi": ansi,
                        **{
                            k: record.get(k)
                            for k in ("door", "op", "form", "ok", "rows", "nan_bits", "error")
                        },
                    }
                )
        for build in BUILDS:
            for key, record in sorted(legs[(build, ansi)].items()):
                if key[0] == "producer":
                    producers.append(
                        {
                            "engine": build,
                            "ansi": ansi,
                            **{
                                k: record.get(k)
                                for k in ("door", "op", "form", "ok", "rows", "nan_bits", "error")
                            },
                        }
                    )
    with out.open("w") as sink:
        for entry in rows:
            sink.write(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
    with out.with_name("cells.tsv").open("w") as sink:
        sink.write("ansi\tdoor\tsrc\tcol\top\tspark\tbase\t1325\thead3\n")
        for entry in rows:
            spark_text = json.dumps(entry["spark"], separators=(",", ":"))
            cells = [
                entry["ansi"],
                entry["door"],
                entry["src"],
                entry["col"],
                entry["op"],
                spark_text,
            ]
            for build in BUILDS:
                mine = json.dumps(entry.get(f"a_{build}"), separators=(",", ":"))
                cells.append("=" if mine == spark_text else mine)
            sink.write("\t".join(cells) + "\n")
    with out.with_name("producers.jsonl").open("w") as sink:
        for entry in producers:
            sink.write(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
    tally: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for entry in rows:
        for build in BUILDS:
            tally[(build, entry["ansi"])][entry[f"v_{build}"]] += 1
    for key, counter in sorted(tally.items()):
        print(key, dict(counter))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
