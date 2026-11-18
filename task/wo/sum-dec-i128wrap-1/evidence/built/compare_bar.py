import json
import sys
from collections import Counter
from pathlib import Path

main_raw, head_raw, out = (Path(arg) for arg in sys.argv[1:4])
GOOD = ("EQUAL", "BOTH_RAISE")


def load(path: Path, revision: bool) -> dict:
    found = {}
    for record in map(json.loads, path.open()):
        run = "revision" if revision else record["run"]
        found[(run, record["ansi"], record["key"])] = record
    return found


def family(run: str, key: str, record: dict) -> str:
    if run == "native":
        return "native"
    if revision_key(key):
        return key.split("/")[0] + "/" + key.split("/")[1]
    return record.get("family") or key.split("/")[0]


def revision_key(key: str) -> bool:
    return key.startswith(("rev/", "obs/")) or (key.startswith("ice/") and "/metrics_" in key)


ORDER = ("sum/distinct/", "sum/distinct_all/", "df/repartition/", "try/s4/", "mp/try/s4/")
ORDER += ("sum/grouped_s4/g11/", "mp/sum/grouped_s4/g11/")
SPLIT = ("sum/ungrouped_s4/", "sum/ungrouped_p2/", "sum/grouped_p2/", "sum/grouped_s4/")


def named(run: str, key: str, record: dict) -> str:
    door = record.get("door") or ""
    spark = json.dumps(record.get("spark", ""))
    text = json.dumps(record.get("repark", ""))
    if run == "native":
        return "native door (NATIVE-DECIMAL-SUM-1, Q3)"
    if record.get("family") == "control" or key.startswith(("int/", "intdf/")):
        return "BIGINT ANSI overflow (SUM-BIGINT-ANSI-OVERFLOW-1, Q4)"
    if key.startswith("side/parquet_bigint_stats"):
        return "BIGINT Parquet statistics assertion (DECIMAL-CAST-SELECTIVITY-1)"
    if "sum_distinct" in key or "sum_distinct" in door or "sumDistinct" in key:
        return "F.sum_distinct missing (FNP-AGG-1)"
    if key.startswith(("rev/view_flip", "rev/dfview_flip", "rev/flip")):
        return "views across an ANSI flip (TEMP-VIEW-SESSION-SETTINGS-1)"
    if key.startswith("obs/"):
        return "seen beside (step 0 sketch section 8)"
    if record.get("family") == "avg" or key.startswith(("avg/", "mp/avg/", "sum/beside/")):
        return "avg (AVG-GROUPED-SUM-STICKY-1, ruling (a))"
    if record["verdict"] == "FORMAT":
        return "decimal-to-string notation (domain table section 10)"
    if "Arithmetic overflow" in text and key.startswith("sum/plus1"):
        return "decimal + at scale 38 (DECIMAL-MULTIPLY-OVERFLOW-1 family)"
    if key.startswith("ice/") and key.endswith("/readback") and "TABLE_OR_VIEW_NOT_FOUND" in spark:
        return "no table on both; RePark's not-found text lacks TABLE_OR_VIEW_NOT_FOUND"
    if (
        run == "revision"
        and "[NUMERIC_VALUE_OUT_OF_RANGE]" in text
        and "NUMERIC_VALUE_OUT_OF_RANGE" in spark
    ):
        return "same condition by message; the revision analyzer reads getCondition"
    if run == "revision" and record["verdict"] == "VALUE" and same_number(record):
        return "numerically equal; the revision analyzer compares text"
    if key.startswith("sum/scalar_sub/") and "Py4JJavaError" in spark:
        return "both raise; Spark's condition is wrapped by Py4J"
    if key.startswith(ORDER):
        return "order: a pass-back sum's partial or merge order differs from Spark's (Q2 (i))"
    if key.startswith(("mp/", *SPLIT)) or "_p2/" in key or "_s4/" in key:
        return "layout: RePark reads each file as a partial where Spark read one split (Q6)"
    return "unclassified"


def same_number(record: dict) -> bool:
    from decimal import Decimal

    try:
        rows = [record[side]["rows"] for side in ("spark", "repark")]
        flat = [[Decimal(cell) for row in side for cell in row] for side in rows]
    except (KeyError, TypeError, ArithmeticError, ValueError):
        return False
    return flat[0] == flat[1]


def main() -> None:
    before = load(main_raw / "verdicts.jsonl", False)
    before.update(load(main_raw / "rev-verdicts.jsonl", True))
    after = load(head_raw / "verdicts.jsonl", False)
    after.update(load(head_raw / "rev-verdicts.jsonl", True))
    transitions: Counter = Counter()
    per_family: Counter = Counter()
    moved = []
    left: Counter = Counter()
    left_cells = []
    missing = sorted(set(before) ^ set(after))
    for cell in sorted(after):
        run, ansi, key = cell
        record = after[cell]
        old = before.get(cell, {}).get("verdict", "MISSING")
        new = record["verdict"]
        fam = family(run, key, record)
        transitions[(run, ansi, old, new)] += 1
        per_family[(run, ansi, fam, new)] += 1
        if old != new:
            moved.append({"run": run, "ansi": ansi, "key": key, "main": old, "head": new})
        if new not in GOOD:
            label = named(run, key, record)
            left[(run, ansi, label, new)] += 1
            left_cells.append(
                {
                    "run": run,
                    "ansi": ansi,
                    "key": key,
                    "head": new,
                    "class": label,
                    "spark": record.get("spark"),
                    "repark": record.get("repark"),
                }
            )
    regressions = [m for m in moved if m["main"] in GOOD and m["head"] not in GOOD]
    out.mkdir(parents=True, exist_ok=True)
    (out / "moved.jsonl").write_text("".join(json.dumps(m) + "\n" for m in moved))
    (out / "left.jsonl").write_text("".join(json.dumps(m) + "\n" for m in left_cells))
    lines = ["# transitions (run, ANSI, main -> head, cells)", ""]
    lines += [f"- {r} {a}: {o} -> {n}: {c}" for (r, a, o, n), c in sorted(transitions.items())]
    lines += ["", "# head verdicts by family", ""]
    lines += [f"- {r} {a} {f} {n}: {c}" for (r, a, f, n), c in sorted(per_family.items())]
    lines += ["", "# non-EQUAL cells left on head, by named class", ""]
    lines += [f"- {r} {a} {label} {n}: {c}" for (r, a, label, n), c in sorted(left.items())]
    lines += ["", f"# EQUAL/BOTH_RAISE -> other on head: {len(regressions)}", ""]
    lines += [f"- {m}" for m in regressions]
    lines += ["", f"# cells present on one side only: {len(missing)}", ""]
    lines += [f"- {m}" for m in missing[:50]]
    (out / "compare.md").write_text("\n".join(lines) + "\n")
    counts = (len(moved), len(regressions), len(left_cells), len(missing))
    print("moved {} regressions {} left {} missing {}".format(*counts))


main()
