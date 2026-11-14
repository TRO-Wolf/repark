from __future__ import annotations

import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

PARTS = ("select", "nan", "context", "dml")
KEY_FIELDS = ("part", "context", "stmt", "kind", "op", "t", "l", "r")
GREATEST_ARRAY = ("greatest", "least")


def load(path: Path) -> list[dict]:
    packed = path.with_name(path.name + ".gz")
    if packed.exists():
        return [r for r in map(json.loads, gzip.open(packed, "rt")) if r.get("record") == "cell"]
    if not path.exists():
        return []
    return [r for r in map(json.loads, path.open()) if r.get("record") == "cell"]


def key(cell: dict, door: str) -> tuple:
    return (door, *tuple(cell.get(f) for f in KEY_FIELDS))


def verdict(spark: dict | None, repark: dict) -> str:
    if repark["outcome"] == "NOT-EXPRESSIBLE":
        return "NOT-EXPRESSIBLE"
    if spark is None:
        return "NO-TRUTH"
    s_ok = spark["outcome"] == "ANSWERED"
    r_ok = repark["outcome"] == "ANSWERED"
    if s_ok and r_ok:
        return "EQUAL" if spark.get("rows") == repark.get("rows") else "WRONG"
    if not s_ok and not r_ok:
        return "BOTH-REFUSED"
    if s_ok:
        return "PANIC" if repark["outcome"] == "PANIC" else "REFUSED"
    return "SPARK-REFUSED"


def truth_door(door: str) -> str:
    return "spark-sql" if door in ("native", "filter-str") else door


def nan_unit(op: str) -> str:
    return "NAN-GREATEST-ARRAY-1" if op in GREATEST_ARRAY else "NAN-COMPARE-LITERAL-1"


def main() -> None:
    raw = Path(sys.argv[1])
    label = sys.argv[2]
    out = Path(sys.argv[3])
    joined = []
    for part in PARTS:
        for ansi in ("true", "false"):
            spark = {}
            for cell in load(raw / f"spark-{part}-ansi-{ansi}.jsonl"):
                spark[key(cell, cell["door"])] = cell
            for cell in load(raw / f"{label}-{part}-ansi-{ansi}.jsonl"):
                door = cell["door"]
                truth = spark.get(key(cell, truth_door(door)))
                if door == "native":
                    other = load_cached(raw, part)
                    truth = other.get(key(cell, "spark-sql"), truth)
                joined.append(
                    {
                        **{f: cell.get(f) for f in KEY_FIELDS},
                        "ansi": "n/a" if door == "native" else ansi,
                        "door": door,
                        "expr": cell.get("expr"),
                        "verdict": verdict(truth, cell),
                        "spark": None
                        if truth is None
                        else (
                            truth.get("rows")
                            if truth["outcome"] == "ANSWERED"
                            else truth["outcome"]
                        ),
                        "repark": cell.get("rows")
                        if cell["outcome"] == "ANSWERED"
                        else cell["outcome"],
                        "repark_error": (cell.get("error") or {}).get("message"),
                        "spark_error": None
                        if truth is None
                        else (truth.get("error") or {}).get("message"),
                    }
                )
    with gzip.open(out, "wt") as sink:
        for row in joined:
            sink.write(json.dumps(row) + "\n")
    report(joined)


_CACHE: dict = {}


def load_cached(raw: Path, part: str) -> dict:
    if part not in _CACHE:
        _CACHE[part] = {key(c, c["door"]): c for c in load(raw / f"spark-{part}-ansi-true.jsonl")}
    return _CACHE[part]


def report(joined: list[dict]) -> None:
    by = defaultdict(Counter)
    for row in joined:
        by[(row["part"], row["door"], row["ansi"])][row["verdict"]] += 1
    verdicts = (
        "EQUAL",
        "WRONG",
        "REFUSED",
        "PANIC",
        "BOTH-REFUSED",
        "SPARK-REFUSED",
        "NOT-EXPRESSIBLE",
        "NO-TRUTH",
    )
    print("| part | door | ANSI | cells | " + " | ".join(verdicts) + " |")
    print("|---" * (4 + len(verdicts)) + "|")
    total = Counter()
    for k in sorted(by):
        c = by[k]
        total.update(c)
        print(
            f"| {k[0]} | {k[1]} | {k[2]} | {sum(c.values())} | "
            + " | ".join(str(c.get(v, 0)) for v in verdicts)
            + " |"
        )
    print(
        f"| all | | | {sum(total.values())} | "
        + " | ".join(str(total.get(v, 0)) for v in verdicts)
        + " |"
    )
    print()
    doors = sorted({r["door"] for r in joined})
    for part in ("select", "nan", "context", "mixed"):
        rows = [r for r in joined if r["part"] == part]
        if not rows:
            continue
        print(
            f"### {part}: WRONG / REFUSED cells by operator and door "
            "(ANSI true | false; native n/a)"
        )
        print()
        print("| op | " + " | ".join(doors) + " |")
        print("|---" * (1 + len(doors)) + "|")
        for op in sorted(
            {r["op"] for r in rows}, key=list(dict.fromkeys(r["op"] for r in rows)).index
        ):
            cells = []
            for door in doors:
                parts = []
                for ansi in ("true", "false", "n/a"):
                    sel = [
                        r for r in rows if r["op"] == op and r["door"] == door and r["ansi"] == ansi
                    ]
                    if not sel:
                        continue
                    bad = sum(r["verdict"] in ("WRONG", "REFUSED", "PANIC") for r in sel)
                    ne = sum(r["verdict"] == "NOT-EXPRESSIBLE" for r in sel)
                    parts.append(f"{bad}/{len(sel) - ne}")
                cells.append(" \\| ".join(parts) if parts else "-")
            print(f"| {op} | " + " | ".join(cells) + " |")
        print()
    print("### dml: every non-EQUAL cell")
    print()
    print("| door | ANSI | kind | stmt | op | type | left | right | verdict | Spark | RePark |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in joined:
        if r["part"] == "dml" and r["verdict"] != "EQUAL":
            answer = r["repark"]
            if answer == "REFUSED":
                answer = "REFUSED: " + (r["repark_error"] or "")[:60]
            head = " | ".join(
                str(r[f]) for f in ("door", "ansi", "kind", "stmt", "op", "t", "l", "r")
            )
            print(f"| {head} | {r['verdict']} | {r['spark']} | {answer} |")
    print()


if __name__ == "__main__":
    main()
