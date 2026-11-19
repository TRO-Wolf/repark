import json
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "probes"))
import cells

MODELLED = ("grouped_s4", "try_grouped_s4", "grouped_p2", "try_grouped_p2")


def files_of(table: str, g: int, p: int) -> list:
    if table == "p2":
        return [[cells.unscaled(t, p) for t in part] for part in cells.P2_SHAPES[g]]
    rows = []
    rid = 0
    for shape_g, shape in cells.SHAPES.items():
        for token in shape:
            rid += 1
            if shape_g == g:
                rows.append((rid % 4, cells.unscaled(token, p)))
    return [[v for f, v in rows if f == k] for k in range(4)]


def predicted(table: str, g: int, col: str, ansi: str, try_sum: bool) -> object:
    p, s = (int(x) for x in col[1:].split("_"))
    limit = 10 ** min(38, p + 10)
    partials = []
    overflowed = False
    for part in files_of(table, g, p):
        values = [v for v in part if v is not None]
        if not values:
            continue
        running = 0
        for v in values:
            running += v
            if abs(running) >= limit or abs(running) >= 2**127:
                overflowed = True
        partials.append(running)
    if not partials:
        return None
    total = sum(partials)
    if overflowed or abs(total) >= limit:
        return "ERR:ARITHMETIC_OVERFLOW" if ansi == "true" and not try_sum else None
    return Decimal(total).scaleb(-s)


def spark_answer(result: object) -> object:
    if isinstance(result, str):
        return "ERR:" + result.split(":")[2]
    value = result["rows"][0][-1]
    return None if value == "null" else Decimal(value)


def verdict(spark: object, repark: object) -> str:
    raises = [isinstance(x, str) for x in (spark, repark)]
    if all(raises):
        return "BOTH_RAISE" if spark == repark else "BOTH_RAISE_OTHER"
    if raises[0]:
        return "SPARK_RAISES"
    if raises[1]:
        return "REPARK_RAISES"
    return "EQUAL" if spark == repark else "VALUE"


def load(path: Path) -> dict:
    return {r["key"]: r for r in map(json.loads, path.open()) if "key" in r}


def main() -> None:
    step0, head, out = (Path(arg) for arg in sys.argv[1:4])
    old = {
        (r["ansi"], r["key"]): r["verdict"]
        for r in map(json.loads, (head / "verdicts.jsonl").open())
    }
    moves: Counter = Counter()
    moved = []
    for ansi in ("true", "false"):
        passes = {
            "": load(step0 / f"spark-{ansi}.jsonl"),
            "mp/": load(step0 / f"spark-mp-{ansi}.jsonl"),
        }
        for key, _fam, pos, col, g, table, _sql in cells.sum_sql_cells():
            if pos not in MODELLED:
                continue
            mine = predicted(table, g, col, ansi, pos.startswith("try_"))
            for prefix, spark in passes.items():
                cell = prefix + key
                new = verdict(spark_answer(spark[cell]["result"]), mine)
                before = old[(ansi, cell)]
                moves[(ansi, prefix or "default", before, new)] += 1
                if before != new:
                    moved.append({"ansi": ansi, "key": cell, "head": before, "predicted": new})
    out.write_text("".join(json.dumps(m) + "\n" for m in moved))
    for (ansi, run, before, new), n in sorted(moves.items()):
        if before != new:
            print(ansi, run, before, "->", new, n)
    print("modelled cells", sum(moves.values()), "moved", len(moved))


if __name__ == "__main__":
    main()
