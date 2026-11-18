import json
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "probes"))
import cells
from predict import predicted

spread = Path(sys.argv[1])
builds = sys.argv[2:]
GOOD = ("EQUAL", "BOTH_RAISE")


def load(path: Path) -> dict:
    return {r["key"]: r["answers"] for r in map(json.loads, path.open()) if "answers" in r}


def value(answer: str) -> object:
    if answer.startswith("ERR:"):
        return answer
    rows = json.loads(answer)["rows"]
    return tuple(None if row[-1] == "null" else Decimal(row[-1]) for row in rows)


def verdict(spark: object, repark: object) -> str:
    raises = [isinstance(x, str) for x in (spark, repark)]
    if all(raises):
        return "BOTH_RAISE" if spark == repark else "BOTH_RAISE_OTHER"
    if raises[0]:
        return "SPARK_RAISES"
    if raises[1]:
        return "REPARK_RAISES"
    return "EQUAL" if spark == repark else "VALUE"


def model(key: str, ansi: str) -> object:
    for k, _fam, pos, col, g, table, _sql in cells.sum_sql_cells():
        if "mp/" + k == key and pos in (
            "grouped_s4",
            "try_grouped_s4",
            "grouped_p2",
            "try_grouped_p2",
        ):
            mine = predicted(table, g, col, ansi, pos.startswith("try_"))
            if isinstance(mine, str):
                return mine
            return (mine,)
    return "unmodelled"


for ansi in ("true", "false"):
    for conf in ("split", "default"):
        spark = load(spread / f"spark-{ansi}-{conf}-passback.jsonl")
        print(f"## ANSI {ansi}, Spark local[4] {conf}: {len(spark)} queries")
        for build in builds:
            mine = load(spread / f"repark-{build}-{ansi}-none-all.jsonl")
            tally: Counter = Counter()
            for key, answers in spark.items():
                assert len(answers) == 1, (key, answers)
                want = value(next(iter(answers)))
                runs = Counter()
                for answer, n in mine[key].items():
                    runs[verdict(want, value(answer)) in GOOD] += n
                tally[
                    "good on every run"
                    if not runs[False]
                    else "good on no run"
                    if not runs[True]
                    else "good on some runs"
                ] += 1
            print(f"- {build}: {dict(tally)}")
        tally = Counter()
        for key, answers in spark.items():
            mine = model(key, ansi)
            if mine == "unmodelled":
                tally["unmodelled"] += 1
                continue
            tally[verdict(value(next(iter(answers))), mine)] += 1
        print(f"- model of the change (s4/p2 grouped cells): {dict(tally)}")
