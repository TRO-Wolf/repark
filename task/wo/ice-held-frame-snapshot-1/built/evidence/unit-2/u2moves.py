import json
import sys
from collections import Counter
from pathlib import Path

sources = [Path(p) for p in sys.argv[1].split(",")]
first, second = sys.argv[2], sys.argv[3]


def load(label: str) -> dict:
    found: dict = {}
    for source in sources:
        for ansi in ("true", "false"):
            for path in sorted(source.glob(f"*-{label}-{ansi}.jsonl")):
                for line in path.read_text().splitlines():
                    record = json.loads(line)
                    found[(record["cell"], ansi)] = record["got"]
    return found


spark, a, b = load("spark"), load(first), load(second)
moves: Counter = Counter()
for key, answer in sorted(spark.items()):
    va = "E" if a.get(key, "MISSING") == answer else "W"
    vb = "E" if b.get(key, "MISSING") == answer else "W"
    moves[va + vb] += 1
    if va != vb or vb == "W":
        print(
            va + "->" + vb,
            key[1],
            key[0],
            "| spark",
            json.dumps(answer)[:90],
            "|",
            second,
            json.dumps(b.get(key))[:90],
        )
print(dict(moves))
