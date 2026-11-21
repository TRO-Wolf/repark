import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import asof
import model


def records(raw: Path) -> list[tuple[str, dict]]:
    out = []
    for path in sorted(raw.glob("*.jsonl*")):
        if not path.name.startswith(("spark-", "repark-")):
            continue
        engine = "spark" if path.name.startswith("spark") else "main"
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt") as handle:
            for line in handle:
                rec = json.loads(line)
                if rec["g"] in ("G", "T") and "rows" in rec and rec["surf"] in ("D", "W1", "W2"):
                    out.append((engine, rec))
    return out


def position(rid: str, rrows: list) -> int | None:
    for row in rrows:
        if str(row[0]) == rid:
            return row[3]
    return None


def main() -> None:
    raw = Path(sys.argv[1])
    picks: dict[tuple, Counter] = defaultdict(Counter)
    near: Counter = Counter()
    dup: Counter = Counter()
    for engine, rec in records(raw):
        rows = [json.loads(text) for text in rec["rows"]]
        if rec["g"] == "T":
            for row in rows:
                key = (engine, rec["surf"], rec["dir"], "lid" + row[0], rec["ansi"])
                picks[key][(rec["order"], rec["part"], rec["shuffle"], row[9])] += 1
            continue
        sets = model.ties(rec)
        for row in rows:
            allowed = sets.get(row[0])
            if not allowed or len(allowed) < 2:
                continue
            spots = {position(r, asof.RIGHT) for r in allowed}
            lp = next(p for i, _, _, p in asof.LEFT if str(i) == row[0])
            if len(spots) > 1:
                side = "before" if (position(row[5], asof.RIGHT) or 0) < lp else "after"
                near[(engine, rec["surf"], side)] += 1
            else:
                rank = sorted(allowed, key=int).index(row[5]) if row[5] in allowed else -1
                dup[(engine, rec["surf"], "lowest-rid" if rank == 0 else "higher-rid")] += 1
    print("nearest ties, which side wins (grid, all types and modes)")
    for k, n in sorted(near.items()):
        print(" ", *k, n)
    print("duplicate right rows at one as-of value, which rid wins (grid)")
    for k, n in sorted(dup.items()):
        print(" ", *k, n)
    print(
        "tie group T: picks per (order, partitioning, shuffle partitions, payload) over 2 repeats"
    )
    for key, counter in sorted(picks.items()):
        payloads = Counter()
        for (_, _, _, payload), n in counter.items():
            payloads[payload] += n
        print(
            " ",
            *key,
            dict(sorted(payloads.items())),
            "by order:",
            {
                o: sorted({c[3] for c in counter if c[0] == o})
                for o in ("asis", "reversed", "rotated")
            },
        )


if __name__ == "__main__":
    main()
