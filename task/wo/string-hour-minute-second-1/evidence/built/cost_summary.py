import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def main() -> None:
    folder = Path(sys.argv[1])
    inner: dict[tuple[str, str], list[float]] = defaultdict(list)
    for line in (folder / "raw-k.jsonl").read_text().splitlines():
        r = json.loads(line)
        inner[(r["case"], r["label"])].append(r["median"])
    walls: dict[tuple[str, str], list[float]] = defaultdict(list)
    for line in (folder / "samples.jsonl").read_text().splitlines():
        r = json.loads(line)
        if r["group"] == "c012":
            walls[(r["case"], r["build"])].append(r["wall"])
    rows = ["| case | measure | main | head | main again | head / main | control band | runs |"]
    rows.append("|---|---|---|---|---|---|---|---|")
    for source, label in ((inner, "in-process median of 10 (s)"), (walls, "process wall (s)")):
        for case in sorted({c for c, _ in source}):
            m = statistics.median(source[(case, "main")])
            h = statistics.median(source[(case, "head")])
            a = statistics.median(source[(case, "main-aa")])
            n = len(source[(case, "head")])
            band = f"{a / m:.3f}"
            rows.append(
                f"| {case} | {label} | {m:.4f} | {h:.4f} | {a:.4f} | {h / m:.3f} | {band} | {n} |"
            )
    if ("k1", "head") in inner and ("k1x", "main") in inner:
        k1 = statistics.median(inner[("k1", "head")])
        k1x = statistics.median(inner[("k1x", "main")])
        k1x_aa = statistics.median(inner[("k1x", "main-aa")])
        rows.append("")
        rows.append(
            f"K1 head / K1x main = {k1 / k1x:.3f}; K1 head / K1x main again = {k1 / k1x_aa:.3f}"
        )
    print("\n".join(rows))


main()
