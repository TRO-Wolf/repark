import json
import sys
from collections import Counter
from pathlib import Path

D4_READERS = ("json_i", "csv_ns", "csv_0")


def records(path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if "tag" in r:
            out[r["tag"]] = r
    return out


def cells(rec: dict | None, by_column: bool) -> dict[str, str]:
    if rec is None:
        return {}
    if "error" in rec or "err" in rec:
        return {"*": "!" + str(rec.get("error", rec.get("err")))[:60]}
    if by_column and "names" in rec and "cols" in rec:
        return {
            name: json.dumps([row[i] for row in rec["cols"]]) for i, name in enumerate(rec["names"])
        }
    return {"*": json.dumps(rec.get("rows"))}


def main() -> None:
    spark_dir, base_dir, head_dir = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    parts = [("W" + str(i), f"spark-W{i}.jsonl", f"rp-W{i}.jsonl") for i in range(4)]
    parts += [("m-Z" + str(i), f"m-spark-Z{i}.jsonl", f"rp-m-Z{i}.jsonl") for i in range(8)]
    ref_dir = Path(sys.argv[4]) if len(sys.argv) > 4 else None
    tally: Counter[str] = Counter()
    lines = []
    for part, spark_name, mine in parts:
        sp = records(spark_dir / spark_name)
        base, head = records(base_dir / mine), records(head_dir / mine)
        ref = records(ref_dir / mine.removeprefix("rp-")) if ref_dir else {}
        for tag in sorted(set(base) | set(head)):
            reader = tag.split("/")[3] if tag.count("/") >= 3 else ""
            by_column = part.startswith("m-")
            s, b, h = (cells(x.get(tag), by_column) for x in (sp, base, head))
            for col in sorted(set(b) | set(h)):
                want = s.get(col)
                vb = "EQUAL" if want is not None and b.get(col) == want else "NOT"
                vh = "EQUAL" if want is not None and h.get(col) == want else "NOT"
                d4 = reader in D4_READERS and (
                    (part.startswith("W") and reader == "json_i")
                    or (part.startswith("m-") and col == "b")
                )
                if d4 and ref_dir and part.startswith("m-"):
                    d4 = cells(ref.get(tag), by_column).get(col) != want
                scope = "d4" if d4 else "other"
                tally[f"{scope} {part[0]} base={vb} head={vh}"] += 1
                if d4 or b.get(col) != h.get(col):
                    row = [
                        scope,
                        part,
                        tag,
                        col,
                        str(want)[:70],
                        str(b.get(col))[:70],
                        str(h.get(col))[:70],
                        vb,
                        vh,
                    ]
                    lines.append("\t".join(row))
    print(json.dumps(dict(sorted(tally.items())), indent=1))
    print("\n".join(lines))


main()
