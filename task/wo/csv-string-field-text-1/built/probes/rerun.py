import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

evidence, raw, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
VARIANTS = {"base", "repark", "text_try", "text_cast", "scan0", "spark"}


def spark_key(tag: str) -> str | None:
    parts = tag.split("/")
    for index, part in enumerate(parts[:3]):
        if part in VARIANTS:
            if part not in {"base", "repark", "spark"}:
                return None
            parts[index] = "spark"
            return "/".join(parts)
    return tag


def read_lines(path: Path) -> list[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as handle:
        return [json.loads(line) for line in handle if line.strip()]


spark: dict[str, dict[str, Any]] = {}
for name in ("cells-spark.jsonl.gz", "revision-cells-spark.jsonl.gz"):
    for record in read_lines(evidence / name):
        spark[record["tag"]] = record
for record in read_lines(evidence / "kernel-cells.jsonl"):
    if "/spark/" in record["tag"]:
        spark[record["tag"]] = record


def verdict(got: dict[str, Any], want: dict[str, Any] | None) -> str:
    if want is None:
        return "NOSPARK"
    got_err = "error" in got or "create_error" in got
    want_err = "error" in want or "create_error" in want
    if got_err and want_err:
        return "BOTHERR"
    if got_err:
        return "RAISE"
    if want_err:
        return "WRONG"
    same = got["rows"] == want["rows"] and got["types"] == want["types"]
    return "EQUAL" if same else "WRONG"


cells: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
for path in sorted(raw.glob("*.jsonl")):
    build, family = path.name.split("-")[:2]
    oracle_mode = path.stem.split("-")[-1] if "-R-oracle-" in path.name else None
    for record in read_lines(path):
        tag = record["tag"]
        if oracle_mode is not None and tag.startswith("O/"):
            tag = f"O/{oracle_mode}/{tag[2:]}"
        if tag.startswith("E/") or tag.endswith("/drop"):
            continue
        key = spark_key(tag)
        if key is None:
            continue
        cells[f"{family}:{key}"][build] = record
table: dict[str, Counter[str]] = defaultdict(Counter)
moves: Counter[str] = Counter()
lost: list[str] = []
rows = []
for key, by_build in sorted(cells.items()):
    family, tag = key.split(":", 1)
    want = spark.get(tag)
    seen = {build: verdict(record, want) for build, record in by_build.items()}
    group = family + "/" + tag.split("/")[0]
    for build, value in seen.items():
        table[f"{group}/{build}"][value] += 1
    base, head = seen.get("base"), seen.get("head")
    if base and head:
        moves[f"{group}: {base} -> {head}"] += 1
        if base == "EQUAL" and head != "EQUAL":
            lost.append(key)
    rows.append({"key": key, **seen})
lines = ["| set / build | EQUAL | WRONG | RAISE | BOTHERR | NOSPARK |", "|---|---|---|---|---|---|"]
for name in sorted(table):
    count = table[name]
    lines.append(
        f"| {name} | {count['EQUAL']} | {count['WRONG']} | {count['RAISE']} | {count['BOTHERR']} | "
        f"{count['NOSPARK']} |"
    )
lines += ["", "| move (base -> head) | cells |", "|---|---|"]
lines += [f"| {move} | {count} |" for move, count in sorted(moves.items())]
lines += ["", f"EQUAL lost: {len(lost)}"] + [f"- {key}" for key in lost]
out.mkdir(parents=True, exist_ok=True)
(out / "rerun-table.md.txt").write_text("\n".join(lines) + "\n")
with gzip.open(out / "rerun-verdicts.jsonl.gz", "wt") as handle:
    handle.writelines(json.dumps(row) + "\n" for row in rows)
print("\n".join(lines[:80]))
