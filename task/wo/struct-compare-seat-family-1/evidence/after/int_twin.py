from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE.parent / "step0"
KEY_LIST = "key-list join"


def load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    records = (json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return {record["id"]: record for record in records}


def leg(raw: Path, name: str) -> dict[str, Any]:
    merged = load(raw / f"{name}.jsonl")
    for tag in ("v3", "add"):
        merged.update(load(raw / f"{name}-{tag}.jsonl"))
    return merged


def key_of(cell: dict[str, Any]) -> str:
    return "k" if cell["shape"] == "INT" else "s"


def null_pattern(records: list[list[Any]], at: int) -> list[Any]:
    keyed = (
        ([value for i, value in enumerate(row) if i != at], row[at] is None) for row in records
    )
    return sorted(keyed, key=json.dumps)


def signature(cell: dict[str, Any], spark: dict[str, Any], after: dict[str, Any]) -> Any:
    key = key_of(cell)
    names = [("KEY" if name == key else name) for name in after["columns"]]
    wanted = [("KEY" if name == key else name) for name in spark["columns"]]
    if sorted(names) != sorted(wanted) or len(set(names)) != len(names):
        return ("COLUMNS", tuple(names), tuple(wanted))
    index = [names.index(name) for name in wanted]
    rows = sorted(([row[i] for i in index] for row in after["rows"]), key=json.dumps)
    if rows == sorted(spark["rows"], key=json.dumps):
        return ("ORDER-ONLY", tuple(names), tuple(wanted))
    at = wanted.index("KEY")
    rest = [name for name in wanted if name != "KEY"]
    left, right = null_pattern(rows, at), null_pattern(spark["rows"], at)
    same_rest = sorted((r for r, _ in left), key=json.dumps) == sorted(
        (r for r, _ in right), key=json.dumps
    )
    nulls = tuple(sorted((json.dumps(r), n) for r, n in left))
    want_nulls = tuple(sorted((json.dumps(r), n) for r, n in right))
    shape = tuple(names), tuple(wanted), tuple(rest)
    return ("SHOWN-KEY" if same_rest else "ROWS", shape, nulls, want_nulls)


def strip_values(sig: Any) -> Any:
    if sig[0] != "SHOWN-KEY":
        return sig
    kind, shape, nulls, want_nulls = sig
    return kind, shape, tuple(n for _, n in nulls), tuple(n for _, n in want_nulls)


def twin_verdict(mine: Any, theirs: Any, other_case: bool) -> str:
    suffix = "-OTHER-CASE" if other_case else ""
    if theirs == mine:
        return "INT-TWIN-SAME" + suffix
    if (
        theirs
        and mine[0] == "ORDER-ONLY"
        and theirs[0] == "SHOWN-KEY"
        and theirs[1][:2] == mine[1:]
    ):
        return "INT-TWIN-SAME-ORDER" + suffix
    return "INT-TWIN-DIFFERS"


def main() -> None:
    after_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "raw"
    sys.path.insert(0, str(STEP0))
    import probe_family as pf
    import probe_family_add as pa

    spark, before, after = (
        leg(STEP0 / "raw", "spark"),
        leg(STEP0 / "raw", "head"),
        leg(after_raw, "head"),
    )
    cells = pf.gen_query_cells() + pa.gen_add_cells()
    twins: dict[tuple[Any, ...], dict[str, Any]] = {}
    for cell in cells:
        if cell["shape"] == "INT" and cell.get("twin"):
            twins[(cell["twin"], cell["door"], cell["detail"], cell["case"])] = cell
            twins.setdefault((cell["twin"], cell["door"], cell["detail"], "any"), cell)
    counts: Counter[tuple[str, str, str]] = Counter()
    moved, outside = [], []
    for cell in cells:
        if cell["seat"] != KEY_LIST or cell["shape"] == "INT":
            continue
        s, a = spark[cell["id"]], after[cell["id"]]
        if pf.qlabel(s, a) != "WRONG-ANSWER":
            continue
        if pf.qlabel(s, before[cell["id"]]) != "WRONG-ANSWER":
            moved.append(f"{cell['id']}:{pf.qlabel(s, before[cell['id']])}")
        twin_key = (cell.get("twin"), cell["door"], cell["detail"])
        twin = twins.get((*twin_key, cell["case"])) or twins.get((*twin_key, "any"))
        mine = strip_values(signature(cell, s, a))
        verdict = "NO-TWIN"
        if twin is not None:
            ts, ta = spark[twin["id"]], after[twin["id"]]
            theirs = strip_values(signature(twin, ts, ta)) if pf.ok(ts) and pf.ok(ta) else None
            verdict = twin_verdict(mine, theirs, twin["case"] != cell["case"])
        counts[(mine[0], verdict, cell["jt"])] += 1
        if not verdict.startswith("INT-TWIN-SAME"):
            twin_id = twin["id"] if twin else "-"
            outside.append(f"{cell['id']} {verdict} twin={twin_id} {mine[0]}")
    for (kind, verdict, join_type), count in sorted(counts.items()):
        sys.stdout.write(f"{count} {kind} {verdict} {join_type}\n")
    sys.stdout.write(f"total {sum(counts.values())}; moved into WRONG-ANSWER {len(moved)}\n")
    sys.stdout.write("moved: " + " ".join(moved) + "\n")
    sys.stdout.write("not INT-TWIN-SAME: " + (" ".join(outside) or "none") + "\n")


if __name__ == "__main__":
    main()
