import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

head_dir, main_dir, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
STRUCT = re.compile(r"\{([a-z]+->[^{}]*)\}")
CLASSES = [
    (
        "agg/having_avg_max/",
        "AVG-DECIMAL-ROUND-1 (side card): the HAVING row now matches; the shown avg truncates",
    ),
    (
        "const/i64_eq_hlit/tx/where",
        (
            "DECIMAL-CAST-SELECTIVITY (new finding): DataFusion's selectivity assertion on a "
            "BIGINT-edge cast; a user-written same-type cast raises on main"
        ),
    ),
    (
        "dfconst/fexpr_a_eq_lit/",
        (
            "FEXPR-DECIMAL-LITERAL (new finding): F.expr types 1.25 as DOUBLE, so the pair is "
            "decimal beside double"
        ),
    ),
    (
        "literal/u_overflow",
        (
            "ruling 6, carded: the literal's own cast refuses at planning "
            "(DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on"
        ),
    ),
    (
        "literal/where_overflow",
        (
            "ruling 6, carded: the literal's own cast refuses at planning "
            "(DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on"
        ),
    ),
    (
        "literal/coalesce_overflow",
        (
            "ruling 6, carded: the literal's own cast refuses at planning "
            "(DECIMAL-LITERAL-OVERFLOW-1, new card)"
        ),
    ),
    (
        "rev/m1/create_map_keys_apart",
        (
            "MAP-FILTER-ORDER (new finding): the map is evaluated above the filter and meets row "
            "6's NULL key; same-type user casts raise on main"
        ),
    ),
    ("rev/m1/map_concat_keys_apart", "MAP-FILTER-ORDER (new finding)"),
    ("rev/m1/map_keys_of_concat", "MAP-FILTER-ORDER (new finding)"),
    (
        "rev/m1/map_concat_keys_all",
        (
            "both raise; RePark's error is NULL_MAP_KEY from MAP-FILTER-ORDER, Spark's "
            "DUPLICATED_MAP_KEY"
        ),
    ),
    ("rev/m1/create_map_keys_id1", "both raise DUPLICATED_MAP_KEY (revision M1, as ruled)"),
    ("rev/m1/map_concat_keys_id1", "both raise DUPLICATED_MAP_KEY (revision M1, as ruled)"),
    ("rev/pin/struct_cmp", "both raise (ruling 5: pin only)"),
]


def load(path: Path) -> dict:
    found = {}
    for line in path.open():
        r = json.loads(line)
        found[(r["engine"], r["ansi"], r["key"])] = r
    return found


def struct_render(value: object) -> object:
    if isinstance(value, dict) and isinstance(value.get("rows"), list):
        rows = []
        for row in value["rows"]:
            previous = None
            while previous != row:
                previous = row
                row = STRUCT.sub(
                    lambda m: (
                        "[" + ",".join(p.split("->", 1)[1] for p in m.group(1).split(",")) + "]"
                    ),
                    row,
                )
            rows.append(row)
        return {"rows": sorted(rows), "types": value["types"]}
    return value


def verdict(r: dict) -> str:
    spark = r["spark"] if isinstance(r["spark"], dict) else {}
    struct = any(t.startswith("struct<") for t in spark.get("types", []))
    if r["verdict"] == "wrong_answer" and struct and struct_render(r["repark"]) == spark:
        return "agree"
    return r["verdict"]


def name_class(key: str) -> str:
    for prefix, label in CLASSES:
        if key.startswith(prefix):
            return label
    return "UNNAMED"


head = load(head_dir / "verdicts.jsonl")
main = load(main_dir / "verdicts.jsonl")
lines = []
counts = defaultdict(Counter)
transitions = Counter()
moved = []
for key, r in sorted(head.items()):
    v = verdict(r)
    m = main.get(key)
    mv = verdict(m) if m else "absent"
    if r["engine"] == "repark":
        counts[(r["cause"], r["ansi"])][v] += 1
    transitions[(r["engine"], r["ansi"], mv, v)] += 1
    if mv == "agree" and v != "agree":
        moved.append((key, mv, v))
lines.append("## Common-type cells (Spark door), per ANSI mode")
order = [
    "agree",
    "both_raise",
    "wrong_answer",
    "wrong_type",
    "wrong_write",
    "over_raise",
    "missed_raise",
]
lines.append("| ANSI | cells | " + " | ".join(order) + " |")
lines.append("|---|---|" + "---|" * len(order))
for ansi in ("false", "true"):
    c = counts[("common-type", ansi)]
    lines.append(f"| {ansi} | {sum(c.values())} | " + " | ".join(str(c[o]) for o in order) + " |")
lines.append("")
lines.append("## Every cause (Spark door), per ANSI mode")
lines.append("| cause | ANSI | cells | " + " | ".join(order) + " |")
lines.append("|---|---|---|" + "---|" * len(order))
for (cause, ansi), c in sorted(counts.items()):
    lines.append(
        f"| {cause} | {ansi} | {sum(c.values())} | " + " | ".join(str(c[o]) for o in order) + " |"
    )
lines.append("")
lines.append(f"## EQUAL on main and not EQUAL on the head: {len(moved)}")
for key, a, b in moved:
    lines.append(f"- {key}: {a} -> {b}")
lines.append("")
lines.append("## Transitions main -> head (engine, ANSI, main, head: cells)")
for (engine, ansi, a, b), n in sorted(transitions.items()):
    lines.append(f"- {engine} {ansi} {a} -> {b}: {n}")
lines.append("")
lines.append("## Every non-EQUAL common-type cell on the head, named")
for key, r in sorted(head.items()):
    if r["engine"] != "repark" or r["cause"] != "common-type":
        continue
    v = verdict(r)
    if v == "agree":
        continue
    lines.append(f"- ANSI {key[1]} `{key[2]}`: {v} — {name_class(key[2])}")
lines.append("")
lines.append("## Struct cells agreed after rendering PySpark's Row and RePark's dict alike")
for key, r in sorted(head.items()):
    if r["verdict"] != verdict(r):
        lines.append(f"- {key[0]} ANSI {key[1]} `{key[2]}`")
out.write_text("\n".join(lines) + "\n")
print("\n".join(lines[:12]))
print("moved", len(moved))
