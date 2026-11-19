import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

rows = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
GAPS = ["equal", "view", "direct", "nospark"]


def table(title: str, keyed: dict[str, Counter[str]], columns: list[str]) -> None:
    print("\n### " + title + "\n")
    print("| key | " + " | ".join(columns) + " | total |")
    print("|---" * (len(columns) + 2) + "|")
    for key in sorted(keyed):
        counts = keyed[key]
        print(
            "| "
            + key
            + " | "
            + " | ".join(str(counts.get(c, 0)) for c in columns)
            + " | "
            + str(sum(counts.values()))
            + " |"
        )


def tally(selected: list[dict[str, Any]], field: str, value: str) -> dict[str, Counter[str]]:
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for record in selected:
        out[record[field]][record[value]] += 1
    return out


print("## Headline")
print(
    "\n| set | cells | cross-setting | main EQUAL / WRONG / RAISE / no Spark"
    " | part 1 same | moves main vs part 1 |"
)
print("|---|---|---|---|---|---|")
for group in ("Z", "A", "T", "S", "W"):
    picked = [r for r in rows if r["group"] == group]
    main = Counter(r["v_main"] for r in picked)
    part1 = Counter(r["v_part1"] for r in picked)
    moved = sum(1 for r in picked if r["moved"])
    cross = sum(1 for r in picked if r["cross"])
    shown = " / ".join(str(main.get(k, 0)) for k in ("EQUAL", "WRONG", "RAISE", "NOSPARK"))
    same = "yes" if main == part1 else json.dumps(dict(part1))
    print(f"| {group} | {len(picked)} | {cross} | {shown} | {same} | {moved} |")
for group in ("Z", "A", "T"):
    cross = [r for r in rows if r["group"] == group and r["cross"]]
    table(group + " cross-setting cells by reader (gap class)", tally(cross, "reader", "gap"), GAPS)
    table(group + " cross-setting cells by door (gap class)", tally(cross, "door", "gap"), GAPS)
    if group == "Z":
        table("Z cross-setting cells by body (gap class)", tally(cross, "body", "gap"), GAPS)
        for record in cross:
            record["pass"] = record["mode"] + " " + record["direction"]
        table("Z cross-setting cells by pass (gap class)", tally(cross, "pass", "gap"), GAPS)
    matching = [r for r in rows if r["group"] == group and not r["cross"]]
    table(
        group + " matching cells by reader (main)",
        tally(matching, "reader", "v_main"),
        ["EQUAL", "WRONG", "RAISE", "NOSPARK"],
    )
for group in ("Z", "A", "T", "W"):
    moved = [r for r in rows if r["group"] == group and r.get("move")]
    table(
        group + " moves, main against part 1, by reader",
        tally(moved, "reader", "move"),
        ["gained", "lost", "changed"],
    )
writes = [r for r in rows if r["group"] == "W"]
table("W by reader (gap class)", tally(writes, "reader", "gap"), GAPS)
for record in writes:
    record["write"] = record["door"]
table("W by source and write (gap class)", tally(writes, "write", "gap"), GAPS)
print("\n### Every wrong write (main), by reader, source and write: passes wrong of four\n")
named: dict[str, list[str]] = defaultdict(list)
for record in writes:
    if record["v_main"] in ("WRONG", "RAISE"):
        named[record["reader"] + " " + record["door"]].append(
            record["mode"][5:] + " " + record["direction"]
        )
for key in sorted(named):
    print("- `" + key + "`: " + ", ".join(sorted(named[key])))
twins = [r for r in rows if "twin_vs_spark" in r]
for record in twins:
    record["twin_key"] = (
        record["group"] + " " + record["reader"] + (" cross" if record["cross"] else " matching")
    )
table(
    "Emulation readers against their Spark twins (main)",
    tally(twins, "twin_key", "twin_vs_spark"),
    ["EQUAL", "WRONG", "RAISE"],
)
