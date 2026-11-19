import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

rows = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
out = Path(sys.argv[2])
by_tag = {r["tag"]: r for r in rows}
MATCH = {
    "Z": {"la_utc": ("utc_utc", "la_la"), "utc_la": ("la_la", "utc_utc")},
    "T": {"LTZ_NTZ": ("NTZ_NTZ", "LTZ_LTZ"), "NTZ_LTZ": ("LTZ_LTZ", "NTZ_NTZ")},
    "A": {"true_false": ("false_false", "true_true"), "false_true": ("true_true", "false_false")},
}
lines = []


def say(text: str) -> None:
    lines.append(text)


def head_gap(record: dict) -> str:
    if record["v_head"] in ("EQUAL", "NOSPARK"):
        return record["v_head"].lower()
    pair = MATCH.get(record["group"], {}).get(record["direction"], ())
    old = "/" + record["direction"] + "/"
    refs = [by_tag.get(record["tag"].replace(old, "/" + t + "/", 1)) for t in pair]
    if any(ref is not None and ref["v_head"] != "EQUAL" for ref in refs):
        return "direct"
    return "view"


say("cells " + str(len(rows)))
for group in ("Z", "A", "T", "S", "W"):
    picked = [r for r in rows if r["group"] == group]
    say(
        group
        + " main "
        + json.dumps(dict(Counter(r["v_main"] for r in picked)))
        + " head "
        + json.dumps(dict(Counter(r["v_head"] for r in picked)))
        + " moved "
        + str(sum(1 for r in picked if r["moved"]))
    )
say("\n## Z matching by reader, head (main)")
for reader in sorted({r["reader"] for r in rows if r["group"] == "Z"}):
    picked = [r for r in rows if r["group"] == "Z" and not r["cross"] and r["reader"] == reader]
    say(
        reader
        + " head "
        + json.dumps(dict(Counter(r["v_head"] for r in picked)))
        + " main "
        + json.dumps(dict(Counter(r["v_main"] for r in picked)))
    )
say("\n## Z cross by reader, head gap class (main gap class)")
for reader in sorted({r["reader"] for r in rows if r["group"] == "Z"}):
    picked = [r for r in rows if r["group"] == "Z" and r["cross"] and r["reader"] == reader]
    say(
        reader
        + " head "
        + json.dumps(dict(Counter(head_gap(r) for r in picked)))
        + " main "
        + json.dumps(dict(Counter(r.get("gap", "") for r in picked)))
    )
say("\n## twins against Spark (csv_fix vs csv_s, emu vs json_s), head and main")
twin = defaultdict(Counter)
for record in rows:
    if "twin_vs_spark" in record:
        twin[
            record["group"]
            + " "
            + record["reader"]
            + (" cross" if record["cross"] else " matching")
        ][record["twin_vs_spark"]] += 1
for key in sorted(twin):
    say(key + " " + json.dumps(dict(twin[key])))
say("\n## T by reader, head (main)")
for reader in sorted({r["reader"] for r in rows if r["group"] == "T"}):
    picked = [r for r in rows if r["group"] == "T" and r["reader"] == reader]
    say(
        reader
        + " head "
        + json.dumps(dict(Counter(r["v_head"] for r in picked)))
        + " main "
        + json.dumps(dict(Counter(r["v_main"] for r in picked)))
    )
say("\n## W csv_s d writes")
for record in rows:
    if record["group"] == "W" and record["reader"] == "csv_s" and record["door"].startswith("d/"):
        say(record["tag"] + " main " + record["v_main"] + " head " + record["v_head"])
say("\n## every moved cell (main -> head)")
moves = Counter()
for record in rows:
    if record["moved"]:
        kind = record["v_main"] + "->" + record["v_head"]
        moves[(record["group"], record["reader"], kind)] += 1
for key in sorted(moves):
    say(" ".join(key) + " " + str(moves[key]))
say("\n## moved cells outside csv_s, csv_f, csv_fix")
for record in rows:
    if record["moved"] and record["reader"] not in ("csv_s", "csv_f", "csv_fix"):
        say(record["tag"] + " | main " + record["main"][:80] + " | head " + record["head"][:80])
say("\n## EQUAL lost")
for record in rows:
    if record["v_main"] == "EQUAL" and record["v_head"] != "EQUAL":
        say(
            record["tag"]
            + " | spark "
            + record["spark"][:60]
            + " | main "
            + record["main"][:60]
            + " | head "
            + record["head"][:60]
        )
say("\n## changed answers that keep the verdict")
for record in rows:
    if record["moved"] and record["v_main"] == record["v_head"]:
        say(
            record["v_main"]
            + " "
            + record["tag"]
            + " | main "
            + record["main"][:90]
            + " | head "
            + record["head"][:90]
        )
out.write_text("\n".join(lines) + "\n")
print("\n".join(lines[:80]))
