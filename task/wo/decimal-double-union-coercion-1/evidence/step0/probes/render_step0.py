import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

cells_dir = Path(sys.argv[1])
summary = json.loads((cells_dir / "summary.json").read_text())
rows = [json.loads(line) for line in (cells_dir / "cells.jsonl").read_text().splitlines()]
GOOD = {"equal", "EQUAL", "BOTH-RAISE", "both-raise"}

by = defaultdict(Counter)
for r in rows:
    k = (r["section"], r["ansi"])
    by[k]["cells"] += 1
    by[k]["main wrong"] += r["main_verdict"] not in GOOD
    by[k]["ff34 wrong"] += r["ff34_verdict"] not in GOOD
    by[k][r["transition"]] += 1
print(
    "| section | ANSI | cells | main wrong | ff34 wrong | matched on main, wrong on ff34 | fixed on ff34 | wrong on both |"
)
print("|---|---|---|---|---|---|---|---|")
tot = Counter()
for (section, mode), c in sorted(by.items()):
    both = c["both-wrong"] + c["both-wrong-differently"]
    print(
        f"| {section} | {mode} | {c['cells']} | {c['main wrong']} | {c['ff34 wrong']} | "
        f"{c['broken-on-ff34']} | {c['fixed-on-ff34']} | {both} |"
    )
    for key in ("cells", "main wrong", "ff34 wrong", "broken-on-ff34", "fixed-on-ff34"):
        tot[key] += c[key]
    tot["both"] += both
print(
    f"| **all** | | **{tot['cells']}** | **{tot['main wrong']}** | **{tot['ff34 wrong']}** | "
    f"**{tot['broken-on-ff34']}** | **{tot['fixed-on-ff34']}** | **{tot['both']}** |"
)
print()
print(
    "| family predicate | ANSI | cells | main wrong | ff34 wrong | matched on main, wrong on ff34 | fixed on ff34 | wrong on both |"
)
print("|---|---|---|---|---|---|---|---|")
for key, v in summary["family"].items():
    mode, pred = key.split("|")
    print(
        f"| `d {pred}` | {mode} | {v['cells']} | {v['main_wrong']} | {v['ff34_wrong']} | "
        f"{v['broken_on_ff34']} | {v['fixed_on_ff34']} | {v['both_wrong']} |"
    )
print()
print(
    "| coercion fuzz shape family | scale | ANSI | cells | matched on main, wrong on ff34 | fixed on ff34 | wrong on both |"
)
print("|---|---|---|---|---|---|---|")
for mode, fams in summary["coerce"].items():
    for fam, c in fams.items():
        name, scale = fam.split("/")
        n = sum(c.values())
        print(
            f"| {name} | {scale} | {mode} | {n} | {c.get('broken-on-ff34', 0)} | "
            f"{c.get('fixed-on-ff34', 0)} | {c.get('both-wrong', 0)} |"
        )
print()
print("| #888 replay root | build | verdicts |")
print("|---|---|---|")
for key, c in summary["replay_888"].items():
    rt, build = key.split("|")
    print(
        f"| {rt} | {build} | " + ", ".join(f"{k} {v}" for k, v in sorted(c.items(), key=str)) + " |"
    )
print()
print("### Every cell that matched Spark on main and is wrong on ff34 (outside the two fuzz sets)")
print()
print("| section | cell | ANSI | Spark | main | ff34 |")
print("|---|---|---|---|---|---|")
for r in rows:
    if r["transition"] == "broken-on-ff34":
        print(
            f"| {r['section']} | `{r['key']}` | {r['ansi']} | {r['spark'][:70]} | {r['main'][:70]} | {r['ff34'][:70]} |"
        )
print()
print("### Every cell fixed on ff34 (outside the two fuzz sets)")
print()
print("| section | cell | ANSI | Spark | main | ff34 |")
print("|---|---|---|---|---|---|")
for r in rows:
    if r["transition"] == "fixed-on-ff34":
        print(
            f"| {r['section']} | `{r['key']}` | {r['ansi']} | {r['spark'][:70]} | {r['main'][:70]} | {r['ff34'][:70]} |"
        )
print()
print("### Every wrong write and DML cell, per build")
print()
print("| section | cell | ANSI | Spark | main | ff34 |")
print("|---|---|---|---|---|---|")
for r in rows:
    if r["section"].startswith(("write", "revision-write", "dml")) and r["transition"] != "both-ok":
        print(
            f"| {r['section']} | `{r['key']}` | {r['ansi']} | {r['spark'][:60]} | "
            f"{r['main_verdict']}: {r['main'][:50]} | {r['ff34_verdict']}: {r['ff34'][:50]} |"
        )
print()
print("### The signed-zero shapes (sketch, Q5'), today and widened")
print()
print("| cell | ANSI | Spark | main | ff34 |")
print("|---|---|---|---|---|")
for r in rows:
    if "zero" in r["key"] and r["section"].startswith("revision"):
        print(
            f"| `{r['key']}` | {r['ansi']} | {r['spark'][:50]} | {r['main_verdict']} | {r['ff34_verdict']} |"
        )
print()
print("### Verdict categories per section and build")
print()
print("| section | ANSI | build | verdicts |")
print("|---|---|---|---|")
for key, c in summary["cells"].items():
    section, mode, build = key.split("|")
    if build == "transition":
        continue
    print(
        f"| {section} | {mode} | {build} | "
        + ", ".join(f"{k} {v}" for k, v in sorted(c.items()))
        + " |"
    )
wide_path = cells_dir / "widened-summary.json"
if wide_path.exists():
    wide = json.loads(wide_path.read_text())
    print()
    print("### The emulated head: the widened forms run on ff34baf5")
    print()
    print(
        "Faithfulness (Spark answers its widened forms as it answers the originals): "
        + json.dumps(wide["faithful"])
    )
    print()
    print(
        "| set | ANSI | family | cells | head wrong | head wrong, main right | head wrong, ff34 right | head right, both wrong |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for mode, fams in wide["coerce"].items():
        for fam, c in fams.items():
            print(
                f"| coercion fuzz | {mode} | {fam} | {c.get('cells', 0)} | {c.get('head wrong', 0)} | "
                f"{c.get('head wrong, main right', 0)} | {c.get('head wrong, ff34 right', 0)} | "
                f"{c.get('head right, both wrong', 0)} |"
            )
    for key, c in wide["family"].items():
        mode, pred = key.split("|")
        print(
            f"| `d {pred}` family | {mode} | | {c['cells']} | {c['head wrong']} | {c['head wrong, main right']} | "
            f"{c['head wrong, ff34 right']} | |"
        )
    for key, c in wide["dml"].items():
        mode, door = key.split("|")
        print(
            f"| DML, {door} door | {mode} | | {c.get('cells', 0)} | {c.get('head wrong', 0)} | "
            f"{c.get('head wrong, main right', 0)} | {c.get('head wrong, ff34 right', 0)} | "
            f"{c.get('head right, both wrong', 0)} |"
        )
    print()
    print(
        "| DECIMAL→DOUBLE kernel | "
        + " | ".join(sorted(next(iter(wide["decimal_to_double"].values()))))
        + " |"
    )
    print("|---|" + "---|" * len(next(iter(wide["decimal_to_double"].values()))))
    for build, c in wide["decimal_to_double"].items():
        print(f"| {build} wrong | " + " | ".join(c[k] for k in sorted(c)) + " |")
