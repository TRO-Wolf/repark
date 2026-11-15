import json
import sys
from collections import Counter
from pathlib import Path

CELLS = Path(sys.argv[1])
summary = json.loads((CELLS / "summary.json").read_text())
head = json.loads((CELLS / "head-summary.json").read_text())
rows = [json.loads(x) for x in (CELLS / "cells.jsonl").read_text().splitlines()]
moved = [json.loads(x) for x in (CELLS / "moved-since-6f9f.jsonl").read_text().splitlines()]
wrong = [json.loads(x) for x in (CELLS / "head-wrong.jsonl").read_text().splitlines()]
out = []
w = out.append
w("# DECIMAL-DOUBLE-UNION-COERCION-1 step 0′ on main `dc6b4c94` — the table")
w("")
w("Rendered by `probes/render_step0b.py` from [cells/](cells/map.md). Spark 4.1.2 on Zulu JDK 17 against main")
w("`dc6b4c94`, both ANSI modes. Every joined cell also carries the recorded `ff34baf5` answer and the step-0 main")
w("(`6f9f2aaa`) answer. The provenance is in [map.md](map.md).")
w("")
w("## 1. The joined cells per section")
w("")
w("| section | ANSI | cells | wrong on main `dc6b4c94` | wrong on `ff34baf5` (recorded) | wrong on `6f9f2aaa` (recorded) | moved since `6f9f2aaa` |")
w("|---|---|---|---|---|---|---|")
for k, v in summary.items():
    if k.startswith("wrong on main"):
        continue
    sec, mode = k.split("|")
    w(f"| {sec} | {mode} | {v.get('cells', 0)} | {v.get('main wrong', 0)} | {v.get('ff34 wrong', 0)} | {v.get('6f9f wrong', 0)} | {v.get('moved since 6f9f', 0)} |")
w("")
w("## 2. Wrong on main, by part")
w("")
parts = sorted({p for k, v in summary.items() if k.startswith("wrong on main by part|") for p in v})
w("| part | " + " | ".join(m for m in ("true", "false", "native")) + " |")
w("|---|---|---|---|")
for p in parts:
    w(f"| {p} | " + " | ".join(str(summary.get(f'wrong on main by part|{m}', {}).get(p, 0)) for m in ("true", "false", "native")) + " |")
w("")
w("Per section (ANSI on):")
w("")
w("| part | sections (wrong on main) |")
w("|---|---|")
for k, v in summary.items():
    if k.startswith("wrong on main by part and section|true|"):
        w(f"| {k.split('|', 2)[2]} | " + ", ".join(f"{s} {n}" for s, n in sorted(v.items())) + " |")
w("")
w("Part 1 counts the cells whose unifying context seat E widens (comparisons, `IN`, `BETWEEN`, set operations without a")
w("`VALUES` input, CASE / `IF` / `coalesce` / `nvl2` / `nullif` / `greatest` / `least` / `array` / `map`, the DML")
w("predicates and the writes they feed) and the written-cast flips. Part 2 counts every cell with a `VALUES` input.")
w("The joined cells are classified by context; the fuzz, structured and DML sets below are measured on the emulated head.")
w("")
w("## 3. Moved since `6f9f2aaa`")
w("")
w("| section | cell | ANSI | `6f9f2aaa` | `dc6b4c94` | Spark |")
w("|---|---|---|---|---|---|")
for r in moved:
    w(f"| {r['section']} | `{r['key']}` | {r['ansi']} | {r['main_6f9f']} ({r['main_6f9f_verdict']}) | {r['main']} ({r['main_verdict']}) | {r['spark']} |")
w("")
w("## 4. The signed-zero shapes on main (critic item 7)")
w("")
w("| cell | ANSI | Spark | main | verdict |")
w("|---|---|---|---|---|")
for r in rows:
    if r["key"].startswith("select/zero_") or r["key"].startswith("delete_zero") or r["key"].startswith("merge_on_zero"):
        w(f"| `{r['key']}` | {r['ansi']} | {r['spark']} | {r['main']} | {r['main_verdict']} |")
w("")
w("## 5. The emulated head on verifier 2's coercion fuzz")
w("")
w("Variants, each run on main's package: `seat_e` casts every DECIMAL operand to DOUBLE by hand (the head that gave N2's")
w("2,761 on `ff34baf5`); `k1` adds K1 on the written casts (the double's JDK 17 text from main, rounded half up at the")
w("scale outside SQL, then fed back as exact digits); `k1k2` also routes every DECIMAL→DOUBLE through its text (K2).")
w("A held cell is wrong on the head and right on main (or, N2's definition, right on main or on `ff34baf5`).")
w("")
w("| ANSI | cells | main wrong | variant | head wrong | held vs main | held vs main or `ff34` | fixed vs main |")
w("|---|---|---|---|---|---|---|---|")
for mode in ("true", "false"):
    a = head["coerce"][mode]["all/all"]
    w(f"| {mode} | {a['cells']} | {a['main wrong']} | `ff34baf5` seat E (recorded run, re-joined) | {a['old emulated head wrong']} | - | {a['old emulated head held (right on main or ff34)']} | - |")
    for v in ("seat_e", "k1", "k1k2"):
        w(f"| {mode} | {a['cells']} | {a['main wrong']} | {v} on main | {a[f'{v} wrong']} | {a[f'{v} held vs main']} | {a[f'{v} held vs main or ff34']} | {a[f'{v} fixed vs main']} |")
w("")
w("Held cells by shape family (ANSI on):")
w("")
w("| family | cells | main wrong | seat_e held | k1 held | k1k2 held | k1k2 wrong |")
w("|---|---|---|---|---|---|---|")
for fam, c in head["coerce"]["true"].items():
    w(f"| {fam} | {c['cells']} | {c['main wrong']} | {c['seat_e held vs main']} | {c['k1 held vs main']} | {c['k1k2 held vs main']} | {c['k1k2 wrong']} |")
w("")
w("The `k1k2` wrong cells with ANSI on are the six `DECIMAL(5,0)` shapes that raise as a whole on both engines with")
w("different texts. With ANSI off, every `DECIMAL(5,0)` shape is wrong on main, on `ff34baf5` and on every head: a")
w("non-ANSI `CAST(double AS DECIMAL(5,0))` that overflows raises (`Arrow error: Invalid argument … too large to store`)")
w("where Spark returns NULL for the row, so the shape fails as a whole. That root is outside the unit's seats.")
w("")
w("## 6. The kernels (5,455 values per type)")
w("")
w("| ANSI | kernel | type | main's cast vs Spark | Spark's text route vs Spark | main's SQL text route vs Spark | main's half-up route vs Spark |")
w("|---|---|---|---|---|---|---|")
for k, v in head["kernels"].items():
    mode, kern, dec = k.split("|")
    w(f"| {mode} | {kern} | {dec} | {v['main direct cast differs from spark']} | {v['spark text route differs from spark']} | {v['main text route differs from spark']} | {v.get('main python half-up route differs from spark', '-')} |")
w("")
w("`k1` is DOUBLE→DECIMAL, `k1f` FLOAT→DECIMAL (through the widened double), `k2` DECIMAL→DOUBLE. RePark's SQL text")
w("route for K1 fails on the 332 (DOUBLE) and 329 (FLOAT) values whose JDK 17 text is in E-notation: main's")
w("`TRY_CAST('1.0E-4' AS DECIMAL(10,2))` is NULL where Spark's is `0.00`. That is a separate STRING→DECIMAL gap, so the")
w("emulation uses the half-up route, which matches Spark on every value.")
w("")
w("## 7. The structured `d = v` set (60,000 rows)")
w("")
w("| ANSI | variant | predicate | main wrong | head wrong | held vs main |")
w("|---|---|---|---|---|---|")
for k, v in head["family"].items():
    mode, var, p = k.split("|")
    w(f"| {mode} | {var} | {p} | {v['main wrong']} | {v['head wrong']} | {v['held vs main']} |")
w("")
w("## 8. The DML cells on the emulated head")
w("")
w("| ANSI | door | variant | cells | main wrong | head wrong | held vs main | fixed vs main |")
w("|---|---|---|---|---|---|---|---|")
for k, v in head["dml"].items():
    mode, door, var = k.split("|")
    w(f"| {mode} | {door} | {var} | {v['cells']} | {v['main wrong']} | {v['head wrong']} | {v['held vs main']} | {v['fixed vs main']} |")
w("")
w("The native-door rows are for information: part 1 does not widen that door (Q2), so its head is main.")
w("")
w("Wrong on the Spark-door `k1k2` head:")
w("")
w("| ANSI | cell | Spark | main | head |")
w("|---|---|---|---|---|")
for r in wrong:
    if r["set"] == "dml-repark" and r["variant"] == "k1k2":
        w(f"| {r['ansi']} | `{r['key']}` | {r['spark']} | {r['main']} | {r['head']} |")
w("")
w("Held on the `seat_e` head (Spark door), which K1 and K2 remove:")
w("")
w("| ANSI | cell | Spark | main | head |")
w("|---|---|---|---|---|")
for r in wrong:
    if r["set"] == "dml-repark" and r["variant"] == "seat_e" and r["main"] == r["spark"]:
        w(f"| {r['ansi']} | `{r['key']}` | {r['spark']} | {r['main']} | {r['head']} |")
w("")
w("## 9. Spark's widened forms")
w("")
w("| set | variant | ANSI | cells | Spark answers the widened form differently |")
w("|---|---|---|---|---|")
for k, v in head["faithful"].items():
    s, var, mode = k.split("|")
    d = v["spark_form_differs"]
    w(f"| {s} | {var} | {mode} | {v['cells']} | {len(d) if isinstance(d, list) else d} |")
w("")
w("The one fuzz cell is `eq_round_d5_0` / `eqf_round_d5_0` with ANSI on: a whole-shape overflow whose message names a")
w("different first value on each Spark run (`local[2]` partition order). Spark's fresh fuzz run differs from its")
w("2026-11-14 recording in that cell only.")
w("")
w("Up: [map.md](map.md).")
print("\n".join(out))
