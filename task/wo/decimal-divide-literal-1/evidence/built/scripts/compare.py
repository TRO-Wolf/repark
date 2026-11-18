import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "probes"))
sys.argv = [sys.argv[0], "/dev/null", "/dev/null"] + sys.argv[1:]
src = {}
exec(compile(Path(Path(__file__).parent / "probes" / "analyze.py").read_text().split("verdicts = []")[0], "analyze", "exec"), src)
classify, renorm = src["classify"], src["renorm"]

main_dir, head_dir, out = Path(sys.argv[3]), Path(sys.argv[4]), Path(sys.argv[5])


def load(path):
    if not path.exists():
        return {}
    return {r["key"]: r for r in map(json.loads, path.open())}


runs = []
for ansi in ("true", "false"):
    for conf in ("base", "apl_false", "pmp_false"):
        runs.append((ansi, conf, f"spark-{ansi}-{conf}.jsonl", f"repark-{ansi}-{conf}.jsonl", ""))
    runs.append((ansi, "native", f"spark-{ansi}-base.jsonl", "native.jsonl", ""))
    runs.append((ansi, "rev", f"spark-{ansi}-base.jsonl", f"repark-{ansi}-base.jsonl", "rev/"))
    runs.append((ansi, "rev-native", f"spark-{ansi}-base.jsonl", "native.jsonl", "rev/"))
rows = []
for ansi, conf, sname, rname, sub in runs:
    sp_all = load(main_dir / sub / sname)
    mp_all = load(main_dir / sub / rname)
    hp_all = load(head_dir / sub / rname)
    if not sp_all or not hp_all:
        print("skip", ansi, conf, bool(sp_all), bool(mp_all), bool(hp_all))
        continue
    for key, sp in sp_all.items():
        if key == "provenance" or key.startswith(("data/", "setup/", "conf/")):
            continue
        mp, hp = mp_all.get(key), hp_all.get(key)
        if mp is None or hp is None:
            continue
        spn = dict(sp, result=renorm(sp.get("result")))
        door = "native" if "native" in conf else sp.get("door")
        vm = classify(spn, dict(mp, result=renorm(mp.get("result")), door=sp.get("door")))
        vh = classify(spn, dict(hp, result=renorm(hp.get("result")), door=sp.get("door")))
        same = mp.get("result") == hp.get("result") and mp.get("statement") == hp.get("statement")
        rows.append({"key": key, "ansi": ansi, "conf": conf, "family": sp.get("family"), "door": door,
                     "pos": sp.get("pos"), "col": sp.get("col"), "main": vm, "head": vh, "same_as_main": same,
                     "head_result": hp.get("result") if vh != "EQUAL" else None,
                     "spark_result": sp.get("result") if vh != "EQUAL" else None})
with out.open("w") as fh:
    for r in rows:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
t = Counter((r["main"], r["head"]) for r in rows)
print("cells", len(rows))
for (a, b), n in sorted(t.items()):
    print(f"  {a:>14} -> {b:<14} {n}")
