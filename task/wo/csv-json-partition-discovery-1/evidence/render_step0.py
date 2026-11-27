from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path


def eq(sv: dict, ov: dict | None) -> bool:
    if ov is None:
        return False
    if sv.get("status") == "err" and ov.get("status") == "err" and not sv.get("condition"):
        return True
    return norm(sv) == norm(ov)


def norm(v: dict) -> str:
    if v.get("status") == "err":
        return "ERR " + str(v.get("condition") or v.get("type"))
    return json.dumps({k: x for k, x in v.items() if k not in ("message", "type")}, sort_keys=True)


def short(v: dict | None) -> str:
    if v is None:
        return "-"
    if v.get("status") == "err":
        return f"ERR {v.get('condition') or v.get('type')}: {v.get('message', '')[:90]}"
    if "schema" in v:
        sch = ",".join(f"{n}:{t}{'' if nl else '!'}" for n, t, nl in v["schema"])
        extra = f" eq={v['equal']}" if "equal" in v else ""
        return f"{sch} rows={len(v.get('rows', []))}{extra}"
    return json.dumps({k: x for k, x in v.items() if k != "status"})[:120]


KNOWN = ("/files_list/", "/glob_dirs/", "/glob_files/", "/basepath_", "/recursive/", "/stray/")
EXPLORE = ("/nullvalue/", "/infer_schema/")
SHARED_WHERE = ("date_vals", "escaped", "str_vals")


def klass(key: str, spark: dict, main_equal: bool) -> str:
    if main_equal:
        return "PROTECTED"
    if any(t in key for t in EXPLORE):
        return "EXPLORATORY"
    if any(t in key for t in KNOWN):
        return "KNOWN_DIVERGENCE"
    if key.startswith(("P/csvh/read/overlap/", "P/csvh/load/overlap/")):
        return "KNOWN_DIVERGENCE"
    if key.startswith(("C/csvh/", "C/csvn/")) and "/schema_" in key:
        return "KNOWN_DIVERGENCE"
    if key.startswith("C/json/") and "/schema_" in key and "/schema_without_p/" not in key:
        return "KNOWN_DIVERGENCE"
    if spark.get("status") == "err" and str(spark.get("condition") or "").startswith(
        "UNRESOLVED_COLUMN"
    ):
        return "KNOWN_DIVERGENCE"
    if any(f"/{name}/" in key for name in SHARED_WHERE) and key.endswith("/where_p2"):
        return "KNOWN_DIVERGENCE"
    if "/case_mixed/" in key and key.startswith("P/"):
        return "KNOWN_DIVERGENCE"
    return "TARGET"


def main() -> None:
    d = Path(sys.argv[1])
    engines = sys.argv[2:]
    data = {e: json.loads((d / f"res_{e}.json").read_text())["run1"] for e in ["spark", *engines]}
    spark = data["spark"]
    cells = {}
    for key in sorted(spark):
        main_equal = eq(spark[key], data["main"].get(key))
        row = {
            "class": klass(key, spark[key], main_equal),
            "spark": spark[key],
            "main": data["main"].get(key),
        }
        if "head" in data:
            row["head"] = data["head"].get(key)
            row["head_eq_spark"] = eq(spark[key], data["head"].get(key))
        row["main_eq_spark"] = main_equal
        cells[key] = row
    ov = d.parent / ("step0.head.json" if "head" in data else "step0.json")
    ov.write_text(json.dumps(cells, indent=1, sort_keys=True), encoding="utf-8")
    tally = Counter()
    for key, row in cells.items():
        group = "/".join(key.split("/")[:2])
        tally[(group, "n")] += 1
        tally[(group, "main")] += row["main_eq_spark"]
        if "head" in data:
            tally[(group, "head")] += row["head_eq_spark"]
            tally[(group, "regressed")] += row["main_eq_spark"] and not row["head_eq_spark"]
        tally[(group, row["class"])] += 1
    groups = sorted({g for g, _ in tally})
    lines = [
        "| group | cells | main = Spark | head = Spark | regressed | TARGET | PROTECTED "
        "| KNOWN_DIVERGENCE | EXPLORATORY |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for g in groups:
        lines.append(
            f"| `{g}` | {tally[(g, 'n')]} | {tally[(g, 'main')]} "
            f"| {tally[(g, 'head')] if 'head' in data else '-'} "
            f"| {tally[(g, 'regressed')] if 'head' in data else '-'} | {tally[(g, 'TARGET')]} "
            f"| {tally[(g, 'PROTECTED')]} | {tally[(g, 'KNOWN_DIVERGENCE')]} "
            f"| {tally[(g, 'EXPLORATORY')]} |"
        )
    tot = len(cells)
    tm = sum(r["main_eq_spark"] for r in cells.values())
    th = sum(r.get("head_eq_spark", False) for r in cells.values()) if "head" in data else "-"
    tr = (
        sum(r["main_eq_spark"] and not r.get("head_eq_spark", True) for r in cells.values())
        if "head" in data
        else "-"
    )
    by = Counter(r["class"] for r in cells.values())
    lines.append(
        f"| **total** | {tot} | {tm} | {th} | {tr} | {by['TARGET']} | {by['PROTECTED']} "
        f"| {by['KNOWN_DIVERGENCE']} | {by['EXPLORATORY']} |"
    )
    if "head" in data:
        tgt = [r for r in cells.values() if r["class"] == "TARGET"]
        prot = [r for r in cells.values() if r["class"] == "PROTECTED"]
        lines.append("")
        known = [r for r in cells.values() if r["class"] in ("KNOWN_DIVERGENCE", "EXPLORATORY")]
        same = sum(
            norm(r["head"] or {"status": "missing"}) == norm(r["main"] or {"status": "missing"})
            for r in known
        )
        lines.append(
            f"TARGET equal to Spark on head: {sum(r['head_eq_spark'] for r in tgt)} of {len(tgt)}; "
            f"PROTECTED equal on head: {sum(r['head_eq_spark'] for r in prot)} of {len(prot)}; "
            f"KNOWN_DIVERGENCE and EXPLORATORY answering as main: {same} of {len(known)}; "
            f"regressed: {tr}"
        )
    print("\n".join(lines))
    out = d / ("diff_head.txt" if "head" in data else "diff_main.txt")
    with out.open("w", encoding="utf-8") as fh:
        for key, row in cells.items():
            mark = row.get("head_eq_spark", row["main_eq_spark"])
            if not mark or not row["main_eq_spark"]:
                fh.write(
                    f"{key} [{row['class']}]\n  spark: {short(row['spark'])}\n"
                    f"  main : {short(row['main'])}\n"
                )
                if "head" in row:
                    fh.write(f"  head : {short(row['head'])}\n")


if __name__ == "__main__":
    main()
