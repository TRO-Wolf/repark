from __future__ import annotations

import argparse
import collections
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
CLONE = Path(os.environ["SZB_CLONE"])
PART_A = CLONE / "task/wo/signed-zero-normalize-1/evidence/probe_signed_zero.py"
ROUTES = ("spark-sql", "f-expr", "typed", "native")
TYPED_ONLY = {"GB-11"}


def load_part_a() -> Any:
    spec = importlib.util.spec_from_file_location("probe_sz", PART_A)
    module = importlib.util.module_from_spec(spec)
    sys.modules["probe_sz"] = module
    spec.loader.exec_module(module)
    return module


PA = load_part_a()


def read(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def short(record: dict[str, Any] | None, limit: int = 110) -> str:
    if record is None:
        return "(none)"
    if record["outcome"] != "ANSWERED":
        message = (record.get("error") or {}).get("message", "") if record.get("error") else ""
        return f"{record['outcome']}: {message[:limit]}"
    rows = record.get("rows") or []
    if not record.get("ordered"):
        rows = sorted(rows, key=lambda r: json.dumps(r, default=str))
    text = "; ".join(",".join("NULL" if v is None else str(v) for v in r) for r in rows)
    return (text or "(no rows)")[:limit]


def cell_md(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def label_all(raw: list[Path]) -> list[dict[str, Any]]:
    spark = []
    mine = []
    for path in raw:
        records = read(path)
        target = spark if path.name.startswith("spark-") else mine
        target += [r for r in records if r.get("record") == "cell"]
    index = {(r["cid"], r["route"], r["layout"]): r for r in spark}
    out = []
    for record in mine:
        spark_route = "spark-sql" if record["route"] == "native" else record["route"]
        truth = index.get((record["cid"], "spark-sql", record["truth_layout"]))
        same = index.get((record["cid"], spark_route, record["test_layout"]))
        if record["site"] in TYPED_ONLY and record["route"] != "spark-sql":
            truth = same
        label = "+".join(PA.label_of(record, truth, same))
        out.append({"record": record, "truth": truth, "same": same, "label": label})
    for record in spark:
        if record["site"] in TYPED_ONLY and record["route"] != "spark-sql":
            continue
        if record["layout"] == "one" and record["truth_layout"] == "view":
            truth = index.get((record["cid"], "spark-sql", "view"))
            answered = truth is not None and truth["outcome"] == record["outcome"] == "ANSWERED"
            if answered and not PA.same_rows(
                truth["rows"], record["rows"], bool(record.get("ordered"))
            ):
                out.append(
                    {
                        "record": record,
                        "truth": truth,
                        "same": None,
                        "label": "SPARK-ICEBERG-DIFFERS-FROM-VIEW",
                    }
                )
    return out


def site_family(record: dict[str, Any]) -> str:
    return record["group"]


def render(raw: list[Path], out_dir: Path, base: str, title: str) -> None:
    labelled = label_all(raw)
    lines: list[str] = []
    jsonl = []
    for item in labelled:
        r = item["record"]
        jsonl.append(
            {
                "cid": r["cid"],
                "group": r["group"],
                "site": r["site"],
                "seat": r["seat"],
                "type": r["type"],
                "build": r.get("build"),
                "kind": r.get("kind"),
                "route": r["route"],
                "engine": r["engine"],
                "label": item["label"],
                "text": r["text"],
                "repark": short(r, 400),
                "truth": short(item["truth"], 400),
                "spark_same_route": short(item["same"], 400),
            }
        )
    (out_dir / "labels.jsonl").write_text(
        "".join(json.dumps(x, sort_keys=True, ensure_ascii=False) + "\n" for x in jsonl),
        encoding="utf-8",
    )
    mine = [x for x in labelled if x["record"]["engine"] == "repark"]
    twin_index = {(x["record"]["cid"], x["record"]["route"]): x["label"] for x in mine}
    for x in mine:
        r = x["record"]
        if r["type"] in ("double", "float") and r["group"] != "floatkeys":
            twin = twin_index.get((r["cid"].replace("|" + r["type"], "|decimal", 1), r["route"]))
            x["twin"] = twin
            x["clean"] = twin is None or (
                twin in ("EQUAL", "BOTH-REFUSED")
                and not (twin == "BOTH-REFUSED" and r["route"] == "native")
            )
        else:
            x["twin"] = None
            x["clean"] = r["group"] == "floatkeys" or r["group"] == "nan-payload"
    for item, row in zip(labelled, jsonl, strict=True):
        row["decimal_twin"] = item.get("twin")
    (out_dir / "labels.jsonl").write_text(
        "".join(json.dumps(x, sort_keys=True, ensure_ascii=False) + "\n" for x in jsonl),
        encoding="utf-8",
    )
    oddities = [x for x in labelled if x["record"]["engine"] == "spark"]
    totals = collections.Counter(x["label"] for x in mine)
    lines.append("# SIGNED-ZERO-NORMALIZE-1 part B, " + title)
    lines.append("")
    lines.append(
        "Generated by `label_render.py` from `raw/`. RePark = main `"
        + base
        + "` release build (part A merged as "
        "`04d84ab9`); truth = Spark 4.1.2 SQL on inline `VALUES` views (reads) or "
        "Spark-on-Iceberg one-file "
        "tables (writes, partitions, floatkeys). Labels are part A's `label_of` "
        "(`evidence/probe_signed_zero.py`), "
        "unchanged. See [provenance.md](" + os.path.relpath(HERE / "provenance.md", out_dir) + ")."
    )
    lines.append("")
    lines.append(
        f"**RePark cells: {len(mine)}.** Totals by label: "
        + ", ".join(f"{k} {v}" for k, v in sorted(totals.items()))
        + "."
    )
    lines.append("")
    lines.append("## 1. Counts by label, per site group and door")
    lines.append("")
    groups = sorted({site_family(x["record"]) for x in mine})
    labels = sorted(totals)
    lines.append("| group | door | " + " | ".join(labels) + " | total |")
    lines.append("|---|---|" + "---|" * len(labels) + "---|")
    for group in groups:
        for route in ROUTES:
            sel = [
                x
                for x in mine
                if site_family(x["record"]) == group and x["record"]["route"] == route
            ]
            if not sel:
                continue
            counts = collections.Counter(x["label"] for x in sel)
            lines.append(
                f"| {group} | {route} | "
                + " | ".join(str(counts.get(lb, "")) for lb in labels)
                + f" | {len(sel)} |"
            )
    lines.append("")
    lines.append("## 2. Counts by label, per type (all doors)")
    lines.append("")
    lines.append("| type | " + " | ".join(labels) + " | total |")
    lines.append("|---|" + "---|" * len(labels) + "---|")
    for type_name in ("double", "float", "decimal"):
        sel = [x for x in mine if x["record"]["type"] == type_name]
        counts = collections.Counter(x["label"] for x in sel)
        lines.append(
            f"| {type_name} | "
            + " | ".join(str(counts.get(lb, "")) for lb in labels)
            + f" | {len(sel)} |"
        )
    lines.append("")
    lines.append("## 3. Per site: labels by door (type double / float / decimal)")
    lines.append("")
    lines.append("| site | seat | build | spark-sql | f-expr | typed | native |")
    lines.append("|---|---|---|---|---|---|---|")
    by_site: dict[tuple[str, str, str], dict[str, dict[str, str]]] = collections.defaultdict(
        lambda: collections.defaultdict(dict)
    )
    seats = {}
    order = []
    for x in mine:
        r = x["record"]
        key = (
            r["group"],
            r["site"],
            (r.get("build") or "") + ("/" + r["kind"] if r.get("kind") else ""),
        )
        if key not in by_site:
            order.append(key)
        by_site[key][r["route"]][r["type"]] = x["label"]
        seats[key] = r["seat"]
    abbrev = {
        "EQUAL": "EQ",
        "WRONG-READ": "WR",
        "WRONG-WRITE": "WW",
        "SIGN-ONLY": "SO",
        "ORDER-DIFFER": "OD",
        "BOTH-REFUSED": "BR",
        "ACCEPTED-WHERE-SPARK-REFUSED": "AWSR",
        "REFUSED-WHERE-SPARK-ACCEPTED": "RWSA",
        "NATIVE-REFUSED": "NR",
        "ORACLE-MISSING": "OM",
        "PANIC": "PANIC",
        "SETUP-REFUSED": "SR",
        "PRUNED-WRONG": "PW",
    }

    def ab(label: str) -> str:
        return "+".join(abbrev.get(p, p) for p in label.split("+"))

    for key in order:
        cols = []
        for route in ROUTES:
            per = by_site[key].get(route)
            if not per:
                cols.append("")
                continue
            cols.append(" / ".join(ab(per.get(t, "-")) for t in ("double", "float", "decimal")))
        lines.append(f"| {key[1]} | {cell_md(seats[key])} | {key[2]} | " + " | ".join(cols) + " |")
    lines.append("")
    lines.append(
        "Abbreviations: "
        + ", ".join(f"{v} = {k}" for k, v in abbrev.items())
        + ". A `-` is a type the site does not run; an empty door "
        "column is a door with no form for the site."
    )
    lines.append("")
    floats = [
        x
        for x in mine
        if x["record"]["type"] in ("double", "float") and x["record"]["group"] not in ("floatkeys",)
    ]
    clean = collections.Counter(x["label"] for x in floats if x["clean"])
    dirty = collections.Counter(
        x["label"] for x in floats if not x["clean"] and x["record"]["group"] != "nan-payload"
    )
    lines.append("## 3b. Float cells split by their decimal twin")
    lines.append("")
    lines.append(
        "A float cell is **signed-zero-attributable** when its decimal-control twin (same site, "
        "build, kind and door) is "
        "EQUAL: whatever the float cell shows is then the float key's doing. A float cell whose "
        "twin is not EQUAL is "
        "**confounded** by a non-signed-zero divergence on that door and is listed in section 4b. "
        "NaN-payload cells have no twin."
    )
    lines.append("")
    lines.append("| float cells | " + " | ".join(labels) + " | total |")
    lines.append("|---|" + "---|" * len(labels) + "---|")
    lines.append(
        "| attributable (twin EQUAL, or no twin) | "
        + " | ".join(str(clean.get(lb, "")) for lb in labels)
        + f" | {sum(clean.values())} |"
    )
    lines.append(
        "| confounded (twin not EQUAL) | "
        + " | ".join(str(dirty.get(lb, "")) for lb in labels)
        + f" | {sum(dirty.values())} |"
    )
    lines.append("")
    lines.append("## 4. Every non-EQUAL RePark cell")
    lines.append("")
    lines.append("| cid | door | label | decimal twin | truth | RePark main |")
    lines.append("|---|---|---|---|---|---|")
    for x in mine:
        if x["label"] == "EQUAL":
            continue
        r = x["record"]
        lines.append(
            f"| {r['cid']} | {r['route']} | {x['label']} | {x['twin'] or ''} | "
            f"{cell_md(short(x['truth']))} | {cell_md(short(r))} |"
        )
    lines.append("")
    lines.append("## 5. Oracle self-check: Spark-on-Iceberg against the Spark view")
    lines.append("")
    if oddities:
        lines.append("| cid | route | view | Iceberg one-file |")
        lines.append("|---|---|---|---|")
        for x in oddities:
            lines.append(
                f"| {x['record']['cid']} | {x['record']['route']} | {cell_md(short(x['truth']))} | "
                f"{cell_md(short(x['record']))} |"
            )
    else:
        lines.append(
            "No read cell differs between Spark's view and Spark's one-file Iceberg answer on any "
            "Spark route."
        )
    lines.append("")
    seeds = []
    for path in raw:
        for r in read(path):
            if r.get("record") == "seed":
                nan_bits = sorted(
                    {
                        b
                        for row in r.get("bits") or []
                        for b in row
                        if b
                        and b[:3] in ("7ff", "fff")
                        and b not in ("7ff0000000000000", "fff0000000000000")
                    }
                )
                seen = {"outcome": "ANSWERED", "rows": r.get("rows") or [], "ordered": True}
                shown = cell_md(short(seen, 200)) if r.get("rows") else r.get("error")
                seeds.append(f"| {path.name} | {r['tag']} | {shown} | {', '.join(nan_bits)} |")
    lines.append("## 6. Stored seeds as read back (sign kept?) and NaN bit patterns")
    lines.append("")
    lines.append("| leg | table | rows (id, then key columns) | NaN bits seen |")
    lines.append("|---|---|---|---|")
    lines += seeds
    lines.append("")
    extra = out_dir / "named-seats.md"
    if extra.exists():
        lines.append(extra.read_text(encoding="utf-8"))
    (out_dir / "table.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"repark cells {len(mine)}; " + ", ".join(f"{k} {v}" for k, v in sorted(totals.items())))
    print(f"spark oddities {len(oddities)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-file", action="append", type=Path)
    parser.add_argument("--out", type=Path, default=HERE)
    parser.add_argument("--base", default="ae247767")
    parser.add_argument("--title", default="step 0: the measured clause table (flat keys)")
    arguments = parser.parse_args()
    render(
        arguments.raw_file or sorted((HERE / "raw").glob("*.jsonl")),
        arguments.out,
        arguments.base,
        arguments.title,
    )
