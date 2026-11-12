import argparse
import collections
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

DE = Path(__file__).resolve().parent.parent
FE = DE.parent.parent / "followup-dml-conjunct-and-signed-zero/evidence"


def load_module(name: Any, path: Any) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def read_jsonl(path: Any) -> Any:
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def domain_labels(files: Any, spark_files: Any = None) -> Any:
    probe = load_module("probe_sz", DE / "probe_signed_zero.py")
    spark = []
    for f in spark_files or [
        DE / "raw/spark-signed-zero-reads.jsonl",
        DE / "raw/spark-signed-zero-dml.jsonl",
    ]:
        spark += read_jsonl(f)
    mine = []
    for f in files:
        mine += read_jsonl(f)
    for record in spark + mine:
        if record.get("record") == "cell":
            probe.unordered_cell(record)
    index = {}
    for record in spark:
        if record.get("record") == "cell":
            index[(record["cid"], record["route"], record["layout"], record["engine"])] = record
    out = {}
    for record in mine:
        if record.get("record") != "cell" or record["layout"] != record["test_layout"]:
            continue
        spark_route = "spark-sql" if record["route"] == "native" else record["route"]
        truth = index.get((record["cid"], "spark-sql", record["truth_layout"], "spark"))
        if record["family"] == "read" and record["truth_layout"] == "none":
            truth = index.get((record["cid"], "spark-sql", "none", "spark"))
        same = index.get((record["cid"], spark_route, record["test_layout"], "spark"))
        key = "|".join((record["cid"], record["route"], record["layout"]))
        out[key] = {
            "group": record["group"],
            "label": "+".join(probe.label_of(record, truth, same)),
            "outcome": record["outcome"],
            "rows": sorted(json.dumps(r, default=str) for r in (record.get("rows") or [])),
            "cols": record.get("cols"),
            "error": str(record.get("error") or record.get("stmt_error") or "")[:160],
            "text": record.get("text"),
        }
    return out


def tableb_labels(files: Any, spark_files: Any = None) -> Any:
    probe = load_module("probe_fu", FE / "probe_followup.py")
    _, spark = probe.load_many([str(FE / "raw/spark-signed-zero.jsonl")])
    _, mine = probe.load_many([str(f) for f in files])
    out = {}
    for key, record in mine.items():
        out[key] = {
            "group": record.get("op"),
            "label": "+".join(probe.sz_labels(record, spark.get(probe.oracle_key(key)))),
            "outcome": record["outcome"],
            "rows": sorted(json.dumps(r, default=str) for r in (record.get("rows") or [])),
            "cols": None,
            "error": str(record.get("error") or "")[:160],
            "text": record.get("statement"),
        }
    return out


def main() -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=("domain", "tableb"), required=True)
    parser.add_argument("--old", nargs="+", required=True)
    parser.add_argument("--new", nargs="+", required=True)
    parser.add_argument("--groups", nargs="*")
    parser.add_argument("--out")
    parser.add_argument("--spark", nargs="*")
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args()
    fn = domain_labels if args.kind == "domain" else tableb_labels
    old = fn(args.old, args.spark)
    new = fn(args.new, args.spark)
    moves = []
    counts = collections.Counter()
    for key in sorted(set(old) | set(new)):
        a = old.get(key)
        b = new.get(key)
        group = (a or b)["group"]
        if args.groups and group not in args.groups:
            continue
        la = a["label"] if a else "ABSENT"
        lb = b["label"] if b else "ABSENT"
        counts[(group, la, lb)] += 1
        changed = (
            a is None
            or b is None
            or la != lb
            or a["rows"] != b["rows"]
            or a["outcome"] != b["outcome"]
            or a["cols"] != b["cols"]
        )
        if changed:
            moves.append({"key": key, "old": a, "new": b, "old_label": la, "new_label": lb})
    for (group, la, lb), n in sorted(counts.items()):
        flag = "" if la == lb else "  MOVE"
        print(f"{group}\t{la}\t->\t{lb}\t{n}{flag}")
    if args.show:
        for key in sorted(new):
            b = new[key]
            if args.groups and b["group"] not in args.groups:
                continue
            print(
                f"{key}\t{b['label']}\t{b['outcome']}\t"
                f"{'; '.join(b['rows'])[:70]}\t{b['error'][:70]}"
            )
    print(f"changed cells (label, rows, outcome or cols): {len(moves)}")
    if args.out:
        Path(args.out).write_text(json.dumps(moves, indent=1, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
