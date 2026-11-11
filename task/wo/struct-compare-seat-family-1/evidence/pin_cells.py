from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE / "step0"
NAMES = HERE / "names" / "raw"
DELTA = HERE / "delta"
DATA = HERE.parents[3] / "python/repark/tests/sql/data"
OUT = DATA / "struct_compare_seat_family_cells.jsonl"
SHOWN = ("right", "full")
CARDS = [
    ("spark", "Unsupported natural join type", "NATURAL-EXISTENCE-JOIN-ACCEPT-1"),
    ("spark", "has no attribute '_jc'", "LATERAL-USING-ACCEPT-1"),
    ("spark", "INVALID_ORDERING_TYPE", "MAP-ORDERING-FORMS-1"),
    ("head", "Unsupported CAST from Struct", "STRUCT-CASE-NAME-CAST-1"),
    ("head", "Converting to Python dictionary", "FACADE-DUP-FIELD-ROW-1"),
    ("head", "No field named", "STRUCT-TYPED-FORM-SCHEMA-1"),
    ("head", "column types must match schema types", "FACADE-INGEST-ARROW-NORMALIZE-1"),
    ("head", "UNRESOLVED_COLUMN", "STRUCT-SETOP-LISTVIEW-COERCION-1"),
    ("head", "Cannot infer common argument type", "STRUCT-SETOP-LISTVIEW-COERCION-1"),
    ("head", "INCOMPATIBLE_COLUMN_TYPE", "STRUCT-SETOP-BY-POSITION-1"),
]
CLASS_CARDS = [
    ("spark", "Unsupported natural join type", None, "NATURAL-EXISTENCE-JOIN-ACCEPT-1"),
    ("spark", "has no attribute '_jc'", None, "LATERAL-USING-ACCEPT-1"),
    ("head", "No field named", None, "STRUCT-TYPED-FORM-SCHEMA-1"),
    ("head", "is not comparable", None, "STRUCT-COMPARE-PLANNER-CLASS-1"),
    ("head", "SET_OPERATION_ON_MAP_TYPE", "set-op control", "STRUCT-SETOP-CONTROL-CLASS-1"),
    ("head", "Cannot infer common argument", "set-op control", "STRUCT-SETOP-CONTROL-CLASS-1"),
    ("head", "Incompatible inputs for Union", "set-op control", "STRUCT-SETOP-CONTROL-CLASS-1"),
    ("head", "Cannot infer common argument", "key-list join", "USING-SCALAR-STRUCT-KEY-CLASS-1"),
    ("head", "Cannot infer common argument", None, "STRUCT-COMPARE-PLANNER-CLASS-1"),
]
EXCLUDED = {"Q2059": "LISTVIEW-INNER-JOIN-KEY-1", "Q2063": "LISTVIEW-INNER-JOIN-KEY-1"}
RESOLVED = {"Q1213", "Q1214", "Q1215", "Q1216", "Q1217"}
RESOLVE = re.compile(r'Cannot resolve "([^"]*)"')
ROW_CONVERSION = "Converting to Python dictionary"


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


def dropped(cell: dict[str, Any]) -> list[str]:
    if cell["jt"] in SHOWN and cell["proj"] in ("key", "star"):
        return ["k" if cell["shape"] == "INT" else "s"]
    return []


def projected(record: dict[str, Any], columns: list[str]) -> list[Any]:
    index = [record["columns"].index(name) for name in columns]
    return sorted(([row[i] for i in index] for row in record["rows"]), key=json.dumps)


def card(label: str, spark: dict[str, Any], head: dict[str, Any], cell_id: str) -> str:
    if label == "WRONG-ANSWER" and head["status"] == "ok":
        raise SystemExit(f"{cell_id}: a wrong answer cannot be known")
    source = spark if label == "ACCEPTED-WHERE-SPARK-REFUSED" else head
    side = "spark" if label == "ACCEPTED-WHERE-SPARK-REFUSED" else "head"
    for where, needle, name in CARDS:
        if where == side and needle in source.get("text", ""):
            return name
    raise SystemExit(f"{cell_id}: {label} has no card: {source.get('text', '')[:120]}")


def class_card(cell: dict[str, Any], spark: dict[str, Any], head: dict[str, Any]) -> str:
    if f"[{spark['class']}]" in head.get("text", ""):
        return ""
    for where, needle, seat, name in CLASS_CARDS:
        source = spark if where == "spark" else head
        if needle in source.get("text", "") and seat in (None, cell["seat"]):
            return name
    raise SystemExit(f"{cell['id']}: refuses without Spark's class: {head.get('text', '')[:120]}")


def query_line(cell: dict[str, Any], spark: dict[str, Any], head: dict[str, Any]) -> str:
    columns = [name for name in spark.get("columns", []) if name not in dropped(cell)]
    if spark["status"] == "ok":
        expect = {"kind": "rows", "columns": columns, "rows": projected(spark, columns)}
    else:
        expect = {"kind": "error", "class": spark["class"]}
    if cell["id"] in RESOLVED:
        expect["resolve"] = RESOLVE.search(spark["text"]).group(1)
    matched = head["status"] == "ok" and spark["status"] == "ok"
    matched = matched and set(columns) <= set(head["columns"])
    matched = matched and projected(head, columns) == expect["rows"]
    refused = head["status"] != "ok" and spark["status"] != "ok"
    refused = refused and ROW_CONVERSION not in head.get("text", "")
    known = class_card(cell, spark, head) if refused else ""
    if known:
        expect["gap"] = True
    if not (matched or refused):
        label = "WRONG-ANSWER" if head["status"] == spark["status"] else ""
        label = label or ("ACCEPTED-WHERE-SPARK-REFUSED" if head["status"] == "ok" else "REFUSED")
        known = card(label, spark, head, cell["id"])
    keys = ("id", "group", "seat", "door", "detail", "jt", "proj", "shape", "case")
    return json.dumps(
        {
            **{name: cell[name] for name in keys},
            "prepare": cell["prepare"],
            "op": cell["op"],
            "expect": expect,
            "known": known,
        },
        sort_keys=True,
    )


def dml_line(pf: Any, cell: dict[str, Any], spark: dict[str, Any], head: dict[str, Any]) -> str:
    oracle, kind = pf.dml_oracle(spark, cell)
    label = pf.dml_label(spark, head, cell)
    known = "" if label.endswith("EQUAL") else label
    keys = ("id", "group", "seat", "setup", "stmt", "table")
    return json.dumps(
        {
            **{name: cell.get(name) for name in keys},
            "kind": "dml",
            "prepare": bool(cell.get("prepare")),
            "expect": {"after": sorted(oracle, key=json.dumps), "oracle": kind},
            "known": known,
        },
        sort_keys=True,
    )


def name_cells() -> list[dict[str, Any]]:
    spark, base, head = (load(NAMES / f"{leg}.jsonl") for leg in ("spark", "base", "head"))
    cells = []
    for cell_id, record in sorted(spark.items()):
        if record["status"] != "ok" or head[cell_id]["status"] != "ok":
            raise SystemExit(f"{cell_id}: a name cell must plan on Spark and on head")
        exact = base[cell_id].get("columns") == record["columns"]
        keys = ("id", "form", "door", "text")
        cells.append(
            {**{name: record[name] for name in keys}, "spark": record["columns"], "exact": exact}
        )
    return cells


def main() -> None:
    sys.path.insert(0, str(STEP0))
    import probe_family as pf
    import probe_family_add as pa

    sys.path.insert(0, str(NAMES.parent))
    import probe_names as pn

    sys.path.insert(0, str(DELTA))
    import probe_delta as pd

    after = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "after" / "raw"
    delta = Path(sys.argv[2]) if len(sys.argv) > 2 else DELTA / "raw"
    spark, head = leg(STEP0 / "raw", "spark"), leg(after, "head")
    spark.update(load(DELTA / "raw" / "spark.jsonl"))
    head.update(load(delta / "head.jsonl"))
    added = [(name, pf.view_sql(name, columns, rows)) for name, columns, rows in pa.ADD_FIXTURES]
    views = pf.view_statements("head") + added + pd.own_views()
    dup = spark[pd.DUP_ID]
    header = {
        "views": views,
        "names": name_cells(),
        "names_view": pn.VIEW,
        "dup": {"id": pd.DUP_ID, "text": pd.DUP_TEXT, "rows": projected(dup, dup["columns"])},
    }
    lines = [json.dumps(header, sort_keys=True)]
    for cell in pf.gen_query_cells() + pa.gen_add_cells() + pd.CELLS:
        if cell["id"] in EXCLUDED:
            continue
        lines.append(query_line(cell, spark[cell["id"]], head[cell["id"]]))
    for cell in pf.gen_dml_cells() + pa.gen_add_dml_cells():
        if pf.dml_oracle(spark[cell["id"]], cell)[0] is not None:
            lines.append(dml_line(pf, cell, spark[cell["id"]], head[cell["id"]]))
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {len(lines) - 1} cells to {OUT.name}")


if __name__ == "__main__":
    main()
