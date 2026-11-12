from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from types import ModuleType
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE / "step0"
ITEM1 = HERE / "item1"
OUT = HERE.parents[3] / "python/repark/tests/sql/data/listview_inner_join_key_1_cells.jsonl"
HOW = {
    "inner": "inner",
    "left": "left",
    "right": "right",
    "full": "full",
    "semi": "left_semi",
    "anti": "left_anti",
    "left_semi": "left_semi",
}
STEP0_FORMS = ("df_str", "df_list", "df_two", "df_expr", "df_expr_and")
PRODUCED_VARIANTS = ("REE_I64", "REE_UTF8", "L_LV", "LL_LV", "FSL_LV", "STRUCT_REE", "L_REE")
VARIANT_FORMS = (("df_str", "inner"), ("df_list", "inner"), ("wt", "inner"), ("df_str", "left"))
PANDAS_ROUTES = ("LV_I64", "LLV_I64", "REE_I64", "UNION_I64")
CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")
USING_CARD = "LISTVIEW-SQL-USING-1"
LATERAL_CARD = "LATERAL-USING-ACCEPT-1"


def probe(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def leg(paths: list[Path]) -> dict[str, Any]:
    records: dict[str, Any] = {}
    for path in paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if "meta" not in row:
                records[row["cell_id"]] = row
    return records


def rows_expect(record: dict[str, Any]) -> dict[str, Any]:
    outcome = record["outcome"]
    if outcome["status"] != "ok":
        raise ValueError(f"{record['cell_id']} has no rows: {outcome}")
    return {"kind": "rows", "n": outcome["n"], "sha": outcome["sha"]}


def error_expect(record: dict[str, Any]) -> dict[str, Any]:
    outcome = record["outcome"]
    found = CLASS.search(outcome.get("error_msg", ""))
    if outcome["status"] == "ok" or found is None:
        raise ValueError(f"{record['cell_id']} has no error class: {outcome}")
    return {"kind": "error", "class": found.group(1)}


def cell(
    cell_id: str, views: dict[str, str], form: str, how: str, expect: dict[str, Any], oracle: str
) -> dict[str, Any]:
    return {
        "id": cell_id,
        "views": views,
        "form": form,
        "how": how,
        "expect": expect,
        "oracle": oracle,
    }


def step0_cells(spark: dict[str, Any], main: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for cell_id, record in spark.items():
        section, kind = cell_id.split("|")[:2]
        if section == "join" and record["form"] in (*STEP0_FORMS, "sql_using"):
            views = {"kind": kind, "ds": record["ds"]}
            how = HOW[record["jt"]]
            if record["form"] == "sql_using" and main[cell_id]["outcome"]["status"] != "ok":
                expect = {**error_expect(main[cell_id]), "card": USING_CARD}
                out.append(
                    cell(f"s0|{cell_id}", views, "sql_using", how, expect, f"main {cell_id}")
                )
                continue
            expect = rows_expect(record)
            out.append(
                cell(f"s0|{cell_id}", views, record["form"], how, expect, f"spark {cell_id}")
            )
        elif section in ("agg", "dml"):
            form = record["op"]
            views = {"kind": record["kind"], "ds": "base"}
            expect = rows_expect(record)
            out.append(cell(f"s0|{cell_id}", views, form, "inner", expect, f"spark {cell_id}"))
    return out


def variant_cells(spark: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for kind in (*PRODUCED_VARIANTS, "MAP_LV"):
        for form, how in VARIANT_FORMS:
            cell_id = f"variant|{kind}|{form}|{how}"
            record = spark[cell_id]
            expect = error_expect(record) if kind == "MAP_LV" else rows_expect(record)
            views = {"kind": kind, "ds": "base"}
            out.append(cell(f"i1|{cell_id}", views, form, how, expect, f"spark {cell_id}"))
    return out


def lateral_cells(step0: dict[str, Any], item1: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for cell_id, record in item1.items():
        if not cell_id.startswith("lateral|"):
            continue
        twin = f"join|{record['kind']}|df_str|{record['jt']}|base"
        expect = {**rows_expect(step0[twin]), "card": LATERAL_CARD}
        views = {"kind": record["kind"], "ds": "base"}
        form, how = record["form"], HOW[record["jt"]]
        out.append(cell(f"i1|{cell_id}", views, form, how, expect, f"spark {twin}"))
    return out


def route_cells(spark: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for kind in PANDAS_ROUTES:
        cell_id = f"route|pandas:{kind}|df_str|inner"
        views = {"route": "pandas", "kind": kind}
        expect = rows_expect(spark[cell_id])
        out.append(cell(f"i1|{cell_id}", views, "df_str", "inner", expect, f"spark {cell_id}"))
    return out


def mixed_cells(spark: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for cell_id, record in spark.items():
        if not cell_id.startswith("mixed|") or record["form"] != "join_str":
            continue
        left, right = record["kind"].split("x")
        right_kind, order = right.split("_")
        views = {"family": f"kL_{left}", "right": f"kR_{right_kind}_{order}"}
        how = HOW[record["jt"]]
        expect = rows_expect(record)
        out.append(cell(f"i1|{cell_id}", views, "join_str", how, expect, f"spark {cell_id}"))
    return out


def using_cells(spark: dict[str, Any], main: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for cell_id, record in spark.items():
        if not cell_id.startswith("using|"):
            continue
        views = {"kind": record["kind"], "ds": "base"}
        how = HOW[record["jt"]]
        if main[cell_id]["outcome"]["status"] != "ok":
            expect = {**error_expect(main[cell_id]), "card": USING_CARD}
            oracle = f"main {cell_id}"
        else:
            expect = rows_expect(record)
            oracle = f"spark {cell_id}"
        out.append(cell(f"i1|{cell_id}", views, record["form"], how, expect, oracle))
    return out


def route_values(item1_probe: ModuleType) -> dict[str, dict[str, list[Any]]]:
    table = {"REE_I64": item1_probe.SCALAR, "UNION_I64": item1_probe.SCALAR}
    sides = {"l": item1_probe.BASE_LEFT, "r": item1_probe.BASE_RIGHT}
    return {
        kind: {
            side: [table.get(kind, item1_probe.LOGICAL)[row[1]] for row in rows]
            for side, rows in sides.items()
        }
        for kind in PANDAS_ROUTES
    }


def reached_types(main: dict[str, Any], kinds: set[str]) -> dict[str, str]:
    reached: dict[str, str] = {}
    for record in main.values():
        section = record["cell_id"].split("|")[0]
        if section in ("failing", "variant", "lateral") and record["kind"] in kinds:
            reached.setdefault(record["kind"], record["reached"])
        elif section == "route" and record["kind"].startswith("pandas:"):
            kind = record["kind"].removeprefix("pandas:")
            if kind in PANDAS_ROUTES:
                reached.setdefault(f"pandas:{kind}", record["reached"])
    return reached


def family_value(item1_probe: ModuleType, kind: str, names: str, a: int, b: int) -> str:
    element = item1_probe.family_struct(names, a, b)
    return item1_probe.family_wrap(kind, element, names, "main")


def family_views(item1_probe: ModuleType) -> list[tuple[str, str]]:
    family = []
    for kind in item1_probe.FAMILY_KINDS:
        left = [(1, family_value(item1_probe, kind, "ab", 1, 2))]
        left.append((2, family_value(item1_probe, kind, "ab", 2, 1)))
        family.append((f"kL_{kind}", "lid", left))
        for order, names in (("rev", "ba"), ("same", "ab")):
            right = [(10, family_value(item1_probe, kind, names, 2, 1))]
            right.append((30, family_value(item1_probe, kind, names, 7, 7)))
            family.append((f"kR_{kind}_{order}", "rid", right))
    return [
        (name, " UNION ALL ".join(f"SELECT {i} AS {idcol}, {v} AS s" for i, v in rows))
        for name, idcol, rows in family
    ]


def header(
    item1_probe: ModuleType, step0_probe: ModuleType, kinds: set[str], main: dict[str, Any]
) -> dict[str, Any]:
    letters = {
        kind: {
            letter: item1_probe.sql_key(item1_probe.KINDS[kind], letter)
            for letter in item1_probe.LOGICAL
        }
        for kind in sorted(kinds)
    }
    datasets = {
        "base": [item1_probe.BASE_LEFT, item1_probe.BASE_RIGHT],
        "one": [step0_probe.ONE_LEFT, step0_probe.ONE_RIGHT],
        "big": [step0_probe.BIG_LEFT, step0_probe.BIG_RIGHT],
    }
    views = family_views(item1_probe)
    return {
        "letters": letters,
        "datasets": datasets,
        "family": views,
        "routes": route_values(item1_probe),
        "reached": reached_types(main, kinds),
    }


def main() -> None:
    item1_probe = probe(ITEM1 / "probe_item1.py", "probe_item1")
    step0_probe = probe(STEP0 / "probe_listview_table.py", "probe_listview_table")
    step0_spark = leg([STEP0 / "raw" / "spark.jsonl"])
    step0_main = leg([STEP0 / "raw" / "main.jsonl"])
    item1_spark = leg(sorted((ITEM1 / "raw" / "spark").glob("*.jsonl")))
    item1_main = leg(sorted((ITEM1 / "raw" / "main").glob("*.jsonl")))
    cells = [
        *step0_cells(step0_spark, step0_main),
        *variant_cells(item1_spark),
        *lateral_cells(step0_spark, item1_spark),
        *route_cells(item1_spark),
        *mixed_cells(item1_spark),
        *using_cells(item1_spark, item1_main),
    ]
    kinds = {cell["views"]["kind"] for cell in cells if "ds" in cell["views"]}
    lines = [json.dumps(header(item1_probe, step0_probe, kinds, item1_main), sort_keys=True)]
    lines += [json.dumps(row, sort_keys=True) for row in cells]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {len(cells)} cells to {OUT.name}")


if __name__ == "__main__":
    main()
