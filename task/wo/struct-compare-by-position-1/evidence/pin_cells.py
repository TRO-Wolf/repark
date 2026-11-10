from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE / "step0"
DATA = HERE.parents[3] / "python/repark/tests/sql/data"
OUT = DATA / "struct_compare_by_position_cells.jsonl"
KIND_OUT = DATA / "struct_compare_kind_cells.jsonl"
VARIANTS = {
    "seatc": (HERE / "variant-seat-c", ("P10",), {"L": {"a": 1, "b": 2}, "R": {"a": 4, "b": 3}}),
    "ext": (HERE / "variant-ext", ("P30", "P31", "P32", "P33", "P34", "P35"), None),
}
EXT_VALUES = {"L": {"a": 1, "b": 2}, "R": {"a": 1, "b": 2}}
VIEWS = ("view", "largeview")
SEAT_SQL = {
    "eq": "SELECT l.s{path} = r.s{path} AS x FROM l CROSS JOIN r",
    "in": "SELECT l.s{path} IN (r.s{path}) AS x FROM l CROSS JOIN r",
    "join": "SELECT l.s{path} AS x FROM l JOIN r ON l.s{path} = r.s{path}",
}
FIXTURE_KEYS = ("W", "T1", "L", "R", "U")


def records(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    plain: dict[str, Any] = {}
    derived: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        record = json.loads(line)
        (derived if "derived" in record else plain)[record["key"]] = record
    return plain, derived


def after(rows: list[list[Any]]) -> list[list[Any]]:
    return sorted(rows, key=json.dumps)


def expected(spark: dict[str, Any], derived: dict[str, Any] | None) -> dict[str, Any]:
    if derived is not None:
        return {"kind": "derived", "after": after(derived["derived"]["expected_rows"])}
    if spark["kind"] == "setup" or (
        spark["kind"] == "error" and spark["answer"]["class"] == "IllegalArgumentException"
    ):
        return {"kind": "oracle-missing"}
    out: dict[str, Any] = {"kind": spark["kind"]}
    if spark["kind"] == "rows":
        out["rows"] = spark["answer"]["rows"]
    elif spark["kind"] == "error":
        out["class"] = spark["answer"]["class"]
    if "post_rows" in spark:
        out["after"] = after(spark["post_rows"])
    return out


def raw(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def card(cell: dict[str, Any]) -> str:
    kinds = (cell["L"]["kind"], cell["R"]["kind"])
    if "dict" in (cell["L"]["enc"], cell["R"]["enc"]):
        return "FACADE-INGEST-ARROW-NORMALIZE-1"
    if kinds[0] != kinds[1] and (kinds[0] in VIEWS or kinds[1] in VIEWS):
        return "STRUCT-SETOP-LISTVIEW-COERCION-1"
    if sorted(kinds) == ["fsl1", "fsl2"]:
        return "STRUCT-FSL-SIZE-COMPARE-1"
    return ""


def kind_cells() -> list[str]:
    lines = []
    for table, (root, plans, values) in VARIANTS.items():
        oracle: dict[str, Any] = {}
        for record in raw(root / "raw" / "spark.jsonl"):
            oracle.setdefault(record["skey"], record)
        for cell in raw(root / "raw" / "base.jsonl"):
            if cell["plan"] not in plans:
                continue
            spark = oracle[cell["skey"]]
            path = "['k']" if cell["shape"] == "map" else ""
            reason = card(cell)
            expect = (
                {"kind": "refused", "card": reason}
                if reason
                else {"kind": "rows", "rows": spark["rows"]}
                if spark["answered"]
                else {"kind": "refused", "card": ""}
            )
            lines.append(
                json.dumps(
                    {
                        "cell": cell["cell"],
                        "table": table,
                        **{name: cell[name] for name in ("cop", "door", "payload", "shape")},
                        "L": cell["L"],
                        "R": cell["R"],
                        "values": values or EXT_VALUES,
                        "stmt": SEAT_SQL[cell["cop"]].format(path=path),
                        "expect": expect,
                    },
                    sort_keys=True,
                )
            )
    return lines


def main() -> None:
    sys.path.insert(0, str(STEP0))
    import pass2_cmp

    cells = [json.loads(line) for line in (STEP0 / "cells.jsonl").read_text().splitlines()]
    spark, derived = records(STEP0 / "raw" / "spark.jsonl")
    lines = [
        json.dumps(
            {"fixtures": {key: list(pass2_cmp.FIXTURES[key]) for key in FIXTURE_KEYS}},
            sort_keys=True,
        )
    ]
    for cell in cells:
        key = cell["key"]
        lines.append(
            json.dumps(
                {
                    **{name: cell[name] for name in ("key", "seat", "door", "shape", "op")},
                    **{name: cell[name] for name in ("types", "case", "ansi", "stmt")},
                    "expect": expected(spark[key], derived.get(key)),
                },
                sort_keys=True,
            )
        )
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {len(lines) - 1} cells to {OUT.name}")
    kinds = kind_cells()
    KIND_OUT.write_text("\n".join(kinds) + "\n", encoding="utf-8")
    print(f"wrote {len(kinds)} cells to {KIND_OUT.name}")


if __name__ == "__main__":
    main()
