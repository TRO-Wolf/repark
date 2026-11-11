from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

VIEW = (
    "CREATE OR REPLACE TEMPORARY VIEW nm AS "
    "SELECT named_struct('a', 1, 'b', 2) AS s, named_struct('b', 2, 'a', 1) AS t, "
    "named_struct('a', 1, 'a', 2) AS d"
)
FORMS = {
    "between": "s BETWEEN t AND t",
    "not_between": "s NOT BETWEEN t AND t",
    "case": "CASE s WHEN t THEN 1 ELSE 0 END",
    "nullif": "nullif(s, t)",
    "nullif_rev": "nullif(t, s)",
}
DUPLICATE_FORMS = {"eq_dup": "d = t", "lt_dup": "d < t"}
STRING_DOORS = ("sql", "select_expr", "f_expr")
TYPED_FORMS = ("between", "not_between", "nullif")


def gen_name_cells() -> list[dict[str, str]]:
    cells = [
        {"form": form, "door": door, "text": text}
        for form, text in FORMS.items()
        if form != "nullif_rev"
        for door in (*STRING_DOORS, "typed")
        if door != "typed" or form in TYPED_FORMS
    ]
    cells += [
        {"form": "nullif_rev", "door": door, "text": FORMS["nullif_rev"]} for door in STRING_DOORS
    ]
    cells += [
        {"form": form, "door": door, "text": text}
        for form, text in DUPLICATE_FORMS.items()
        for door in STRING_DOORS
    ]
    return [{"id": f"N{index:04d}", **cell} for index, cell in enumerate(cells, start=1)]


def session(leg: str) -> Any:
    if leg == "spark":
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )
    else:
        from repark import ReparkSession

        builder = ReparkSession.builder.appName("family-names-" + leg).config(
            "spark.sql.session.timeZone", "UTC"
        )
    return builder.getOrCreate()


def functions_of(leg: str) -> Any:
    if leg == "spark":
        from pyspark.sql import functions as sf
    else:
        from repark.spark import functions as sf
    return sf


def typed_column(sf: Any, form: str) -> Any:
    s, t = sf.col("s"), sf.col("t")
    if form == "between":
        return s.between(t, t)
    if form == "not_between":
        return ~s.between(t, t)
    return sf.nullif(s, t)


def frame_of(spark: Any, sf: Any, cell: dict[str, str]) -> Any:
    if cell["door"] == "sql":
        return spark.sql(f"SELECT {cell['text']} FROM nm")
    table = spark.table("nm")
    if cell["door"] == "select_expr":
        return table.selectExpr(cell["text"])
    if cell["door"] == "f_expr":
        return table.select(sf.expr(cell["text"]))
    return table.select(typed_column(sf, cell["form"]))


def measure(spark: Any, sf: Any, cell: dict[str, str]) -> dict[str, Any]:
    try:
        frame = frame_of(spark, sf, cell)
        return {**cell, "status": "ok", "columns": list(frame.columns)}
    except BaseException as error:
        return {**cell, "status": "error", "class": type(error).__name__, "text": str(error)[:400]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", required=True, choices=("spark", "base", "head"))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    spark = session(args.leg)
    sf = functions_of(args.leg)
    spark.sql(VIEW).collect()
    spark.conf.set("spark.sql.caseSensitive", "false")
    records = [measure(spark, sf, cell) for cell in gen_name_cells()]
    spark.stop()
    lines = [json.dumps(record, sort_keys=True) for record in records]
    Path(args.out).write_text("\n".join(lines) + "\n", encoding="utf-8")
    sys.stdout.write(f"{args.leg}: {len(records)} name cells\n")


if __name__ == "__main__":
    main()
