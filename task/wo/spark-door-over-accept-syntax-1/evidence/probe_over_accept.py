from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any

CLASS_PATTERN = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")

EXPRESSION_CELLS: list[dict[str, Any]] = [
    {"id": "E01", "role": "card", "item": "1e2foo AS v", "pred": "1e2foo = 100"},
    {"id": "E02", "role": "card", "item": "1e2.foo AS v", "pred": "1e2.foo = 100"},
    {"id": "E03", "role": "variant", "item": "1e2Dx AS v", "pred": "1e2Dx = 100"},
    {"id": "E04", "role": "variant", "item": "1e2.D AS v", "pred": "1e2.D = 100"},
    {"id": "E05", "role": "variant", "item": "1E2_x AS v"},
    {"id": "E06", "role": "variant", "item": "1e2.BD AS v"},
    {"id": "E07", "role": "variant", "item": "1.5e10foo AS v"},
    {"id": "E08", "role": "variant", "item": "-1e2foo AS v"},
    {"id": "E09", "role": "neighbour", "item": "1e2`foo`"},
    {"id": "E10", "role": "neighbour", "item": "1e2 AS v", "pred": "1e2 = 100"},
    {"id": "E11", "role": "neighbour", "item": "1E-3 AS v"},
    {"id": "E12", "role": "neighbour", "item": "1e2 AS foo"},
    {"id": "E13", "role": "neighbour", "item": "1e2D AS v", "pred": "1e2D = 100"},
    {"id": "E14", "role": "neighbour", "item": "1e2d AS v"},
    {"id": "E15", "role": "neighbour", "item": "1.5e10BD AS v"},
    {"id": "E16", "role": "neighbour", "item": "1e2BD AS v"},
    {"id": "E17", "role": "neighbour", "item": "1e2F AS v"},
    {"id": "E18", "role": "neighbour", "item": "1.e2D AS v"},
    {"id": "E19", "role": "neighbour", "item": ".5e2 AS v"},
    {"id": "E20", "role": "neighbour", "item": "1e2L AS v"},
    {"id": "E21", "role": "neighbour", "item": "1e2S AS v"},
    {"id": "E22", "role": "neighbour", "item": "1e2+1 AS v"},
    {"id": "E23", "role": "neighbour", "item": "-1e2 AS v"},
    {"id": "E24", "role": "neighbour", "item": "1e2 foo"},
    {"id": "E25", "role": "neighbour", "item": "5.D AS v"},
    {"id": "E26", "role": "neighbour", "item": "2.5D AS v"},
    {"id": "E27", "role": "neighbour", "item": "1e2D + 1 AS v"},
    {"id": "E28", "role": "neighbour", "item": "1.5E10 AS v"},
    {"id": "E29", "role": "neighbour", "item": "1e2Y AS v"},
    {
        "id": "E30",
        "role": "neighbour",
        "sql": "SELECT 1e2foo FROM (SELECT 7 AS 1e2foo) t",
        "item": "`1e2foo` AS v",
    },
    {
        "id": "E31",
        "role": "neighbour",
        "sql": "SELECT `1e2foo` AS v FROM (SELECT 7 AS `1e2foo`) t",
    },
    {"id": "E32", "role": "neighbour", "sql": "SELECT 1a FROM (SELECT 5 AS 1a) t"},
    {"id": "E33", "role": "neighbour", "sql": "SELECT 12abc AS v FROM (SELECT 5 AS 12abc) t"},
    {"id": "E34", "role": "neighbour", "item": "1e2 * a AS v", "pred": "1e2 * a = 100"},
    {"id": "E40", "role": "neighbour", "item": "(1e2).foo AS v"},
    {"id": "E41", "role": "neighbour", "item": "1.foo AS v"},
    {"id": "E42", "role": "neighbour", "item": "a.foo AS v"},
    {"id": "E43", "role": "neighbour", "item": "1.`5e10foo` AS v"},
    {"id": "E44", "role": "neighbour", "item": "1.5.foo AS v"},
    {"id": "E45", "role": "neighbour", "item": "CAST(1e2 AS DOUBLE).foo AS v"},
    {"id": "E46", "role": "neighbour", "item": "1.5foo AS v"},
    {"id": "E47", "role": "neighbour", "item": "1foo AS v"},
    {"id": "S28", "role": "neighbour", "item": "named_struct('a', 1, 'b', rand()).a AS v"},
    {"id": "S29", "role": "neighbour", "item": "named_struct('a', 1, 'b', (SELECT 2)).a AS v"},
    {"id": "S01", "role": "card", "item": "named_struct('a', 1, 'b', no_such_col_xyz).a AS v"},
    {
        "id": "S02",
        "role": "card",
        "item": "named_struct('a', 1, 'b', CAST('notanint' AS INT)).a AS v",
        "pred": "named_struct('a', 1, 'b', CAST('notanint' AS INT)).a = 1",
    },
    {"id": "S03", "role": "variant", "item": "named_struct('a', 1, 'b', 1 / 0).a AS v"},
    {"id": "S04", "role": "variant", "item": "named_struct('a', 1, 'b', abs(1, 2)).a AS v"},
    {
        "id": "S05",
        "role": "variant",
        "item": "named_struct('s', named_struct('a', 1, 'b', no_such_col_xyz)).s.a AS v",
    },
    {
        "id": "S06",
        "role": "variant",
        "item": "named_struct('a', 1, 'b', CAST(s AS INT)).a AS v",
        "pred": "named_struct('a', 1, 'b', CAST(s AS INT)).a = 1",
    },
    {
        "id": "S07",
        "role": "variant",
        "item": "named_struct('a', 1, 'b', no_such_col_xyz).a AS v",
        "pred": "named_struct('a', 1, 'b', no_such_col_xyz).a = 1",
    },
    {
        "id": "S08",
        "role": "variant",
        "item": "named_struct('a', CAST('notanint' AS INT), 'b', 1).b AS v",
    },
    {"id": "S09", "role": "variant", "item": "named_struct('a', 1, 'b', 'x' + 1).a AS v"},
    {"id": "S10", "role": "neighbour", "item": "named_struct('a', 1).a AS v"},
    {"id": "S11", "role": "neighbour", "item": "named_struct('a', 1, 'b', 2).b AS v"},
    {"id": "S12", "role": "neighbour", "item": "named_struct('a', 1, 'b', 'x').a"},
    {"id": "S13", "role": "neighbour", "item": "named_struct('s', named_struct('a', 1)).s.a AS v"},
    {"id": "S14", "role": "neighbour", "item": "named_struct('a', a, 'b', b + 1).b AS v"},
    {"id": "S15", "role": "neighbour", "item": "struct(1 AS a, 2 AS b).a AS v"},
    {"id": "S16", "role": "neighbour", "item": "struct(1, 2).col1 AS v"},
    {
        "id": "S17",
        "role": "neighbour",
        "item": "named_struct('a', 1, 'b', CAST('5' AS INT)).a AS v",
    },
    {"id": "S18", "role": "neighbour", "item": "named_struct('a', 1, 'b', NULL).a AS v"},
    {"id": "S19", "role": "neighbour", "item": "named_struct('a', 1, 'b', s).a AS v"},
    {"id": "S20", "role": "neighbour", "item": "named_struct('a', a, 'b', 2).a AS v"},
    {"id": "S21", "role": "neighbour", "item": "named_struct('a', 1, 'b', 2.5D).a AS v"},
    {"id": "S22", "role": "neighbour", "item": "named_struct('a', 1, 'b', upper(s)).a AS v"},
    {"id": "S23", "role": "neighbour", "item": "named_struct('a', 1, 'b', no_such_col_xyz) AS v"},
    {"id": "S24", "role": "neighbour", "item": "named_struct('a', 1, 'b', -2).a AS v"},
    {"id": "S25", "role": "neighbour", "item": "named_struct('a', 1, 'b', array(1, 2)).a AS v"},
    {"id": "S26", "role": "neighbour", "item": "named_struct('a', 1, 'b', true).a AS v"},
    {"id": "S27", "role": "neighbour", "item": "named_struct('a', 1, 'b', a).a AS v"},
    {
        "id": "W01",
        "role": "card",
        "sql": "SELECT * EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t",
        "item": "* EXCLUDE (b)",
    },
    {
        "id": "W02",
        "role": "variant",
        "sql": "SELECT t.* EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t",
    },
    {
        "id": "W03",
        "role": "variant",
        "sql": "SELECT * exclude (a, b), 3 AS c FROM (SELECT 1 AS a, 2 AS b) t",
    },
    {
        "id": "W04",
        "role": "neighbour",
        "sql": "SELECT * EXCEPT (b) FROM (SELECT 1 AS a, 2 AS b) t",
        "item": "* EXCEPT (b)",
    },
    {
        "id": "W05",
        "role": "neighbour",
        "sql": "SELECT * EXCLUDE b FROM (SELECT 1 AS a, 2 AS b) t",
    },
    {
        "id": "W10",
        "role": "neighbour",
        "sql": "SELECT * FROM (SELECT 1 AS a, 2 AS b) t",
        "item": "*",
    },
    {"id": "W11", "role": "neighbour", "sql": "SELECT t.* FROM (SELECT 1 AS a, 2 AS b) t"},
    {"id": "W12", "role": "neighbour", "sql": "SELECT count(*) AS n FROM (SELECT 1 AS a) t"},
    {"id": "W13", "role": "neighbour", "item": "2 * 3 AS v"},
    {"id": "W14", "role": "neighbour", "item": "a * b AS v"},
    {
        "id": "W15",
        "role": "neighbour",
        "sql": "SELECT *, a + 1 AS c FROM (SELECT 1 AS a, 2 AS b) t",
    },
    {
        "id": "W16",
        "role": "neighbour",
        "sql": "SELECT s.* FROM (SELECT named_struct('x', 1, 'y', 2) AS s) t",
    },
    {
        "id": "W17",
        "role": "neighbour",
        "sql": "SELECT 2 * exclude AS v FROM (SELECT 3 AS exclude) t",
    },
    {
        "id": "W18",
        "role": "neighbour",
        "sql": "SELECT a exclude FROM (SELECT 1 AS a) t",
    },
    {
        "id": "T01",
        "role": "card",
        "sql": "SELECT drop temporary FROM (SELECT 1 AS drop) t",
        "item": "drop temporary",
    },
    {
        "id": "T02",
        "role": "variant",
        "sql": "SELECT drop temporary, 2 AS x FROM (SELECT 1 AS drop) t",
    },
    {
        "id": "T03",
        "role": "variant",
        "sql": "SELECT drop\n  temporary FROM (SELECT 1 AS drop) t",
    },
    {
        "id": "T10",
        "role": "neighbour",
        "sql": "SELECT drop FROM (SELECT 1 AS drop) t",
        "item": "drop",
    },
    {
        "id": "T11",
        "role": "neighbour",
        "sql": "SELECT temporary FROM (SELECT 1 AS temporary) t",
    },
    {
        "id": "T12",
        "role": "neighbour",
        "sql": "SELECT drop AS temporary FROM (SELECT 1 AS drop) t",
        "item": "drop AS temporary",
    },
]

STATEMENT_CELLS: list[dict[str, Any]] = [
    {"id": "T20", "role": "neighbour", "sql": "DROP TEMPORARY VIEW IF EXISTS probe_missing_view"},
    {
        "id": "T21",
        "role": "neighbour",
        "sql": "DROP TEMPORARY FUNCTION IF EXISTS probe_missing_function",
    },
    {"id": "T22", "role": "neighbour", "sql": "DROP VIEW IF EXISTS probe_missing_view"},
    {
        "id": "T23",
        "role": "neighbour",
        "setup": ["CREATE OR REPLACE TEMP VIEW probe_drop_view AS SELECT 1 AS a"],
        "sql": "DROP TEMPORARY VIEW probe_drop_view",
        "after": "SELECT * FROM probe_drop_view",
    },
    {
        "id": "T24",
        "role": "neighbour",
        "setup": ["CREATE OR REPLACE TEMP VIEW probe_drop_view2 AS SELECT 1 AS a"],
        "sql": "drop   temporary   view probe_drop_view2",
        "after": "SELECT * FROM probe_drop_view2",
    },
    {"id": "T25", "role": "neighbour", "sql": "DROP TEMPORARY VARIABLE IF EXISTS probe_var"},
    {"id": "T26", "role": "neighbour", "sql": "DROP TEMPORARY TABLE IF EXISTS probe_table"},
]

DML_CELLS: list[dict[str, Any]] = [
    {"id": "D01", "role": "card", "sql": "DELETE ice.ns.{t} WHERE a = 1L"},
    {"id": "D02", "role": "variant", "sql": "DELETE ice.ns.{t}"},
    {"id": "D03", "role": "variant", "sql": "DELETE `ice`.`ns`.`{t}` WHERE a = 1"},
    {"id": "D04", "role": "variant", "sql": "DELETE ice.ns.{t} AS x WHERE x.a = 1"},
    {"id": "D05", "role": "variant", "sql": "delete ice.ns.{t} where a = 1"},
    {"id": "D06", "role": "variant", "sql": "DELETE ice.ns.{t} x WHERE x.a = 1"},
    {"id": "D07", "role": "variant", "sql": "  DELETE\n ice.ns.{t} WHERE a IN (SELECT 1)"},
    {"id": "D10", "role": "neighbour", "sql": "DELETE FROM ice.ns.{t} WHERE a = 1L"},
    {"id": "D11", "role": "neighbour", "sql": "DELETE FROM ice.ns.{t} AS x WHERE x.a = 1"},
    {"id": "D12", "role": "neighbour", "sql": "DELETE FROM ice.ns.{t} x WHERE x.a = 1"},
    {"id": "D13", "role": "neighbour", "sql": "DELETE FROM `ice`.`ns`.`{t}` WHERE a = 1"},
    {"id": "D14", "role": "neighbour", "sql": "delete from ice.ns.{t} where a = 1"},
    {"id": "D15", "role": "neighbour", "sql": "DELETE FROM ice.ns.{t} WHERE a = 1e2"},
    {
        "id": "D16",
        "role": "neighbour",
        "sql": "MERGE INTO ice.ns.{t} t USING (SELECT 1L AS a) s ON t.a = s.a "
        "WHEN MATCHED THEN DELETE",
    },
    {"id": "U01", "role": "variant", "sql": "UPDATE ice.ns.{t} SET b = 1e2foo WHERE a = 1"},
    {"id": "U02", "role": "variant", "sql": "UPDATE ice.ns.{t} SET b = 1e2 WHERE a = 1e2foo"},
    {
        "id": "U03",
        "role": "variant",
        "sql": "UPDATE ice.ns.{t} SET b = named_struct('x', 5.0D, 'y', no_such_col_xyz).x "
        "WHERE a = 1",
    },
    {
        "id": "U04",
        "role": "variant",
        "sql": "DELETE FROM ice.ns.{t} WHERE a = named_struct('x', 1, 'y', no_such_col_xyz).x",
    },
    {
        "id": "U05",
        "role": "variant",
        "sql": "MERGE INTO ice.ns.{t} t USING (SELECT 1L AS a) s ON t.a = s.a "
        "WHEN MATCHED THEN UPDATE SET b = 1e2foo",
    },
    {"id": "U06", "role": "variant", "sql": "DELETE FROM ice.ns.{t} WHERE a = 1e2foo"},
    {"id": "U10", "role": "neighbour", "sql": "UPDATE ice.ns.{t} SET b = 1e2 WHERE a = 1"},
    {"id": "U11", "role": "neighbour", "sql": "UPDATE ice.ns.{t} SET b = 1e2D WHERE a = 1"},
    {
        "id": "U12",
        "role": "neighbour",
        "sql": "UPDATE ice.ns.{t} SET b = named_struct('x', 5.0D, 'y', a).x WHERE a = 1",
    },
    {
        "id": "U13",
        "role": "neighbour",
        "sql": "MERGE INTO ice.ns.{t} t USING (SELECT 1L AS a) s ON t.a = s.a "
        "WHEN MATCHED THEN UPDATE SET b = 1e2",
    },
]

NATIVE_CELLS: list[str] = [
    "SELECT 1e2 AS v",
    "SELECT 1e2foo AS v",
    "SELECT 1e2.foo AS v",
    "SELECT 1e2D AS v",
    "SELECT * EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t",
    "SELECT drop temporary FROM (SELECT 1 AS drop) t",
    "SELECT named_struct('a', 1, 'b', no_such_col_xyz).a AS v",
]


def normalize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        return round(value, 6)
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if hasattr(value, "asDict"):
        return {key: normalize_value(item) for key, item in value.asDict().items()}
    return str(value)


def describe_error(exc: BaseException) -> dict[str, Any]:
    text = str(exc).strip()
    found = CLASS_PATTERN.search(text)
    return {
        "kind": "error",
        "type": type(exc).__name__,
        "class": found.group(1) if found else None,
        "first": (text.splitlines()[0] if text else type(exc).__name__)[:200],
    }


def capture_frame(frame: Any) -> dict[str, Any]:
    try:
        fields = [
            [field.name, field.dataType.simpleString(), bool(field.nullable)]
            for field in frame.schema.fields
        ]
        rows = [[normalize_value(cell) for cell in row] for row in frame.collect()]
    except Exception as exc:
        return describe_error(exc)
    return {"kind": "ok", "fields": fields, "rows": rows}


def run_frame(build: Any) -> dict[str, Any]:
    try:
        frame = build()
    except Exception as exc:
        return describe_error(exc)
    return capture_frame(frame)


def make_session(engine: str, warehouse: Path, ansi: str) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession, functions

        jar = os.environ["SPARK_ICEBERG_JAR"]
        session = (
            SparkSession.builder.master("local[2]")
            .appName(f"overaccept-{ansi}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.jars", jar)
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        return session, functions

    import repark
    from repark import ReparkSession
    from repark.spark import functions
    from repark.spark.session import _reset_active_session_for_tests

    print("native:", repark._native.__file__)
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(f"overaccept-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )
    return session, functions


def expr_column(functions: Any, item: str) -> Any:
    if item.endswith(" AS v"):
        return functions.expr(item[: -len(" AS v")]).alias("v")
    return functions.expr(item)


def run_view(session: Any, sql: str) -> dict[str, Any]:
    try:
        session.sql(f"CREATE OR REPLACE TEMP VIEW probe_body_view AS {sql}").collect()
    except Exception as exc:
        return describe_error(exc)
    return run_frame(lambda: session.sql("SELECT * FROM probe_body_view"))


def run_expression_cell(session: Any, functions: Any, base: Any, cell: dict[str, Any]) -> dict:
    results: dict[str, Any] = {}
    item = cell.get("item")
    sql = cell.get("sql") or f"SELECT {item}"
    results["sql"] = run_frame(lambda: session.sql(sql))
    results["view"] = run_view(session, sql)
    if item is not None:
        results["expr"] = run_frame(lambda: base.select(expr_column(functions, item)))
        results["selectExpr"] = run_frame(lambda: base.selectExpr(item))
    if cell.get("pred") is not None:
        results["filter"] = run_frame(lambda: base.filter(cell["pred"]))
    return results


def run_statement_cell(session: Any, cell: dict[str, Any]) -> dict[str, Any]:
    try:
        for statement in cell.get("setup", []):
            session.sql(statement).collect()
    except Exception as exc:
        return {"sql": describe_error(exc)}
    outcome = run_frame(lambda: session.sql(cell["sql"]))
    if cell.get("after"):
        outcome["after"] = run_frame(lambda: session.sql(cell["after"]))
    return {"sql": outcome}


def run_dml_cell(session: Any, cell: dict[str, Any], ansi: str) -> dict[str, Any]:
    table = f"t_{cell['id'].lower()}_{ansi}"
    try:
        session.sql(f"DROP TABLE IF EXISTS ice.ns.{table}").collect()
        session.sql(f"CREATE TABLE ice.ns.{table} (a BIGINT, b DOUBLE) USING iceberg").collect()
        session.sql(f"INSERT INTO ice.ns.{table} VALUES (1, 10.0), (2, 20.0), (3, 30.0)").collect()
    except Exception as exc:
        return {"dml": describe_error(exc)}
    outcome = run_frame(lambda: session.sql(cell["sql"].format(t=table)))
    outcome["after"] = run_frame(lambda: session.sql(f"SELECT a, b FROM ice.ns.{table} ORDER BY a"))
    return {"dml": outcome}


def run_native(cells: list[str]) -> dict[str, Any]:
    import repark

    return {sql: run_frame(lambda sql=sql: repark.sql(sql)) for sql in cells}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark", "repark"], required=True)
    parser.add_argument("--ansi", choices=["true", "false"], required=True)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--only", default="")
    arguments = parser.parse_args()
    if arguments.warehouse.exists():
        shutil.rmtree(arguments.warehouse)
    arguments.warehouse.mkdir(parents=True)
    session, functions = make_session(arguments.engine, arguments.warehouse, arguments.ansi)
    session.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
    base = session.createDataFrame([(1, 2, "notanint", 1)], "a INT, b INT, s STRING, drop INT")
    prefix = arguments.only
    report: dict[str, Any] = {"engine": arguments.engine, "ansi": arguments.ansi, "cells": {}}
    for cell in EXPRESSION_CELLS:
        if cell["id"].startswith(prefix):
            report["cells"][cell["id"]] = {
                "role": cell["role"],
                "text": cell.get("sql") or cell.get("item"),
                "item": cell.get("item"),
                "pred": cell.get("pred"),
                "doors": run_expression_cell(session, functions, base, cell),
            }
    for cell in STATEMENT_CELLS:
        if cell["id"].startswith(prefix):
            report["cells"][cell["id"]] = {
                "role": cell["role"],
                "text": cell["sql"],
                "doors": run_statement_cell(session, cell),
            }
    for cell in DML_CELLS:
        if cell["id"].startswith(prefix):
            report["cells"][cell["id"]] = {
                "role": cell["role"],
                "text": cell["sql"].format(t="t"),
                "doors": run_dml_cell(session, cell, arguments.ansi),
            }
    if arguments.engine == "repark":
        report["native"] = run_native(NATIVE_CELLS)
    arguments.out.write_text(json.dumps(report, indent=1, sort_keys=True))
    print("cells:", len(report["cells"]))


if __name__ == "__main__":
    main()
