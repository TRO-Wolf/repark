from __future__ import annotations

import datetime
import decimal
import json
import re
import shutil
import struct
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

FIXTURE = Path(__file__).resolve().parent / "setop_all_multiset_1_spark_oracle.json"
ANSI_MODES = ("true", "false")
OPS = ("INTERSECT ALL", "EXCEPT ALL", "MINUS ALL")
CONTROL_OPS = ("UNION ALL", "UNION", "INTERSECT", "EXCEPT", "INTERSECT DISTINCT", "EXCEPT DISTINCT")
SPELLINGS = (
    "intersect all",
    "Intersect All",
    "except all",
    "ExCePt AlL",
    "minus all",
    "MiNuS aLl",
    "INTERSECT /* gap */ ALL",
    "EXCEPT\n  ALL",
    "MINUS -- gap\n ALL",
)
SHAPES = (
    "top",
    "cte",
    "scalar",
    "in",
    "temp-view",
    "persistent-view",
    "iceberg-source",
    "insert",
    "ctas",
    "merge",
    "union-nested",
    "explain",
)
WRITE_SHAPES = ("insert", "ctas", "merge")
LEFT = (1, 1, 1, 2, 2, 4)
RIGHT = (1, 2, 2, 3)
NULL_STRUCT = "CASE WHEN 1 = 0 THEN named_struct('x', CAST(1 AS INT), 'y', 'a') END"


def scalar(kind: str, items: list[str | None]) -> list[str]:
    return [f"CAST({'NULL' if item is None else repr(item)} AS {kind}) AS a" for item in items]


def pair(x: int | None, y: str | None) -> str:
    x_sql = "NULL" if x is None else str(x)
    y_sql = "NULL" if y is None else repr(y)
    return f"named_struct('x', CAST({x_sql} AS INT), 'y', CAST({y_sql} AS STRING)) AS a"


def row3(a: int | None, b: str | None, d: str | None) -> str:
    a_sql = "NULL" if a is None else str(a)
    b_sql = "NULL" if b is None else repr(b)
    d_sql = "NULL" if d is None else repr(d)
    return f"CAST({a_sql} AS INT) AS a, CAST({b_sql} AS STRING) AS b, CAST({d_sql} AS DOUBLE) AS d"


FLOATS_LEFT = ["-0.0", "0.0", "0.0", "NaN", "NaN", "1.5", "1.5", None, None]
FLOATS_RIGHT = ["0.0", "-0.0", "NaN", "2.5", None]
TYPE_CELLS: dict[str, tuple[list[str], list[str]]] = {
    "int": (
        scalar("INT", ["1", "1", "1", "2", "2", "4", None, None, None]),
        scalar("INT", ["1", "2", "2", "3", None]),
    ),
    "bigint": (
        scalar("BIGINT", ["9000000000"] * 3 + ["-7", "-7", "4", None, None, None]),
        scalar("BIGINT", ["9000000000", "-7", "-7", "3", None]),
    ),
    "decimal": (
        scalar("DECIMAL(10,2)", ["1.1", "1.10", "1.10", "-2.20", "-2.2", "4", None, None, None]),
        scalar("DECIMAL(10,2)", ["1.10", "-2.20", "-2.20", "3", None]),
    ),
    "double": (scalar("DOUBLE", FLOATS_LEFT), scalar("DOUBLE", FLOATS_RIGHT)),
    "float": (scalar("FLOAT", FLOATS_LEFT), scalar("FLOAT", FLOATS_RIGHT)),
    "double-signs": (scalar("DOUBLE", ["-0.0"] * 3), scalar("DOUBLE", ["0.0"] * 2)),
    "float-signs": (scalar("FLOAT", ["-0.0"] * 3), scalar("FLOAT", ["0.0"] * 2)),
    "string": (
        scalar("STRING", ["a", "a", "a", "b", "b", "", "A", None, None]),
        scalar("STRING", ["a", "b", "b", "c", " a", None]),
    ),
    "date": (
        scalar("DATE", ["2024-01-01"] * 3 + ["1970-01-01"] * 2 + ["2024-02-29", None, None]),
        scalar("DATE", ["2024-01-01", "1970-01-01", "1970-01-01", "2000-01-01", None]),
    ),
    "timestamp": (
        scalar(
            "TIMESTAMP",
            ["2024-01-01 10:00:00"] * 3
            + ["1970-01-01 00:00:00"] * 2
            + ["2024-02-29 23:59:59.123456", None, None],
        ),
        scalar(
            "TIMESTAMP",
            ["2024-01-01 10:00:00", "1970-01-01 00:00:00", "1970-01-01 00:00:00", None],
        ),
    ),
    "null": (["NULL AS a"] * 3, ["NULL AS a"]),
    "struct": (
        [pair(1, "a")] * 3 + [pair(2, None)] * 2 + [pair(None, "b")] + [f"{NULL_STRUCT} AS a"] * 2,
        [pair(1, "a"), pair(2, None), pair(2, None), pair(3, "c"), f"{NULL_STRUCT} AS a"],
    ),
    "columns": (
        [row3(1, "a", "0.0")] * 2
        + [row3(1, "a", "-0.0"), row3(1, "b", "0.0"), row3(None, None, None)]
        + [row3(None, None, None), row3(2, "a", "NaN")],
        [row3(1, "a", "0.0")] * 2
        + [row3(None, None, None), row3(2, "a", "NaN"), row3(2, "a", "NaN"), row3(1, None, "0.0")],
    ),
}
EDGE_CELLS: dict[str, tuple[list[str], list[str]]] = {
    "empty-left": ([], scalar("INT", ["1", "2"])),
    "empty-right": (scalar("INT", ["1", "1", "2"]), []),
    "both-empty": ([], []),
    "only-duplicates": (scalar("INT", ["7"] * 4), scalar("INT", ["7"] * 2)),
    "only-duplicates-right-more": (scalar("INT", ["7"] * 2), scalar("INT", ["7"] * 5)),
}
ERROR_CELLS: dict[str, tuple[str, str]] = {
    "column-count": (
        "SELECT CAST(1 AS INT) AS a, CAST(2 AS INT) AS b",
        "SELECT CAST(1 AS INT) AS a",
    ),
    "column-type": ("SELECT array(CAST(1 AS INT)) AS a", "SELECT CAST(1 AS INT) AS a"),
}
DATAFRAME_METHODS = ("intersectAll", "exceptAll", "intersect_all", "except_all")


def side(rows: list[str]) -> str:
    if not rows:
        return "SELECT CAST(NULL AS INT) AS a WHERE 1 = 0"
    return " UNION ALL ".join(f"SELECT {row}" for row in rows)


def operation(left: str, op: str, right: str) -> str:
    return f"SELECT * FROM ({left}) AS sl {op} SELECT * FROM ({right}) AS sr"


def spark_method(method: str) -> str:
    return {"intersect_all": "intersectAll", "except_all": "exceptAll"}.get(method, method)


def float_bits(value: float) -> str:
    return "f" + struct.pack(">d", value).hex()


def encode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return float_bits(value)
    if isinstance(value, decimal.Decimal):
        return (
            f"d{value.normalize():f}" if value == value.to_integral() else f"d{value.normalize()}"
        )
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        return "t" + value.isoformat()
    if isinstance(value, datetime.date):
        return "t" + value.isoformat()
    if isinstance(value, dict):
        return [[key, encode(item)] for key, item in value.items()]
    if hasattr(value, "asDict"):
        return [[key, encode(item)] for key, item in value.asDict().items()]
    if isinstance(value, (list, tuple)):
        return [encode(item) for item in value]
    return repr(value)


def rows_of(rows: list[Any]) -> list[Any]:
    encoded = [[encode(item) for item in row] for row in rows]
    return sorted(encoded, key=lambda row: json.dumps(row, sort_keys=True))


def error_of(error: BaseException) -> dict[str, Any]:
    found: dict[str, Any] = {"kind": "error", "class": type(error).__name__}
    for name, attribute in (("condition", "getCondition"), ("sqlstate", "getSqlState")):
        getter = getattr(error, attribute, None)
        if callable(getter):
            try:
                found[name] = getter()
            except Exception as failure:
                found[name] = f"unavailable: {type(failure).__name__}"
    text = " ".join(str(error).split())
    found["text"] = text[:600]
    match = re.search(r"\[([A-Z][A-Z0-9_.]+)\]", text)
    found["bracket"] = match.group(1) if match else None
    match = re.search(r"SQLSTATE:? ?([0-9A-Z]{5})", text)
    found["state"] = match.group(1) if match else None
    return found


def attempt(run: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return run()
    except Exception as error:
        return error_of(error)


class Door:
    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = root
        self.session: Any = None
        self.native: Any = None
        self.cold_reader: Callable[[str], list[Any]] | None = None

    def is_native(self) -> bool:
        return self.name == "native"

    def frame(self, sql: str) -> Any:
        if self.is_native():
            import repark

            return repark.sql(sql)
        return self.session.sql(sql)

    def rows(self, sql: str) -> list[Any]:
        return self.collect(self.frame(sql))

    def collect(self, frame: Any) -> list[Any]:
        if self.name == "spark":
            return [tuple(row) for row in frame.collect()]
        table = frame.to_arrow() if self.is_native() else frame.toArrow()
        return [tuple(row.values()) for row in table.to_pylist()]

    def answer(self, sql: str) -> dict[str, Any]:
        frame = self.frame(sql)
        return {"kind": "rows", "rows": rows_of(self.collect(frame)), "type": self.type_of(frame)}

    def type_of(self, frame: Any) -> str:
        if self.is_native():
            return str(frame.to_arrow().schema)
        return frame.schema.simpleString()

    def run(self, sql: str) -> None:
        if self.is_native():
            import repark

            repark.sql(sql).to_arrow()
        else:
            self.session.sql(sql).collect()

    def temp_view(self) -> str:
        return "VIEW" if self.is_native() else "TEMPORARY VIEW"

    def using(self) -> str:
        return "" if self.is_native() else " USING iceberg"

    def values(self, items: tuple[int, ...]) -> str:
        body = ", ".join(f"({item})" for item in items)
        if self.is_native():
            return f"SELECT * FROM (VALUES {body}) AS t(a)"
        return f"SELECT * FROM VALUES {body} AS t(a)"

    def persistent_view(self, name: str) -> str:
        return f"spark_catalog.default.{name}" if self.name == "spark" else f"ice.ns.{name}"

    def seed(self, prefix: str) -> None:
        self.run(f"CREATE OR REPLACE {self.temp_view()} l AS {self.values(LEFT)}")
        self.run(f"CREATE OR REPLACE {self.temp_view()} r AS {self.values(RIGHT)}")
        for name, items in ((f"{prefix}pl", LEFT), (f"{prefix}pr", RIGHT)):
            self.run(f"CREATE TABLE ice.ns.{name} (a INT){self.using()}")
            body = ", ".join(f"({item})" for item in items)
            self.run(f"INSERT INTO ice.ns.{name} VALUES {body}")


def shape_cell(door: Door, shape: str, op: str, slot: str, prefix: str) -> dict[str, Any]:
    q = f"SELECT a FROM l {op} SELECT a FROM r"
    tables = f"SELECT a FROM ice.ns.{prefix}pl {op} SELECT a FROM ice.ns.{prefix}pr"
    if shape == "top":
        return attempt(lambda: door.answer(q))
    if shape == "cte":
        return attempt(lambda: door.answer(f"WITH x AS ({q}) SELECT a FROM x"))
    if shape == "scalar":
        return attempt(lambda: door.answer(f"SELECT (SELECT count(*) FROM ({q}) x) AS c"))
    if shape == "in":
        return attempt(lambda: door.answer(f"SELECT a FROM r WHERE a IN ({q})"))
    if shape == "temp-view":
        name = f"vt_{slot}"
        created = attempt(
            lambda: {
                "kind": "ok",
                "run": door.run(f"CREATE OR REPLACE {door.temp_view()} {name} AS {q}"),
            }
        )
        return {**attempt(lambda: door.answer(f"SELECT a FROM {name}")), "create": created["kind"]}
    if shape == "persistent-view":
        name = door.persistent_view(f"vp_{slot}")
        created = attempt(
            lambda: {"kind": "ok", "run": door.run(f"CREATE VIEW {name} AS {tables}")}
        )
        return {**attempt(lambda: door.answer(f"SELECT a FROM {name}")), "create": created["kind"]}
    if shape == "iceberg-source":
        return attempt(lambda: door.answer(tables))
    if shape == "union-nested":
        return attempt(lambda: door.answer(f"SELECT a FROM (SELECT 9 AS a UNION ALL ({q})) x"))
    if shape == "explain":
        found = attempt(lambda: door.answer(f"EXPLAIN {q}"))
        return {"kind": found["kind"], **({} if found["kind"] == "rows" else found)}
    table = f"ice.ns.w_{slot}"
    if shape == "ctas":
        statement = f"CREATE TABLE {table}{door.using()} AS {q}"
    else:
        door.run(f"CREATE TABLE {table} (a INT){door.using()}")
        if shape == "insert":
            statement = f"INSERT INTO {table} {q}"
        else:
            statement = (
                f"MERGE INTO {table} t USING ({q}) s ON t.a = s.a "
                "WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)"
            )
    ran = attempt(lambda: {"kind": "ok", "run": door.run(statement)})
    hot = attempt(lambda: door.answer(f"SELECT a FROM {table}"))
    return {"kind": ran["kind"], "statement": ran, "table": f"w_{slot}", "hot": hot}


def fragment_cells(door: Door, op: str) -> dict[str, dict[str, Any]]:
    if door.name == "spark":
        from pyspark.sql import functions as sf
    else:
        from repark.spark import functions as sf
    inline_left = "SELECT a FROM VALUES (1), (1), (1), (2), (2), (4) AS t(a)"
    inline_right = "SELECT a FROM VALUES (1), (2), (2), (3) AS t(a)"
    counted = f"(SELECT count(*) FROM ({inline_left} {op} {inline_right}) x)"
    member = f"a IN ({inline_left} {op} {inline_right})"
    right = door.session.sql("SELECT a FROM r")
    one = door.session.sql("SELECT 1 AS one")

    def rows(frame: Any) -> dict[str, Any]:
        return {"kind": "rows", "rows": rows_of(door.collect(frame))}

    return {
        "fexpr-scalar": attempt(lambda: rows(one.select(sf.expr(counted).alias("c")))),
        "fexpr-filter-in": attempt(lambda: rows(right.filter(member))),
        "fexpr-filter-col": attempt(lambda: rows(right.filter(sf.expr(member)))),
        "selectexpr-scalar": attempt(lambda: rows(right.selectExpr(f"{counted} AS c"))),
    }


def dataframe_cell(door: Door, cell: str, method: str) -> dict[str, Any]:
    left_rows, right_rows = TYPE_CELLS[cell]
    left = door.session.sql(side(left_rows))
    right = door.session.sql(side(right_rows))
    name = spark_method(method) if door.name == "spark" else method
    frame = getattr(left, name)(right)
    return {"kind": "rows", "rows": rows_of(door.collect(frame)), "type": door.type_of(frame)}


GUARD_SIDES = {
    "g1l": scalar("INT", ["1", "1", "2", None, None]),
    "g1r": scalar("INT", ["1", "1", None, "3"]),
    "g2l": [row3(1, "x", "0.0")] * 2 + [row3(2, "y", "0.0")] + [row3(None, None, "0.0")] * 2,
    "g2r": [row3(1, "x", "0.0")] * 2 + [row3(None, None, "0.0"), row3(3, "z", "0.0")],
    "gdl": scalar("DECIMAL(10,2)", ["1.10", "1.10", "2.00", None]),
    "gdr": [f"CAST({item} AS DOUBLE) AS x" for item in ("1.1", "1.1", "NULL")],
}
GUARD_JOINS = {
    "one": ("g1l", "g1r", (("a", "a"),)),
    "two": ("g2l", "g2r", (("a", "a"), ("b", "b"))),
    "decimal": ("gdl", "gdr", (("a", "x"),)),
}
GUARD_KINDS = ("semi", "anti")


def guard_frame(door: Door, left: str, right: str, keys: Any, kind: str) -> dict[str, Any]:
    lf = door.session.sql(f"SELECT * FROM {left}")
    rf = door.session.sql(f"SELECT * FROM {right}")
    condition = None
    for lk, rk in keys:
        term = lf[lk].eqNullSafe(rf[rk])
        condition = term if condition is None else condition & term
    joined = lf.join(rf, condition, f"left_{kind}")
    return {"kind": "rows", "rows": rows_of(door.collect(joined)), "type": door.type_of(joined)}


def guard_cells(door: Door) -> dict[str, dict[str, Any]]:
    for name, rows in GUARD_SIDES.items():
        sql = side(rows)
        if name == "g2l" or name == "g2r":
            sql = f"SELECT a, b FROM ({sql}) AS s"
        door.run(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {sql}")
    cells: dict[str, dict[str, Any]] = {}
    for name, (left, right, keys) in GUARD_JOINS.items():
        for kind in GUARD_KINDS:
            on = " AND ".join(f"{left}.{lk} <=> {right}.{rk}" for lk, rk in keys)
            sql = f"SELECT * FROM {left} LEFT {kind.upper()} JOIN {right} ON {on}"
            cells[f"guard|sql|{name}|{kind}"] = attempt(lambda sql=sql: door.answer(sql))

            cells[f"guard|df|{name}|{kind}"] = attempt(
                lambda left=left, right=right, keys=keys, kind=kind: guard_frame(
                    door, left, right, keys, kind
                )
            )
    return cells


def measure(door: Door, ansi: str) -> dict[str, dict[str, Any]]:
    prefix = "n" if door.is_native() else ("a" if ansi == "true" else "b")
    door.seed(prefix)
    cells: dict[str, dict[str, Any]] = {}
    for cell, (left_rows, right_rows) in {**TYPE_CELLS, **EDGE_CELLS}.items():
        group = "type" if cell in TYPE_CELLS else "edge"
        for op in OPS:
            forward = operation(side(left_rows), op, side(right_rows))
            cells[f"{group}|{cell}|{op}|lr"] = attempt(lambda sql=forward: door.answer(sql))
            if group == "type":
                backward = operation(side(right_rows), op, side(left_rows))
                cells[f"{group}|{cell}|{op}|rl"] = attempt(lambda sql=backward: door.answer(sql))
    for cell, (left, right) in ERROR_CELLS.items():
        for op in OPS:
            sql = operation(left, op, right)
            cells[f"error|{cell}|{op}"] = attempt(lambda sql=sql: door.answer(sql))
    for index, (op, shape) in enumerate((op, shape) for op in OPS for shape in SHAPES):
        cells[f"shape|{shape}|{op}"] = shape_cell(door, shape, op, f"{prefix}{index}", prefix)
    for index, spelled in enumerate(SPELLINGS):
        sql = f"SELECT a FROM l {spelled} SELECT a FROM r"
        cells[f"spelling|{index}"] = attempt(lambda sql=sql: door.answer(sql))
    for op in CONTROL_OPS:
        sql = f"SELECT a FROM l {op} SELECT a FROM r"
        cells[f"control|{op}"] = attempt(lambda sql=sql: door.answer(sql))
    if not door.is_native():
        cells.update(guard_cells(door))
        for op in OPS:
            for name, result in fragment_cells(door, op).items():
                cells[f"fragment|{name}|{op}"] = result
        for cell in TYPE_CELLS:
            for method in DATAFRAME_METHODS:
                cells[f"dataframe|{cell}|{method}"] = attempt(
                    lambda cell=cell, method=method: dataframe_cell(door, cell, method)
                )
    return cells


def cold_read(
    door: Door, cells: dict[str, dict[str, Any]], read: Callable[[str], list[Any]]
) -> None:
    for cell in cells.values():
        table = cell.get("table")
        if table is not None:
            cell["cold"] = attempt(
                lambda table=table: {"kind": "rows", "rows": rows_of(read(table))}
            )


def open_spark(root: Path) -> Any:
    import os

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(root))
        .config("spark.sql.warehouse.dir", str(root / "spark-warehouse"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
    return spark


def record_spark(root: Path) -> dict[str, Any]:
    spark = open_spark(root)
    door = Door("spark", root)
    door.session = spark
    found: dict[str, Any] = {}
    for ansi in ANSI_MODES:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        cells = measure(door, ansi)
        cold_read(
            door,
            cells,
            lambda table: [
                tuple(row)
                for row in spark.read.format("iceberg").load(str(root / "ns" / table)).collect()
            ],
        )
        found[ansi] = cells
    version = spark.version
    spark.stop()
    return {"spark": version, "cells": found}


def main() -> None:
    root = Path(sys.argv[1])
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    Path(sys.argv[2]).write_text(
        json.dumps(record_spark(root), indent=1, sort_keys=True) + "\n", "utf-8"
    )


if __name__ == "__main__":
    main()
