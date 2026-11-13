from __future__ import annotations

import json
import os
import re
import shutil
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

LEFT = (1, 1, 1, 2, 2, 4)
RIGHT = (1, 2, 2, 3)
REFUSED_OPS = ("INTERSECT ALL", "EXCEPT ALL", "MINUS ALL")
CONTROL_OPS = (
    "UNION ALL",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "EXCEPT DISTINCT",
    "INTERSECT DISTINCT",
    "MINUS",
)
SPELLINGS = (
    "intersect all",
    "Intersect All",
    "except all",
    "ExCePt AlL",
    "minus all",
    "MiNuS aLl",
    "INTERSECT /* gap */ ALL",
    "EXCEPT\n  ALL",
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
CONTROL_SHAPES = ("top", "cte", "insert", "ctas")


class Door:
    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = root
        self.spark: Any = None
        self.native: Any = None

    def open(self) -> None:
        if self.name == "spark":
            from pyspark.sql import SparkSession

            self.spark = (
                SparkSession.builder.master("local[2]")
                .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.ice.type", "hadoop")
                .config("spark.sql.catalog.ice.warehouse", str(self.root))
                .config("spark.sql.warehouse.dir", str(self.root / "spark-warehouse"))
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            self.spark.sparkContext.setLogLevel("ERROR")
            self.spark.sql("CREATE NAMESPACE ice.ns").collect()
        elif self.name == "spark-door":
            from repark import ReparkSession

            self.spark = ReparkSession.builder.appName("setop-all-step0").getOrCreate()
            self.spark.register_memory_catalog("ice", str(self.root))
            self.spark.sql("CREATE NAMESPACE ice.ns").collect()
        else:
            import repark
            from repark._native import PyReparkSession

            self.native = PyReparkSession.native()
            self.native.register_memory_catalog("ice", str(self.root))
            self.native.create_namespace("ice", "ns", str(self.root / "ns"))
            repark._ANSI_NATIVE = self.native

    def run(self, sql: str) -> list[tuple[Any, ...]]:
        if self.name == "native":
            import repark

            rows = repark.sql(sql).to_arrow().to_pylist()
            return [tuple(row.values()) for row in rows]
        if self.name == "spark":
            return [tuple(row) for row in self.spark.sql(sql).collect()]
        rows = self.spark.sql(sql).toArrow().to_pylist()
        return [tuple(row.values()) for row in rows]

    def values(self, items: tuple[int, ...]) -> str:
        body = ", ".join(f"({item})" for item in items)
        if self.name == "native":
            return f"SELECT * FROM (VALUES {body}) AS t(a)"
        return f"SELECT * FROM VALUES {body} AS t(a)"

    def seed(self) -> None:
        temp = "VIEW" if self.name == "native" else "TEMPORARY VIEW"
        self.run(f"CREATE OR REPLACE {temp} l AS {self.values(LEFT)}")
        self.run(f"CREATE OR REPLACE {temp} r AS {self.values(RIGHT)}")
        for name, items in (("pl", LEFT), ("pr", RIGHT)):
            self.run(self.table_ddl(name))
            body = ", ".join(f"({item})" for item in items)
            self.run(f"INSERT INTO ice.ns.{name} VALUES {body}")

    def using(self) -> str:
        return "" if self.name == "native" else " USING iceberg"

    def table_ddl(self, name: str) -> str:
        return f"CREATE TABLE ice.ns.{name} (a INT){self.using()}"

    def persistent_view(self, name: str) -> str:
        if self.name == "spark":
            return f"spark_catalog.default.{name}"
        return f"ice.ns.{name}"

    def stop(self) -> None:
        if self.spark is not None:
            self.spark.stop()


def sort_rows(rows: list[tuple[Any, ...]]) -> list[list[Any]]:
    return sorted(([*row] for row in rows), key=lambda row: [(v is None, v) for v in row])


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def table_state(root: Path, table: str) -> dict[str, Any]:
    metadata_dir = root / "ns" / table / "metadata"
    files = sorted(metadata_dir.glob("*.metadata.json"), key=metadata_order)
    if not files:
        return {"exists": (root / "ns" / table).exists(), "snapshots": None}
    document = json.loads(files[-1].read_text(encoding="utf-8"))
    return {"exists": True, "snapshots": len(document.get("snapshots") or [])}


def query(op: str) -> str:
    return f"SELECT a FROM l {op} SELECT a FROM r"


def table_query(op: str) -> str:
    return f"SELECT a FROM ice.ns.pl {op} SELECT a FROM ice.ns.pr"


def outcome(fn: Callable[[], list[tuple[Any, ...]]]) -> dict[str, Any]:
    try:
        return {"kind": "rows", "rows": sort_rows(fn())}
    except Exception as error:
        text = " ".join(str(error).split())
        return {"kind": "refused", "class": type(error).__name__, "text": text[:400]}


def write_cell(door: Door, table: str, statement: str, create: bool) -> dict[str, Any]:
    if create:
        door.run(door.table_ddl(table))
    before = table_state(door.root, table)
    result = outcome(lambda: door.run(statement))
    result["before"] = before
    result["after"] = table_state(door.root, table)
    stored = outcome(lambda: door.run(f"SELECT a FROM ice.ns.{table}"))
    result["stored"] = stored.get("rows", stored.get("class"))
    return result


def shape_cell(door: Door, shape: str, op: str, slot: int) -> dict[str, Any]:
    q = query(op)
    temp = "VIEW" if door.name == "native" else "TEMPORARY VIEW"
    if shape == "top":
        return outcome(lambda: door.run(q))
    if shape == "cte":
        return outcome(lambda: door.run(f"WITH x AS ({q}) SELECT a FROM x"))
    if shape == "scalar":
        return outcome(lambda: door.run(f"SELECT (SELECT count(*) FROM ({q}) x) AS c"))
    if shape == "in":
        return outcome(lambda: door.run(f"SELECT a FROM r WHERE a IN ({q})"))
    if shape == "temp-view":
        name = f"vt_{slot}"
        created = outcome(lambda: door.run(f"CREATE OR REPLACE {temp} {name} AS {q}"))
        read = outcome(lambda: door.run(f"SELECT a FROM {name}"))
        return {**read, "create": created}
    if shape == "persistent-view":
        name = door.persistent_view(f"vp_{slot}")
        created = outcome(lambda: door.run(f"CREATE VIEW {name} AS {table_query(op)}"))
        read = outcome(lambda: door.run(f"SELECT a FROM {name}"))
        return {**read, "create": created}
    if shape == "iceberg-source":
        return outcome(lambda: door.run(table_query(op)))
    if shape == "insert":
        table = f"t_{slot}"
        return write_cell(door, table, f"INSERT INTO ice.ns.{table} {q}", True)
    if shape == "ctas":
        table = f"c_{slot}"
        return write_cell(door, table, f"CREATE TABLE ice.ns.{table}{door.using()} AS {q}", False)
    if shape == "merge":
        table = f"m_{slot}"
        statement = (
            f"MERGE INTO ice.ns.{table} t USING ({q}) s ON t.a = s.a WHEN NOT MATCHED THEN INSERT *"
        )
        return write_cell(door, table, statement, True)
    if shape == "union-nested":
        return outcome(lambda: door.run(f"SELECT a FROM (SELECT 9 AS a UNION ALL ({q})) x"))
    return {**outcome(lambda: door.run(f"EXPLAIN {q}")), "rows": "elided"}


def text_controls(door: Door) -> dict[str, dict[str, Any]]:
    quote = '"' if door.name == "native" else "`"
    cells = {
        "string-literal": "SELECT 'a INTERSECT ALL b' AS s",
        "block-comment": "SELECT 1 AS a /* EXCEPT ALL */",
        "line-comment": "SELECT 1 AS a -- MINUS ALL\n",
        "quoted-ident": f"SELECT 1 AS {quote}except all{quote}",
        "all-prefix-ident": "SELECT a AS all_rows FROM l INTERSECT SELECT a FROM r",
    }
    return {name: outcome(lambda sql=sql: door.run(sql)) for name, sql in cells.items()}


def numbered(side: str) -> str:
    return f"SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) AS n FROM {side}"


def workaround_cells(door: Door) -> dict[str, dict[str, Any]]:
    cells = {
        "numbered-intersect": f"SELECT a FROM ({numbered('l')} INTERSECT {numbered('r')}) x",
        "numbered-except": f"SELECT a FROM ({numbered('l')} EXCEPT {numbered('r')}) x",
    }
    return {name: outcome(lambda sql=sql: door.run(sql)) for name, sql in cells.items()}


def dataframe_cells(door: Door) -> dict[str, dict[str, Any]]:
    if door.name == "native":
        return {}
    left = door.spark.sql("SELECT a FROM l")
    right = door.spark.sql("SELECT a FROM r")
    if door.name == "spark":
        from pyspark.sql import functions as sf
    else:
        from repark.spark import functions as sf

    inline_left = "SELECT a FROM VALUES (1), (1), (1), (2), (2), (4) AS t(a)"
    inline_right = "SELECT a FROM VALUES (1), (2), (2), (3) AS t(a)"
    fragment = f"(SELECT count(*) FROM ({inline_left} INTERSECT ALL {inline_right}) x)"
    in_filter = f"a IN ({inline_left} EXCEPT ALL {inline_right})"
    control = f"(SELECT count(*) FROM ({inline_left} INTERSECT {inline_right}) x)"

    def collect(frame: Any) -> list[tuple[Any, ...]]:
        return [tuple(row) for row in frame.collect()]

    return {
        "df-intersectAll": outcome(lambda: collect(left.intersectAll(right))),
        "df-exceptAll": outcome(lambda: collect(left.exceptAll(right))),
        "df-intersect": outcome(lambda: collect(left.intersect(right))),
        "df-subtract": outcome(lambda: collect(left.subtract(right))),
        "fexpr-scalar": outcome(
            lambda: collect(door.spark.sql("SELECT 1 AS one").select(sf.expr(fragment)))
        ),
        "fexpr-filter-in": outcome(lambda: collect(right.filter(in_filter))),
        "fexpr-filter-col": outcome(lambda: collect(right.filter(sf.expr(in_filter)))),
        "fexpr-control": outcome(
            lambda: collect(door.spark.sql("SELECT 1 AS one").select(sf.expr(control)))
        ),
        "selectexpr-scalar": outcome(lambda: collect(right.selectExpr(fragment))),
    }


def main() -> None:
    door_name, root_arg, out_arg = sys.argv[1], sys.argv[2], sys.argv[3]
    root = Path(root_arg)
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    door = Door(door_name, root)
    door.open()
    door.seed()
    cells: list[dict[str, Any]] = []
    slot = 0
    for op in REFUSED_OPS:
        for shape in SHAPES:
            slot += 1
            cells.append(
                {"group": "refuse", "op": op, "shape": shape, **shape_cell(door, shape, op, slot)}
            )
    for op in SPELLINGS:
        slot += 1
        cells.append(
            {"group": "spelling", "op": op, "shape": "top", **shape_cell(door, "top", op, slot)}
        )
        slot += 1
        cells.append(
            {
                "group": "spelling",
                "op": op,
                "shape": "insert",
                **shape_cell(door, "insert", op, slot),
            }
        )
    for op in CONTROL_OPS:
        for shape in CONTROL_SHAPES:
            slot += 1
            cells.append(
                {"group": "control", "op": op, "shape": shape, **shape_cell(door, shape, op, slot)}
            )
    for name, result in text_controls(door).items():
        cells.append({"group": "text-control", "op": name, "shape": "top", **result})
    for name, result in workaround_cells(door).items():
        cells.append({"group": "workaround", "op": name, "shape": "top", **result})
    for name, result in dataframe_cells(door).items():
        cells.append({"group": "dataframe", "op": name, "shape": "api", **result})
    with Path(out_arg).open("w", encoding="utf-8") as handle:
        for cell in cells:
            handle.write(json.dumps({"door": door_name, **cell}, sort_keys=True) + "\n")
    door.stop()


if __name__ == "__main__":
    main()
