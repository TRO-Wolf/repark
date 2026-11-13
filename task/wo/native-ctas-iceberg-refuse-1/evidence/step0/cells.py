from __future__ import annotations

import json
import os
import re
import shutil
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

SOURCE = (1, 1, 2, 3)
SOURCE2 = (2, 3, 4)
EXISTING = (7, 8)
SHAPES = {
    "literal": ("SELECT 1 AS id", "id INT"),
    "plain": ("SELECT id FROM {c}.ns.src", "id INT"),
    "union": ("SELECT id FROM {c}.ns.src UNION SELECT id FROM {c}.ns.src2", "id INT"),
    "union_all": ("SELECT id FROM {c}.ns.src UNION ALL SELECT id FROM {c}.ns.src2", "id INT"),
    "distinct": ("SELECT DISTINCT id FROM {c}.ns.src", "id INT"),
    "intersect": ("SELECT id FROM {c}.ns.src INTERSECT SELECT id FROM {c}.ns.src2", "id INT"),
    "except": ("SELECT id FROM {c}.ns.src EXCEPT SELECT id FROM {c}.ns.src2", "id INT"),
    "values": ("VALUES (1), (2)", "id INT"),
    "aggregate": ("SELECT id, count(*) AS n FROM {c}.ns.src GROUP BY id", "id INT, n BIGINT"),
    "join": ("SELECT a.id FROM {c}.ns.src a JOIN {c}.ns.src2 b ON a.id = b.id", "id INT"),
}
NATIVE_FORMS = {
    "ctas": "CREATE TABLE {t} AS {q}",
    "ctas_lower": "create table {t} as {q}",
    "ctas_mixed": "Create Table {t} As {q}",
    "if_not_exists": "CREATE TABLE IF NOT EXISTS {t} AS {q}",
    "or_replace": "CREATE OR REPLACE TABLE {t} AS {q}",
    "columns": "CREATE TABLE {t} ({cols}) AS {q}",
    "replace": "REPLACE TABLE {t} AS {q}",
    "temporary": "CREATE TEMPORARY TABLE {t} AS {q}",
    "multi_statement": "CREATE TABLE {t} AS {q}; SELECT 1",
    "explain": "EXPLAIN CREATE TABLE {t} AS {q}",
    "explain_analyze": "EXPLAIN ANALYZE CREATE TABLE {t} AS {q}",
    "prepare": "PREPARE {p} AS CREATE TABLE {t} AS {q}",
}
MEMORY_FORMS = ("ctas", "if_not_exists", "or_replace", "columns", "ctas_lower")
SPARK_FORMS = {
    "ctas": "CREATE TABLE {t} AS {q}",
    "ctas_lower": "create table {t} as {q}",
    "if_not_exists": "CREATE TABLE IF NOT EXISTS {t} AS {q}",
    "or_replace": "CREATE OR REPLACE TABLE {t} AS {q}",
    "replace": "REPLACE TABLE {t} AS {q}",
}
EXISTING_CASES = {
    "or_replace_failing": (
        "CREATE OR REPLACE TABLE {t} AS SELECT 10 / (id - id) AS id FROM {c}.ns.src"
    ),
    "or_replace_succeeding": "CREATE OR REPLACE TABLE {t} AS SELECT id FROM {c}.ns.src",
    "if_not_exists_existing": "CREATE TABLE IF NOT EXISTS {t} AS SELECT id FROM {c}.ns.src",
    "ctas_existing": "CREATE TABLE {t} AS SELECT id FROM {c}.ns.src",
}


def outcome(run: Callable[[], list[tuple[Any, ...]]]) -> dict[str, Any]:
    try:
        rows = run()
    except Exception as error:
        text = " ".join(str(error).split())
        return {"kind": "refused", "class": type(error).__name__, "text": text}
    return {"kind": "rows", "rows": [list(row) for row in rows]}


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


class Door:
    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = root
        self.spark: Any = None
        self.native: Any = None

    def iceberg_catalogs(self) -> tuple[str, ...]:
        return ("ice", "hd") if self.name == "native" else ("hd",)

    def open(self) -> None:
        hadoop = {
            "spark.sql.catalog.hd": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hd.type": "hadoop",
            "spark.sql.catalog.hd.warehouse": str(self.root / "hd"),
        }
        if self.name == "spark":
            from pyspark.sql import SparkSession

            builder = (
                SparkSession.builder.master("local[2]")
                .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config("spark.sql.warehouse.dir", str(self.root / "spark-warehouse"))
                .config("spark.ui.enabled", "false")
            )
            for key, value in hadoop.items():
                builder = builder.config(key, value)
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")
            self.spark.sql("CREATE NAMESPACE hd.ns").collect()
        elif self.name == "spark-door":
            from repark import ReparkSession

            builder = ReparkSession.builder.appName("native-ctas-step0")
            for key, value in hadoop.items():
                builder = builder.config(key, value)
            self.spark = builder.getOrCreate()
            self.spark.sql("CREATE NAMESPACE hd.ns").collect()
        else:
            import repark
            from repark._native import PyReparkSession

            self.native = PyReparkSession.native(config=hadoop)
            self.native.register_memory_catalog("ice", str(self.root / "ice"))
            self.native.create_namespace("ice", "ns", str(self.root / "ice" / "ns"))
            self.native.create_namespace("hd", "ns", None)
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

    def using(self) -> str:
        return "" if self.name == "native" else " USING iceberg"

    def seed(self) -> None:
        for catalog in self.iceberg_catalogs():
            for name, items in (("src", SOURCE), ("src2", SOURCE2)):
                self.run(f"CREATE TABLE {catalog}.ns.{name} (id INT){self.using()}")
                body = ", ".join(f"({item})" for item in items)
                self.run(f"INSERT INTO {catalog}.ns.{name} VALUES {body}")

    def table_dir(self, target: str) -> Path | None:
        parts = target.lower().split(".")
        if len(parts) != 3 or parts[0] not in ("ice", "hd"):
            return None
        return self.root / parts[0] / parts[1] / parts[2]

    def state(self, target: str) -> dict[str, Any]:
        read = outcome(lambda: self.run(f"SELECT * FROM {target}"))
        state: dict[str, Any] = {"readable": read["kind"] == "rows"}
        if read["kind"] == "rows":
            state["rows"] = sorted(read["rows"])
        else:
            state["read_error"] = read["text"][:160]
        table_dir = self.table_dir(target)
        if table_dir is not None:
            files = sorted((table_dir / "metadata").glob("*.metadata.json"), key=metadata_order)
            state["metadata_files"] = len(files)
            if files:
                document = json.loads(files[-1].read_text(encoding="utf-8"))
                state["metadata_latest"] = files[-1].name
                state["snapshots"] = len(document.get("snapshots") or [])
                state["schema"] = [
                    field["name"] for field in document["schemas"][-1].get("fields", [])
                ]
        return state

    def stop(self) -> None:
        if self.spark is not None:
            self.spark.stop()


def write_cell(door: Door, target: str, statements: list[str]) -> dict[str, Any]:
    before = door.state(target)
    results = [outcome(lambda sql=sql: door.run(sql)) for sql in statements]
    cell = {"statements": statements, "before": before, "after": door.state(target)}
    for index, result in enumerate(results):
        suffix = "" if index == 0 else f"_{index}"
        for key, value in result.items():
            cell[f"{key}{suffix}"] = value
    return cell


class Slots:
    def __init__(self) -> None:
        self.count = 0

    def next(self, prefix: str) -> str:
        self.count += 1
        return f"{prefix}{self.count}"


def iceberg_ctas_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    forms = NATIVE_FORMS if door.name == "native" else SPARK_FORMS
    for catalog in door.iceberg_catalogs():
        for form, template in forms.items():
            for shape, (query, cols) in SHAPES.items():
                name = slots.next("t")
                target = f"{catalog}.ns.{name}"
                statement = template.format(
                    t=target, q=query.format(c=catalog), cols=cols, p=f"p_{name}"
                )
                statements = [statement]
                if form == "prepare":
                    statements.append(f"EXECUTE p_{name}")
                cell = write_cell(door, target, statements)
                cells.append(
                    {"group": "iceberg-ctas", "target": catalog, "form": form, "shape": shape}
                    | cell
                )
    if door.name == "native":
        for shape in ("literal", "distinct"):
            name = slots.next("t")
            query = SHAPES[shape][0].format(c="ice")
            target = f"Ice.NS.{name}"
            cell = write_cell(door, target, [f"CREATE TABLE {target} AS {query}"])
            cells.append(
                {"group": "iceberg-ctas", "target": "ice", "form": "mixed_case_target"}
                | {"shape": shape}
                | cell
            )
    return cells


def workaround_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for catalog in door.iceberg_catalogs():
        for shape, (query, cols) in SHAPES.items():
            name = slots.next("w")
            target = f"{catalog}.ns.{name}"
            statements = [
                f"CREATE TABLE {target} ({cols}){door.using()}",
                f"INSERT INTO {target} {query.format(c=catalog)}",
            ]
            cell = write_cell(door, target, statements)
            cells.append(
                {"group": "workaround", "target": catalog, "form": "create_then_insert"}
                | {"shape": shape}
                | cell
            )
    return cells


def memory_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for prefix in ("", "datafusion.public."):
        for form in MEMORY_FORMS:
            for shape, (query, cols) in SHAPES.items():
                name = slots.next("m")
                target = f"{prefix}{name}"
                statement = NATIVE_FORMS[form].format(t=target, q=query.format(c="ice"), cols=cols)
                cell = write_cell(door, target, [statement])
                kind = "memory-qualified" if prefix else "memory-bare"
                cells.append(
                    {"group": "memory-ctas", "target": kind, "form": form, "shape": shape} | cell
                )
    return cells


def existing_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for catalog in door.iceberg_catalogs():
        for case, template in EXISTING_CASES.items():
            name = slots.next("e")
            target = f"{catalog}.ns.{name}"
            door.run(f"CREATE TABLE {target} (id INT){door.using()}")
            body = ", ".join(f"({item})" for item in EXISTING)
            door.run(f"INSERT INTO {target} VALUES {body}")
            cell = write_cell(door, target, [template.format(t=target, c=catalog)])
            cells.append(
                {"group": "existing", "target": catalog, "form": case, "shape": "-"} | cell
            )
    return cells


def control_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for catalog in door.iceberg_catalogs():
        name = slots.next("c")
        target = f"{catalog}.ns.{name}"
        cell = write_cell(door, target, [f"CREATE TABLE {target} (id INT){door.using()}"])
        cells.append(
            {"group": "control", "target": catalog, "form": "create_no_query", "shape": "-"} | cell
        )
        name = slots.next("c")
        target = f"{catalog}.ns.{name}"
        door.run(f"CREATE TABLE {target} (id INT){door.using()}")
        cell = write_cell(
            door,
            target,
            [
                f"INSERT INTO {target} SELECT id FROM {catalog}.ns.src "
                f"UNION SELECT id FROM {catalog}.ns.src2"
            ],
        )
        cells.append(
            {"group": "control", "target": catalog, "form": "insert_select", "shape": "union"}
            | cell
        )
        if door.name == "native" and catalog == "ice":
            name = slots.next("v")
            target = f"{catalog}.ns.{name}"
            cell = write_cell(
                door, target, [f"CREATE VIEW {target} AS SELECT DISTINCT id FROM {catalog}.ns.src"]
            )
            cells.append(
                {"group": "control", "target": catalog, "form": "create_view", "shape": "distinct"}
                | cell
            )
    if door.name == "native":
        name = slots.next("v")
        cell = write_cell(door, name, [f"CREATE VIEW {name} AS SELECT DISTINCT id FROM ice.ns.src"])
        cells.append(
            {"group": "control", "target": "memory-bare", "form": "create_view"}
            | {"shape": "distinct"}
            | cell
        )
    return cells


def external_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    if door.name != "native":
        return []
    import pyarrow as pa
    import pyarrow.parquet as pq

    data = door.root / "ice" / "extdata"
    data.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": pa.array([5, 6], pa.int32())}), data / "part.parquet")
    cells: list[dict[str, Any]] = []
    for catalog in ("ice", "hd"):
        name = slots.next("x")
        target = f"{catalog}.ns.{name}"
        statement = f"CREATE EXTERNAL TABLE {target} STORED AS PARQUET LOCATION '{data}/'"
        cell = write_cell(door, target, [statement])
        cells.append(
            {"group": "iceberg-ctas", "target": catalog, "form": "external", "shape": "-"} | cell
        )
    return cells


def default_catalog_cells(door: Door, slots: Slots) -> list[dict[str, Any]]:
    if door.name != "native":
        return []
    import repark
    from repark._native import PyReparkSession

    root = door.root / "dc"
    session = PyReparkSession.native()
    session.register_memory_catalog("ice", str(root / "ice"))
    session.create_namespace("ice", "ns", str(root / "ice" / "ns"))
    saved = repark._ANSI_NATIVE
    repark._ANSI_NATIVE = session
    shadow = Door("native", root)
    shadow.native = session
    shadow.run("CREATE TABLE ice.ns.src (id INT)")
    shadow.run("INSERT INTO ice.ns.src VALUES (1), (1), (2), (3)")
    setting = outcome(lambda: shadow.run("SET datafusion.catalog.default_catalog = 'ice'"))
    setting2 = outcome(lambda: shadow.run("SET datafusion.catalog.default_schema = 'ns'"))
    cells: list[dict[str, Any]] = []
    for shape in ("literal", "distinct"):
        name = slots.next("d")
        query = SHAPES[shape][0].format(c="ice")
        cell = write_cell(shadow, f"ice.ns.{name}", [f"CREATE TABLE {name} AS {query}"])
        cells.append(
            {"group": "iceberg-ctas", "target": "ice", "form": "default_catalog_unqualified"}
            | {"shape": shape, "set": [setting, setting2]}
            | cell
        )
    repark._ANSI_NATIVE = saved
    return cells


def main() -> None:
    door_name, root_arg, out_arg = sys.argv[1], sys.argv[2], sys.argv[3]
    root = Path(root_arg)
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    door = Door(door_name, root)
    door.open()
    door.seed()
    slots = Slots()
    cells = iceberg_ctas_cells(door, slots)
    cells += external_cells(door, slots)
    cells += default_catalog_cells(door, slots)
    cells += workaround_cells(door, slots)
    cells += existing_cells(door, slots)
    cells += control_cells(door, slots)
    if door.name == "native":
        cells += memory_cells(door, slots)
    with Path(out_arg).open("w", encoding="utf-8") as handle:
        for cell in cells:
            handle.write(json.dumps({"door": door_name, **cell}, sort_keys=True, default=str))
            handle.write("\n")
    door.stop()


if __name__ == "__main__":
    main()
