from __future__ import annotations

import datetime
import decimal
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
CELLS = HERE / "struct_domain_cells.jsonl"
CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")
WRITES = ("DELETE", "UPDATE", "MERGE", "INSERT")


def value_of(value: Any) -> Any:
    """Return a JSON-comparable form of one collected value."""
    if hasattr(value, "asDict"):
        return {key: value_of(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [value_of(item) for item in value]
    if isinstance(value, dict):
        return {str(key): value_of(item) for key, item in sorted(value.items())}
    if isinstance(value, (datetime.date, datetime.datetime, decimal.Decimal)):
        return str(value)
    return value


def rows_of(frame: Any) -> list[list[Any]]:
    """Collect a frame into sorted JSON-comparable rows."""
    rows = [[value_of(item) for item in row] for row in frame.collect()]
    return sorted(rows, key=json.dumps)


def error_of(error: BaseException) -> str:
    """Return the error class of a refusal, or its first words when it has none."""
    text = str(error)
    found = CLASS.search(text)
    return found.group(1) if found else " ".join(text.split())[:160]


class Grid:
    """Run struct-domain cells on one session, with a fresh table per write."""

    def __init__(self, spark: Any, functions: Any, namespace: str) -> None:
        from struct_domain_grid import FIXTURES

        self.spark = spark
        self.functions = functions
        self.namespace = namespace
        self.fixtures = FIXTURES
        self.count = 0
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}").collect()
        self.wide = self.fresh("W")

    def fresh(self, fixture: str) -> str:
        """Create and seed a new table of the fixture and return its name."""
        self.count += 1
        name = f"{self.namespace}.t{self.count}"
        create, seed, _ = self.fixtures[fixture]
        self.spark.sql(create.replace("{t}", name)).collect()
        self.spark.sql(seed.replace("{t}", name)).collect()
        return name

    def run(self, cell: dict[str, Any]) -> dict[str, Any]:
        """Return the kind, answer and readback of one cell."""
        stmt = cell["stmt"].replace("{W}", self.wide)
        fixture = next((name for name in ("T", "VI", "VL") if "{" + name + "}" in stmt), None)
        target = None
        if fixture is not None:
            target = self.fresh("W" if fixture == "T" else fixture)
            stmt = stmt.replace("{" + fixture + "}", target)
        kind, answer = self.answer(cell["seat"], stmt)
        readback = None
        if target is not None:
            readback_sql = self.fixtures["W" if fixture == "T" else fixture][2]
            readback = rows_of(self.spark.sql(readback_sql.replace("{t}", target)))
        return {"kind": kind, "answer": answer, "readback": readback}

    def answer(self, seat: str, stmt: str) -> tuple[str, Any]:
        """Run one statement or frame and return its kind and answer."""
        try:
            if seat.startswith("df_"):
                scope = {
                    "F": self.functions,
                    "w": self.spark.table(self.wide),
                    "insert_into": lambda name, frame: frame.write.insertInto(name),
                    "append": lambda name, frame: frame.write.mode("append").saveAsTable(name),
                }
                frame = eval(stmt, scope)
                if frame is None:
                    return "ok", "ok"
                return "rows", rows_of(frame)
            if seat == "view":
                self.spark.sql(stmt).collect()
                return "rows", rows_of(self.spark.sql("SELECT * FROM sdv"))
            if stmt.startswith(WRITES):
                self.spark.sql(stmt).collect()
                return "ok", "ok"
            return "rows", rows_of(self.spark.sql(stmt))
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            if type(error).__name__ == "PanicException":
                return "error", "PANIC " + error_of(error)
            return "error", error_of(error)


def _spark_session(warehouse: str, util_dir: str) -> tuple[Any, Any]:
    sys.path.insert(0, util_dir)
    from pyspark.sql import functions
    from spark_util import spark_session

    return spark_session(warehouse, "struct_domain"), functions


def _repark_session(warehouse: str) -> tuple[Any, Any]:
    from repark import ReparkSession
    from repark.spark import functions

    spark = (
        ReparkSession.builder.appName("struct_domain")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark, functions


def main() -> None:
    """Measure the grid: `spark <warehouse> <out> <util dir>` or `repark <warehouse> <out>`."""
    sys.path.insert(0, str(HERE))
    from struct_domain_grid import cells

    engine, warehouse, out = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    if engine == "spark":
        spark, functions = _spark_session(warehouse, sys.argv[4])
    else:
        spark, functions = _repark_session(warehouse)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    grid = Grid(spark, functions, "hd.sd")
    lines = []
    for cell in cells():
        result = grid.run(cell)
        record = dict(cell)
        record["spark_kind"] = result["kind"]
        record["spark"] = result["answer"]
        record["readback"] = result["readback"]
        lines.append(json.dumps(record, sort_keys=True))
    out.write_text("\n".join(lines) + "\n")
    if engine == "spark":
        spark.stop()


if __name__ == "__main__":
    main()
