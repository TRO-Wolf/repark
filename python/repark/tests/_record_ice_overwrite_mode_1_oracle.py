"""Record or re-check the ICE-OVERWRITE-MODE-1 Spark oracle cells on live PySpark.

Usage (a PySpark 4.1.2 interpreter with a JVM on PATH)::

    python python/repark/tests/_record_ice_overwrite_mode_1_oracle.py \
        --warehouse <empty-dir> record
    ... check

``record`` replays the 44 overwrite shapes on format versions 2 and 3 (88
cells) against live Spark 4.1.2 + Iceberg 1.11.0 over an InMemory catalog and
prints the fixture JSON to stdout. ``check`` replays the same cells and exits
non-zero naming the first mismatch against the committed
``ice_overwrite_mode_1_spark_oracle.json``. Each cell records the table rows
and the snapshot history (operation plus seven summary counters), or the
error class, condition, and SQLSTATE. The shapes and the cell runner are
shared with ``test_ice_overwrite_mode_1.py``, which replays them on RePark.
The Iceberg runtime GAV comes from :mod:`_oracle_pins` (CP-8).

pins: ice-overwrite-mode-1/C-001
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_overwrite_mode_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
MODE_KEY = "spark.sql.sources.partitionOverwriteMode"
DYNAMIC = {MODE_KEY: "dynamic"}
STATIC = {MODE_KEY: "static"}
ONE_LEVEL = "id BIGINT, data STRING, cat STRING"
TWO_LEVEL = "id BIGINT, data STRING, cat STRING, sub STRING"
BY_CAT = "PARTITIONED BY (cat)"
BY_CAT_SUB = "PARTITIONED BY (cat, sub)"
SUMMARY_KEYS = (
    "added-records",
    "deleted-records",
    "total-records",
    "changed-partition-count",
    "added-data-files",
    "deleted-data-files",
    "total-data-files",
)
PDYN = "INSERT OVERWRITE {T} PARTITION (cat) SELECT 9, 'z', 'x'"
PSTATIC = "INSERT OVERWRITE {T} PARTITION (cat = 'x') SELECT 9, 'z'"
NOCLAUSE = "INSERT OVERWRITE {T} SELECT 9, 'z', 'x'"
EMPTY = "INSERT OVERWRITE {T} SELECT 9, 'z', 'x' WHERE false"
MIXED = "INSERT OVERWRITE {T} PARTITION (cat = 'x', sub) SELECT 9, 'z', 'p'"
MIXED_EMPTY = MIXED + " WHERE false"
BY_NAME_MIXED = (
    "INSERT OVERWRITE {T} PARTITION (cat = 'x', sub) BY NAME "
    "SELECT 'z' AS data, 9 AS id, 'p' AS sub"
)
BY_BUCKET = "PARTITIONED BY (bucket(4, id))"


class OverwriteShape(BaseModel):
    """One overwrite shape: a SQL statement or a DataFrame writer action on a seeded table."""

    model_config = ConfigDict(frozen=True)

    key: str
    title: str
    statement: str = ""
    action: str = ""
    partition: str = BY_CAT
    conf: dict[str, str] = {}
    two_level: bool = False
    group: str = ""
    columns: str = ""
    seed: str = ""
    setup: tuple[str, ...] = ()

    def cell_id(self, version: int) -> str:
        """Return the fixture cell id for this shape on ``version``."""
        if self.group:
            return f"{self.group}-{self.key}-V{version}"
        family = "DF" if self.action else "SQL"
        return f"OW-{family}-{self.key}-V{version}"

    def column_ddl(self) -> str:
        """Return the table's column list."""
        return self.columns or (TWO_LEVEL if self.two_level else ONE_LEVEL)


SHAPES: tuple[OverwriteShape, ...] = (
    OverwriteShape(key="PDYN-STATIC", title="PARTITION (cat), static default", statement=PDYN),
    OverwriteShape(
        key="PDYN-STATIC-EXPL",
        title="PARTITION (cat), static explicit",
        statement=PDYN,
        conf=STATIC,
    ),
    OverwriteShape(
        key="PDYN-DYNMODE", title="PARTITION (cat), dynamic mode", statement=PDYN, conf=DYNAMIC
    ),
    OverwriteShape(key="PSTATIC", title="PARTITION (cat='x')", statement=PSTATIC),
    OverwriteShape(
        key="PSTATIC-DYNMODE",
        title="PARTITION (cat='x'), dynamic mode",
        statement=PSTATIC,
        conf=DYNAMIC,
    ),
    OverwriteShape(
        key="PSTATIC-NEWPART",
        title="PARTITION (cat='w') new partition",
        statement="INSERT OVERWRITE {T} PARTITION (cat = 'w') SELECT 9, 'z'",
    ),
    OverwriteShape(key="NOCLAUSE-STATIC", title="no PARTITION clause, static", statement=NOCLAUSE),
    OverwriteShape(
        key="NOCLAUSE-DYNMODE",
        title="no PARTITION clause, dynamic mode",
        statement=NOCLAUSE,
        conf=DYNAMIC,
    ),
    OverwriteShape(key="EMPTY-STATIC", title="empty source, static", statement=EMPTY),
    OverwriteShape(
        key="EMPTY-DYNMODE", title="empty source, dynamic mode", statement=EMPTY, conf=DYNAMIC
    ),
    OverwriteShape(
        key="PDYN-EMPTY-STATIC",
        title="PARTITION (cat) empty source, static",
        statement="INSERT OVERWRITE {T} PARTITION (cat) SELECT 9, 'z', 'x' WHERE false",
    ),
    OverwriteShape(
        key="PSTATIC-EMPTY",
        title="PARTITION (cat='x') empty source",
        statement="INSERT OVERWRITE {T} PARTITION (cat = 'x') SELECT 9, 'z' WHERE false",
    ),
    OverwriteShape(
        key="UNPART-PDYN-ERR",
        title="unpartitioned table with PARTITION (cat)",
        statement=PDYN,
        partition="",
    ),
    OverwriteShape(
        key="TWO-MIXED-STATIC",
        title="PARTITION (cat='x', sub), static",
        statement=MIXED,
        partition=BY_CAT_SUB,
        two_level=True,
    ),
    OverwriteShape(
        key="TWO-MIXED-DYNMODE",
        title="PARTITION (cat='x', sub), dynamic mode",
        statement=MIXED,
        partition=BY_CAT_SUB,
        conf=DYNAMIC,
        two_level=True,
    ),
    OverwriteShape(
        key="TWO-BOTH-DYN-STATIC",
        title="PARTITION (cat, sub), static",
        statement="INSERT OVERWRITE {T} PARTITION (cat, sub) SELECT 9, 'z', 'x', 'p'",
        partition=BY_CAT_SUB,
        two_level=True,
    ),
    OverwriteShape(
        key="TRANSFORM-STATIC",
        title="bucket table, no clause, static",
        statement=NOCLAUSE,
        partition="PARTITIONED BY (bucket(4, id))",
    ),
    OverwriteShape(
        key="TRANSFORM-DYNMODE",
        title="bucket table, no clause, dynamic mode",
        statement=NOCLAUSE,
        partition="PARTITIONED BY (bucket(4, id))",
        conf=DYNAMIC,
    ),
    OverwriteShape(
        key="PDYN-TABLE-KW",
        title="INSERT OVERWRITE TABLE t PARTITION (cat), static",
        statement="INSERT OVERWRITE TABLE {T} PARTITION (cat) SELECT 9, 'z', 'x'",
    ),
    OverwriteShape(key="INSERTINTO-STATIC", title="insertInto overwrite", action="insert_into"),
    OverwriteShape(
        key="INSERTINTO-DYNMODE",
        title="insertInto overwrite, dynamic session",
        action="insert_into",
        conf=DYNAMIC,
    ),
    OverwriteShape(
        key="OPT-OWMODE-DYN",
        title="overwrite-mode=dynamic insertInto overwrite",
        action="option_dynamic_insert_into",
    ),
    OverwriteShape(
        key="OPT-OWMODE-STATIC-DYNSESSION",
        title="overwrite-mode=static, dynamic session",
        action="option_static_insert_into",
        conf=DYNAMIC,
    ),
    OverwriteShape(
        key="OPT-PARTOWMODE-DYN",
        title="partitionOverwriteMode=dynamic writer option",
        action="option_partition_mode_insert_into",
    ),
    OverwriteShape(
        key="OPT-OWMODE-DYN-MODE-OW",
        title="overwrite-mode=dynamic + mode(overwrite).insertInto",
        action="option_dynamic_mode_overwrite",
    ),
    OverwriteShape(
        key="OPT-OWMODE-BAD", title="overwrite-mode=bogus", action="option_bogus_insert_into"
    ),
    OverwriteShape(
        key="WRITETO-OVERWRITEPARTS",
        title="writeTo.overwritePartitions",
        action="overwrite_partitions",
    ),
    OverwriteShape(
        key="WRITETO-OVERWRITEPARTS-OPT",
        title="writeTo.option(overwrite-mode=static).overwritePartitions",
        action="option_static_overwrite_partitions",
    ),
    OverwriteShape(
        key="SAVEASTABLE-OW-OPT",
        title="overwrite-mode=dynamic saveAsTable overwrite",
        action="option_dynamic_save_as_table",
    ),
    OverwriteShape(
        key="SAVEASTABLE-OW-DYNSESSION",
        title="saveAsTable overwrite, dynamic session",
        action="save_as_table",
        conf=DYNAMIC,
    ),
    OverwriteShape(
        group="OW2",
        key="R2-PDYN-EMPTY-DYN",
        title="PARTITION (cat) empty source, dynamic mode",
        statement="INSERT OVERWRITE {T} PARTITION (cat) SELECT 9, 'z', 'x' WHERE false",
        conf=DYNAMIC,
    ),
    OverwriteShape(
        group="OW2",
        key="R2-MIXED-EMPTY-DYN",
        title="PARTITION (cat='x', sub) empty source, dynamic mode",
        statement=MIXED_EMPTY,
        partition=BY_CAT_SUB,
        conf=DYNAMIC,
        two_level=True,
    ),
    OverwriteShape(
        group="OW2",
        key="R2-MIXED-EMPTY-STA",
        title="PARTITION (cat='x', sub) empty source, static mode",
        statement=MIXED_EMPTY,
        partition=BY_CAT_SUB,
        two_level=True,
    ),
    OverwriteShape(
        group="OW2",
        key="R1-BUCKET-STATIC",
        title="PARTITION (id = 1) on a bucket(4, id) table, static mode",
        statement="INSERT OVERWRITE {T} PARTITION (id = 1) SELECT 'z', 'x'",
        partition=BY_BUCKET,
    ),
    OverwriteShape(
        group="OW2",
        key="R1-BUCKET-DYN",
        title="PARTITION (id) on a bucket(4, id) table, dynamic mode",
        statement="INSERT OVERWRITE {T} PARTITION (id) SELECT 'z', 'x', 1",
        partition=BY_BUCKET,
        conf=DYNAMIC,
    ),
    OverwriteShape(
        group="OW2",
        key="R1-DAYS-STATIC",
        title="PARTITION (ts = ...) on a days(ts) table",
        statement=(
            "INSERT OVERWRITE {T} PARTITION (ts = TIMESTAMP'2024-01-01 00:00:00') SELECT 9, 'z'"
        ),
        partition="PARTITIONED BY (days(ts))",
        columns="id BIGINT, data STRING, ts TIMESTAMP",
        seed="(1, 'a', TIMESTAMP'2024-01-01 05:00:00'), (2, 'b', TIMESTAMP'2024-01-02 05:00:00')",
    ),
    OverwriteShape(
        group="OW2",
        key="R1-IDENT-AND-BUCKET",
        title="PARTITION (cat = 'x') on (cat, bucket(2, id))",
        statement="INSERT OVERWRITE {T} PARTITION (cat = 'x') SELECT 9, 'z'",
        partition="PARTITIONED BY (cat, bucket(2, id))",
    ),
    OverwriteShape(
        group="OW2",
        key="R3-BYNAME-MIXED-STA",
        title="BY NAME mixed PARTITION (cat='x', sub), static mode",
        statement=BY_NAME_MIXED,
        partition=BY_CAT_SUB,
        two_level=True,
    ),
    OverwriteShape(
        group="OW2",
        key="R3-BYNAME-MIXED-DYN",
        title="BY NAME mixed PARTITION (cat='x', sub), dynamic mode",
        statement=BY_NAME_MIXED,
        partition=BY_CAT_SUB,
        conf=DYNAMIC,
        two_level=True,
    ),
    OverwriteShape(
        group="OW2",
        key="R3-BYNAME-STATIC",
        title="BY NAME PARTITION (cat='x'), static mode",
        statement="INSERT OVERWRITE {T} PARTITION (cat = 'x') BY NAME SELECT 'z' AS data, 9 AS id",
    ),
    OverwriteShape(
        group="OW2",
        key="NULL-STATIC",
        title="PARTITION (cat = NULL), static mode",
        statement="INSERT OVERWRITE {T} PARTITION (cat = NULL) SELECT 9, 'z'",
        setup=("INSERT INTO {T} VALUES (5, 'n', NULL)",),
    ),
    OverwriteShape(
        group="OW2",
        key="CAST-STATIC-DATE",
        title="PARTITION (d = '2024-01-01') on a DATE partition",
        statement="INSERT OVERWRITE {T} PARTITION (d = '2024-01-01') SELECT 9, 'z'",
        partition="PARTITIONED BY (d)",
        columns="id BIGINT, data STRING, d DATE",
        seed="(1, 'a', DATE'2024-01-01'), (2, 'b', DATE'2024-01-02')",
    ),
    OverwriteShape(
        group="OW2",
        key="CASE-STATIC",
        title="PARTITION (CAT = 'x') upper-case column",
        statement="INSERT OVERWRITE {T} PARTITION (CAT = 'x') SELECT 9, 'z'",
    ),
    OverwriteShape(
        group="OW2",
        key="OPT-BOTH-SQL",
        title="session mode DYNAMIC upper-case + PARTITION (cat)",
        statement=PDYN,
        conf={MODE_KEY: "DYNAMIC"},
    ),
)

VERSIONS = (2, 3)


def insert_into(frame: Any, table: str) -> None:
    """Overwrite through ``insertInto(t, overwrite=True)``."""
    frame.write.insertInto(table, overwrite=True)


def option_dynamic_insert_into(frame: Any, table: str) -> None:
    """Overwrite through ``insertInto`` with ``overwrite-mode=dynamic``."""
    frame.write.format("iceberg").option("overwrite-mode", "dynamic").insertInto(
        table, overwrite=True
    )


def option_static_insert_into(frame: Any, table: str) -> None:
    """Overwrite through ``insertInto`` with ``overwrite-mode=static``."""
    frame.write.format("iceberg").option("overwrite-mode", "static").insertInto(
        table, overwrite=True
    )


def option_partition_mode_insert_into(frame: Any, table: str) -> None:
    """Overwrite through ``insertInto`` with the ``partitionOverwriteMode`` writer option."""
    frame.write.option("partitionOverwriteMode", "dynamic").insertInto(table, overwrite=True)


def option_dynamic_mode_overwrite(frame: Any, table: str) -> None:
    """Overwrite through ``mode("overwrite").insertInto`` with ``overwrite-mode=dynamic``."""
    frame.write.option("overwrite-mode", "dynamic").mode("overwrite").insertInto(table)


def option_bogus_insert_into(frame: Any, table: str) -> None:
    """Overwrite through ``insertInto`` with an unknown ``overwrite-mode`` value."""
    frame.write.option("overwrite-mode", "bogus").insertInto(table, overwrite=True)


def overwrite_partitions(frame: Any, table: str) -> None:
    """Overwrite through ``writeTo(t).overwritePartitions()``."""
    frame.writeTo(table).overwritePartitions()


def option_static_overwrite_partitions(frame: Any, table: str) -> None:
    """Overwrite through ``overwritePartitions`` with ``overwrite-mode=static``."""
    frame.writeTo(table).option("overwrite-mode", "static").overwritePartitions()


def option_dynamic_save_as_table(frame: Any, table: str) -> None:
    """Overwrite through ``saveAsTable`` with ``overwrite-mode=dynamic``."""
    frame.write.format("iceberg").option("overwrite-mode", "dynamic").mode("overwrite").saveAsTable(
        table
    )


def save_as_table(frame: Any, table: str) -> None:
    """Overwrite through ``mode("overwrite").saveAsTable``."""
    frame.write.format("iceberg").mode("overwrite").saveAsTable(table)


ACTIONS: dict[str, Callable[[Any, str], None]] = {
    "insert_into": insert_into,
    "option_dynamic_insert_into": option_dynamic_insert_into,
    "option_static_insert_into": option_static_insert_into,
    "option_partition_mode_insert_into": option_partition_mode_insert_into,
    "option_dynamic_mode_overwrite": option_dynamic_mode_overwrite,
    "option_bogus_insert_into": option_bogus_insert_into,
    "overwrite_partitions": overwrite_partitions,
    "option_static_overwrite_partitions": option_static_overwrite_partitions,
    "option_dynamic_save_as_table": option_dynamic_save_as_table,
    "save_as_table": save_as_table,
}


def table_name(shape: OverwriteShape, version: int) -> str:
    """Return the per-cell table name in catalog ``sc``."""
    return "sc.ns.t_" + shape.cell_id(version).lower().replace("-", "_")


def seed_table(session: Any, shape: OverwriteShape, version: int) -> str:
    """Create and seed the cell's table; return its name."""
    table = table_name(shape, version)
    session.sql(
        f"CREATE TABLE {table} ({shape.column_ddl()}) USING iceberg {shape.partition} "
        f"TBLPROPERTIES ('format-version'='{version}')"
    ).collect()
    if shape.seed:
        rows = shape.seed
    elif shape.two_level:
        rows = "(1, 'a', 'x', 'p'), (2, 'b', 'y', 'p'), (3, 'c', 'x', 'q'), (4, 'd', 'y', 'q')"
    else:
        rows = "(1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"
    session.sql(f"INSERT INTO {table} VALUES {rows}").collect()
    for statement in shape.setup:
        session.sql(statement.format(T=table)).collect()
    return table


def apply_shape(session: Any, shape: OverwriteShape, table: str) -> None:
    """Run the shape's overwrite against ``table``."""
    if shape.statement:
        session.sql(shape.statement.format(T=table)).collect()
        return
    frame = session.createDataFrame([(7, "g", "x"), (8, "h", "w")], ONE_LEVEL)
    ACTIONS[shape.action](frame, table)


def plain_value(value: Any) -> Any:
    """Return ``value`` with dates and timestamps as ISO strings."""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def table_rows(session: Any, table: str) -> list[list[Any]]:
    """Return every row of ``table`` as lists of plain values sorted by ``repr``."""
    rows = session.sql(f"SELECT * FROM {table}").collect()
    return sorted(([plain_value(value) for value in row] for row in rows), key=repr)


def snapshot_history(session: Any, table: str) -> list[list[Any]]:
    """Return ``[operation, {summary counter: value}]`` per snapshot in commit order."""
    rows = session.sql(
        f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    history: list[list[Any]] = []
    for row in rows:
        summary = dict(row[1])
        kept = {key: summary[key] for key in SUMMARY_KEYS if summary.get(key) is not None}
        history.append([row[0], dict(sorted(kept.items()))])
    return history


def error_shape(error: BaseException) -> dict[str, Any]:
    """Return the class, condition, SQLSTATE, and first message line of ``error``."""
    condition = getattr(error, "getCondition", None)
    sql_state = getattr(error, "getSqlState", None)
    return {
        "error_class": type(error).__name__,
        "condition": condition() if callable(condition) else None,
        "sql_state": sql_state() if callable(sql_state) else None,
        "message": str(error).splitlines()[0] if str(error) else "",
    }


def run_cell(session: Any, shape: OverwriteShape, version: int) -> dict[str, Any]:
    """Seed, overwrite, and observe one cell under the shape's session conf."""
    for key, value in shape.conf.items():
        session.conf.set(key, value)
    try:
        table = seed_table(session, shape, version)
        try:
            apply_shape(session, shape, table)
        except Exception as error:
            return {"status": "error", **error_shape(error)}
        return {
            "status": "ok",
            "data": table_rows(session, table),
            "snapshots": snapshot_history(session, table),
        }
    finally:
        for key in shape.conf:
            session.conf.unset(key)


def _spark_session(warehouse: Path) -> Any:
    """Build the live oracle session over an InMemory Iceberg catalog."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("ice-overwrite-mode-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def record_all(warehouse: Path) -> dict[str, Any]:
    """Replay every cell on live Spark and return the fixture document."""
    warehouse.mkdir(parents=True, exist_ok=True)
    session = _spark_session(warehouse)
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
        cells = {
            shape.cell_id(version): run_cell(session, shape, version)
            for shape in SHAPES
            for version in VERSIONS
        }
    finally:
        session.stop()
    return {"provenance": provenance(len(cells)), "cells": cells}


def provenance(cell_count: int) -> dict[str, Any]:
    """Return the fixture's provenance block for ``cell_count`` cells."""
    return {
        "spark": "4.1.2",
        "iceberg": "1.11.0",
        "catalog": "InMemoryCatalog",
        "recorded": "2026-09-19",
        "cells": cell_count,
        "shapes": f"{len(SHAPES)} shapes x format v2 and v3",
        "seed": "(1,a,x),(2,b,y),(3,c,x) PARTITIONED BY (cat); two-level tables add sub: "
        "(1,a,x,p),(2,b,y,p),(3,c,x,q),(4,d,y,q) PARTITIONED BY (cat, sub); a shape's "
        "own columns, seed and setup statements replace them",
        "dataframe_source": "(7,g,x),(8,h,w) as id BIGINT, data STRING, cat STRING",
        "summary_keys": list(SUMMARY_KEYS),
        "ow2_cells": "the 28 OW2 cells were measured 2026-09-19 by an independent harness "
        "recording on the same Spark and Iceberg pins, folded in with this recorder's cell "
        "shape, and are re-derived by its check mode",
    }


def check_against_fixture(derived: dict[str, Any]) -> list[str]:
    """Return one line per cell whose live answer differs from the committed fixture."""
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))["cells"]
    mismatches = [
        f"{cell}: want={json.dumps(want)[:400]} got={json.dumps(derived['cells'].get(cell))[:400]}"
        for cell, want in expected.items()
        if derived["cells"].get(cell) != want
    ]
    mismatches.extend(
        f"{cell}: extra cell not in fixture" for cell in derived["cells"] if cell not in expected
    )
    return mismatches


def main(argv: list[str]) -> int:
    """Record the fixture to stdout or check it against live Spark."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args(argv)
    derived = record_all(args.warehouse)
    if args.mode == "record":
        print(json.dumps(derived, indent=1))
        return 0
    mismatches = check_against_fixture(derived)
    if mismatches:
        print(f"{len(mismatches)} oracle cell(s) drifted from {FIXTURE.name}:", flush=True)
        for mismatch in mismatches:
            print(mismatch, flush=True)
        return 1
    print(f"oracle fixture {FIXTURE.name} reproduces on live Spark", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
