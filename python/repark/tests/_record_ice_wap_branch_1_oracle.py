"""Record or re-check the ICE-WAP-BRANCH-1 Spark oracle cells on live PySpark 4.1.2.

Usage (PySpark 4.1.2 interpreter, e.g. ``/tmp/sparkenv/bin/python``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_wap_branch_1_oracle.py \
        --warehouse /tmp/wap-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell and exits
non-zero naming the first mismatch against the committed
``ice_wap_branch_1_spark_oracle.json``. The Iceberg runtime GAV comes from
:mod:`_oracle_pins` (CP-8: never restate a version literal).

Each cell seeds ``(1,'a','x'),(2,'b','y')`` into a format-2 table, optionally creates the
``audit`` branch, sets the session confs, runs the cell's steps, and records five
observations: ``session-read`` (a plain read while the conf is set), ``refs`` (name, type,
and the snapshot's position in timestamp order), ``main``, ``branch``, and
``plain-read-after-unset``.

pins: ice-wap-branch-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
C-011, C-012, C-013
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_wap_branch_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
CATALOG = "sc"
NAMESPACE = "ns"
SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
SEED_ROWS = "(1, 'a', 'x'), (2, 'b', 'y')"
WAP_PROPERTY = ", 'write.wap.enabled'='true'"
MOR_PROPERTIES = (
    ", 'write.delete.mode'='merge-on-read'"
    ", 'write.update.mode'='merge-on-read'"
    ", 'write.merge.mode'='merge-on-read'"
)
INSERT_NINE = "INSERT INTO {T} VALUES (9, 'z', 'q')"
BRANCH_CONF = {"spark.wap.branch": "audit"}
APPEND_ROW = (9, "z", "q")

CELLS: tuple[dict[str, Any], ...] = (
    {
        "id": "QW-INSERT",
        "title": "INSERT with spark.wap.branch",
        "conf": BRANCH_CONF,
        "steps": [{"sql": INSERT_NINE}],
    },
    {
        "id": "QW-INSERT-NO-BRANCH",
        "title": "wap.branch naming a branch that does not exist",
        "conf": BRANCH_CONF,
        "branch": None,
        "steps": [{"sql": INSERT_NINE}],
    },
    {
        "id": "QW-INSERT-NOT-ENABLED",
        "title": "wap.branch without write.wap.enabled",
        "conf": BRANCH_CONF,
        "properties": "",
        "steps": [{"sql": INSERT_NINE}],
    },
    {
        "id": "QW-READ-ONLY",
        "title": "wap.branch set, read only",
        "conf": BRANCH_CONF,
        "steps": [],
    },
    {
        "id": "QW-DF-APPEND",
        "title": "DataFrame append",
        "conf": BRANCH_CONF,
        "steps": [{"frame": "write_to_append"}],
    },
    {
        "id": "QW-DF-SAVEASTABLE",
        "title": "saveAsTable append",
        "conf": BRANCH_CONF,
        "steps": [{"frame": "save_as_table_append"}],
    },
    {
        "id": "QW-DELETE-COW",
        "title": "DELETE CoW",
        "conf": BRANCH_CONF,
        "steps": [{"sql": "DELETE FROM {T} WHERE id = 1"}],
    },
    {
        "id": "QW-DELETE-MOR",
        "title": "DELETE MoR",
        "conf": BRANCH_CONF,
        "properties": WAP_PROPERTY + MOR_PROPERTIES,
        "steps": [{"sql": "DELETE FROM {T} WHERE id = 1"}],
    },
    {
        "id": "QW-UPDATE",
        "title": "UPDATE",
        "conf": BRANCH_CONF,
        "steps": [{"sql": "UPDATE {T} SET data = 'u' WHERE id = 1"}],
    },
    {
        "id": "QW-MERGE",
        "title": "MERGE",
        "conf": BRANCH_CONF,
        "steps": [
            {
                "sql": "MERGE INTO {T} t USING (SELECT 1 AS id, 'm' AS data, 'x' AS cat) s"
                " ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
                " WHEN NOT MATCHED THEN INSERT *"
            }
        ],
    },
    {
        "id": "QW-OVERWRITE",
        "title": "INSERT OVERWRITE",
        "conf": BRANCH_CONF,
        "steps": [{"sql": "INSERT OVERWRITE {T} VALUES (9, 'z', 'q')"}],
    },
    {
        "id": "QW-TWO-INSERTS",
        "title": "two INSERT statements stack on the branch",
        "conf": BRANCH_CONF,
        "steps": [{"sql": INSERT_NINE}, {"sql": "INSERT INTO {T} VALUES (8, 'y', 'q')"}],
    },
    {
        "id": "QW-EXPLICIT-BRANCH",
        "title": "write to t.branch_b with wap.branch set",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": "ALTER TABLE {T} CREATE BRANCH other"},
            {"sql": "INSERT INTO {T}.branch_other VALUES (9, 'z', 'q')"},
        ],
    },
    {
        "id": "QW-WITH-WAP-ID",
        "title": "wap.branch + wap.id together",
        "conf": {"spark.wap.branch": "audit", "spark.wap.id": "w1"},
        "steps": [{"sql": INSERT_NINE}],
    },
    {
        "id": "QW-SET-SQL",
        "title": "SQL SET spark.wap.branch",
        "conf": {},
        "steps": [
            {"sql": "SET spark.wap.branch = audit"},
            {"sql": INSERT_NINE},
            {"sql": "RESET spark.wap.branch"},
        ],
    },
    {
        "id": "QW-VERSION-AS-OF-MAIN",
        "title": "explicit VERSION AS OF main while wap.branch set",
        "conf": BRANCH_CONF,
        "steps": [{"observe": "explicit-main", "sql": "SELECT * FROM {T} VERSION AS OF 'main'"}],
    },
    {
        "id": "QW-METADATA-TABLE",
        "title": "t.snapshots while wap.branch set",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": INSERT_NINE},
            {"observe": "snapshots-ops", "sql": "SELECT operation FROM {T}.snapshots"},
        ],
    },
    {
        "id": "QW-FAST-FORWARD",
        "title": "publish with fast_forward after wap.branch write",
        "conf": BRANCH_CONF,
        "steps": [{"sql": INSERT_NINE}],
        "after": [
            {"sql": "CALL " + CATALOG + ".system.fast_forward('{SHORT}', 'main', 'audit')"},
            {"observe": "main-after-ff", "sql": "SELECT * FROM {T} VERSION AS OF 'main'"},
        ],
    },
    {
        "id": "QW-DELETE-NO-BRANCH",
        "title": "DELETE with wap.branch naming a branch that does not exist",
        "conf": BRANCH_CONF,
        "branch": None,
        "steps": [{"sql": "DELETE FROM {T} WHERE id = 1"}],
    },
    {
        "id": "QW-UPDATE-NO-BRANCH",
        "title": "UPDATE with wap.branch naming a branch that does not exist",
        "conf": BRANCH_CONF,
        "branch": None,
        "steps": [{"sql": "UPDATE {T} SET data = 'u' WHERE id = 1"}],
    },
    {
        "id": "QW-DELETE-MOR-NO-BRANCH",
        "title": "DELETE MoR with wap.branch naming a branch that does not exist",
        "conf": BRANCH_CONF,
        "properties": WAP_PROPERTY + MOR_PROPERTIES,
        "branch": None,
        "steps": [{"sql": "DELETE FROM {T} WHERE id = 1"}],
    },
    {
        "id": "QW-MERGE-NO-BRANCH",
        "title": "MERGE with wap.branch naming a branch that does not exist",
        "conf": BRANCH_CONF,
        "branch": None,
        "steps": [
            {
                "sql": "MERGE INTO {T} t USING (SELECT 1 AS id, 'm' AS data, 'x' AS cat) s"
                " ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
                " WHEN NOT MATCHED THEN INSERT *"
            }
        ],
    },
    {
        "id": "QW-COMMA-JOIN",
        "title": "comma FROM-list and nested relations while the branch is ahead of main",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": INSERT_NINE},
            {"observe": "comma-self-join", "sql": "SELECT a.id, b.id FROM {T} a, {T} b"},
            {"observe": "comma-count", "sql": "SELECT count(*) FROM {T} a, {T} b"},
            {"observe": "join-on", "sql": "SELECT a.id, b.id FROM {T} a JOIN {T} b ON a.id = b.id"},
            {
                "observe": "in-subquery",
                "sql": "SELECT id FROM {T} WHERE id IN (SELECT id FROM {T})",
            },
            {
                "observe": "exists-subquery",
                "sql": "SELECT id FROM {T} a WHERE EXISTS"
                " (SELECT 1 FROM {T} b WHERE b.id = a.id + 7)",
            },
            {"observe": "union-all", "sql": "SELECT id FROM {T} UNION ALL SELECT id FROM {T}"},
            {
                "observe": "comma-after-subquery",
                "sql": "SELECT s.id, b.id FROM (SELECT id FROM {T}) s, {T} b",
            },
            {
                "observe": "cte-comma",
                "sql": "WITH c AS (SELECT id FROM {T}) SELECT c.id, b.id FROM c, {T} b",
            },
            {
                "observe": "three-way-comma",
                "sql": "SELECT a.id, b.id, c.id FROM {T} a, {T} b, {T} c"
                " WHERE a.id = b.id AND b.id = c.id",
            },
        ],
    },
    {
        "id": "QW-READ-MAIN-AHEAD",
        "title": "wap.branch set for reads while main is ahead of the branch",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": "INSERT INTO {T}.branch_main VALUES (7, 'm', 'q')"},
            {"observe": "explicit-main", "sql": "SELECT * FROM {T} VERSION AS OF 'main'"},
            {"observe": "explicit-branch", "sql": "SELECT * FROM {T} VERSION AS OF 'audit'"},
        ],
    },
    {
        "id": "QW-READ-BRANCH-AHEAD",
        "title": "wap.branch set for reads while the branch is ahead of main",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": "INSERT INTO {T}.branch_audit VALUES (9, 'z', 'q')"},
            {"observe": "explicit-main", "sql": "SELECT * FROM {T} VERSION AS OF 'main'"},
        ],
    },
    {
        "id": "QW-VERSION-AS-OF-MAIN-DIVERGED",
        "title": "explicit VERSION AS OF main after a wap.branch write",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": INSERT_NINE},
            {"observe": "explicit-main", "sql": "SELECT * FROM {T} VERSION AS OF 'main'"},
        ],
    },
    {
        "id": "QW-CTAS-OTHER",
        "title": "CTAS into a new table while wap.branch set",
        "conf": BRANCH_CONF,
        "steps": [
            {"sql": "CREATE TABLE {T2} USING iceberg AS SELECT 1 AS id"},
            {"observe_ref_names": "ctas-refs"},
        ],
    },
)


def cell_table_names(cell_id: str) -> tuple[str, str, str]:
    """Return the ``(qualified, second, short)`` table names this cell writes."""
    suffix = cell_id.lower().replace("-", "_")
    short = f"{NAMESPACE}.t_{suffix}"
    return f"{CATALOG}.{short}", f"{CATALOG}.{NAMESPACE}.u_{suffix}", short


def render(text: str, cell_id: str) -> str:
    """Substitute the per-cell table names into one statement template."""
    qualified, second, short = cell_table_names(cell_id)
    return text.replace("{T2}", second).replace("{T}", qualified).replace("{SHORT}", short)


def spark_session(warehouse: Path) -> Any:
    """Build the pinned PySpark session with a hadoop Iceberg catalog named ``sc``."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ice-wap-branch-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()


def table_metadata(spark: Any, name: str) -> dict[str, Any]:
    """Return the table's current metadata document as parsed JSON."""
    jvm = spark._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    parser = jvm.org.apache.iceberg.TableMetadataParser
    return json.loads(parser.toJson(table.operations().current()))


def snapshot_order(metadata: dict[str, Any]) -> dict[int, int]:
    """Map snapshot id to its position in ``(timestamp-ms, snapshot-id)`` order."""
    ordered = sorted(
        metadata.get("snapshots", []),
        key=lambda snapshot: (snapshot["timestamp-ms"], snapshot["snapshot-id"]),
    )
    return {snapshot["snapshot-id"]: index for index, snapshot in enumerate(ordered)}


def refs_of(spark: Any, name: str) -> list[list[Any]]:
    """Return sorted ``[ref, type, snapshot position]`` triples for one table."""
    metadata = table_metadata(spark, name)
    order = snapshot_order(metadata)
    return sorted(
        [ref, body.get("type"), order.get(body["snapshot-id"])]
        for ref, body in metadata.get("refs", {}).items()
    )


def ref_names_of(spark: Any, name: str) -> list[str]:
    """Return the sorted ref names of one table."""
    return sorted(table_metadata(spark, name).get("refs", {}).keys())


def rows_of(spark: Any, query: str) -> list[Any]:
    """Collect one query as sorted plain lists, or an ``ERR`` triple when it raises."""
    try:
        collected = spark.sql(query).collect()
    except Exception as error:
        return ["ERR", type(error).__name__, str(error).split("\n")[0][:160]]
    return sorted((list(row) for row in collected), key=repr)


def run_frame_step(spark: Any, kind: str, qualified: str) -> None:
    """Run one DataFrame write step (``writeTo().append()`` or ``saveAsTable``)."""
    frame = spark.createDataFrame([APPEND_ROW], "id BIGINT, data STRING, cat STRING")
    if kind == "write_to_append":
        frame.writeTo(qualified).append()
        return
    if kind == "save_as_table_append":
        frame.write.format("iceberg").mode("append").saveAsTable(qualified)
        return
    raise AssertionError(f"unknown frame step {kind!r}")


def run_steps(spark: Any, cell: dict[str, Any], steps: list[dict[str, Any]]) -> dict[str, Any]:
    """Run one cell's step list, returning the observations the steps record."""
    qualified, second, _ = cell_table_names(cell["id"])
    observations: dict[str, Any] = {}
    for step in steps:
        if "frame" in step:
            run_frame_step(spark, step["frame"], qualified)
            continue
        if "observe_ref_names" in step:
            observations[step["observe_ref_names"]] = ref_names_of(spark, second)
            continue
        statement = render(step["sql"], cell["id"])
        if "observe" in step:
            observations[step["observe"]] = rows_of(spark, statement)
            continue
        spark.sql(statement).collect()
    return observations


def record_cell(spark: Any, cell: dict[str, Any]) -> dict[str, Any]:
    """Run one cell end to end and return its recorded fixture entry."""
    qualified, second, _ = cell_table_names(cell["id"])
    branch = cell.get("branch", "audit")
    properties = cell.get("properties", WAP_PROPERTY)
    spark.sql(f"DROP TABLE IF EXISTS {qualified}")
    spark.sql(f"DROP TABLE IF EXISTS {second}")
    spark.sql(
        f"CREATE TABLE {qualified} {SEED_DDL} USING iceberg"
        f" TBLPROPERTIES ('format-version'='2'{properties})"
    )
    spark.sql(f"INSERT INTO {qualified} VALUES {SEED_ROWS}")
    if branch:
        spark.sql(f"ALTER TABLE {qualified} CREATE BRANCH {branch}")
    entry: dict[str, Any] = {
        "title": cell["title"],
        "conf": cell["conf"],
        "properties": properties,
        "branch": branch,
        "steps": cell["steps"],
        "after": cell.get("after", []),
        "status": "ok",
    }
    observations: dict[str, Any] = {}
    for key, value in cell["conf"].items():
        spark.conf.set(key, value)
    try:
        observations.update(run_steps(spark, cell, cell["steps"]))
        observations["session-read"] = rows_of(spark, f"SELECT * FROM {qualified}")
    except Exception as error:
        entry["status"] = "error"
        entry["error"] = {"type": type(error).__name__, "message": str(error).split("\n")[0]}
    finally:
        for key in cell["conf"]:
            spark.conf.unset(key)
    if entry["status"] == "ok":
        observations["refs"] = refs_of(spark, qualified)
        observations["main"] = rows_of(spark, f"SELECT * FROM {qualified} VERSION AS OF 'main'")
        if branch:
            observations["branch"] = rows_of(
                spark, f"SELECT * FROM {qualified} VERSION AS OF '{branch}'"
            )
        observations["plain-read-after-unset"] = rows_of(spark, f"SELECT * FROM {qualified}")
        observations.update(run_steps(spark, cell, cell.get("after", [])))
    entry["obs"] = observations
    return entry


def record(spark: Any) -> dict[str, Any]:
    """Run every cell and assemble the fixture document."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    return {
        "unit": "ice-wap-branch-1",
        "oracle": {
            "spark": "4.1.2",
            "iceberg": "1.11.0",
            "session_tz": "UTC",
            "catalog": CATALOG,
            "seed": SEED_ROWS,
            "source": "run 25c cells QW-* (harness cells qc5 and qc13)",
        },
        "cells": {cell["id"]: record_cell(spark, cell) for cell in CELLS},
    }


def first_mismatch(recorded: dict[str, Any], committed: dict[str, Any]) -> str | None:
    """Return a message naming the first cell that differs, or ``None`` when equal."""
    for cell_id, entry in recorded["cells"].items():
        pinned = committed["cells"].get(cell_id)
        if pinned is None:
            return f"{cell_id}: missing from the committed fixture"
        if entry != pinned:
            return (
                f"{cell_id}: live Spark no longer matches the fixture\n"
                f"  live:      {json.dumps(entry, sort_keys=True)}\n"
                f"  committed: {json.dumps(pinned, sort_keys=True)}"
            )
    missing = set(committed["cells"]) - set(recorded["cells"])
    if missing:
        return f"cells in the fixture the generator no longer records: {sorted(missing)}"
    return None


def main() -> int:
    """Record the fixture to stdout, or check live Spark against the committed fixture."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("record", "check"))
    parser.add_argument("--warehouse", required=True)
    arguments = parser.parse_args()
    warehouse = Path(arguments.warehouse)
    shutil.rmtree(warehouse, ignore_errors=True)
    warehouse.mkdir(parents=True, exist_ok=True)
    spark = spark_session(warehouse)
    spark.sparkContext.setLogLevel("ERROR")
    recorded = record(spark)
    if arguments.mode == "record":
        print(json.dumps(recorded, indent=1))
        return 0
    committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
    mismatch = first_mismatch(recorded, committed)
    if mismatch is not None:
        print(mismatch, file=sys.stderr)
        return 1
    print(f"ice-wap-branch-1: {len(recorded['cells'])} cells match the committed oracle")
    return 0


if __name__ == "__main__":
    sys.exit(main())
