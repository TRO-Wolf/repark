"""Record or re-check the ICE-REPLACE-COLUMNS-1 Spark oracle cells on live PySpark 4.1.2.

Usage (PySpark 4.1.2 interpreter, e.g. ``/tmp/sparkenv/bin/python``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
        <pyspark-python> python/repark/tests/_record_ice_replace_columns_1_oracle.py \
        --warehouse <scratch>/rc-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell
and exits non-zero naming the first mismatch against the committed
``ice_replace_columns_1_spark_oracle.json``. Both modes read the cells' SQL
(``create`` / ``seed`` / ``statements``) from that fixture, so the statements
have one home. Messages are compared after :func:`normalize_message` folds the
run-stamped parts Spark stamps into a plan dump (attribute ids, catalog
identity hash, the Py4J gateway object name) and the cell's table name.

The Iceberg runtime GAV comes from :mod:`_oracle_pins` (CP-8: never restate a
version literal). Nothing at module scope imports pyspark or starts a JVM, so
the offline test module can import the shared observation helpers.

pins: ice-replace-columns-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_replace_columns_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def iceberg_type_name(field_type: Any) -> str:
    """Render an Iceberg metadata type as the oracle's flat type token."""
    if isinstance(field_type, str):
        return field_type
    kind = field_type.get("type")
    if kind == "struct":
        children = ",".join(
            f"{child['name']}:{iceberg_type_name(child['type'])}" for child in field_type["fields"]
        )
        return f"struct<{children}>"
    if kind == "list":
        return f"list<{iceberg_type_name(field_type['element'])}>"
    if kind == "map":
        key = iceberg_type_name(field_type["key"])
        value = iceberg_type_name(field_type["value"])
        return f"map<{key},{value}>"
    return str(field_type)


def flatten_schema(fields: list[dict[str, Any]], prefix: str = "") -> list[list[Any]]:
    """Flatten a schema's fields to ``[name, type, required, doc]``, structs included."""
    out: list[list[Any]] = []
    for field in fields:
        name = prefix + field["name"]
        out.append([name, iceberg_type_name(field["type"]), field["required"], field.get("doc")])
        if isinstance(field["type"], dict) and field["type"].get("type") == "struct":
            out.extend(flatten_schema(field["type"]["fields"], name + "."))
    return out


def schema_observations(metadata: dict[str, Any]) -> dict[str, Any]:
    """The schema half of a cell's observations, read from Iceberg table metadata."""
    current = metadata["current-schema-id"]
    schema = next(one for one in metadata["schemas"] if one["schema-id"] == current)
    return {
        "field-ids": [
            [field["name"], field["id"], field["required"]] for field in schema["fields"]
        ],
        "identifier-field-ids": sorted(schema.get("identifier-field-ids", [])),
        "last-column-id": metadata["last-column-id"],
        "schema-count": len(metadata["schemas"]),
        "schema": flatten_schema(schema["fields"]),
    }


def normalize_rows(rows: list[list[Any]]) -> list[list[Any]]:
    """Sort a cell's rows and render bytes as hex so the comparison is stable."""
    out = []
    for row in rows:
        out.append(
            [
                "0x" + bytes(value).hex() if isinstance(value, (bytes, bytearray)) else value
                for value in row
            ]
        )
    return sorted(out, key=repr)


def normalize_message(message: str, table: str) -> str:
    """Fold the run-stamped parts of a Spark refusal message plus the cell's table name."""
    folded = message.replace(table, "{T}").replace(table.split(".")[-1], "{t}")
    folded = re.sub(r"#\d+L?", "#N", folded)
    folded = re.sub(r"@[0-9a-f]+", "@X", folded)
    folded = re.sub(r"\bo\d+\.sql\b", "oN.sql", folded)
    folded = re.sub(r"\n\s*(JVM stacktrace|at |\tat ).*", "", folded, flags=re.S)
    return folded.strip()[:700]


def _spark_session(warehouse: Path) -> Any:
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ice-replace-columns-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def _table_metadata(spark: Any, table: str) -> dict[str, Any]:
    loaded = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    parser = spark._jvm.org.apache.iceberg.TableMetadataParser
    return json.loads(parser.toJson(loaded.operations().current()))


def _first_snapshot(spark: Any, table: str) -> int:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    return int(rows[0][0])


def run_cell(spark: Any, cell: dict[str, Any]) -> dict[str, Any]:
    """Replay one oracle cell on live Spark and return its recorded outcome."""
    table = cell["table"]
    statements = [cell["create"], *cell["seed"], *cell["statements"]]
    for statement in statements:
        try:
            spark.sql(statement)
        except Exception as failure:
            return {
                "status": "error",
                "error": {
                    "type": type(failure).__name__,
                    "condition": _condition_of(failure),
                    "message": normalize_message(str(failure), table),
                    "failing_statement": statement,
                },
            }
    observations = schema_observations(_table_metadata(spark, table))
    observations["data"] = normalize_rows(
        [list(row) for row in spark.sql(f"SELECT * FROM {table}").collect()]
    )
    if cell["time_travel_first_snapshot"]:
        snapshot = _first_snapshot(spark, table)
        observations["time-travel-rows"] = normalize_rows(
            [
                list(row)
                for row in spark.sql(f"SELECT * FROM {table} VERSION AS OF {snapshot}").collect()
            ]
        )
    return {"status": "ok", "obs": observations}


def _condition_of(failure: Exception) -> str | None:
    getter = getattr(failure, "getCondition", None)
    if callable(getter):
        try:
            return str(getter())
        except Exception:
            return None
    return None


def record_all(warehouse: Path) -> dict[str, Any]:
    """Replay every fixture cell on a fresh warehouse and return the rebuilt fixture."""
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    spark = _spark_session(warehouse)
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
        for name, cell in fixture["cells"].items():
            outcome = run_cell(spark, cell)
            cell.pop("obs", None)
            cell.pop("error", None)
            cell["status"] = outcome["status"]
            cell.update({key: value for key, value in outcome.items() if key != "status"})
            print(f"{name}: {outcome['status']}", file=sys.stderr, flush=True)
    finally:
        spark.stop()
    return fixture


def check_against_fixture(derived: dict[str, Any]) -> list[str]:
    """Names every cell whose live replay drifted from the committed fixture."""
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    mismatches: list[str] = []
    for name, want in expected["cells"].items():
        got = derived["cells"][name]
        if got["status"] != want["status"]:
            mismatches.append(f"{name}: status {got['status']} != {want['status']}")
            continue
        if want["status"] == "ok":
            if got["obs"] != want["obs"]:
                mismatches.append(
                    f"{name}:\n  want={json.dumps(want['obs'])[:600]}"
                    f"\n  got ={json.dumps(got['obs'])[:600]}"
                )
            continue
        wanted_error = dict(want["error"])
        wanted_error["message"] = normalize_message(wanted_error["message"], want["table"])
        if got["error"] != wanted_error:
            mismatches.append(
                f"{name}:\n  want={json.dumps(wanted_error)[:600]}"
                f"\n  got ={json.dumps(got['error'])[:600]}"
            )
    return mismatches


def main(argv: list[str]) -> int:
    """CLI entry point: ``record`` prints the fixture, ``check`` diffs it."""
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
        print(f"{len(mismatches)} oracle cell(s) drifted from {FIXTURE}:", flush=True)
        for mismatch in mismatches:
            print(mismatch, flush=True)
        return 1
    print(f"oracle fixture {FIXTURE.name} reproduces on live Spark", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
