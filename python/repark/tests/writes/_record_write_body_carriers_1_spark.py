"""Record live Spark 4.1.2 answers for CTAS statements whose body is a write.

NOT a ``test_`` module: pytest never collects it. It owns the 20 cells of
WRITE-BODY-CARRIERS-1 step 0 on Spark: ``CREATE TABLE z AS <body>`` and
``CREATE OR REPLACE TABLE z AS <body>`` for the INSERT, INSERT OVERWRITE,
UPDATE, DELETE and MERGE bodies, with ``z`` new and ``z`` existing. Each cell
carries the refusal (exception class, error class, condition, SQLSTATE,
message parameters, first line, full text) or the answer, plus the snapshot
count, row count and rows of ``t`` and ``z`` before and after the statement.

Run with the PySpark 4.1.2 interpreter that resolves the Iceberg runtime GAV
from ``_oracle_pins``::

    _record_write_body_carriers_1_spark.py [warehouse]

The warehouse is created fresh (removed first) and defaults to a scratch
directory. The run writes ``spark-cells.json`` and ``spark-cells.md`` into
``task/wo/write-body-carriers-1/evidence/``.
"""

from __future__ import annotations

import datetime
import json
import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

CATALOG = "wbc1"
NAMESPACE = "ns"
EVIDENCE = (
    Path(__file__).resolve().parents[3] / "task" / "wo" / "write-body-carriers-1" / "evidence"
)
JSON_NAME = "spark-cells.json"
MARKDOWN_NAME = "spark-cells.md"
BODIES: dict[str, str] = {
    "insert": "INSERT INTO {t} VALUES (3, 'c')",
    "overwrite": "INSERT OVERWRITE {t} VALUES (10, 'w')",
    "update": "UPDATE {t} SET v = 'x' WHERE id = 1",
    "delete": "DELETE FROM {t} WHERE id = 2",
    "merge": (
        "MERGE INTO {t} USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, 'n' AS v) s "
        "ON {t}.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    ),
}
CREATORS: dict[str, str] = {
    "create": "CREATE TABLE {z} AS {body}",
    "or_replace": "CREATE OR REPLACE TABLE {z} AS {body}",
}
ZSTATES: tuple[str, ...] = ("z_new", "z_exists")


def cell_id(creator: str, body: str, zstate: str) -> str:
    """Return the evidence key of one cell."""
    return f"{creator}_{body}_{zstate}"


def error_record(error: Exception) -> dict[str, Any]:
    """Capture one refusal as its class, condition, SQLSTATE and first line."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fields: dict[str, Any] = {}
        for key, attr in (
            ("error_class", "getErrorClass"),
            ("condition", "getCondition"),
            ("sqlstate", "getSqlState"),
        ):
            try:
                fields[key] = getattr(error, attr)()
            except Exception as access_error:
                fields[key] = type(access_error).__name__
        try:
            params = error.getMessageParameters()
        except Exception:
            params = None
    if params is None:
        fields["parameters"] = None
    else:
        try:
            fields["parameters"] = {str(k): str(v) for k, v in dict(params).items()}
        except Exception:
            fields["parameters"] = str(params)
    text = str(error)
    fields["first_line"] = next((line.strip() for line in text.splitlines() if line.strip()), "")
    fields["message"] = text
    return {"class": type(error).__name__, **fields}


def table_state(session: Any, table: str) -> dict[str, Any]:
    """Return the snapshot count, row count and rows of one table."""
    try:
        snapshots = session.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").collect()[0]["n"]
        rows = [list(row) for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()]
    except Exception as error:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(error):
            return {"exists": False}
        raise
    return {"exists": True, "snapshots": snapshots, "row_count": len(rows), "rows": rows}


def run_cell(session: Any, creator: str, body: str, zstate: str) -> dict[str, Any]:
    """Seed one cell, run its CTAS, and read both tables back."""
    cell = cell_id(creator, body, zstate)
    t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
    z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
    session.sql(f"DROP TABLE IF EXISTS {t}")
    session.sql(f"DROP TABLE IF EXISTS {z}")
    session.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg")
    session.sql(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b')")
    if zstate == "z_exists":
        session.sql(f"CREATE TABLE {z} (id INT, v STRING) USING iceberg")
        session.sql(f"INSERT INTO {z} VALUES (100, 'z')")
    before = {"t": table_state(session, t), "z": table_state(session, z)}
    statement = CREATORS[creator].format(z=z, body=BODIES[body].format(t=t))
    try:
        answer = session.sql(statement).collect()
        outcome: dict[str, Any] = {"answered": True, "answer_rows": len(answer)}
    except Exception as error:
        outcome = {"answered": False, **error_record(error)}
    after = {"t": table_state(session, t), "z": table_state(session, z)}
    return {
        "id": cell,
        "creator": creator,
        "body": body,
        "z_state": zstate,
        "statement": statement,
        "outcome": outcome,
        "before": before,
        "after": after,
    }


def build_session(warehouse: Path) -> Any:
    """Return a live Spark session over a Hadoop catalog at the warehouse."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("record-write-body-carriers-1-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def record(warehouse: Path) -> dict[str, Any]:
    """Run every cell on live Spark and return the evidence document."""
    shutil.rmtree(warehouse, ignore_errors=True)
    session = build_session(warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    cells = [
        run_cell(session, creator, body, zstate)
        for creator in CREATORS
        for body in BODIES
        for zstate in ZSTATES
    ]
    version = session.version
    session.stop()
    return {
        "provenance": {
            "engine": "spark",
            "spark": version,
            "iceberg": ICEBERG_SPARK_RUNTIME_GAV,
            "date": datetime.datetime.now(datetime.UTC).date().isoformat(),
        },
        "cells": cells,
    }


def unchanged(cell: dict[str, Any], table: str) -> bool:
    """Return whether one table reads back exactly as before the statement."""
    return cell["before"][table] == cell["after"][table]


def markdown_cell(value: str) -> str:
    """Escape one markdown table cell without changing its text."""
    return value.replace("|", "\\|").replace("\n", " ")


def render_markdown(document: dict[str, Any]) -> str:
    """Render the evidence table from the recorded document."""
    provenance = document["provenance"]
    lines = [
        "# WRITE-BODY-CARRIERS-1 step 0: Spark's answer",
        "",
        f"Recorded {provenance['date']} on live Spark {provenance['spark']} with "
        f"`{provenance['iceberg']}` by "
        "`python/repark/tests/_record_write_body_carriers_1_spark.py`. "
        f"Full texts and row values live in [{JSON_NAME}]({JSON_NAME}); "
        "this table is a view over it.",
        "",
        "| cell | exception | error class | condition | SQLSTATE | first line | t | z |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for cell in document["cells"]:
        outcome = cell["outcome"]
        if outcome["answered"]:
            row = [cell["id"], "answered", "", "", "", "", "", ""]
        else:
            row = [
                cell["id"],
                outcome["class"],
                str(outcome["error_class"]),
                str(outcome["condition"]),
                str(outcome["sqlstate"]),
                outcome["first_line"],
                "",
                "",
            ]
        row[6] = "same" if unchanged(cell, "t") else "MOVED"
        row[7] = "same" if unchanged(cell, "z") else "MOVED"
        lines.append("| " + " | ".join(markdown_cell(part) for part in row) + " |")
    lines.append("")
    refused = sum(1 for cell in document["cells"] if not cell["outcome"]["answered"])
    moved = sum(
        1 for cell in document["cells"] if not unchanged(cell, "t") or not unchanged(cell, "z")
    )
    lines.append(
        f"{len(document['cells'])} cells, {refused} refused, "
        f"{moved} with a table that reads back different."
    )
    lines.append("")
    return "\n".join(lines)


def write_evidence(document: dict[str, Any]) -> None:
    """Write the JSON and the markdown view beside it."""
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / JSON_NAME).write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    (EVIDENCE / MARKDOWN_NAME).write_text(render_markdown(document), encoding="utf-8")


def main(argv: list[str]) -> int:
    """Record every cell and write the evidence files."""
    warehouse = Path(argv[0]) if argv else Path(tempfile.mkdtemp()) / "wh-wbc1-spark"
    document = record(warehouse)
    write_evidence(document)
    refused = sum(1 for cell in document["cells"] if not cell["outcome"]["answered"])
    print(f"spark cells recorded={len(document['cells'])} refused={refused} -> {EVIDENCE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
