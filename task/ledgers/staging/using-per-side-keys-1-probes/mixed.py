from __future__ import annotations

import json
import sys
from pathlib import Path

ENGINE = sys.argv[1]
OUT = sys.argv[2]

if ENGINE == "spark":
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("OFF")
else:
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("mixed")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

SIDES = {
    "int": "SELECT CAST(c AS INT) AS id FROM VALUES (1), (2) AS t(c)",
    "smallint": "SELECT CAST(c AS SMALLINT) AS id FROM VALUES (2), (7) AS t(c)",
    "bigint": "SELECT CAST(c AS BIGINT) AS id FROM VALUES (2), (5000000000) AS t(c)",
    "decimal": "SELECT CAST(c AS DECIMAL(10,2)) AS id FROM VALUES (2.00), (3.50) AS t(c)",
    "double": "SELECT CAST(c AS DOUBLE) AS id FROM VALUES (2.0), (3.7) AS t(c)",
    "float": "SELECT CAST(c AS FLOAT) AS id FROM VALUES (2.0), (3.5) AS t(c)",
    "string": "SELECT CAST(c AS STRING) AS id FROM VALUES ('2'), ('3') AS t(c)",
    "date": "SELECT CAST(c AS DATE) AS id FROM VALUES ('2026-01-02'), ('2026-01-05') AS t(c)",
    "timestamp": "SELECT CAST(c AS TIMESTAMP) AS id "
    "FROM VALUES ('2026-01-02 00:00:00'), ('2026-01-03 12:34:56') AS t(c)",
}
PAIRS = [
    ("int", "smallint"),
    ("int", "bigint"),
    ("int", "decimal"),
    ("int", "double"),
    ("int", "float"),
    ("int", "string"),
    ("bigint", "decimal"),
    ("bigint", "double"),
    ("decimal", "double"),
    ("float", "double"),
    ("double", "string"),
    ("date", "timestamp"),
    ("date", "string"),
    ("timestamp", "string"),
]


def cell(left: str, right: str, how: str) -> dict[str, object]:
    try:
        frame = session.sql(SIDES[left]).join(session.sql(SIDES[right]), "id", how)
        shown = frame.schema.simpleString()
        rows = sorted(str(row[0]) for row in frame.select(frame["id"].cast("string")).collect())
    except Exception as error:
        return {"refused": type(error).__name__}
    return {"type": shown, "rows": rows}


cells = {}
for first, second in PAIRS:
    for left, right in ((first, second), (second, first)):
        for how in ("inner", "left", "right", "full"):
            cells[f"{left}|{right}|{how}"] = cell(left, right, how)
lines = ",\n".join(
    f"{json.dumps(name)}: {json.dumps(cells[name], sort_keys=True)}" for name in sorted(cells)
)
Path(OUT).write_text("{\n" + lines + "\n}\n")
print(len(cells), "cells")
session.stop()
