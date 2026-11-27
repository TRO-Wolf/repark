from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from pathlib import Path

ORD = "'2024-06-01 12:00:00'"
GAP = "'2024-03-10 02:30:00'"
PLAIN = "'2024-06-01 09:00:00'"
NY = "America/New_York"
KOLKATA = "Asia/Kolkata"
LTZ_CAST = f"CAST({ORD} AS TIMESTAMP_LTZ)"
FN = f"to_timestamp_ltz({ORD})"
NTZ_LIT = f"TIMESTAMP_NTZ {GAP}"
NTZ_CAST = f"CAST({GAP} AS TIMESTAMP_NTZ)"
CELLS = {
    "target_fn_ntz_session_into_ntz": ("TARGET", NY, "TIMESTAMP_NTZ", "NTZ", f"VALUES (0, {FN}), (1, {NTZ_LIT})"),
    "target_fn_ntz_session_into_ltz": ("TARGET", NY, "TIMESTAMP_NTZ", "LTZ", f"VALUES (0, {FN}), (1, {NTZ_LIT})"),
    "target_fn_ntz_first_into_ltz": ("TARGET", NY, "TIMESTAMP_NTZ", "LTZ", f"VALUES (1, {NTZ_LIT}), (0, {FN})"),
    "target_fn_interval_into_ntz": (
        "TARGET", NY, "TIMESTAMP_NTZ", "NTZ", f"VALUES (0, {FN} + INTERVAL '0' SECOND), (1, {NTZ_LIT})",
    ),
    "target_fn_kolkata_into_ntz": (
        "TARGET", KOLKATA, "TIMESTAMP_NTZ", "NTZ", f"VALUES (0, {FN}), (1, TIMESTAMP_NTZ {PLAIN})",
    ),
    "target_fn_merge_source_into_ntz": (
        "TARGET", NY, "TIMESTAMP_NTZ", "NTZ", f"MERGE:SELECT * FROM VALUES (0, {FN}), (1, {NTZ_LIT}) AS x(id, v)",
    ),
    "target_coalesce_fn_ntz_session_into_ltz": (
        "TARGET", NY, "TIMESTAMP_NTZ", "LTZ", f"VALUES (0, COALESCE({FN}, NULL)), (1, {NTZ_LIT})",
    ),
    "target_coalesce_ltz_session": (
        "TARGET", NY, "TIMESTAMP_LTZ", "NTZ", f"VALUES (0, COALESCE({LTZ_CAST}, NULL)), (1, {NTZ_LIT})",
    ),
    "target_case_ltz_session": (
        "TARGET", NY, "TIMESTAMP_LTZ", "NTZ", f"VALUES (0, CASE WHEN 1 = 1 THEN {LTZ_CAST} END), (1, {NTZ_LIT})",
    ),
    "target_if_ltz_session": (
        "TARGET", NY, "TIMESTAMP_LTZ", "NTZ", f"VALUES (0, IF(1 = 1, {LTZ_CAST}, NULL)), (1, {NTZ_LIT})",
    ),
    "target_ctas_fn_ntz_session": (
        "TARGET", NY, "TIMESTAMP_NTZ", "CTAS", f"SELECT * FROM VALUES (0, {FN}), (1, {NTZ_LIT}) AS x(id, v)",
    ),
    "protected_cast_ltz_session": ("PROTECTED", NY, "TIMESTAMP_LTZ", "NTZ", f"VALUES (0, {LTZ_CAST}), (1, {NTZ_LIT})"),
    "protected_cast_ltz_session_into_ltz": (
        "PROTECTED", NY, "TIMESTAMP_LTZ", "LTZ", f"VALUES (0, {LTZ_CAST}), (1, {NTZ_LIT})",
    ),
    "protected_kernel_cancel_ntz_session": (
        "PROTECTED", NY, "TIMESTAMP_NTZ", "NTZ", f"VALUES (0, {LTZ_CAST}), (1, {NTZ_CAST})",
    ),
    "protected_kernel_cancel_coalesce": (
        "PROTECTED", NY, "TIMESTAMP_LTZ", "NTZ", f"VALUES (0, COALESCE({LTZ_CAST}, NULL)), (1, {NTZ_CAST})",
    ),
    "protected_wrapped_ntz_ltz_session": (
        "PROTECTED", NY, "TIMESTAMP_LTZ", "NTZ", f"VALUES (0, COALESCE({NTZ_LIT}, NULL)), (1, {NTZ_LIT})",
    ),
    "protected_ntz_only_ntz_session": ("PROTECTED", NY, "TIMESTAMP_NTZ", "NTZ", f"VALUES (0, {NTZ_LIT}), (1, TIMESTAMP_NTZ {PLAIN})"),
    "protected_fn_utc_ntz_session": (
        "PROTECTED", "UTC", "TIMESTAMP_NTZ", "NTZ", f"VALUES (0, {FN}), (1, {NTZ_LIT})",
    ),
    "refuse_or_spark_cast_ntz_session_into_ltz": (
        "TARGET_OR_REFUSE", NY, "TIMESTAMP_NTZ", "LTZ", f"VALUES (0, {LTZ_CAST}), (1, {NTZ_LIT})",
    ),
}


def tag(error: BaseException) -> str:
    found = re.search(r"\[([A-Z][A-Z0-9_.]+)\]", str(error))
    return "ERR:" + (found.group(1) if found else type(error).__name__)


def store(spark, table: str, target: str, source: str) -> list:
    if target == "CTAS":
        spark.sql(f"CREATE TABLE {table} USING iceberg AS {source}")
    elif source.startswith("MERGE:"):
        spark.sql(f"CREATE TABLE {table} (id INT, v TIMESTAMP_{target}) USING iceberg")
        spark.sql(
            f"MERGE INTO {table} t USING ({source[6:]}) x ON t.id = x.id "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (x.id, x.v)"
        )
    else:
        spark.sql(f"CREATE TABLE {table} (id INT, v TIMESTAMP_{target}) USING iceberg")
        spark.sql(f"INSERT INTO {table} {source}")
    rows = spark.sql(f"SELECT id, typeof(v), CAST(v AS STRING) FROM {table} ORDER BY id").collect()
    return [[row[0], row[1], row[2]] for row in rows]


def record(warehouse: Path) -> dict:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    cells = {}
    for index, (name, (kind, zone, ttype, target, source)) in enumerate(sorted(CELLS.items())):
        spark.conf.set("spark.sql.session.timeZone", zone)
        spark.conf.set("spark.sql.timestampType", ttype)
        spark.conf.set("spark.sql.ansi.enabled", "false")
        try:
            answer = store(spark, f"sc.ns.t{index}", target, source)
        except Exception as error:
            answer = tag(error)
        cells[name] = {"kind": kind, "zone": zone, "timestamp_type": ttype, "target": target,
                       "source": source, "spark": answer}
    spark.stop()
    return cells


def main() -> None:
    out = Path(__file__).with_name("timestamp_ltz_explicit_cast_1_spark_oracle.json")
    with tempfile.TemporaryDirectory() as root:
        cells = record(Path(root) / "wh")
    payload = {"oracle": "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, local[2], ANSI off", "cells": cells}
    out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(f"wrote {len(cells)} cells\n")


if __name__ == "__main__":
    main()
