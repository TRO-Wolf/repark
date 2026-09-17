"""Record the ICE-NAN-PUSHDOWN-1 Spark oracle — truth JSON plus fixture warehouses.

NOT a ``test_`` module: pytest never collects it. It builds the four NaN tables on
live PySpark at their canonical paths, runs the clause grid, writes
``ice_nan_pushdown_1_oracle.json`` beside this file, and copies the warehouses into
``fixtures/ice_nan_pushdown_1/``. The pin test copies them back to the same
canonical paths before ``register_table``, so manifest file URIs stay valid.

Run it (needs a JVM and ``pyspark`` 4.1.2, one JVM at a time)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/oc-worker/jb-jvm.sh /tmp/sparkenv/bin/python \\
        python/repark/tests/_record_ice_nan_pushdown_1.py

Re-running it re-derives every cell from live Spark and exits non-zero on drift;
it rewrites the truth JSON only when ``--rewrite`` is passed, so routine runs
verify rather than launder.

Spark basis: ``local[2]``, ANSI on, shuffle partitions 2, UI off,
``spark.sql.session.timeZone=UTC``,
``org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`` (see ``_oracle_pins.py``).
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

_CANONICAL_ROOT = Path("/tmp/repark-ice-nan-pushdown-1")
_WAREHOUSE = _CANONICAL_ROOT / "wh"
_CATALOG = "rec"
_HERE = Path(__file__).resolve().parent
_TRUTH_PATH = _HERE / "ice_nan_pushdown_1_oracle.json"
_FIXTURE_DIR = _HERE / "fixtures" / "ice_nan_pushdown_1"
_FIXTURE_TABLES = ("v2/mixed", "v2/nan_only", "v3/mixed", "v3/nan_only")
NAN_D = "CAST('NaN' AS DOUBLE)"
ONE_D = "CAST(1.0 AS DOUBLE)"
NAN_F = "CAST('NaN' AS FLOAT)"
ONE_F = "CAST(1.0 AS FLOAT)"
_MIXED_FILE_ONE = (
    "(1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), "
    "(2, CAST(1.0 AS DOUBLE), CAST(1.0 AS FLOAT)), (3, NULL, NULL)"
)
_MIXED_FILE_TWO = (
    "(4, CAST(0.5 AS DOUBLE), CAST(0.5 AS FLOAT)), "
    "(5, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), "
    "(6, CAST(-2.0 AS DOUBLE), CAST(-2.0 AS FLOAT))"
)
_NAN_ONLY_ROWS = (
    "(1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), "
    "(2, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT))"
)


def _double_clauses() -> dict[str, str]:
    """The double-column clause grid: name to WHERE text."""
    return {
        "eq": f"d = {NAN_D}",
        "eq_rev": f"{NAN_D} = d",
        "nullsafe": f"d <=> {NAN_D}",
        "neq": f"d != {NAN_D}",
        "lt": f"d < {NAN_D}",
        "le": f"d <= {NAN_D}",
        "gt": f"d > {NAN_D}",
        "ge": f"d >= {NAN_D}",
        "in_nan": f"d IN ({NAN_D})",
        "in_nan_1": f"d IN ({NAN_D}, {ONE_D})",
        "not_in_nan": f"d NOT IN ({NAN_D})",
        "between": f"d BETWEEN {ONE_D} AND {NAN_D}",
        "not_eq": f"NOT (d = {NAN_D})",
        "isnan": "isnan(d)",
    }


def _float_clauses() -> dict[str, str]:
    """The float-column clause grid: name to WHERE text."""
    return {
        "eq": f"f = {NAN_F}",
        "nullsafe": f"f <=> {NAN_F}",
        "neq": f"f != {NAN_F}",
        "lt": f"f < {NAN_F}",
        "gt": f"f > {NAN_F}",
        "ge": f"f >= {NAN_F}",
        "in_nan": f"f IN ({NAN_F})",
        "in_nan_1": f"f IN ({NAN_F}, {ONE_F})",
        "not_in_nan": f"f NOT IN ({NAN_F})",
        "isnan": "isnan(f)",
    }


def _spark_session() -> Any:
    """The recorded basis, built once."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-ice-nan-pushdown-1-record")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        )
        .config("spark.jars.ivy", "/tmp/ib-scratch/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(_WAREHOUSE))
        .getOrCreate()
    )


def _ids(spark: Any, query: str) -> list[int]:
    """Sorted id column of a query as plain ints."""
    return sorted(row[0] for row in spark.sql(query).collect())


def _build_tables(spark: Any) -> dict[str, str]:
    """Create the four fixture tables; return shape key to fully qualified name."""
    if _CANONICAL_ROOT.exists():
        shutil.rmtree(_CANONICAL_ROOT)
    _WAREHOUSE.mkdir(parents=True)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns")
    names: dict[str, str] = {}
    for version in ("2", "3"):
        for shape in ("mixed", "nan_only"):
            table = f"{_CATALOG}.ns.nan_{shape}_v{version}"
            spark.sql(
                f"CREATE TABLE {table} (id INT, d DOUBLE, f FLOAT) USING iceberg "
                f"TBLPROPERTIES ('format-version'='{version}')"
            )
            if shape == "mixed":
                spark.sql(
                    f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {_MIXED_FILE_ONE} AS x(id, d, f)"
                )
                spark.sql(
                    f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {_MIXED_FILE_TWO} AS x(id, d, f)"
                )
            else:
                spark.sql(
                    f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ * "
                    f"FROM VALUES {_NAN_ONLY_ROWS} AS x(id, d, f)"
                )
            names[f"v{version}/{shape}"] = table
    return names


def _record_reads(spark: Any, names: dict[str, str]) -> dict[str, dict[str, list[int]]]:
    """Every clause id set per shape key."""
    reads: dict[str, dict[str, list[int]]] = {}
    for key, table in names.items():
        cells: dict[str, list[int]] = {}
        for name, where in _double_clauses().items():
            cells[f"d_{name}"] = _ids(spark, f"SELECT id FROM {table} WHERE {where}")
        if key.endswith("mixed"):
            for name, where in _float_clauses().items():
                cells[f"f_{name}"] = _ids(spark, f"SELECT id FROM {table} WHERE {where}")
        else:
            for name in ("eq", "neq", "in_nan", "isnan"):
                where = _float_clauses()[name]
                cells[f"f_{name}"] = _ids(spark, f"SELECT id FROM {table} WHERE {where}")
        reads[key] = cells
    return reads


_SCRATCH_DDL = "(id INT, d DOUBLE, f FLOAT) USING iceberg TBLPROPERTIES ('format-version'='2')"


def _record_dml(spark: Any) -> dict[str, Any]:
    """DELETE and UPDATE row outcomes on scratch copies of the mixed v2 table."""
    spark.sql(f"CREATE TABLE {_CATALOG}.ns.nan_dml_v2 {_SCRATCH_DDL}")
    spark.sql(
        f"INSERT INTO {_CATALOG}.ns.nan_dml_v2 SELECT /*+ COALESCE(1) */ * "
        f"FROM VALUES {_MIXED_FILE_ONE} AS x(id, d, f)"
    )
    spark.sql(
        f"INSERT INTO {_CATALOG}.ns.nan_dml_v2 SELECT /*+ COALESCE(1) */ * "
        f"FROM VALUES {_MIXED_FILE_TWO} AS x(id, d, f)"
    )
    spark.sql(f"DELETE FROM {_CATALOG}.ns.nan_dml_v2 WHERE d = {NAN_D}")
    remaining = _ids(spark, f"SELECT id FROM {_CATALOG}.ns.nan_dml_v2")
    spark.sql(f"CREATE TABLE {_CATALOG}.ns.nan_upd_v2 {_SCRATCH_DDL}")
    spark.sql(
        f"INSERT INTO {_CATALOG}.ns.nan_upd_v2 SELECT /*+ COALESCE(1) */ * "
        f"FROM VALUES {_MIXED_FILE_ONE} AS x(id, d, f)"
    )
    spark.sql(
        f"INSERT INTO {_CATALOG}.ns.nan_upd_v2 SELECT /*+ COALESCE(1) */ * "
        f"FROM VALUES {_MIXED_FILE_TWO} AS x(id, d, f)"
    )
    spark.sql(f"UPDATE {_CATALOG}.ns.nan_upd_v2 SET f = {ONE_F} WHERE d = {NAN_D}")
    updated = _ids(
        spark, f"SELECT id FROM {_CATALOG}.ns.nan_upd_v2 WHERE f = {ONE_F} AND d = {NAN_D}"
    )
    untouched = _ids(spark, f"SELECT id FROM {_CATALOG}.ns.nan_upd_v2 WHERE NOT (f = {ONE_F})")
    return {"delete_remaining": remaining, "update_touched": updated, "update_untouched": untouched}


def _copy_fixtures() -> None:
    """Copy the four canonical warehouses into the checked-in fixture dir."""
    if _FIXTURE_DIR.exists():
        shutil.rmtree(_FIXTURE_DIR)
    for key in _FIXTURE_TABLES:
        version, shape = key.split("/")
        src = _WAREHOUSE / "ns" / f"nan_{shape}_{version}"
        dest = _FIXTURE_DIR / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest)


def _payload(reads: dict[str, dict[str, list[int]]], dml: dict[str, Any]) -> dict[str, Any]:
    """The truth JSON document."""
    return {
        "oracle": "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, local[2], ANSI on, UTC",
        "tables": {
            "v2/mixed": "ids 1..6: (NaN,NaN),(1.0,1.0),(NULL,NULL) in file one; "
            "(0.5,0.5),(NaN,NaN),(-2.0,-2.0) in file two",
            "v2/nan_only": "ids 1..2, every d and f NaN, one file",
            "v3/mixed": "same rows as v2/mixed at format-version 3",
            "v3/nan_only": "same rows as v2/nan_only at format-version 3",
        },
        "reads": reads,
        "dml": dml,
    }


def main(argv: list[str]) -> int:
    """Build, record, verify against the checked-in truth, rewrite only on --rewrite."""
    spark = _spark_session()
    try:
        names = _build_tables(spark)
        reads = _record_reads(spark, names)
        dml = _record_dml(spark)
    finally:
        spark.stop()
    payload = _payload(reads, dml)
    if _TRUTH_PATH.exists() and "--rewrite" not in argv:
        recorded = json.loads(_TRUTH_PATH.read_text(encoding="utf-8"))
        if recorded["reads"] == payload["reads"] and recorded["dml"] == payload["dml"]:
            print("oracle matches the checked-in truth")
            return 0
        print("ORACLE DRIFT versus the checked-in truth:")
        for key in payload["reads"]:
            for cell, live in payload["reads"][key].items():
                old = recorded["reads"].get(key, {}).get(cell)
                if old != live:
                    print(f"  reads {key} {cell}: truth={old} live={live}")
        for cell, live in payload["dml"].items():
            if recorded["dml"].get(cell) != live:
                print(f"  dml {cell}: truth={recorded['dml'].get(cell)} live={live}")
        return 1
    _TRUTH_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _copy_fixtures()
    print(f"wrote {_TRUTH_PATH} and {_FIXTURE_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
