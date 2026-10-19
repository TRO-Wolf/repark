"""Record the NTZ-1 Slice 3 Spark-written table fixture and its Spark read answers.

NOT a ``test_`` module: pytest never collects it. It builds the ``xc.ns.x``
table on live PySpark at its canonical path (``id INT``, ``c TIMESTAMP_NTZ``,
``z TIMESTAMP``, one row ``2024-01-01 12:34:56.123456``), runs the read grid the
pin test replays, writes ``ntz-xc-spark.json`` beside the NTZ probe JSONs, and
copies the one-row warehouse into ``fixtures/ntz_1_spark_table/`` before the
re-insert round trip. The pin test copies it back to the same canonical path
before ``register_table``, so manifest file URIs stay valid.

Run it (needs a JVM and ``pyspark`` 4.1.2, one JVM at a time)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=/tmp/sparkenv/lib/python3.12/site-packages \\
        /tmp/oc-worker/_lib/jvm-lock.sh .venv/bin/python \\
        python/repark/tests/_record_ntz_1_spark_table.py

Re-running it re-derives every cell from live Spark and exits non-zero on drift;
it rewrites the probe-side JSON and the fixture only when ``--rewrite`` is
passed, so routine runs verify rather than launder.

Spark basis: ``local[1]``, driver memory 2g, UI off,
``spark.sql.session.timeZone=UTC``,
``org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`` via the local jar.
"""

from __future__ import annotations

import datetime
import decimal
import json
import shutil
import sys
from pathlib import Path
from typing import Any

_CANONICAL_ROOT = Path("/tmp/repark-ntz-1-spark-table")
_WAREHOUSE = _CANONICAL_ROOT / "wh"
_CATALOG = "xc"
_TABLE = f"{_CATALOG}.ns.x"
_JAR = (
    "/tmp/oc-worker/ice-rating/scratch/.ivy2/jars/"
    "org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar"
)
_HERE = Path(__file__).resolve().parent
_PROBE_SIDE_JSON = Path("/tmp/oc-worker/direct/wo/ntz-1-probes/ntz-xc-spark.json")
_FIXTURE_DIR = _HERE / "fixtures" / "ntz_1_spark_table"
_WALL = "2024-01-01 12:34:56.123456"


def _norm(value: Any) -> Any:
    """One collected value in the NTZ probe shape (naive datetimes keep no zone)."""
    if isinstance(value, datetime.datetime):
        return f"{value.isoformat()}|tz={value.tzinfo}"
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _norm(item) for key, item in value.items()}
    if hasattr(value, "asDict"):
        return {key: _norm(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _spark_session() -> Any:
    """The recorded basis, built once."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("repark-ntz-1-spark-table-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", _JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(_WAREHOUSE))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _build_table(spark: Any) -> None:
    """Create ``xc.ns.x`` and its one row at the canonical path."""
    if _CANONICAL_ROOT.exists():
        shutil.rmtree(_CANONICAL_ROOT)
    _WAREHOUSE.mkdir(parents=True)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns")
    spark.sql(f"CREATE TABLE {_TABLE} (id INT, c TIMESTAMP_NTZ, z TIMESTAMP) USING iceberg")
    spark.sql(f"INSERT INTO {_TABLE} VALUES (0, TIMESTAMP_NTZ'{_WALL}', TIMESTAMP'{_WALL}')")


def _query(spark: Any, out: dict[str, Any], key: str, sql: str) -> None:
    """One SELECT cell: probe-normalized rows plus name/type pairs."""
    frame = spark.sql(sql)
    out[key] = {
        "sql": sql,
        "rows": [[_norm(value) for value in row] for row in frame.collect()],
        "schema": [[field.name, field.dataType.simpleString()] for field in frame.schema.fields],
    }


def _meta_table(out: dict[str, Any]) -> None:
    """The table metadata cell: format version plus Iceberg field types."""
    files = sorted(
        (_WAREHOUSE / "ns" / "x" / "metadata").glob("*.metadata.json"),
        key=lambda path: path.stat().st_mtime_ns,
    )
    metadata = json.loads(files[-1].read_text(encoding="utf-8"))
    current = metadata["current-schema-id"]
    schema = next(entry for entry in metadata["schemas"] if entry["schema-id"] == current)
    out["meta_x"] = {
        "fv": metadata["format-version"],
        "fields": [[field["name"], field["type"], field["required"]] for field in schema["fields"]],
    }


def _record_pre_reads(spark: Any) -> dict[str, Any]:
    """Every one-row read cell the pin test replays."""
    out: dict[str, Any] = {}
    _query(spark, out, "sel_all", f"SELECT * FROM {_TABLE}")
    _query(
        spark,
        out,
        "sel_str",
        f"SELECT id, CAST(c AS STRING) AS c, CAST(z AS STRING) AS z FROM {_TABLE} ORDER BY id",
    )
    _query(spark, out, "filter", f"SELECT id FROM {_TABLE} WHERE c IS NOT NULL")
    _query(
        spark,
        out,
        "casts_hour",
        f"SELECT CAST(c AS STRING), CAST(z AS STRING), hour(c) FROM {_TABLE}",
    )
    out["dtypes"] = [[name, dtype] for name, dtype in spark.table(_TABLE).dtypes]
    try:
        out["to_arrow"] = str(spark.table(_TABLE).toArrow().schema)
    except Exception as error:
        out["to_arrow"] = {"error": type(error).__name__, "msg": str(error)[:200]}
    _query(
        spark,
        out,
        "files_bounds",
        f"SELECT readable_metrics.c.lower_bound, readable_metrics.c.upper_bound, "
        f"readable_metrics.z.lower_bound, readable_metrics.z.upper_bound FROM {_TABLE}.files",
    )
    _query(
        spark,
        out,
        "files_types",
        f"SELECT typeof(readable_metrics.c.lower_bound), "
        f"typeof(readable_metrics.z.lower_bound) FROM {_TABLE}.files LIMIT 1",
    )
    return out


def _record_reinsert(spark: Any, out: dict[str, Any]) -> None:
    """The re-insert round trip on top of the one-row table."""
    spark.sql(f"INSERT INTO {_TABLE} SELECT id + 1, c, z FROM {_TABLE}")
    _query(
        spark,
        out,
        "sel_after_reinsert",
        f"SELECT id, CAST(c AS STRING) AS c, CAST(z AS STRING) AS z FROM {_TABLE} ORDER BY id",
    )


def _record_alters(spark: Any) -> dict[str, Any]:
    """The three ALTER COLUMN refusals with Spark's full first lines."""
    out: dict[str, Any] = {}
    spark.sql(f"CREATE TABLE {_CATALOG}.ns.alt_ntz (id INT, c TIMESTAMP_NTZ) USING iceberg")
    spark.sql(f"CREATE TABLE {_CATALOG}.ns.alt_ltz (id INT, c TIMESTAMP) USING iceberg")
    spark.sql(
        f"CREATE TABLE {_CATALOG}.ns.alt_date (id INT, d DATE) USING iceberg "
        "TBLPROPERTIES ('format-version'='3')"
    )
    legs = (
        ("alter_ntz_to_ts", f"ALTER TABLE {_CATALOG}.ns.alt_ntz ALTER COLUMN c TYPE TIMESTAMP"),
        ("alter_ts_to_ntz", f"ALTER TABLE {_CATALOG}.ns.alt_ltz ALTER COLUMN c TYPE TIMESTAMP_NTZ"),
        (
            "alter_date_to_ntz_v3",
            f"ALTER TABLE {_CATALOG}.ns.alt_date ALTER COLUMN d TYPE TIMESTAMP_NTZ",
        ),
        (
            "alter_ntz_to_ntz",
            f"ALTER TABLE {_CATALOG}.ns.alt_ntz ALTER COLUMN c TYPE TIMESTAMP_NTZ",
        ),
    )
    for key, sql in legs:
        try:
            spark.sql(sql).collect()
            out[key] = {"sql": sql, "rows": []}
        except Exception as error:
            out[key] = {"sql": sql, "error": type(error).__name__, "msg": str(error)[:800]}
    return out


def _copy_fixture() -> None:
    """Copy the canonical table into the fixture dir, skipping sidecars, keeping map.md."""
    src = _WAREHOUSE / "ns" / "x"
    _FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("metadata", "data"):
        dest = _FIXTURE_DIR / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src / name, dest, ignore=shutil.ignore_patterns("*.crc"))


def _payload(reads: dict[str, Any], alters: dict[str, Any]) -> dict[str, Any]:
    """The probe-side JSON document."""
    meta: dict[str, Any] = {}
    _meta_table(meta)
    return {
        "oracle": "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, local[1], UTC",
        "table": "xc.ns.x (id INT, c TIMESTAMP_NTZ, z TIMESTAMP), one row " + _WALL,
        "meta": meta["meta_x"],
        "reads": reads,
        "alters": alters,
    }


def _report_drift(recorded: dict[str, Any], payload: dict[str, Any]) -> None:
    """Print every truth-vs-live cell difference."""
    for group in ("reads", "alters"):
        for key, live in payload[group].items():
            if recorded.get(group, {}).get(key) != live:
                print(f"  {group} {key}: truth={recorded[group][key]} live={live}")
    if recorded.get("meta") != payload["meta"]:
        print(f"  meta: truth={recorded.get('meta')} live={payload['meta']}")


def main(argv: list[str]) -> int:
    """Build, record, verify against the probe-side truth, rewrite only on --rewrite."""
    spark = _spark_session()
    try:
        _build_table(spark)
        reads = _record_pre_reads(spark)
        if "--rewrite" in argv or not _PROBE_SIDE_JSON.exists():
            _copy_fixture()
        _record_reinsert(spark, reads)
        alters = _record_alters(spark)
    finally:
        spark.stop()
    payload = _payload(reads, alters)
    if _PROBE_SIDE_JSON.exists() and "--rewrite" not in argv:
        recorded = json.loads(_PROBE_SIDE_JSON.read_text(encoding="utf-8"))
        if recorded == payload:
            print("oracle matches the probe-side truth")
            return 0
        print("ORACLE DRIFT versus the probe-side truth:")
        _report_drift(recorded, payload)
        return 1
    _PROBE_SIDE_JSON.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {_PROBE_SIDE_JSON} and {_FIXTURE_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
