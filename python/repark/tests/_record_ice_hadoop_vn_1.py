"""Record the ICE-HADOOP-VN-1 Spark oracle and fixture.

Live PySpark 4.1.2 builds the adopted Hadoop table, then drives every unit shape
while Spark reads: the conc fan-out (two RePark catalogs, one winner), the
re-register recovery, conc2 (Spark commits first), and the planted-v3 collision.
Outputs are the checked-in Spark-written fixture
(python/repark-parity/fixtures/torture/data/ice_hadoop_vn_1) and the truth JSON
beside the pins (ice_hadoop_vn_1_spark_oracle.json).

pins: ice-hadoop-vn-1/C-006, C-007

Run explicitly, never from the suite: JVM plus both engines on one interpreter.
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
PYTHONPATH=/tmp/jb-vn/python/repark/src:/tmp/jb-vn/.venv/lib/python3.12/site-packages \\
/tmp/oc-worker/jb-jvm.sh /tmp/sparkenv/bin/python python/repark/tests/_record_ice_hadoop_vn_1.py
"""

from __future__ import annotations

import json
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parents[2]
FIXTURE_DIR = REPO_ROOT / "python/repark-parity/fixtures/torture/data/ice_hadoop_vn_1"
ORACLE_PATH = TESTS_DIR / "ice_hadoop_vn_1_spark_oracle.json"
BAKE_WAREHOUSE = Path("/tmp/repark-ice-hadoop-vn-1")
BAKE_TABLE_ROOT = BAKE_WAREHOUSE / "ns" / "conc"
RECORDER = "python/repark/tests/_record_ice_hadoop_vn_1.py"

Row = list[Any]


def _rows_of(arrow_table: Any) -> list[Row]:
    data = arrow_table.to_pylist()
    return sorted(([row["id"], row["s"]] for row in data), key=repr)


def _spark_session(warehouse: Path) -> Any:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("ice-hadoop-vn-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.jars.ivy", "/tmp/ib-scratch/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def _repark_session(tmp: Path) -> Any:
    from repark import ReparkSession

    session = ReparkSession.builder.appName("ice-hadoop-vn-1-record").getOrCreate()
    session.register_memory_catalog("rp", tmp / "rp_wh")
    session.register_memory_catalog("rp2", tmp / "rp2_wh")
    session.register_memory_catalog("rp3", tmp / "rp3_wh")
    session.sql("CREATE NAMESPACE rp.ns")
    session.sql("CREATE NAMESPACE rp2.ns")
    session.sql("CREATE NAMESPACE rp3.ns")
    return session


def _metadata_names() -> list[str]:
    return sorted(path.name for path in (BAKE_TABLE_ROOT / "metadata").glob("*.metadata.json"))


def _record_error_label(exc: BaseException) -> dict[str, str]:
    return {
        "class": f"{type(exc).__module__}.{type(exc).__name__}",
        "message": str(exc).splitlines()[0][:300] if str(exc) else "",
    }


def main() -> None:
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV, ICEBERG_SPARK_RUNTIME_NOTE

    shutil.rmtree(BAKE_WAREHOUSE, ignore_errors=True)
    BAKE_WAREHOUSE.mkdir(parents=True)
    spark = _spark_session(BAKE_WAREHOUSE)
    banner = {
        "spark_version": spark.version,
        "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
        "gav": ICEBERG_SPARK_RUNTIME_GAV,
        "note": ICEBERG_SPARK_RUNTIME_NOTE,
        "recorded": date.today().isoformat(),
        "recorder": RECORDER,
    }
    print(f"banner: {banner}", flush=True)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    spark.sql(
        "CREATE TABLE sc.ns.conc (id INT, s STRING) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql("INSERT INTO sc.ns.conc VALUES (1,'seed')")
    assert _metadata_names() == ["v1.metadata.json", "v2.metadata.json"]
    seed_rows: list[Row] = _rows_of(spark.sql("SELECT id, s FROM sc.ns.conc").toArrow())
    assert seed_rows == [[1, "seed"]], seed_rows

    shutil.rmtree(FIXTURE_DIR, ignore_errors=True)
    (FIXTURE_DIR / "metadata").mkdir(parents=True)
    (FIXTURE_DIR / "data").mkdir(parents=True)
    for name in ("metadata", "data"):
        for path in (BAKE_TABLE_ROOT / name).rglob("*"):
            if path.name.startswith(".") or path.name.endswith(".crc"):
                continue
            target = FIXTURE_DIR / name / path.relative_to(BAKE_TABLE_ROOT / name)
            if path.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                shutil.copy(path, target)
    hint = BAKE_TABLE_ROOT / "metadata" / "version-hint.text"
    if hint.exists():
        shutil.copy(hint, FIXTURE_DIR / "metadata" / "version-hint.text")
    (FIXTURE_DIR / "truth.json").write_text(
        json.dumps(
            {
                "unit": "ice-hadoop-vn-1",
                "writer": "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, hadoop catalog",
                "recorded": banner["recorded"],
                "recorder": RECORDER,
                "seed_rows": seed_rows,
                "format_version": 2,
                "table": "ns.conc",
                "table_location": str(BAKE_TABLE_ROOT),
                "metadata_file": "metadata/v2.metadata.json",
                "schema": "struct<id:int,s:string>",
                "version_hint": hint.read_text(encoding="utf-8").strip() if hint.exists() else None,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    tmp = Path(tempfile.mkdtemp(prefix="ice-hadoop-vn-1-record"))
    rp = _repark_session(tmp)
    v2 = BAKE_TABLE_ROOT / "metadata" / "v2.metadata.json"
    rp.sql(f"CALL rp.system.register_table(table => 'ns.conc', metadata_file => '{v2}')").collect()
    rp.sql(f"CALL rp2.system.register_table(table => 'ns.conc', metadata_file => '{v2}')").collect()
    rp.sql("INSERT INTO rp.ns.conc VALUES (2,'rp-cat1')").collect()
    assert _metadata_names() == ["v1.metadata.json", "v2.metadata.json", "v3.metadata.json"]
    v3_bytes = (BAKE_TABLE_ROOT / "metadata" / "v3.metadata.json").read_bytes()
    stale_errors: dict[str, dict[str, str]] = {}
    for label, sql in (
        ("insert", "INSERT INTO rp2.ns.conc VALUES (3,'rp-cat2')"),
        (
            "merge",
            "MERGE INTO rp2.ns.conc t USING (SELECT 1 AS id, 'merged' AS s) src "
            "ON t.id = src.id WHEN MATCHED THEN UPDATE SET s = src.s",
        ),
        ("delete", "DELETE FROM rp2.ns.conc WHERE id = 1"),
        ("update", "UPDATE rp2.ns.conc SET s = 'stale' WHERE id = 1"),
    ):
        try:
            rp.sql(sql).collect()
            stale_errors[label] = {"class": "COMMITTED", "message": ""}
        except Exception as exc:
            stale_errors[label] = _record_error_label(exc)
        assert (BAKE_TABLE_ROOT / "metadata" / "v3.metadata.json").read_bytes() == v3_bytes
    rows_after_conc: list[Row] = _rows_of(rp.sql("SELECT id, s FROM rp.ns.conc").to_arrow())
    spark.sql("REFRESH TABLE sc.ns.conc")
    spark_rows_after_conc: list[Row] = _rows_of(spark.sql("SELECT id, s FROM sc.ns.conc").toArrow())
    assert rows_after_conc == spark_rows_after_conc == [[1, "seed"], [2, "rp-cat1"]]

    newest = BAKE_TABLE_ROOT / "metadata" / "v3.metadata.json"
    rp.sql(
        f"CALL rp3.system.register_table(table => 'ns.conc', metadata_file => '{newest}')"
    ).collect()
    rp.sql("INSERT INTO rp3.ns.conc VALUES (4,'rp-recovered')").collect()
    assert "v4.metadata.json" in _metadata_names()
    spark.sql("REFRESH TABLE sc.ns.conc")
    rows_after_recovery: list[Row] = _rows_of(spark.sql("SELECT id, s FROM sc.ns.conc").toArrow())
    assert rows_after_recovery == [[1, "seed"], [2, "rp-cat1"], [4, "rp-recovered"]]

    shutil.rmtree(BAKE_TABLE_ROOT)
    BAKE_TABLE_ROOT.mkdir(parents=True)
    for name in ("metadata", "data"):
        shutil.copytree(FIXTURE_DIR / name, BAKE_TABLE_ROOT / name)
    spark.sql("REFRESH TABLE sc.ns.conc")
    spark.sql("INSERT INTO sc.ns.conc VALUES (2,'spark')")
    assert _metadata_names() == ["v1.metadata.json", "v2.metadata.json", "v3.metadata.json"]
    spark_v3_bytes = (BAKE_TABLE_ROOT / "metadata" / "v3.metadata.json").read_bytes()
    rp.sql(f"CALL rp.system.register_table(table => 'ns.conc2', metadata_file => '{v2}')").collect()
    try:
        rp.sql("INSERT INTO rp.ns.conc2 VALUES (3,'repark-stale')").collect()
        conc2_error: dict[str, str] = {"class": "COMMITTED", "message": ""}
    except Exception as exc:
        conc2_error = _record_error_label(exc)
    assert (BAKE_TABLE_ROOT / "metadata" / "v3.metadata.json").read_bytes() == spark_v3_bytes
    spark.sql("REFRESH TABLE sc.ns.conc")
    rows_after_conc2: list[Row] = _rows_of(spark.sql("SELECT id, s FROM sc.ns.conc").toArrow())
    spark.sql("INSERT INTO sc.ns.conc VALUES (4,'spark-again')")
    spark.sql("REFRESH TABLE sc.ns.conc")
    rows_after_conc2_second: list[Row] = _rows_of(
        spark.sql("SELECT id, s FROM sc.ns.conc").toArrow()
    )
    assert rows_after_conc2 == [[1, "seed"], [2, "spark"]]
    assert rows_after_conc2_second == [[1, "seed"], [2, "spark"], [4, "spark-again"]]

    planted = BAKE_TABLE_ROOT / "metadata" / "v5.metadata.json"
    planted.write_bytes((BAKE_TABLE_ROOT / "metadata" / "v2.metadata.json").read_bytes())
    try:
        spark.sql("INSERT INTO sc.ns.conc VALUES (5,'planted')").collect()
        planted_error: dict[str, str] = {"class": "COMMITTED", "message": ""}
    except Exception as exc:
        planted_error = _record_error_label(exc)
    if planted.read_bytes() == (BAKE_TABLE_ROOT / "metadata" / "v2.metadata.json").read_bytes():
        planted.unlink()
    spark.sql("REFRESH TABLE sc.ns.conc")

    oracle = {
        "provenance": banner,
        "seed_rows": seed_rows,
        "rows_after_conc": rows_after_conc,
        "spark_rows_after_conc": spark_rows_after_conc,
        "rows_after_recovery": rows_after_recovery,
        "rows_after_conc2": rows_after_conc2,
        "rows_after_conc2_second": rows_after_conc2_second,
        "repark_stale_commit": {
            "class": "repark.errors.PySparkException",
            "message_starts_with": "CatalogCommitConflicts => Cannot commit table metadata to",
            "message_contains": "version file already exists",
            "per_writer": stale_errors,
            "conc2": conc2_error,
        },
        "spark_stale_commit": {
            "raises": planted_error["class"] != "COMMITTED",
            "planted_next_version_insert": planted_error,
            "note": (
                "Live Spark 4.1.2 lists the metadata directory and continues at the "
                "next free version, so neither a racing commit (probe: 400k-row "
                "INSERT committed beside a RePark commit, 400002 rows) nor a "
                "planted next-version file raises a PySpark-visible error. Java's "
                "CommitFailedException needs a true simultaneous-commit race."
            ),
        },
        "fixture": "python/repark-parity/fixtures/torture/data/ice_hadoop_vn_1",
    }
    ORACLE_PATH.write_text(json.dumps(oracle, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {ORACLE_PATH}", flush=True)
    print(f"fixture: {FIXTURE_DIR}", flush=True)
    spark.stop()
    rp.stop()


if __name__ == "__main__":
    main()
