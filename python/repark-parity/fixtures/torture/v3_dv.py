"""The v3_dv torture family: a Spark-written format-v3 merge-on-read Iceberg table."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, ConfigDict

from repark_parity.torture.family import (
    refuse_bad_rows,
    refuse_bad_seed,
    refuse_repository_output,
)

NAME = "v3_dv"
LIVE_ENV = "REPARK_PARITY_LIVE"
SPARK_CATALOG = "torture_v3dv"
NAMESPACE = "ns"
TABLE = "v3dv"
TRUTH_NAME = "truth.json"
DELETE_MODULUS = 5
INSERT_BATCHES = 12
ICEBERG_SPARK_RUNTIME_GAV = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
FIXTURE_TABLE_DIR = Path(__file__).resolve().parent / "data" / NAME
CANONICAL_TABLE_DIR = Path("/tmp/repark-torture-v3dv") / NAMESPACE / TABLE

DECLARED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("part", pa.int32()),
    ]
)

_MOR_V3_TBLPROPERTIES = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)


class V3DvResult(BaseModel):
    """What one v3_dv generate call wrote: the table, its newest metadata, the truth record."""

    model_config = ConfigDict(extra="forbid")

    rows: int
    written_rows: int
    deleted_rows: int
    table_root: Path
    metadata_file: Path
    truth_path: Path


def _delete_rule(rows: int, seed: int) -> tuple[str, dict[str, Any]]:
    """The seeded delete clause: a compact modulo predicate, or the last row when it is empty."""
    residue = seed % DELETE_MODULUS
    if any(i % DELETE_MODULUS == residue for i in range(1, rows + 1)):
        return (
            f"id % {DELETE_MODULUS} = {residue}",
            {"modulus": DELETE_MODULUS, "residue": residue},
        )
    return f"id = {rows}", {"ids": [rows]}


def _batch_ranges(rows: int) -> list[range]:
    """Split ids 1..rows into contiguous insert batches so the table holds many files."""
    count = min(INSERT_BATCHES, rows)
    base, extra = divmod(rows, count)
    ranges: list[range] = []
    start = 1
    for index in range(count):
        stop = start + base + (1 if index < extra else 0)
        ranges.append(range(start, stop))
        start = stop
    return ranges


def _catalog_conf(warehouse: Path) -> dict[str, str]:
    """The session conf pairs that arm the module-private Iceberg Hadoop catalog."""
    return {
        "spark.jars.packages": ICEBERG_SPARK_RUNTIME_GAV,
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        f"spark.sql.catalog.{SPARK_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{SPARK_CATALOG}.type": "hadoop",
        f"spark.sql.catalog.{SPARK_CATALOG}.warehouse": str(warehouse),
    }


def _spark_session(warehouse: Path) -> tuple[Any, bool]:
    """Build the Iceberg-armed session or adopt the active one; (session, we_created)."""
    from pyspark.sql import SparkSession

    conf = _catalog_conf(warehouse)
    prior = SparkSession.getActiveSession()
    if prior is None:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("repark-torture-v3dv")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
        )
        for key, value in conf.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        return session, True
    for key, value in conf.items():
        if key in ("spark.jars.packages", "spark.sql.extensions"):
            continue
        prior.conf.set(key, value)
    return prior, False


def _newest_metadata_file(table_root: Path) -> Path:
    """The highest-versioned vN.metadata.json under the table's metadata directory."""
    versions = sorted(
        (table_root / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    if not versions:
        raise ValueError(f"no metadata files under {table_root}/metadata")
    return versions[-1]


def _count_rows(session: Any, fq_table: str, metadata_table: str) -> int:
    """One integer out of a metadata-table count query on the live session."""
    frame = session.sql(f"SELECT COUNT(*) AS c FROM {fq_table}.{metadata_table}")
    return int(frame.toArrow().column("c")[0].as_py())


def _count_data_files(session: Any, fq_table: str) -> int:
    """Data-file entries only: the files table also lists delete files (content 1/2)."""
    frame = session.sql(f"SELECT COUNT(*) AS c FROM {fq_table}.files WHERE content = 0")
    return int(frame.toArrow().column("c")[0].as_py())


class V3DvFamily:
    """The v3_dv family: many data files plus live Puffin deletion vectors under format v3."""

    name = NAME

    def generate(self, rows: int, seed: int, out: Path) -> V3DvResult:
        """Write the table under out/ns/v3dv with live Spark; refuses without the live flag."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        if os.environ.get(LIVE_ENV) != "1":
            raise RuntimeError(
                f"the v3_dv family needs live Spark to write deletion vectors: "
                f"re-run with {LIVE_ENV}=1 and a Java 17 JAVA_HOME"
            )
        out.mkdir(parents=True, exist_ok=True)
        session, created = _spark_session(out)
        fq_table = f"{SPARK_CATALOG}.{NAMESPACE}.{TABLE}"
        try:
            session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{NAMESPACE}")
            session.sql(f"DROP TABLE IF EXISTS {fq_table}")
            session.sql(
                f"CREATE TABLE {fq_table} (id INT, name STRING, part INT) "
                f"USING iceberg PARTITIONED BY (part) TBLPROPERTIES ({_MOR_V3_TBLPROPERTIES})"
            )
            for ids in _batch_ranges(rows):
                values = ", ".join(
                    f"({i}, 'name-{(i + seed) % 100_000:05d}', {i % 2})" for i in ids
                )
                session.sql(f"INSERT INTO {fq_table} VALUES {values}")
            delete_sql, delete_rule = _delete_rule(rows, seed)
            session.sql(f"DELETE FROM {fq_table} WHERE {delete_sql}")
            true_rows = int(
                session.sql(f"SELECT COUNT(*) AS c FROM {fq_table}")
                .toArrow()
                .column("c")[0]
                .as_py()
            )
            deleted_rows = rows - true_rows
            if deleted_rows < 1:
                raise RuntimeError(f"v3_dv delete removed no rows: {delete_sql}")
            data_files = _count_data_files(session, fq_table)
            delete_files = _count_rows(session, fq_table, "delete_files")
            table_root = out / NAMESPACE / TABLE
            metadata_file = _newest_metadata_file(table_root)
            truth = {
                "family": NAME,
                "seed": seed,
                "rows_written": rows,
                "delete": delete_rule,
                "rows_deleted": deleted_rows,
                "true_rows": true_rows,
                "data_files": data_files,
                "delete_files": delete_files,
                "format_version": 3,
                "table": f"{NAMESPACE}.{TABLE}",
                "table_location": str(table_root.resolve()),
                "metadata_file": f"metadata/{metadata_file.name}",
                "schema": "struct<id:int,name:string,part:int>",
            }
            truth_path = table_root / TRUTH_NAME
            truth_path.write_text(json.dumps(truth, indent=2) + "\n", encoding="utf-8")
            return V3DvResult(
                rows=true_rows,
                written_rows=rows,
                deleted_rows=deleted_rows,
                table_root=table_root,
                metadata_file=metadata_file,
                truth_path=truth_path,
            )
        finally:
            if created:
                session.stop()


V3DV_FAMILY = V3DvFamily()
