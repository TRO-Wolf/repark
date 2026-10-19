"""Record or re-check the ICE-AVRO-NAME-1 Spark oracle cells on live PySpark 4.1.2.

Usage (PySpark 4.1.2 interpreter, e.g. ``/tmp/sparkenv/bin/python``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_avro_name_1_oracle.py \
        --warehouse /tmp/avro-name-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell and exits
non-zero naming the first mismatch against the committed
``ice_avro_name_1_spark_oracle.json``. Each cell partitions a Hadoop-catalog table by a
column whose name is not a valid Avro name (plus one valid control), inserts two rows and
records the rows, the ``partitions`` metadata table and the partition record of the first
data manifest's Avro schema. The Iceberg runtime GAV comes from :mod:`_oracle_pins`.

pins: rp-35-fork-pin/C-001
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

FIXTURE = Path(__file__).with_name("ice_avro_name_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
CELLS: dict[str, tuple[str, str, str]] = {
    "space": ("`my col` STRING", "`my col`", "'x'"),
    "leading_digit": ("`1st` STRING", "`1st`", "'x'"),
    "dash": ("`a-b` STRING", "`a-b`", "'x'"),
    "dot": ("`a.b` STRING", "`a.b`", "'x'"),
    "non_ascii_letter": ("`é` STRING", "`é`", "'x'"),
    "cjk": ("`列` STRING", "`列`", "'x'"),
    "emoji": ("`c😀` STRING", "`c😀`", "'x'"),
    "bucket_space": ("`my col` STRING", "bucket(4, `my col`)", "'x'"),
    "truncate_dash": ("`a-b` STRING", "truncate(2, `a-b`)", "'xyz'"),
    "valid": ("`ok_col` STRING", "`ok_col`", "'x'"),
}


def read_zigzag(data: bytes, pos: int) -> tuple[int, int]:
    """Decode one Avro zig-zag varint at ``pos``; return the value and the next offset."""
    shift = 0
    acc = 0
    while True:
        byte = data[pos]
        pos += 1
        acc |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    return (acc >> 1) ^ -(acc & 1), pos


def avro_header(path: Path) -> dict[str, bytes]:
    """Return the key/value metadata block of an Avro object-container file."""
    data = path.read_bytes()
    if data[:4] != b"Obj\x01":
        raise ValueError(f"{path} is not an Avro object container")
    pos = 4
    meta: dict[str, bytes] = {}
    while True:
        count, pos = read_zigzag(data, pos)
        if count == 0:
            return meta
        if count < 0:
            count = -count
            _, pos = read_zigzag(data, pos)
        for _ in range(count):
            key_len, pos = read_zigzag(data, pos)
            key = data[pos : pos + key_len].decode()
            pos += key_len
            value_len, pos = read_zigzag(data, pos)
            meta[key] = data[pos : pos + value_len]
            pos += value_len


def manifest_partition_fields(metadata_dir: Path) -> list[dict[str, Any]]:
    """Return the partition record fields of the first data manifest under ``metadata_dir``."""
    manifests = sorted(
        path for path in metadata_dir.rglob("*.avro") if not path.name.startswith("snap-")
    )
    schema = json.loads(avro_header(manifests[0])["avro.schema"])
    data_file = next(field for field in schema["fields"] if field["name"] == "data_file")
    partition = next(field for field in data_file["type"]["fields"] if field["name"] == "partition")
    return [
        {
            "name": field["name"],
            "iceberg-field-name": field.get("iceberg-field-name"),
            "field-id": field.get("field-id"),
        }
        for field in partition["type"]["fields"]
    ]


def build_session(warehouse: Path) -> Any:
    """Build a local PySpark session with a Hadoop Iceberg catalog ``sc`` at ``warehouse``."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("record-ice-avro-name-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def record_cells(warehouse: Path) -> dict[str, Any]:
    """Run every cell on live Spark and return the fixture document."""
    shutil.rmtree(warehouse, ignore_errors=True)
    session = build_session(warehouse)
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    cells: dict[str, Any] = {}
    for name, (coldef, part, value) in CELLS.items():
        for version in (2, 3):
            table = f"sc.ns.{name}_v{version}"
            column = coldef.rsplit(" ", 1)[0]
            session.sql(
                f"CREATE TABLE {table} (id INT, {coldef}) USING iceberg PARTITIONED BY ({part}) "
                f"TBLPROPERTIES ('format-version'='{version}')"
            )
            session.sql(f"INSERT INTO {table} VALUES (1, {value}), (2, {value})")
            rows = session.sql(f"SELECT * FROM {table} WHERE {column} = {value} ORDER BY id")
            parts = session.sql(f"SELECT partition FROM {table}.partitions").collect()
            cells[f"{name}_v{version}"] = {
                "create": f"(id INT, {coldef}) PARTITIONED BY ({part})",
                "column": column,
                "value": value,
                "version": version,
                "rows": [list(row) for row in rows.collect()],
                "parts": [row[0].asDict() for row in parts],
                "manifest_partition_fields": manifest_partition_fields(
                    warehouse / "ns" / f"{name}_v{version}" / "metadata"
                ),
            }
    banner = session.version
    session.stop()
    return {"banner": banner, "runtime": ICEBERG_SPARK_RUNTIME_GAV, "cells": cells}


def main() -> int:
    """Record the fixture to stdout, or check it against the committed file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args()
    document = record_cells(args.warehouse)
    if args.mode == "record":
        print(json.dumps(document, indent=1, ensure_ascii=False))
        return 0
    committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for key, cell in committed["cells"].items():
        if document["cells"].get(key) != cell:
            print(f"mismatch in {key}: {document['cells'].get(key)} != {cell}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
