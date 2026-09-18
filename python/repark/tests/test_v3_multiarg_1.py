"""V3-MULTIARG-1 — multi-argument partition transforms are DECLARED out of 1.x.

pins: v3-multiarg-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

_HERE = Path(__file__).resolve().parent
_ORACLE = _HERE / "v3_multiarg_1_spark_oracle.json"
_CATALOG = "v3_multiarg_1"
_NAMESPACE = "ns"
_SQL_ARITY_FRAGMENT = "expects (numBuckets, column), got 3 argument(s)"
_REGISTER_FRAGMENT = "data did not match any variant of untagged enum TableMetadataEnum"
_SPARK_FRAGMENT = "Cannot convert transform with more than one column reference"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog plus namespace."""
    session = ReparkSession.builder.appName("pytest-v3-multiarg-1").getOrCreate()
    session.register_memory_catalog(_CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
    return session


def multiarg_metadata(location: str) -> dict[str, Any]:
    """A minimal v3 metadata document with one two-source partition field."""
    return {
        "format-version": 3,
        "table-uuid": "11111111-2222-3333-4444-555555555555",
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": 1789753201035,
        "last-column-id": 2,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": False, "type": "int"},
                    {"id": 2, "name": "name", "required": False, "type": "string"},
                ],
            }
        ],
        "current-schema-id": 0,
        "partition-specs": [
            {
                "spec-id": 0,
                "fields": [
                    {
                        "field-id": 1000,
                        "name": "id_name_bucket",
                        "transform": "bucket[4]",
                        "source-ids": [1, 2],
                    }
                ],
            }
        ],
        "default-spec-id": 0,
        "last-partition-id": 1000,
        "properties": {"write.parquet.compression-codec": "zstd"},
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "refs": {},
        "next-row-id": 0,
    }


def test_sql_door_refuses_multiarg_bucket(spark: ReparkSession) -> None:
    """The SQL door refuses bucket with two source columns and the arity text."""
    with pytest.raises(AnalysisException, match=re.escape(_SQL_ARITY_FRAGMENT)):
        spark.sql(
            f"CREATE TABLE {_CATALOG}.{_NAMESPACE}.t (id INT, name STRING) "
            "USING iceberg PARTITIONED BY (bucket(4, id, name))"
        )


def test_register_table_with_source_ids_refuses_at_register(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """A foreign table carrying source-ids refuses loud at register time."""
    metadata_file = tmp_path / "multiarg.metadata.json"
    metadata_file.write_text(json.dumps(multiarg_metadata(str(tmp_path / "m"))), encoding="utf-8")
    with pytest.raises(PySparkException, match=re.escape(_REGISTER_FRAGMENT)):
        spark.sql(
            f"CALL {_CATALOG}.system.register_table("
            f"table => '{_NAMESPACE}.m', metadata_file => '{metadata_file}')"
        )


def test_spark_oracle_refused_the_multiarg_ddl() -> None:
    """The recorded Spark cell shows Spark 4.1.2 cannot create the table."""
    oracle = json.loads(_ORACLE.read_text(encoding="utf-8"))
    cell = oracle["cell"]
    assert cell["id"] == "MULTIARG-DDL-01"
    assert cell["class"] == "IllegalArgumentException"
    assert _SPARK_FRAGMENT in cell["message"]
    assert "bucket(4, id, name)" in cell["message"]
