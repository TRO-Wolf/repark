"""Non-MERGE write-path ANSI store-assignment gate (WI-1) — the `INSERT OVERWRITE` door.

Oracle: Spark's DML store-assignment policy (``Cast.canANSIStoreAssign``, default
``spark.sql.storeAssignmentPolicy=ANSI``) refuses ``date→int``, ``boolean→int``,
``timestamp→bigint`` and ``string→numeric`` at ANALYSIS time —
``INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST``, measured against live PySpark 4.1.2 ANSI on
2026-08-15 (``planning/hardening/G63-DATE-INT-DESIGN.md`` §1.4).

Before the gate the engine ran the full arrow cast kernel on the way to the staged data files, so
``INSERT OVERWRITE int_table SELECT date_col …`` silently committed **18262** — days since
1970-01-01 — into a durable Iceberg file. That is strictly worse than the SELECT-side
``CAST(DATE AS INT)`` divergence, which at least stays in memory: the caller never wrote a CAST.

Scope of this file is the door WI-1 actually gated. ``INSERT INTO … SELECT``,
``INSERT INTO … VALUES``, ``writeTo().append()`` and ``write.insertInto()`` (no ``overwrite``) are
lowered by DataFusion's own ``insert_to_plan`` — which injects the ``CAST`` and hands a
schema-conformed plan to the fork's ``IcebergTableProvider::insert_into`` — so they never reach a
``crates/repark-iceberg/src/write/`` seam and stay ungated. They are named in
``task/wi1-insert-store-gate-ledger.md`` §4 with the seam a follow-on unit needs, not pinned here.

The MERGE twins live in ``test_merge_store_assign.py`` and share the ``NEEDLE`` below; this file
does not touch them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

FQ = "mem.ns.ow_t"
SRC = "mem.ns.ow_s"
NEEDLE = r"not ANSI-store-assignable"
SPARK_CLASS = r"INCOMPATIBLE_DATA_FOR_TABLE\.CANNOT_SAFELY_CAST"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-insert-store-assign").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _tables(spark: ReparkSession, target_cols: str, source_cols: str) -> None:
    spark.sql(f"CREATE TABLE {FQ} ({target_cols}) USING iceberg")
    spark.sql(f"CREATE TABLE {SRC} ({source_cols}) USING iceberg")


def _rows(spark: ReparkSession, projection: str) -> list[dict[str, object]]:
    return spark.sql(f"SELECT {projection} FROM {FQ}").to_arrow().to_pylist()


# === Refusals — the measured silently-wrong pairs =========================================


def test_date_to_int_overwrite_refuses(spark: ReparkSession) -> None:
    """The WI-1 headline: DATE source into an INT target wrote 18262 days-since-epoch."""
    _tables(spark, "k INT, v INT", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k, v") == []


def test_date_to_bigint_overwrite_refuses(spark: ReparkSession) -> None:
    """The BIGINT twin — arrow-rs permits Date32→Int64 as the same reinterpretation."""
    _tables(spark, "k INT, v BIGINT", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k, v") == []


def test_boolean_to_int_overwrite_refuses(spark: ReparkSession) -> None:
    """r13b's non-MERGE twin: boolean into INT was a silent 1."""
    _tables(spark, "k INT, flag INT", "k INT, b BOOLEAN")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, true)")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, b FROM {SRC}")
    assert _rows(spark, "k, flag") == []


def test_timestamp_to_bigint_overwrite_refuses(spark: ReparkSession) -> None:
    """r13c's non-MERGE twin: timestamp into BIGINT was a silent raw-epoch write."""
    _tables(spark, "k INT, ts_us BIGINT", "k INT, ev TIMESTAMP")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, TIMESTAMP '2026-01-01 00:00:00')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, ev FROM {SRC}")
    assert _rows(spark, "k, ts_us") == []


def test_string_to_bigint_overwrite_refuses(spark: ReparkSession) -> None:
    """string→numeric is not ANSI store-assignable (Spark refuses even for numeric strings)."""
    _tables(spark, "k INT, n BIGINT", "k INT, txt STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, '42')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, txt FROM {SRC}")
    assert _rows(spark, "k, n") == []


def test_column_list_overwrite_refuses(spark: ReparkSession) -> None:
    """The named-column arm of the positional map runs the SAME gate as the all-columns arm."""
    _tables(spark, "k INT, v INT, note STRING", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT OVERWRITE {FQ} (k, v) SELECT k, v FROM {SRC}")
    assert _rows(spark, "k, v, note") == []


def test_insert_into_overwrite_mode_refuses(spark: ReparkSession) -> None:
    """Facade door: ``write.mode('overwrite').insertInto`` lowers onto ``INSERT OVERWRITE``."""
    _tables(spark, "k INT, v INT", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.table(SRC).write.format("iceberg").mode("overwrite").insertInto(FQ)
    assert _rows(spark, "k, v") == []


# === The needle =============================================================================


def test_refusal_names_the_column_both_types_and_the_spark_class(spark: ReparkSession) -> None:
    """One message must carry everything a caller needs: path, column, both types, Spark class."""
    _tables(spark, "k INT, v INT", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    message = str(excinfo.value)
    assert "INSERT OVERWRITE cannot store-assign column `v`" in message, message
    assert "not ANSI-store-assignable" in message, message
    assert "Date32" in message, message
    assert "Int32" in message, message
    assert "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST" in message, message


def test_refusal_leaves_the_prior_snapshot_intact(spark: ReparkSession) -> None:
    """The durable half: a refused overwrite must not wipe or corrupt what was already there."""
    _tables(spark, "k INT, v INT", "k INT, v DATE")
    spark.sql(f"INSERT INTO {FQ} VALUES (7, 700)")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    with pytest.raises(AnalysisException, match=SPARK_CLASS):
        spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k, v") == [{"k": 7, "v": 700}]


# === Positive controls — the gate must not narrow a legal write ============================


def test_numeric_widening_still_overwrites(spark: ReparkSession) -> None:
    """INT→BIGINT widening is ANSI-store-assignable and must still write."""
    _tables(spark, "k INT, big BIGINT", "k INT, small INT")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, 9)")
    spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, small FROM {SRC}")
    assert _rows(spark, "k, big") == [{"k": 1, "big": 9}]


def test_numeric_narrowing_still_overwrites(spark: ReparkSession) -> None:
    """Narrowing is analysis-legal in Spark too — overflow is the runtime ANSI error, not this."""
    _tables(spark, "k INT, small INT", "k INT, big BIGINT")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, 9)")
    spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, big FROM {SRC}")
    assert _rows(spark, "k, small") == [{"k": 1, "small": 9}]


def test_atomic_to_string_still_overwrites(spark: ReparkSession) -> None:
    """AtomicType→StringType is store-assignable — including the DATE this unit refuses into INT."""
    _tables(spark, "k INT, s STRING", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "s") == [{"s": "2020-01-01"}]


def test_date_to_timestamp_still_overwrites(spark: ReparkSession) -> None:
    """Date↔Timestamp is store-assignable in both directions (Spark's own matrix)."""
    _tables(spark, "k INT, ev TIMESTAMP", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k") == [{"k": 1}]


def test_null_fill_of_an_omitted_column_still_overwrites(spark: ReparkSession) -> None:
    """NullType→anything: the column list may omit a nullable field and NULL-fill it."""
    _tables(spark, "k INT, big BIGINT, extra STRING", "k INT, small INT")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, 9)")
    spark.sql(f"INSERT OVERWRITE {FQ} (k, big) SELECT k, small FROM {SRC}")
    assert _rows(spark, "k, big, extra") == [{"k": 1, "big": 9, "extra": None}]


def test_identity_roundtrip_still_overwrites(spark: ReparkSession) -> None:
    """The commonest shape of all — same types in, same types out — is untouched."""
    _tables(spark, "k INT, v DATE", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")
    spark.sql(f"INSERT OVERWRITE {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k") == [{"k": 1}]
