"""MERGE scan-prune / residual-probe hardening — M1, M5, M6, M7 (r1 / r2 / r3 / r11).

Oracle surface: Spark 4 MERGE result sets from the 2026-08-14 MERGE-audit repro battery
(planning/hardening/merge-repro-battery.py cases r1, r2, r3, r11 — converted, not imported).
End-to-end pins run against the in-memory Iceberg catalog (local only — no AWS). All
assertions ride the Arrow path (``to_arrow``), value AND type.

These pins MUST go red if the corresponding skip-conjunct / char-boundary / case-resolve
fix in ``scan_prune.rs`` is reverted.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

FQ = "mem.ns.prune_t"
SRC = "mem.ns.prune_s"
COW_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-merge-scan-prune").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _create(spark: ReparkSession, table: str, columns: str) -> None:
    spark.sql(f"CREATE TABLE {table} ({columns}) USING iceberg TBLPROPERTIES ({COW_PROPS})")


def _is_string(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string(arrow_type) or arrow_type == pa.large_string()


def _make_session(tmp_path: Path, app_name: str, *, scan_pruning: bool) -> ReparkSession:
    builder = ReparkSession.builder.appName(app_name)
    if not scan_pruning:
        builder = builder.config("repark.merge.scan-pruning", "false")
    session = builder.getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def test_merge_string_source_int_target_updates_both_keys(spark: ReparkSession) -> None:
    """R1 / M1: Utf8 source keys vs INT target must yield Spark's 2-row upsert.

    Without the identical-type skip, lexicographic min/max of ``'10'``/``'9'``
    strict-casts to ``id >= 10 AND id <= 9``, every file is pruned, updates are
    lost, and both source rows insert as duplicates (4 rows).
    """
    _create(spark, FQ, "id INT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (9,'x'),(10,'y')")
    _create(spark, SRC, "id STRING, v STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES ('9','a'),('10','b')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    )
    out = spark.sql(f"SELECT id, v FROM {FQ} ORDER BY id").to_arrow()
    assert out.schema.field("id").type == pa.int32()
    assert _is_string(out.schema.field("v").type)
    assert out.to_pylist() == [{"id": 9, "v": "a"}, {"id": 10, "v": "b"}]


def test_merge_bigint_source_int_target_does_not_abort(spark: ReparkSession) -> None:
    """R2 / M6: BIGINT source key 3e9 vs INT target must not abort MERGE.

    The in-range row updates; the out-of-Int32 source row matches nothing (Spark
    widens). A leftover ``?``-cast on the bounds probe raises ArrowInvalid.
    """
    _create(spark, FQ, "id INT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (5,'x')")
    _create(spark, SRC, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (5,'a'),(3000000000,'big')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"
    )
    out = spark.sql(f"SELECT id, v FROM {FQ} ORDER BY id").to_arrow()
    assert out.schema.field("id").type == pa.int32()
    assert _is_string(out.schema.field("v").type)
    assert out.to_pylist() == [{"id": 5, "v": "a"}]


def test_merge_on_utf8_literal_does_not_panic(spark: ReparkSession) -> None:
    """R3 / M5: non-ASCII ON literal (battery shape) must not panic the scanners.

    The byte-offset scanners sliced ``&sql[index..]`` mid-``ü``. After
    ``char_indices()`` the MERGE updates the matched row.
    """
    _create(spark, FQ, "id BIGINT, city STRING, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (1,'Zürich','x')")
    _create(spark, SRC, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (1,'a')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s "
        "ON t.id = s.id AND t.city = 'Zürich' "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )
    out = spark.sql(f"SELECT id, v FROM {FQ} ORDER BY id").to_arrow()
    assert out.schema.field("id").type == pa.int64()
    assert _is_string(out.schema.field("v").type)
    assert out.to_pylist() == [{"id": 1, "v": "a"}]


def test_merge_on_utf8_column_name_does_not_panic(spark: ReparkSession) -> None:
    """M5: non-ASCII COLUMN name in ON (unquoted ident via CTAS) must not panic.

    Complements the r3 literal pin. The scanners walk ``t.Zürich``; the id
    equality still drives the update. Column created by CTAS alias — no quoted
    DDL.
    """
    spark.sql(
        f"CREATE TABLE {FQ} USING iceberg TBLPROPERTIES ({COW_PROPS}) AS "
        "SELECT CAST(1 AS BIGINT) AS id, 'Zürich' AS Zürich, 'x' AS v"
    )
    _create(spark, SRC, "id BIGINT, city STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (1,'Zürich')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s "
        "ON t.id = s.id AND t.Zürich = s.city "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )
    out = spark.sql(f"SELECT id, v FROM {FQ} ORDER BY id").to_arrow()
    assert out.schema.field("id").type == pa.int64()
    assert _is_string(out.schema.field("v").type)
    assert out.to_pylist() == [{"id": 1, "v": "a"}]


def test_merge_mixed_case_on_matches_pruning_off(tmp_path: Path) -> None:
    """R11 / M7: mixed-case ON over lowercase schema, pruning on ≡ pruning off.

    Quoting the unresolved ``CustomerId`` (case-sensitive) aborted the bounds
    probe while the join itself resolved. After case-insensitive resolve-then-
    quote, both knobs yield the same Arrow result.
    """
    on_session = _make_session(tmp_path / "on", "pytest-m7-on", scan_pruning=True)
    _create(on_session, FQ, "customerid BIGINT, amt DOUBLE")
    on_session.sql(f"INSERT INTO {FQ} VALUES (1, 1.0)")
    _create(on_session, SRC, "customerid BIGINT, amt DOUBLE")
    on_session.sql(f"INSERT INTO {SRC} VALUES (1, 2.0)")
    on_session.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.CustomerId = s.CustomerId "
        "WHEN MATCHED THEN UPDATE SET amt = s.amt"
    )
    on_out = on_session.sql(f"SELECT customerid, amt FROM {FQ} ORDER BY customerid").to_arrow()
    on_session.stop()

    off_session = _make_session(tmp_path / "off", "pytest-m7-off", scan_pruning=False)
    _create(off_session, FQ, "customerid BIGINT, amt DOUBLE")
    off_session.sql(f"INSERT INTO {FQ} VALUES (1, 1.0)")
    _create(off_session, SRC, "customerid BIGINT, amt DOUBLE")
    off_session.sql(f"INSERT INTO {SRC} VALUES (1, 2.0)")
    off_session.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.CustomerId = s.CustomerId "
        "WHEN MATCHED THEN UPDATE SET amt = s.amt"
    )
    off_out = off_session.sql(f"SELECT customerid, amt FROM {FQ} ORDER BY customerid").to_arrow()
    off_session.stop()

    assert on_out.schema.field("customerid").type == pa.int64()
    assert on_out.schema.field("amt").type == pa.float64()
    assert on_out.to_pylist() == [{"customerid": 1, "amt": 2.0}]
    assert on_out.to_pylist() == off_out.to_pylist()
    assert on_out.schema.field("customerid").type == off_out.schema.field("customerid").type
    assert on_out.schema.field("amt").type == off_out.schema.field("amt").type
