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

WI-1 gated ``INSERT OVERWRITE`` and the public ``append`` entry point at the batch-conform seam.
The four plain-INSERT doors — ``INSERT INTO … SELECT``, ``INSERT INTO … VALUES``,
``writeTo().append()`` and ``write.insertInto()`` — never reach a
``crates/repark-iceberg/src/write/`` seam at all: DataFusion's own ``insert_to_plan`` injects the
conforming ``CAST`` at SQL-planning time and hands a schema-conformed plan to the fork's
``IcebergTableProvider::insert_into``, by which point the source type is gone. **WI-2** closes
them one stage earlier, with an ``AnalyzerRule`` over ``LogicalPlan::Dml(WriteOp::Insert)``
(``crates/repark-iceberg/src/write/insert_gate.rs``) that runs the SAME matrix — imported, never
duplicated — against the pre-cast types still visible in the synthesized projection's input
schema. Those pins are the ``=== WI-2`` section at the bottom of this file.

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


# Refusals — the measured silently-wrong pairs


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


# The needle


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


# Positive controls — the gate must not narrow a legal write


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


# WI-2 — the four plain-INSERT doors
# The refusal is an ANALYSIS refusal: nothing is staged and no snapshot moves.


def _plain_insert_tables(spark: ReparkSession) -> None:
    """Target ``INT``, source ``DATE`` — the measured silently-wrong pair, one row loaded."""
    _tables(spark, "k INT, v INT", "k INT, v DATE")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, DATE '2020-01-01')")


def test_insert_into_select_refuses(spark: ReparkSession) -> None:
    """Door 1: the plainest write of all — no CAST anywhere in the statement the caller wrote."""
    _plain_insert_tables(spark)
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT INTO {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k, v") == []


def test_write_to_append_refuses(spark: ReparkSession) -> None:
    """Door 4: ``df.writeTo(t).append()`` builds a by-name projection into an ``INSERT INTO``."""
    _plain_insert_tables(spark)
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.table(SRC).writeTo(FQ).append()
    assert _rows(spark, "k, v") == []


def test_write_insert_into_append_mode_refuses(spark: ReparkSession) -> None:
    """Door 5: ``df.write.mode('append').insertInto`` builds a positional ``INSERT INTO``."""
    _plain_insert_tables(spark)
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.table(SRC).write.format("iceberg").mode("append").insertInto(FQ)
    assert _rows(spark, "k, v") == []


def test_plain_insert_refusal_names_the_statement_the_caller_wrote(spark: ReparkSession) -> None:
    """The path label is ``INSERT INTO``, not ``INSERT OVERWRITE`` — one message, one statement."""
    _plain_insert_tables(spark)
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"INSERT INTO {FQ} SELECT k, v FROM {SRC}")
    message = str(excinfo.value)
    assert "INSERT INTO cannot store-assign column `v`" in message, message
    assert "Date32" in message and "Int32" in message, message
    assert "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST" in message, message


def test_plain_insert_refuses_the_rest_of_the_matrix(spark: ReparkSession) -> None:
    """boolean→int, timestamp→bigint and string→numeric reach the same node as date→int."""
    for target, source, value in [
        ("k INT, v INT", "k INT, v BOOLEAN", "true"),
        ("k INT, v BIGINT", "k INT, v TIMESTAMP", "TIMESTAMP '2026-01-01 00:00:00'"),
        ("k INT, v BIGINT", "k INT, v STRING", "'42'"),
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {FQ}")
        spark.sql(f"DROP TABLE IF EXISTS {SRC}")
        _tables(spark, target, source)
        spark.sql(f"INSERT INTO {SRC} VALUES (1, {value})")
        with pytest.raises(AnalysisException, match=NEEDLE):
            spark.sql(f"INSERT INTO {FQ} SELECT k, v FROM {SRC}")
        assert _rows(spark, "k, v") == []


def test_plain_insert_refuses_the_g6_5_reverse_pair(spark: ReparkSession) -> None:
    """The write-path twin of G6-5: INT into a DATE column. The matrix was always right here."""
    _tables(spark, "k INT, d DATE", "k INT, n INT")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, 18262)")
    with pytest.raises(AnalysisException, match=NEEDLE):
        spark.sql(f"INSERT INTO {FQ} SELECT k, n FROM {SRC}")
    assert _rows(spark, "k, d") == []


def test_an_explicit_user_cast_still_writes(spark: ReparkSession) -> None:
    """**The constraint that shapes the rule.** Spark treats an explicit CAST as the user's stated
    intent and accepts it where bare store assignment refuses. The gate judges ONLY the conform
    cast DataFusion synthesizes over a source COLUMN, so the explicit form — which reaches the DML
    projection already conformed, as a bare column reference — is invisible to it.
    """
    _tables(spark, "k INT, v INT", "k INT, b BOOLEAN")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, true)")
    spark.sql(f"INSERT INTO {FQ} SELECT k, CAST(b AS INT) FROM {SRC}")
    assert _rows(spark, "k, v") == [{"k": 1, "v": 1}]


def test_plain_insert_positive_controls_still_write(spark: ReparkSession) -> None:
    """Widening, atomic→string and identity are store-assignable and must still write."""
    _tables(spark, "k INT, v BIGINT", "k INT, v INT")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, 9)")
    spark.sql(f"INSERT INTO {FQ} SELECT k, v FROM {SRC}")
    assert _rows(spark, "k, v") == [{"k": 1, "v": 9}]


def test_the_named_residual_is_a_literal_values_row(spark: ReparkSession) -> None:
    """WI-2's honest residual, pinned so it cannot rot into a surprise.

    ``INSERT INTO … VALUES`` conforms its literals inside the ``Values`` node
    (``LogicalPlanBuilder::infer_inner`` rewrites each row element as
    ``row[j].cast_to(field_type, schema)``), where a synthesized cast and a user-written
    ``CAST(x AS INT)`` are byte-identical — the outer conform is a no-op once the inner cast
    already yielded the target type. Gating it would refuse a legal explicit cast, which is worse
    than the gap, so the rule judges only ``Cast(Column, …)``.

    The half of the residual that carried a silently-wrong VALUE is closed anyway: ``DATE → INT``
    is refused wherever the cast appears, ``Values`` node included, by the G6-3 cast-legality gate
    — with the CAST class rather than the write class, which this test records rather than hides.
    What is genuinely open is a cast-legal-but-not-store-assignable literal, and ``true`` into an
    ``INT`` column is exactly that.
    """
    _tables(spark, "k INT, v INT", "k INT, b BOOLEAN")
    # Open: writes 1, where Spark refuses INCOMPATIBLE_DATA_FOR_TABLE.
    spark.sql(f"INSERT INTO {FQ} VALUES (1, true)")
    assert _rows(spark, "k, v") == [{"k": 1, "v": 1}]
    # Closed, by the other gate and with the other class.
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH\.CAST_WITH_FUNC_SUGGESTION"):
        spark.sql(f"INSERT INTO {FQ} VALUES (2, DATE '2020-01-01')")
