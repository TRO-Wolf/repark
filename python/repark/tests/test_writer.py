"""Facade tests for ``DataFrame.write`` (Group E: E6).

The :class:`~repark.dataframe.DataFrameWriter` routes writes through the engine's **existing
sanctioned SQL paths** — CTAS (``CREATE TABLE … USING iceberg AS SELECT``), ``INSERT INTO``, and
``INSERT OVERWRITE`` — via a throwaway temp view. It adds no commit/transaction machinery. These
tests drive the real boundary end to end against the in-memory Iceberg catalog (the same path
``test_catalog_flow.py`` uses), reading each write back to verify it landed.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

TABLE = "glue_catalog.writer_ns.tbl"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog + namespace (local, AWS-free)."""
    session = ReparkSession.builder.appName("pytest-writer").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql("CREATE NAMESPACE glue_catalog.writer_ns")
    return session


def _read(spark: ReparkSession, table: str = TABLE) -> list[dict[str, object]]:
    """Read a table back, ordered by id, as a list of dicts."""
    return spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow().to_pylist()


def _source(spark: ReparkSession, rows: str) -> object:
    """A small (id, name) DataFrame from an inline VALUES list, e.g. ``"(1,'a'),(2,'b')"``."""
    return spark.sql(f"SELECT * FROM (VALUES {rows}) AS t(id, name)")


# ==================================================================================================
# E6 — saveAsTable: create / append / overwrite / error / ignore
# ==================================================================================================


def test_save_as_table_creates_via_ctas(spark: ReparkSession) -> None:
    assert not spark.catalog.tableExists(TABLE)
    _source(spark, "(1,'a'),(2,'b')").write.saveAsTable(TABLE)
    assert spark.catalog.tableExists(TABLE)
    assert _read(spark) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_save_as_table_append_adds_rows(spark: ReparkSession) -> None:
    _source(spark, "(1,'a'),(2,'b')").write.saveAsTable(TABLE)
    _source(spark, "(3,'c')").write.mode("append").saveAsTable(TABLE)
    assert _read(spark) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]


def test_save_as_table_overwrite_replaces_rows(spark: ReparkSession) -> None:
    _source(spark, "(1,'a'),(2,'b')").write.saveAsTable(TABLE)
    _source(spark, "(9,'z')").write.mode("overwrite").saveAsTable(TABLE)
    assert _read(spark) == [{"id": 9, "name": "z"}]


def test_save_as_table_error_mode_raises_on_existing(spark: ReparkSession) -> None:
    _source(spark, "(1,'a')").write.saveAsTable(TABLE)
    with pytest.raises(AnalysisException, match="already exists"):
        _source(spark, "(2,'b')").write.mode("error").saveAsTable(TABLE)
    # errorifexists is the same mode under its PySpark spelling.
    with pytest.raises(AnalysisException, match="already exists"):
        _source(spark, "(2,'b')").write.mode("errorifexists").saveAsTable(TABLE)


def test_save_as_table_default_mode_is_errorifexists(spark: ReparkSession) -> None:
    # No .mode(...) → PySpark default errorifexists: a second save into the same table raises.
    _source(spark, "(1,'a')").write.saveAsTable(TABLE)
    with pytest.raises(AnalysisException, match="already exists"):
        _source(spark, "(2,'b')").write.saveAsTable(TABLE)


def test_save_as_table_ignore_mode_is_noop_on_existing(spark: ReparkSession) -> None:
    _source(spark, "(1,'a')").write.saveAsTable(TABLE)
    _source(spark, "(2,'b')").write.mode("ignore").saveAsTable(TABLE)
    assert _read(spark) == [{"id": 1, "name": "a"}], "ignore leaves the existing table untouched"


# ==================================================================================================
# E6 — insertInto (position-based) + partitionBy
# ==================================================================================================


def test_insert_into_is_position_based(spark: ReparkSession) -> None:
    _source(spark, "(1,'a')").write.saveAsTable(TABLE)
    # A source whose columns are named differently inserts BY POSITION (Spark insertInto).
    spark.sql("SELECT * FROM (VALUES (2, 'b')) AS t(other_id, other_name)").write.insertInto(TABLE)
    assert _read(spark) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_insert_into_overwrite(spark: ReparkSession) -> None:
    _source(spark, "(1,'a'),(2,'b')").write.saveAsTable(TABLE)
    _source(spark, "(9,'z')").write.insertInto(TABLE, overwrite=True)
    assert _read(spark) == [{"id": 9, "name": "z"}]


# ==================================================================================================
# E6/R1 — saveAsTable resolves columns BY NAME (append/overwrite), unlike positional insertInto
# ==================================================================================================


def _read2(spark: ReparkSession, table: str) -> pa.Table:
    """Read an (a, b) table back ordered by a, as an Arrow table (for value+type pins)."""
    return spark.sql(f"SELECT a, b FROM {table} ORDER BY a").to_arrow()


def test_parity_save_as_table_append_resolves_by_name(spark: ReparkSession) -> None:
    """R1 (S1 — the transposition bug): append into an existing table resolves columns BY NAME.

    The judges proved the old positional ``INSERT INTO … SELECT *`` silently TRANSPOSES a reordered
    same-typed frame into the persisted table. PySpark ``DataFrameWriter.saveAsTable`` resolves by
    name (its docs: unlike ``insertInto``). Recorded from live PySpark 4.1.2 (Java 17): creating
    ``t(a,b)`` from ``[(1,10)]`` then appending a frame whose columns are spelled ``(b, a)`` with
    values ``(20, 2)`` yields rows ``[{a:1,b:10}, {a:2,b:20}]`` — ``a`` gets 2 and ``b`` gets 20
    (by NAME), NOT a=20 / b=2 (position).
    Both columns are ``bigint``. repark now agrees (both engines), pinned on value AND Arrow type.
    """
    table = "glue_catalog.writer_ns.byname"
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(table)
    # source columns REORDERED to (b, a); same type so a positional write would transpose silently.
    spark.createDataFrame([(20, 2)], ["b", "a"]).write.mode("append").saveAsTable(table)
    got = _read2(spark, table)
    golden = pa.table(
        [pa.array([1, 2], pa.int64()), pa.array([10, 20], pa.int64())],
        schema=pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.int64())]),
    )
    assert got.to_pylist() == golden.to_pylist(), (
        "append resolves by NAME (a=2, b=20), not position"
    )
    assert got.schema.field("a").type == pa.int64()
    assert got.schema.field("b").type == pa.int64()


def test_parity_save_as_table_overwrite_resolves_by_name(spark: ReparkSession) -> None:
    # overwrite also resolves by name (recorded oracle: overwrite with (b=99,a=999) → a=999,b=99).
    table = "glue_catalog.writer_ns.bynameow"
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(table)
    spark.createDataFrame([(99, 999)], ["b", "a"]).write.mode("overwrite").saveAsTable(table)
    assert _read2(spark, table).to_pylist() == [{"a": 999, "b": 99}]


def test_parity_save_as_table_append_case_insensitive_by_name(spark: ReparkSession) -> None:
    """Audit BUG-007: saveAsTable by-name conform is case-insensitive (Spark caseSensitive=false).

    Target columns ``a``/``b``; source frame spells ``A``/``B``. Pre-fix the exact-set match
    refused; post-fix the values land by casefold name (positional SELECT uses source names
    in target order).
    """
    table = "glue_catalog.writer_ns.byname_ci"
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(table)
    spark.createDataFrame([(20, 2)], ["B", "A"]).write.mode("append").saveAsTable(table)
    got = _read2(spark, table)
    assert got.to_pylist() == [{"a": 1, "b": 10}, {"a": 2, "b": 20}]
    assert got.schema.field("a").type == pa.int64()
    assert got.schema.field("b").type == pa.int64()


def test_insert_into_positional_vs_save_as_table_by_name_discriminator(
    spark: ReparkSession,
) -> None:
    """The re-judge's discriminating case: the SAME reordered frame lands DIFFERENTLY through
    positional ``insertInto`` vs by-name ``saveAsTable`` — so a test that passed with the positional
    saveAsTable bug present cannot also pass now. Oracle (PySpark 4.1.2): a ``(b, a)=(20, 2)`` frame
    → ``insertInto`` gives ``a=20, b=2`` (position); ``saveAsTable`` gives ``a=2, b=20`` (name).
    """
    t_pos = "glue_catalog.writer_ns.pos"
    t_name = "glue_catalog.writer_ns.name"
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(t_pos)
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(t_name)
    reordered_pos = spark.createDataFrame([(20, 2)], ["b", "a"])
    reordered_name = spark.createDataFrame([(20, 2)], ["b", "a"])
    reordered_pos.write.insertInto(t_pos)  # positional → a=20, b=2
    reordered_name.write.mode("append").saveAsTable(t_name)  # by name → a=2, b=20
    pos_row = _read2(spark, t_pos).to_pylist()[1]
    name_row = _read2(spark, t_name).to_pylist()[1]
    assert pos_row == {"a": 20, "b": 2}, "insertInto is POSITIONAL"
    assert name_row == {"a": 2, "b": 20}, "saveAsTable is BY NAME"
    assert pos_row != name_row, "the two writers genuinely diverge on a reordered frame"


def test_save_as_table_append_extra_column_raises(spark: ReparkSession) -> None:
    # An EXTRA source column (source columns ⊋ table columns) → AnalysisException, never a silent
    # drop (Spark parity — oracle: "column number ... doesn't match the data schema").
    table = "glue_catalog.writer_ns.extra"
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(table)
    with pytest.raises(AnalysisException, match="by name"):
        spark.createDataFrame([(2, 20, 99)], ["a", "b", "extra"]).write.mode("append").saveAsTable(
            table
        )
    # the table is untouched (the write never ran).
    assert _read2(spark, table).to_pylist() == [{"a": 1, "b": 10}]


def test_save_as_table_append_missing_column_raises(spark: ReparkSession) -> None:
    # A MISSING source column (source columns ⊊ table columns) → AnalysisException likewise.
    table = "glue_catalog.writer_ns.missing"
    spark.createDataFrame([(1, 10)], ["a", "b"]).write.saveAsTable(table)
    with pytest.raises(AnalysisException, match="by name"):
        spark.createDataFrame([(2,)], ["a"]).write.mode("append").saveAsTable(table)
    assert _read2(spark, table).to_pylist() == [{"a": 1, "b": 10}]


def test_save_as_table_partition_by(spark: ReparkSession) -> None:
    # Identity partitionBy threads into CTAS PARTITIONED BY; a partition-filtered read returns just
    # that partition's rows, proving the table really is partitioned.
    table = "glue_catalog.writer_ns.parted"
    spark.sql(
        "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS t(id, category)"
    ).write.partitionBy("category").saveAsTable(table)
    everything = spark.sql(f"SELECT id, category FROM {table} ORDER BY id").to_arrow().to_pylist()
    assert everything == [
        {"id": 1, "category": "a"},
        {"id": 2, "category": "b"},
        {"id": 3, "category": "a"},
    ]
    only_a = (
        spark.sql(f"SELECT id FROM {table} WHERE category = 'a' ORDER BY id").to_arrow().to_pylist()
    )
    assert only_a == [{"id": 1}, {"id": 3}]


# ==================================================================================================
# E6/E8 — format / mode validation (reject loudly)
# ==================================================================================================


def test_format_rejects_non_iceberg_for_save_as_table(spark: ReparkSession) -> None:
    # format() accepts the name (path writes use parquet); saveAsTable still requires iceberg.
    for bad in ("json", "csv", "delta", "parquet"):
        with pytest.raises(ValueError, match="iceberg"):
            _source(spark, "(1,'a')").write.format(bad).saveAsTable(TABLE)


def test_format_iceberg_is_accepted(spark: ReparkSession) -> None:
    # The explicit-but-default format still writes (fluent chaining returns the writer).
    _source(spark, "(1,'a')").write.format("iceberg").saveAsTable(TABLE)
    assert _read(spark) == [{"id": 1, "name": "a"}]


def test_mode_rejects_invalid(spark: ReparkSession) -> None:
    # Group X (Critic F5): the CLASS is AnalysisException, not ValueError. Live pyspark 4.0.0
    # rejects an unknown save mode JVM-side with `[INVALID_SAVE_MODE] The specified save mode
    # "bogus" is invalid…` — an AnalysisException — so this is NOT Python-side arg validation and
    # must not be a PySpark*Error wrapper. This pin used to assert `ValueError`, codifying the
    # divergence; flipped in the same commit as the fix (the Group S discipline).
    # MUTATION: revert the one-line raise in `DataFrameWriter.mode` to PySparkValueError → RED
    # (facade-side, so no maturin rebuild is needed to reproduce).
    with pytest.raises(AnalysisException, match="mode must be one of") as raised:
        _source(spark, "(1,'a')").write.mode("upsert")
    # Spark's error-condition tag travels with the class (message parity, not the claim).
    assert "[INVALID_SAVE_MODE]" in str(raised.value)
    # Parent catch-compat, the S pattern — and the deliberate break: no longer a ValueError
    # (PySpark's isn't either, so `except ValueError` never caught this in PySpark).
    assert isinstance(raised.value, PySparkException)
    assert isinstance(raised.value, RuntimeError)
    assert not isinstance(raised.value, ValueError)


# ==================================================================================================
# C1-SEC-001 — writer table-name identifier injection (quote + reject SQL fragments)
# ==================================================================================================


def test_save_as_table_rejects_sql_fragment_name(spark: ReparkSession) -> None:
    """Malicious table names must raise AnalysisException — never reach SQL as bare text."""
    with pytest.raises(AnalysisException, match=r"invalid table identifier|SQL fragments"):
        _source(spark, "(1,'a')").write.saveAsTable("t; DROP")


def test_save_as_table_rejects_path_escape_segment(spark: ReparkSession) -> None:
    """O3-C4-SEC-001: quoted '..' / separator segments fail at _sql_table_ref (not only CTAS)."""
    with pytest.raises(AnalysisException, match=r"path traversal|path separators|invalid"):
        _source(spark, "(1,'a')").write.saveAsTable('glue_catalog.writer_ns.".."')
    with pytest.raises(AnalysisException, match=r"path traversal|path separators|invalid"):
        _source(spark, "(1,'a')").write.saveAsTable('glue_catalog."a/b".t')


def test_insert_into_rejects_sql_fragment_name(spark: ReparkSession) -> None:
    _source(spark, "(1,'a')").write.saveAsTable(TABLE)
    with pytest.raises(AnalysisException, match=r"invalid table identifier|SQL fragments"):
        _source(spark, "(2,'b')").write.insertInto("t; DROP TABLE glue_catalog.writer_ns.tbl")


def test_write_to_rejects_sql_fragment_name(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"invalid table identifier|SQL fragments"):
        _source(spark, "(1,'a')").writeTo("t; DROP").create()


def test_save_as_table_accepts_valid_multipart_name(spark: ReparkSession) -> None:
    """Valid multipart names round-trip after quoting (regression guard for _sql_table_ref)."""
    table = "glue_catalog.writer_ns.valid_multipart"
    _source(spark, "(1,'a')").write.saveAsTable(table)
    assert _read(spark, table) == [{"id": 1, "name": "a"}]
    _source(spark, "(2,'b')").write.mode("append").saveAsTable(table)
    assert _read(spark, table) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_by_name_casefold_map_rejects_ambiguous_source_columns() -> None:
    """Critic-1 Q-004: case-colliding source columns fail loud (no last-write-wins)."""
    from repark.dataframe import _by_name_casefold_map
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException, match="ambiguous"):
        _by_name_casefold_map(["id", "ID"], surface="DataFrame")


def test_by_name_casefold_map_rejects_exact_duplicate_columns() -> None:
    """P4C1-Q-006: exact duplicate names (['id','id']) refuse, not last-write-win."""
    from repark.dataframe import _by_name_casefold_map
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException, match="duplicate"):
        _by_name_casefold_map(["id", "id"], surface="DataFrame")
