"""SE-1 PR-D1 facade pins for ``declareSorted(..., tightenNulls=True)``.

The existing hint-mode nodes in ``test_declare_sorted.py`` stay byte-identical.
This file pins the c+ flag: value AND type, refuse-on-nulls, and hint-after-tighten
restore. Serving-shape SortExec elision is the Rust Spark-door execution-layer pin
(``crates/repark-spark/tests/declared_sorted_tighten.rs``) — facade EXPLAIN is still
the unwritten plan until PR-D3.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException
from repark.spark.dataframe import DataFrame
from repark.spark.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from repark.spark.window import Window

SCHEMA = StructType(
    [
        StructField("sym", StringType(), True),
        StructField("ts", LongType(), True),
        StructField("val", DoubleType(), True),
    ]
)

SORTED_ROWS = [(sym, tick, float(tick)) for sym in ("AAA", "BBB", "CCC") for tick in range(50)]


@pytest.fixture
def spark() -> ReparkSession:
    return (
        ReparkSession.builder.appName("pytest-declare-sorted-tighten")
        .config("repark.target.partitions", 1)
        .getOrCreate()
    )


def test_tighten_results_match_hint_and_keys_report_non_nullable(spark: ReparkSession) -> None:
    """Value-identical to hint; tightened keys are non-nullable on the Arrow path.

    Also pins exact ``df.schema`` types (R-3 type-exactness) and that no internal
    ``repark.tighten_nulls`` tag is visible on ``to_arrow()`` — an observation, not a strip
    discriminator (see the MEASURED note at that assertion).
    """
    hint = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts")
    tight = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts", tightenNulls=True)
    window = Window.partitionBy("sym").orderBy("ts")
    hint_arrow = hint.withColumn("rn", F.row_number().over(window)).to_arrow()
    tight_arrow = tight.withColumn("rn", F.row_number().over(window)).to_arrow()
    # Compare values only: tighten changes key nullability (the lever).
    assert hint_arrow.column("rn").equals(tight_arrow.column("rn"))
    assert hint_arrow.column("sym").equals(tight_arrow.column("sym"))
    assert hint_arrow.column("ts").equals(tight_arrow.column("ts"))
    assert tight_arrow.schema.field("sym").nullable is False
    assert tight_arrow.schema.field("ts").nullable is False
    assert hint_arrow.schema.field("sym").nullable is True
    assert hint_arrow.schema.field("ts").nullable is True
    # F4: df.schema is the analyzed logical path, distinct from to_arrow().
    assert tight.schema["sym"].nullable is False
    assert tight.schema["ts"].nullable is False
    assert hint.schema["sym"].nullable is True
    assert hint.schema["ts"].nullable is True
    assert isinstance(tight.schema["sym"].dataType, StringType)
    assert isinstance(tight.schema["ts"].dataType, LongType)
    assert isinstance(tight.schema["val"].dataType, DoubleType)
    assert tight.schema["val"].nullable is True
    assert tight_arrow.schema.field("val").nullable is True
    # NOT a strip discriminator (Y-6, MEASURED): with BOTH the Rust
    # `strip_tighten_export_metadata` and the facade `_strip_internal_tighten_metadata` no-oped,
    # the collected schema's field metadata is already empty — DataFusion drops field metadata
    # across physical execution. Kept as a user-visible no-leak assertion; the export-boundary
    # discriminator is `test_analyzed_schema_export_carries_no_tighten_tag`.
    ts_meta = tight_arrow.schema.field("ts").metadata or {}
    assert b"repark.tighten_nulls" not in ts_meta
    assert "repark.tighten_nulls" not in ts_meta
    table_meta = tight_arrow.schema.metadata or {}
    assert b"repark.tighten_nulls" not in table_meta
    assert "repark.tighten_nulls" not in table_meta


def test_analyzed_schema_export_carries_no_tighten_tag(spark: ReparkSession) -> None:
    """Y-6 (round 4): the ``df.schema`` / Arrow-C-schema export boundary, not just the helper.

    MEASURED: no-oping ``repark_core::strip_tighten_export_metadata`` leaves
    ``analyzed_arrow_schema`` reporting ``{b'repark.tighten_nulls': b'1'}`` on both keys —
    this node is the one that goes red. The ``to_arrow()`` assertions in
    ``test_tighten_results_match_hint_and_keys_report_non_nullable`` do NOT cover that layer:
    with BOTH strips no-oped the collected schema's field metadata is already empty (DataFusion
    drops field metadata across physical execution), so they are non-discriminating for the
    strip and are documented as such there.
    """
    import pyarrow as pa

    class _CapsuleWrapper:
        def __init__(self, capsule: object) -> None:
            self._capsule = capsule

        def __arrow_c_schema__(self) -> object:
            return self._capsule

    tight = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts", tightenNulls=True)
    exported = pa.schema(_CapsuleWrapper(tight._inner.analyzed_arrow_schema()))
    for name in ("sym", "ts"):
        metadata = exported.field(name).metadata or {}
        assert b"repark.tighten_nulls" not in metadata, (
            f"{name} leaks the internal tag through the analyzed-schema export: {metadata}"
        )
        # The lever itself must survive the strip.
        assert exported.field(name).nullable is False
    assert b"repark.tighten_nulls" not in (exported.metadata or {})


def test_tighten_refuses_nulls_in_a_declared_key(spark: ReparkSession) -> None:
    rows = [("AAA", 1, 1.0), ("AAA", None, 2.0)]
    frame = spark.createDataFrame(rows, SCHEMA)
    with pytest.raises(AnalysisException, match="tightenNulls") as excinfo:
        frame.declareSorted("sym", "ts", tightenNulls=True)
    assert "'ts'" in str(excinfo.value) or "ts" in str(excinfo.value)
    # View still answers and is still nullable.
    assert frame.count() == 2
    assert frame.to_arrow().schema.field("ts").nullable is True


def test_hint_after_tighten_restores_nullability(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    frame.declareSorted("sym", "ts", tightenNulls=True)
    assert frame.to_arrow().schema.field("ts").nullable is False
    assert frame.schema["ts"].nullable is False
    frame.declareSorted("sym", "ts")
    assert frame.to_arrow().schema.field("ts").nullable is True
    assert frame.schema["ts"].nullable is True
    frame.declareSorted("sym", "ts", tightenNulls=True)
    assert frame.to_arrow().schema.field("ts").nullable is False
    assert frame.schema["ts"].nullable is False


def test_tighten_nulls_keyword_is_shared_by_both_spellings() -> None:
    assert DataFrame.declareSorted is DataFrame.declare_sorted


@pytest.fixture
def spark_catalog(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-declare-sorted-tighten-write").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql("CREATE NAMESPACE glue_catalog.writer_ns")
    return session


def test_save_as_table_create_refuses_tightened_and_derived(
    spark_catalog: ReparkSession,
) -> None:
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    with pytest.raises(AnalysisException, match="tightenNulls"):
        tight.write.saveAsTable("glue_catalog.writer_ns.tight_src")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        derived.write.saveAsTable("glue_catalog.writer_ns.tight_derived")


def test_write_to_create_refuses_tightened_and_derived(spark_catalog: ReparkSession) -> None:
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    with pytest.raises(AnalysisException, match="tightenNulls"):
        tight.writeTo("glue_catalog.writer_ns.wt_src").create()
    with pytest.raises(AnalysisException, match="tightenNulls"):
        derived.writeTo("glue_catalog.writer_ns.wt_derived").create()


def test_facade_layer_refuses_when_engine_source_walk_is_silent(
    spark_catalog: ReparkSession,
) -> None:
    """Kills: deleting ``_refuse_tightened_iceberg_create`` from saveAsTable / writeTo.

    Engine plan-source walk is silent here (no tagged scan). Only the facade marker
    refuses. A delete-the-facade-layer mutant lets CREATE succeed.
    """
    loose = spark_catalog.createDataFrame([(1,)], "x INT")
    marked = loose.select(F.lit(1).alias("one"))
    marked._tighten_derived = True
    assert marked.schema["one"].nullable is False
    with pytest.raises(AnalysisException, match="tightenNulls"):
        marked.write.saveAsTable("glue_catalog.writer_ns.facade_only")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        marked.writeTo("glue_catalog.writer_ns.facade_only_wt").create()
    with pytest.raises(AnalysisException, match="tightenNulls"):
        marked.writeTo("glue_catalog.writer_ns.facade_only_cor").createOrReplace()


def test_right_side_combinators_propagate_tighten_marker(spark: ReparkSession) -> None:
    """Kills: ``_spawn`` copying ``_tighten_derived`` from self only (R-C)."""
    loose = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    tight = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts", tightenNulls=True)
    assert loose.union(tight)._tighten_derived is True
    assert loose.unionByName(tight)._tighten_derived is True
    assert loose.intersect(tight)._tighten_derived is True
    assert loose.subtract(tight)._tighten_derived is True
    assert loose.crossJoin(tight.limit(1))._tighten_derived is True
    assert loose.join(tight, "sym")._tighten_derived is True

    def _identity(batches: object) -> object:
        yield from batches  # type: ignore[misc]

    mapped = tight.mapInArrow(_identity, SCHEMA)
    assert mapped._tighten_derived is True
    joined = loose.pl.join(tight.pl, on="sym").spark
    assert joined._tighten_derived is True


def test_cache_of_derived_still_refuses_iceberg_create(spark_catalog: ReparkSession) -> None:
    """Kills: cache/persist remint dropping tighten provenance (R-A) — SQL half only.

    Y-7 (round 4) ran the previously NOT-RUN verifier P-3 on the ledger's "cache saveAsTable"
    cell. MEASURED, no-oping the R-A stamp (``apply_tighten_provenance_on_materialize``):

    ===========================================  ======================================
    assertion                                    under the R-A mutant
    ===========================================  ======================================
    ``cached.write.saveAsTable(…)``              still refuses — the FACADE
                                                 ``_tighten_derived`` marker survives cache
    ``sql("CREATE TABLE … AS SELECT * FROM
    cached_derived")``                           **DID NOT RAISE** — this is the R-A
                                                 discriminator
    ===========================================  ======================================

    So the cell is genuinely green, but the ``saveAsTable`` statement is guarded by the facade
    layer, not by the engine remint. The engine half is pinned here by the SQL statement and by
    the Rust twins ``iceberg_create_from_cached_derived_frame_refuses`` (Spark door) /
    ``ansi_ctas_from_cached_derived_frame_refuses`` (ANSI door), both of which the same mutant
    turns red.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    cached = derived.cache()
    cached.count()
    with pytest.raises(AnalysisException, match="tightenNulls"):
        cached.write.saveAsTable("glue_catalog.writer_ns.cached_derived")
    cached.createOrReplaceTempView("cached_derived")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        spark_catalog.sql(
            "CREATE TABLE glue_catalog.writer_ns.cached_sql AS SELECT * FROM cached_derived"
        )


def test_all_nullable_projection_create_and_insert_are_allowed(
    spark_catalog: ReparkSession,
) -> None:
    """Kills: hoisting refuse onto all-nullable CREATE or onto INSERT/append (R-D)."""
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    only_val = tight.select("val")
    only_val.write.saveAsTable("glue_catalog.writer_ns.nullable_only")
    only_val.write.mode("append").saveAsTable("glue_catalog.writer_ns.nullable_only")


def test_literal_over_tightened_source_is_refused(spark_catalog: ReparkSession) -> None:
    """Belt-and-suspenders guard on the R-D conservative class — NOT a discriminator.

    Kills: nothing on its own. ~~Kills: dropping the facade R-D half.~~ **Struck, round 4
    (Y-1), MEASURED both directions** on this tree:

    ==================================================  ==============================
    mutant                                              this node
    ==================================================  ==============================
    facade ``_refuse_tightened_iceberg_create`` no-oped  **green** (engine walk refuses)
    engine ``refuse_iceberg_create_of_tightened_plan``   **green** (facade refuses)
      no-oped
    ==================================================  ==============================

    Two independent layers see the same statement — the write plan scans the tightened
    ``MemTable`` (engine source walk) *and* the frame carries ``_tighten_derived`` with a
    non-nullable ``lit(1)`` output (facade marker) — so no single-layer mutant can turn it
    red. A genuinely facade-only discriminator needs a write plan the engine walk cannot
    see, i.e. a frame with the marker but **no** tagged scan; that shape exists and is
    already pinned by ``test_facade_layer_refuses_when_engine_source_walk_is_silent``
    (the only node the facade no-op kills). This node keeps its value as an end-to-end
    guard that the R-D conservative class (a literal over a tightened source) stays
    refused through the real ``saveAsTable`` door.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    with pytest.raises(AnalysisException, match="tightenNulls"):
        tight.select(F.lit(1).alias("one")).write.saveAsTable(
            "glue_catalog.writer_ns.lit_over_tight"
        )


def test_sql_derived_write_and_lazy_view_create_refuse(
    spark_catalog: ReparkSession,
) -> None:
    """Kills: engine walk that does not enter into_view / lazy temp-view plans (Q-001)."""
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    tight.createOrReplaceTempView("tight_src")
    derived_sql = spark_catalog.sql("SELECT ts + 1 AS ts2 FROM tight_src")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        derived_sql.write.saveAsTable("glue_catalog.writer_ns.sql_derived_write")
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    derived.createOrReplaceTempView("d")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        spark_catalog.sql("CREATE TABLE glue_catalog.writer_ns.view_hop AS SELECT * FROM d")


def test_create_view_into_catalog_over_tightened_source_refuses(
    spark_catalog: ReparkSession,
) -> None:
    """Y-3 (round 4). Kills: the Spark router's ``_ => execute_passthrough`` catch-all.

    MEASURED on BASE (fe742a6): ``CREATE VIEW <catalog>.<ns>.v AS SELECT * FROM tight LIMIT 0``
    returned a frame and the fork's ``register_table`` sink persisted a format-v2 Iceberg TABLE
    whose ``sym``/``ts`` were ``required``. The refuse now lives on the planned DDL body
    (``refuse_iceberg_create_of_tightened_ddl``); deleting that call turns this red.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    tight.createOrReplaceTempView("view_sink_src")
    for statement in (
        "CREATE VIEW glue_catalog.writer_ns.v_limit AS SELECT * FROM view_sink_src LIMIT 0",
        "CREATE VIEW glue_catalog.writer_ns.v_false AS SELECT * FROM view_sink_src WHERE false",
    ):
        with pytest.raises(AnalysisException, match="tightenNulls"):
            spark_catalog.sql(statement)


def test_select_into_catalog_over_tightened_source_refuses(
    spark_catalog: ReparkSession,
) -> None:
    """Y-4 (round 4). Independent statement: ``SELECT … INTO`` plans as ``CreateMemoryTable``.

    A fix wired only to the ``CreateView`` DDL arm leaves this green — measured both ways in
    ``crates/repark-spark/tests/declared_sorted_tighten.rs``.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    tight.createOrReplaceTempView("into_sink_src")
    for statement in (
        "SELECT * INTO glue_catalog.writer_ns.t_limit FROM into_sink_src LIMIT 0",
        "SELECT * INTO glue_catalog.writer_ns.t_false FROM into_sink_src WHERE false",
    ):
        with pytest.raises(AnalysisException, match="tightenNulls"):
            spark_catalog.sql(statement)


def test_session_scoped_create_view_and_select_into_stay_allowed(
    spark_catalog: ReparkSession,
) -> None:
    """Y-3/Y-4 allowed side. Kills: a blanket DDL refuse.

    A one-part name is not a registered Iceberg catalog and persists nothing, so it must keep
    working — ``test_sql_derived_write_and_lazy_view_create_refuse`` depends on exactly that.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    tight.createOrReplaceTempView("allowed_src")
    spark_catalog.sql("CREATE VIEW session_v AS SELECT * FROM allowed_src").collect()
    spark_catalog.sql("SELECT * INTO session_t FROM allowed_src").collect()
    assert spark_catalog.sql("SELECT count(*) AS n FROM session_v").collect()[0]["n"] == len(
        SORTED_ROWS
    )


def test_facade_rd_walks_array_and_map_element_nullability() -> None:
    """Kills: facade R-D looking only at StructType.fields (C1-Q-003)."""
    from repark.spark.dataframe.plan_collapse import _output_field_would_persist_required
    from repark.spark.types import IntegerType, MapType

    required_element = StructField("arr", ArrayType(LongType(), False), True)
    optional_element = StructField("arr", ArrayType(LongType(), True), True)
    required_value = StructField("m", MapType(StringType(), IntegerType(), False), True)
    optional_value = StructField("m", MapType(StringType(), IntegerType(), True), True)
    assert _output_field_would_persist_required(required_element)
    assert not _output_field_would_persist_required(optional_element)
    assert _output_field_would_persist_required(required_value)
    assert not _output_field_would_persist_required(optional_value)


def test_declare_sorted_docstring_examples_execute() -> None:
    """Kills: fabricated ``>>>`` examples that raise when run as doctest."""
    import doctest

    from repark.spark.dataframe.core import DataFrame as FacadeDataFrame

    finder = doctest.DocTestFinder()
    runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    failed = 0
    for test in finder.find(FacadeDataFrame.declare_sorted, name="declare_sorted"):
        failed += runner.run(test).failed
    assert failed == 0


# ===========================================================================================
# SQM round 6 (R6-1) — the temp-view API is not a catalog-write door.
# ===========================================================================================


def test_qualified_temp_view_name_refuses_and_persists_nothing(
    spark_catalog: ReparkSession,
) -> None:
    """R6-1 facade half. Kills: ``createOrReplaceTempView`` forwarding a qualified name to the
    engine's ``register_table``, which resolved it into the Iceberg catalog provider and
    PERSISTED a real table — carrying the ``tightenNulls`` ``required: True`` payload the DDL
    doors refuse.

    MEASURED on BASE (``68e98f4``) through the same fixture: a 3-part name with a LAZY/empty body
    returned success and ``tableExists`` was **True**; with a non-empty body the Iceberg provider
    itself errored ("register_table does not support tables with data"). Both are now one loud
    ``AnalysisException`` and nothing reaches the catalog.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    for name in ("glue_catalog.writer_ns.tv3", "writer_ns.tv2"):
        with pytest.raises(AnalysisException, match="SESSION-LOCAL"):
            tight.createOrReplaceTempView(name)
    assert not spark_catalog.catalog.tableExists("glue_catalog.writer_ns.tv3")
    assert not spark_catalog.catalog.tableExists("glue_catalog.writer_ns.tv2")


def test_one_part_temp_view_stays_session_local_under_set_default_catalog(
    spark_catalog: ReparkSession,
) -> None:
    """R6-1 facade half, the ``SET`` leg. Kills: resolving a one-part temp-view name against the
    LIVE ``datafusion.catalog.default_catalog``.

    MEASURED on BASE: after ``SET datafusion.catalog.default_catalog = 'glue_catalog'`` the
    registration left the session and hit the Iceberg schema provider. Now the view registers in
    the session's pinned home, ``tableExists`` on the catalog name is FALSE, and the one-part
    name still answers True.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    spark_catalog.sql("SET datafusion.catalog.default_catalog = 'glue_catalog'")
    spark_catalog.sql("SET datafusion.catalog.default_schema = 'writer_ns'")
    tight.createOrReplaceTempView("tv_bare")
    assert not spark_catalog.catalog.tableExists("glue_catalog.writer_ns.tv_bare")
    assert spark_catalog.catalog.tableExists("tv_bare")


def test_one_part_temp_view_still_works_and_reads_back(spark: ReparkSession) -> None:
    """R6-1 allowed side. Kills: the refusal turning into a blanket temp-view refusal — the
    ordinary one-part ``createOrReplaceTempView`` must be untouched, value for value."""
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    frame.createOrReplaceTempView("plain_tv")
    assert spark.sql("SELECT count(*) AS n FROM plain_tv").collect()[0]["n"] == len(SORTED_ROWS)
    assert spark.catalog.tableExists("plain_tv")


def test_a_catalog_over_the_build_time_default_is_not_a_temp_view_home(tmp_path: Path) -> None:
    """R6-1 second pass (round-6 critic S1), facade half. Kills: pinning the temp-view home to
    the CONFIGURED default-catalog NAME.

    ``datafusion.catalog.default_catalog`` is a supported BUILD-time conf, so a session built
    with it pointing at the name a catalog is registered under had its home taken over by that
    catalog. MEASURED on the name-only fix, this exact session shape:
    ``createDataFrame([], schema)`` = Ok, ``df.createOrReplaceTempView("v_leak")`` = Ok,
    ``spark.catalog.tableExists("ice.sales.v_leak")`` = **True** — the ``required: True``
    tighten payload PERSISTED through the temp-view API.

    MEASURED now: the session-local home is gone, so the very first temp-view mint
    (``createDataFrame`` itself) refuses ``AnalysisException`` and the catalog stays empty.
    """
    session = (
        ReparkSession.builder.appName("pytest-temp-view-home")
        .config("datafusion.catalog.default_catalog", "ice")
        .config("datafusion.catalog.default_schema", "sales")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", tmp_path)
    session.sql("CREATE NAMESPACE IF NOT EXISTS ice.sales")
    with pytest.raises(AnalysisException, match="SESSION-LOCAL"):
        session.createDataFrame(SORTED_ROWS, SCHEMA).createOrReplaceTempView("v_leak")
    assert not session.catalog.tableExists("ice.sales.v_leak")


def test_named_read_paths_find_a_temp_view_under_set_default_catalog(
    spark_catalog: ReparkSession,
) -> None:
    """R7-1. Kills: product read paths emitting a BARE reference for a session-local view.

    R6-1 pinned the temp-view WRITE to the session's build-time home; the READ side still emitted
    the caller's bare name, which DataFusion re-resolves against the **live**
    ``datafusion.catalog.default_catalog``. MEASURED on the round-7 BASE (``3910ac7``), this exact
    shape — ``SET`` to a second catalog, then mint:

    * ``spark.catalog.tableExists("tv")`` = **True** (it asks the home) —
    * ``spark.table("tv")`` = ``AnalysisException: table 'glue_catalog.writer_ns.tv' not found``
    * ``cache()`` / ``persist()`` = the same miss on ``__repark_cache_*``
    * ``createDataFrame(...)`` = the same miss on ``__repark_cdf_*``
    * ``selectExpr`` / ``alias`` = the same miss on ``__repark_selx_*`` / the alias name

    So ``tableExists`` and every named read path DISAGREED. Now the facade spells a session-local
    view against its home (``python/repark/src/repark/spark/_temp_views.py``) and they agree.

    SCOPE, so this pin is not read as more than it proves: it covers the paths asserted below —
    the FACADE's own view spellings. It does NOT cover the scratch relations the ENGINE crates
    register for themselves under a bare name (`repark-iceberg`'s MERGE / identity-DML tables,
    the `__repark_tt_*` time-travel view). Those are still RED under this same ``SET`` — MEASURED
    equally red on the round-7 BASE, so round 7 neither caused nor cured them; they need the home
    plumbed into those crates and are recorded as a round-8 item in
    ``task/se1-declared-sorted-ledger.md``.
    """
    spark_catalog.sql("SET datafusion.catalog.default_catalog = 'glue_catalog'")
    spark_catalog.sql("SET datafusion.catalog.default_schema = 'writer_ns'")
    frame = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA)
    frame.createOrReplaceTempView("tv_read")

    assert spark_catalog.catalog.tableExists("tv_read")
    assert spark_catalog.table("tv_read").count() == len(SORTED_ROWS)
    assert spark_catalog.sql("SELECT count(*) AS n FROM tv_read").collect()[0]["n"] == len(
        SORTED_ROWS
    )
    # cache / persist / checkpoint all re-register a MemTable and re-scan it by name.
    cached = spark_catalog.table("tv_read").cache()
    assert cached.count() == len(SORTED_ROWS)
    persisted = spark_catalog.table("tv_read").persist()
    assert persisted.count() == len(SORTED_ROWS)
    checkpointed = spark_catalog.table("tv_read").localCheckpoint(eager=True)
    assert checkpointed.count() == len(SORTED_ROWS)
    # createDataFrame materializes through its own internal scratch view, then re-scans it.
    assert spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).count() == len(SORTED_ROWS)
    # DataFrame ops that re-reference the source through a scratch view.
    assert spark_catalog.table("tv_read").selectExpr("ts + 1 AS ts1").count() == len(SORTED_ROWS)
    assert spark_catalog.table("tv_read").alias("tv_alias").count() == len(SORTED_ROWS)
    # The value, not just the row count: the rows must be the VIEW's rows.
    assert spark_catalog.table("tv_read").selectExpr("min(ts) AS lo").collect()[0]["lo"] == 0


def test_no_set_leaves_every_named_read_path_byte_identical(spark: ReparkSession) -> None:
    """R7-1 no-SET leg. Kills: the home spelling changing an ORDINARY session's results.

    Without a ``SET`` the home IS the live default, so the fix must be invisible: same rows, same
    column names, same ``listTables``/``tableExists`` answers (which stay ONE-part — the home
    spelling is a SQL reference, never a rename).
    """
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    frame.createOrReplaceTempView("plain_read")
    assert spark.table("plain_read").columns == ["sym", "ts", "val"]
    assert spark.table("plain_read").count() == len(SORTED_ROWS)
    assert spark.table("plain_read").selectExpr("ts + 1 AS ts1").columns == ["ts1"]
    assert spark.catalog.tableExists("plain_read")
    assert "plain_read" in spark.list_temp_view_names()
    assert [name for name in spark.list_temp_view_names() if "." in name] == []
    cached = spark.table("plain_read").cache()
    assert cached.count() == len(SORTED_ROWS)
    assert spark.table("plain_read").alias("plain_alias").columns == ["sym", "ts", "val"]


def test_a_catalog_over_the_home_refuses_the_read_spelling_too(tmp_path: Path) -> None:
    """R7-1 plus R6-1 S1. Kills: the read seam becoming a way AROUND ``assert_home_intact``.

    A session built with ``default_catalog = ice`` and a catalog registered under that same name
    has no session-local home. Asking for the home spelling must refuse exactly like the write
    side, not hand back ``ice.sales.<view>`` — that would be a catalog read dressed as a temp
    view. MEASURED: the mint refuses first, and the native home lookup refuses on its own.
    """
    session = (
        ReparkSession.builder.appName("pytest-temp-view-home-read")
        .config("datafusion.catalog.default_catalog", "ice")
        .config("datafusion.catalog.default_schema", "sales")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", tmp_path)
    session.sql("CREATE NAMESPACE IF NOT EXISTS ice.sales")
    native = session._ensure_alive()
    with pytest.raises(Exception, match="no session-local temp-view home"):
        native.temp_view_home()
    with pytest.raises(Exception, match="no session-local temp-view home"):
        native.resolve_temp_view_home_ref("v")
    with pytest.raises(AnalysisException, match="SESSION-LOCAL"):
        session.createDataFrame(SORTED_ROWS, SCHEMA).createOrReplaceTempView("v_read_leak")
    assert not session.catalog.tableExists("ice.sales.v_read_leak")
