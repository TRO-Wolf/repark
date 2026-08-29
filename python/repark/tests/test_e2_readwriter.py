"""Readwriter + session residuals: bare-name resolution, parquet path forms, ndarray lit."""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.session import (
    _default_namespace_from_builder_config,
    _parse_table_identifier_segments,
    _reset_active_session_for_tests,
    _sql_table_ref,
    resolve_table_name,
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Session with in-memory Iceberg catalog + default NS for bare-name tests."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-e2-readwriter").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.create_namespace("glue_catalog", "default")
    yield session
    session.stop()
    _reset_active_session_for_tests()


# Bare / default-namespace resolution (shared layer)
def test_resolve_table_name_one_two_three_part() -> None:
    known = {"glue_catalog"}
    assert (
        resolve_table_name(
            "t",
            current_catalog="glue_catalog",
            current_database="default",
            known_catalogs=known,
        )
        == "glue_catalog.default.t"
    )
    assert (
        resolve_table_name(
            "ns.t",
            current_catalog="glue_catalog",
            current_database="default",
            known_catalogs=known,
        )
        == "glue_catalog.ns.t"
    )
    assert (
        resolve_table_name(
            "glue_catalog.ns.t",
            current_catalog="glue_catalog",
            current_database="default",
            known_catalogs=known,
        )
        == "glue_catalog.ns.t"
    )


def test_resolve_spark_catalog_alias() -> None:
    known = {"glue_catalog"}
    assert (
        resolve_table_name(
            "spark_catalog.ns.t",
            current_catalog="glue_catalog",
            current_database="default",
            known_catalogs=known,
        )
        == "glue_catalog.ns.t"
    )


def test_resolve_prefer_temp_view() -> None:
    """A one-part name that IS a temp view resolves to that view's session-local HOME.

    A bare reference is re-resolved by the engine against the live
    ``datafusion.catalog.default_catalog``, so the probe must answer the home segments (or
    ``None``).
    """
    known = {"glue_catalog"}
    assert (
        resolve_table_name(
            "v",
            current_catalog="glue_catalog",
            current_database="default",
            known_catalogs=known,
            prefer_temp_view=True,
            temp_view_home_ref=lambda name: ["datafusion", "public", name] if name == "v" else None,
        )
        == "datafusion.public.v"
    )
    # No temp view of that name → ordinary catalog qualification, unchanged.
    assert (
        resolve_table_name(
            "v",
            current_catalog="glue_catalog",
            current_database="default",
            known_catalogs=known,
            prefer_temp_view=True,
            temp_view_home_ref=lambda name: None,
        )
        == "glue_catalog.default.v"
    )


def test_resolve_quoted_dotted_segments_survive_rejoin() -> None:
    """Dotted quoted segments must not re-split after resolve → _sql_table_ref."""
    known = {"glue_catalog"}
    three_part = 'glue_catalog."analytics.v2".events'
    resolved_three = resolve_table_name(
        three_part,
        current_catalog="glue_catalog",
        current_database="default",
        known_catalogs=known,
    )
    assert _parse_table_identifier_segments(resolved_three) == [
        "glue_catalog",
        "analytics.v2",
        "events",
    ]
    assert _parse_table_identifier_segments(_sql_table_ref(resolved_three)) == [
        "glue_catalog",
        "analytics.v2",
        "events",
    ]

    bare_dotted = '"backup.v2"'
    resolved_bare = resolve_table_name(
        bare_dotted,
        current_catalog="glue_catalog",
        current_database="default",
        known_catalogs=known,
    )
    assert _parse_table_identifier_segments(resolved_bare) == [
        "glue_catalog",
        "default",
        "backup.v2",
    ]
    assert _parse_table_identifier_segments(_sql_table_ref(resolved_bare)) == [
        "glue_catalog",
        "default",
        "backup.v2",
    ]


def test_default_namespace_builder_key() -> None:
    assert (
        _default_namespace_from_builder_config({"spark.sql.defaultNamespace": "analytics"})
        == "analytics"
    )
    assert _default_namespace_from_builder_config({}) is None


def test_bare_save_as_table_and_table_read(spark: ReparkSession) -> None:
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.saveAsTable("bare_t")
    assert spark.catalog.tableExists("bare_t")
    assert spark.catalog.tableExists("glue_catalog.default.bare_t")
    rows = sorted(spark.table("bare_t").to_arrow().to_pylist(), key=lambda row: row["id"])
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_table_prefers_temp_view_over_catalog_table(spark: ReparkSession) -> None:
    """session.table() wiring prefers temp view, not only the injectable unit probe."""
    spark.createDataFrame([(1,)], ["id"]).write.saveAsTable("shadow_tv")
    spark.createDataFrame([(99,)], ["id"]).createOrReplaceTempView("shadow_tv")
    rows = spark.table("shadow_tv").to_arrow().to_pylist()
    assert rows == [{"id": 99}]
    # Catalog table still exists under the qualified name; bare read must not use it.
    assert spark.catalog.tableExists("glue_catalog.default.shadow_tv")
    catalog_rows = spark.table("glue_catalog.default.shadow_tv").to_arrow().to_pylist()
    assert catalog_rows == [{"id": 1}]


def test_format_iceberg_load_does_not_prefer_temp_view(spark: ReparkSession) -> None:
    """format('iceberg').load(bare) is catalog-table only, not temp-view shadow.

    Counterexample held by this pin: saveAsTable id=1 + temp view id=99 → load must yield 1;
    session.table() still prefers the temp.
    """
    spark.createDataFrame([(1,)], ["id"]).write.saveAsTable("shadow_iceberg_load")
    spark.createDataFrame([(99,)], ["id"]).createOrReplaceTempView("shadow_iceberg_load")
    loaded = spark.read.format("iceberg").load("shadow_iceberg_load")
    assert loaded.to_arrow().to_pylist() == [{"id": 1}]
    # Control: bare spark.table still prefers the temp view.
    assert spark.table("shadow_iceberg_load").to_arrow().to_pylist() == [{"id": 99}]


def test_list_tables_spark_catalog_alias(spark: ReparkSession) -> None:
    """listTables two-part catalog.db aliases spark_catalog like tableExists."""
    spark.createDataFrame([(1,)], ["id"]).write.saveAsTable("listed_t")
    names_real = {table.name for table in spark.catalog.listTables("glue_catalog.default")}
    names_alias = {table.name for table in spark.catalog.listTables("spark_catalog.default")}
    assert "listed_t" in names_real
    assert "listed_t" in names_alias
    # Alias must not raise SCHEMA_NOT_FOUND while the real catalog.db lists.
    assert names_alias == names_real or "listed_t" in names_alias


def test_read_table_time_travel_resolves_bare_name(spark: ReparkSession) -> None:
    """DataFrameReader.table + snapshot-id qualifies bare names."""
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable("tt_bare")
    snaps = spark._testing_list_snapshots("glue_catalog.default.tt_bare")
    assert snaps, "expected at least one snapshot after saveAsTable"
    snapshot_id = int(snaps[-1][0])
    # Bare name with time-travel options must resolve under current catalog/NS.
    frame = spark.read.option("snapshot-id", str(snapshot_id)).table("tt_bare")
    rows = frame.to_arrow().to_pylist()
    assert rows == [{"id": 1, "name": "a"}]
    # spark_catalog alias path through the same read_iceberg_table resolve layer.
    frame_alias = spark.read.option("snapshot-id", str(snapshot_id)).table(
        "spark_catalog.default.tt_bare"
    )
    assert frame_alias.to_arrow().to_pylist() == [{"id": 1, "name": "a"}]


def test_bare_write_to_create(spark: ReparkSession) -> None:
    spark.createDataFrame([(9, "z")], ["id", "name"]).writeTo("bare_v2").create()
    assert spark.table("bare_v2").count() == 1


def test_bare_insert_into(spark: ReparkSession) -> None:
    """bare insertInto resolves under currentCatalog.currentDatabase end-to-end."""
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable("ins_t")
    spark.createDataFrame([(2, "b")], ["id", "name"]).write.insertInto("ins_t")
    rows = sorted(spark.table("ins_t").to_arrow().to_pylist(), key=lambda row: row["id"])
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert spark.catalog.tableExists("glue_catalog.default.ins_t")


def test_bare_merge_into(spark: ReparkSession) -> None:
    """bare mergeInto target resolves under current catalog/NS end-to-end."""
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable("merge_t")
    source = spark.createDataFrame([(1, "A"), (2, "b")], ["id", "name"])
    (
        source.mergeInto("merge_t", "id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    rows = sorted(spark.table("merge_t").to_arrow().to_pylist(), key=lambda row: row["id"])
    assert rows == [{"id": 1, "name": "A"}, {"id": 2, "name": "b"}]


def test_spark_catalog_alias_table_exists(spark: ReparkSession) -> None:
    """tableExists aliases spark_catalog like table()/writers."""
    spark.createDataFrame([(1,)], ["id"]).write.saveAsTable("alias_t")
    assert spark.catalog.tableExists("glue_catalog.default.alias_t")
    assert spark.catalog.tableExists("spark_catalog.default.alias_t") is True
    assert spark.table("spark_catalog.default.alias_t").count() == 1
    # databaseExists two-part form must alias too (not soft-False while table loads).
    assert spark.catalog.databaseExists("spark_catalog.default") is True
    assert spark.catalog.databaseExists("glue_catalog.default") is True


def test_spark_catalog_alias_writer_paths(spark: ReparkSession) -> None:
    """saveAsTable / writeTo / insertInto / MERGE honor spark_catalog.* end-to-end.

    Bare-name pins alone stay green if ``_resolve_writer_table`` drops ``known_catalogs``
    (empty set → ``spark_catalog`` no longer aliases), so these four writer entry points must
    land under the real catalog and read back via both spellings.
    """
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable(
        "spark_catalog.default.alias_sat"
    )
    assert spark.catalog.tableExists("glue_catalog.default.alias_sat")
    assert spark.table("glue_catalog.default.alias_sat").to_arrow().to_pylist() == [
        {"id": 1, "name": "a"}
    ]
    assert spark.table("spark_catalog.default.alias_sat").count() == 1

    spark.createDataFrame([(9,)], ["id"]).writeTo("spark_catalog.default.alias_wt").create()
    assert spark.catalog.tableExists("glue_catalog.default.alias_wt")
    assert spark.table("glue_catalog.default.alias_wt").to_arrow().to_pylist() == [{"id": 9}]

    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable("alias_ins")
    spark.createDataFrame([(2, "b")], ["id", "name"]).write.insertInto(
        "spark_catalog.default.alias_ins"
    )
    insert_rows = sorted(
        spark.table("glue_catalog.default.alias_ins").to_arrow().to_pylist(),
        key=lambda row: row["id"],
    )
    assert insert_rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable("alias_merge")
    source = spark.createDataFrame([(1, "A"), (2, "b")], ["id", "name"])
    (
        source.mergeInto("spark_catalog.default.alias_merge", "id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    merge_rows = sorted(
        spark.table("glue_catalog.default.alias_merge").to_arrow().to_pylist(),
        key=lambda row: row["id"],
    )
    assert merge_rows == [{"id": 1, "name": "A"}, {"id": 2, "name": "b"}]


def test_write_to_re_resolves_after_set_current_database(spark: ReparkSession) -> None:
    """writeTo action uses NS at create(), not frozen at construction."""
    spark.create_namespace("glue_catalog", "analytics")
    writer = spark.createDataFrame([(1,)], ["id"]).writeTo("late_ns_t")
    spark.catalog.setCurrentDatabase("analytics")
    writer.create()
    assert spark.catalog.tableExists("glue_catalog.analytics.late_ns_t")
    assert not spark.catalog.tableExists("glue_catalog.default.late_ns_t")
    # Restore default for other tests that share the fixture session.
    spark.catalog.setCurrentDatabase("default")


def test_merge_re_resolves_after_set_current_database(spark: ReparkSession) -> None:
    """mergeInto action qualifies under NS at merge(), not at construction."""
    spark.create_namespace("glue_catalog", "analytics")
    spark.catalog.setCurrentDatabase("analytics")
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.saveAsTable("merge_late")
    spark.catalog.setCurrentDatabase("default")
    source = spark.createDataFrame([(1, "Z")], ["id", "name"])
    writer = source.mergeInto("merge_late", "id").whenMatched().updateAll()
    spark.catalog.setCurrentDatabase("analytics")
    writer.merge()
    rows = spark.table("glue_catalog.analytics.merge_late").to_arrow().to_pylist()
    assert rows == [{"id": 1, "name": "Z"}]
    spark.catalog.setCurrentDatabase("default")


def test_bare_drop_table_sql_entry_point(spark: ReparkSession) -> None:
    spark.createDataFrame([(1,)], ["id"]).write.saveAsTable("to_drop")
    assert spark.catalog.tableExists("to_drop")
    # sqlutils.table() cleanup path — load-bearing free SQL.
    drop_sql = "DROP" + " " + "TABLE IF EXISTS to_drop"
    spark.sql(drop_sql)
    assert not spark.catalog.tableExists("to_drop")


def test_drop_expander_does_not_rewrite_non_drop_sql(spark: ReparkSession) -> None:
    """DROP expander is whole-statement-only + exact rewrite (no injection).

    Substring-only positive pins stay green if the expander injects extra targets; exact
    equality is the mutation-proof bar.
    """
    bare_drop = "DROP TABLE IF EXISTS bare_x"
    expanded_drop = spark._expand_bare_table_names_in_sql(bare_drop)
    assert expanded_drop == 'DROP TABLE IF EXISTS "glue_catalog"."default"."bare_x"'

    multi_drop = "DROP TABLE IF EXISTS bare_a, bare_b"
    expanded_multi = spark._expand_bare_table_names_in_sql(multi_drop)
    assert expanded_multi == (
        'DROP TABLE IF EXISTS "glue_catalog"."default"."bare_a", "glue_catalog"."default"."bare_b"'
    )

    plain_drop = "DROP TABLE bare_y"
    assert spark._expand_bare_table_names_in_sql(plain_drop) == (
        'DROP TABLE "glue_catalog"."default"."bare_y"'
    )

    select_sql = "SELECT * FROM bare_x"
    assert spark._expand_bare_table_names_in_sql(select_sql) == (
        'SELECT * FROM "glue_catalog"."default"."bare_x"'
    )

    insert_sql = "INSERT INTO bare_x VALUES (1)"
    assert spark._expand_bare_table_names_in_sql(insert_sql) == (
        'INSERT INTO "glue_catalog"."default"."bare_x" VALUES (1)'
    )

    drop_view_sql = "DROP VIEW IF EXISTS bare_x"
    assert spark._expand_bare_table_names_in_sql(drop_view_sql) == drop_view_sql

    # Multi-statement scripts still whole-statement only (no mid-script inject).
    script = "SELECT 1; DROP TABLE IF EXISTS bare_x"
    assert spark._expand_bare_table_names_in_sql(script) == script


def test_default_namespace_seeds_current_database(tmp_path: Path) -> None:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("e2-default-ns")
        .config("spark.sql.defaultNamespace", "analytics")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("glue_catalog", tmp_path)
        assert session.catalog.currentDatabase() == "analytics"
        # Memory catalog does NOT auto-create current NS (honest residual — callers create).
        session.create_namespace("glue_catalog", "analytics")
        session.createDataFrame([(1,)], ["id"]).write.saveAsTable("t_ns")
        assert session.catalog.tableExists("glue_catalog.analytics.t_ns")
    finally:
        session.stop()
        _reset_active_session_for_tests()


# save(path) / load(path) parquet + loud unsupported
def test_parquet_save_load_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "part_out"
    frame = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    frame.write.mode("overwrite").format("parquet").save(str(path))
    loaded = spark.read.format("parquet").load(str(path))
    rows = sorted(loaded.to_arrow().to_pylist(), key=lambda row: row["id"])
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_save_unsupported_format_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Path save of an unsupported format must be DATA_SOURCE_NOT_FOUND-shaped.

    The match requires the Spark error-class token — a format-name-only AnalysisException must
    not keep this pin green (the load pin is the same strict shape).
    """
    path = tmp_path / "orc_out"
    with pytest.raises(AnalysisException, match="DATA_SOURCE_NOT_FOUND") as raised:
        spark.createDataFrame([(1,)], ["id"]).write.format("orc").save(str(path))
    # Format name still surfaces in the message (mutation-proofs the shown source).
    assert "orc" in str(raised.value).lower()


def test_write_csv_json_round_trip_e2(spark: ReparkSession, tmp_path: Path) -> None:
    """Writer.csv/json round-trip paths are wired."""
    frame = spark.createDataFrame([(1, "a")], ["id", "name"])
    csv_path = tmp_path / "c"
    json_path = tmp_path / "j"
    frame.write.mode("overwrite").csv(str(csv_path), header=True)
    frame.write.mode("overwrite").json(str(json_path))
    csv_rows = spark.read.csv(str(csv_path), header=True, inferSchema=True).to_arrow().to_pylist()
    json_rows = spark.read.json(str(json_path)).to_arrow().to_pylist()
    assert csv_rows == [{"id": 1, "name": "a"}]
    assert json_rows == [{"id": 1, "name": "a"}]


def test_load_unsupported_format_loud(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match="DATA_SOURCE_NOT_FOUND"):
        spark.read.format("orc").load("/tmp/does-not-matter")


# ndarray lit
@pytest.mark.parametrize(
    ("dtype_name", "expected_simple"),
    [
        ("int8", "array<tinyint>"),
        ("int16", "array<smallint>"),
        ("int32", "array<int>"),
        ("int64", "array<bigint>"),
        ("float32", "array<float>"),
        ("float64", "array<double>"),
    ],
)
def test_lit_ndarray_dtypes(spark: ReparkSession, dtype_name: str, expected_simple: str) -> None:
    """Dtype + Arrow values: dtype-only pin stays green under value corruption."""
    np = pytest.importorskip("numpy")
    arr = np.array([1, 2]).astype(dtype_name)
    frame = spark.range(1).select(F.lit(arr).alias("b"))
    assert frame.dtypes == [("b", expected_simple)]
    # Mutation-proof values on collect/to_arrow (never dtypes alone).
    rows = frame.to_arrow().to_pylist()
    assert len(rows) == 1
    values = list(rows[0]["b"])
    if dtype_name.startswith("float"):
        assert values == pytest.approx([1.0, 2.0])
    else:
        assert values == [1, 2]


def test_lit_empty_ndarray_keeps_element_type(spark: ReparkSession) -> None:
    np = pytest.importorskip("numpy")
    arr = np.array([]).astype("int8")
    frame = spark.range(1).select(F.lit(arr).alias("b"))
    assert frame.dtypes == [("b", "array<tinyint>")]
    # Empty array value pin: type alone would miss non-empty corruption.
    assert frame.to_arrow().to_pylist() == [{"b": []}]


def test_lit_bool_and_str_ndarray(spark: ReparkSession) -> None:
    np = pytest.importorskip("numpy")
    bool_frame = spark.range(1).select(F.lit(np.array([True, False], np.bool_)).alias("a"))
    assert bool_frame.dtypes == [("a", "array<boolean>")]
    assert bool_frame.to_arrow().to_pylist() == [{"a": [True, False]}]
    str_frame = spark.range(1).select(F.lit(np.array(["a"], np.str_)).alias("a"))
    assert str_frame.dtypes == [("a", "array<string>")]
    assert str_frame.to_arrow().to_pylist() == [{"a": ["a"]}]


def test_lit_uint_ndarray_unsupported(spark: ReparkSession) -> None:
    np = pytest.importorskip("numpy")
    arr = np.array([1, 2]).astype(np.uint)
    with pytest.raises(PySparkTypeError) as raised:
        spark.range(1).select(F.lit(arr).alias("b")).collect()
    assert raised.value.getErrorClass() == "UNSUPPORTED_NUMPY_ARRAY_SCALAR"
    params = raised.value.getMessageParameters()
    assert params is not None
    assert "uint" in params.get("dtype", "")


def test_lit_object_ndarray_unsupported(spark: ReparkSession) -> None:
    """object dtype must refuse (Spark 4.1.2) — not fail-open array<string>."""
    np = pytest.importorskip("numpy")
    arr = np.array([1, 2], dtype=object)
    with pytest.raises(PySparkTypeError) as raised:
        spark.range(1).select(F.lit(arr).alias("b")).collect()
    assert raised.value.getErrorClass() == "UNSUPPORTED_NUMPY_ARRAY_SCALAR"
    params = raised.value.getMessageParameters()
    assert params is not None
    assert "object" in params.get("dtype", "")
    # Mutation-proof: must not silently return array<string> if refuse is removed.
    with pytest.raises(PySparkTypeError):
        _ = F.lit(arr)


def test_lit_bytes_ndarray_unsupported(spark: ReparkSession) -> None:
    """|S (bytes) dtype must refuse — not coerce to array<string>."""
    np = pytest.importorskip("numpy")
    arr = np.array([b"a", b"b"], dtype="|S1")
    with pytest.raises(PySparkTypeError) as raised:
        spark.range(1).select(F.lit(arr).alias("b")).collect()
    assert raised.value.getErrorClass() == "UNSUPPORTED_NUMPY_ARRAY_SCALAR"
    params = raised.value.getMessageParameters()
    assert params is not None
    dtype_param = params.get("dtype", "")
    # |S1 or bytes* — kind-S refuse path; not silent string.
    assert "S" in dtype_param or "bytes" in dtype_param.lower()
    with pytest.raises(PySparkTypeError):
        _ = F.lit(arr)


def test_lit_column_in_list_raises(spark: ReparkSession) -> None:
    with pytest.raises(PySparkValueError) as raised:
        F.lit([spark.range(1).id, spark.range(1).id])
    assert raised.value.getErrorClass() == "COLUMN_IN_LIST"
    assert raised.value.getMessageParameters() == {"func_name": "lit"}
