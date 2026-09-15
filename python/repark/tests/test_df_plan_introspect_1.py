"""DF-PLAN-INTROSPECT-1 — Rust plan-introspection pins driven by the live oracle."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812

_ORACLE = json.loads((Path(__file__).parent / "facade_dataframe_surface_oracle.json").read_text())


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE["cells"][name]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-df-plan-introspect-1").getOrCreate()
    yield session
    session.stop()


def _kv(spark: ReparkSession):
    """Build the oracle's two-row key/a/b frame."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def _write_parquet(spark: ReparkSession, path: str):
    """Write the kv frame to path as parquet."""
    _kv(spark).coalesce(1).write.parquet(path)


def test_inputfiles_parquet_lists_file_uri(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_parquet."""
    assert _cell("inputFiles_parquet")["result"]["kind"] == "list"
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    files = spark.read.parquet(target).inputFiles()
    assert isinstance(files, list)
    assert len(files) == 1
    assert all(isinstance(item, str) for item in files)
    assert files[0].startswith("file://")
    assert files[0].endswith(".parquet")
    assert Path(files[0].removeprefix("file://")).exists()


def test_inputfiles_local_frame_is_empty(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_local."""
    assert _kv(spark).inputFiles() == _cell("inputFiles_local")["result"]["items"]


def test_inputfiles_union_dedupes(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_union."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target)
    assert len(frame.union(frame).inputFiles()) == _cell("inputFiles_union")["result"]["value"]


def test_inputfiles_filtered_scan_keeps_file(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_csv_filter."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target).filter("a > 100")
    assert len(frame.inputFiles()) == _cell("inputFiles_csv_filter")["result"]["value"]


def test_inputfiles_path_with_comma_and_brackets(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — comma/bracket path stays one URI."""
    target = str(tmp_path / "if, a")
    _write_parquet(spark, target)
    written = list(Path(target).iterdir())
    assert len(written) == 1
    renamed = Path(target) / "data [0], x.parquet"
    written[0].rename(renamed)
    files = spark.read.parquet(target).inputFiles()
    assert len(files) == 1
    assert ", " in files[0]
    assert "[" in files[0] and "]" in files[0]
    assert files[0].startswith("file://")
    assert Path(files[0].removeprefix("file://")).exists()


def test_inputfiles_join_of_overlapping_scans(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — join lists both sides, no explain crash."""
    left_path = str(tmp_path / "left")
    right_path = str(tmp_path / "right")
    _write_parquet(spark, left_path)
    _write_parquet(spark, right_path)
    left = spark.read.parquet(left_path)
    right = spark.read.parquet(right_path)
    joined = left.join(right, "key").inputFiles()
    assert sorted(joined) == sorted(set(left.inputFiles()) | set(right.inputFiles()))
    assert len(joined) == len(left.inputFiles()) + len(right.inputFiles())


def test_inputfiles_csv_and_json(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — csv and json scans list their files."""
    csv_file = tmp_path / "csv_single" / "part.csv"
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.write_text("key,a,b\nx,1,2\ny,3,4\n")
    csv_files = spark.read.csv(str(csv_file)).inputFiles()
    assert len(csv_files) == 1
    assert csv_files[0].startswith("file://")
    json_file = tmp_path / "json_single" / "part.json"
    json_file.parent.mkdir(parents=True, exist_ok=True)
    json_file.write_text('{"key": "x", "a": 1}\n')
    json_files = spark.read.json(str(json_file)).inputFiles()
    assert len(json_files) == 1
    assert json_files[0].startswith("file://")


def test_inputfiles_glob_lists_matches(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — a glob read lists every match."""
    first = str(tmp_path / "g1")
    second = str(tmp_path / "g2")
    _write_parquet(spark, first)
    _write_parquet(spark, second)
    first_files = spark.read.parquet(first).inputFiles()
    second_files = spark.read.parquet(second).inputFiles()
    assert first_files and second_files
    assert not set(first_files) & set(second_files)
    globbed = spark.read.parquet(str(tmp_path / "g*")).inputFiles()
    assert sorted(globbed) == sorted(first_files + second_files)


def test_inputfiles_cached_frame_keeps_source_files(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cached frame still lists source files."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target)
    before = frame.inputFiles()
    frame.cache()
    assert frame.collect()
    assert frame.inputFiles() == before


def test_inputfiles_sql_door_matches_python_door(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — spark.sql frame lists the same files."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    direct = spark.read.parquet(target)
    direct.createOrReplaceTempView("plan_intro_v")
    try:
        via_sql = spark.sql("SELECT * FROM plan_intro_v")
        assert via_sql.inputFiles() == direct.inputFiles()
    finally:
        spark.catalog.dropTempView("plan_intro_v")


def test_semantichash_answers_int(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_type."""
    value = _kv(spark).semanticHash()
    assert type(value).__name__ == _cell("semanticHash_type")["result"]["value"]
    assert -(2**31) <= value <= 2**31 - 1


def test_semantichash_equal_plans(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_equal_plans."""
    left = spark.range(3).filter("id > 1").semanticHash()
    right = spark.range(3).filter("id > 1").semanticHash()
    assert (left == right) == _cell("semanticHash_equal_plans")["result"]["value"]


def test_semantichash_alias_equal(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_alias_equal."""
    left = spark.range(3).select(F.col("id").alias("a")).semanticHash()
    right = spark.range(3).select(F.col("id").alias("b")).semanticHash()
    assert (left == right) == _cell("semanticHash_alias_equal")["result"]["value"]


def test_semantichash_diff(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_diff."""
    assert (spark.range(3).semanticHash() == spark.range(4).semanticHash()) == _cell(
        "semanticHash_diff"
    )["result"]["value"]


def test_semantichash_filter_order(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_filter_order."""
    first = spark.range(3).filter("id > 1").filter("id < 3").semanticHash()
    second = spark.range(3).filter("id < 3").filter("id > 1").semanticHash()
    assert (first == second) == _cell("semanticHash_filter_order")["result"]["value"]


def test_semantichash_literals_columns_limits_casts_paths(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — literals, order, limit, cast, path arms."""
    assert (
        spark.range(5).filter("id > 1").semanticHash()
        != spark.range(5).filter("id > 2").semanticHash()
    )
    assert (
        _kv(spark).select("key", "a").semanticHash() != _kv(spark).select("a", "key").semanticHash()
    )
    assert _kv(spark).limit(5).semanticHash() != _kv(spark).limit(10).semanticHash()
    assert (
        _kv(spark).select(F.col("a").cast("int").alias("v")).semanticHash()
        != _kv(spark).select(F.col("a").cast("bigint").alias("v")).semanticHash()
    )


def test_semantichash_file_paths_differ(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-002 — two file paths hash differently."""
    first = str(tmp_path / "h1")
    second = str(tmp_path / "h2")
    _write_parquet(spark, first)
    _write_parquet(spark, second)
    assert spark.read.parquet(first).semanticHash() != spark.read.parquet(second).semanticHash()


def test_semantichash_ignores_batch_size(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — batch_size never reaches the hash."""
    before = spark.range(3).filter("id > 1").semanticHash()
    spark.conf.set("datafusion.execution.batch_size", "1024")
    try:
        assert spark.range(3).filter("id > 1").semanticHash() == before
    finally:
        spark.conf.set("datafusion.execution.batch_size", "65536")


def test_semantichash_stable_and_sql_door(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — stable in process, equal over spark.sql."""
    frame = spark.range(3).filter("id > 1")
    assert frame.semanticHash() == frame.semanticHash()
    assert spark.sql("SELECT 1 AS a").semanticHash() == spark.sql("SELECT 1 AS b").semanticHash()


def test_semantichash_local_frames_differ_by_data(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-006 — cells planintro_local_data_*."""
    first = spark.createDataFrame([(1,)], "a int")
    second = spark.createDataFrame([(2,)], "a int")
    twin = spark.createDataFrame([(1,)], "a int")
    assert (first.semanticHash() == second.semanticHash()) == _cell(
        "planintro_local_data_different_data_equal_hash"
    )["result"]["value"]
    assert (
        first.sameSemantics(second)
        == _cell("planintro_local_data_different_data_same_semantics")["result"]["value"]
    )
    assert (first.semanticHash() == twin.semanticHash()) == _cell(
        "planintro_local_data_same_data_equal_hash"
    )["result"]["value"]
    assert (
        first.sameSemantics(twin)
        == _cell("planintro_local_data_same_data_same_semantics")["result"]["value"]
    )


def test_semantichash_stable_for_one_frame(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-006 — one registration hashes stable."""
    frame = spark.createDataFrame([(1,)], "a int")
    assert frame.semanticHash() == frame.semanticHash()
    assert frame.filter("a > 0").semanticHash() == frame.filter("a > 0").semanticHash()


def test_semantichash_cache_keeps_hash(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-006 — cells planintro_cached_hash_*."""
    left_path = str(tmp_path / "left")
    right_path = str(tmp_path / "right")
    spark.createDataFrame([(1, 10), (2, 20)], "key int, a int").coalesce(1).write.parquet(left_path)
    spark.createDataFrame([(3, 30), (4, 40)], "key int, a int").coalesce(1).write.parquet(
        right_path
    )
    left = spark.read.parquet(left_path)
    right = spark.read.parquet(right_path)
    before_left = left.semanticHash()
    assert (before_left != right.semanticHash()) == _cell("planintro_cached_hash_uncached_differ")[
        "result"
    ]["value"]
    left.cache()
    right.cache()
    assert left.count() == 2
    assert right.count() == 2
    assert (left.semanticHash() != right.semanticHash()) == _cell(
        "planintro_cached_hash_cached_differ"
    )["result"]["value"]
    assert (before_left == left.semanticHash()) == _cell("planintro_cached_hash_cache_keeps_hash")[
        "result"
    ]["value"]
    child_cached = left.filter("a > 0").semanticHash()
    child_fresh = spark.read.parquet(left_path).filter("a > 0").semanticHash()
    assert (child_cached == child_fresh) == _cell(
        "planintro_cached_hash_filter_child_on_cached_equals_uncached_child"
    )["result"]["value"]
    left.unpersist()
    right.unpersist()


def test_inputfiles_cached_frame_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-006 — cells planintro_cached_input_files_*."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target)
    assert (
        len(frame.inputFiles())
        == _cell("planintro_cached_input_files_uncached_len")["result"]["value"]
    )
    frame.cache()
    assert (
        len(frame.inputFiles())
        == _cell("planintro_cached_input_files_cached_before_action_len")["result"]["value"]
    )
    assert frame.count() == 2
    assert (
        len(frame.inputFiles())
        == _cell("planintro_cached_input_files_cached_after_action_len")["result"]["value"]
    )
    assert (
        len(frame.filter("a > 0").inputFiles())
        == _cell("planintro_cached_input_files_filter_child_len")["result"]["value"]
    )
    frame.unpersist()


def test_semantichash_cube_rollup_differ(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-006 — cells planintro_cube_rollup_*."""
    frame = _kv(spark)
    cube = frame.cube("key", "a").count()
    roll = frame.rollup("key", "a").count()
    assert len(cube.collect()) == _cell("planintro_cube_rollup_cube_rows")["result"]["value"]
    assert len(roll.collect()) == _cell("planintro_cube_rollup_rollup_rows")["result"]["value"]
    assert (cube.semanticHash() == roll.semanticHash()) == _cell(
        "planintro_cube_rollup_equal_hash"
    )["result"]["value"]


def test_semantichash_hex_names_differ(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-006 — cell planintro_hex_names_equal_hash."""
    first = spark.createDataFrame([(1,)], "deadbeef int")
    second = spark.createDataFrame([(1,)], "cafebabe int")
    assert (first.semanticHash() == second.semanticHash()) == _cell(
        "planintro_hex_names_equal_hash"
    )["result"]["value"]


def test_semantichash_temp_view_matches_frame(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-006 — cells planintro_temp_view_* (hash and files)."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    direct = spark.read.parquet(target)
    direct.createOrReplaceTempView("plan_intro_v")
    try:
        via_sql = spark.sql("SELECT * FROM plan_intro_v")
        via_table = spark.table("plan_intro_v")
        assert (via_sql.semanticHash() == direct.semanticHash()) == _cell(
            "planintro_temp_view_sql_equal_hash"
        )["result"]["value"]
        assert (via_table.semanticHash() == direct.semanticHash()) == _cell(
            "planintro_temp_view_table_equal_hash"
        )["result"]["value"]
        assert (
            len(via_sql.inputFiles())
            == _cell("planintro_temp_view_sql_input_files_len")["result"]["value"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_v")


def test_inputfiles_uri_form(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-006 — cell planintro_uri_form_prefix."""
    prefix = _cell("planintro_uri_form_prefix")["result"]["items"][0]["value"]
    assert prefix == "file:///tmp/"
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    files = spark.read.parquet(target).inputFiles()
    assert len(files) == 1
    assert files[0].startswith(prefix)
    assert files[0].endswith(".parquet")


def _ab_scan(spark: ReparkSession, tmp_path: Path):
    """Write the oracle's two-row a/b frame and register view ``plan_intro_ab``."""
    target = str(tmp_path / "t.parquet")
    spark.createDataFrame([(1, "x"), (2, "y")], "a int, b string").coalesce(1).write.parquet(target)
    frame = spark.read.parquet(target)
    frame.createOrReplaceTempView("plan_intro_ab")
    return frame


def test_semantichash_df_filter_matches_sql_where(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-008 — DF filter, Column filter, and where match SQL WHERE.
    Cells: planintro_df_filter_vs_sql_where, planintro_df_filter_col_vs_sql_where,
    planintro_where_vs_filter."""
    frame = _ab_scan(spark, tmp_path)
    try:
        string_frame = frame.filter("a > 1")
        via_sql = spark.sql("SELECT * FROM plan_intro_ab WHERE a > 1")
        assert (string_frame.semanticHash() == via_sql.semanticHash()) == _cell(
            "planintro_df_filter_vs_sql_where"
        )["result"]["equal_hash"]
        assert (
            string_frame.sameSemantics(via_sql)
            == _cell("planintro_df_filter_vs_sql_where")["result"]["sameSemantics"]
        )
        col_frame = frame.filter(F.col("a") > 1)
        assert (col_frame.semanticHash() == via_sql.semanticHash()) == _cell(
            "planintro_df_filter_col_vs_sql_where"
        )["result"]["equal_hash"]
        assert (
            col_frame.sameSemantics(via_sql)
            == _cell("planintro_df_filter_col_vs_sql_where")["result"]["sameSemantics"]
        )
        where_frame = frame.where("a > 1")
        assert (where_frame.semanticHash() == string_frame.semanticHash()) == _cell(
            "planintro_where_vs_filter"
        )["result"]["equal_hash"]
        assert (
            where_frame.sameSemantics(string_frame)
            == _cell("planintro_where_vs_filter")["result"]["sameSemantics"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_ab")


def test_semantichash_filter_then_select_matches_sql(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-008 — cell planintro_filter_then_select_vs_sql."""
    frame = _ab_scan(spark, tmp_path)
    try:
        left = frame.filter("a > 1").select("a", "b")
        right = spark.sql("SELECT a, b FROM plan_intro_ab WHERE a > 1")
        assert (left.semanticHash() == right.semanticHash()) == _cell(
            "planintro_filter_then_select_vs_sql"
        )["result"]["equal_hash"]
        assert (
            left.sameSemantics(right)
            == _cell("planintro_filter_then_select_vs_sql")["result"]["sameSemantics"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_ab")


def test_semantichash_view_over_filtered_matches_frame(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """pins: df-plan-introspect-1/C-008 — a view over a filtered frame reads back equal.
    Cells: planintro_view_over_filtered_sql, planintro_view_over_filtered_table."""
    frame = _ab_scan(spark, tmp_path)
    filtered = frame.filter("a > 1")
    filtered.createOrReplaceTempView("plan_intro_vf")
    try:
        via_sql = spark.sql("SELECT * FROM plan_intro_vf")
        via_table = spark.table("plan_intro_vf")
        assert (via_sql.semanticHash() == filtered.semanticHash()) == _cell(
            "planintro_view_over_filtered_sql"
        )["result"]["equal_hash"]
        assert (
            via_sql.sameSemantics(filtered)
            == _cell("planintro_view_over_filtered_sql")["result"]["sameSemantics"]
        )
        assert (via_table.semanticHash() == filtered.semanticHash()) == _cell(
            "planintro_view_over_filtered_table"
        )["result"]["equal_hash"]
        assert (
            via_table.sameSemantics(filtered)
            == _cell("planintro_view_over_filtered_table")["result"]["sameSemantics"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_vf")
        spark.catalog.dropTempView("plan_intro_ab")


def test_semantichash_identity_selects_match_frame(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-009 — identity selects match the frame on the DF door.
    Cells: planintro_select_all_named_vs_df, planintro_select_cols_vs_df,
    planintro_select_star_vs_df."""
    frame = _ab_scan(spark, tmp_path)
    try:
        base = frame.semanticHash()
        named = frame.select("a", "b")
        assert (named.semanticHash() == base) == _cell("planintro_select_all_named_vs_df")[
            "result"
        ]["equal_hash"]
        assert (
            named.sameSemantics(frame)
            == _cell("planintro_select_all_named_vs_df")["result"]["sameSemantics"]
        )
        cols = frame.select(F.col("a"), F.col("b"))
        assert (cols.semanticHash() == base) == _cell("planintro_select_cols_vs_df")["result"][
            "equal_hash"
        ]
        assert (
            cols.sameSemantics(frame)
            == _cell("planintro_select_cols_vs_df")["result"]["sameSemantics"]
        )
        star = frame.select("*")
        assert (star.semanticHash() == base) == _cell("planintro_select_star_vs_df")["result"][
            "equal_hash"
        ]
        assert (
            star.sameSemantics(frame)
            == _cell("planintro_select_star_vs_df")["result"]["sameSemantics"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_ab")


def test_semantichash_sql_selects_match_frame(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-009 — identity selects match the frame on the SQL door.
    Cells: planintro_sql_select_named_vs_df, planintro_sql_select_star_vs_df."""
    frame = _ab_scan(spark, tmp_path)
    try:
        base = frame.semanticHash()
        sql_named = spark.sql("SELECT a, b FROM plan_intro_ab")
        assert (sql_named.semanticHash() == base) == _cell("planintro_sql_select_named_vs_df")[
            "result"
        ]["equal_hash"]
        assert (
            sql_named.sameSemantics(frame)
            == _cell("planintro_sql_select_named_vs_df")["result"]["sameSemantics"]
        )
        sql_star = spark.sql("SELECT * FROM plan_intro_ab")
        assert (sql_star.semanticHash() == base) == _cell("planintro_sql_select_star_vs_df")[
            "result"
        ]["equal_hash"]
        assert (
            sql_star.sameSemantics(frame)
            == _cell("planintro_sql_select_star_vs_df")["result"]["sameSemantics"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_ab")


def test_semantichash_reordered_select_differs(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-009 — cell planintro_select_reordered_vs_df."""
    frame = _ab_scan(spark, tmp_path)
    try:
        reordered = frame.select("b", "a")
        assert (reordered.semanticHash() == frame.semanticHash()) == _cell(
            "planintro_select_reordered_vs_df"
        )["result"]["equal_hash"]
        assert (
            reordered.sameSemantics(frame)
            == _cell("planintro_select_reordered_vs_df")["result"]["sameSemantics"]
        )
    finally:
        spark.catalog.dropTempView("plan_intro_ab")


def test_inputfiles_iceberg_scan_is_empty(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-010 — an Iceberg table scan answers ``[]`` like Spark."""
    spark.sql("CREATE TABLE plan_intro_ice (a INT) USING iceberg")
    spark.sql("INSERT INTO plan_intro_ice VALUES (1), (2)")
    frame = spark.table("plan_intro_ice")
    assert [row["a"] for row in frame.collect()] == [1, 2]
    assert frame.inputFiles() == []


def test_semantichash_non_identity_selects_differ(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-009 — a rename, a subset, and an expression stay projections."""
    frame = _ab_scan(spark, tmp_path)
    try:
        base = frame.semanticHash()
        assert frame.select(F.col("a").alias("z")).semanticHash() != base
        assert frame.select("a").semanticHash() != base
        assert frame.select((F.col("a") + 1).alias("a")).semanticHash() != base
        assert (
            spark.sql("SELECT CAST('1' AS INT)").semanticHash()
            != spark.sql("SELECT 1").semanticHash()
        )
    finally:
        spark.catalog.dropTempView("plan_intro_ab")


def _cast_scan(spark: ReparkSession, tmp_path: Path):
    """Write the cast probe's ``a int, big bigint, b string`` frame and register view ``v``."""
    target = str(tmp_path / "t.parquet")
    spark.createDataFrame(
        [(1, 5000000000, "x"), (2, 7, "y")], "a int, big bigint, b string"
    ).write.parquet(target)
    frame = spark.read.parquet(target)
    frame.createOrReplaceTempView("v")
    return frame


def test_semantichash_cast_cells_match_oracle(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-011 — every ``planintro_cast_*`` cell, both fields."""
    frame = _cast_scan(spark, tmp_path)
    try:
        pairs = [
            (
                "planintro_cast_narrow_cast_vs_plain_col",
                frame.filter(F.col("big").cast("int") > 5),
                frame.filter(F.col("big") > 5),
            ),
            (
                "planintro_cast_narrow_cast_vs_plain_sql",
                spark.sql("SELECT * FROM v WHERE CAST(big AS INT) > 5"),
                spark.sql("SELECT * FROM v WHERE big > 5"),
            ),
            (
                "planintro_cast_widen_cast_vs_plain_col",
                frame.filter(F.col("a").cast("bigint") > 5),
                frame.filter(F.col("a") > 5),
            ),
            (
                "planintro_cast_int_col_lit_vs_sql",
                frame.filter(F.col("a") > 5),
                spark.sql("SELECT * FROM v WHERE a > 5"),
            ),
            ("planintro_cast_alias_twin", frame.alias("x"), frame),
            (
                "planintro_cast_filter_twin_same_frame",
                frame.filter("a > 1"),
                frame.filter("a > 1"),
            ),
            (
                "planintro_cast_cast_to_string_vs_plain",
                frame.filter(F.col("a").cast("string") == "1"),
                frame.filter(F.col("a") == 1),
            ),
            (
                "planintro_cast_literal_long_vs_int",
                frame.filter(F.col("big") > F.lit(5)),
                frame.filter(F.col("big") > F.lit(5).cast("bigint")),
            ),
        ]
        for name, left, right in pairs:
            assert (left.semanticHash() == right.semanticHash()) == _cell(name)["result"][
                "equal_hash"
            ]
            assert left.sameSemantics(right) == _cell(name)["result"]["sameSemantics"]
    finally:
        spark.catalog.dropTempView("v")
