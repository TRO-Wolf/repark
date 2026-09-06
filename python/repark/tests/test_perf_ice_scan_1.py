"""PERF-ICE-SCAN-1 — count(*) folds without a scan; small tables scan in parallel."""

import os
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live scan oracle is skipped (CI is JVM-free)"
F27_SKIP = "fork pin predates F-27 — the fold and parallel-scan pins ride the RP-13 bump"

CATALOG = "scan_perf"
THREADS = "8"
FILES = 8
ROWS_PER_FILE = 3
TOTAL_ROWS = FILES * ROWS_PER_FILE
V3 = "'format-version' = '3'"
MOR = "'write.delete.mode' = 'merge-on-read'"

SCAN_N = re.compile(r"IcebergTableScan[^\n]*N=(\d+)")


def _session(name: str, warehouse: Path) -> ReparkSession:
    """A shuffle-8 session on a module-private catalog."""
    engine = (
        ReparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", THREADS)
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return engine


def _insert_8_files(engine: ReparkSession, table: str) -> None:
    """Eight three-row appends: eight data files, ids 1..24."""
    for index in range(FILES):
        base = index * ROWS_PER_FILE
        engine.sql(
            f"INSERT INTO {table} VALUES ({base + 1}, 'a'), ({base + 2}, 'b'), ({base + 3}, 'c')"
        ).collect()


def _physical_plan(engine: ReparkSession, query: str) -> str:
    """The physical-plan text of query."""
    rows = engine.sql(f"EXPLAIN {query}").collect()
    physical = [row["plan"] for row in rows if row["plan_type"] == "physical_plan"]
    assert physical, f"EXPLAIN produced no physical plan: {rows}"
    return "\n".join(physical)


def _scan_n(plan: str) -> int | None:
    """The IcebergTableScan partition count, or None when the plan has no scan."""
    found = SCAN_N.findall(plan)
    assert len(found) <= 1, f"expected one scan, found {found}"
    return int(found[0]) if found else None


def _count(engine: ReparkSession, query: str) -> int:
    """The scalar answer of a count(*) query."""
    rows = engine.sql(query).to_arrow()
    return int(rows.column(0)[0].as_py())


def _ids(engine: ReparkSession, query: str) -> list[int]:
    """The sorted id column of query, with the Arrow type checked."""
    import pyarrow as pa

    rows = engine.sql(query).to_arrow()
    assert rows.schema.field("id").type == pa.int32(), rows.schema
    found = [int(value.as_py()) for value in rows.column("id")]
    found.sort()
    return found


@pytest.fixture(scope="module")
def f27_present(tmp_path_factory: pytest.TempPathFactory) -> bool:
    """The F-27 probe, once: a folded count(*) plan has no scan."""
    warehouse = tmp_path_factory.mktemp("scan-probe")
    engine = _session("scan-probe", warehouse)
    try:
        engine.sql(f"CREATE TABLE {CATALOG}.w.t (id INT, name STRING) USING iceberg").collect()
        _insert_8_files(engine, f"{CATALOG}.w.t")
        plan = _physical_plan(engine, f"SELECT count(*) FROM {CATALOG}.w.t")
        return "IcebergTableScan" not in plan
    finally:
        engine.stop()


@pytest.fixture()
def bed(tmp_path: Path, f27_present: bool) -> dict[str, Any]:
    """Eight files of three rows plus an empty table, on a per-test session."""
    engine = _session("scan-bed", tmp_path / "wh")
    try:
        engine.sql(f"CREATE TABLE {CATALOG}.w.t (id INT, name STRING) USING iceberg").collect()
        _insert_8_files(engine, f"{CATALOG}.w.t")
        engine.sql(f"CREATE TABLE {CATALOG}.w.empty (id INT, name STRING) USING iceberg").collect()
        yield {"engine": engine, "f27": f27_present}
    finally:
        engine.stop()


def _need_f27(bed: dict[str, Any]) -> ReparkSession:
    """Skip with the named reason until the fork pin carries F-27."""
    if not bed["f27"]:
        pytest.skip(F27_SKIP)
    engine = bed["engine"]
    assert isinstance(engine, ReparkSession)
    return engine


def test_count_star_counts_all_rows(bed: dict[str, Any]) -> None:
    """C-002: count(*) answers the full row count, and zero on an empty table."""
    engine = bed["engine"]
    assert _count(engine, f"SELECT count(*) FROM {CATALOG}.w.t") == TOTAL_ROWS
    assert _count(engine, f"SELECT count(*) FROM {CATALOG}.w.empty") == 0


def test_count_star_counts_live_rows_with_deletes(bed: dict[str, Any]) -> None:
    """C-003: count(*) answers the live rows after a DELETE."""
    engine = bed["engine"]
    engine.sql(f"CREATE TABLE {CATALOG}.w.dv (id INT, name STRING) USING iceberg").collect()
    _insert_8_files(engine, f"{CATALOG}.w.dv")
    engine.sql(f"DELETE FROM {CATALOG}.w.dv WHERE id = 7").collect()
    assert _count(engine, f"SELECT count(*) FROM {CATALOG}.w.dv") == TOTAL_ROWS - 1


def test_count_star_with_where_counts_matching_rows(bed: dict[str, Any]) -> None:
    """C-003: count(*) with a residual answers the matching rows."""
    engine = bed["engine"]
    assert _count(engine, f"SELECT count(*) FROM {CATALOG}.w.t WHERE id > 20") == 4


def test_count_star_with_limit_returns_limited_rows(bed: dict[str, Any]) -> None:
    """C-003: LIMIT above count(*) limits the single-row answer, never the count."""
    engine = bed["engine"]
    rows = engine.sql(f"SELECT count(*) AS n FROM {CATALOG}.w.t LIMIT 0").to_arrow()
    assert rows.num_rows == 0
    rows = engine.sql(f"SELECT count(*) AS n FROM {CATALOG}.w.t LIMIT 1").to_arrow()
    assert rows.num_rows == 1
    assert int(rows.column("n")[0].as_py()) == TOTAL_ROWS


def test_identity_delete_removes_exact_rows(bed: dict[str, Any]) -> None:
    """C-005: DELETE over eight files removes exactly the matching rows."""
    engine = bed["engine"]
    engine.sql(f"CREATE TABLE {CATALOG}.w.d (id INT, name STRING) USING iceberg").collect()
    _insert_8_files(engine, f"{CATALOG}.w.d")
    engine.sql(f"DELETE FROM {CATALOG}.w.d WHERE id % 2 = 0").collect()
    assert _ids(engine, f"SELECT id FROM {CATALOG}.w.d") == [i for i in range(1, 25) if i % 2]


def test_merge_into_upserts_exact_rows(bed: dict[str, Any]) -> None:
    """C-005: MERGE over eight files upserts exactly the matched and new rows."""
    engine = bed["engine"]
    engine.sql(f"CREATE TABLE {CATALOG}.w.m (id INT, name STRING) USING iceberg").collect()
    _insert_8_files(engine, f"{CATALOG}.w.m")
    engine.sql("SELECT 2 AS id, 'z' AS name UNION ALL SELECT 25, 'y'").createOrReplaceTempView(
        "scan_upd"
    )
    engine.sql(
        f"MERGE INTO {CATALOG}.w.m AS target USING scan_upd AS source "
        "ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    ).collect()
    assert _ids(engine, f"SELECT id FROM {CATALOG}.w.m") == list(range(1, 26))
    rows = engine.sql(f"SELECT name FROM {CATALOG}.w.m WHERE id = 2").to_arrow()
    assert rows.column("name")[0].as_py() == "z"


def test_count_star_folds_on_plain_table(bed: dict[str, Any]) -> None:
    """C-002: count(*) on a plain table folds — the plan has no scan."""
    engine = _need_f27(bed)
    plan = _physical_plan(engine, f"SELECT count(*) FROM {CATALOG}.w.t")
    assert "IcebergTableScan" not in plan, plan
    assert _count(engine, f"SELECT count(*) FROM {CATALOG}.w.t") == TOTAL_ROWS


def _mor_dv_table(engine: ReparkSession, name: str) -> str:
    """Eight files with one MoR-deleted row: 24 rows, 23 live."""
    table = f"{CATALOG}.w.{name}"
    engine.sql(
        f"CREATE TABLE {table} (id INT, name STRING) USING iceberg TBLPROPERTIES ({V3}, {MOR})"
    ).collect()
    _insert_8_files(engine, table)
    engine.sql(f"DELETE FROM {table} WHERE id = 7").collect()
    return table


def test_count_star_with_deletes_does_not_fold(bed: dict[str, Any]) -> None:
    """C-003: count(*) with a DV scans, and answers the live rows."""
    engine = _need_f27(bed)
    table = _mor_dv_table(engine, "dv_mor_fold")
    plan = _physical_plan(engine, f"SELECT count(*) FROM {table}")
    assert "IcebergTableScan" in plan, plan
    assert _count(engine, f"SELECT count(*) FROM {table}") == TOTAL_ROWS - 1


def test_count_star_with_where_does_not_fold(bed: dict[str, Any]) -> None:
    """C-003: count(*) with a residual scans."""
    engine = _need_f27(bed)
    plan = _physical_plan(engine, f"SELECT count(*) FROM {CATALOG}.w.t WHERE id > 20")
    assert "IcebergTableScan" in plan, plan


def test_small_table_scans_in_parallel(bed: dict[str, Any]) -> None:
    """C-004: eight files scan as eight partitions with the row set intact."""
    engine = _need_f27(bed)
    plan = _physical_plan(engine, f"SELECT id, name FROM {CATALOG}.w.t")
    assert _scan_n(plan) == FILES, plan
    assert _ids(engine, f"SELECT id FROM {CATALOG}.w.t") == list(range(1, 25))


def test_unfolded_count_star_stays_single_partition(bed: dict[str, Any]) -> None:
    """C-004: the empty projection never splits — the DV count scans as one partition."""
    engine = _need_f27(bed)
    table = _mor_dv_table(engine, "dv_mor_n1")
    plan = _physical_plan(engine, f"SELECT count(*) FROM {table}")
    assert _scan_n(plan) == 1, plan


def test_row_id_order_unchanged(bed: dict[str, Any]) -> None:
    """C-009: _row_id still tiles 0..N over eight files under F-27 splits."""
    engine = _need_f27(bed)
    engine.sql(
        f"CREATE TABLE {CATALOG}.w.v3 (id INT, name STRING) USING iceberg TBLPROPERTIES ({V3})"
    ).collect()
    _insert_8_files(engine, f"{CATALOG}.w.v3")
    rows = engine.sql(f"SELECT id, _row_id FROM {CATALOG}.w.v3 ORDER BY id").to_arrow()
    assert [int(value.as_py()) for value in rows.column("id")] == list(range(1, 25))
    assert [int(value.as_py()) for value in rows.column("_row_id")] == list(range(24))


def _live_oracle(warehouse: Path) -> Any:
    """The pinned Spark oracle on its own warehouse."""
    import _live_parity as live_parity

    return live_parity.build_spark_iceberg_engine(warehouse)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_partitioned_row_set_matches_spark(tmp_path: Path, f27_present: bool) -> None:
    """C-009: the partitioned bed reads the same row set as Spark."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    if not f27_present:
        pytest.skip(F27_SKIP)
    engine = _session("scan-part-live", tmp_path / "wh")
    try:
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.p (id INT, part INT, name STRING) USING iceberg "
            "PARTITIONED BY (part)"
        ).collect()
        for index in range(FILES):
            base = index * ROWS_PER_FILE
            engine.sql(
                f"INSERT INTO {CATALOG}.w.p VALUES ({base + 1}, {index % 2}, 'a'), "
                f"({base + 2}, {index % 2}, 'b'), ({base + 3}, {index % 2}, 'c')"
            ).collect()
        engine_rows = engine.sql(f"SELECT id, part, name FROM {CATALOG}.w.p ORDER BY id").to_arrow()
    finally:
        engine.stop()

    owned = SparkSession.getActiveSession() is None
    oracle = _live_oracle(tmp_path / "spark-wh")
    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    session = oracle.session
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.w")
        session.sql(
            f"CREATE TABLE {catalog}.w.p (id INT, part INT, name STRING) USING iceberg "
            "PARTITIONED BY (part)"
        )
        for index in range(FILES):
            base = index * ROWS_PER_FILE
            session.sql(
                f"INSERT INTO {catalog}.w.p VALUES ({base + 1}, {index % 2}, 'a'), "
                f"({base + 2}, {index % 2}, 'b'), ({base + 3}, {index % 2}, 'c')"
            )
        spark_rows = session.sql(f"SELECT id, part, name FROM {catalog}.w.p ORDER BY id")
        assert [tuple(row) for row in spark_rows.collect()] == [
            tuple(row)
            for row in zip(
                engine_rows.column("id").to_pylist(),
                engine_rows.column("part").to_pylist(),
                engine_rows.column("name").to_pylist(),
                strict=True,
            )
        ]
    finally:
        if owned:
            session.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_identity_delete_row_set_matches_spark(tmp_path: Path, f27_present: bool) -> None:
    """C-009: DELETE over eight files leaves the same rows as Spark."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    if not f27_present:
        pytest.skip(F27_SKIP)
    engine = _session("scan-del-live", tmp_path / "wh")
    try:
        engine.sql(f"CREATE TABLE {CATALOG}.w.d (id INT, name STRING) USING iceberg").collect()
        _insert_8_files(engine, f"{CATALOG}.w.d")
        engine.sql(f"DELETE FROM {CATALOG}.w.d WHERE id % 2 = 0").collect()
        engine_ids = _ids(engine, f"SELECT id FROM {CATALOG}.w.d")
    finally:
        engine.stop()

    owned = SparkSession.getActiveSession() is None
    oracle = _live_oracle(tmp_path / "spark-wh")
    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    session = oracle.session
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.w")
        session.sql(f"CREATE TABLE {catalog}.w.d (id INT, name STRING) USING iceberg")
        for index in range(FILES):
            base = index * ROWS_PER_FILE
            session.sql(
                f"INSERT INTO {catalog}.w.d VALUES ({base + 1}, 'a'), "
                f"({base + 2}, 'b'), ({base + 3}, 'c')"
            )
        session.sql(f"DELETE FROM {catalog}.w.d WHERE id % 2 = 0")
        spark_ids = sorted(row[0] for row in session.sql(f"SELECT id FROM {catalog}.w.d").collect())
        assert spark_ids == engine_ids
    finally:
        if owned:
            session.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_dv_count_matches_spark(tmp_path: Path, f27_present: bool) -> None:
    """C-003: the MoR count answers what Spark answers."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    if not f27_present:
        pytest.skip(F27_SKIP)
    engine = _session("scan-dv-live", tmp_path / "wh")
    try:
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.dv (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({V3}, {MOR})"
        ).collect()
        _insert_8_files(engine, f"{CATALOG}.w.dv")
        engine.sql(f"DELETE FROM {CATALOG}.w.dv WHERE id = 7").collect()
        engine_count = _count(engine, f"SELECT count(*) FROM {CATALOG}.w.dv")
    finally:
        engine.stop()

    owned = SparkSession.getActiveSession() is None
    oracle = _live_oracle(tmp_path / "spark-wh")
    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    session = oracle.session
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.w")
        session.sql(
            f"CREATE TABLE {catalog}.w.dv (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({V3}, {MOR})"
        )
        for index in range(FILES):
            base = index * ROWS_PER_FILE
            session.sql(
                f"INSERT INTO {catalog}.w.dv VALUES ({base + 1}, 'a'), "
                f"({base + 2}, 'b'), ({base + 3}, 'c')"
            )
        session.sql(f"DELETE FROM {catalog}.w.dv WHERE id = 7")
        spark_count = session.sql(f"SELECT count(*) FROM {catalog}.w.dv").collect()[0][0]
        assert spark_count == engine_count == TOTAL_ROWS - 1
    finally:
        if owned:
            session.stop()
