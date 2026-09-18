"""ICE-OCC-SCOPED-1 — concurrent Iceberg DML storms commit and refuse exactly where Spark does.

Every count, row and loser message is read from the Spark 4.1.2 recordings under
``python/repark-parity/fixtures/torture/data/ice_occ_scoped_1``. MERGE and INSERT run on both
doors (SQL and ``DataFrame.mergeInto`` / ``writeTo().append()``); UPDATE and DELETE have no
DataFrame spelling in PySpark 4.1, so they run on the SQL door in both door cells. A plain-WHERE
UPDATE runs the fork's DataFusion exec with its own scan predicate as the conflict filter since
RP-27 (fork #294 F-OCC-EXEC-1): its storm replays Spark's recorded 4-of-4 answer (registry row
ICE-OCC-SCOPED-1-PLAIN-UPDATE, FIXED 2026-09-18).

pins: ice-occ-scoped-1/C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013
pins: ice-occ-scoped-1/C-015, C-016, C-017
"""

from __future__ import annotations

import json
import re
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "repark-parity"
    / "fixtures"
    / "torture"
    / "data"
    / "ice_occ_scoped_1"
)
_UNSCOPED = json.loads((_FIXTURE / "spark_occ_oracle.json").read_text(encoding="utf-8"))["cells"]
_SCOPED = json.loads((_FIXTURE / "spark_occ_oracle2.json").read_text(encoding="utf-8"))["cells"]
_SPARK_PREFIX = "sc.ns."
_LOSER_CLASS = "PySparkException"
_VALIDATION = re.compile(r"ValidationException: (?P<head>.+?) matching (?P<tail>.*)", re.S)
_MERGE = re.compile(
    r"MERGE INTO (?P<table>\S+) t USING \((?P<source>.+)\) s ON (?P<on>.+) "
    r"WHEN MATCHED THEN UPDATE SET v = s\.v"
)
_CONJUNCT = re.compile(r"(?P<left>[ts]\.\w+) (?P<op>=|<|>=) (?P<right>.+)")
_VERSIONS = ("2", "3")
_MODES = ("merge-on-read", "copy-on-write")
_DOORS = ("sql", "dataframe")


def _session(warehouse: Path) -> tuple[Any, str]:
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("ice-occ-scoped-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    catalog = f"occ_{uuid.uuid4().hex[:12]}"
    session.register_memory_catalog(catalog, warehouse)
    session.sql(f"CREATE NAMESPACE {catalog}.ns").collect()
    session.range(100).createOrReplaceTempView("hundred")
    return session, catalog


def _storm(actions: list[Callable[[], None]]) -> tuple[int, list[Exception]]:
    barrier = threading.Barrier(len(actions))
    outcomes: list[Exception | None] = [None] * len(actions)

    def run(index: int, action: Callable[[], None]) -> None:
        barrier.wait()
        try:
            action()
        except Exception as error:
            outcomes[index] = error

    threads = [
        threading.Thread(target=run, args=(index, action)) for index, action in enumerate(actions)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    losers = [outcome for outcome in outcomes if outcome is not None]
    return len(actions) - len(losers), losers


def _assert_storm(
    cell: dict[str, Any], committed: int, losers: list[Exception], label: str
) -> None:
    assert (committed, len(losers)) == (cell["committed"], cell["of"] - cell["committed"]), (
        f"{label}: RePark committed {committed} of {cell['of']}, Spark {cell['committed']}; "
        f"losers: {[str(loser)[:200] for loser in losers]}"
    )
    spark = [_VALIDATION.search(text) for text in cell["errors"]]
    for loser in losers:
        assert type(loser).__name__ == _LOSER_CLASS, f"{label}: {type(loser).__name__}: {loser}"
        assert any(match is not None and _same_validation(match, str(loser)) for match in spark), (
            f"{label}: RePark's loser {str(loser)[:300]!r} is none of Spark's {cell['errors']}"
        )


def _same_validation(spark: re.Match[str], repark: str) -> bool:
    ours = _VALIDATION.search(f"ValidationException: {repark}")
    if ours is None or spark["head"] not in repark:
        return False
    spark_unscoped = spark["tail"].lower().startswith("true")
    return spark_unscoped == ours["tail"].lower().startswith("true")


def _rows(session: Any, query: str) -> list[list[Any]]:
    return [list(row) for row in session.sql(query).collect()]


def _ours(text: str, catalog: str) -> str:
    return text.replace(_SPARK_PREFIX, f"{catalog}.ns.")


def _merge_action(session: Any, statement: str, door: str) -> Callable[[], None]:
    if door == "sql":
        return lambda: session.sql(statement).collect()
    from repark.spark import functions

    shape = _MERGE.fullmatch(statement)
    assert shape is not None, statement
    condition = None
    for conjunct in shape["on"].split(" AND "):
        parts = _CONJUNCT.fullmatch(conjunct)
        assert parts is not None, conjunct
        left = functions.col(parts["left"].replace("t.", "target.", 1))
        raw = parts["right"]
        if raw.startswith("s."):
            right = functions.col(raw.replace("s.", "source.", 1))
        elif raw.startswith("'"):
            right = functions.lit(raw.strip("'"))
        else:
            right = functions.lit(int(raw))
        term = {"=": left == right, "<": left < right, ">=": left >= right}[parts["op"]]
        condition = term if condition is None else condition & term
    source = shape["source"].removeprefix("SELECT ")
    table = shape["table"]

    def run() -> None:
        writer = session.sql(f"SELECT {source}").mergeInto(table, condition)
        writer.whenMatched().update({"v": functions.col("source.v")}).merge()

    return run


def _insert_action(session: Any, table: str, select: str, door: str) -> Callable[[], None]:
    if door == "sql":
        return lambda: session.sql(f"INSERT INTO {table} {select}").collect()
    return lambda: session.sql(select).writeTo(table).append()


def _statement_action(session: Any, statement: str, door: str) -> Callable[[], None]:
    if statement.startswith("MERGE INTO"):
        return _merge_action(session, statement, door)
    insert = re.fullmatch(r"INSERT INTO (\S+) VALUES \((\d+), '(\w+)', '(\w+)'\)", statement)
    if insert is not None:
        table, row_id, key, value = insert.groups()
        select = f"SELECT CAST({row_id} AS BIGINT) AS id, '{key}' AS k, '{value}' AS v"
        return _insert_action(session, table, select, door)
    return lambda: session.sql(statement).collect()


def _replay_storm(session: Any, catalog: str, label: str, door: str) -> None:
    cell = _SCOPED[label]
    actions = [_statement_action(session, _ours(sql, catalog), door) for sql in cell["statements"]]
    committed, losers = _storm(actions)
    _assert_storm(cell, committed, losers, f"{label}/{door}")


def _assert_rows(session: Any, catalog: str, label: str, door: str) -> None:
    cell = _SCOPED[label]
    assert _rows(session, _ours(cell["query"], catalog)) == cell["rows"], f"{label}/{door}"


@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("mode", _MODES)
@pytest.mark.parametrize("version", _VERSIONS)
def test_partition_scoped_storms_match_spark(
    version: str, mode: str, door: str, tmp_path: Path
) -> None:
    """Partition-scoped MERGE and DELETE storms and MERGE vs INSERT commit as Spark's do."""
    session, catalog = _session(tmp_path)
    tag = f"v{version}_{mode.replace('-', '')}"
    table = f"{catalog}.ns.pm{version}_{mode.replace('-', '')}"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, k STRING, v STRING) USING iceberg PARTITIONED BY (k) "
        f"TBLPROPERTIES ('format-version'='{version}', 'write.merge.mode'='{mode}', "
        f"'write.update.mode'='{mode}', 'write.delete.mode'='{mode}')"
    ).collect()
    session.sql(
        f"INSERT INTO {table} SELECT CAST(id AS BIGINT) AS id, CASE WHEN id % 4 = 0 THEN 'a' "
        "WHEN id % 4 = 1 THEN 'b' WHEN id % 4 = 2 THEN 'c' ELSE 'd' END AS k, 'init' AS v "
        "FROM hundred"
    ).collect()
    _replay_storm(session, catalog, f"{tag}_4_partition_scoped_merges", door)
    _assert_rows(session, catalog, f"{tag}_rows_after_4_partition_scoped_merges", door)
    _replay_storm(session, catalog, f"{tag}_4_partition_scoped_updates", door)
    _assert_rows(session, catalog, f"{tag}_rows_after_4_partition_scoped_updates", door)
    for storm, rows in (
        ("4_partition_scoped_deletes", "rows_after_4_partition_scoped_deletes"),
        ("merge_vs_concurrent_insert_other_partition", "rows_after_merge_vs_insert"),
    ):
        _replay_storm(session, catalog, f"{tag}_{storm}", door)
        _assert_rows(session, catalog, f"{tag}_{rows}", door)


@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("mode", _MODES)
@pytest.mark.parametrize("version", _VERSIONS)
def test_range_scoped_merges_match_spark(
    version: str, mode: str, door: str, tmp_path: Path
) -> None:
    """Copy-on-write commits both disjoint-range MERGEs; merge-on-read refuses one, like Spark."""
    session, catalog = _session(tmp_path)
    tag = f"v{version}_{mode.replace('-', '')}"
    table = f"{catalog}.ns.um{version}_{mode.replace('-', '')}"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, v STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}', 'write.merge.mode'='{mode}')"
    ).collect()
    for low, high in ((0, 50), (50, 100)):
        session.sql(
            f"INSERT INTO {table} SELECT CAST(id AS BIGINT) AS id, 'init' AS v FROM hundred "
            f"WHERE id >= {low} AND id < {high}"
        ).collect()
    _replay_storm(session, catalog, f"{tag}_2_range_scoped_merges_unpartitioned", door)
    _assert_rows(session, catalog, f"{tag}_rows_after_2_range_scoped_merges", door)


@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("isolation", ["serializable", "snapshot"])
@pytest.mark.parametrize("version", _VERSIONS)
def test_disjoint_key_merges_refuse_like_spark(
    version: str, isolation: str, door: str, tmp_path: Path
) -> None:
    """Eight MERGEs on disjoint keys with no target-only conjunct: one commits, as in Spark."""
    session, catalog = _session(tmp_path)
    table = f"{catalog}.ns.mrg{version}_{isolation}"
    extra = ", 'write.merge.isolation-level'='snapshot'" if isolation == "snapshot" else ""
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, v STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}', 'write.merge.mode'='merge-on-read'{extra})"
    ).collect()
    session.sql(
        f"INSERT INTO {table} SELECT CAST(id AS BIGINT) AS id, 'init' AS v FROM hundred"
    ).collect()
    statements = [
        f"MERGE INTO {table} t USING (SELECT CAST({index} AS BIGINT) AS id, 'm{index}' AS v) s "
        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"
        for index in range(8)
    ]
    label = f"v{version}_8_disjoint_merges_{isolation}"
    committed, losers = _storm([_merge_action(session, sql, door) for sql in statements])
    _assert_storm(_UNSCOPED[label], committed, losers, f"{label}/{door}")
    rows = _rows(
        session,
        f"SELECT count(*), sum(CASE WHEN v LIKE 'm%' THEN 1 ELSE 0 END), count(DISTINCT id) "
        f"FROM {table}",
    )
    assert rows == _UNSCOPED[f"v{version}_rows_after_8_disjoint_merges_{isolation}"]["rows"]


@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("version", _VERSIONS)
def test_insert_storm_loses_only_to_the_retry_budget(
    version: str, door: str, tmp_path: Path
) -> None:
    """Sixteen appends: every commit is durable, every loser is a catalog commit conflict.

    Over repetitions Spark commits 7–15 of 16 (Hadoop v2 11–15, v3 9–14; InMemory v2 8–9,
    v3 7–9 — fixture `spark_occ_oracle4.json`); RePark's losers exhaust the fork's
    commit-retry budget — registry row ICE-OCC-SCOPED-1-INSERT-STORM (BACKLOG).
    """
    session, catalog = _session(tmp_path)
    table = f"{catalog}.ns.ins{version}"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, w INT) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    ).collect()
    selects = [
        f"SELECT CAST({index} AS BIGINT) AS id, CAST({index} AS INT) AS w" for index in range(16)
    ]
    committed, losers = _storm([_insert_action(session, table, sql, door) for sql in selects])
    cell = _UNSCOPED[f"v{version}_16_concurrent_inserts"]
    assert committed + len(losers) == cell["of"]
    assert committed >= 1
    for loser in losers:
        assert type(loser).__name__ == _LOSER_CLASS, loser
        assert "CatalogCommitConflicts" in str(loser), str(loser)[:300]
    assert all("CommitFailedException" in text for text in cell["errors"])
    assert _rows(session, f"SELECT count(*), count(DISTINCT id) FROM {table}") == [
        [committed, committed]
    ]
    assert _rows(session, f"SELECT count(*) FROM {table}.snapshots") == [[committed]]


@pytest.mark.parametrize("version", _VERSIONS)
def test_whole_partition_deletes_both_commit(version: str, tmp_path: Path) -> None:
    """Two DELETE statements of two different whole partitions both commit, as in Spark."""
    session, catalog = _session(tmp_path)
    table = f"{catalog}.ns.ovr{version}"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, k STRING) USING iceberg PARTITIONED BY (k) "
        f"TBLPROPERTIES ('format-version'='{version}')"
    ).collect()
    session.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')").collect()
    statements = [f"DELETE FROM {table} WHERE k = 'a'", f"DELETE FROM {table} WHERE k = 'b'"]
    committed, losers = _storm([_statement_action(session, sql, "sql") for sql in statements])
    label = f"v{version}_2_disjoint_partition_deletes"
    _assert_storm(_UNSCOPED[label], committed, losers, label)
    assert (
        _rows(session, f"SELECT count(*) FROM {table}")
        == _UNSCOPED[f"v{version}_rows_after_disjoint_deletes"]["rows"]
    )
