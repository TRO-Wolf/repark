"""ICE-MERGE-APPEND-1 pins: an INSERT commits through ``merge_append``.

Replays the recorded Spark oracle (``ice_merge_append_1_truth.json``, PySpark
4.1.2 + Iceberg 1.11.0, recorded 2026-09-19) against RePark's own append commit
path — ``INSERT INTO … BY NAME``, which lowers to
``repark_iceberg::write::commit_append_to``. Java's ``Table.newAppend()`` is the
MERGING append producer and every Spark batch write uses it
(``SparkWrite$BatchAppend``); only the structured-streaming micro-batch uses
``newFastAppend()``, and RePark has no streaming writer. So every RePark append
site is a merging-append site, and the three ``commit.manifest*`` table
properties must take effect exactly as they do in Spark.

Two halves of the parity are DECLARED rather than fixed here, each with a strict
xfail that flips the moment the fork lands its half:

* the ``manifests-created`` / ``-kept`` / ``-replaced`` snapshot summary keys,
  which the fork's ``MergeAppendAction`` does not write (fork ask #322);
* the bare ``INSERT INTO`` statement, which falls through to DataFusion and
  commits inside the fork's ``IcebergCommitExec`` (``fast_append``), a commit
  site this repository cannot reach at fork pin ``44834673``.

pins: ice-merge-append-1/C-002, C-003, C-004, C-005, C-006, C-009
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

_TRUTH = json.loads((Path(__file__).parent / "ice_merge_append_1_truth.json").read_text())
_CATALOG = "ma"
_NAMESPACE = "ns"
_VARIANTS = ("defaults", "min_count_5", "merge_disabled")
_SUMMARY_KEYS = ("manifests-created", "manifests-kept", "manifests-replaced")
_FORK_SUMMARY_ASK = (
    "fork #322: MergeAppendAction does not write the manifests-created/-kept/-replaced "
    "summary keys (the fork's own named deviation: 'extra summary keys — same shape as "
    "fast_append'). DECLARED in docs/spark-sql-iceberg-parity.md as ICE-MERGE-APPEND-SUMMARY-1."
)
_FORK_INSERT_ASK = (
    "fork ask ICE-MERGE-APPEND-COMMITEXEC: a bare INSERT INTO plans on the fork's "
    "IcebergCommitExec, whose InsertOp::Append arm commits through fast_append(); the exec is "
    "pub(crate), so RePark cannot route it at pin 44834673. DECLARED in "
    "docs/spark-sql-iceberg-parity.md as ICE-MERGE-APPEND-INSERT-1."
)


def _table_properties(variant: str) -> str:
    extra = "".join(f", '{key}'='{value}'" for key, value in _TRUTH["variants"][variant].items())
    return f"'format-version'='2'{extra}"


def _create(session: ReparkSession, table: str, variant: str) -> None:
    session.sql(
        f"CREATE TABLE {table} (id INT, v STRING) USING iceberg "
        f"TBLPROPERTIES ({_table_properties(variant)})"
    ).collect()


def _counts(session: ReparkSession, table: str) -> tuple[int, int]:
    manifests = session.sql(f"SELECT count(*) FROM {table}.manifests").collect()[0][0]
    files = session.sql(f"SELECT count(*) FROM {table}.files").collect()[0][0]
    return manifests, files


def _last_summary(session: ReparkSession, table: str) -> dict[str, str]:
    row = session.sql(
        f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
    ).collect()[-1]
    return dict(row[1]) if not isinstance(row[1], dict) else row[1]


def _replay(session: ReparkSession, table: str, by_name: bool) -> list[dict[str, int]]:
    probes = set(_TRUTH["probes"])
    measured: list[dict[str, int]] = []
    for step in range(1, _TRUTH["steps"] + 1):
        if by_name:
            statement = f"INSERT INTO {table} BY NAME SELECT {step} AS id, 'v{step}' AS v"
        else:
            statement = f"INSERT INTO {table} VALUES ({step}, 'v{step}')"
        session.sql(statement).collect()
        if step in probes:
            manifests, files = _counts(session, table)
            measured.append({"appends": step, "manifests": manifests, "data_files": files})
    return measured


def _expected(variant: str) -> list[dict[str, int]]:
    return [
        {"appends": row["appends"], "manifests": row["manifests"], "data_files": row["data_files"]}
        for row in _TRUTH["series"][variant]
    ]


@pytest.fixture
def session(tmp_path: Path) -> Any:
    """A session with a memory catalog for the recorded-series replay."""
    built = (
        ReparkSession.builder.appName("pytest-ice-merge-append-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    built.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
    built.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
    yield built
    built.stop()


def test_the_fixture_is_the_recorded_spark_oracle() -> None:
    """The fixture is Spark 4.1.2 + Iceberg 1.11.0, not a hand-computed expectation."""
    assert _TRUTH["oracle"]["spark"] == "4.1.2"
    assert _TRUTH["oracle"]["iceberg"] == "1.11.0"
    assert _TRUTH["probes"] == [1, 5, 20, 50, 99, 100, 101, 110, 120]
    assert set(_TRUTH["series"]) == set(_VARIANTS)


@pytest.mark.parametrize("variant", _VARIANTS)
def test_by_name_append_series_matches_spark(session: ReparkSession, variant: str) -> None:
    """RePark's own append path reproduces Spark's manifest series in all three variants."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_{variant}"
    _create(session, table, variant)
    assert _replay(session, table, by_name=True) == _expected(variant)


def test_defaults_collapse_to_one_manifest_at_the_hundredth_append(
    session: ReparkSession,
) -> None:
    """The headline number: 100 default appends leave ONE manifest, as Spark does."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_collapse"
    _create(session, table, "defaults")
    for step in range(1, 101):
        session.sql(f"INSERT INTO {table} BY NAME SELECT {step} AS id, 'v{step}' AS v").collect()
    assert _counts(session, table) == (1, 100)
    assert session.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 100


def test_merge_disabled_is_a_real_escape_hatch(session: ReparkSession) -> None:
    """``commit.manifest-merge.enabled=false`` keeps one manifest per append."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_escape"
    _create(session, table, "merge_disabled")
    for step in range(1, 101):
        session.sql(f"INSERT INTO {table} BY NAME SELECT {step} AS id, 'v{step}' AS v").collect()
    assert _counts(session, table) == (100, 100)


@pytest.mark.xfail(strict=True, reason=_FORK_SUMMARY_ASK)
def test_merging_commit_stamps_the_manifests_summary_keys(session: ReparkSession) -> None:
    """Spark stamps manifests-created/-kept/-replaced on EVERY append snapshot."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_summary"
    _create(session, table, "min_count_5")
    for step in range(1, 6):
        session.sql(f"INSERT INTO {table} BY NAME SELECT {step} AS id, 'v{step}' AS v").collect()
    summary = _last_summary(session, table)
    assert [summary.get(key) for key in _SUMMARY_KEYS] == ["1", "0", "4"]


def test_bare_insert_into_still_commits_through_the_fork_commit_exec(
    session: ReparkSession,
) -> None:
    """Today's fork-side behaviour, pinned so the fork's fix reds this on purpose."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_bare"
    _create(session, table, "defaults")
    for step in range(1, 101):
        session.sql(f"INSERT INTO {table} VALUES ({step}, 'v{step}')").collect()
    assert _counts(session, table) == (100, 100)
    plan = session.sql(f"EXPLAIN INSERT INTO {table} VALUES (101, 'v101')").collect()
    assert any("IcebergCommitExec" in str(row[1]) for row in plan)


@pytest.mark.xfail(strict=True, reason=_FORK_INSERT_ASK)
def test_bare_insert_into_merges_like_spark(session: ReparkSession) -> None:
    """Spark's bare INSERT INTO merges at the hundredth append; RePark's does not."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_bare_spark"
    _create(session, table, "defaults")
    for step in range(1, 101):
        session.sql(f"INSERT INTO {table} VALUES ({step}, 'v{step}')").collect()
    assert _counts(session, table) == (1, 100)


def test_v3_row_lineage_survives_a_merging_append(session: ReparkSession) -> None:
    """A merging commit leaves `_row_id` contiguous and the rows intact on a v3 table."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_lineage"
    session.sql(
        f"CREATE TABLE {table} (id INT, v STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='3')"
    ).collect()
    for step in range(1, 101):
        session.sql(f"INSERT INTO {table} BY NAME SELECT {step} AS id, 'v{step}' AS v").collect()
    assert _counts(session, table) == (1, 100)
    rows = session.sql(f"SELECT id, v, _row_id FROM {table} ORDER BY id").collect()
    assert [row[0] for row in rows] == list(range(1, 101))
    assert [row[1] for row in rows] == [f"v{step}" for step in range(1, 101)]
    assert sorted(row[2] for row in rows) == list(range(100))


def test_a_merging_append_keeps_the_rows_readable(session: ReparkSession) -> None:
    """The merged manifest still reads every row of every append, in order."""
    table = f"{_CATALOG}.{_NAMESPACE}.t_readback"
    _create(session, table, "min_count_5")
    for step in range(1, 21):
        session.sql(f"INSERT INTO {table} BY NAME SELECT {step} AS id, 'v{step}' AS v").collect()
    rows = session.sql(f"SELECT id, v FROM {table} ORDER BY id").collect()
    assert [(row[0], row[1]) for row in rows] == [(step, f"v{step}") for step in range(1, 21)]
