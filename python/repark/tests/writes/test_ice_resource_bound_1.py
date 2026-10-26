"""ICE-RESOURCE-BOUND-1: above the stated bound a statement refuses; below it the write commits."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException, PySparkException

CATALOG = "rbound"
TABLE = f"{CATALOG}.w.t"
ORDERED_KEY = "repark.write.ordered.max-input-fraction"
MERGE_KEY = "repark.merge.mor.max-updated-fraction"
CHANGELOG_KEY = "repark.changelog.max-source-fraction"
KEYS = (ORDERED_KEY, MERGE_KEY, CHANGELOG_KEY)
DENIED = ("delete", "drop", "truncate", "expire", "remove", "purge")
COLUMNS = "id BIGINT, k BIGINT, p INT, a DOUBLE, b DOUBLE, s STRING, c INT, flag BOOLEAN"
SELECT_LIST = (
    "id, pmod(id * 7919, {rows}) AS k, CAST(pmod(id, 32) AS INT) AS p, "
    "CAST(id AS DOUBLE) * 0.5 AS a, CAST(pmod(id * 13, 1000) AS DOUBLE) AS b, "
    "concat('s', lpad(CAST(pmod(id * 31, 100000000) AS STRING), 12, '0')) AS s, "
    "CAST(pmod(id, 1000) AS INT) AS c, pmod(id, 2) = 0 AS flag"
)
MOR_PROPERTIES = (
    ", 'write.merge.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
    "'write.delete.mode'='merge-on-read'"
)
FORMATS = (2, 3)
SCALE = os.environ.get("REPARK_RESOURCE_BOUND_SCALE") == "1"
SCALE_SKIP = "REPARK_RESOURCE_BOUND_SCALE != 1 — the 4x and 16x cells run in the scheduled job"
NOT_MATCHED_BY_SOURCE = " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.c = -1"
MOR_MERGE = (
    f"MERGE INTO {TABLE} t USING msrc s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.a = s.a"
)


def _session(warehouse: Path, pool: str, keys: dict[str, str] | None = None) -> ReparkSession:
    """Build a facade session with the given memory pool and bound keys."""
    builder = (
        ReparkSession.builder.appName("ice-resource-bound-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("datafusion.runtime.memory_limit", pool)
    )
    for key, value in (keys or {}).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.register_memory_catalog(CATALOG, str(warehouse))
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return session


def _source(session: ReparkSession, rows: int) -> None:
    """Register the eight-column source view ``src`` over ``rows`` ids."""
    session.range(rows).createOrReplaceTempView("src_range")
    columns = SELECT_LIST.format(rows=rows)
    session.sql(f"SELECT {columns} FROM src_range").createOrReplaceTempView("src")


def _create(session: ReparkSession, format_version: int, merge_on_read: bool) -> None:
    """Create the target table on the given format version."""
    modes = MOR_PROPERTIES if merge_on_read else ""
    session.sql(
        f"CREATE TABLE {TABLE} ({COLUMNS}) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{format_version}'{modes})"
    )


def _seed(session: ReparkSession, rows: int, files: int) -> None:
    """Write ``rows`` source rows into the target as ``files`` appends."""
    step = rows // files
    for index in range(files):
        start = index * step
        stop = rows if index == files - 1 else start + step
        session.sql(
            f"INSERT INTO {TABLE} SELECT * FROM src WHERE id >= {start} AND id < {stop}"
        ).collect()


def _state(session: ReparkSession, warehouse: Path) -> tuple[list[int], list[tuple], set[str]]:
    """Return the snapshot ids, the rows ordered by id and every file under the warehouse."""
    snapshots = sorted(
        row[0] for row in session.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()
    )
    rows = [tuple(row) for row in session.sql(f"SELECT * FROM {TABLE} ORDER BY id").collect()]
    files = {str(path) for path in warehouse.rglob("*") if path.is_file()}
    return snapshots, rows, files


def _assert_refusal(message: str, kind: str, key: str) -> None:
    """Assert the ruled refusal text: the path, the key, the bound and no data-deleting verb."""
    assert "Resources exhausted: REPARK: the " + kind + " holds " in message, message
    assert f"Raise `{key}` (0 = no bound) or raise the memory pool." in message, message
    assert " x pool " in message, message
    lowered = message.lower()
    for word in DENIED:
        assert word not in lowered, message


def _ordered_target(
    warehouse: Path, pool: str, format_version: int, rows: int = 100_000
) -> ReparkSession:
    """Build a session with an ordered empty target and a ``rows``-row source."""
    session = _session(warehouse, pool)
    _source(session, rows)
    _create(session, format_version, merge_on_read=False)
    session.sql(f"ALTER TABLE {TABLE} WRITE ORDERED BY k")
    return session


def _merge_target(
    warehouse: Path,
    pool: str,
    format_version: int,
    keys: dict[str, str] | None,
    rows: int = 200_000,
    match_percent: int = 100,
) -> ReparkSession:
    """Build a session with a merge-on-read target seeded as 32 files and a match source."""
    session = _session(warehouse, pool, keys)
    _source(session, rows)
    _create(session, format_version, merge_on_read=True)
    _seed(session, rows, 32)
    session.sql(
        f"SELECT id, a + 1.0 AS a FROM src WHERE pmod(id * 7, 100) < {match_percent}"
    ).createOrReplaceTempView("msrc")
    return session


@pytest.mark.parametrize("format_version", FORMATS)
def test_ordered_append_refuses_above_the_bound(tmp_path: Path, format_version: int) -> None:
    """D2 append on an ordered table at a 32 MiB pool refuses and leaves the table unchanged."""
    session = _ordered_target(tmp_path, "32M", format_version)
    before = _state(session, tmp_path)
    with pytest.raises(PySparkException) as raised:
        session.sql("SELECT * FROM src").writeTo(TABLE).option(
            "snapshot-property.rbound", "1"
        ).append()
    assert not isinstance(raised.value, IllegalArgumentException)
    _assert_refusal(str(raised.value), "ordered write", ORDERED_KEY)
    assert _state(session, tmp_path) == before
    session.stop()


@pytest.mark.parametrize("format_version", FORMATS)
def test_ordered_append_commits_below_the_bound(tmp_path: Path, format_version: int) -> None:
    """D2 append on an ordered table at a 128 MiB pool commits every source row."""
    session = _ordered_target(tmp_path, "128M", format_version)
    session.sql("SELECT * FROM src").writeTo(TABLE).option("snapshot-property.rbound", "1").append()
    written = session.sql(f"SELECT * FROM {TABLE} ORDER BY id").collect()
    expected = session.sql("SELECT * FROM src ORDER BY id").collect()
    assert [tuple(row) for row in written] == [tuple(row) for row in expected]
    written_types = [field.dataType for field in session.table(TABLE).schema.fields]
    assert written_types == [field.dataType for field in session.table("src").schema.fields]
    session.stop()


@pytest.mark.parametrize("format_version", FORMATS)
def test_merge_on_read_refuses_above_the_lowered_bound(tmp_path: Path, format_version: int) -> None:
    """A merge-on-read MERGE with the key at 0.002 of a 2048 MiB pool refuses."""
    session = _merge_target(tmp_path, "2048M", format_version, {MERGE_KEY: "0.002"})
    before = _state(session, tmp_path)
    with pytest.raises(PySparkException) as raised:
        session.sql(MOR_MERGE).collect()
    _assert_refusal(str(raised.value), "merge-on-read MERGE", MERGE_KEY)
    assert _state(session, tmp_path) == before
    session.stop()


@pytest.mark.parametrize("format_version", FORMATS)
def test_merge_on_read_commits_at_the_default_bound(tmp_path: Path, format_version: int) -> None:
    """The same MERGE at the default fraction commits every updated row."""
    session = _merge_target(tmp_path, "2048M", format_version, None)
    session.sql(MOR_MERGE).collect()
    written = session.sql(f"SELECT id, a FROM {TABLE} ORDER BY id").collect()
    expected = session.sql("SELECT id, a FROM msrc ORDER BY id").collect()
    assert [tuple(row) for row in written] == [tuple(row) for row in expected]
    session.stop()


def _changelog(
    warehouse: Path, pool: str, format_version: int, rows: int = 100_000
) -> ReparkSession:
    """Build a session with a ``rows``-row target over ten snapshots and its changelog view."""
    session = _session(warehouse, pool)
    _source(session, rows)
    _create(session, format_version, merge_on_read=False)
    _seed(session, rows, 10)
    session.sql(f"CALL {CATALOG}.system.create_changelog_view(table => 'w.t')").collect()
    return session


@pytest.mark.parametrize("format_version", FORMATS)
def test_changelog_read_refuses_above_the_bound(tmp_path: Path, format_version: int) -> None:
    """A changelog read at a 32 MiB pool refuses on collect and on the Arrow export."""
    session = _changelog(tmp_path, "32M", format_version)
    with pytest.raises(PySparkException) as raised:
        session.sql("SELECT id FROM t_changes LIMIT 10").collect()
    _assert_refusal(str(raised.value), "changelog read", CHANGELOG_KEY)
    with pytest.raises(PySparkException) as exported:
        session.sql("SELECT count(id) FROM t_changes").to_arrow()
    _assert_refusal(str(exported.value), "changelog read", CHANGELOG_KEY)
    assert "REPARK: raise the FairSpillPool" in str(exported.value)
    session.stop()


@pytest.mark.parametrize("format_version", FORMATS)
def test_changelog_read_answers_below_the_bound(tmp_path: Path, format_version: int) -> None:
    """The same changelog read at a 2048 MiB pool answers."""
    session = _changelog(tmp_path, "2048M", format_version)
    assert len(session.sql("SELECT id FROM t_changes LIMIT 10").collect()) == 10
    assert session.sql("SELECT count(id) FROM t_changes").collect()[0][0] == 100_000
    session.stop()


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("value", ["-0.1", "abc", "inf"])
def test_builder_refuses_an_invalid_fraction(tmp_path: Path, key: str, value: str) -> None:
    """An invalid fraction refuses at build as IllegalArgumentException naming the key."""
    with pytest.raises(IllegalArgumentException) as raised:
        _session(tmp_path, "128M", {key: value})
    assert key in str(raised.value)


@pytest.mark.parametrize("key", KEYS)
def test_runtime_conf_set_refuses_with_the_build_time_hint(tmp_path: Path, key: str) -> None:
    """A runtime conf.set of a bound key refuses and names the builder door."""
    session = _session(tmp_path, "128M")
    with pytest.raises(IllegalArgumentException) as raised:
        session.conf.set(key, "0.5")
    message = str(raised.value)
    assert key in message
    assert "build-time" in message
    assert "ReparkSession.builder.config" in message
    session.stop()


@pytest.mark.skipif(not SCALE, reason=SCALE_SKIP)
@pytest.mark.parametrize("format_version", FORMATS)
@pytest.mark.parametrize("rows", [4_000_000, 16_000_000])
def test_scale_ordered_append_refuses_at_32_mib(
    tmp_path: Path, format_version: int, rows: int
) -> None:
    """The 4x and 16x ordered appends refuse at a 32 MiB pool and leave the table unchanged."""
    session = _ordered_target(tmp_path, "32M", format_version, rows)
    before = _state(session, tmp_path)
    with pytest.raises(PySparkException) as raised:
        session.sql("SELECT * FROM src").writeTo(TABLE).option(
            "snapshot-property.rbound", "1"
        ).append()
    _assert_refusal(str(raised.value), "ordered write", ORDERED_KEY)
    assert _state(session, tmp_path) == before
    session.stop()


@pytest.mark.skipif(not SCALE, reason=SCALE_SKIP)
@pytest.mark.parametrize("format_version", FORMATS)
def test_scale_ordered_append_commits_at_8_gib(tmp_path: Path, format_version: int) -> None:
    """The 4x ordered append commits every row at an 8 GiB pool."""
    session = _ordered_target(tmp_path, "8192M", format_version, 4_000_000)
    session.sql("SELECT * FROM src").writeTo(TABLE).option("snapshot-property.rbound", "1").append()
    assert session.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0] == 4_000_000
    session.stop()


@pytest.mark.skipif(not SCALE, reason=SCALE_SKIP)
@pytest.mark.parametrize("format_version", FORMATS)
@pytest.mark.parametrize(("rows", "pool"), [(4_000_000, "2048M"), (16_000_000, "8192M")])
def test_scale_merge_refuses_at_the_default_fraction(
    tmp_path: Path, format_version: int, rows: int, pool: str
) -> None:
    """The 4x MERGE at 2 GiB and the 16x MERGE at 8 GiB refuse at the default fraction."""
    session = _merge_target(tmp_path, pool, format_version, None, rows, 10)
    with pytest.raises(PySparkException) as raised:
        session.sql(MOR_MERGE + NOT_MATCHED_BY_SOURCE).collect()
    _assert_refusal(str(raised.value), "merge-on-read MERGE", MERGE_KEY)
    session.stop()


@pytest.mark.skipif(not SCALE, reason=SCALE_SKIP)
@pytest.mark.parametrize("format_version", FORMATS)
def test_scale_merge_commits_at_8_gib(tmp_path: Path, format_version: int) -> None:
    """The 4x MERGE with its NOT MATCHED BY SOURCE leg commits at an 8 GiB pool."""
    session = _merge_target(tmp_path, "8192M", format_version, None, 4_000_000, 10)
    session.sql(MOR_MERGE + NOT_MATCHED_BY_SOURCE).collect()
    untouched = session.sql(f"SELECT count(*) FROM {TABLE} WHERE c = -1").collect()[0][0]
    assert untouched == 3_600_000
    session.stop()


@pytest.mark.skipif(not SCALE, reason=SCALE_SKIP)
@pytest.mark.parametrize("format_version", FORMATS)
@pytest.mark.parametrize("rows", [4_000_000, 16_000_000])
def test_scale_changelog_refuses_at_32_mib(tmp_path: Path, format_version: int, rows: int) -> None:
    """The 4x and 16x changelog reads refuse at a 32 MiB pool."""
    session = _changelog(tmp_path, "32M", format_version, rows)
    with pytest.raises(PySparkException) as raised:
        session.sql("SELECT id FROM t_changes LIMIT 10").collect()
    _assert_refusal(str(raised.value), "changelog read", CHANGELOG_KEY)
    session.stop()


@pytest.mark.skipif(not SCALE, reason=SCALE_SKIP)
@pytest.mark.parametrize("format_version", FORMATS)
def test_scale_changelog_answers_at_8_gib(tmp_path: Path, format_version: int) -> None:
    """The 4x changelog read answers at an 8 GiB pool."""
    session = _changelog(tmp_path, "8192M", format_version, 4_000_000)
    assert session.sql("SELECT count(id) FROM t_changes").collect()[0][0] == 4_000_000
    session.stop()
