"""ICE-RM-DELETES-1 rewrite_manifests pins against the recorded Spark oracle."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException

FIXTURE_PATH = Path(__file__).with_name("ice_rm_deletes_1_spark_oracle.json")
FIXTURE2_PATH = Path(__file__).with_name("ice_rm_deletes_1_spark_oracle2.json")

_SEEDS = (
    "INSERT INTO {t} VALUES (1, 'x', 'a'), (2, 'y', 'b')",
    "INSERT INTO {t} VALUES (3, 'x', 'c'), (4, 'y', 'd')",
    "INSERT INTO {t} VALUES (5, 'x', 'e'), (6, 'z', 'f')",
)
_DEL_1 = "DELETE FROM {t} WHERE id = 1"
_DEL_4 = "DELETE FROM {t} WHERE id = 4"
_DEL_5 = "DELETE FROM {t} WHERE id = 5"
_DEL_7 = "DELETE FROM {t} WHERE id = 7"
_DEL_8 = "DELETE FROM {t} WHERE id = 8"
_DEL_9 = "DELETE FROM {t} WHERE id = 9"
_INSERT_4 = "INSERT INTO {t} VALUES (7, 'x', 'g'), (8, 'x', 'h'), (9, 'y', 'i'), (10, 'y', 'j')"
_INSERT_9 = "INSERT INTO {t} VALUES (9, 'x', 'q')"
_EVOLVE = "ALTER TABLE {t} ADD PARTITION FIELD bucket(2, id)"
_PART = "PARTITIONED BY (cat)"
_MOR = "'write.delete.mode' = 'merge-on-read'"

_BUILDS: dict[str, dict[str, Any]] = {
    "unpart_mor": {"part": "", "dml": [_DEL_1, _DEL_4], "call": ""},
    "part_mor": {"part": _PART, "dml": [_DEL_1, _DEL_4, _DEL_5], "call": ""},
    "part_mor_spec": {"part": _PART, "dml": [_DEL_1, _DEL_4], "call": ", spec_id => 0"},
    "part_mor_nocache": {
        "part": _PART,
        "dml": [_DEL_1, _DEL_4],
        "call": ", use_caching => false",
    },
    "no_deletes": {"part": _PART, "dml": [], "call": ""},
    "evolved_spec": {
        "part": _PART,
        "dml": [_DEL_1, _EVOLVE, _INSERT_9, _DEL_9],
        "call": "",
    },
    "part_mor_real": {
        "part": _PART,
        "dml": [_INSERT_4, _DEL_7, _DEL_9, _DEL_8],
        "call": "",
        "oracle2": True,
    },
}


def _fixture() -> dict[str, Any]:
    """Load the recorded Spark oracle cells."""
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _fixture2() -> dict[str, Any]:
    """Load the recorded Spark oracle cells with real delete manifests."""
    return json.loads(FIXTURE2_PATH.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _session(warehouse: Path) -> ReparkSession:
    """Return a RePark session with a memory catalog at `warehouse`."""
    session = (
        ReparkSession.builder.appName("ice-rm-deletes-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", warehouse)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _replay(session: ReparkSession, table: str, build: dict[str, Any], version: str) -> None:
    """Create `table` and run one cell's seeds plus DML through RePark."""
    part = str(build["part"])
    session.sql(
        f"CREATE TABLE {table} (id INT, cat STRING, v STRING) USING iceberg {part} "
        f"TBLPROPERTIES ('format-version' = '{version}', {_MOR})"
    )
    for stmt in (*_SEEDS, *[str(raw) for raw in build["dml"]]):
        session.sql(stmt.format(t=table)).collect()


def _result_row(session: ReparkSession, sql: str) -> tuple[int, int]:
    """Run one CALL and return its rewritten/added counts."""
    batch = session.sql(sql).to_arrow()
    return (
        int(batch.column("rewritten_manifests_count")[0].as_py()),
        int(batch.column("added_manifests_count")[0].as_py()),
    )


def _layout(session: ReparkSession, table: str) -> list[tuple[int, int, int, int]]:
    """Return per-manifest (content, spec, data files, delete files), ordered."""
    batch = session.sql(
        f"SELECT content, partition_spec_id, "
        f"added_data_files_count + existing_data_files_count AS data_files, "
        f"added_delete_files_count + existing_delete_files_count AS delete_files "
        f"FROM {table}.manifests ORDER BY content, partition_spec_id"
    ).to_arrow()
    shaped = [
        (int(content.as_py()), int(spec.as_py()), int(data.as_py()), int(deleted.as_py()))
        for content, spec, data, deleted in zip(
            batch.column("content"),
            batch.column("partition_spec_id"),
            batch.column("data_files"),
            batch.column("delete_files"),
            strict=True,
        )
    ]
    return sorted(shaped)


def _live_ids(session: ReparkSession, table: str) -> list[int]:
    """Return the table's live ids in order."""
    batch = session.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    return [int(value.as_py()) for value in batch.column("id")]


def _last_op(session: ReparkSession, table: str) -> tuple[int, str]:
    """Return the snapshot count and the current snapshot's operation."""
    batch = session.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at").to_arrow()
    ops = [str(op.as_py()) for op in batch.column("operation")]
    return (len(ops), ops[-1])


def _cell(name: str, version: str) -> dict[str, Any]:
    """Return one oracle cell by name and format version."""
    key = f"{name}_v{version}"
    cell = _fixture2()[key] if name == "part_mor_real" else _fixture()[key]
    assert isinstance(cell, dict)
    return cell


def _check_rows(name: str, version: str, table: str, session: ReparkSession) -> None:
    """Assert the surviving rows equal the recorded Spark rows."""
    want = [row[0] for row in _cell(name, version)["rows"]]
    assert _live_ids(session, table) == want, f"{name}_v{version} rows"


def _ref(table: str) -> str:
    """Return the CALL table argument (namespace plus name, no catalog)."""
    return table.split(".", 1)[1]


def _check_result(name: str, version: str, session: ReparkSession, table: str) -> None:
    """Run one cell's CALL and assert the recorded result row."""
    build = _BUILDS[name]
    call = f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}'{build['call']})"
    want = _cell(name, version)["result"][0]
    assert _result_row(session, call) == (want[0], want[1]), f"{name}_v{version} result"


@pytest.mark.parametrize("version", ["2", "3"])
def test_unpart_mor_matches_spark_end_to_end(tmp_path: Path, version: str) -> None:
    """The unpartitioned MoR cells match Spark exactly: layout, counts, rows, op."""
    session = _session(tmp_path)
    table = f"mem.ns.unpart_mor_v{version}"
    _replay(session, table, _BUILDS["unpart_mor"], version)
    assert _layout(session, table) == [(0, 0, 1, 0)] * 3 + [(1, 0, 0, 1)] * 2
    _check_result("unpart_mor", version, session, table)
    assert _layout(session, table) == [(0, 0, 3, 0), (1, 0, 0, 2)]
    _check_rows("unpart_mor", version, table, session)
    assert _last_op(session, table)[1] == "replace"
    session.stop()


@pytest.mark.parametrize("version", ["2", "3"])
def test_no_deletes_matches_spark_end_to_end(tmp_path: Path, version: str) -> None:
    """The data-only cells match Spark exactly: layout, counts, rows, op."""
    session = _session(tmp_path)
    table = f"mem.ns.no_deletes_v{version}"
    _replay(session, table, _BUILDS["no_deletes"], version)
    assert _layout(session, table) == [(0, 0, 2, 0)] * 3
    _check_result("no_deletes", version, session, table)
    assert _layout(session, table) == [(0, 0, 6, 0)]
    _check_rows("no_deletes", version, table, session)
    assert _last_op(session, table)[1] == "replace"
    session.stop()


@pytest.mark.parametrize("version", ["2", "3"])
def test_part_mor_rewrites_both_legs(tmp_path: Path, version: str) -> None:
    """The partitioned cells merge both legs; rows follow the recorded cells."""
    session = _session(tmp_path)
    table = f"mem.ns.part_mor_v{version}"
    _replay(session, table, _BUILDS["part_mor"], version)
    before = _layout(session, table)
    assert len(before) == 6, f"part_mor_v{version} before: {before}"
    got = _result_row(session, f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}')")
    assert got == (6, 2), f"part_mor_v{version} result: {got}"
    after = _layout(session, table)
    assert len(after) == 2, f"part_mor_v{version} after: {after}"
    assert [entry[0] for entry in after] == [0, 1]
    _check_rows("part_mor", version, table, session)
    assert _last_op(session, table)[1] == "replace"
    session.stop()


@pytest.mark.parametrize("version", ["2", "3"])
def test_part_mor_spec_honours_spec_id(tmp_path: Path, version: str) -> None:
    """An explicit current spec id rewrites like the default call."""
    session = _session(tmp_path)
    table = f"mem.ns.part_mor_spec_v{version}"
    _replay(session, table, _BUILDS["part_mor_spec"], version)
    got = _result_row(
        session, f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}', spec_id => 0)"
    )
    assert got == (5, 2), f"part_mor_spec_v{version} result: {got}"
    assert _layout(session, table) == [(0, 0, 6, 0), (1, 0, 0, 2)]
    _check_rows("part_mor_spec", version, table, session)
    assert _last_op(session, table)[1] == "replace"
    session.stop()


@pytest.mark.parametrize("version", ["2", "3"])
def test_part_mor_nocache_matches_cached(tmp_path: Path, version: str) -> None:
    """`use_caching => false` answers like the default call on the same shape."""
    session = _session(tmp_path)
    table = f"mem.ns.part_mor_nocache_v{version}"
    _replay(session, table, _BUILDS["part_mor_nocache"], version)
    call = f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}', use_caching => false)"
    got = _result_row(session, call)
    assert got == (5, 2), f"part_mor_nocache_v{version} result: {got}"
    assert _layout(session, table) == [(0, 0, 6, 0), (1, 0, 0, 2)]
    _check_rows("part_mor_nocache", version, table, session)
    assert _last_op(session, table)[1] == "replace"
    session.stop()


@pytest.mark.parametrize("version", ["2", "3"])
def test_evolved_spec_default_is_a_no_op(tmp_path: Path, version: str) -> None:
    """The default call on the evolved table answers zeros and commits nothing."""
    session = _session(tmp_path)
    table = f"mem.ns.evolved_spec_v{version}"
    _replay(session, table, _BUILDS["evolved_spec"], version)
    before = _layout(session, table)
    snapshots_before = _last_op(session, table)[0]
    got = _result_row(session, f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}')")
    assert got == (0, 0), f"evolved_spec_v{version} result: {got}"
    assert _last_op(session, table) == (snapshots_before, "delete")
    assert _layout(session, table) == before
    _check_rows("evolved_spec", version, table, session)
    session.stop()


@pytest.mark.parametrize("version", ["2", "3"])
def test_part_mor_real_rewrites_both_legs(tmp_path: Path, version: str) -> None:
    """The real-delete-shape cells merge both legs; rows follow the recorded cells."""
    session = _session(tmp_path)
    table = f"mem.ns.part_mor_real_v{version}"
    _replay(session, table, _BUILDS["part_mor_real"], version)
    before = _layout(session, table)
    assert len(before) == 7, f"part_mor_real_v{version} before: {before}"
    got = _result_row(session, f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}')")
    assert got == (7, 2), f"part_mor_real_v{version} result: {got}"
    after = _layout(session, table)
    assert len(after) == 2, f"part_mor_real_v{version} after: {after}"
    assert [entry[0] for entry in after] == [0, 1]
    _check_rows("part_mor_real", version, table, session)
    assert _last_op(session, table)[1] == "replace"
    session.stop()


def test_unknown_spec_id_refuses(tmp_path: Path) -> None:
    """An unknown spec id raises IllegalArgumentException naming the reference."""
    session = _session(tmp_path)
    table = "mem.ns.unknown_spec"
    _replay(session, table, _BUILDS["part_mor_spec"], "2")
    with pytest.raises(IllegalArgumentException, match="Invalid spec id 99"):
        session.sql(f"CALL mem.system.rewrite_manifests(table => '{_ref(table)}', spec_id => 99)")
    session.stop()


def test_positional_spec_id_runs(tmp_path: Path) -> None:
    """The positional spec id spelling reaches the same engine as the named one."""
    session = _session(tmp_path)
    table = "mem.ns.positional_spec"
    _replay(session, table, _BUILDS["part_mor_spec"], "2")
    got = _result_row(session, f"CALL mem.system.rewrite_manifests('{_ref(table)}', true, 0)")
    assert got == (5, 2), f"positional spec result: {got}"
    session.stop()
