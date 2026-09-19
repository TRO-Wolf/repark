"""ICE-PAGE-PRUNE-1 — page-pruning scans answer Spark row for row, both doors.

Oracle: ``fixtures/ice_page_prune_1/truth.json`` beside the fixtures, recorded
from live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 by
``_record_ice_page_prune_1.py`` (re-run verifies, ``--rewrite`` re-records;
row lists are id runs, row-id and sequence segments on v3, decoded by
``expand_cell``). The five Spark-written tables (``base_v2``, ``base_v3``,
``del_v2`` with position deletes, ``del_v3`` with deletion vectors plus an
update, ``evo_v2`` with promotions, a rename, a drop/re-add and an add) carry
many pages per file (``page-row-limit`` 100); the pins hold on today's
pruning-off engine and must stay green when page selection turns on. The
rewritten ``del_v2`` position-delete files disagree with their manifest sizes
(``rewrite_table_path`` rewrote the bytes, not the sizes), so every ``del_v2``
read fails loud with a parquet-metadata error while Spark answers — pinned as
a divergence, with the recorded Spark answers in ``truth.json`` as the fix
target.

Every assertion runs on the Arrow path (id values AND the ``int64`` Arrow
type, lineage ``int64`` on v3) on both doors: ``session.sql`` carries the
predicate grid except the three bare-decimal range cells on the NaN-holding
``d`` column, which refuse loud under ICE-NAN-DECIMAL-LITERAL-1 (BACKLOG) and
are pinned on their needle; the DataFrame door carries every cell through
``table().filter().select("id").orderBy("id").to_arrow()`` with double bounds
(the frame schema does not resolve the v3 lineage columns — SQL-door-only
today — so lineage rides the SQL door). The live tier re-derives the answers
from live Spark under ``REPARK_PARITY_LIVE=1`` and asserts
repark == pinned truth == live Spark.

RePark-written tables (300,000 rows; full files carry four pages per narrow
column and six on the wide string column) prove self-consistency past a
merge-on-read DELETE and an UPDATE: every selective predicate equals the
unfiltered read filtered in Python on both doors, and on v3 every surviving
row keeps its unfiltered ``_row_id`` and ``_last_updated_sequence_number``.

pins: ice-page-prune-1/C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_ice_page_prune_1 import (
    EVO_QUERIES,
    PREFIX,
    QUERIES,
    _apply_deletes,
    _build_evo,
    _seed_table,
    expand_cell,
)

_HERE = Path(__file__).resolve().parent
_TRUTH = json.loads(
    (_HERE / "fixtures" / "ice_page_prune_1" / "truth.json").read_text(encoding="utf-8")
)["answers"]
_FIXTURE_SRC = _HERE / "fixtures" / "ice_page_prune_1"
_CANONICAL_ROOT = Path("/tmp/repark-ice-page-prune-1/wh")
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "ice_page_prune_1"
_NAMESPACE = "ns"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_TABLES = ("base_v2", "base_v3", "del_v3", "evo_v2")
_DELETE_TABLES = ("del_v2",)
_LINEAGE_TABLES = frozenset({"base_v3", "del_v3"})
_DECIMAL_RANGE_CELLS = ("d_lt", "d_gt", "d_not_lt")
_DECIMAL_RANGE_NEEDLE = "Overflowing on NaN"
_DELETE_READ_NEEDLE = "Failed to load Parquet metadata"


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(f"fixture lock {path} held for 2 minutes") from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize(table: str) -> Iterator[Path]:
    """Copy one fixture warehouse to its canonical path; yield newest metadata file."""
    dest = _CANONICAL_ROOT / "ns" / table
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(_FIXTURE_SRC / table, dest)
        versions = sorted(
            (dest / "metadata").glob("v*.metadata.json"),
            key=lambda path: int(path.name[1:].split(".", 1)[0]),
        )
        assert versions, f"no Hadoop metadata under {dest}/metadata"
        yield versions[-1]
    finally:
        with suppress(OSError):
            if dest.exists():
                shutil.rmtree(dest)
        lock.close()


def _new_session(app: str) -> Any:
    """A RePark session allowed to create v3 tables."""
    from repark import ReparkSession

    return ReparkSession.builder.appName(app).config(_ALLOW_CREATE_V3_KEY, "true").getOrCreate()


def _register(session: Any, table_arg: str, metadata_file: Path) -> None:
    """Adopt one Hadoop metadata file into the memory catalog."""
    session.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => '{table_arg}', metadata_file => '{metadata_file}')"
    )


def _table_rows(table: pa.Table, lineage: bool) -> list[list[int]]:
    """Answer rows as plain ints, failing unless the Arrow types are ``int64``."""
    assert table.schema.field("id").type == pa.int64(), table.schema
    ids = [int(value) for value in table.column("id").to_pylist()]
    if not lineage:
        return [[value] for value in ids]
    assert table.schema.field("_row_id").type == pa.int64(), table.schema
    assert table.schema.field("_last_updated_sequence_number").type == pa.int64(), table.schema
    row_ids = [int(value) for value in table.column("_row_id").to_pylist()]
    sequences = [int(value) for value in table.column("_last_updated_sequence_number").to_pylist()]
    return [
        [value, row_id, sequence]
        for value, row_id, sequence in zip(ids, row_ids, sequences, strict=True)
    ]


def _queries(table: str) -> dict[str, str]:
    """The predicate grid of one table."""
    return EVO_QUERIES if table == "evo_v2" else QUERIES


def _columns(lineage: bool) -> str:
    """The SELECT list, with lineage on v3."""
    return "id, _row_id, _last_updated_sequence_number" if lineage else "id"


def test_sql_door_answers_every_recorded_cell() -> None:
    """The SQL door equals Spark on every predicate, lineage, and unfiltered cell."""
    session = _new_session("ice-page-prune-1-sql")
    try:
        session.register_memory_catalog(_CATALOG, Path("/tmp/repark-ice-page-prune-1-mem"))
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for table in _TABLES:
            with _materialize(table) as metadata_file:
                table_arg = f"{_NAMESPACE}.{table}"
                _register(session, table_arg, metadata_file)
                qualified = f"{_CATALOG}.{table_arg}"
                lineage = table in _LINEAGE_TABLES
                columns = _columns(lineage)
                for cell, predicate in _queries(table).items():
                    if cell in _DECIMAL_RANGE_CELLS:
                        continue
                    arrow = session.sql(
                        f"SELECT {columns} FROM {qualified} WHERE {predicate} ORDER BY id"
                    ).to_arrow()
                    assert _table_rows(arrow, lineage) == expand_cell(_TRUTH[table][cell]), (
                        table,
                        cell,
                    )
                arrow = session.sql(f"SELECT {columns} FROM {qualified} ORDER BY id").to_arrow()
                assert _table_rows(arrow, lineage) == expand_cell(_TRUTH[table]["_unfiltered"]), (
                    table,
                    "_unfiltered",
                )
    finally:
        session.stop()


def test_sql_door_rewritten_position_deletes_refuse_loud() -> None:
    """Rewritten v2 position deletes fail loud on every read, never silently.

    ``rewrite_table_path`` rewrote the delete-file bytes without updating the
    manifest sizes; RePark sizes its reads from the manifest while Spark reads
    the footer, so RePark refuses and Spark answers the recorded rows. The
    sub-message races with the scan (corrupt footer or short read); the stable
    needle is the metadata-load failure. The recorded Spark answers in
    ``truth.json`` are the fix target.
    """
    session = _new_session("ice-page-prune-1-deletes")
    try:
        session.register_memory_catalog(_CATALOG, Path("/tmp/repark-ice-page-prune-1-mem"))
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for table in _DELETE_TABLES:
            with _materialize(table) as metadata_file:
                table_arg = f"{_NAMESPACE}.{table}"
                _register(session, table_arg, metadata_file)
                qualified = f"{_CATALOG}.{table_arg}"
                for query in (
                    f"SELECT count(*) FROM {qualified}",
                    f"SELECT id FROM {qualified} WHERE {_queries(table)['id_range']} ORDER BY id",
                ):
                    with pytest.raises(Exception) as raised:
                        session.sql(query).to_arrow()
                    assert _DELETE_READ_NEEDLE in str(raised.value), (
                        table,
                        query[:60],
                        str(raised.value)[:200],
                    )
    finally:
        session.stop()


def test_sql_door_bare_decimal_ranges_refuse_loud() -> None:
    """ICE-NAN-DECIMAL-LITERAL-1 (BACKLOG): bare decimals on NaN ``d`` fail loud.

    The pins codify today's needle so the fix reds them on purpose; the
    recorded Spark answers in ``truth.json`` are the fix target.
    """
    session = _new_session("ice-page-prune-1-decimal")
    try:
        session.register_memory_catalog(_CATALOG, Path("/tmp/repark-ice-page-prune-1-mem"))
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for table in ("base_v2", "base_v3", "del_v3"):
            with _materialize(table) as metadata_file:
                table_arg = f"{_NAMESPACE}.{table}"
                _register(session, table_arg, metadata_file)
                qualified = f"{_CATALOG}.{table_arg}"
                for cell in _DECIMAL_RANGE_CELLS:
                    with pytest.raises(Exception) as raised:
                        session.sql(
                            f"SELECT id FROM {qualified} WHERE {_queries(table)[cell]}"
                        ).to_arrow()
                    assert _DECIMAL_RANGE_NEEDLE in str(raised.value), (
                        table,
                        cell,
                        str(raised.value)[:200],
                    )
    finally:
        session.stop()


def _frame_filter(frame: Any, cell: str) -> Any:
    """One predicate cell as a DataFrame filter."""
    from repark import functions as F  # noqa: N812 — PySpark idiom

    if cell == "id_range":
        return frame.filter(F.col("id").between(400, 520))
    if cell == "id_eq":
        return frame.filter(F.col("id") == 1234)
    if cell == "id_lt":
        return frame.filter(F.col("id") < 150)
    if cell == "i_gt":
        return frame.filter(F.col("i") > 1800)
    if cell == "s_eq":
        return frame.filter(F.col("s") == f"{PREFIX}001234")
    if cell == "s_ge":
        return frame.filter(F.col("s") >= f"{PREFIX}001900")
    if cell == "s_starts":
        return frame.filter(F.col("s").like(f"{PREFIX}0019%"))
    if cell == "s_not_starts":
        return frame.filter(~F.col("s").like(f"{PREFIX}0%"))
    if cell == "n_is_null":
        return frame.filter(F.col("n").isNull())
    if cell == "n_not_null":
        return frame.filter(F.col("n").isNotNull())
    if cell == "n_eq":
        return frame.filter(F.col("n") == 900)
    if cell == "n_ne":
        return frame.filter(F.col("n") != 900)
    if cell == "n_not_in":
        return frame.filter(~F.col("n").isin(900, 901))
    if cell == "d_isnan":
        return frame.filter(F.isnan(F.col("d")))
    if cell == "d_not_nan":
        return frame.filter(~F.isnan(F.col("d")))
    if cell == "d_lt":
        return frame.filter(F.col("d") < 100.0)
    if cell == "d_gt":
        return frame.filter(F.col("d") > 1000.0)
    if cell == "d_not_lt":
        return frame.filter(~(F.col("d") < 1000.0))
    if cell == "f_gt":
        return frame.filter(F.col("f") > 2000.0)
    if cell == "ts_range":
        return frame.filter(F.expr(QUERIES["ts_range"]))
    if cell == "i_promoted_gt":
        return frame.filter(F.col("i") > 1800)
    if cell == "i_promoted_big":
        return frame.filter(F.col("i") > 3000000000)
    if cell == "f_promoted_gt":
        return frame.filter(F.col("f") > 2000.0)
    if cell == "dec_promoted_gt":
        return frame.filter(F.col("dec") > 1500.00)
    if cell == "renamed_eq":
        return frame.filter(F.col("s2") == f"{PREFIX}001234")
    if cell == "added_is_null":
        return frame.filter(F.col("addc").isNull())
    if cell == "added_eq":
        return frame.filter(F.col("addc") == 7)
    if cell == "added_not_null":
        return frame.filter(F.col("addc").isNotNull())
    if cell == "readded_is_null":
        return frame.filter(F.col("n").isNull())
    if cell == "readded_eq":
        return frame.filter(F.col("n") == 900)
    raise AssertionError(f"unknown cell {cell}")


def test_dataframe_door_answers_every_recorded_cell() -> None:
    """The DataFrame door equals Spark on every predicate and unfiltered cell."""
    session = _new_session("ice-page-prune-1-frame")
    try:
        session.register_memory_catalog(_CATALOG, Path("/tmp/repark-ice-page-prune-1-mem"))
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for table in _TABLES:
            with _materialize(table) as metadata_file:
                table_arg = f"{_NAMESPACE}.{table}"
                _register(session, table_arg, metadata_file)
                frame = session.table(f"{_CATALOG}.{table_arg}")
                for cell in _queries(table):
                    arrow = _frame_filter(frame, cell).select("id").orderBy("id").to_arrow()
                    assert _table_rows(arrow, False) == [
                        [row[0]] for row in expand_cell(_TRUTH[table][cell])
                    ], (table, cell)
                arrow = frame.select("id").orderBy("id").to_arrow()
                assert _table_rows(arrow, False) == [
                    [row[0]] for row in expand_cell(_TRUTH[table]["_unfiltered"])
                ], (table, "_unfiltered")
    finally:
        session.stop()


_BIG_ROWS = 300000
_BIG_PREFIX = "p" * 80
_BIG_COLUMNS = ("id", "i", "s", "n", "d")
_BIG_QUERIES: dict[str, str] = {
    "id_range": "id BETWEEN 40000 AND 40100",
    "id_eq": "id = 123456",
    "i_gt": "i > 290000",
    "n_is_null": "n IS NULL",
    "s_starts": f"s LIKE '{_BIG_PREFIX}1500%'",
    "d_lt": "d >= CAST(20000.0 AS DOUBLE) AND d < CAST(20100.0 AS DOUBLE)",
    "d_not_nan": "NOT isnan(d)",
    "n_eq": "n = 150001",
}


def _match_id_range(row: dict[str, Any]) -> bool:
    """Whether a row falls in the narrow id band."""
    return 40000 <= row["id"] <= 40100


def _match_id_eq(row: dict[str, Any]) -> bool:
    """Whether a row carries the equality needle."""
    return row["id"] == 123456


def _match_i_gt(row: dict[str, Any]) -> bool:
    """Whether a row passes the tail range, updated rows included."""
    return row["i"] > 290000


def _match_n_is_null(row: dict[str, Any]) -> bool:
    """Whether a row sits in the all-null band or the scattered nulls."""
    return row["n"] is None


def _match_s_starts(row: dict[str, Any]) -> bool:
    """Whether a row carries the recorded string prefix."""
    return row["s"].startswith(_BIG_PREFIX + "1500")


def _match_d_lt(row: dict[str, Any]) -> bool:
    """Whether a row passes the double range, NaN never passing."""
    value = row["d"]
    return value is not None and value == value and 20000.0 <= value < 20100.0


def _match_d_not_nan(row: dict[str, Any]) -> bool:
    """Whether a row holds a real double, NaN and NULL failing."""
    value = row["d"]
    return value is not None and value == value


def _match_n_eq(row: dict[str, Any]) -> bool:
    """Whether a row carries the nullable-column needle."""
    return row["n"] == 150001


_BIG_MATCHERS: dict[str, Callable[[dict[str, Any]], bool]] = {
    "id_range": _match_id_range,
    "id_eq": _match_id_eq,
    "i_gt": _match_i_gt,
    "n_is_null": _match_n_is_null,
    "s_starts": _match_s_starts,
    "d_lt": _match_d_lt,
    "d_not_nan": _match_d_not_nan,
    "n_eq": _match_n_eq,
}


def _normalize(value: Any) -> Any:
    """Plain values with NaN folded to a comparable token."""
    if isinstance(value, float) and value != value:
        return "NaN"
    return value


def _normalize_row(row: dict[str, Any], names: list[str]) -> list[Any]:
    """One row as normalized values in column order."""
    return [_normalize(row[name]) for name in names]


def _filter_expected(rows: list[dict[str, Any]], names: list[str], cell: str) -> list[list[Any]]:
    """The unfiltered rows one predicate keeps, matched raw then normalized."""
    matcher = _BIG_MATCHERS[cell]
    ordered = sorted(rows, key=lambda row: row["id"])
    return [_normalize_row(row, names) for row in ordered if matcher(row)]


def _seed_big_table(session: Any, table: str, version: str) -> None:
    """Create and seed one 300,000-row table, then DELETE and UPDATE it."""
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, i INT, s STRING, n INT, d DOUBLE) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(
        f"INSERT INTO {table} SELECT id AS id, CAST(id AS INT) AS i, "
        f"concat('{_BIG_PREFIX}', lpad(CAST(id AS STRING), 6, '0')) AS s, "
        "CASE WHEN id >= 100000 AND id < 120000 THEN NULL "
        "WHEN id % 10 = 0 THEN NULL ELSE CAST(id AS INT) END AS n, "
        "CASE WHEN id >= 200000 AND id < 210000 THEN CAST('NaN' AS DOUBLE) "
        f"ELSE CAST(id AS DOUBLE) END AS d FROM range({_BIG_ROWS})"
    )
    session.sql(f"DELETE FROM {table} WHERE id < 16384 OR (id % 13 = 0 AND id < 60000)")
    session.sql(f"UPDATE {table} SET i = i + 1000000 WHERE id BETWEEN 150000 AND 150020")


def _big_unfiltered(session: Any, table: str, lineage: bool) -> list[dict[str, Any]]:
    """Every surviving row after the DELETE and UPDATE legs."""
    columns = ", ".join(_BIG_COLUMNS)
    if lineage:
        columns += ", _row_id, _last_updated_sequence_number"
    return session.sql(f"SELECT {columns} FROM {table} ORDER BY id").to_arrow().to_pylist()


def _big_frame_filter(frame: Any, cell: str) -> Any:
    """One self-consistency cell as a DataFrame filter."""
    from repark import functions as F  # noqa: N812 — PySpark idiom

    if cell == "id_range":
        return frame.filter(F.col("id").between(40000, 40100))
    if cell == "id_eq":
        return frame.filter(F.col("id") == 123456)
    if cell == "i_gt":
        return frame.filter(F.col("i") > 290000)
    if cell == "n_is_null":
        return frame.filter(F.col("n").isNull())
    if cell == "s_starts":
        return frame.filter(F.col("s").like(f"{_BIG_PREFIX}1500%"))
    if cell == "d_lt":
        return frame.filter((F.col("d") >= 20000.0) & (F.col("d") < 20100.0))
    if cell == "d_not_nan":
        return frame.filter(~F.isnan(F.col("d")))
    if cell == "n_eq":
        return frame.filter(F.col("n") == 150001)
    raise AssertionError(f"unknown cell {cell}")


def _assert_big_cells(session: Any, table: str, lineage: bool) -> None:
    """Fail unless every predicate equals the unfiltered read filtered in Python."""
    names = list(_BIG_COLUMNS) + (["_row_id", "_last_updated_sequence_number"] if lineage else [])
    rows = _big_unfiltered(session, table, lineage)
    assert len(rows) < _BIG_ROWS, "the DELETE leg removed rows"
    columns = ", ".join(names)
    for cell, predicate in _BIG_QUERIES.items():
        arrow = session.sql(f"SELECT {columns} FROM {table} WHERE {predicate} ORDER BY id")
        got = [[_normalize(row[name]) for name in names] for row in arrow.to_arrow().to_pylist()]
        assert got == _filter_expected(rows, names, cell), (table, cell, "sql")
    frame = session.table(table)
    frame_names = list(_BIG_COLUMNS)
    for cell in _BIG_QUERIES:
        arrow = _big_frame_filter(frame, cell).select(*frame_names).orderBy("id").to_arrow()
        got = [[_normalize(row[name]) for name in frame_names] for row in arrow.to_pylist()]
        assert got == _filter_expected(rows, frame_names, cell), (table, cell, "frame")


def test_repark_written_v2_answers_its_own_reads(tmp_path: Path) -> None:
    """Filtered v2 reads equal the unfiltered read filtered in Python, both doors."""
    session = _new_session("ice-page-prune-1-big-v2")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        table = f"{_CATALOG}.{_NAMESPACE}.big_v2"
        _seed_big_table(session, table, "2")
        _assert_big_cells(session, table, False)
    finally:
        session.stop()


def test_repark_written_v3_answers_its_own_reads_with_stable_lineage(tmp_path: Path) -> None:
    """Filtered v3 reads keep each surviving row's unfiltered lineage, both doors."""
    session = _new_session("ice-page-prune-1-big-v3")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        table = f"{_CATALOG}.{_NAMESPACE}.big_v3"
        _seed_big_table(session, table, "3")
        _assert_big_cells(session, table, True)
    finally:
        session.stop()


def test_repark_written_files_carry_page_indexes(tmp_path: Path) -> None:
    """Every column chunk of every written file carries a column and offset index."""
    session = _new_session("ice-page-prune-1-indexes")
    try:
        session.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        table = f"{_CATALOG}.{_NAMESPACE}.big_idx"
        _seed_big_table(session, table, "2")
        files = session.sql(f"SELECT file_path FROM {table}.files").to_arrow()
        paths = [row["file_path"] for row in files.to_pylist()]
        assert len(paths) > 4, "data plus position-delete files"
        import pyarrow.parquet as pq

        for path in paths:
            meta = pq.ParquetFile(path).metadata
            for group in range(meta.num_row_groups):
                for column in range(meta.num_columns):
                    chunk = meta.row_group(group).column(column)
                    assert chunk.has_column_index and chunk.has_offset_index, (path, group, column)
    finally:
        session.stop()


_LIVE_CATALOG = "ice_page_prune_1_live"


def _build_live_tables(spark: Any, catalog: str) -> dict[str, str]:
    """Seed the five live tables from the recorder's seed path; return key to name."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    names: dict[str, str] = {}
    for version in ("2", "3"):
        base = f"{catalog}.ns.base_v{version}"
        _seed_table(spark, base, version)
        names[f"base_v{version}"] = base
        delete_table = f"{catalog}.ns.del_v{version}"
        _seed_table(spark, delete_table, version)
        _apply_deletes(spark, delete_table, version)
        names[f"del_v{version}"] = delete_table
    evo = f"{catalog}.ns.evo_v2"
    _build_evo(spark, evo)
    names["evo_v2"] = evo
    return names


def _live_rows(spark: Any, table: str, predicate: str | None, lineage: bool) -> list[list[int]]:
    """Sorted answer rows of one live Spark read leg as plain ints."""
    extra = ", _row_id, _last_updated_sequence_number" if lineage else ""
    where = "" if predicate is None else f" WHERE {predicate}"
    query = f"SELECT id{extra} FROM {table}{where} ORDER BY id"
    return [[int(value) for value in row] for row in spark.sql(query).collect()]


def _newest_live_metadata(warehouse: Path, table: str) -> Path:
    """Newest Hadoop metadata file of one live table."""
    root = warehouse / "ns" / table.split(".")[-1]
    versions = sorted(
        (root / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    assert versions, f"no Hadoop metadata under {root}/metadata"
    return versions[-1]


def _assert_live_grid(spark: Any, live_names: dict[str, str]) -> None:
    """Fail unless live Spark re-derives every recorded truth cell (oracle drift)."""
    for key, table in live_names.items():
        lineage = key in _LINEAGE_TABLES
        legs = [(cell, predicate) for cell, predicate in _queries(key).items()]
        legs.append(("_unfiltered", None))
        for cell, predicate in legs:
            got = _live_rows(spark, table, predicate, lineage)
            assert got == expand_cell(_TRUTH[key][cell]), ("oracle drift", key, cell)


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_grid_replays_spark(tmp_path: Path) -> None:
    """Live Spark re-derives the grid; RePark matches truth and live on both doors."""
    import _live_parity as lp

    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog=_LIVE_CATALOG).session
    live_names = _build_live_tables(spark, _LIVE_CATALOG)
    _assert_live_grid(spark, live_names)
    repark = _new_session("ice-page-prune-1-live")
    try:
        repark.register_memory_catalog(_CATALOG, tmp_path / "repark-warehouse")
        repark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        for key, live_table in live_names.items():
            table_arg = f"{_NAMESPACE}.live_{key.replace('/', '_')}"
            _register(repark, table_arg, _newest_live_metadata(warehouse, live_table))
            qualified = f"{_CATALOG}.{table_arg}"
            lineage = key in _LINEAGE_TABLES
            columns = _columns(lineage)
            for cell, predicate in _queries(key).items():
                if cell in _DECIMAL_RANGE_CELLS:
                    continue
                arrow = repark.sql(
                    f"SELECT {columns} FROM {qualified} WHERE {predicate} ORDER BY id"
                ).to_arrow()
                assert _table_rows(arrow, lineage) == expand_cell(_TRUTH[key][cell]), (
                    "live",
                    key,
                    cell,
                )
            arrow = repark.sql(f"SELECT {columns} FROM {qualified} ORDER BY id").to_arrow()
            assert _table_rows(arrow, lineage) == expand_cell(_TRUTH[key]["_unfiltered"]), (
                "live",
                key,
                "_unfiltered",
            )
    finally:
        repark.stop()
