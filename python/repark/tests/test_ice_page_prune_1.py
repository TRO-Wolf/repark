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

pins: ice-page-prune-1/C-003, C-004
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_ice_page_prune_1 import (
    EVO_QUERIES,
    PREFIX,
    QUERIES,
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
