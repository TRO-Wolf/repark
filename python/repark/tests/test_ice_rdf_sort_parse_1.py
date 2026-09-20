"""ICE-RDF-SORT-PARSE-1 — rewrite_data_files sort and zorder against the Spark oracle.

pins: ice-rdf-sort-parse-1/C-004, C-005, C-006, C-007, C-008, C-009, C-010

The three inventory cells this unit closes — ``P-RDF-SORT``, ``P-RDF-SORT-TABLE-ORDER``
and ``P-RDF-ZORDER`` — record the *same* output row, the same ``files_after``, the same
eight rows and the same metadata on Spark. Everything the harness captures is identical,
so a rewriter that ignored ``strategy`` and ``sort_order`` and bin-packed would turn all
three cells EQUAL while doing nothing at all. The pins below are what stands between that
no-op and a green gate: they read each rewritten data file straight out of parquet and
assert the row order inside it, against sequences measured on live Spark by
``_record_ice_rdf_sort_parse_1_oracle.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException, UnsupportedOperationException

_ORACLE = json.loads(Path(__file__).with_name("ice_rdf_sort_parse_1_spark_oracle.json").read_text())

CATALOG = "mem"
APPENDS = (
    ((1, "a", "x"), (2, "b", "y"), (3, "c", "x")),
    ((4, "d", "x"), (6, "f", "x")),
    ((5, "e", "y"), (7, "g", "x"), (8, "h", "x")),
)
ZDISC_ROWS = (
    (1, "A", "D"),
    (2, "D", "A"),
    (3, "B", "C"),
    (4, "C", "B"),
    (5, "A", "A"),
    (6, "D", "D"),
    (7, "B", "D"),
    (8, "C", "A"),
)
NULL_ROWS = ((3, "c", "c"), (None, "n", "n"), (1, "a", "a"), (8, "h", "h"), (5, "e", "e"))
OUT_COLUMNS = (
    "rewritten_data_files_count",
    "added_data_files_count",
    "rewritten_bytes_count",
    "failed_data_files_count",
    "removed_delete_files_count",
)


@pytest.fixture
def engine(tmp_path: Path) -> ReparkSession:
    """Memory-catalog session for the sort and z-order pins."""
    session = ReparkSession.builder.appName("pytest-ice-rdf-sort-parse-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _literal(value: Any) -> str:
    """One SQL literal for an int, a string or a NULL."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def _insert(engine: ReparkSession, table: str, rows: tuple[tuple[Any, ...], ...]) -> None:
    """One INSERT, one snapshot."""
    values = ", ".join("(" + ", ".join(_literal(cell) for cell in row) + ")" for row in rows)
    engine.sql(f"INSERT INTO {table} VALUES {values}").collect()


def _cell_table(engine: ReparkSession, name: str, order_ddl: str | None = None) -> str:
    """The cells' fixture: cat-partitioned, three appends, eight rows."""
    table = f"{CATALOG}.ns.{name}"
    engine.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) "
        "USING iceberg PARTITIONED BY (cat)"
    ).collect()
    if order_ddl is not None:
        engine.sql(f"ALTER TABLE {table} {order_ddl}").collect()
    for batch in APPENDS:
        _insert(engine, table, batch)
    return table


def _flat_table(engine: ReparkSession, name: str, rows: tuple[tuple[Any, ...], ...]) -> str:
    """An unpartitioned three-column fixture, one row per append."""
    table = f"{CATALOG}.ns.{name}"
    engine.sql(f"CREATE TABLE {table} (id BIGINT, p STRING, q STRING) USING iceberg").collect()
    for row in rows:
        _insert(engine, table, (row,))
    return table


def _rewrite(
    engine: ReparkSession,
    table: str,
    sort_order: str | None = None,
    strategy: str | None = None,
) -> list[Any]:
    """One rewrite_data_files CALL with rewrite-all, returning its output row."""
    args = [f"table => '{table.split('.', 1)[1]}'"]
    if strategy is not None:
        args.append(f"strategy => '{strategy}'")
    if sort_order is not None:
        args.append(f"sort_order => '{sort_order}'")
    args.append("options => map('rewrite-all', 'true')")
    arrow = engine.sql(f"CALL {CATALOG}.system.rewrite_data_files({', '.join(args)})").to_arrow()
    assert arrow.column_names == list(OUT_COLUMNS), arrow.column_names
    row = [arrow.column(name)[0].as_py() for name in OUT_COLUMNS]
    row[2] = ">0" if int(row[2]) > 0 else "0"
    return row


def _file_ids(engine: ReparkSession, table: str) -> list[dict[str, Any]]:
    """Every live data file's partition and its ids in parquet row order."""
    arrow = engine.sql(f"SELECT file_path, record_count, partition FROM {table}.files").to_arrow()
    out = []
    for index in range(arrow.num_rows):
        path = arrow.column("file_path")[index].as_py()
        local = path[len("file:") :] if path.startswith("file:") else path
        partition = arrow.column("partition")[index].as_py()
        if partition is not None and not any(value is not None for value in partition.values()):
            partition = None
        out.append(
            {
                "partition": partition,
                "record_count": int(arrow.column("record_count")[index].as_py()),
                "ids": pq.read_table(local, columns=["id"]).column("id").to_pylist(),
            }
        )
    return sorted(out, key=lambda entry: str(entry["partition"]))


def _assert_cell(engine: ReparkSession, table: str, key: str) -> None:
    """The cell's output row, its files and their in-file row order, all Spark's."""
    expected = _ORACLE[key]
    assert _file_ids(engine, table) == expected["files"], key
    rows = engine.sql(f"SELECT id, data, cat FROM {table} ORDER BY id").to_arrow()
    assert rows.num_rows == 8, key


def test_rdf_sort_explicit_order(engine: ReparkSession) -> None:
    """C-004: strategy sort plus a sort_order sorts each rewritten file, Spark's order."""
    table = _cell_table(engine, "sort")
    assert _rewrite(engine, table, "id DESC NULLS LAST", "sort") == _ORACLE["sort"]["out"]
    _assert_cell(engine, table, "sort")


def test_rdf_sort_table_order(engine: ReparkSession) -> None:
    """C-005: strategy sort with no sort_order falls back to the table's own order."""
    table = _cell_table(engine, "tableorder", "WRITE ORDERED BY (id DESC NULLS LAST)")
    assert _rewrite(engine, table, None, "sort") == _ORACLE["tableorder"]["out"]
    _assert_cell(engine, table, "tableorder")


def test_rdf_zorder(engine: ReparkSession) -> None:
    """C-006: a zorder sort_order interleaves, and lands where Spark lands."""
    table = _cell_table(engine, "zorder")
    assert _rewrite(engine, table, "zorder(id, data)", "sort") == _ORACLE["zorder"]["out"]
    _assert_cell(engine, table, "zorder")


def test_rdf_sort_is_not_a_no_op(engine: ReparkSession) -> None:
    """C-004: the sort and z-order cells differ inside the file, where the harness cannot see."""
    sort_ids = [entry["ids"] for entry in _ORACLE["sort"]["files"]]
    zorder_ids = [entry["ids"] for entry in _ORACLE["zorder"]["files"]]
    assert sort_ids != zorder_ids, "the fixture must tell sort from z-order"
    table = _cell_table(engine, "both")
    assert _rewrite(engine, table, "id DESC NULLS LAST", "sort") == _ORACLE["sort"]["out"]
    assert [entry["ids"] for entry in _file_ids(engine, table)] == sort_ids
    assert [entry["ids"] for entry in _file_ids(engine, table)] != zorder_ids


def test_rdf_zorder_is_column_order_sensitive(engine: ReparkSession) -> None:
    """C-007: zorder(p, q), zorder(q, p) and id DESC land in three different orders."""
    keys = ("zdisc_id_desc", "zdisc_zorder_p_q", "zdisc_zorder_q_p")
    expected = [_ORACLE[key]["files"][0]["ids"] for key in keys]
    assert len(set(map(tuple, expected))) == 3, "the fixture must discriminate all three"
    for key, order in zip(
        keys, ("id DESC", "zorder(p, q)", "zorder(q, p)"), strict=True
    ):
        table = _flat_table(engine, key, ZDISC_ROWS)
        assert _rewrite(engine, table, order, "sort") == _ORACLE[key]["out"], key
        assert _file_ids(engine, table) == _ORACLE[key]["files"], key


def test_rdf_sort_bare_desc_puts_nulls_last(engine: ReparkSession) -> None:
    """C-008: a bare DESC ties to NULLS LAST at runtime, not only in the parser."""
    table = _flat_table(engine, "nulls", NULL_ROWS)
    assert _rewrite(engine, table, "id DESC", "sort") == _ORACLE["nulls"]["out"]
    assert _file_ids(engine, table) == _ORACLE["nulls"]["files"]
    assert _ORACLE["nulls"]["files"][0]["ids"][-1] is None


def test_rdf_sort_on_unsorted_table_refuses(engine: ReparkSession) -> None:
    """C-009: strategy sort with no order and an unsorted table surfaces the fork's refusal."""
    table = _flat_table(engine, "unsorted", ZDISC_ROWS)
    before = _file_ids(engine, table)
    with pytest.raises(IllegalArgumentException) as caught:
        _rewrite(engine, table, None, "sort")
    message = str(caught.value)
    assert "Cannot sort data without a valid sort order" in message
    assert "is unsorted and no sort order is provided" in message
    assert _file_ids(engine, table) == before


def test_rdf_binpack_with_sort_order_refuses(engine: ReparkSession) -> None:
    """C-009: binpack plus any sort_order is Java's already-set rewrite mode."""
    table = _flat_table(engine, "bp", ZDISC_ROWS)
    for order in ("p ASC", "zorder(p, q)"):
        with pytest.raises(IllegalArgumentException) as caught:
            _rewrite(engine, table, order, "binpack")
        assert "Cannot set rewrite mode, it has already been set to BIN-PACK" in str(caught.value)


def test_rdf_sort_order_without_strategy_still_sorts(engine: ReparkSession) -> None:
    """C-010: an omitted strategy is not bin-pack when a sort_order is present."""
    table = _flat_table(engine, "nostrategy", ZDISC_ROWS)
    assert _rewrite(engine, table, "id DESC") == _ORACLE["zdisc_id_desc"]["out"]
    assert _file_ids(engine, table) == _ORACLE["zdisc_id_desc"]["files"]


def test_rdf_zorder_refusals_are_the_forks(engine: ReparkSession) -> None:
    """C-009: an unknown z-order column and an empty z-order raise Iceberg's own text."""
    table = _flat_table(engine, "zbad", ZDISC_ROWS)
    for order, needle in (
        ("zorder(nope)", "Cannot find column 'nope' in table schema (case sensitive = false)"),
        ("zorder()", "Cannot ZOrder when no columns are specified"),
    ):
        with pytest.raises(IllegalArgumentException) as caught:
            _rewrite(engine, table, order, "sort")
        assert needle in str(caught.value), order


def test_rdf_sort_order_transform_is_a_declared_refusal(engine: ReparkSession) -> None:
    """C-009: a non-zorder transform sort term refuses on the CALL door (RDF-SORT-TRANSFORM-1)."""
    table = _flat_table(engine, "xf", ZDISC_ROWS)
    with pytest.raises(UnsupportedOperationException) as caught:
        _rewrite(engine, table, "bucket(4, id)", "sort")
    assert "transform `bucket(…)` is not supported yet" in str(caught.value)


def test_rdf_mixed_sort_terms_refuse(engine: ReparkSession) -> None:
    """C-009: identity and z-order terms in one sort_order raise Java's mix refusal."""
    table = _flat_table(engine, "mix", ZDISC_ROWS)
    with pytest.raises(IllegalArgumentException) as caught:
        _rewrite(engine, table, "id, zorder(p)", "sort")
    assert "Cannot mix identity sort columns and a Zorder sort expression: id, zorder(p)" in str(
        caught.value
    )


def test_rdf_binpack_unchanged(engine: ReparkSession) -> None:
    """C-011: default and explicit binpack behave exactly as before this unit."""
    for name, strategy in (("bpd", None), ("bpe", "binpack")):
        table = _cell_table(engine, name)
        assert _rewrite(engine, table, None, strategy) == [5, 2, ">0", 0, 0]
        files = _file_ids(engine, table)
        assert [entry["record_count"] for entry in files] == [6, 2]
        assert sorted(sum((entry["ids"] for entry in files), [])) == list(range(1, 9))
