"""R-TAIL — DataFrame action surface: take / head / first / tail / isEmpty / toLocalIterator.

Oracle: live PySpark 4.1.2 under ``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``,
``SPARK_LOCAL_IP=127.0.0.1``, ANSI on, Arrow on. Full capture in
``/tmp/sepmo-dogfood-r2-2026-07-28/r-tail-oracle.{py,out}`` (2026-07-28).

Return types (verbatim from the oracle):

* ``take(n)`` → ``list[Row]``
* ``head()`` → ``Row | None`` (None on empty)
* ``head(n)`` → ``list[Row]`` (incl. ``head(0) → []``)
* ``first()`` → ``Row | None``
* ``tail(n)`` → ``list[Row]``
* ``isEmpty()`` → ``bool``
* ``toLocalIterator()`` → iterator of ``Row``

Edges:

* ``n=0`` → empty list (take/head/tail)
* ``n > count`` → all rows
* ``take(-1)`` / ``head(-1)`` → ``AnalysisException``
  ``[INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE]`` (minus SQLSTATE + plan dump)
* ``tail(-1)`` → ``[]`` (Spark does **not** raise — live-recorded)

Value pins use ``collect`` / ``to_arrow`` (value AND type), never only ``show``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException, PySparkTypeError
from repark.spark.row import Row


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-r-tail-actions").getOrCreate()
    yield session
    session.stop()


def _ordered_frame(spark: ReparkSession) -> object:
    """Five ordered rows via createDataFrame (stable; no shuffle)."""
    return spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")],
        ["id", "s"],
    )


def _row_tuples(rows: list[Row]) -> list[tuple[object, ...]]:
    return [tuple(row) for row in rows]


# ==================================================================================================
# take
# ==================================================================================================


def test_take_returns_list_of_rows_prefix(spark: ReparkSession) -> None:
    # Oracle: take(2) → [Row(id=1,s='a'), Row(id=2,s='b')]
    df = _ordered_frame(spark)
    taken = df.take(2)
    assert isinstance(taken, list)
    assert all(isinstance(row, Row) for row in taken)
    assert _row_tuples(taken) == [(1, "a"), (2, "b")]
    # Arrow path: same values + types for the source plan.
    table = df.limit(2).to_arrow()
    assert table.column("id").to_pylist() == [1, 2]
    assert table.schema.field("id").type == pa.int64()
    string_type = table.schema.field("s").type
    assert (
        pa.types.is_string(string_type)
        or pa.types.is_large_string(string_type)
        or pa.types.is_string_view(string_type)
    ), string_type


def test_take_zero_and_oversize(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    assert df.take(0) == []
    oversize = df.take(100)
    assert _row_tuples(oversize) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]


def test_take_negative_raises_analysis_exception(spark: ReparkSession) -> None:
    # Oracle: AnalysisException [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] …
    df = _ordered_frame(spark)
    with pytest.raises(
        AnalysisException,
        match=r"INVALID_LIMIT_LIKE_EXPRESSION\.IS_NEGATIVE",
    ) as raised:
        df.take(-1)
    message = str(raised.value)
    assert "must be equal to or greater than 0" in message
    assert "got -1" in message
    # Taxonomy: AnalysisException is a PySparkException / RuntimeError (near-drop-in).
    assert isinstance(raised.value, PySparkException)
    assert isinstance(raised.value, RuntimeError)


def test_take_rejects_non_int(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.take("2")  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.take(True)  # type: ignore[arg-type]


def test_take_rejects_bool_false(spark: ReparkSession) -> None:
    """Oracle EDGE: bool⊂int reject spans False, not only True (C8-Q-001).

    ``isinstance(False, int)`` is True in Python. A residual ``num is True`` (or truthy-only)
    guard still raises on ``take(True)`` while accepting ``take(False)`` as limit/collect of 0
    → silent ``[]``. Pin both bool domain members on the take path.
    """
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.take(False)  # type: ignore[arg-type]


def test_take_rejects_none_and_float(spark: ReparkSession) -> None:
    """Oracle EDGE type matrix: take(None) / take(1.5) → type error (C8-Q-002).

    Live PySpark 4.1.2 rejects null and Double on the JVM limit path. Production raises
    ``PySparkTypeError`` naming ``num``. Without these pins: ``if num is None: return []``
    short-circuits before the type guard (``take(0)==[]`` stays green), and a float-truncating
    path (``int(1.5)→1`` / ``DataFrame.limit``'s ``int(n)``) still passes string/True pins while
    returning the wrong prefix length.
    """
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.take(None)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.take(1.5)  # type: ignore[arg-type]


# ==================================================================================================
# head / first
# ==================================================================================================


def test_head_no_arg_returns_row(spark: ReparkSession) -> None:
    # Oracle: head() → Row (not list); empty → None
    df = _ordered_frame(spark)
    first_row = df.head()
    assert isinstance(first_row, Row)
    assert tuple(first_row) == (1, "a")
    assert first_row.id == 1
    assert first_row.s == "a"


def test_head_n_returns_list(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    rows = df.head(2)
    assert isinstance(rows, list)
    assert _row_tuples(rows) == [(1, "a"), (2, "b")]
    assert df.head(0) == []
    assert _row_tuples(df.head(100)) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]


def test_head_one_returns_list_not_row(spark: ReparkSession) -> None:
    """Oracle: head(1) → list[Row]; head() (no arg) → Row. Polymorphism pin (C2-Q-002).

    A wrong ``if n == 1: return row`` branch would still pass ``head(2)`` list pins while
    breaking the head(1) vs head() return-type split recorded live.
    """
    df = _ordered_frame(spark)
    one = df.head(1)
    assert isinstance(one, list)
    assert not isinstance(one, Row)
    assert len(one) == 1
    assert isinstance(one[0], Row)
    assert tuple(one[0]) == (1, "a")
    # Contrast: no-arg head is a single Row, not a one-element list.
    bare = df.head()
    assert isinstance(bare, Row)
    assert not isinstance(bare, list)


def test_head_empty_frame(spark: ReparkSession) -> None:
    empty = spark.createDataFrame([], ["id", "s"])
    assert empty.head() is None
    assert empty.head(2) == []


def test_first_matches_head_no_arg(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    assert isinstance(df.first(), Row)
    assert tuple(df.first()) == tuple(df.head())  # type: ignore[arg-type]
    empty = spark.createDataFrame([], ["id", "s"])
    assert empty.first() is None


def test_head_negative_raises_analysis_exception(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    with pytest.raises(AnalysisException, match=r"INVALID_LIMIT_LIKE_EXPRESSION\.IS_NEGATIVE"):
        df.head(-1)


def test_head_rejects_bool(spark: ReparkSession) -> None:
    """Oracle EDGE head(True): reject bool⊂int (C2-Q-003).

    take(True)/tail(True) are pinned separately; without this, head could route True→limit(1)
    and return a list while take still raises.
    """
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.head(True)  # type: ignore[arg-type]


def test_head_rejects_bool_false_and_float(spark: ReparkSession) -> None:
    """head routes n≠None through take; pin False + float on that path (C8-Q-001/002).

    ``head(False)`` must not become ``head(0)→[]`` under a truthy-only bool guard.
    ``head(1.5)`` must not truncate via ``int(1.5)`` while True/string pins stay green.
    """
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.head(False)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.head(1.5)  # type: ignore[arg-type]


def test_head_none_returns_first_row(spark: ReparkSession) -> None:
    """Oracle EDGE: head(None) is OK → first Row (same as no-arg head) (C8-Q-002).

    ``None`` is the default sentinel for bare ``head()``, not a type reject. take(None)/tail(None)
    raise; pin the polymorphism so a universal ``if n is None: raise`` mutation fails this pin
    while those rejects stay green.
    """
    df = _ordered_frame(spark)
    first_row = df.head(None)
    assert isinstance(first_row, Row)
    assert tuple(first_row) == (1, "a")
    assert first_row is not None
    # Contrast: take(None) is rejected (pinned in test_take_rejects_none_and_float).
    empty = spark.createDataFrame([], ["id", "s"])
    assert empty.head(None) is None


# ==================================================================================================
# tail
# ==================================================================================================


def test_tail_returns_last_n_rows(spark: ReparkSession) -> None:
    # Oracle: tail(2) → [Row(id=4,s='d'), Row(id=5,s='e')]
    df = _ordered_frame(spark)
    tailed = df.tail(2)
    assert isinstance(tailed, list)
    assert all(isinstance(row, Row) for row in tailed)
    assert _row_tuples(tailed) == [(4, "d"), (5, "e")]


def test_tail_zero_oversize_and_full(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    assert df.tail(0) == []
    assert _row_tuples(df.tail(100)) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]
    assert _row_tuples(df.tail(5)) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]


def test_tail_negative_returns_empty_list(spark: ReparkSession) -> None:
    # Oracle (live PySpark 4.1.2): tail(-1) → [] — does NOT raise (unlike take/head).
    df = _ordered_frame(spark)
    assert df.tail(-1) == []


def test_tail_empty_frame(spark: ReparkSession) -> None:
    empty = spark.createDataFrame([], ["id", "s"])
    assert empty.tail(2) == []


def test_tail_rejects_non_int(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.tail("2")  # type: ignore[arg-type]
    # Mutation guard (C1-Q-003): bool is a subclass of int — without an explicit bool reject,
    # ``tail(True)`` becomes ``tail(1)`` and returns the last row while take(True) still raises
    # (take is pinned separately). Live PySpark rejects Boolean on the JVM limit/tail path.
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.tail(True)  # type: ignore[arg-type]


def test_tail_rejects_bool_false_none_and_float(spark: ReparkSession) -> None:
    """tail local type guard: False / None / float → PySparkTypeError (C8-Q-001/002).

    tail does **not** share take's ``_require_non_negative_limit``; it has a duplicate
    ``isinstance(num, bool) or not isinstance(num, int)`` check. Pins on take alone leave tail
    free to accept ``False→[]`` (``num <= 0`` short-circuit), ``None→[]``, or ``int(1.5)→1``
    last-row truncation while the take EDGE pins stay green.
    """
    df = _ordered_frame(spark)
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.tail(False)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.tail(None)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match=r"num"):
        df.tail(1.5)  # type: ignore[arg-type]


def test_tail_is_not_limit_head(spark: ReparkSession) -> None:
    """Mutation guard: tail must not be implemented as limit (which truncates the head)."""
    df = _ordered_frame(spark)
    assert _row_tuples(df.tail(2)) != _row_tuples(df.take(2))
    assert _row_tuples(df.tail(2)) == [(4, "d"), (5, "e")]


def test_tail_zero_and_negative_raise_after_stop() -> None:
    """Stopped-session contract: tail(0)/tail(-1) must not silent-return [] (C2-L-001).

    Production previously short-circuited ``num <= 0`` before ``_ensure_alive``; take(0) still
    fails loud via limit/collect. Pin both zero and negative after stop.
    """
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-r-tail-stop").getOrCreate()
    try:
        frame = session.createDataFrame([(1, "a")], ["id", "s"])
        session.stop()
        with pytest.raises(RuntimeError, match="stopped"):
            frame.tail(0)
        with pytest.raises(RuntimeError, match="stopped"):
            frame.tail(-1)
        with pytest.raises(RuntimeError, match="stopped"):
            frame.tail(1)
    finally:
        session_module._reset_active_session_for_tests()


def test_take_and_head_zero_raise_after_stop() -> None:
    """Stopped-session contract: take(0)/head(0) must not silent-return [] (C5-Q-001).

    Production routes take/head through ``limit`` + ``collect`` (and ``_spawn``), so they fail
    loud after stop today. A zero short-circuit before limit/collect — the same class of bug
    that pre-C2-L-001 had on ``tail`` — would return ``[]`` while positive-n pins, live-frame
    ``take(0)==[]`` / ``head(0)==[]``, and the tail-stop pin all stay green. The take docstring
    claims the limit+collect path; pin the zero path after stop explicitly.
    """
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-r-tail-take-stop").getOrCreate()
    try:
        frame = session.createDataFrame([(1, "a")], ["id", "s"])
        session.stop()
        with pytest.raises(RuntimeError, match="stopped"):
            frame.take(0)
        with pytest.raises(RuntimeError, match="stopped"):
            frame.head(0)
        # Positive n still fails loud (regression guard; not a zero short-circuit hole alone).
        with pytest.raises(RuntimeError, match="stopped"):
            frame.take(1)
        with pytest.raises(RuntimeError, match="stopped"):
            frame.head(1)
    finally:
        session_module._reset_active_session_for_tests()


def test_is_empty_to_local_iterator_first_raise_after_stop() -> None:
    """Stopped-session contract: isEmpty/toLocalIterator/first/bare head fail loud (C6-Q-002).

    take/head(n)/tail after stop are pinned elsewhere (C5-Q-001 / C2-L-001). Without this pin,
    ``isEmpty``, ``toLocalIterator``, ``first``, and bare ``head()`` can short-circuit to a
    live-frame-shaped success (``False``/``[]``/``None``/row) while those other stop pins and
    the happy-path return-type matrix stay green. Production routes them through
    ``limit``/``count``/``collect``/``_spawn`` (or ``head``→``take``), which already gate
    liveness — pin the lifecycle parity explicitly.
    """
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-r-tail-actions-stop").getOrCreate()
    try:
        frame = session.createDataFrame([(1, "a")], ["id", "s"])
        session.stop()
        with pytest.raises(RuntimeError, match="stopped"):
            frame.isEmpty()
        with pytest.raises(RuntimeError, match="stopped"):
            frame.is_empty()
        with pytest.raises(RuntimeError, match="stopped"):
            list(frame.toLocalIterator())
        with pytest.raises(RuntimeError, match="stopped"):
            list(frame.to_local_iterator())
        with pytest.raises(RuntimeError, match="stopped"):
            frame.first()
        with pytest.raises(RuntimeError, match="stopped"):
            frame.head()
    finally:
        session_module._reset_active_session_for_tests()


# ==================================================================================================
# isEmpty
# ==================================================================================================


def test_is_empty_bool(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    assert df.isEmpty() is False
    assert df.is_empty() is False  # snake_case alias
    empty = spark.createDataFrame([], ["id", "s"])
    assert empty.isEmpty() is True
    # Null-only row is NOT empty (oracle Example 3).
    nulls = spark.createDataFrame([(None, None)], ["a", "b"])
    assert nulls.isEmpty() is False


# ==================================================================================================
# toLocalIterator
# ==================================================================================================


def test_to_local_iterator_yields_rows(spark: ReparkSession) -> None:
    df = _ordered_frame(spark)
    iterator = df.toLocalIterator()
    assert isinstance(iterator, Iterator)
    materialised = list(iterator)
    assert all(isinstance(row, Row) for row in materialised)
    assert _row_tuples(materialised) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]
    # Snake alias + prefetchPartitions accepted.
    assert _row_tuples(list(df.to_local_iterator(prefetchPartitions=True))) == _row_tuples(
        materialised
    )


def test_to_local_iterator_empty(spark: ReparkSession) -> None:
    empty = spark.createDataFrame([], ["id", "s"])
    assert list(empty.toLocalIterator()) == []


def test_to_local_iterator_matches_collect_values_and_types(spark: ReparkSession) -> None:
    """P2b: streaming iterator is value+type equivalent to collect/to_arrow (not show-only)."""
    df = _ordered_frame(spark)
    collected = df.collect()
    streamed = list(df.toLocalIterator())
    assert _row_tuples(streamed) == _row_tuples(collected)
    table = df.to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5]
    assert table.schema.field("id").type == pa.int64()


def test_to_local_iterator_partial_consume_is_iterator(spark: ReparkSession) -> None:
    """P2b: return kind is a live iterator — first() style partial pull works without list()."""
    df = _ordered_frame(spark)
    iterator = df.toLocalIterator()
    first = next(iterator)
    assert tuple(first) == (1, "a")
    second = next(iterator)
    assert tuple(second) == (2, "b")
    # Remaining rows still available on the same generator.
    rest = list(iterator)
    assert _row_tuples(rest) == [(3, "c"), (4, "d"), (5, "e")]


def test_to_local_iterator_maps_nulls_match_collect(spark: ReparkSession) -> None:
    """P2b octo C1: stream Row path shares map→dict + null conversion with collect."""
    frame = spark.createDataFrame(
        [({"a": 1}, None), ({}, "x"), ({"b": 2, "c": 3}, "y")],
        "m map<string,int>, s string",
    )
    collected = frame.collect()
    streamed = list(frame.toLocalIterator())
    assert len(streamed) == len(collected) == 3
    for stream_row, collect_row in zip(streamed, collected, strict=True):
        assert stream_row.asDict(recursive=True) == collect_row.asDict(recursive=True)
        assert isinstance(stream_row["m"], dict)
    assert streamed[0]["m"] == {"a": 1}
    assert streamed[1]["m"] == {}
    assert streamed[0]["s"] is None
    # Nested array<map> also converts (shared _arrow_cell_to_spark_python).
    nested = spark.sql("select array(map('k', 1), map('k2', 2)) as am")
    assert next(iter(nested.toLocalIterator()))["am"] == [{"k": 1}, {"k2": 2}]
    assert nested.collect()[0]["am"] == [{"k": 1}, {"k2": 2}]


def test_to_local_iterator_partial_abandon_then_full_action(spark: ReparkSession) -> None:
    """P2b octo C1: abandon a partial stream; later actions on the handle still work."""
    frame = spark.range(20_000).orderBy("id")
    iterator = frame.toLocalIterator()
    assert next(iterator)[0] == 0
    iterator.close()
    del iterator
    # Fresh full actions must not see a poisoned plan/stream.
    assert frame.count() == 20_000
    assert len(frame.collect()) == 20_000
    assert [row[0] for row in frame.limit(3).collect()] == [0, 1, 2]


# ==================================================================================================
# to_arrow_batches (P2b repark extension)
# ==================================================================================================


def test_to_arrow_batches_concat_equals_to_arrow(spark: ReparkSession) -> None:
    """P2b: batch iterator reconstructs the full table (value + schema types)."""
    df = _ordered_frame(spark)
    batches = list(df.to_arrow_batches())
    assert batches, "expected at least one RecordBatch"
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    reconstructed = pa.Table.from_batches(batches)
    full = df.to_arrow()
    assert reconstructed.schema.equals(full.schema)
    assert reconstructed.to_pydict() == full.to_pydict()
    # CamelCase alias.
    camel = list(df.toArrowBatches())
    assert pa.Table.from_batches(camel).to_pydict() == full.to_pydict()


def test_to_arrow_batches_multibatch_orderby_equals_to_arrow(spark: ReparkSession) -> None:
    """P2b octo C1: multi-batch stream concat ≡ to_arrow under orderBy (positional)."""
    # range(n) without orderBy is multi-partition unordered across separate executions;
    # orderBy pins a stable row order so concat equality is mutation-proof.
    # 200_000 > the 65536 session-default batch_size, so the stream is genuinely multi-batch.
    frame = spark.range(200_000).selectExpr("id", "cast(id as string) as s").orderBy("id")
    batches = list(frame.to_arrow_batches())
    assert len(batches) >= 2, f"expected multi-batch plan, got {len(batches)} batch(es)"
    reconstructed = pa.Table.from_batches(batches)
    full = frame.to_arrow()
    assert reconstructed.num_rows == full.num_rows == 200_000
    assert reconstructed.schema.equals(full.schema)
    assert reconstructed.to_pydict() == full.to_pydict()
    # Stream rows match collect under the same orderBy (value + int type).
    streamed = list(frame.toLocalIterator())
    collected = frame.collect()
    assert _row_tuples(streamed) == _row_tuples(collected)
    assert streamed[0][0] == 0 and streamed[-1][0] == 199_999
    assert full.schema.field("id").type == pa.int64()


def test_to_arrow_batches_empty_frame(spark: ReparkSession) -> None:
    """P2b octo C1: empty emits a schema-bearing zero-row batch (twin of to_arrow)."""
    empty = spark.createDataFrame([], ["id", "s"])
    batches = list(empty.to_arrow_batches())
    assert len(batches) == 1, "empty must yield exactly one zero-row schema batch"
    assert batches[0].num_rows == 0
    table = pa.Table.from_batches(batches)
    full = empty.to_arrow()
    assert table.num_rows == 0
    assert table.schema.equals(full.schema)
    assert table.schema.names == ["id", "s"]
    # Zero-row filter path also preserves schema (not only createDataFrame empty).
    filtered = spark.range(10).filter("id < 0")
    filter_batches = list(filtered.to_arrow_batches())
    assert len(filter_batches) == 1 and filter_batches[0].num_rows == 0
    assert pa.Table.from_batches(filter_batches).schema.equals(filtered.to_arrow().schema)


def test_to_arrow_batches_empty_nested_schemas(spark: ReparkSession) -> None:
    """P2b octo C4: empty nested/extension schemas still get a schema-eq zero-row batch."""
    cases = [
        "select array(1, 2) as a where 1=0",
        "select named_struct('x', 1, 'y', 'z') as s where 1=0",
        "select cast(1.23 as decimal(10,2)) as d where 1=0",
        "select timestamp '2020-01-02 03:04:05' as t where 1=0",
        "select str_to_map('a:1,b:2') as m where 1=0",
    ]
    for sql in cases:
        frame = spark.sql(sql)
        batches = list(frame.to_arrow_batches())
        assert len(batches) == 1 and batches[0].num_rows == 0, sql
        assert pa.Table.from_batches(batches).schema.equals(frame.to_arrow().schema), sql
    # Wide empty (many columns) — from_pylist empty must not drop fields.
    wide_select = ", ".join(f"cast(null as bigint) as c{index}" for index in range(40))
    wide = spark.sql(f"select {wide_select} where 1=0")
    wide_batches = list(wide.to_arrow_batches())
    assert len(wide_batches) == 1 and wide_batches[0].num_rows == 0
    assert len(wide_batches[0].schema) == 40
    assert pa.Table.from_batches(wide_batches).schema.equals(wide.to_arrow().schema)


def test_to_arrow_batches_mid_stream_error_is_pyspark_exception(spark: ReparkSession) -> None:
    """P2b octo C1: mid-stream engine error → PySparkException (same contract as to_arrow)."""
    # element_at index 0 is invalid in Spark (1-based); fails at execution when the batch pulls.
    bad = spark.sql("select element_at(array(1, 2), 0) as x")
    with pytest.raises(PySparkException) as batches_exc:
        list(bad.to_arrow_batches())
    with pytest.raises(PySparkException) as arrow_exc:
        bad.to_arrow()
    # Message preserved (engine text); both paths use the same exception class.
    message = str(batches_exc.value).lower()
    assert "element_at" in message or "index" in message
    assert type(batches_exc.value) is type(arrow_exc.value)
    # Session remains usable after a failed stream pull.
    assert spark.range(2).collect()[0][0] == 0


def test_schema_metadata_stable_across_repeated_access(spark: ReparkSession) -> None:
    """P2b: repeated schema/columns access on one handle is stable (cache-safe)."""
    df = spark.range(10).selectExpr("id", "id * 2 as doubled")
    first_schema = df.schema
    first_columns = list(df.columns)
    for _ in range(50):
        assert list(df.columns) == first_columns
        assert df.schema.simpleString() == first_schema.simpleString()
    # Arrow physical schema also stable (analysis-only capsule path).
    arrow_a = df._analyzed_arrow_schema()
    arrow_b = df._analyzed_arrow_schema()
    assert arrow_a.equals(arrow_b)
    assert arrow_a.field("id").type == pa.int64()
    # Stream open must not invalidate / diverge cached schema metadata.
    _ = list(df.to_arrow_batches())
    assert df.schema.simpleString() == first_schema.simpleString()
    assert df._analyzed_arrow_schema().equals(arrow_a)


def test_collect_stream_complex_types_and_dual_iterators(spark: ReparkSession) -> None:
    """P2b octo C2: collect≡stream on decimal/struct/date/ts; dual iterators independent."""
    from datetime import date, datetime
    from decimal import Decimal

    frame = spark.sql(
        """
        select
          cast(12.34 as decimal(10,2)) as dec,
          named_struct('x', 1, 'y', 'z') as st,
          date '2020-01-02' as d,
          timestamp '2020-01-02 03:04:05' as t,
          cast(null as struct<x:int, y:string>) as st_null
        """
    )
    collected = frame.collect()
    streamed = list(frame.toLocalIterator())
    assert len(collected) == len(streamed) == 1
    assert collected[0].asDict(recursive=True) == streamed[0].asDict(recursive=True)
    assert collected[0]["dec"] == Decimal("12.34")
    assert collected[0]["st"] == {"x": 1, "y": "z"}
    assert collected[0]["d"] == date(2020, 1, 2)
    assert collected[0]["st_null"] is None
    # Timestamp may be datetime or pandas.Timestamp — both paths must match each other.
    assert collected[0]["t"] == streamed[0]["t"]
    assert collected[0]["t"] == datetime(2020, 1, 2, 3, 4, 5) or str(collected[0]["t"]).startswith(
        "2020-01-02 03:04:05"
    )

    # Dual independent iterators (interleaved next) — no shared cursor / double-free.
    ordered = spark.range(100).orderBy("id")
    iterator_a = ordered.toLocalIterator()
    iterator_b = ordered.toLocalIterator()
    pairs = [(next(iterator_a)[0], next(iterator_b)[0]) for _ in range(5)]
    assert pairs == [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
    assert len(list(iterator_a)) == 95
    assert len(list(iterator_b)) == 95

    # range(0) empty: schema-bearing batch; collect/stream yield no rows.
    empty_range = spark.range(0)
    empty_batches = list(empty_range.to_arrow_batches())
    assert len(empty_batches) == 1 and empty_batches[0].num_rows == 0
    assert pa.Table.from_batches(empty_batches).schema.equals(empty_range.to_arrow().schema)
    assert empty_range.collect() == []
    assert list(empty_range.toLocalIterator()) == []


def test_collect_matches_tolocaliterator_ordered(spark: ReparkSession) -> None:
    """P2b octo C2: collect (batch-wise) ≡ toLocalIterator under orderBy (dual-peak fix)."""
    frame = spark.range(5_000).selectExpr("id", "cast(id as string) as s").orderBy("id")
    assert _row_tuples(frame.collect()) == _row_tuples(list(frame.toLocalIterator()))


def test_cache_stream_and_collect_paths(spark: ReparkSession) -> None:
    """P2b octo C3: cached MemTable is visible to collect / stream / batches; unpersist re-runs."""
    frame = spark.range(100).selectExpr("id", "id * 2 as d").orderBy("id").cache()
    try:
        assert frame.count() == 100
        collected = frame.collect()
        streamed = list(frame.toLocalIterator())
        batch_rows = sum(batch.num_rows for batch in frame.to_arrow_batches())
        assert len(collected) == len(streamed) == batch_rows == 100
        assert _row_tuples(collected) == _row_tuples(streamed)
        assert collected[0][0] == 0 and collected[-1][0] == 99
    finally:
        frame.unpersist()
    # After unpersist, actions still work (re-execute plan).
    assert len(frame.collect()) == 100
    assert len(list(frame.toLocalIterator())) == 100


def test_collect_mid_stream_error_is_pyspark_exception(spark: ReparkSession) -> None:
    """P2b octo C3: collect batch-wise path maps engine errors like to_arrow/batches."""
    bad = spark.sql("select element_at(array(1, 2), 0) as x")
    with pytest.raises(PySparkException) as collect_exc:
        bad.collect()
    with pytest.raises(PySparkException) as stream_exc:
        list(bad.toLocalIterator())
    message = str(collect_exc.value).lower()
    assert "element_at" in message or "index" in message
    assert type(collect_exc.value) is type(stream_exc.value)
    assert spark.range(1).count() == 1


# ==================================================================================================
# r22 P5: collect Row materialization (primitive fast path + map convert)
# ==================================================================================================


def test_p5_collect_primitive_fast_path_matches_arrow(spark: ReparkSession) -> None:
    """P5: all-primitive collect matches to_arrow values AND types (Arrow path, not show)."""
    from repark.spark.dataframe import DataFrame

    frame = (
        spark.range(1_000)
        .selectExpr(
            "id",
            "cast(id as double) as d",
            "cast(id as string) as s",
            "id % 2 = 0 as flag",
        )
        .orderBy("id")
    )
    collected = frame.collect()
    table = frame.to_arrow()
    assert len(collected) == table.num_rows == 1_000
    assert [row[0] for row in collected] == table.column("id").to_pylist()
    assert [row[1] for row in collected] == table.column("d").to_pylist()
    assert [row[2] for row in collected] == table.column("s").to_pylist()
    assert [row[3] for row in collected] == table.column("flag").to_pylist()
    assert table.schema.field("id").type == pa.int64()
    assert pa.types.is_floating(table.schema.field("d").type)
    assert isinstance(collected[0][0], int)
    assert isinstance(collected[0][1], float)
    assert isinstance(collected[0][2], str)
    assert isinstance(collected[0][3], bool)
    # Schema classifiers: primitives identity; maps need convert; intervals may calendar.
    assert DataFrame._arrow_type_needs_spark_python_convert(pa.int64()) is False
    assert DataFrame._arrow_type_needs_spark_python_convert(pa.list_(pa.int64())) is False
    assert (
        DataFrame._arrow_type_needs_spark_python_convert(pa.map_(pa.string(), pa.int32())) is True
    )
    assert (
        DataFrame._arrow_type_needs_spark_python_convert(pa.list_(pa.map_(pa.string(), pa.int32())))
        is True
    )
    assert DataFrame._arrow_type_may_hold_calendar_interval(pa.int64()) is False
    assert DataFrame._arrow_type_may_hold_calendar_interval(pa.month_day_nano_interval()) is True


def test_p5_collect_map_and_nested_array_map_convert(spark: ReparkSession) -> None:
    """P5: map→dict + array<map> still convert on the optimized collect path."""
    frame = spark.createDataFrame(
        [({"a": 1},), ({},), ({"b": 2, "c": 3},)],
        "m map<string,int>",
    )
    rows = frame.collect()
    assert rows[0]["m"] == {"a": 1}
    assert rows[1]["m"] == {}
    assert rows[2]["m"] == {"b": 2, "c": 3}
    assert all(isinstance(row["m"], dict) for row in rows)
    nested = spark.sql("select array(map('k', 1), map('k2', 2)) as am")
    assert nested.collect()[0]["am"] == [{"k": 1}, {"k2": 2}]
    # Stream path shares conversion (toLocalIterator ≡ collect values).
    assert nested.collect()[0].asDict(recursive=True) == next(
        iter(nested.toLocalIterator())
    ).asDict(recursive=True)


def test_p5_collect_nested_empty_map_value_is_dict(spark: ReparkSession) -> None:
    """P5 octo C1: nested empty map values are ``{}`` (not ``[]``) — schema-aware item convert."""
    frame = spark.createDataFrame(
        [({"a": {}, "b": {"x": 1}},)],
        "m map<string,map<string,int>>",
    )
    cell = frame.collect()[0]["m"]
    assert cell == {"a": {}, "b": {"x": 1}}
    assert isinstance(cell["a"], dict)
    assert isinstance(cell["b"], dict)
    # Map values that are empty arrays stay lists (item type is array, not map).
    arr_vals = spark.sql(
        "SELECT map('a', array(1, 2), 'b', cast(array() as array<int>)) AS m"
    ).collect()[0]["m"]
    assert arr_vals == {"a": [1, 2], "b": []}
    assert isinstance(arr_vals["b"], list)


def test_p5_collect_nan_and_none_preserved(spark: ReparkSession) -> None:
    """P5: NaN stays float NaN and SQL null stays None on the identity collect path."""
    import math

    rows = spark.sql(
        "SELECT * FROM VALUES (CAST('NaN' AS DOUBLE), CAST(NULL AS DOUBLE)), "
        "(CAST(1.5 AS DOUBLE), CAST(2.0 AS DOUBLE)) AS t(a, b)"
    ).collect()
    assert math.isnan(rows[0][0])
    assert rows[0][1] is None
    assert rows[1][0] == 1.5
    assert rows[1][1] == 2.0
    assert isinstance(rows[0][0], float)


def test_p5_nested_calendar_interval_refused(spark: ReparkSession) -> None:
    """P5 octo C1: nested MonthDayNano refuses on collect (list/struct/map containers)."""
    from repark.errors import PySparkNotImplementedError
    from repark.spark.dataframe import DataFrame

    del spark
    mdn = pa.scalar((1, 2, 3), type=pa.month_day_nano_interval()).as_py()
    cases = [
        pa.table({"c": pa.array([[mdn]], type=pa.list_(pa.month_day_nano_interval()))}),
        pa.table(
            {
                "s": pa.array(
                    [(mdn, 1)],
                    type=pa.struct([("i", pa.month_day_nano_interval()), ("n", pa.int64())]),
                )
            }
        ),
        pa.table(
            {
                "m": pa.array(
                    [[("a", mdn)]],
                    type=pa.map_(pa.string(), pa.month_day_nano_interval()),
                )
            }
        ),
    ]
    for table in cases:
        with pytest.raises(PySparkNotImplementedError) as caught:
            DataFrame._rows_from_arrow_table(table)
        assert caught.value.getErrorClass() == "NOT_IMPLEMENTED"


def test_p5_collect_duplicate_display_names_positional(spark: ReparkSession) -> None:
    """P5: duplicate field names stay positional (T3 pins) under bulk Row assembly."""
    from repark.spark.dataframe import DataFrame

    table = pa.table({"a": [1, 2], "b": [10, 20]})
    # Simulate H1 multi-name rename: two columns both display as ``id``.
    table = table.rename_columns(["id", "id"])
    rows = DataFrame._rows_from_arrow_table(table)
    assert len(rows) == 2
    assert rows[0].__fields__ == ["id", "id"]
    assert rows[0][0] == 1 and rows[0][1] == 10
    assert rows[1][0] == 2 and rows[1][1] == 20
    # asDict collapses dups (Spark-shaped); positional access is the durable pin.
    assert rows[0][0] != rows[0][1]


def test_p5_rows_from_arrow_empty_and_zero_column(spark: ReparkSession) -> None:
    """P5: empty table and zero-column rows do not break bulk assembly."""
    from repark.spark.dataframe import DataFrame

    empty = pa.table({"id": pa.array([], type=pa.int64())})
    assert DataFrame._rows_from_arrow_table(empty) == []
    # Zero-column empty schema (zip(*[]) would otherwise drop rows if row_count > 0).
    schema = pa.schema([])
    batch = pa.RecordBatch.from_arrays([], schema=schema)
    # 0-col 0-row is the supported empty case.
    assert DataFrame._rows_from_arrow_table(batch) == []
    assert DataFrame._rows_from_arrow_table(pa.Table.from_batches([], schema=schema)) == []
    # Live zero-column frame with rows (drop sole column) still materializes n empty Rows.
    zero_col_rows = spark.range(3).drop("id").collect()
    assert len(zero_col_rows) == 3
    assert zero_col_rows[0].__fields__ == []
    assert list(zero_col_rows[0]) == []


# ==================================================================================================
# Empty-frame matrix (oracle EMPTY FRAME section)
# ==================================================================================================


def test_empty_frame_action_matrix(spark: ReparkSession) -> None:
    empty = spark.createDataFrame([], ["id", "s"])
    assert empty.collect() == []
    assert empty.head() is None
    assert empty.head(2) == []
    assert empty.first() is None
    assert empty.take(3) == []
    assert empty.tail(2) == []
    assert empty.isEmpty() is True
    assert list(empty.toLocalIterator()) == []
