"""r24 SB1 — trust boundaries: array cardinality ceilings (SEC-01) + local DDL gate (SEC-02).

Facade entry point (`F.array_repeat` / `F.sequence` / `F.repeat`) and free-SQL path both refuse
planner-visible expansions over ``repark.sql.maxArrayElements`` (default 10_000_000) with a
catchable :class:`~repark.errors.AnalysisException` that names the conf.
"""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException

_MAX_ARRAY_ELEMENTS_KEY = "repark.sql.maxArrayElements"
_ALLOW_LOCAL_FS_DDL_KEY = "repark.sql.allowLocalFilesystemDDL"
# One past the default ceiling (10_000_000).
_OVER_CEILING = 10_000_001


@pytest.fixture
def session() -> ReparkSession:
    spark = (
        ReparkSession.builder.appName("test_sec_boundaries")
        .config("repark.memory.limit.gb", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_facade_array_repeat_over_ceiling_raises_analysis_exception(
    session: ReparkSession,
) -> None:
    """SEC-01 facade path (column.rs): literal count above default refuses naming conf."""
    frame = session.range(1)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY) as raised:
        frame.select(F.array_repeat(F.lit(1), _OVER_CEILING).alias("a")).collect()
    assert "array_repeat" in str(raised.value).lower() or _MAX_ARRAY_ELEMENTS_KEY in str(
        raised.value
    )


def test_facade_sequence_over_ceiling_raises_analysis_exception(
    session: ReparkSession,
) -> None:
    """SEC-01 facade path: sequence(start, stop) cardinality over ceiling."""
    frame = session.range(1)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.select(F.sequence(F.lit(1), F.lit(_OVER_CEILING)).alias("s")).collect()


def test_facade_repeat_over_ceiling_raises_analysis_exception(
    session: ReparkSession,
) -> None:
    """SEC-01 facade path: string repeat count over ceiling."""
    frame = session.range(1)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.select(F.repeat(F.lit("x"), _OVER_CEILING).alias("r")).collect()


def test_free_sql_array_repeat_over_ceiling_raises_analysis_exception(
    session: ReparkSession,
) -> None:
    """SEC-01 free-SQL path: SELECT array_repeat(…) over ceiling."""
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        session.sql(f"SELECT cardinality(array_repeat(1, {_OVER_CEILING})) AS n").collect()


def test_free_sql_array_repeat_under_ceiling_ok(session: ReparkSession) -> None:
    """Under-ceiling expansion still plans and runs."""
    rows = session.sql("SELECT cardinality(array_repeat(1, 3)) AS n").collect()
    assert rows[0]["n"] == 3


def test_copy_to_local_outside_warehouse_refuses_by_default(
    session: ReparkSession, tmp_path
) -> None:
    """SEC-02: bare COPY TO a local path outside warehouse refuses naming conf."""
    dest = tmp_path / "blocked_copy"
    with pytest.raises(AnalysisException, match=_ALLOW_LOCAL_FS_DDL_KEY):
        session.sql(f"COPY (SELECT 1 AS a) TO '{dest}' STORED AS PARQUET").collect()
    assert not dest.exists()


def test_copy_to_allowed_when_conf_true(session: ReparkSession, tmp_path) -> None:
    """SEC-02: opt-in conf permits local COPY outside warehouse (builder rebuild)."""
    # Runtime conf store does not re-attach the Rust extension; rebuild with conf set.
    session.stop()
    spark = (
        ReparkSession.builder.appName("test_sec_boundaries_allow")
        .config("repark.memory.limit.gb", "1")
        .config(_ALLOW_LOCAL_FS_DDL_KEY, "true")
        .getOrCreate()
    )
    try:
        dest = tmp_path / "allowed_copy"
        spark.sql(f"COPY (SELECT 1 AS a) TO '{dest}' STORED AS PARQUET").collect()
        assert dest.exists() or any(dest.parent.glob("**/*"))
    finally:
        spark.stop()


def test_copy_to_inside_registered_warehouse_is_grandfathered(
    session: ReparkSession, tmp_path
) -> None:
    """SEC-02 grandfather: local COPY TO *under a registered catalog warehouse* stays allowed.

    The gate defaults to refusing local-filesystem DDL, but must not break local-dev CTAS into
    the warehouse the session itself registered (r24 greylight Q22). The exemption is scoped to
    that warehouse root — a sibling path outside it still refuses, so the grandfather cannot be
    used as a general escape. Rust-side coverage is `local_fs_ddl.rs`; this pins the Python
    entry point, which is the one local-dev actually uses.
    """
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    session.register_memory_catalog("grandfathered", str(warehouse))

    inside = warehouse / "inside.parquet"
    session.sql(f"COPY (SELECT 1 AS a) TO '{inside}' STORED AS PARQUET").collect()

    outside = tmp_path / "outside.parquet"
    with pytest.raises(AnalysisException, match=_ALLOW_LOCAL_FS_DDL_KEY):
        session.sql(f"COPY (SELECT 1 AS a) TO '{outside}' STORED AS PARQUET").collect()


def test_runtime_conf_set_cannot_loosen_local_fs_ddl_gate(session: ReparkSession, tmp_path) -> None:
    """SEC-02 fails closed: a *runtime* ``conf.set`` cannot open the local-filesystem DDL gate.

    The gate reads its flag from the DataFusion config extension attached at session build, so
    the opt-in is build-time only (see ``test_copy_to_allowed_when_conf_true``, which rebuilds
    the builder). Both spellings are pinned because the extension stores snake_case while the
    user-facing key is camelCase.

    Known limitation (r25 seed): ``conf.set`` *reads back* the value it did not honor, rather
    than refusing the way Spark refuses a static-conf write. That is a truthfulness bug, not a
    hole — the failure direction is closed, which is what this pin guards. If runtime conf
    plumbing lands later, this test must be rewritten deliberately, not deleted.
    """
    for key in (_ALLOW_LOCAL_FS_DDL_KEY, "repark.sql.allow_local_filesystem_ddl"):
        session.conf.set(key, "true")
        dest = tmp_path / f"runtime_{key.rsplit('.', maxsplit=1)[-1]}.parquet"
        with pytest.raises(AnalysisException, match=_ALLOW_LOCAL_FS_DDL_KEY):
            session.sql(f"COPY (SELECT 1 AS a) TO '{dest}' STORED AS PARQUET").collect()


def test_typed_writer_works_by_default_but_free_sql_to_same_path_still_refuses(
    session: ReparkSession, tmp_path
) -> None:
    """SEC-02 is scoped to *free* SQL — the typed writer must keep working with the gate on.

    Regression pin (r24 morning). The facade implements `df.write.<fmt>(path)` as a generated
    `COPY … TO`, which shares the SQL path the gate guards, so the gate refused every local
    write: 31 writer tests failed on the combined branch though each unit gate was green.

    The narrowing must not become a general escape, so this pins both halves:
      1. the typed writer succeeds at its own destination, with the gate at its default; and
      2. *free* SQL COPY TO a different local path still refuses.

    `SECURITY.md` "Input surfaces" gates the **Free SQL** rows only, and lists the typed
    `spark.read.*` API as un-gated — this pins the code to that documented boundary.
    """
    dest = tmp_path / "typed_writer_out"
    session.range(3).write.mode("overwrite").option("header", "true").csv(str(dest))
    assert dest.exists(), "typed writer must produce its destination with the gate at default"
    assert session.read.option("header", "true").csv(str(dest)).count() == 3

    # Deliberately a SIBLING of the write destination: trusting the destination's *parent*
    # would open every neighbouring path to free SQL, so this must still refuse.
    elsewhere = tmp_path / "free_sql_escape.parquet"
    with pytest.raises(AnalysisException, match=_ALLOW_LOCAL_FS_DDL_KEY):
        session.sql(f"COPY (SELECT 1 AS a) TO '{elsewhere}' STORED AS PARQUET").collect()
