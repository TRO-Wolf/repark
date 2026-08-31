"""MERGE INTO differential rows (gap G3, record-side) — post-MERGE table vs live Spark.

**Oracle.** Every ``spark`` table / error needle below was RECORDED in record mode against live
PySpark 4.1.2 + Apache Iceberg (zulu-17, ``master("local[2]")``, ANSI on,
``spark.sql.shuffle.partitions=2``). The record driver provisions Spark with the pinned Iceberg
runtime GAV (see :data:`ICEBERG_SPARK_RUNTIME_GAV`) and a local Hadoop warehouse
catalog — vanilla Spark cannot run ``MERGE INTO`` against temp views. One multi-step recipe per
row runs on BOTH engines (create → seed → MERGE → read back), so the recipe under test and the
recipe the oracle ran are the same code path.

**Why some rows are DISCLOSURES.** When the engines agree on value AND Arrow type AND nullability,
the row is a plain equality (``repark is None``). When they honestly disagree the row pins BOTH
halves and asserts the divergence still holds. A silent CONVERGENCE goes red and forces the
disclosure to be revisited rather than laundered into "parity".

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``) through the parity
comparator, so schema name, Arrow type and nullability are part of every content assertion —
never ``show``. Error-class rows pin the error *token* both engines raise (honest class compare),
not a full stack trace.

**Re-deriving the goldens (record mode).** Provision the same extras the live parity tier uses
(parity-live sync line — load-bearing flags; dual-wired Makefile ↔ ``parity-live.yml``)::

    uv sync --locked --extra record \\
        --extra numpy --extra pandas --extra polars --extra ml-ext \\
        --no-install-package repark

Then run the record driver (JVM + pinned pyspark from the ``record`` extra; never collected by
pytest; Iceberg jar is a **record-time** dependency only via ``spark.jars.packages``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_merge_differential_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own lifecycle recipe, so the recorded
golden and the asserted recipe cannot drift apart. CI stays JVM-free.

**Entry point.** Every content row goes through the facade ``sql()`` door over a real Iceberg
table (memory catalog). The builder ``mergeInto`` path is already covered by
``test_merge_into.py``; this corpus is the SQL-MERGE result-set differential.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest
from _oracle_pins import (
    ICEBERG_RUNTIME_VERSION,
    ICEBERG_SPARK_RUNTIME_GAV,
    ICEBERG_SPARK_RUNTIME_NOTE,
    ICEBERG_SPARK_SCALA_BINARY,
    _pinned_pyspark_version,
    _spark_major_minor,
)

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Re-export oracle pins for in-module callers; the SSOT is :mod:`_oracle_pins`. The record
# driver imports GAV from ``_oracle_pins`` directly (never here).
_ICEBERG_SPARK_SCALA_BINARY = ICEBERG_SPARK_SCALA_BINARY
_ICEBERG_RUNTIME_VERSION = ICEBERG_RUNTIME_VERSION


# repark memory catalog name used by the suite; the record driver uses "local" for Spark's
# Hadoop catalog. Both are substituted into recipes via the lifecycle helper.
REPARK_CATALOG = "mem"
REPARK_NAMESPACE = "ns"

# Shared COW table properties so repark's merge mode is explicit (matches test_merge_into.py).
# Spark Iceberg 1.11 defaults accept MERGE without these; repark pins COW for determinism.
COW_TBLPROPERTIES = (
    "'format-version' = '2', "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write'"
)


# Arrow helpers


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


_I64 = pa.int64()
_STR = pa.string()


# Row shape


@dataclass(frozen=True)
class MergeDiffRow:
    """One differential row: a multi-step MERGE recipe + recorded Spark half + repark half.

    ``kind="content"`` — post-MERGE table contents on the Arrow path.
      * ``repark is None`` means the engines AGREE — plain equality against ``spark``.
      * ``repark is not None`` means DISCLOSURE: repark's actual output is pinned, and a
        convergence onto the recorded Spark output is detected and reported as one.

    ``kind="error"`` — both engines refuse the MERGE; pins the error *token* each raises.
      * ``spark_error_needle`` / ``repark_error_needle`` are substrings that must appear in the
        raised message (honest class compare — e.g. both raise ``MERGE_CARDINALITY_VIOLATION``).

    ``kind="split"`` — one engine succeeds (content) and the other refuses (error). Pins both
    halves as a surface/grammar disclosure (e.g. ``WHEN NOT MATCHED BY SOURCE``).
      * Success side: the ``spark`` or ``repark`` table is set; the other is ``None``.
      * Error side: the matching ``*_error_needle`` is set.
      * If the refuse side starts succeeding, the harness classifies CONVERGED (matches the
        success golden → flip to content equality) vs regression (committed but mismatched).
    """

    name: str
    kind: Literal["content", "error", "split"]
    # Target DDL column list (no parens): "id BIGINT, name STRING"
    target_columns: str
    seed_sql: str
    source_sql: str
    # MERGE body after ON … ; may use {target} placeholder. Source view is always merge_src.
    merge_sql: str
    # Read-back after MERGE; uses {target}. Ignored for pure-error rows that never commit.
    read_sql: str
    note: str
    spark: pa.Table | None = None
    repark: pa.Table | None = None
    spark_error_needle: str | None = None
    repark_error_needle: str | None = None
    # Lifecycle helpers take ``with_cow_props``: repark callers pass True (COW pin for
    # determinism); Spark record-mode callers pass False (Iceberg 1.11 accepts MERGE without).


# Lifecycle helper — create → seed → MERGE → read back (BOTH engines)

# Lives beside the record driver by design: this module is the recipe SSOT the driver imports.


SOURCE_VIEW = "merge_src"


def target_fqn(catalog: str, namespace: str, table: str) -> str:
    """Three-part Iceberg table name both engines accept."""
    return f"{catalog}.{namespace}.{table}"


def ensure_namespace(session: Any, catalog: str, namespace: str) -> None:
    """Create ``catalog.namespace`` if missing (idempotent on both engines)."""
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def drop_table_if_exists(session: Any, fq_table: str) -> None:
    """Drop an Iceberg table if present. Best-effort: a missing table is fine."""
    with contextlib.suppress(Exception):
        session.sql(f"DROP TABLE IF EXISTS {fq_table}")
    with contextlib.suppress(Exception):
        session.sql(f"DROP TABLE {fq_table}")


def drop_source_view(session: Any) -> None:
    """Drop the shared source temp view if the session still holds it."""
    drop_temp = getattr(session, "catalog", None)
    if drop_temp is not None and hasattr(drop_temp, "dropTempView"):
        with contextlib.suppress(Exception):
            session.catalog.dropTempView(SOURCE_VIEW)
    # Spark also accepts DROP VIEW; ignore failures.
    with contextlib.suppress(Exception):
        session.sql(f"DROP VIEW IF EXISTS {SOURCE_VIEW}")


def create_seeded_table(
    session: Any,
    *,
    fq_table: str,
    target_columns: str,
    seed_sql: str,
    with_cow_props: bool,
) -> None:
    """CREATE TABLE (Iceberg) + INSERT seed rows. Drops any prior table of the same name first."""
    drop_table_if_exists(session, fq_table)
    props = f" TBLPROPERTIES ({COW_TBLPROPERTIES})" if with_cow_props else ""
    session.sql(f"CREATE TABLE {fq_table} ({target_columns}) USING iceberg{props}")
    session.sql(f"INSERT INTO {fq_table} {seed_sql}")


def register_source(session: Any, source_sql: str) -> None:
    """Register ``merge_src`` from a SELECT via the DataFrame temp-view path (both engines)."""
    drop_source_view(session)
    frame = session.sql(source_sql)
    frame.createOrReplaceTempView(SOURCE_VIEW)


def read_table(session: Any, read_sql: str) -> pa.Table:
    """Run the read-back SQL and return Arrow (facade ``to_arrow`` or Spark ``toArrow``)."""
    frame = session.sql(read_sql)
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def run_merge_lifecycle(
    session: Any,
    row: MergeDiffRow,
    *,
    catalog: str,
    namespace: str,
    with_cow_props: bool,
) -> pa.Table:
    """Create → seed → MERGE → read back. Returns the post-MERGE Arrow table.

    On MERGE failure the exception propagates AFTER cleanup of the target table and source view,
    so a failed row leaves no stray tables in the warehouse (see
    ``test_lifecycle_cleanup_after_failed_merge``).
    """
    table = row.name
    fq_table = target_fqn(catalog, namespace, table)
    ensure_namespace(session, catalog, namespace)
    create_seeded_table(
        session,
        fq_table=fq_table,
        target_columns=row.target_columns,
        seed_sql=row.seed_sql,
        with_cow_props=with_cow_props,
    )
    register_source(session, row.source_sql)
    merge_sql = row.merge_sql.format(target=fq_table)
    try:
        session.sql(merge_sql)
        read_sql = row.read_sql.format(target=fq_table)
        return read_table(session, read_sql)
    finally:
        # Always drop the per-row target so the warehouse does not accumulate tables across rows
        # or after a failed MERGE. The source view is session-scoped; drop it too.
        drop_table_if_exists(session, fq_table)
        drop_source_view(session)


def run_merge_expect_error(
    session: Any,
    row: MergeDiffRow,
    *,
    catalog: str,
    namespace: str,
    with_cow_props: bool,
) -> str:
    """Run the lifecycle expecting MERGE to raise; return the error message text.

    Cleanup is identical to :func:`run_merge_lifecycle` — the target table is dropped even when
    MERGE fails, so a cardinality (or other) error leaves no warehouse residue.
    """
    table = row.name
    fq_table = target_fqn(catalog, namespace, table)
    ensure_namespace(session, catalog, namespace)
    create_seeded_table(
        session,
        fq_table=fq_table,
        target_columns=row.target_columns,
        seed_sql=row.seed_sql,
        with_cow_props=with_cow_props,
    )
    register_source(session, row.source_sql)
    merge_sql = row.merge_sql.format(target=fq_table)
    try:
        session.sql(merge_sql)
    except Exception as exc:  # both engines' error types; message is the pin
        return str(exc)
    finally:
        drop_table_if_exists(session, fq_table)
        drop_source_view(session)
    raise AssertionError(f"{row.name}: expected MERGE to raise, but it committed")


# The corpus (gap G3: 11 rows, budget 8-11)

ROWS: list[MergeDiffRow] = [
    # ----- 1. control: basic upsert (matched UPDATE * + not-matched INSERT *) --------------------
    MergeDiffRow(
        name="basic_upsert_update_and_insert",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        source_sql=(
            "SELECT CAST(2 AS BIGINT) AS id, 'bee' AS name UNION ALL SELECT CAST(3 AS BIGINT), 'c'"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "bee", "c"]},
        ),
        repark=None,
        note="control equality: publish-job upsert shape. Both engines agree on value AND type.",
    ),
    # ----- 2. duplicate source keys + WHEN MATCHED → cardinality error on BOTH -------------------
    MergeDiffRow(
        name="duplicate_source_keys_with_matched_raises",
        kind="error",
        target_columns="id BIGINT, name STRING",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        source_sql=(
            "SELECT CAST(2 AS BIGINT) AS id, 'x' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'y'"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id",
        spark=None,
        repark=None,
        spark_error_needle="MERGE_CARDINALITY_VIOLATION",
        repark_error_needle="MERGE_CARDINALITY_VIOLATION",
        note=(
            "duplicate source keys under a WHEN MATCHED arm: BOTH engines refuse with "
            "MERGE_CARDINALITY_VIOLATION (honest class compare — Spark's SQLSTATE 23K01 message "
            "and repark's Execution error share the token). Not a silent last-writer-wins."
        ),
    ),
    # ----- 3. duplicate source keys, INSERT-only → both insert both rows -------------------------
    MergeDiffRow(
        name="duplicate_source_keys_insert_only_commits_both",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql="SELECT CAST(1 AS BIGINT) AS id, 'a' AS name",
        source_sql=(
            "SELECT CAST(9 AS BIGINT) AS id, 'x' AS name UNION ALL SELECT CAST(9 AS BIGINT), 'y'"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id, name",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 9, 9], "name": ["a", "x", "y"]},
        ),
        repark=None,
        note=(
            "insert-only MERGE has no WHEN MATCHED arm, so the cardinality check does not fire "
            "(repark docs: check runs when any WHEN MATCHED exists). Both engines insert BOTH "
            "source rows. Order-insensitive compare; ORDER BY id, name stabilises the read."
        ),
    ),
    # ----- 4. WHEN MATCHED AND arm ordering — first-match-wins (update then delete) --------------
    MergeDiffRow(
        name="matched_and_arm_order_update_then_delete",
        kind="content",
        target_columns="id BIGINT, score BIGINT",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, CAST(10 AS BIGINT) AS score "
            "UNION ALL SELECT CAST(2 AS BIGINT), CAST(20 AS BIGINT)"
        ),
        source_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, CAST(100 AS BIGINT) AS score "
            "UNION ALL SELECT CAST(2 AS BIGINT), CAST(200 AS BIGINT)"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED AND target.score = 10 THEN UPDATE SET target.score = source.score "
            "WHEN MATCHED THEN DELETE"
        ),
        read_sql="SELECT id, score FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("score", _I64, True)],
            {"id": [1], "score": [100]},
        ),
        repark=None,
        note=(
            "first-match-wins: id=1 hits the conditional UPDATE (score=10); id=2 falls through "
            "to the unconditional DELETE. Spark allows an omitted condition only on the LAST "
            "MATCHED clause — this shape is legal on both engines."
        ),
    ),
    # ----- 5. multi-arm threshold (update high scores, delete low) --------------------------------
    MergeDiffRow(
        name="matched_and_threshold_update_or_delete",
        kind="content",
        target_columns="id BIGINT, score BIGINT",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, CAST(10 AS BIGINT) AS score "
            "UNION ALL SELECT CAST(2 AS BIGINT), CAST(20 AS BIGINT) "
            "UNION ALL SELECT CAST(3 AS BIGINT), CAST(5 AS BIGINT)"
        ),
        source_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, CAST(100 AS BIGINT) AS score "
            "UNION ALL SELECT CAST(2 AS BIGINT), CAST(200 AS BIGINT) "
            "UNION ALL SELECT CAST(3 AS BIGINT), CAST(50 AS BIGINT)"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED AND target.score >= 15 THEN UPDATE SET target.score = source.score "
            "WHEN MATCHED AND target.score < 15 THEN DELETE "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, score FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("score", _I64, True)],
            {"id": [2], "score": [200]},
        ),
        repark=None,
        note=(
            "threshold first-match: id=2 (score=20>=15) updates to 200; id=1 and id=3 "
            "(score<15) delete. Only the updated high-score row survives."
        ),
    ),
    # ----- 7. NULL merge keys — NULL = NULL is unknown, so no match ------------------------------
    MergeDiffRow(
        name="null_merge_keys_do_not_match",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name "
            "UNION ALL SELECT CAST(NULL AS BIGINT), 'n' "
            "UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        source_sql=(
            "SELECT CAST(NULL AS BIGINT) AS id, 'N' AS name "
            "UNION ALL SELECT CAST(2 AS BIGINT), 'B' "
            "UNION ALL SELECT CAST(3 AS BIGINT), 'c'"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id NULLS FIRST, name",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            # ORDER BY id NULLS FIRST, name → NULL rows sort by name ('N' < 'n' in UTF-8).
            {"id": [None, None, 1, 2, 3], "name": ["N", "n", "a", "B", "c"]},
        ),
        repark=None,
        note=(
            "SQL three-valued logic: NULL = NULL is unknown, so the target NULL row does NOT "
            "match the source NULL row. Source NULL inserts as NOT MATCHED; target NULL stays. "
            "id=2 updates to 'B'; id=3 inserts. Both engines agree."
        ),
    ),
    # ----- 8. insert-only arm (matched source rows ignored) --------------------------------------
    MergeDiffRow(
        name="insert_only_ignores_matched_source_rows",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql="SELECT CAST(1 AS BIGINT) AS id, 'a' AS name",
        source_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'X' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2], "name": ["a", "b"]},
        ),
        repark=None,
        note=(
            "insert-only: source id=1 matches but there is no WHEN MATCHED arm, so target stays "
            "'a'; source id=2 inserts. Control for the delete/update arms."
        ),
    ),
    # ----- 9. delete matched arm -----------------------------------------------------------------
    MergeDiffRow(
        name="delete_matched_removes_target_row",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        source_sql="SELECT CAST(1 AS BIGINT) AS id, 'x' AS name",
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED THEN DELETE"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [2], "name": ["b"]},
        ),
        repark=None,
        note="WHEN MATCHED THEN DELETE: id=1 removed, id=2 untouched. Both engines agree.",
    ),
    # ----- 10. conditional MATCHED update (target predicate) + NOT MATCHED insert ----------------
    MergeDiffRow(
        name="conditional_matched_update_by_target_predicate",
        kind="content",
        target_columns="id BIGINT, score BIGINT",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, CAST(5 AS BIGINT) AS score "
            "UNION ALL SELECT CAST(2 AS BIGINT), CAST(50 AS BIGINT)"
        ),
        source_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, CAST(99 AS BIGINT) AS score "
            "UNION ALL SELECT CAST(2 AS BIGINT), CAST(88 AS BIGINT) "
            "UNION ALL SELECT CAST(3 AS BIGINT), CAST(7 AS BIGINT)"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED AND target.score > 40 THEN UPDATE SET target.score = source.score "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        read_sql="SELECT id, score FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("score", _I64, True)],
            {"id": [1, 2, 3], "score": [5, 88, 7]},
        ),
        repark=None,
        note=(
            "WHEN MATCHED AND target.score > 40: id=1 (score=5) matches but predicate fails so "
            "it is left alone; id=2 updates to 88; id=3 inserts. Pins the AND-predicate path."
        ),
    ),
    # ----- 11. WHEN NOT MATCHED BY SOURCE — engines agree (DML-A) -------------------------------
    MergeDiffRow(
        name="not_matched_by_source",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        source_sql="SELECT CAST(1 AS BIGINT) AS id, 'aa' AS name",
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1], "name": ["aa"]},
        ),
        repark=None,
        spark_error_needle=None,
        repark_error_needle=None,
        note=(
            "pins: dml-a-merge-not-matched-by-source/C-002. Matched id=1 updates to 'aa'; "
            "unmatched target id=2 is deleted. Live Spark 4.1.2 + Iceberg 1.11.0 2026-08-30."
        ),
    ),
    # ----- 12. dup source keys + SINGLE unconditional MATCHED DELETE — engines agree -------------
    MergeDiffRow(
        name="dup_source_keys_unconditional_delete",
        kind="content",
        target_columns="id BIGINT, name STRING",
        seed_sql=(
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
        ),
        source_sql=(
            "SELECT CAST(2 AS BIGINT) AS id, 'x' AS name UNION ALL SELECT CAST(2 AS BIGINT), 'y'"
        ),
        merge_sql=(
            "MERGE INTO {target} AS target USING merge_src AS source "
            "ON target.id = source.id "
            "WHEN MATCHED THEN DELETE"
        ),
        read_sql="SELECT id, name FROM {target} ORDER BY id",
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1], "name": ["a"]},
        ),
        repark=None,  # engines agree — equality against the recorded Spark survivor table
        spark_error_needle=None,
        repark_error_needle=None,
        note=(
            "Exemption landed (audit M11): duplicate source keys (id=2 twice) against a SINGLE "
            "unconditional WHEN MATCHED THEN DELETE. Spark's "
            "RewriteMergeIntoTable.isCardinalityCheckNeeded is false for this exact shape — "
            "deleting a target row twice is idempotent — and repark now skips the same check and "
            "commits the delete. The recorded Spark survivor table (id=1 / name='a') is the "
            "golden. Contrast row 2 (duplicate_source_keys_with_matched_raises): with an UPDATE "
            "arm both engines still refuse."
        ),
    ),
]


# Session builders


def _repark_session(warehouse: Path) -> ReparkSession:
    """A repark session with a memory Iceberg catalog rooted at ``warehouse``."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("merge-differential-parity").getOrCreate()
    session.register_memory_catalog(REPARK_CATALOG, warehouse)
    ensure_namespace(session, REPARK_CATALOG, REPARK_NAMESPACE)
    return session


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


# The rows


@pytest.fixture
def repark_warehouse(tmp_path: Path) -> Path:
    """Per-test warehouse directory for the repark memory catalog."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return warehouse


@pytest.fixture
def repark(repark_warehouse: Path) -> Iterator[ReparkSession]:
    """Repark session with a fresh memory catalog (facade door). Yields then stops."""
    session = _repark_session(repark_warehouse)
    try:
        yield session
    finally:
        with contextlib.suppress(Exception):
            session.stop()


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_merge_differential_row(row: MergeDiffRow, repark: ReparkSession) -> None:
    """Every recorded row on the Arrow path (value AND type AND nullability) or error class.

    Content equality rows assert ``repark == Spark``. Content disclosure rows assert repark's
    pinned actual output, with failures CLASSIFIED (CONVERGED vs regression). Error rows assert
    both engines' needles appear in the raised message (honest class compare). Split rows assert
    repark still refuses with its needle AND (when ``row.spark`` is set) that the recorded Spark
    success half is well-formed; if repark starts succeeding, classify CONVERGED (matches the
    Spark golden → flip to content equality) vs regression (committed but not the recorded half).
    """
    if row.kind == "error":
        assert row.repark_error_needle is not None
        message = run_merge_expect_error(
            repark,
            row,
            catalog=REPARK_CATALOG,
            namespace=REPARK_NAMESPACE,
            with_cow_props=True,
        )
        assert row.repark_error_needle in message, (
            f"{row.name}: repark error missing {row.repark_error_needle!r}: {message!r}. {row.note}"
        )
        return

    if row.kind == "split":
        # repark refuses; Spark succeeds (spark half is the recorded golden). Drive the real
        # lifecycle so a future engine that starts accepting the surface is CLASSIFIED:
        # matching the Spark golden → CONVERGED (flip guidance); other result → regression.
        assert row.repark_error_needle is not None
        assert row.spark is not None
        try:
            actual = run_merge_lifecycle(
                repark,
                row,
                catalog=REPARK_CATALOG,
                namespace=REPARK_NAMESPACE,
                with_cow_props=True,
            )
        except Exception as exc:  # both engines' error types; message is the pin
            message = str(exc)
            assert row.repark_error_needle in message, (
                f"{row.name}: repark was expected to refuse with {row.repark_error_needle!r}, "
                f"got: {message!r}. {row.note}"
            )
            # The recorded Spark half must remain a well-formed disclosure partner (non-empty).
            assert row.spark.num_rows >= 1, f"{row.name}: spark golden is empty — re-record"
            return

        # MERGE committed — repark no longer refuses this surface.
        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED — repark now succeeds with the "
                f"RECORDED SPARK output, so this split disclosure is stale. Do not delete the "
                f"row: flip it to a content equality row (kind='content', repark=None, clear the "
                f"error needle) and record the convergence. {row.note}"
            )
        raise AssertionError(
            f"{row.name}: repark no longer refuses (MERGE committed) but the result does NOT "
            f"match the recorded Spark golden — this is a regression/partial change, not a clean "
            f"convergence. Re-derive both halves in record mode (see this module's docstring) "
            f"before flipping the pin. {row.note}"
        )

    # kind == "content"
    assert row.spark is not None
    actual = run_merge_lifecycle(
        repark,
        row,
        catalog=REPARK_CATALOG,
        namespace=REPARK_NAMESPACE,
        with_cow_props=True,
    )

    if row.repark is None:
        assert_frames_equal(actual, row.spark)
        return

    try:
        assert_frames_equal(actual, row.repark)
    except FrameMismatchError as mismatch:
        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED — repark now produces the RECORDED "
                f"SPARK output, so this disclosure is stale. Do not delete the row: flip it to an "
                f"equality row (repark=None) and record the convergence. {row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the recorded "
            f"Spark golden either — this is a regression, not a convergence. Re-derive both "
            f"halves in record mode (see this module's docstring) before touching the pin. "
            f"{row.note}"
        ) from mismatch

    assert _frames_differ(row.repark, row.spark), (
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure at "
        f"all — flip it to an equality row (repark=None) or re-record it. {row.note}"
    )


def test_merge_differential_row_set_covers_g3_budget() -> None:
    """The pin budget is part of the unit — corpus size and class coverage are pinned."""
    # Ceiling 11: the dup-key + single unconditional MATCHED DELETE disclosure row. The
    # budget stays a real gate, not an open drawer.
    assert 8 <= len(ROWS) <= 11, f"G3 budget 8-11 differential rows (got {len(ROWS)})"
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    kinds = {row.kind for row in ROWS}
    assert "content" in kinds, "at least one content row"
    assert "error" in kinds, "at least one error-class row (duplicate source keys)"

    names = {row.name for row in ROWS}
    required_substrings = [
        "duplicate_source",
        "arm_order",
        "null_merge",
        "insert_only",
        "delete_matched",
        "not_matched_by_source",
    ]
    for needle in required_substrings:
        assert any(needle in name for name in names), f"missing coverage for {needle!r}"

    # At least one plain equality control (not an all-disclosure corpus).
    equality = [row for row in ROWS if row.kind == "content" and row.repark is None]
    assert equality, (
        "at least one content equality row must assert repark == Spark — an all-disclosure "
        "corpus cannot tell agreement from a broken comparator"
    )

    nmbs = [row for row in ROWS if "not_matched_by_source" in row.name]
    assert len(nmbs) == 1 and nmbs[0].kind == "content"
    assert nmbs[0].repark is None
    assert nmbs[0].repark_error_needle is None


def test_lifecycle_cleanup_after_failed_merge(
    repark: ReparkSession, repark_warehouse: Path
) -> None:
    """A failed MERGE leaves no stray tables in the warehouse (lifecycle helper cleanup).

    The cardinality-error row creates a target, MERGEs, fails, and the helper drops the target
    in ``finally``; ``listTables`` must not contain that table name, and a second successful
    row must not see residue.
    """
    error_row = next(row for row in ROWS if row.kind == "error")
    message = run_merge_expect_error(
        repark,
        error_row,
        catalog=REPARK_CATALOG,
        namespace=REPARK_NAMESPACE,
        with_cow_props=True,
    )
    assert "MERGE_CARDINALITY_VIOLATION" in message

    tables = repark.catalog.listTables(f"{REPARK_CATALOG}.{REPARK_NAMESPACE}")
    managed = [table.name for table in tables if not getattr(table, "isTemporary", False)]
    assert error_row.name not in managed, (
        f"failed MERGE left stray table {error_row.name!r} in warehouse; managed={managed}"
    )

    # A subsequent content row still runs clean on the same session/warehouse.
    control = next(row for row in ROWS if row.name == "basic_upsert_update_and_insert")
    actual = run_merge_lifecycle(
        repark,
        control,
        catalog=REPARK_CATALOG,
        namespace=REPARK_NAMESPACE,
        with_cow_props=True,
    )
    assert_frames_equal(actual, control.spark)  # type: ignore[arg-type]

    tables_after = repark.catalog.listTables(f"{REPARK_CATALOG}.{REPARK_NAMESPACE}")
    managed_after = [
        table.name for table in tables_after if not getattr(table, "isTemporary", False)
    ]
    assert control.name not in managed_after, (
        f"successful row left table {control.name!r} after lifecycle cleanup; "
        f"managed={managed_after}"
    )
    # Warehouse directory may hold Iceberg metadata residue on disk; the catalog table list is
    # the SSOT for "no stray tables".
    _ = repark_warehouse  # fixture presence documents the warehouse root


def test_iceberg_gav_pin_is_exact_spark_minor() -> None:
    """Record-time GAV Spark-minor is derived from the pinned pyspark version.

    The expected ``{major}.{minor}_{scala}`` token is computed from
    ``python/repark-parity/pyproject.toml``'s ``pyspark==X.Y.Z`` record-extra pin — never a
    restated constant that would stay green if the pin and the GAV drifted apart.
    """
    pinned = _pinned_pyspark_version()
    major_minor = _spark_major_minor(pinned)
    expected_token = f"{major_minor}_{_ICEBERG_SPARK_SCALA_BINARY}"
    assert expected_token in ICEBERG_SPARK_RUNTIME_GAV, (
        f"prefer iceberg-spark-runtime whose Spark minor matches pinned pyspark {pinned} "
        f"(expected token {expected_token!r} in {ICEBERG_SPARK_RUNTIME_GAV!r}); if forced to a "
        f"mismatched minor, every golden must carry an oracle-environment caveat — never silent"
    )
    assert ICEBERG_SPARK_RUNTIME_GAV.endswith(f":{_ICEBERG_RUNTIME_VERSION}"), (
        f"GAV must pin Iceberg runtime {_ICEBERG_RUNTIME_VERSION}, "
        f"got {ICEBERG_SPARK_RUNTIME_GAV!r}"
    )
    assert major_minor in ICEBERG_SPARK_RUNTIME_NOTE, (
        f"NOTE must name the derived Spark minor {major_minor!r}: {ICEBERG_SPARK_RUNTIME_NOTE!r}"
    )
    assert pinned in ICEBERG_SPARK_RUNTIME_NOTE, (
        f"NOTE must name the pinned pyspark {pinned!r}: {ICEBERG_SPARK_RUNTIME_NOTE!r}"
    )
