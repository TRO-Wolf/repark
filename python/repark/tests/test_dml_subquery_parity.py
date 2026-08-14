"""DELETE/UPDATE subquery-predicate differential rows (defect G3-E8) — the fix's goldens, now.

**Why this corpus exists before the fix.** `DELETE FROM t WHERE id IN (SELECT …)` used to empty the
whole table, and `UPDATE … WHERE id IN (SELECT …)` used to rewrite every row — silently, with a
successful Iceberg commit. The predicate is lost at DataFusion's DML planning boundary
(``extract_dml_filters`` recovers nothing from the semi/anti join the optimizer decorrelated the
subquery into) and an empty filter list is the provider's spelling of *no WHERE clause*. The
engine now **refuses** the whole class (the G3-E8 valve, both SQL doors); the capability itself
returns in a later unit. These rows record what live Spark does for each spelling **now**, so the
fix unit inherits its oracle instead of re-deriving one under time pressure.

**Row kinds.** Residual subquery rows are **split**: repark refuses (its needle is pinned) and the
Spark half is the recorded post-DML table. **content** rows include two non-subquery equality
controls plus the executed holes (`DELETE … IN` / `NOT IN` + NULL trap, `[NOT] EXISTS`
± correlation, correlated IN, and identity `UPDATE … IN`, recorded against live Spark 4.1.2).
Without the non-subquery controls a comparator that always "passed" would go unnoticed.

**The NULL trap is recorded, not reasoned.** ``NOT IN`` over a subquery whose result contains NULL
is SQL's three-valued-logic trap: every row's test evaluates to UNKNOWN, so Spark matches
*nothing*. The rows named ``*_with_null_key`` pin what Spark ACTUALLY did on the recorded run —
the fix unit must reproduce that, not the intuitive answer.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2 +
Apache Iceberg (zulu-17, ``master("local[2]")``, ANSI on, ``spark.sql.shuffle.partitions=2``) on
2026-08-11. The record driver provisions the pinned Iceberg runtime GAV (see
:data:`ICEBERG_SPARK_RUNTIME_GAV`) and a local Hadoop warehouse catalog — vanilla Spark cannot run
Iceberg DML. One multi-step recipe per row runs on BOTH engines (create → seed → create key table →
seed → DML → read back), so the recipe under test and the recipe the oracle ran are the same code.

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``) through the parity comparator,
so schema name, Arrow type and nullability are part of every content assertion — never ``show``.
The refuse half pins the guard's OWN message tokens, never a stack trace.

**Re-deriving the goldens (record mode).** The driver that recorded every Spark half is committed
beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_dml_subquery_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own lifecycle recipe, so the recorded
golden and the asserted recipe cannot drift apart. It needs a JVM + ``pyspark``
(``uv sync --extra record``) and is never collected by pytest; CI stays JVM-free.

**When the fix lands.** Each split row flips to ``kind="content"`` with ``repark=None`` and the
needle cleared. The classifier below prints exactly that instruction when repark stops refusing
and its result matches the recorded Spark golden (CONVERGED); a non-matching result is reported as
a regression instead, so a partial fix cannot be laundered into parity.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# ==================================================================================================
# Oracle environment pin (record-time only; never a CI dependency)
# ==================================================================================================

# Same ruling as the MERGE corpus (docs/history/hardening-h1/n2-merge-ledger.md §1.3): an
# iceberg-spark-runtime whose Spark minor matches 4.1 exactly, under zulu-17 + PySpark 4.1.2.
ICEBERG_SPARK_RUNTIME_GAV = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

# repark memory catalog name used by the suite; the record driver uses "local" for Spark's Hadoop
# catalog. Both are substituted into recipes via the lifecycle helper.
REPARK_CATALOG = "mem"
REPARK_NAMESPACE = "ns"

# Copy-on-write everywhere so the row-level write mode is explicit rather than default-dependent.
# `format-version` is deliberately absent: repark refuses it as a reserved property, and pinning
# it is not this corpus's subject — the write MODE is.
COW_TBLPROPERTIES = "'write.delete.mode' = 'copy-on-write', 'write.update.mode' = 'copy-on-write'"

# The token every G3-E8 refusal carries, in BOTH doors. A split row asserts THIS, not a generic
# failure — a vacuous "it raised something" pin would go green on a typo in the SQL.
G3E8_NEEDLE = "subquery predicates are silently mis-executed"


# ==================================================================================================
# Arrow helpers
# ==================================================================================================


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


_I64 = pa.int64()
_STR = pa.string()

# Every row shares one target shape and one seed, so the only variable across rows is the
# predicate — which is the thing under test.
TARGET_COLUMNS = "id BIGINT, name STRING"
TARGET_SEED = (
    "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name "
    "UNION ALL SELECT CAST(2 AS BIGINT), 'b' "
    "UNION ALL SELECT CAST(3 AS BIGINT), 'c'"
)
KEYS_COLUMNS = "id BIGINT, name STRING"
# The one-key source: id=2, with a name ('K') that appears in no target row.
KEYS_SEED = "SELECT CAST(2 AS BIGINT) AS id, 'K' AS name"
# The NULL-bearing source: the three-valued-logic trap for NOT IN.
KEYS_SEED_WITH_NULL = (
    "SELECT CAST(2 AS BIGINT) AS id, 'K' AS name UNION ALL SELECT CAST(NULL AS BIGINT), 'N'"
)
KEYS_SEED_EMPTY = (
    "SELECT CAST(2 AS BIGINT) AS id, 'K' AS name WHERE CAST(1 AS INT) = CAST(0 AS INT)"
)
KEYS_SEED_NONE = "SELECT CAST(99 AS BIGINT) AS id, 'K' AS name"
KEYS_SEED_ALL = (
    "SELECT CAST(1 AS BIGINT) AS id, 'K' AS name "
    "UNION ALL SELECT CAST(2 AS BIGINT), 'K' "
    "UNION ALL SELECT CAST(3 AS BIGINT), 'K'"
)
KEYS_SEED_DUPS = (
    "SELECT CAST(1 AS BIGINT) AS id, 'K' AS name UNION ALL SELECT CAST(1 AS BIGINT), 'K'"
)
TARGET_SEED_WITH_NULL = (
    "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name "
    "UNION ALL SELECT CAST(2 AS BIGINT), 'b' "
    "UNION ALL SELECT CAST(NULL AS BIGINT), 'n'"
)
TARGET_SEED_DUPS = (
    "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name "
    "UNION ALL SELECT CAST(1 AS BIGINT), 'a' "
    "UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
)
EXISTS_CORRELATED = (
    "DELETE FROM {target} WHERE EXISTS (SELECT 1 FROM {keys} k WHERE k.id = {target}.id)"
)
NOT_EXISTS_CORRELATED = (
    "DELETE FROM {target} WHERE NOT EXISTS (SELECT 1 FROM {keys} k WHERE k.id = {target}.id)"
)
EXISTS_UNCORRELATED = "DELETE FROM {target} WHERE EXISTS (SELECT 1 FROM {keys})"
NOT_EXISTS_UNCORRELATED = "DELETE FROM {target} WHERE NOT EXISTS (SELECT 1 FROM {keys})"


# ==================================================================================================
# Row shape
# ==================================================================================================


@dataclass(frozen=True)
class DmlSubqueryRow:
    """One differential row: a DELETE/UPDATE recipe + the recorded Spark half + repark's half.

    ``kind="split"`` — repark REFUSES (G3-E8 valve) and Spark succeeds. ``spark`` is the recorded
    post-DML table; ``repark_error_needle`` is the substring repark's refusal must contain. If
    repark ever stops refusing, the failure is CLASSIFIED: matching the Spark golden is a
    CONVERGENCE (flip the row to ``content``), anything else is a regression.

    ``kind="content"`` — a plain equality control: both engines run the statement and must agree
    on value AND Arrow type AND nullability.
    """

    name: str
    kind: Literal["split", "content"]
    dml_sql: str
    """The statement under test. ``{target}`` / ``{keys}`` are substituted with the FQNs."""
    read_sql: str
    """Post-DML read-back; uses ``{target}``."""
    note: str
    keys_seed_sql: str = KEYS_SEED
    target_seed_sql: str = TARGET_SEED
    spark: pa.Table | None = None
    repark_error_needle: str | None = None


# ==================================================================================================
# Lifecycle helper — create → seed → create keys → seed → DML → read back (BOTH engines)
# ==================================================================================================
#
# Lives in this module by design: it is the recipe SSOT the record driver imports, so there is one
# recipe and not two copies (the same rule the MERGE corpus follows).


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


def create_seeded_table(session: Any, *, fq_table: str, columns: str, seed_sql: str) -> None:
    """CREATE TABLE (Iceberg, COW) + INSERT seed rows. Drops any prior table of that name first.

    Explicit DDL + INSERT rather than CTAS: CTAS infers different column types and nullability on
    the two engines (the N-2 lesson), and this corpus's subject is the predicate, not inference.
    """
    drop_table_if_exists(session, fq_table)
    session.sql(
        f"CREATE TABLE {fq_table} ({columns}) USING iceberg TBLPROPERTIES ({COW_TBLPROPERTIES})"
    )
    session.sql(f"INSERT INTO {fq_table} {seed_sql}")


def read_table(session: Any, read_sql: str) -> pa.Table:
    """Run the read-back SQL and return Arrow (facade ``to_arrow`` or Spark ``toArrow``)."""
    frame = session.sql(read_sql)
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def run_dml_lifecycle(
    session: Any, row: DmlSubqueryRow, *, catalog: str, namespace: str
) -> pa.Table:
    """Create → seed → create keys → seed → run the DML → read back the target.

    Both per-row tables are dropped in ``finally``, so a refused or failed statement leaves no
    stray tables behind (pinned by :func:`test_lifecycle_cleanup_after_refused_dml`).
    """
    fq_target = target_fqn(catalog, namespace, row.name)
    fq_keys = target_fqn(catalog, namespace, f"{row.name}_keys")
    ensure_namespace(session, catalog, namespace)
    create_seeded_table(
        session, fq_table=fq_target, columns=TARGET_COLUMNS, seed_sql=row.target_seed_sql
    )
    create_seeded_table(session, fq_table=fq_keys, columns=KEYS_COLUMNS, seed_sql=row.keys_seed_sql)
    try:
        session.sql(row.dml_sql.format(target=fq_target, keys=fq_keys))
        return read_table(session, row.read_sql.format(target=fq_target))
    finally:
        drop_table_if_exists(session, fq_target)
        drop_table_if_exists(session, fq_keys)


def run_dml_expect_error(session: Any, row: DmlSubqueryRow, *, catalog: str, namespace: str) -> str:
    """Run the lifecycle expecting the DML to raise; return the error message text."""
    fq_target = target_fqn(catalog, namespace, row.name)
    fq_keys = target_fqn(catalog, namespace, f"{row.name}_keys")
    ensure_namespace(session, catalog, namespace)
    create_seeded_table(
        session, fq_table=fq_target, columns=TARGET_COLUMNS, seed_sql=row.target_seed_sql
    )
    create_seeded_table(session, fq_table=fq_keys, columns=KEYS_COLUMNS, seed_sql=row.keys_seed_sql)
    try:
        session.sql(row.dml_sql.format(target=fq_target, keys=fq_keys))
    except Exception as exc:  # both engines' error types; the message is the pin
        return str(exc)
    finally:
        drop_table_if_exists(session, fq_target)
        drop_table_if_exists(session, fq_keys)
    raise AssertionError(f"{row.name}: expected the DML to raise, but it committed")


READ_BACK = "SELECT id, name FROM {target} ORDER BY id"


# ==================================================================================================
# The corpus (defect G3-E8: executed IN/NOT IN/EXISTS + residual UPDATE/correlated-IN splits)
# ==================================================================================================

ROWS: list[DmlSubqueryRow] = [
    # ----- 1. control: DELETE with a non-subquery predicate (both engines agree) ----------------
    DmlSubqueryRow(
        name="control_delete_without_subquery",
        kind="content",
        dml_sql="DELETE FROM {target} WHERE id = 2",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 3], "name": ["a", "c"]},
        ),
        note=(
            "equality control: the ordinary DELETE path is untouched by the G3-E8 valve. Without "
            "this row an all-split corpus could not tell a working comparator from a broken one."
        ),
    ),
    # ----- 2. control: UPDATE with a non-subquery predicate --------------------------------------
    DmlSubqueryRow(
        name="control_update_without_subquery",
        kind="content",
        dml_sql="UPDATE {target} SET name = 'z' WHERE id = 2",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "z", "c"]},
        ),
        note="equality control for the UPDATE arm — the valve must not widen into this shape.",
    ),
    # ----- 3. DELETE … IN (subquery) — the confirmed repro ---------------------------------------
    DmlSubqueryRow(
        name="delete_in_subquery",
        kind="content",
        dml_sql="DELETE FROM {target} WHERE id IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 3], "name": ["a", "c"]},
        ),
        note=(
            "THE intake repro (G3-E8): pre-guard this statement emptied the whole table. The "
            "identity path now deletes exactly the matching row — content equality vs the "
            "recorded Spark golden."
        ),
    ),
    # ----- 4. DELETE … NOT IN (subquery) ---------------------------------------------------------
    DmlSubqueryRow(
        name="delete_not_in_subquery",
        kind="content",
        dml_sql="DELETE FROM {target} WHERE id NOT IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [2], "name": ["b"]},
        ),
        note=(
            "anti-join spelling: identity SELECT keeps only the key row (Spark `{2}`). Flipped "
            "split → content when PR-2 proved DataFusion 3VL matches the recorded golden."
        ),
    ),
    # ----- 5. DELETE … NOT IN (subquery WITH A NULL) — the three-valued-logic trap ---------------
    DmlSubqueryRow(
        name="delete_not_in_subquery_with_null_key",
        kind="content",
        keys_seed_sql=KEYS_SEED_WITH_NULL,
        dml_sql="DELETE FROM {target} WHERE id NOT IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note=(
            "SQL three-valued logic: with a NULL in the subquery result, `id NOT IN (…)` is "
            "UNKNOWN for EVERY row, so Spark deletes NOTHING. The golden is what live Spark did — "
            "the identity SELECT must reproduce this, not the intuitive 'delete the non-matching "
            "rows'."
        ),
    ),
    # ----- 6. DELETE … EXISTS (correlated, matching some) ----------------------------------------
    DmlSubqueryRow(
        name="delete_exists_correlated",
        kind="content",
        dml_sql=EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 3], "name": ["a", "c"]},
        ),
        note=(
            "correlated EXISTS: identity SELECT is a per-row semi-join. Flipped split → content "
            "when PR-3 proved the executed SELECT matches live Spark `{1,3}`."
        ),
    ),
    # ----- 7. DELETE … NOT EXISTS (correlated, matching some) ------------------------------------
    DmlSubqueryRow(
        name="delete_not_exists_correlated",
        kind="content",
        dml_sql=NOT_EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [2], "name": ["b"]},
        ),
        note=(
            "NOT EXISTS is NULL-safe where NOT IN is not — contrast with "
            "`delete_not_in_subquery_with_null_key`, which deletes nothing under the same data."
        ),
    ),
    # ----- 8. DELETE … IN (correlated subquery) --------------------------------------------------
    DmlSubqueryRow(
        name="delete_correlated_in_subquery",
        kind="content",
        dml_sql=(
            "DELETE FROM {target} WHERE id IN (SELECT k.id FROM {keys} k WHERE k.id = {target}.id)"
        ),
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 3], "name": ["a", "c"]},
        ),
        note=(
            "correlated IN — recorded equivalent to correlated EXISTS on every fixture "
            "(same remaining `{1,3}`). Flipped split → content in PR-4."
        ),
    ),
    # ----- EXISTS family extras (recorded 2026-08-13 vs Spark 4.1.2) ------------------------------
    DmlSubqueryRow(
        name="delete_exists_uncorrelated",
        kind="content",
        dml_sql=EXISTS_UNCORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [], "name": []},
        ),
        note="uncorrelated nonempty EXISTS is TRUE for every row — Spark deletes the whole table.",
    ),
    DmlSubqueryRow(
        name="delete_exists_uncorrelated_empty",
        kind="content",
        keys_seed_sql=KEYS_SEED_EMPTY,
        dml_sql=EXISTS_UNCORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note=(
            "uncorrelated empty EXISTS is FALSE — Spark deletes nothing. Not a match-all shortcut."
        ),
    ),
    DmlSubqueryRow(
        name="delete_not_exists_uncorrelated",
        kind="content",
        dml_sql=NOT_EXISTS_UNCORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note="uncorrelated nonempty NOT EXISTS is FALSE for every row — Spark deletes nothing.",
    ),
    DmlSubqueryRow(
        name="delete_not_exists_uncorrelated_empty",
        kind="content",
        keys_seed_sql=KEYS_SEED_EMPTY,
        dml_sql=NOT_EXISTS_UNCORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [], "name": []},
        ),
        note="uncorrelated empty NOT EXISTS is TRUE — Spark deletes every row.",
    ),
    DmlSubqueryRow(
        name="delete_exists_correlated_none",
        kind="content",
        keys_seed_sql=KEYS_SEED_NONE,
        dml_sql=EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note="correlated EXISTS matching none — keys={99}, Spark deletes nothing.",
    ),
    DmlSubqueryRow(
        name="delete_exists_correlated_all",
        kind="content",
        keys_seed_sql=KEYS_SEED_ALL,
        dml_sql=EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [], "name": []},
        ),
        note="correlated EXISTS matching all — keys={1,2,3}, Spark empties the table.",
    ),
    DmlSubqueryRow(
        name="delete_exists_correlated_empty",
        kind="content",
        keys_seed_sql=KEYS_SEED_EMPTY,
        dml_sql=EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note="correlated EXISTS over an empty keys table — Spark deletes nothing.",
    ),
    DmlSubqueryRow(
        name="delete_not_exists_correlated_none",
        kind="content",
        keys_seed_sql=KEYS_SEED_NONE,
        dml_sql=NOT_EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [], "name": []},
        ),
        note="correlated NOT EXISTS matching none — Spark deletes every row.",
    ),
    DmlSubqueryRow(
        name="delete_not_exists_correlated_all",
        kind="content",
        keys_seed_sql=KEYS_SEED_ALL,
        dml_sql=NOT_EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note="correlated NOT EXISTS matching all — Spark deletes nothing.",
    ),
    DmlSubqueryRow(
        name="delete_not_exists_correlated_empty",
        kind="content",
        keys_seed_sql=KEYS_SEED_EMPTY,
        dml_sql=NOT_EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [], "name": []},
        ),
        note="correlated NOT EXISTS over empty keys — Spark deletes every row.",
    ),
    DmlSubqueryRow(
        name="delete_exists_correlated_null_keys",
        kind="content",
        target_seed_sql=TARGET_SEED_WITH_NULL,
        keys_seed_sql=KEYS_SEED_WITH_NULL,
        dml_sql=EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [None, 1], "name": ["n", "a"]},
        ),
        note=(
            "NULL = NULL is unknown, so a NULL target id does not EXISTS-match a NULL key. "
            "Spark remaining `{NULL, 1}` (deleted id=2). Recorded 2026-08-13."
        ),
    ),
    DmlSubqueryRow(
        name="delete_not_exists_correlated_null_keys",
        kind="content",
        target_seed_sql=TARGET_SEED_WITH_NULL,
        keys_seed_sql=KEYS_SEED_WITH_NULL,
        dml_sql=NOT_EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [2], "name": ["b"]},
        ),
        note=(
            "NOT EXISTS + NULL keys: NULL id has no TRUE match so it is deleted. "
            "This is NOT the NOT IN 3VL trap."
        ),
    ),
    DmlSubqueryRow(
        name="delete_exists_correlated_duplicates",
        kind="content",
        target_seed_sql=TARGET_SEED_DUPS,
        keys_seed_sql=KEYS_SEED_DUPS,
        dml_sql=EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [2], "name": ["b"]},
        ),
        note="duplicate keys and duplicate target rows: EXISTS deletes every matching copy.",
    ),
    DmlSubqueryRow(
        name="delete_not_exists_correlated_duplicates",
        kind="content",
        target_seed_sql=TARGET_SEED_DUPS,
        keys_seed_sql=KEYS_SEED_DUPS,
        dml_sql=NOT_EXISTS_CORRELATED,
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 1], "name": ["a", "a"]},
        ),
        note="NOT EXISTS keeps every non-matching copy (both id=1 rows).",
    ),
    DmlSubqueryRow(
        name="update_in_subquery",
        kind="content",
        dml_sql="UPDATE {target} SET name = 'z' WHERE id IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "z", "c"]},
        ),
        note=(
            "identity UPDATE IN: Spark rewrites only the matching row. Flipped split → content "
            "in PR-4 against the recorded golden."
        ),
    ),
    DmlSubqueryRow(
        name="update_in_subquery_multi_set",
        kind="content",
        dml_sql="UPDATE {target} SET name = 'z', id = id + 10 WHERE id IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 3, 12], "name": ["a", "c", "z"]},
        ),
        note="multi-column scalar SET on the identity-UPDATE path (recorded 2026-08-14).",
    ),
    DmlSubqueryRow(
        name="update_in_subquery_expr",
        kind="content",
        dml_sql="UPDATE {target} SET name = concat(name, '_x') WHERE id IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b_x", "c"]},
        ),
        note=(
            "SET with a scalar expression (concat) — D-4 SET-subquery stays ungated/unimplemented."
        ),
    ),
    DmlSubqueryRow(
        name="update_in_subquery_empty",
        kind="content",
        keys_seed_sql=KEYS_SEED_EMPTY,
        dml_sql="UPDATE {target} SET name = 'z' WHERE id IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        note="empty subquery ⇒ IN is FALSE; Spark rewrites nothing.",
    ),
    # ----- 10. UPDATE … WHERE NOT IN (subquery WITH A NULL) --------------------------------------
    DmlSubqueryRow(
        name="update_not_in_subquery_with_null_key",
        kind="split",
        keys_seed_sql=KEYS_SEED_WITH_NULL,
        dml_sql="UPDATE {target} SET name = 'z' WHERE id NOT IN (SELECT id FROM {keys})",
        read_sql=READ_BACK,
        spark=_table(
            [("id", _I64, True), ("name", _STR, True)],
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        ),
        repark_error_needle=G3E8_NEEDLE,
        note=(
            "the UPDATE twin of the NULL trap: NOT IN over a NULL-bearing subquery matches no "
            "row, so Spark rewrites nothing. Recorded, not reasoned."
        ),
    ),
]


# ==================================================================================================
# Session builders + fixtures
# ==================================================================================================


def _repark_session(warehouse: Path) -> ReparkSession:
    """A repark session with a memory Iceberg catalog rooted at ``warehouse``."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("dml-subquery-parity").getOrCreate()
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


# ==================================================================================================
# The rows
# ==================================================================================================


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_dml_subquery_row(row: DmlSubqueryRow, repark: ReparkSession) -> None:
    """Every recorded row, on the Arrow path (value AND type AND nullability) or refuse class.

    Content rows assert ``repark == Spark``.

    Split rows drive the REAL lifecycle — create, seed, run the statement — so a future engine
    that starts accepting the surface is CLASSIFIED rather than silently green: a result matching
    the recorded Spark golden is a CONVERGENCE (flip the row to content equality), anything else
    is a regression that must be re-derived in record mode before any pin moves.
    """
    if row.kind == "content":
        assert row.spark is not None
        actual = run_dml_lifecycle(repark, row, catalog=REPARK_CATALOG, namespace=REPARK_NAMESPACE)
        assert_frames_equal(actual, row.spark)
        return

    # kind == "split": repark refuses; the Spark half is the recorded golden.
    assert row.repark_error_needle is not None
    assert row.spark is not None
    try:
        actual = run_dml_lifecycle(repark, row, catalog=REPARK_CATALOG, namespace=REPARK_NAMESPACE)
    except Exception as exc:  # the refusal — its message is the pin
        message = str(exc)
        assert row.repark_error_needle in message, (
            f"{row.name}: repark was expected to refuse with {row.repark_error_needle!r}, "
            f"got: {message!r}. {row.note}"
        )
        assert row.spark.num_rows >= 1, f"{row.name}: spark golden is empty — re-record"
        return

    if not _frames_differ(actual, row.spark):
        raise AssertionError(
            f"{row.name}: repark and Spark have CONVERGED — repark now runs this subquery "
            f"predicate and produces the RECORDED SPARK result, so the G3-E8 split disclosure is "
            f"stale. Do not delete the row: flip it to kind='content', clear "
            f"repark_error_needle, and record the convergence (the underlying fix has landed). "
            f"{row.note}"
        )
    raise AssertionError(
        f"{row.name}: repark no longer refuses (the statement committed) but the result does NOT "
        f"match the recorded Spark golden — this is a regression or a partial fix, not a clean "
        f"convergence, and it is exactly the silent-data-loss shape G3-E8 named. Re-derive both "
        f"halves in record mode (see this module's docstring) before flipping the pin. {row.note}"
    )


def test_refusal_leaves_every_row_untouched(repark: ReparkSession) -> None:
    """The point of the guard: a refused statement must not have written anything.

    The parametrized split rows drop their tables in ``finally``, so they cannot observe the
    post-refusal contents. This row keeps the target alive and reads it back — the assertion that
    would have caught the original defect (the table came back EMPTY).
    """
    row = next(item for item in ROWS if item.name == "update_not_in_subquery_with_null_key")
    fq_target = target_fqn(REPARK_CATALOG, REPARK_NAMESPACE, "guard_residue")
    fq_keys = target_fqn(REPARK_CATALOG, REPARK_NAMESPACE, "guard_residue_keys")
    ensure_namespace(repark, REPARK_CATALOG, REPARK_NAMESPACE)
    create_seeded_table(repark, fq_table=fq_target, columns=TARGET_COLUMNS, seed_sql=TARGET_SEED)
    create_seeded_table(repark, fq_table=fq_keys, columns=KEYS_COLUMNS, seed_sql=KEYS_SEED)
    try:
        with pytest.raises(Exception, match="subquery predicates are silently mis-executed"):
            repark.sql(row.dml_sql.format(target=fq_target, keys=fq_keys))

        after = read_table(repark, READ_BACK.format(target=fq_target))
        assert_frames_equal(
            after,
            _table(
                [("id", _I64, True), ("name", _STR, True)],
                {"id": [1, 2, 3], "name": ["a", "b", "c"]},
            ),
        )
    finally:
        drop_table_if_exists(repark, fq_target)
        drop_table_if_exists(repark, fq_keys)


def test_lifecycle_cleanup_after_refused_dml(repark: ReparkSession) -> None:
    """A refused row leaves no stray tables in the warehouse (lifecycle helper cleanup)."""
    row = next(item for item in ROWS if item.kind == "split")
    message = run_dml_expect_error(repark, row, catalog=REPARK_CATALOG, namespace=REPARK_NAMESPACE)
    assert G3E8_NEEDLE in message

    tables = repark.catalog.listTables(f"{REPARK_CATALOG}.{REPARK_NAMESPACE}")
    managed = [table.name for table in tables if not getattr(table, "isTemporary", False)]
    assert row.name not in managed, f"refused DML left stray table {row.name!r}; managed={managed}"
    assert f"{row.name}_keys" not in managed, f"refused DML left the key table; managed={managed}"


def test_dml_subquery_row_set_covers_the_g3e8_budget() -> None:
    """The pin budget is part of the unit — corpus size and class coverage are pinned.

    Coverage assertions are NAME-gated so a control row cannot satisfy them (the tautological-pin
    lesson: a family pin that any row can green is not a pin).
    """
    assert 20 <= len(ROWS) <= 32, (
        f"G3-E8 budget 20-32 rows after PR-4 family close (got {len(ROWS)})"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    splits = [row for row in ROWS if row.kind == "split"]
    assert 1 <= len(splits) <= 10, f"1-10 residual split rows required (got {len(splits)})"

    controls = [row for row in ROWS if row.kind == "content"]
    assert controls, (
        "at least one content equality control must assert repark == Spark — an all-split corpus "
        "cannot tell agreement from a broken comparator"
    )
    for control in controls:
        # Whitespace-tolerant: `( SELECT`, `(\n  SELECT` and `(SELECT` are all subqueries.
        # PR-1 flips `delete_in_subquery` to content — that one spelling is allowed to
        # carry a subquery. Every other content row must stay subquery-free (panel L1 N-3).
        has_subquery = re.search(r"\(\s*SELECT", control.dml_sql, re.IGNORECASE) is not None
        if control.name in {
            "delete_in_subquery",
            "delete_not_in_subquery",
            "delete_not_in_subquery_with_null_key",
            "delete_exists_correlated",
            "delete_not_exists_correlated",
            "delete_exists_uncorrelated",
            "delete_exists_uncorrelated_empty",
            "delete_not_exists_uncorrelated",
            "delete_not_exists_uncorrelated_empty",
            "delete_exists_correlated_none",
            "delete_exists_correlated_all",
            "delete_exists_correlated_empty",
            "delete_not_exists_correlated_none",
            "delete_not_exists_correlated_all",
            "delete_not_exists_correlated_empty",
            "delete_exists_correlated_null_keys",
            "delete_not_exists_correlated_null_keys",
            "delete_exists_correlated_duplicates",
            "delete_not_exists_correlated_duplicates",
            "delete_correlated_in_subquery",
            "update_in_subquery",
            "update_in_subquery_multi_set",
            "update_in_subquery_expr",
            "update_in_subquery_empty",
        }:
            assert has_subquery, f"{control.name}: the content hole must keep its subquery"
            assert control.repark_error_needle is None
            continue
        assert not has_subquery, (
            f"{control.name}: a content control must NOT carry a subquery predicate — it would "
            f"be refused, and the row would be silently mis-classified"
        )

    names = {row.name for row in splits}
    for needle in ("update_not_in_subquery_with_null_key",):
        assert needle in names, f"missing split coverage for {needle!r}"
    contents = {row.name for row in ROWS if row.kind == "content"}
    assert "delete_in_subquery" in contents, "IN-DELETE is the PR-1 content hole"
    assert "delete_not_in_subquery" in contents, "NOT IN-DELETE is the PR-2 content hole"
    assert "delete_not_in_subquery_with_null_key" in contents, "NOT IN NULL trap is content"
    assert "delete_exists_correlated" in contents, "correlated EXISTS is the PR-3 content hole"
    assert "delete_not_exists_correlated" in contents, "correlated NOT EXISTS is content"
    assert "delete_exists_uncorrelated" in contents, "uncorrelated EXISTS is content"
    assert "delete_not_exists_uncorrelated" in contents, "uncorrelated NOT EXISTS is content"
    assert "delete_correlated_in_subquery" in contents, "correlated IN is the PR-4 content hole"
    assert "update_in_subquery" in contents, "UPDATE IN is the PR-4 content hole"
    assert "update_in_subquery_multi_set" in contents, "multi-column UPDATE IN is content"
    assert "update_in_subquery_expr" in contents, "SET-expression UPDATE IN is content"

    # The NULL trap needs BOTH verbs — DELETE now executes; UPDATE stays refused.
    null_rows = [row for row in ROWS if row.name.endswith("_with_null_key")]
    assert len(null_rows) >= 2, "NOT IN with a NULL key must be pinned for DELETE *and* UPDATE"
    for row in null_rows:
        assert "NOT IN" in row.dml_sql, f"{row.name}: the NULL trap row must use NOT IN"
        assert "NULL" in row.keys_seed_sql, f"{row.name}: its key table must actually seed a NULL"
        assert row.spark is not None and row.spark.num_rows == 3, (
            f"{row.name}: the recorded Spark golden must show NOTHING matched (all 3 rows "
            f"survive) — if it does not, the trap was not exercised"
        )
    delete_trap = next(row for row in null_rows if row.name.startswith("delete_"))
    update_trap = next(row for row in null_rows if row.name.startswith("update_"))
    assert delete_trap.kind == "content", "DELETE NULL trap is the PR-2 content hole"
    assert update_trap.kind == "split", "UPDATE NOT IN NULL trap stays refused (not this PR's hole)"

    # Every split row pins the guard's OWN needle, never a generic failure.
    for row in splits:
        assert row.repark_error_needle == G3E8_NEEDLE, (
            f"{row.name}: a split row must pin the G3-E8 valve's own message"
        )
        assert row.spark is not None, f"{row.name}: a split row needs its recorded Spark half"


def test_iceberg_gav_pin_is_exact_spark_minor() -> None:
    """The declared GAV constant is shaped as an exact Spark-4.1 Iceberg runtime coordinate.

    Scope, stated honestly (panel L2 N3): this asserts the CONSTANT in this module, and nothing
    else. It cannot detect an oracle mismatch — the record driver reads the same constant, so the
    two agree by construction, and no assertion here observes the jar Spark actually loaded. What
    it does buy is that a hand-edit to a different Spark minor (or a snapshot/RC coordinate) reds
    instead of silently re-recording the corpus against a different runtime.

    The mechanical fix — one GAV helper that both the constant and the live session read, so the
    claim becomes checkable — is W-2b's single-home GAV work, in flight in another lane. This
    docstring is deliberately narrowed rather than the test rewritten, so the two lanes do not
    collide (see ``task/g3e8-guard-ledger.md`` §10.7, CP-8).
    """
    assert "4.1_2.13" in ICEBERG_SPARK_RUNTIME_GAV
    assert ICEBERG_SPARK_RUNTIME_GAV.endswith(":1.11.0")
