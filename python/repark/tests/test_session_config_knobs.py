"""Engine-knob config validation at the REAL user entry point (audit SAF-006 / SAF-007).

The entry point users migrate on is ``ReparkSession.builder.config(key, value).getOrCreate()`` —
not ``_native.PyReparkSession(...)`` (pinned in ``crates/repark-python/tests/bindings.rs``) and not
the Rust ``ReparkSession::builder()`` (pinned in ``crates/repark-session/src/lib.rs``). Every knob
family and **both spellings in each family** are pinned here, on the ``to_arrow`` path (value AND
Arrow type) for the accepting cases and on the exception CLASS + verbatim message for the refusing
ones — on the fresh-build path AND on the ``getOrCreate`` REUSE path.

Oracle: **live PySpark 4.1.2 under zulu-17, re-run during the audit-G3 remediation pass — not
memory, and not inferred from the jar's raw ``checkValue`` argument.** Captures:

* ``spark.sql.execution.arrow.maxRecordsPerBatch = 0`` → ``getOrCreate`` OK, ``conf.get`` returns
  ``'0'``, ``SELECT id FROM range(5)`` returns 5 rows; and on the reuse path (``S4``) it likewise
  does not raise. ``SQLConf`` declares this key with **no** ``checkValue`` and documents "If set to
  zero or negative there is no limit". Zero is a legal PySpark program; repark must not refuse it.
* ``spark.sql.shuffle.partitions = 0`` on a FRESH process → ``getOrCreate`` returns OK (Spark
  validates lazily; the raise only surfaces at the first ``sessionState`` touch). repark validates
  eagerly — the deliberate TIMING divergence disclosed on ``ReparkSession.Builder.config``.
* ``spark.sql.shuffle.partitions = 0`` against an **already-active** session (either
  ``spark.conf.set`` or ``SparkSession.builder.config(...).getOrCreate()``, which PySpark routes
  through ``setConfString``) raises, verbatim::

      pyspark.errors.exceptions.captured.IllegalArgumentException:
      [INVALID_CONF_VALUE.REQUIREMENT] The value '0' in the config
      "spark.sql.shuffle.partitions" is invalid. The value of
      spark.sql.shuffle.partitions must be positive SQLSTATE: 22022

  (The ``must be positive`` payload carries **no** trailing period — byte-checked against
  ``SQLConf$.class`` in ``spark-catalyst_2.13-4.1.2.jar``.) The same capture also shows PySpark
  *applying* builder options on reuse: 200 → 7 for a valid value.

RECORDED DELTAS vs that live message (repark emits the shape verbatim otherwise):

1. the trailing ``SQLSTATE: 22022`` is dropped — no repark error carries SQLSTATE, exactly as
   recorded for ``[AMBIGUOUS_REFERENCE]`` / ``[INVALID_SAVE_MODE]``;
2. the repark-native spellings (``repark.target.partitions`` / ``repark.memory.limit.gb``) have no
   Spark counterpart — Spark would ignore them entirely — so repark emits the same shape with the
   repark key substituted;
3. on reuse repark **validates but does not apply** the knob (engine knobs are fixed at build); it
   warns "some configuration may not apply" and returns the active session unchanged.

MUTATION: apply either key's rule to the other family and this module reds in both directions.
"""

from __future__ import annotations

import warnings

import pyarrow as pa
import pytest

from repark import ReparkSession, _native
from repark.errors import IllegalArgumentException, PySparkException
from repark.session import (
    _BATCH_SIZE_KEYS,
    _MEMORY_LIMIT_KEYS,
    _TARGET_PARTITIONS_KEYS,
    _reset_dropin_warnings_for_tests,
)

#: The live pyspark 4.1.2 raise for ``spark.sql.shuffle.partitions = 0`` against an active session,
#: minus the ``SQLSTATE: 22022`` suffix (recorded delta 1). Asserted by EQUALITY below.
_SPARK_412_SHUFFLE_ZERO = (
    "[INVALID_CONF_VALUE.REQUIREMENT] The value '0' in the config "
    '"spark.sql.shuffle.partitions" is invalid. '
    "The value of spark.sql.shuffle.partitions must be positive"
)


def _assert_session_runs(spark: ReparkSession) -> None:
    """Prove the built session actually executes on the Arrow path (value AND type)."""
    table = spark.sql("SELECT 1 AS n").to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column("n").to_pylist() == [1]
    assert table.schema.field("n").type == pa.int64()


def _live_session() -> ReparkSession:
    """Build and register a plain active session, so the next ``getOrCreate`` takes the reuse path.

    The autouse ``conftest`` fixture clears ``_active_session`` around every test, so a case that
    wants the reuse path must establish one itself — that gap is exactly what hid the G3 defect.
    """
    spark = ReparkSession.builder.getOrCreate()
    _assert_session_runs(spark)
    return spark


# ---------------------------------------------------------------------------------------------
# Batch size — Spark's documented "no limit" sentinel (0 / negative) is LEGAL and must not raise.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("key", _BATCH_SIZE_KEYS)
@pytest.mark.parametrize("value", ["0", "-1"])
def test_batch_size_no_limit_sentinel_builds_and_warns(key: str, value: str) -> None:
    # SAF-006 DEFECT 1: a hard refusal here would break a legal PySpark program (live-verified
    # above). The knob is accepted, the session builds and runs; the one-time UserWarning discloses
    # that repark cannot emit unbounded Arrow batches, so the engine default batching applies.
    _reset_dropin_warnings_for_tests()
    with pytest.warns(UserWarning, match=r"no limit"):
        spark = ReparkSession.builder.config(key, value).getOrCreate()
    assert isinstance(spark, ReparkSession)
    _assert_session_runs(spark)
    spark.stop()


@pytest.mark.parametrize("key", _BATCH_SIZE_KEYS)
def test_batch_size_no_limit_disclosure_warns_once_per_process(key: str) -> None:
    # Warn-once semantics match the OTH-010 master disclosure: a re-armed process warns again,
    # a second session in the same process does not.
    _reset_dropin_warnings_for_tests()
    with pytest.warns(UserWarning, match=r"no limit"):
        first = ReparkSession.builder.config(key, "0").getOrCreate()
    first.stop()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        second = ReparkSession.builder.config(key, "0").getOrCreate()
    assert not [w for w in caught if "no limit" in str(w.message)]
    _assert_session_runs(second)
    second.stop()


@pytest.mark.parametrize("key", _BATCH_SIZE_KEYS)
def test_batch_size_positive_applies_without_disclosure(key: str) -> None:
    # The discriminator for the sentinel branch: a positive value is forwarded to the engine and
    # never warns (if the branch swallowed every value, this reds).
    _reset_dropin_warnings_for_tests()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        spark = ReparkSession.builder.config(key, "1024").getOrCreate()
    assert not [w for w in caught if "no limit" in str(w.message)]
    _assert_session_runs(spark)
    spark.stop()


@pytest.mark.parametrize("key", _BATCH_SIZE_KEYS)
def test_batch_size_sentinel_disclosure_fires_on_the_getorcreate_reuse_path(key: str) -> None:
    # G3-C2: the reuse short-circuit used to swallow this — a user setting the sentinel against a
    # live session was never told repark cannot honour it. Knob resolution now runs BEFORE the
    # short-circuit, so the disclosure fires on both paths.
    # MUTATION: move `_resolve_batch_size()` back below the `_active_session` short-circuit → RED.
    _reset_dropin_warnings_for_tests()
    existing = _live_session()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        reused = ReparkSession.builder.config(key, "0").getOrCreate()
    assert reused is existing, "the sentinel must not force a rebuild"
    assert [w for w in caught if "no limit" in str(w.message)], (
        "the SAF-006 batch-sentinel disclosure must not be swallowed by the reuse path"
    )
    _assert_session_runs(reused)
    reused.stop()


def test_native_constructor_still_refuses_zero_batch_size() -> None:
    # The Spark sentinel is translated by the FACADE; the native engine knob keeps its `>= 1`
    # contract (DataFusion has no unbounded batch size), and refuses with the same class the
    # facade contracts. MUTATION: make the native accept 0 and this reds.
    with pytest.raises(IllegalArgumentException, match=r"batch_size must be >= 1"):
        _native.PyReparkSession(batch_size=0)


def test_native_constructor_still_refuses_zero_target_partitions() -> None:
    # G3-C5: the twin of the batch-size boundary pin. Without it, reverting the PyO3
    # `if let Some(0) = target_partitions` guard back to the silent `.filter(|&p| p > 0)` is
    # invisible to the Python suite. MUTATION: restore that filter and rebuild → RED.
    with pytest.raises(IllegalArgumentException, match=r"target_partitions must be >= 1"):
        _native.PyReparkSession(target_partitions=0)


# ---------------------------------------------------------------------------------------------
# Shuffle partitions — Spark's `checkValue(_ > 0)` key: 0 / negative are config errors.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("key", _TARGET_PARTITIONS_KEYS)
@pytest.mark.parametrize("value", ["0", "-1"])
def test_target_partitions_non_positive_raises_illegal_argument(key: str, value: str) -> None:
    # Spark parity: `spark.sql.shuffle.partitions` must be positive (live oracle above). The class
    # is IllegalArgumentException — for the NEGATIVE case this also pins that the value never
    # reaches the native `Option<usize>` argument, which would raise a bare OverflowError.
    with pytest.raises(IllegalArgumentException) as raised:
        ReparkSession.builder.config(key, value).getOrCreate()
    assert str(raised.value) == (
        f"[INVALID_CONF_VALUE.REQUIREMENT] The value '{value}' in the config "
        f'"{key}" is invalid. The value of {key} must be positive'
    )
    assert isinstance(raised.value, PySparkException)
    assert isinstance(raised.value, RuntimeError)
    assert not isinstance(raised.value, ValueError)
    assert not isinstance(raised.value, OverflowError)


def test_shuffle_partitions_zero_message_is_live_spark_412_verbatim() -> None:
    # G3-C1: the Spark-spelled key reproduces live pyspark 4.1.2's raise BYTE FOR BYTE, minus the
    # recorded SQLSTATE delta. The old string (`'0' in <key> is invalid. … must be positive.`)
    # was the Spark 3.x `ConfigBuilder.checkValue` shape and drifted from the mandated 4.1.2
    # oracle. MUTATION: restore that shape → RED (error class prefix, quoted key, and the absent
    # trailing period are each load-bearing here).
    with pytest.raises(IllegalArgumentException) as raised:
        ReparkSession.builder.config("spark.sql.shuffle.partitions", "0").getOrCreate()
    assert str(raised.value) == _SPARK_412_SHUFFLE_ZERO
    assert str(raised.value).startswith("[INVALID_CONF_VALUE.REQUIREMENT] ")
    assert not str(raised.value).endswith("."), "Spark's checkValue payload has no trailing period"
    assert "SQLSTATE" not in str(raised.value), "recorded delta: repark carries no SQLSTATE"


@pytest.mark.parametrize("key", _TARGET_PARTITIONS_KEYS)
@pytest.mark.parametrize("value", ["0", "-1"])
def test_target_partitions_non_positive_raises_on_the_getorcreate_reuse_path(
    key: str, value: str
) -> None:
    # G3-C2, the defect this unit came back for: live pyspark 4.1.2 raises for
    # `spark.sql.shuffle.partitions=0` against an ALREADY-ACTIVE session (captured verbatim in the
    # module docstring), because `getOrCreate` applies builder options via `setConfString`. repark
    # previously short-circuited on `_active_session` first and silently accepted it — strictly
    # LESS strict than Spark on the ubiquitous notebook / long-lived-process path.
    # MUTATION: move knob resolution back below the `_active_session` short-circuit → RED.
    existing = _live_session()
    with pytest.raises(IllegalArgumentException) as raised:
        ReparkSession.builder.config(key, value).getOrCreate()
    assert str(raised.value) == (
        f"[INVALID_CONF_VALUE.REQUIREMENT] The value '{value}' in the config "
        f'"{key}" is invalid. The value of {key} must be positive'
    )
    # The refusal must not have torn down the session the user already had.
    _assert_session_runs(existing)
    existing.stop()


@pytest.mark.parametrize("key", _TARGET_PARTITIONS_KEYS)
def test_valid_knob_on_the_reuse_path_still_returns_the_active_session(key: str) -> None:
    # The discriminator for validate-before-short-circuit: a LEGAL value must not start rebuilding
    # sessions. It warns (engine knobs are fixed at build — the recorded delta vs PySpark, which
    # actually applies the value) and hands back the same object.
    existing = _live_session()
    with pytest.warns(UserWarning, match=r"some configuration may not apply"):
        reused = ReparkSession.builder.config(key, "4").getOrCreate()
    assert reused is existing
    _assert_session_runs(reused)
    reused.stop()


@pytest.mark.parametrize("key", _TARGET_PARTITIONS_KEYS)
def test_target_partitions_positive_builds(key: str) -> None:
    spark = ReparkSession.builder.config(key, "4").getOrCreate()
    _assert_session_runs(spark)
    spark.stop()


# ---------------------------------------------------------------------------------------------
# Memory limit — repark-only knob: 0 opts out of the bounded pool, negative is a config error.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("key", _MEMORY_LIMIT_KEYS)
def test_memory_limit_gb_zero_opts_out_and_still_builds(key: str) -> None:
    # SAF-007 must not have made `0` unreachable: it is the documented opt-out of the bounded
    # FairSpillPool (the Rust twin pins the pool itself is Infinite), and the session still runs.
    spark = ReparkSession.builder.config(key, "0").getOrCreate()
    _assert_session_runs(spark)
    spark.stop()


@pytest.mark.parametrize("key", _MEMORY_LIMIT_KEYS)
def test_memory_limit_gb_negative_raises_illegal_argument(key: str) -> None:
    # A negative budget is a config error, not PyO3's OverflowError on `Option<usize>`. This is a
    # repark-only key (recorded delta 2), so it borrows Spark's shape with the repark key.
    with pytest.raises(IllegalArgumentException, match=r"must not be negative") as raised:
        ReparkSession.builder.config(key, "-1").getOrCreate()
    assert str(raised.value) == (
        f"[INVALID_CONF_VALUE.REQUIREMENT] The value '-1' in the config \"{key}\" is invalid. "
        f"The value of {key} must not be negative (0 opts out of the bounded memory pool)"
    )
    assert isinstance(raised.value, PySparkException)
    assert not isinstance(raised.value, OverflowError)


@pytest.mark.parametrize("key", _MEMORY_LIMIT_KEYS)
def test_memory_limit_gb_negative_raises_on_the_getorcreate_reuse_path(key: str) -> None:
    # The third key family gets the same reuse-path pin: validation runs before the short-circuit
    # for every family, not just the one the defect was reported against.
    existing = _live_session()
    with pytest.raises(IllegalArgumentException, match=r"must not be negative"):
        ReparkSession.builder.config(key, "-1").getOrCreate()
    _assert_session_runs(existing)
    existing.stop()


@pytest.mark.parametrize("key", _MEMORY_LIMIT_KEYS)
def test_memory_limit_gb_one_is_below_no_floor_and_builds(key: str) -> None:
    # SAF-007 reachability: the smallest non-zero budget this entry point can express is 1 GiB,
    # which is far above the engine's 1 MiB floor — so the floor is unreachable from Python
    # (the Rust twin `memory_limit_gb_never_lands_below_the_floor` pins the arithmetic).
    spark = ReparkSession.builder.config(key, "1").getOrCreate()
    _assert_session_runs(spark)
    spark.stop()
