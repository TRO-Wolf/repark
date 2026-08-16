"""r21 T2 — ExternalSorter / datafusion conf passthrough / export error UX.

Measure-first diagnosis (hour-0, synthetic OHLCV + 17 float cols, no AWS):

* Default pool is FairSpillPool, RAM-relative (``clamp(0.6 * detected, 1 MiB, 8 GiB)``).
* Under ``repark.memory.limit.gb=1``, a 2M-row x ~23-col reverse sort fails with
  DataFusion pool-pressure class text (``Resources exhausted`` / ``not enough
  memory``; operator may be ExternalSorter *or* SortPreservingMergeExec) naming
  ``fair(pool_size: …)`` and usually ``datafusion.runtime.memory_limit`` /
  ``sort_spill_reservation_bytes``.
* Disconfirming measurement WIN: large growth requests are FairSpillPool pressure
  on a wide projection against the default/small pool — fix altitude is conf
  surface + error UX, not a plan rewrite.
* ``spark.conf.set("datafusion.*")`` was facade-local only; this module pins the
  forward-to-engine path, one-truth vs builder memory, and clean PySparkException shape.

Oracle: engine message text is DataFusion's (not hand-computed). Conf round-trip is
get/set equality on the facade store after a successful engine SET.
"""

from __future__ import annotations

import re
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pytest

import repark.spark.session as session_module
from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.errors import IllegalArgumentException, PySparkException


def _clear_active() -> None:
    session_module._active_session = None


@pytest.fixture(autouse=True)
def _isolate_session() -> None:
    _clear_active()
    yield
    _clear_active()


def _wide_frame(spark: ReparkSession, n_rows: int):
    """Synthetic OHLCV + 17 float cols (operator shape; invented values only)."""
    base = spark.range(n_rows)
    frame = base.select(
        F.col("id").alias("ts"),
        (F.col("id") % 1000).cast("double").alias("open"),
        (F.col("id") % 1000 + 1).cast("double").alias("high"),
        (F.col("id") % 1000 - 1).cast("double").alias("low"),
        (F.col("id") % 1000).cast("double").alias("close"),
        (F.col("id") % 5000).cast("double").alias("volume"),
    )
    for index in range(17):
        frame = frame.withColumn(
            f"f{index}",
            (F.col("close") * (index + 1) + F.col("ts") % (index + 3)).cast("double"),
        )
    return frame


# ---------------------------------------------------------------------------------------------
# datafusion.* conf allow-list — get/set round-trip + refuse-loud unknown
# ---------------------------------------------------------------------------------------------


def test_datafusion_execution_batch_size_conf_round_trip() -> None:
    spark = ReparkSession.builder.getOrCreate()
    spark.conf.set("datafusion.execution.batch_size", "4096")
    assert spark.conf.get("datafusion.execution.batch_size") == "4096"
    # Engine accepted the SET — SHOW ALL (needs information_schema) reflects the live value.
    spark._ensure_information_schema()
    shown = {row["name"]: row["value"] for row in spark.sql("SHOW ALL").to_arrow().to_pylist()}
    assert shown.get("datafusion.execution.batch_size") == "4096"
    # Second set + get (facade store + engine) still round-trips.
    spark.conf.set("datafusion.execution.batch_size", "2048")
    assert spark.conf.get("datafusion.execution.batch_size") == "2048"


def test_datafusion_runtime_memory_limit_conf_round_trip_and_pool() -> None:
    """conf.set forwards memory_limit; OOM text shows the new FairSpillPool size."""
    spark = ReparkSession.builder.config("repark.memory.limit.gb", "1").getOrCreate()
    spark.conf.set("datafusion.runtime.memory_limit", "256M")
    assert spark.conf.get("datafusion.runtime.memory_limit") == "256M"
    # Deterministic pool pressure on any box (CI runners have few cores → few partitions →
    # small spill reservations): pin 128 partitions so reservations alone exceed the pool.
    spark.conf.set("datafusion.execution.target_partitions", "128")

    frame = _wide_frame(spark, 2_000_000)
    with pytest.raises(PySparkException) as raised:
        frame.sort(F.col("close").desc()).to_arrow()
    message = str(raised.value)
    assert "Resources exhausted" in message or "not enough memory" in message.lower()
    # Live pool after conf.set is 256 MiB FairSpillPool (not the builder 1 GiB, not greedy).
    assert re.search(r"pool_size:\s*256\.0\s*MB", message), message
    assert "fair(" in message.lower(), message
    assert "greedy(" not in message.lower(), message
    assert "datafusion.runtime.memory_limit" in message
    assert "repark.memory.limit.gb" in message  # REPARK conf hint


def test_datafusion_unknown_key_refuses_loud() -> None:
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set("datafusion.nope.missing_key", "1")
    message = str(raised.value)
    assert "INVALID_CONF_VALUE" in message
    assert "datafusion.nope.missing_key" in message
    # Facade store must not keep a rejected key.
    assert spark.conf.get("datafusion.nope.missing_key", None) is None


def test_datafusion_malformed_key_refuses_loud() -> None:
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set("datafusion.;drop", "1")
    assert "datafusion." in str(raised.value)


def test_datafusion_noncanonical_case_refuses_loud_no_store() -> None:
    """Mixed-case lookalike must not become a silent facade-only twin (octo T2 C2)."""
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set("DataFusion.execution.batch_size", "4096")
    message = str(raised.value)
    assert "INVALID_CONF_VALUE" in message
    assert "DataFusion.execution.batch_size" in message
    assert spark.conf.get("DataFusion.execution.batch_size", None) is None
    assert spark.conf.get("datafusion.execution.batch_size", None) is None


def test_datafusion_padded_key_refuses_loud_no_store() -> None:
    """Leading/trailing whitespace lookalike refuses — no store-only twin (octo T2 C2)."""
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set(" datafusion.execution.batch_size", "4096")
    assert "datafusion" in str(raised.value).lower()
    assert spark.conf.get(" datafusion.execution.batch_size", None) is None


def test_datafusion_trailing_newline_key_refuses_loud_no_store_no_engine() -> None:
    """Trailing ``\\n`` must not pass the key regex (Python ``$`` hole) — extra-octo T2 E1-1.

    Pre-fix: ``datafusion.execution.batch_size\\n`` matched ``…$``, SQL SET still updated the
    live option (newline-as-whitespace), and the facade stored a non-canonical twin while
    ``get(canonical)`` stayed ``None``.
    """
    spark = ReparkSession.builder.getOrCreate()
    # Capture pre-SET engine value so we can prove the refuse path did not mutate it.
    spark._ensure_information_schema()
    shown_before = {
        row["name"]: row["value"] for row in spark.sql("SHOW ALL").to_arrow().to_pylist()
    }
    before = shown_before.get("datafusion.execution.batch_size")
    newline_key = "datafusion.execution.batch_size\n"
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set(newline_key, "8192")
    message = str(raised.value)
    assert "INVALID_CONF_VALUE" in message
    assert "datafusion" in message.lower()
    assert spark.conf.get(newline_key, None) is None
    assert spark.conf.get("datafusion.execution.batch_size", None) is None
    assert not any(key.endswith("\n") for key in spark.conf.getAll)
    shown_after = {
        row["name"]: row["value"] for row in spark.sql("SHOW ALL").to_arrow().to_pylist()
    }
    assert shown_after.get("datafusion.execution.batch_size") == before


def test_datafusion_set_value_quote_escape_no_injection() -> None:
    """Value is single-quoted + quote-doubled; engine parse fails closed (octo T2 C4)."""
    from repark.spark.session import _format_datafusion_set_sql

    assert (
        _format_datafusion_set_sql("datafusion.execution.batch_size", "a'b")
        == "SET datafusion.execution.batch_size = 'a''b'"
    )
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set("datafusion.execution.batch_size", "4096'; SELECT 1")
    assert "INVALID_CONF_VALUE" in str(raised.value)
    assert spark.conf.get("datafusion.execution.batch_size", None) is None


def test_runtime_repark_memory_limit_gb_refuses_loud() -> None:
    """repark.memory.limit.gb is build-time only — conf.set must not lie (octo T2 C3)."""
    spark = ReparkSession.builder.config("repark.memory.limit.gb", "1").getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set("repark.memory.limit.gb", "8")
    message = str(raised.value)
    assert "build-time" in message.lower() or "getOrCreate" in message
    assert "datafusion.runtime.memory_limit" in message
    # Facade store unchanged (still the builder snapshot value).
    assert spark.conf.get("repark.memory.limit.gb") == "1"


def test_builder_datafusion_memory_limit_alone_applies() -> None:
    """Builder-only datafusion.runtime.memory_limit re-sizes the pool (no repark twin)."""
    spark = ReparkSession.builder.config("datafusion.runtime.memory_limit", "256M").getOrCreate()
    assert spark.conf.get("datafusion.runtime.memory_limit") == "256M"
    # Deterministic pool pressure on any box (CI runners have few cores → few partitions →
    # small spill reservations): pin 128 partitions so reservations alone exceed the pool.
    spark.conf.set("datafusion.execution.target_partitions", "128")
    frame = _wide_frame(spark, 2_000_000)
    with pytest.raises(PySparkException) as raised:
        frame.sort(F.col("close").desc()).to_arrow()
    message = str(raised.value)
    assert re.search(r"pool_size:\s*256\.0\s*MB", message), message
    assert "fair(" in message.lower(), message
    assert "greedy(" not in message.lower(), message


def test_dual_memory_knobs_refuse_loud() -> None:
    """One truth: builder must not carry both repark.memory.limit.gb and DF memory_limit."""
    with pytest.raises(IllegalArgumentException) as raised:
        (
            ReparkSession.builder.config("repark.memory.limit.gb", "2")
            .config("datafusion.runtime.memory_limit", "4G")
            .getOrCreate()
        )
    message = str(raised.value)
    assert "repark.memory.limit.gb" in message
    assert "datafusion.runtime.memory_limit" in message
    assert "FairSpillPool" in message or "same" in message.lower()


# ---------------------------------------------------------------------------------------------
# Error UX — clean PySparkException, no pyarrow dynamic-source wrapper
# ---------------------------------------------------------------------------------------------


def test_sort_oom_error_is_pyspark_exception_with_df_message_and_hint() -> None:
    spark = ReparkSession.builder.config("repark.memory.limit.gb", "1").getOrCreate()
    # Deterministic pool pressure on any box (CI runners have few cores → few partitions →
    # small spill reservations): pin 128 partitions so reservations alone exceed the pool.
    spark.conf.set("datafusion.execution.target_partitions", "128")
    frame = _wide_frame(spark, 2_000_000)
    with pytest.raises(PySparkException) as raised:
        frame.sort(F.col("close").desc()).to_arrow()
    message = str(raised.value)
    lower = message.lower()
    assert isinstance(raised.value, RuntimeError)
    assert "dynamically evaluated source" not in lower
    # Pool-pressure class, not a single operator name: DF may surface ExternalSorter
    # *or* SortPreservingMergeExec under the same FairSpillPool shortfall (octo T2 C1
    # flake: ExternalSorter-only assert RED under suite load without product regression).
    assert "resources exhausted" in lower or "not enough memory" in lower
    assert (
        "externalsorter" in lower
        or "external sort" in lower
        or "sortpreservingmerge" in lower
        or "failed to allocate additional" in lower
    )
    assert "fair(" in lower, message
    assert "greedy(" not in lower, message
    assert "datafusion.runtime.memory_limit" in message
    assert "repark.memory.limit.gb" in message


def test_sort_oom_collect_path_same_error_class() -> None:
    spark = ReparkSession.builder.config("repark.memory.limit.gb", "1").getOrCreate()
    # Deterministic pool pressure on any box (CI runners have few cores → few partitions →
    # small spill reservations): pin 128 partitions so reservations alone exceed the pool.
    spark.conf.set("datafusion.execution.target_partitions", "128")
    frame = _wide_frame(spark, 2_000_000)
    with pytest.raises(PySparkException) as raised:
        frame.sort(F.col("close").desc()).collect()
    message = str(raised.value)
    assert "dynamically evaluated source" not in message.lower()
    assert "repark.memory.limit.gb" in message


def test_export_error_helper_strips_pyarrow_noise() -> None:
    """Unit pin: wrapper noise loses to an engine Resources exhausted payload."""
    from repark.spark.dataframe import _export_engine_error, _export_error_message

    class _OuterError(Exception):
        pass

    engine = Exception(
        "Resources exhausted: Failed to allocate additional 25.0 GB for "
        "ExternalSorter[0] with 0.0 B already allocated for this reservation - "
        "0.0 B remain available for the total memory pool: fair(pool_size: 8.0 GB)"
    )
    outer = _OuterError("Could not get source, probably due dynamically evaluated source code")
    outer.__cause__ = engine
    cleaned = _export_error_message(outer)
    assert "dynamically evaluated" not in cleaned.lower()
    assert "ExternalSorter[0]" in cleaned
    assert "25.0 GB" in cleaned

    wrapped = _export_engine_error(outer)
    assert isinstance(wrapped, PySparkException)
    text = str(wrapped)
    assert "repark.memory.limit.gb" in text
    assert "datafusion.runtime.memory_limit" in text
    assert "dynamically evaluated" not in text.lower()


# ---------------------------------------------------------------------------------------------
# Measure-first before/after bench (recorded; not a flaky wall-time assert)
# ---------------------------------------------------------------------------------------------


def test_runtime_temp_directory_refuses_loud_no_store() -> None:
    """Runtime temp_directory must refuse (names TMPDIR) — no silent facade twin."""
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises(IllegalArgumentException) as raised:
        spark.conf.set("datafusion.runtime.temp_directory", "/tmp/repark-spill-must-not-stick")
    message = str(raised.value)
    assert "TMPDIR" in message, message
    assert "datafusion.runtime.temp_directory" in message
    assert spark.conf.get("datafusion.runtime.temp_directory", None) is None


def test_sql_set_temp_directory_refuses_loud() -> None:
    spark = ReparkSession.builder.getOrCreate()
    with pytest.raises((IllegalArgumentException, PySparkException)) as raised:
        spark.sql("SET datafusion.runtime.temp_directory = '/tmp/repark-spill-sql'")
    assert "TMPDIR" in str(raised.value), str(raised.value)


def test_builder_temp_directory_creates_datafusion_workdir() -> None:
    """Build-time key wires DiskManager: a datafusion-* dir appears under the path."""
    with tempfile.TemporaryDirectory() as scratch:
        spark = ReparkSession.builder.config(
            "datafusion.runtime.temp_directory", scratch
        ).getOrCreate()
        children = list(Path(scratch).glob("datafusion-*"))
        assert children, f"expected a datafusion-* workdir under {scratch}"
        _ = spark  # keep the session (and DiskManager TempDir) alive across the glob


def test_reverse_sort_succeeds_when_pool_raised() -> None:
    """After raising the pool via conf, reverse sort of the wide frame completes."""
    spark = ReparkSession.builder.config("repark.memory.limit.gb", "1").getOrCreate()
    # Raise live pool (same FairSpillPool) so 500k x 23 cols reverse-sorts cleanly.
    spark.conf.set("datafusion.runtime.memory_limit", "2G")
    frame = _wide_frame(spark, 500_000)
    started = time.perf_counter()
    table = frame.sort(F.col("close").desc()).to_arrow()
    wall_seconds = time.perf_counter() - started
    assert isinstance(table, pa.Table)
    assert table.num_rows == 500_000
    assert table.schema.field("close").type == pa.float64()
    # Sanity: finished in a local-dev window (debug wheel; not a perf claim).
    assert wall_seconds < 120.0
    # Asc twin for the operator reverse-sort observation (same path).
    asc = frame.sort(F.col("close").asc()).to_arrow()
    assert asc.num_rows == 500_000
