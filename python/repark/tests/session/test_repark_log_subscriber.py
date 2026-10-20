"""R-TRACE-SUBSCRIBER — env-gated tracing reaches the wheel for live MERGE phase profiles.

RUST_LOG / REPARK_LOG were inert through the maturin wheel: spans exist in repark-write, but
nothing installed a subscriber. This pins:

1. ``REPARK_LOG=info`` + local MoR MERGE → stderr contains all five ``merge.*`` phase names
   and span close timings (``FmtSpan::CLOSE``).
2. Subprocess WITHOUT the env → stderr has none of those span names (zero overhead path).

Subprocess isolation is load-bearing: the subscriber is process-global (``try_init`` once per
import of ``repark._native``), so in-process tests cannot safely re-arm filters.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

# Minimal local MoR upsert: format-version 2 + write-on-read, one matched update + one insert.
# Must exercise write_deletes (position deletes) so all five phase spans fire.
_MOR_MERGE_SCRIPT = textwrap.dedent(
    """
    from pathlib import Path
    import tempfile
    from repark import ReparkSession

    warehouse = Path(tempfile.mkdtemp(prefix="repark-log-"))
    spark = ReparkSession.builder.appName("repark-log-sub").getOrCreate()
    spark.register_memory_catalog("mem", warehouse)
    spark.sql("CREATE NAMESPACE mem.ns")
    spark.sql(
        "CREATE TABLE mem.ns.t USING iceberg TBLPROPERTIES ("
        "  'format-version' = '2',"
        "  'write.merge.mode' = 'merge-on-read'"
        ") AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, name)"
    )
    spark.sql(
        "SELECT * FROM (VALUES (2, 'bee'), (4, 'd')) AS v(id, name)"
    ).createOrReplaceTempView("src")
    spark.sql(
        "MERGE INTO mem.ns.t AS t USING src AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    assert spark.sql("SELECT count(*) AS c FROM mem.ns.t").to_arrow().column("c")[0].as_py() == 4
    spark.stop()
    print("MERGE_OK")
    """
)

_PHASE_SPANS = (
    "merge.target_scan",
    "merge.join",
    "merge.write_data",
    "merge.write_deletes",
    "merge.commit",
)


def _run_merge_subprocess(*, env_extra: dict[str, str] | None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    # Isolate from the host's RUST_LOG / REPARK_LOG so pins are deterministic.
    env.pop("REPARK_LOG", None)
    env.pop("RUST_LOG", None)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-c", _MOR_MERGE_SCRIPT],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )


def test_repark_log_emits_merge_phase_spans_with_close_timings() -> None:
    """With REPARK_LOG set, stderr carries all five merge phase spans and close timings."""
    result = _run_merge_subprocess(env_extra={"REPARK_LOG": "info"})
    assert result.returncode == 0, (
        f"MoR MERGE subprocess failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "MERGE_OK" in result.stdout
    stderr = result.stderr
    for name in _PHASE_SPANS:
        assert name in stderr, f"expected span {name!r} in stderr; got:\n{stderr}"
    close_signal = (
        "close" in stderr.lower()
        or "time.busy" in stderr
        or "time.idle" in stderr
        or "busy=" in stderr
    )
    assert close_signal, f"expected span CLOSE timings in stderr; got:\n{stderr}"


def test_without_log_env_stderr_has_no_merge_phase_spans() -> None:
    """Absent REPARK_LOG/RUST_LOG, no subscriber → no merge phase names on stderr."""
    result = _run_merge_subprocess(env_extra=None)
    assert result.returncode == 0, (
        f"MoR MERGE subprocess failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "MERGE_OK" in result.stdout
    stderr = result.stderr
    for name in _PHASE_SPANS:
        assert name not in stderr, (
            f"span {name!r} must NOT appear without log env; stderr:\n{stderr}"
        )
