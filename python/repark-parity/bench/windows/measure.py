"""RePark driver for W-0 cells. Imports the native module; not loaded by make py-test."""

from __future__ import annotations

import statistics
import time
from pathlib import Path
from typing import Any

from .classify import (
    OUTCOME_CRASH,
    OUTCOME_ERROR,
    OUTCOME_OK,
    classify_exception_text,
)
from .datagen import cleanup_scratch, directory_bytes, write_seed_parquet
from .hardware import hardware_fields, native_build_flavor
from .models import CellResult, CellTiming, ProbeRow, RunResult
from .oracles import (
    duckdb_version,
    peak_rss_bytes,
    pyspark_version,
    run_duckdb_sql,
    run_pyspark_sql,
)
from .roster import (
    DEFAULT_ITERATIONS,
    DEFAULT_SEED,
    DEFAULT_WARMUP,
    FULL_UNPARTITIONED_ROWS,
    GATE_ROWS,
    ICEBERG_FULL_ROWS,
    ICEBERG_QUICK_ROWS,
    MEMORY_LIMIT_FULL_ROWS,
    MEMORY_LIMIT_QUICK_ROWS,
    MEMORY_LIMIT_SETTING,
    PROBE_SPECS,
    QUICK_ITERATIONS,
    QUICK_UNPARTITIONED_ROWS,
    QUICK_WARMUP,
    SLIDING_FRAME,
    SLIDING_PROBE_ROWS,
    SLIDING_TIMED_FULL_ROWS,
    SLIDING_TIMED_QUICK_ROWS,
    TIMED_SLIDING_FRAME,
    TIMED_SLIDING_NAMES,
    constant_select,
    lead_lag_select,
    retract_class,
    sliding_select,
    sliding_sum_select,
    spec_by_name,
    unpartitioned_select,
)

PLAN_TOKEN_NAMES: tuple[str, ...] = (
    "SortExec",
    "WindowAggExec",
    "BoundedWindowAggExec",
    "ParquetExec",
    "Iceberg",
    "SortPreserving",
)
CATALOG = "w0cat"
NAMESPACE = "w0ns"
ICEBERG_TABLE = f"{CATALOG}.{NAMESPACE}.scan"


def _median(samples: list[float]) -> float | None:
    """Median of a non-empty sample list."""
    if not samples:
        return None
    return float(statistics.median(samples))


def scale_counts(scale: str) -> dict[str, int]:
    """Row counts per cell family for ``quick`` or ``full``.

    Args:
        scale: ``quick``, ``full``, or ``gate``.

    Returns:
        Mapping of family name to row count.

    Raises:
        ValueError: unknown scale.
    """
    if scale == "full":
        return {
            "probe": SLIDING_PROBE_ROWS,
            "sliding": SLIDING_TIMED_FULL_ROWS,
            "constant": SLIDING_TIMED_FULL_ROWS,
            "unpartitioned": FULL_UNPARTITIONED_ROWS,
            "iceberg": ICEBERG_FULL_ROWS,
            "memory": MEMORY_LIMIT_FULL_ROWS,
        }
    if scale == "quick":
        return {
            "probe": SLIDING_PROBE_ROWS,
            "sliding": SLIDING_TIMED_QUICK_ROWS,
            "constant": SLIDING_TIMED_QUICK_ROWS,
            "unpartitioned": QUICK_UNPARTITIONED_ROWS,
            "iceberg": ICEBERG_QUICK_ROWS,
            "memory": MEMORY_LIMIT_QUICK_ROWS,
        }
    if scale == "gate":
        return {
            "probe": GATE_ROWS,
            "sliding": GATE_ROWS,
            "constant": GATE_ROWS,
            "unpartitioned": GATE_ROWS,
            "iceberg": GATE_ROWS,
            "memory": GATE_ROWS,
        }
    raise ValueError(f"unknown scale {scale!r}")


def make_session(app_name: str, *, memory_limit: str | None = None) -> Any:
    """Build a RePark session, optionally with a DataFusion memory pool size.

    Args:
        app_name: builder application name.
        memory_limit: ``datafusion.runtime.memory_limit`` value such as ``16M``.

    Returns:
        A live ``ReparkSession``.
    """
    from repark import ReparkSession

    builder = ReparkSession.builder.appName(app_name)
    if memory_limit is not None:
        builder = builder.config("datafusion.runtime.memory_limit", memory_limit)
    return builder.getOrCreate()


def stop_session(session: Any) -> None:
    """Stop ``session`` if it is not None."""
    if session is None:
        return
    session.stop()


def register_seed(session: Any, parquet_path: Path, view: str = "t") -> None:
    """Register a parquet file as temp view ``view``."""
    session.read.parquet(str(parquet_path)).createOrReplaceTempView(view)


def explain_tokens(session: Any, sql: str) -> list[str]:
    """Collect DataFusion plan-token names present in ``EXPLAIN sql``.

    Args:
        session: live session.
        sql: statement to explain (not ANALYZE).

    Returns:
        Tokens from :data:`PLAN_TOKEN_NAMES` that appear in the plan text.
    """
    rows = session.sql(f"EXPLAIN {sql}").collect()
    chunks: list[str] = []
    for row in rows:
        mapping = row.asDict(recursive=False) if hasattr(row, "asDict") else None
        if mapping is not None and "plan" in mapping:
            chunks.append(str(mapping["plan"]))
        elif hasattr(row, "__getitem__") and len(row) > 1:
            chunks.append(str(row[1]))
        else:
            chunks.append(str(row))
    text = "\n".join(chunks)
    return [token for token in PLAN_TOKEN_NAMES if token in text]


def run_repark_sql(
    session: Any,
    sql: str,
    *,
    warmup: int,
    iterations: int,
    collect_plan: bool,
) -> CellTiming:
    """Time ``sql`` on an open RePark session.

    Args:
        session: live session with ``t`` (or the query's tables) registered.
        sql: statement to run.
        warmup: untimed passes.
        iterations: timed passes.
        collect_plan: whether to run ``EXPLAIN`` first.

    Returns:
        A :class:`CellTiming` for engine ``repark``.
    """
    from repark import __version__ as repark_version

    tokens: list[str] = []
    try:
        if collect_plan:
            tokens = explain_tokens(session, sql)
        for _ in range(warmup):
            session.sql(sql).to_arrow()
        samples: list[float] = []
        answer: Any = None
        for _ in range(iterations):
            started = time.perf_counter()
            table = session.sql(sql).to_arrow()
            samples.append((time.perf_counter() - started) * 1000.0)
            answer = table.to_pylist()
        return CellTiming(
            engine="repark",
            outcome=OUTCOME_OK,
            warmup=warmup,
            iterations=iterations,
            samples_ms=samples,
            median_ms=_median(samples),
            peak_rss_bytes=peak_rss_bytes(),
            plan_tokens=tokens,
            answer=answer,
            version=str(repark_version),
        )
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        text = f"{type(error).__name__}: {error}"
        outcome = classify_exception_text(text)
        if isinstance(error, (MemoryError, OSError)) and outcome == OUTCOME_ERROR:
            outcome = classify_exception_text(text + " out of memory")
        return CellTiming(
            engine="repark",
            outcome=outcome,
            warmup=warmup,
            iterations=0,
            plan_tokens=tokens,
            message=text,
            version=str(repark_version),
            peak_rss_bytes=peak_rss_bytes(),
        )


def probe_sliding(session: Any, parquet_path: Path) -> list[ProbeRow]:
    """Classify every roster name on a sliding frame.

    Args:
        session: live session.
        parquet_path: seed parquet registered as ``t``.

    Returns:
        One :class:`ProbeRow` per roster spec.
    """
    register_seed(session, parquet_path)
    rows: list[ProbeRow] = []
    for spec in PROBE_SPECS:
        sql = sliding_select(spec.sql_expr, frame=SLIDING_FRAME)
        timing = run_repark_sql(session, sql, warmup=0, iterations=1, collect_plan=False)
        rows.append(
            ProbeRow(
                name=spec.name,
                sql_expr=spec.sql_expr,
                intake_class=retract_class(spec.name),
                outcome=timing.outcome,
                message=timing.message,
            )
        )
    return rows


def oracle_timings(
    parquet_path: Path,
    sql: str,
    *,
    warmup: int,
    iterations: int,
    skip_duckdb: bool,
    skip_pyspark: bool,
    pyspark_session: Any | None,
) -> list[CellTiming]:
    """Run DuckDB and PySpark oracles for ``sql``.

    Args:
        parquet_path: seed file.
        sql: statement.
        warmup: untimed passes.
        iterations: timed passes.
        skip_duckdb: do not call DuckDB.
        skip_pyspark: do not call PySpark.
        pyspark_session: reused Spark session, if any.

    Returns:
        Zero, one, or two :class:`CellTiming` values.
    """
    timings: list[CellTiming] = []
    if not skip_duckdb:
        timings.append(run_duckdb_sql(parquet_path, sql, warmup=warmup, iterations=iterations))
    if not skip_pyspark:
        timings.append(
            run_pyspark_sql(
                parquet_path,
                sql,
                warmup=warmup,
                iterations=iterations,
                session=pyspark_session,
            )
        )
    return timings


def create_iceberg_scan(session: Any, warehouse: Path, parquet_path: Path) -> int:
    """CTAS an unsorted Iceberg table from ``parquet_path``. Returns warehouse bytes.

    Args:
        session: live session.
        warehouse: memory-catalog warehouse root.
        parquet_path: seed parquet.

    Returns:
        Byte size of the warehouse after CTAS.
    """
    session.register_memory_catalog(CATALOG, str(warehouse))
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    register_seed(session, parquet_path, "seed")
    session.sql(
        f"CREATE TABLE {ICEBERG_TABLE} USING iceberg TBLPROPERTIES "
        f"('format-version' = '2') AS SELECT * FROM seed"
    )
    session.sql(f"SELECT * FROM {ICEBERG_TABLE}").createOrReplaceTempView("t")
    return directory_bytes(warehouse)


def run_window_measurement(
    scratch: Path,
    *,
    scale: str,
    seed: int = DEFAULT_SEED,
    keep_scratch: bool = False,
    skip_duckdb: bool = False,
    skip_pyspark: bool = False,
) -> RunResult:
    """Run the W-0 battery into ``scratch`` and return a :class:`RunResult`.

    Generated files are deleted unless ``keep_scratch`` is true. A crash on the
    over-limit cell is recorded as outcome ``crash`` when the interpreter
    survives; an abort that kills the process is the caller's to notice.

    Args:
        scratch: working directory (created).
        scale: ``quick``, ``full``, or ``gate``.
        seed: generator seed.
        keep_scratch: leave generated files in place.
        skip_duckdb: skip the DuckDB oracle.
        skip_pyspark: skip the PySpark oracle.

    Returns:
        The populated run record.
    """
    from repark import __version__ as repark_version

    started = time.perf_counter()
    scratch.mkdir(parents=True, exist_ok=True)
    counts = scale_counts(scale)
    warmup = QUICK_WARMUP if scale != "full" else DEFAULT_WARMUP
    iterations = QUICK_ITERATIONS if scale != "full" else DEFAULT_ITERATIONS
    dataset_bytes: dict[str, int] = {}
    cells: list[CellResult] = []

    seed_probe = scratch / "seed_probe.parquet"
    seed_sliding = scratch / "seed_sliding.parquet"
    seed_unpartitioned = scratch / "seed_unpartitioned.parquet"
    seed_iceberg = scratch / "seed_iceberg.parquet"
    seed_memory = scratch / "seed_memory.parquet"
    warehouse = scratch / "warehouse"
    dataset_bytes[str(seed_probe)] = write_seed_parquet(seed_probe, counts["probe"], seed=seed)
    dataset_bytes[str(seed_sliding)] = write_seed_parquet(
        seed_sliding, counts["sliding"], seed=seed
    )
    dataset_bytes[str(seed_unpartitioned)] = write_seed_parquet(
        seed_unpartitioned, counts["unpartitioned"], seed=seed
    )
    dataset_bytes[str(seed_iceberg)] = write_seed_parquet(
        seed_iceberg, counts["iceberg"], seed=seed
    )
    dataset_bytes[str(seed_memory)] = write_seed_parquet(seed_memory, counts["memory"], seed=seed)

    duck_version, duck_skip = duckdb_version()
    spark_version, spark_skip = pyspark_version()
    skip_duckdb = skip_duckdb or duck_version is None
    skip_pyspark = skip_pyspark or spark_skip is not None

    pyspark_session: Any | None = None
    if not skip_pyspark:
        from pyspark.sql import SparkSession

        pyspark_session = (
            SparkSession.builder.master("local[1]")
            .appName("w0-oracle")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    session = make_session("w0-window-bench")
    probe = probe_sliding(session, seed_probe)

    register_seed(session, seed_sliding)
    for name in TIMED_SLIDING_NAMES:
        sql = sliding_sum_select(spec_by_name(name).sql_expr, frame=TIMED_SLIDING_FRAME)
        repark_timing = run_repark_sql(
            session, sql, warmup=warmup, iterations=iterations, collect_plan=True
        )
        extras = oracle_timings(
            seed_sliding,
            sql,
            warmup=warmup,
            iterations=iterations,
            skip_duckdb=skip_duckdb,
            skip_pyspark=skip_pyspark,
            pyspark_session=pyspark_session,
        )
        cells.append(
            CellResult(
                label=f"sliding_{name}",
                sql=sql,
                rows=counts["sliding"],
                timings=[repark_timing, *extras],
            )
        )

    constant_sql = constant_select("sum(v)")
    register_seed(session, seed_sliding)
    constant_timing = run_repark_sql(
        session, constant_sql, warmup=warmup, iterations=iterations, collect_plan=True
    )
    cells.append(
        CellResult(
            label="constant_sum",
            sql=constant_sql,
            rows=counts["constant"],
            timings=[
                constant_timing,
                *oracle_timings(
                    seed_sliding,
                    constant_sql,
                    warmup=warmup,
                    iterations=iterations,
                    skip_duckdb=skip_duckdb,
                    skip_pyspark=skip_pyspark,
                    pyspark_session=pyspark_session,
                ),
            ],
        )
    )

    unpart_sql = unpartitioned_select("sum(v)")
    register_seed(session, seed_unpartitioned)
    unpart_timing = run_repark_sql(
        session, unpart_sql, warmup=warmup, iterations=iterations, collect_plan=True
    )
    cells.append(
        CellResult(
            label="unpartitioned_order_by",
            sql=unpart_sql,
            rows=counts["unpartitioned"],
            timings=[
                unpart_timing,
                *oracle_timings(
                    seed_unpartitioned,
                    unpart_sql,
                    warmup=warmup,
                    iterations=iterations,
                    skip_duckdb=skip_duckdb,
                    skip_pyspark=skip_pyspark,
                    pyspark_session=pyspark_session,
                ),
            ],
        )
    )

    iceberg_sql = lead_lag_select()
    warehouse_bytes = create_iceberg_scan(session, warehouse, seed_iceberg)
    dataset_bytes[str(warehouse)] = warehouse_bytes
    iceberg_timing = run_repark_sql(
        session, iceberg_sql, warmup=warmup, iterations=iterations, collect_plan=True
    )
    cells.append(
        CellResult(
            label="iceberg_lead_lag",
            sql=iceberg_sql,
            rows=counts["iceberg"],
            timings=[
                iceberg_timing,
                *oracle_timings(
                    seed_iceberg,
                    iceberg_sql,
                    warmup=warmup,
                    iterations=iterations,
                    skip_duckdb=skip_duckdb,
                    skip_pyspark=skip_pyspark,
                    pyspark_session=pyspark_session,
                ),
            ],
        )
    )

    stop_session(session)
    memory_sql = unpartitioned_select("sum(v)")
    memory_session = make_session("w0-memory-limit", memory_limit=MEMORY_LIMIT_SETTING)
    register_seed(memory_session, seed_memory)
    try:
        memory_timing = run_repark_sql(
            memory_session,
            memory_sql,
            warmup=0,
            iterations=1,
            collect_plan=True,
        )
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        memory_timing = CellTiming(
            engine="repark",
            outcome=OUTCOME_CRASH,
            warmup=0,
            iterations=0,
            message=f"{type(error).__name__}: {error}",
            peak_rss_bytes=peak_rss_bytes(),
        )
    stop_session(memory_session)
    cells.append(
        CellResult(
            label=f"memory_limit_{MEMORY_LIMIT_SETTING}",
            sql=memory_sql,
            rows=counts["memory"],
            timings=[memory_timing],
        )
    )

    if pyspark_session is not None:
        pyspark_session.stop()

    scratch_deleted = cleanup_scratch(scratch, keep=keep_scratch)

    return RunResult(
        scale=scale,
        seed=seed,
        engine_version=f"repark-{repark_version}",
        duckdb_version=None if skip_duckdb else duck_version,
        pyspark_version=None if skip_pyspark else spark_version,
        pyspark_skip_reason=spark_skip if skip_pyspark else None,
        duckdb_skip_reason=duck_skip if skip_duckdb else None,
        native_build=native_build_flavor(),
        machine=hardware_fields(),
        dataset_bytes=dataset_bytes,
        probe=probe,
        cells=cells,
        peak_rss_bytes=peak_rss_bytes(),
        wall_seconds=time.perf_counter() - started,
        scratch_deleted=scratch_deleted,
    )


def refuse_names(probe: list[ProbeRow]) -> list[str]:
    """Roster names whose live class is sliding-frame refuse.

    Args:
        probe: output of :func:`probe_sliding`.

    Returns:
        Sorted unique names.
    """
    return sorted({row.name for row in probe if row.outcome == "refuse"})
