"""One-cell subprocess worker (JSON in / JSON out). Isolation for peak RSS."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from windows.oracles import peak_rss_bytes  # noqa: E402


def _median(samples: list[float]) -> float | None:
    """Median of a non-empty sample list."""
    if not samples:
        return None
    return float(statistics.median(samples))


def _count_plan_nodes(session: Any, frame: Any) -> int | None:
    """Count Projection / Unnest tokens in ``EXPLAIN`` of ``frame``."""
    view = f"dynflatten_explain_{int(time.time() * 1000) % 1_000_000}"
    frame.createOrReplaceTempView(view)
    try:
        rows = session.sql(f"EXPLAIN SELECT * FROM {view}").collect()
    finally:
        drop = getattr(session, "drop_temp_view", None) or getattr(
            getattr(session, "catalog", None), "dropTempView", None
        )
        if drop is not None:
            drop(view)
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
    markers = ("Projection", "Unnest", "ParquetExec", "FilterExec", "CoalesceBatches")
    return sum(text.count(marker) for marker in markers)


def _load_source(session: Any, parquet_path: Path, ddl: str) -> Any:
    """Materialize parquet into an in-memory frame (see DYNFLATTEN-QUALNAME-1)."""
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    return session.createDataFrame(table.to_pylist(), schema=ddl)


def run_repark_cell(
    parquet_path: Path,
    *,
    ddl: str,
    warmup: int,
    iterations: int,
    target_partitions: int | None,
) -> dict[str, Any]:
    """Time ``dynamicFlatten`` on one parquet in this process."""
    from repark import ReparkSession, _native
    from repark import __version__ as repark_version

    if getattr(_native, "__debug_assertions__", True):
        return {
            "engine": "repark",
            "outcome": "error",
            "warmup": warmup,
            "iterations": 0,
            "message": "refusing to measure: native module is a debug build (H-3a)",
        }
    builder = ReparkSession.builder.appName("dynflatten-cell").master("local[1]")
    if target_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(target_partitions))
    session = builder.getOrCreate()
    try:
        source = _load_source(session, parquet_path, ddl)
        for _ in range(warmup):
            source.dynamicFlatten().to_arrow()
        rewrite_samples: list[float] = []
        execute_samples: list[float] = []
        rows_out = 0
        plan_nodes: int | None = None
        for _ in range(iterations):
            started = time.perf_counter()
            flat = source.dynamicFlatten()
            rewrite_samples.append((time.perf_counter() - started) * 1000.0)
            started = time.perf_counter()
            table = flat.to_arrow()
            execute_samples.append((time.perf_counter() - started) * 1000.0)
            rows_out = table.num_rows
        if plan_nodes is None:
            try:
                plan_nodes = _count_plan_nodes(session, flat)
            except Exception:
                plan_nodes = None
        wall = [left + right for left, right in zip(rewrite_samples, execute_samples, strict=True)]
        return {
            "engine": "repark",
            "outcome": "ok",
            "warmup": warmup,
            "iterations": iterations,
            "target_partitions": target_partitions,
            "rewrite_ms": rewrite_samples,
            "execute_ms": execute_samples,
            "median_rewrite_ms": _median(rewrite_samples),
            "median_execute_ms": _median(execute_samples),
            "median_wall_ms": _median(wall),
            "min_wall_ms": min(wall) if wall else None,
            "min_execute_ms": min(execute_samples) if execute_samples else None,
            "min_rewrite_ms": min(rewrite_samples) if rewrite_samples else None,
            "peak_rss_bytes": peak_rss_bytes(),
            "rows_out": rows_out,
            "plan_nodes": plan_nodes,
            "version": str(repark_version),
        }
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return {
            "engine": "repark",
            "outcome": "error",
            "warmup": warmup,
            "iterations": 0,
            "message": f"{type(error).__name__}: {error}",
            "peak_rss_bytes": peak_rss_bytes(),
        }
    finally:
        session.stop()


def main(argv: list[str] | None = None) -> int:
    """CLI for one isolated repark cell."""
    parser = argparse.ArgumentParser(description="dynamicFlatten one-cell worker")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--ddl", type=str, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--target-partitions", type=int, default=None)
    args = parser.parse_args(argv)
    payload = run_repark_cell(
        args.parquet,
        ddl=args.ddl,
        warmup=args.warmup,
        iterations=args.iterations,
        target_partitions=args.target_partitions,
    )
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload), encoding="utf-8")
    return 0 if payload.get("outcome") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
