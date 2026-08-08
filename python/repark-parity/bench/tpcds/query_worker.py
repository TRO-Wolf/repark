#!/usr/bin/env python3
"""Isolated TPC-DS query worker.

Parent scoreboard may spawn one process per query so an OOM-kill (or other
fatal signal) records DIED without killing the scoreboard. Config + result are
JSON files; never logs secrets (none exist on this path).
"""

from __future__ import annotations

import contextlib
import json
import resource
import sys
import types
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("usage: query_worker.py <config.json>", file=sys.stderr)
        return 2
    config_path = Path(args[0])
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"worker config read failed: {exc}", file=sys.stderr)
        return 2

    out_path = Path(str(config["result_path"]))
    query_nr = int(config.get("query_nr", -1))

    tpcds_dir = Path(__file__).resolve().parent
    package_name = "repark_tpcds_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(tpcds_dir)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package

    import importlib

    runner = importlib.import_module(f"{package_name}.runner")
    queries_mod = importlib.import_module(f"{package_name}.queries")

    duckdb_conn = None
    spark = None
    try:
        query = queries_mod.TpcdsQuery(
            query_nr=query_nr,
            original_sql=str(config["original_sql"]),
            sql_for_repark=str(config["sql_for_repark"]),
            rewrite_note=config.get("rewrite_note"),
        )
        data_dir = Path(str(config["data_dir"]))

        duckdb_conn = runner._open_duckdb_over_parquet(data_dir)
        spark = runner._open_repark_over_parquet(data_dir)
        result = runner._run_one_query(
            spark=spark,
            duckdb_conn=duckdb_conn,
            query=query,
            repeats=int(config["repeats"]),
            timeout_s=float(config["timeout_s"]),
            timeout_retry_s=float(config.get("timeout_retry_s", 300.0)),
        )
        result.rss_peak_kb = _max_rss_kb()
    except Exception as exc:
        # Structured ERROR so the parent scoreboard never depends on stderr alone.
        result = runner.QueryResult(
            query_nr=query_nr,
            status="ERROR",
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
            error_class="WorkerException",
            error_message=f"{type(exc).__name__}: {exc}",
            rewrite_note=config.get("rewrite_note"),
        )
    finally:
        if spark is not None:
            with contextlib.suppress(Exception):
                spark.stop()
        if duckdb_conn is not None:
            with contextlib.suppress(Exception):
                duckdb_conn.close()

    payload = runner.query_result_to_dict(result)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return 0


def _max_rss_kb() -> int:
    """Peak RSS of this process in kibibytes (Linux ru_maxrss unit)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Linux: kilobytes; macOS: bytes — we only claim Linux for measurement hosts.
    return int(usage.ru_maxrss)


if __name__ == "__main__":
    raise SystemExit(main())
