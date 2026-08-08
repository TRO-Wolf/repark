"""TPC-H scoreboard harness (R-TPCH-HARNESS).

Datagen via DuckDB ``dbgen`` → parquet under ``/tmp/tpch-data/sf{N}/``; queries from
``tpch_queries()``; repark vs DuckDB wall + correctness matrix. No AWS.
"""

from __future__ import annotations

__all__ = [
    "TABLES",
    "compare_result_sets",
    "ensure_parquet_sf",
    "load_queries",
    "run_scoreboard",
]

from .compare import compare_result_sets
from .datagen import TABLES, ensure_parquet_sf
from .queries import load_queries
from .runner import run_scoreboard
