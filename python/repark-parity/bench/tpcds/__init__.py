"""TPC-DS scoreboard harness (R-TPCDS-HARNESS / D1).

Datagen via DuckDB ``dsdgen`` → parquet under ``$XDG_CACHE_HOME/repark-tpcds/sf{N}/``;
queries from ``tpcds_queries()``; repark vs DuckDB wall + correctness matrix. No AWS.
D1: parquet temp views only — no Iceberg leg.
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
