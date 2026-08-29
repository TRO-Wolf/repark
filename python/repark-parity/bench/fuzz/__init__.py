"""Seeded differential SQL fuzzer — RePark vs DuckDB.

Determinism contract (HARD): every run is keyed by an explicit integer seed
(CLI / env ``REPARK_FUZZ_SEED``; test default fixed literal ``42``).
Same seed → byte-identical query set; no time-based seeding anywhere.
"""

from __future__ import annotations

from .compare import CompareResult, compare_result_sets
from .datagen import FuzzDatabase, generate_database
from .generator import DEFAULT_SEED, generate_queries
from .runner import FuzzRunResult, run_fuzzer

__all__ = [
    "DEFAULT_SEED",
    "CompareResult",
    "FuzzDatabase",
    "FuzzRunResult",
    "compare_result_sets",
    "generate_database",
    "generate_queries",
    "run_fuzzer",
]
