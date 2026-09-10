"""Seeded torture-test dataset generators for the repark readers; data is never committed."""

from __future__ import annotations

from repark_parity.torture.family import CSV_NAME, PARQUET_NAME, Family, FamilyOutput
from repark_parity.torture.inference import INFERENCE_FAMILY
from repark_parity.torture.nested import NESTED_FAMILY
from repark_parity.torture.tiers import CI_ROWS, FULL_ROWS, tier_from_env, tier_rows

FAMILIES: dict[str, Family] = {"inference": INFERENCE_FAMILY, "nested": NESTED_FAMILY}

__all__ = [
    "CI_ROWS",
    "CSV_NAME",
    "FAMILIES",
    "FULL_ROWS",
    "INFERENCE_FAMILY",
    "NESTED_FAMILY",
    "PARQUET_NAME",
    "Family",
    "FamilyOutput",
    "tier_from_env",
    "tier_rows",
]
