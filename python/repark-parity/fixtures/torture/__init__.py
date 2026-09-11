"""Seeded torture-test dataset generators for the repark readers; data is never committed."""

from __future__ import annotations

from repark_parity.torture.decimal_overflow import DECIMAL_OVERFLOW_FAMILY
from repark_parity.torture.extreme_types import EXTREME_TYPES_FAMILY
from repark_parity.torture.family import CSV_NAME, PARQUET_NAME, Family, FamilyOutput
from repark_parity.torture.inference import INFERENCE_FAMILY
from repark_parity.torture.nested import NESTED_FAMILY
from repark_parity.torture.secrets import SECRETS_FAMILY
from repark_parity.torture.smartcsv import SMARTCSV_FAMILY
from repark_parity.torture.temporal import TEMPORAL_FAMILY
from repark_parity.torture.tiers import CI_ROWS, FULL_ROWS, tier_from_env, tier_rows

FAMILIES: dict[str, Family] = {
    "decimal_overflow": DECIMAL_OVERFLOW_FAMILY,
    "extreme_types": EXTREME_TYPES_FAMILY,
    "inference": INFERENCE_FAMILY,
    "nested": NESTED_FAMILY,
    "secrets": SECRETS_FAMILY,
    "smartcsv": SMARTCSV_FAMILY,
    "temporal": TEMPORAL_FAMILY,
}

__all__ = [
    "CI_ROWS",
    "CSV_NAME",
    "DECIMAL_OVERFLOW_FAMILY",
    "EXTREME_TYPES_FAMILY",
    "FAMILIES",
    "FULL_ROWS",
    "INFERENCE_FAMILY",
    "NESTED_FAMILY",
    "PARQUET_NAME",
    "SECRETS_FAMILY",
    "SMARTCSV_FAMILY",
    "TEMPORAL_FAMILY",
    "Family",
    "FamilyOutput",
    "tier_from_env",
    "tier_rows",
]
