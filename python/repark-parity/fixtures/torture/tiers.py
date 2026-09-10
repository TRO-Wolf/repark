"""Torture tier resolution: ci generates 10k rows at test time; full reuses /tmp/torture."""

from __future__ import annotations

import os
from pathlib import Path

TIER_ENV = "TORTURE_TIER"
CI_TIER = "ci"
FULL_TIER = "full"
TIERS = (CI_TIER, FULL_TIER)
CI_ROWS = 10_000
FULL_ROWS = 1_000_000
DEFAULT_SEED = 7
FULL_ROOT = Path("/tmp/torture")


def tier_from_env(env: dict[str, str] | None = None) -> str:
    """Read TORTURE_TIER (default ci) and refuse unknown tier names."""
    source = os.environ if env is None else env
    tier = source.get(TIER_ENV, CI_TIER)
    if tier not in TIERS:
        raise ValueError(f"unknown {TIER_ENV}: {tier}")
    return tier


def tier_rows(tier: str) -> int:
    """Map a tier name to its row budget."""
    if tier == CI_TIER:
        return CI_ROWS
    if tier == FULL_TIER:
        return FULL_ROWS
    raise ValueError(f"unknown tier: {tier}")
