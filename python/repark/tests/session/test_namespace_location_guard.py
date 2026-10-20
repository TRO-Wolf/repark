"""Facade pin: namespace-create location guard (G-6 Q1 / R-6).

Memory catalog only — no AWS. Four shapes through ``spark.create_namespace`` (the
programmatic facade path that hits core ``Session::create_namespace``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException


def test_create_namespace_location_guard_four_shapes(tmp_path: Path) -> None:
    """Create-new / same / conflicting / no-location through the public facade.

    Value-and-message pin on the Arrow-path exception (never ``show``): the
    conflict names both warehouse paths. Matching and no-location stay
    idempotent. Mutation: drop the core exists-check → same-location and
    no-location raise; drop the location compare → conflict silently adopts.
    """
    spark = ReparkSession.builder.appName("pytest-ns-location-guard").getOrCreate()
    spark.register_memory_catalog("guard_catalog", tmp_path)
    existing = str(tmp_path / "existing_ns")
    requested = str(tmp_path / "requested_ns")

    spark.create_namespace("guard_catalog", "silver", location=existing)

    spark.create_namespace("guard_catalog", "silver", location=existing)

    with pytest.raises(AnalysisException) as raised:
        spark.create_namespace("guard_catalog", "silver", location=requested)
    message = str(raised.value)
    assert existing in message, message
    assert requested in message, message

    spark.create_namespace("guard_catalog", "silver")
