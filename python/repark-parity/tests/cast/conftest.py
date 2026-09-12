"""Guards for the CAST-cost pins: the live cell needs the native module."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "repark",
    reason="the CAST-cost pin reads through the product; build the native "
    "module. The isolated parity job (`make py-test`, ci.yml) has no native "
    "build, so it collects nothing here.",
)
