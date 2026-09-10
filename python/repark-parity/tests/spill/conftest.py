"""Guards for the spill-matrix suite: the cells run the product, so the native module is needed."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "repark",
    reason="the spill matrix reads through the product in worker subprocesses; build the native "
    "module and run it via `make py-test-spill-matrix`. The isolated parity job (`make py-test`, "
    "ci.yml) has no native build, so it collects nothing here.",
)
