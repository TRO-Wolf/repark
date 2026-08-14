"""Version SSOT pins (design/python-facade.md §4 Q6, armed with release.yml).

The wheel's version comes from the Cargo workspace (maturin injects it; pyproject declares
``dynamic = ["version"]``). These pins hold on every install path the suite runs under
(editable ``make develop`` and the packaged wheel alike): the distribution version is the
single source ``repark.__version__`` reflects, and it is a real PEP 440 release version —
not the ``0.0.0`` placeholder era.
"""

from __future__ import annotations

import re
from importlib.metadata import version as _distribution_version

import repark


def test_dunder_version_is_the_distribution_version() -> None:
    assert repark.__version__ == _distribution_version("repark")


def test_version_is_pep440_release_shaped() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", repark.__version__), repark.__version__


def test_version_left_the_placeholder_era() -> None:
    # 0.0.0 was the pre-SSOT dev placeholder; 0.0.1 is the PyPI name reservation. The
    # workspace version must be past both so a tagged wheel outversions the reservation.
    major, minor, patch = (int(x) for x in repark.__version__.split("."))
    assert (major, minor, patch) > (0, 0, 1), repark.__version__
