"""Version SSOT pins (design/python-facade.md §4 Q6, armed with release.yml).

The wheel version comes from the Cargo workspace (maturin injects it; pyproject declares
``dynamic = ["version"]``): the distribution version is the single source ``repark.__version__``
reflects, and it is a real PEP 440 release version.
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
    # 0.0.1 is the PyPI name reservation; the workspace version must outversion it so a
    # tagged wheel wins.
    major, minor, patch = (int(x) for x in repark.__version__.split("."))
    assert (major, minor, patch) > (0, 0, 1), repark.__version__
