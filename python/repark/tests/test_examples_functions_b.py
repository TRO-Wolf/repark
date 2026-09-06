"""Divergence pins for the EX-28 F.* scalar remainder."""

from __future__ import annotations

import pytest

from repark.errors import UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812


def test_try_to_timestamp_refuses() -> None:
    """try_to_timestamp refuses; Spark answers the timestamp or NULL (EX-FN-20)."""
    with pytest.raises(UnsupportedOperationException, match="try_to_timestamp"):
        F.try_to_timestamp("s")


def test_unix_timestamp_format_refuses() -> None:
    """unix_timestamp format argument refuses; Spark parses the pattern (EX-FN-21)."""
    with pytest.raises(UnsupportedOperationException, match="format argument"):
        F.unix_timestamp("s", "yyyy-MM-dd")
