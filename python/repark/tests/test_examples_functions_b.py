"""Divergence pins for the EX-28 F.* scalar remainder and the EX-30 F.* remainder."""

from __future__ import annotations

import pytest

from repark.errors import UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812
from repark.spark.types import LongType


def test_try_to_timestamp_refuses() -> None:
    """try_to_timestamp refuses; Spark answers the timestamp or NULL (EX-FN-20)."""
    with pytest.raises(UnsupportedOperationException, match="try_to_timestamp"):
        F.try_to_timestamp("s")


def test_unix_timestamp_format_refuses() -> None:
    """unix_timestamp format argument refuses; Spark parses the pattern (EX-FN-21)."""
    with pytest.raises(UnsupportedOperationException, match="format argument"):
        F.unix_timestamp("s", "yyyy-MM-dd")


def test_from_xml_refuses() -> None:
    """from_xml refuses as an E1 stub; Spark parses the row struct (EX-FN-22)."""
    with pytest.raises(UnsupportedOperationException, match="from_xml"):
        F.from_xml("x", "b INT")


def test_schema_of_xml_refuses() -> None:
    """schema_of_xml refuses as an E1 stub; Spark infers the struct (EX-FN-22)."""
    with pytest.raises(UnsupportedOperationException, match="schema_of_xml"):
        F.schema_of_xml(F.lit("<a><b>1</b></a>"))


def test_udf_factories_answer_typed_wrappers() -> None:
    """udf/pandas_udf answer typed UDF objects; Spark 4.1.2 answers plain functions (EX-FN-23)."""
    assert isinstance(F.udf(lambda value: value), F.UserDefinedFunction)
    assert (
        type(F.pandas_udf(lambda series: series, returnType=LongType())).__name__
        == "PandasUDFFunction"
    )
