"""The declared orc/xml/jdbc IO refusals and the ``na.replace`` delegation.

pins: io-declared-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)

COVERS: list[str] = [
    "DataFrameReader.orc",
    "DataFrameReader.xml",
    "DataFrameReader.jdbc",
    "DataFrameWriter.orc",
    "DataFrameWriter.xml",
    "DataFrameWriter.jdbc",
    "DataFrameNaFunctions.replace",
]


def _expect_refusal(call: Callable[[], Any], feature: str) -> None:
    """Assert one declared NOT_IMPLEMENTED IO refusal."""
    try:
        call()
    except PySparkNotImplementedError as error:
        if error.getCondition() != "NOT_IMPLEMENTED":
            raise SystemExit(f"{feature} condition {error.getCondition()!r}") from error
        if error.getMessageParameters() != {"feature": feature}:
            raise SystemExit(f"{feature} params {error.getMessageParameters()!r}") from error
        return
    raise SystemExit(f"{feature} did not refuse")


def _expect_row_tag_missing(call: Callable[[], Any]) -> None:
    """Assert the XML_ROW_TAG_MISSING refusal."""
    try:
        call()
    except AnalysisException as error:
        if error.getCondition() != "XML_ROW_TAG_MISSING":
            raise SystemExit(f"rowTag condition {error.getCondition()!r}") from error
        return
    raise SystemExit("missing rowTag did not refuse")


def main() -> None:
    """Assert each declared refusal and the na.replace delegation on one local frame."""
    repark = ReparkSession.builder.appName("ex-io-declared").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, "a"), (2, "b")], "x int, y string")
        reader = repark.read
        _expect_refusal(lambda: reader.orc("/tmp/unused-io-declared/orc"), "orc")
        _expect_row_tag_missing(lambda: reader.xml("/tmp/unused-io-declared/xml"))
        _expect_refusal(lambda: reader.xml("/tmp/unused-io-declared/xml", rowTag="row"), "xml")
        _expect_refusal(lambda: reader.jdbc("jdbc:postgresql://127.0.0.1:1/x", "t"), "jdbc")
        writer = frame.write
        _expect_refusal(lambda: writer.orc("/tmp/unused-io-declared/orc"), "orc")
        _expect_row_tag_missing(lambda: writer.xml("/tmp/unused-io-declared/xml"))
        _expect_refusal(lambda: writer.jdbc("jdbc:postgresql://127.0.0.1:1/x", "t"), "jdbc")
        na = frame.na
        replaced = na.replace("a", "z")
        rows = [repr(row) for row in replaced.collect()]
        if rows != ["Row(x=1, y='z')", "Row(x=2, y='b')"]:
            raise SystemExit(f"na.replace rows {rows!r}")
        try:
            na.replace("a")
        except PySparkTypeError as error:
            if error.getCondition() != "ARGUMENT_REQUIRED":
                raise SystemExit(f"omitted value {error.getCondition()!r}") from error
        else:
            raise SystemExit("na.replace omitted value did not refuse")
        try:
            na.replace({1: 10, "b": "B"})
        except PySparkValueError as error:
            if error.getCondition() != "MIXED_TYPE_REPLACEMENT":
                raise SystemExit(f"mixed dict {error.getCondition()!r}") from error
        else:
            raise SystemExit("na.replace mixed dict did not refuse")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
