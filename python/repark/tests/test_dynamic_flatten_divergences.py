"""Measured dynamicFlatten divergences from Spark; cited in map.md."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import LongType, StructField, StructType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dynflatten-divergences").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_three_level_struct_with_keep_column_hits_qualified_name_clash(
    spark: ReparkSession,
) -> None:
    """Three struct expands beside a keep column clash on qualified vs unqualified id."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "Payload",
                StructType(
                    [
                        StructField(
                            "L1",
                            StructType(
                                [
                                    StructField(
                                        "L2",
                                        StructType([StructField("Val", LongType(), True)]),
                                        True,
                                    )
                                ]
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame(
        [{"id": 1, "Payload": {"L1": {"L2": {"Val": 9}}}}],
        schema=schema,
    )
    with pytest.raises(
        AnalysisException,
        match=r"push_down_leaf_projections[\s\S]*qualified field name",
    ):
        frame.dynamicFlatten().to_arrow()
