"""Grouped map UDFs: apply/applyInArrow, cogroup, and the state-API refusals.

pins: grouped-surface-1/C-007
"""

from __future__ import annotations

import warnings

import pandas as pd

from repark.errors import (
    PySparkNotImplementedError,
    UnsupportedOperationException,
)
from repark.spark import ReparkSession
from repark.spark.functions import PandasUDFType
from repark.spark.functions_udf import PandasUDFFunction

COVERS: list[str] = [
    "GroupedData.apply",
    "GroupedData.applyInArrow",
    "GroupedData.applyInPandasWithState",
    "GroupedData.cogroup",
    "GroupedData.transformWithState",
    "GroupedData.transformWithStateInPandas",
]


def _demean(pdf: pd.DataFrame) -> pd.DataFrame:
    return pdf.assign(v=pdf.v - pdf.v.mean())


def main() -> None:
    """Run the grouped map UDF answers and the state-API refusals."""
    repark = ReparkSession.builder.appName("ex-df-grouped-udfs").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0)], "id long, v double")
        grouped = frame.groupBy("id")

        udf = PandasUDFFunction(
            _demean, "id long, v double", function_type=PandasUDFType.GROUPED_MAP
        )
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always")
            applied = grouped.apply(udf)
        if not any("applyInPandas" in str(item.message) for item in seen):
            raise SystemExit("GroupedData.apply did not emit the applyInPandas warning")
        applied_rows = sorted(tuple(row) for row in applied.collect())
        applied_expected = [(1, -0.5), (1, 0.5), (2, 0.0)]
        if applied_rows != applied_expected:
            raise SystemExit(f"GroupedData.apply rows {applied_rows!r} != {applied_expected!r}")

        arrow_rows = sorted(
            tuple(row) for row in grouped.applyInArrow(lambda t: t, "id long, v double").collect()
        )
        arrow_expected = [(1, 1.0), (1, 2.0), (2, 3.0)]
        if arrow_rows != arrow_expected:
            raise SystemExit(f"GroupedData.applyInArrow rows {arrow_rows!r} != {arrow_expected!r}")

        cogrouped = grouped.cogroup(frame.filter("v > 1").groupBy("id"))
        paired = cogrouped.applyInPandas(
            lambda left, right: pd.DataFrame({"nl": [len(left)], "nr": [len(right)]}),
            "nl long, nr long",
        )
        paired_rows = sorted(tuple(row) for row in paired.collect())
        paired_expected = [(1, 1), (2, 1)]
        if paired_rows != paired_expected:
            raise SystemExit(
                f"PandasCogroupedOps.applyInPandas rows {paired_rows!r} != {paired_expected!r}"
            )

        try:
            grouped.applyInPandasWithState(
                lambda key, pdfs, state: iter([]),
                "id long",
                "c long",
                "Append",
                "NoTimeout",
            )
            raise SystemExit("GroupedData.applyInPandasWithState did not refuse")
        except UnsupportedOperationException as error:
            if error.getCondition() != "_LEGACY_ERROR_TEMP_3176":
                raise SystemExit(
                    f"applyInPandasWithState condition {error.getCondition()!r}"
                ) from error

        try:
            grouped.transformWithState(object(), "id long", "Append", "None")
            raise SystemExit("GroupedData.transformWithState did not refuse")
        except PySparkNotImplementedError as error:
            if error.getMessageParameters() != {"feature": "transformWithState"}:
                raise SystemExit(
                    f"transformWithState params {error.getMessageParameters()!r}"
                ) from error
        try:
            grouped.transformWithStateInPandas(object(), "id long", "Append", "None")
            raise SystemExit("GroupedData.transformWithStateInPandas did not refuse")
        except PySparkNotImplementedError as error:
            if error.getMessageParameters() != {"feature": "transformWithStateInPandas"}:
                raise SystemExit(
                    f"transformWithStateInPandas params {error.getMessageParameters()!r}"
                ) from error
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
