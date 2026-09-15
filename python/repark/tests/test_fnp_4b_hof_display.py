"""FNP-4B round 6 — selectExpr higher-order display must hide the packing marker.

Q3 hand-off to run 16a (owns the HOF rewrite): the selectExpr door packs the
lambda array argument into __repark_hof_array_field__ and the packed form leaks
into the output display name, while the SQL door shows the unpacked column. The
round-6 ruling bans a display band-aid over the semantic difference, so this pin
stays red on 16a's fix.
pins: fnp-4b/C-026
"""

from __future__ import annotations

import pytest

from repark import ReparkSession


def _session() -> ReparkSession:
    """Session behind the marker-leak repro."""
    return ReparkSession.builder.appName("fnp-4b-hof-display").getOrCreate()


@pytest.mark.xfail(
    strict=True,
    reason="hand-off to run 16a: FNP-4B C-026 selectExpr leaks the HOF packing marker",
)
def test_select_expr_transform_display_hides_the_packing_marker() -> None:
    """Q3 red pin: selectExpr transform display carries no packing marker."""
    spark = _session()
    spark.sql("SELECT array(1, 2, 3) AS arr").createOrReplaceTempView("fnp4b_hof_arr")
    frame = spark.sql("SELECT * FROM fnp4b_hof_arr").selectExpr("transform(arr, x -> x + 1)")
    assert frame.to_arrow().column(0).to_pylist() == [[2, 3, 4]]
    name = frame.to_arrow().schema[0].name
    assert "__repark_hof_array_field__" not in name
