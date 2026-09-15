"""The abstract type bases: isinstance answers and the ``types.Row`` alias.

pins: types-bases-1/C-006
"""

from __future__ import annotations

from repark.spark import row as row_module
from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.AnsiIntervalType",
    "types.AnyTimeType",
    "types.AtomicType",
    "types.DatetimeType",
    "types.FractionalType",
    "types.IntegralType",
    "types.NumericType",
    "types.Row",
    "types.SpatialType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Check the recorded isinstance answers and the Row identity."""
    expect("AtomicType repr", repr(T.AtomicType()), "AtomicType()")
    expect("NumericType typeName", T.NumericType.typeName(), "numeric")
    expect("IntegralType simpleString", T.IntegralType().simpleString(), "integral")
    expect("FractionalType json", T.FractionalType().json(), '"fractional"')
    expect("DatetimeType typeName", T.DatetimeType.typeName(), "datetime")
    expect("AnyTimeType typeName", T.AnyTimeType.typeName(), "anytime")
    expect("AnsiIntervalType typeName", T.AnsiIntervalType.typeName(), "ansiinterval")
    expect("SpatialType typeName", T.SpatialType.typeName(), "spatial")
    expect("IntegerType is AtomicType", isinstance(T.IntegerType(), T.AtomicType), True)
    expect("IntegerType is NumericType", isinstance(T.IntegerType(), T.NumericType), True)
    expect("IntegerType is IntegralType", isinstance(T.IntegerType(), T.IntegralType), True)
    expect(
        "IntegerType is FractionalType",
        isinstance(T.IntegerType(), T.FractionalType),
        False,
    )
    expect("DoubleType is FractionalType", isinstance(T.DoubleType(), T.FractionalType), True)
    expect("DateType is DatetimeType", isinstance(T.DateType(), T.DatetimeType), True)
    expect("TimeType is AnyTimeType", isinstance(T.TimeType(), T.AnyTimeType), True)
    expect(
        "TimestampType is AnyTimeType",
        isinstance(T.TimestampType(), T.AnyTimeType),
        False,
    )
    expect(
        "DayTimeIntervalType is AnsiIntervalType",
        isinstance(T.DayTimeIntervalType(), T.AnsiIntervalType),
        True,
    )
    expect(
        "GeographyType is SpatialType",
        isinstance(T.GeographyType(4326), T.SpatialType),
        True,
    )
    expect("StringType is SpatialType", isinstance(T.StringType(), T.SpatialType), False)
    expect("types.Row is repark.spark.row.Row", T.Row is row_module.Row, True)


if __name__ == "__main__":
    main()
