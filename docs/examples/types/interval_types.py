"""ANSI interval types and the time-of-day type: field ranges and display strings.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.TimeType",
    "types.DayTimeIntervalType",
    "types.YearMonthIntervalType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured construction and display answers for the interval family."""
    expect("TimeType.simpleString", T.TimeType().simpleString(), "time(6)")
    expect("TimeType(9).simpleString", T.TimeType(9).simpleString(), "time(9)")
    expect("TimeType.repr", repr(T.TimeType(9)), "TimeType(9)")

    day_second = T.DayTimeIntervalType()
    expect("DayTimeIntervalType.simpleString", day_second.simpleString(), "interval day to second")
    expect("DayTimeIntervalType.fields", (day_second.startField, day_second.endField), (0, 3))
    expect("DayTimeIntervalType.DAY", T.DayTimeIntervalType.DAY, 0)
    minute_second = T.DayTimeIntervalType(2, 3)
    expect(
        "DayTimeIntervalType(2,3).simpleString",
        minute_second.simpleString(),
        "interval minute to second",
    )
    expect("DayTimeIntervalType(2,3).repr", repr(minute_second), "DayTimeIntervalType(2, 3)")

    expect(
        "YearMonthIntervalType.simpleString",
        T.YearMonthIntervalType().simpleString(),
        "interval year to month",
    )
    expect(
        "YearMonthIntervalType(1,1).repr",
        repr(T.YearMonthIntervalType(1, 1)),
        "YearMonthIntervalType(1, 1)",
    )


if __name__ == "__main__":
    main()
