"""Match, search, and slice strings with the seven string predicates.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "Column.contains",
    "Column.startswith",
    "Column.endswith",
    "Column.like",
    "Column.ilike",
    "Column.rlike",
    "Column.substr",
]


def main() -> None:
    """Run the measured substring, prefix, suffix, LIKE, regex, and slice answers."""
    repark = ReparkSession.builder.appName("ex-col-strings").master("local[1]").getOrCreate()
    try:
        words = repark.createDataFrame([("apple",), ("mango",), ("cherry",), ("apple pie",)], ["s"])
        found = words.filter(words.s.contains("an"))
        if set(found.collect()) != {("mango",)}:
            raise SystemExit(f"Column.contains rows {set(found.collect())!r} != {('mango',)}")

        prefix = words.filter(words.s.startswith("app"))
        prefix_expected = {("apple",), ("apple pie",)}
        prefix_rows = set(prefix.collect())
        if prefix_rows != prefix_expected:
            raise SystemExit(f"Column.startswith rows {prefix_rows!r} != {prefix_expected!r}")
        suffix = words.filter(words.s.endswith("e"))
        suffix_rows = set(suffix.collect())
        if suffix_rows != prefix_expected:
            raise SystemExit(f"Column.endswith rows {suffix_rows!r} != {prefix_expected!r}")
        wildcard = words.filter(words.s.like("app%"))
        if set(wildcard.collect()) != prefix_expected:
            raise SystemExit(f"Column.like rows {set(wildcard.collect())!r} != {prefix_expected!r}")
        wildcard_upper = words.filter(words.s.ilike("APP%"))
        if set(wildcard_upper.collect()) != prefix_expected:
            raise SystemExit(
                f"Column.ilike rows {set(wildcard_upper.collect())!r} != {prefix_expected!r}"
            )
        regex = words.filter(words.s.rlike("^a"))
        if set(regex.collect()) != prefix_expected:
            raise SystemExit(f"Column.rlike rows {set(regex.collect())!r} != {prefix_expected!r}")

        heads = words.select(words.s.substr(1, 3))
        if heads.columns != ["substr(s, 1, 3)"]:
            raise SystemExit(f"Column.substr columns {heads.columns!r} != ['substr(s, 1, 3)']")
        head_rows = sorted(heads.collect(), key=tuple)
        head_expected = [("app",), ("app",), ("che",), ("man",)]
        if head_rows != head_expected:
            raise SystemExit(f"Column.substr rows {head_rows!r} != {head_expected!r}")
        zero_start = sorted(words.select(words.s.substr(0, 2)).collect(), key=tuple)
        zero_expected = [("ap",), ("ap",), ("ch",), ("ma",)]
        if zero_start != zero_expected:
            raise SystemExit(f"Column.substr rows {zero_start!r} != {zero_expected!r}")
        column_args = sorted(words.select(words.s.substr(F.lit(2), F.lit(2))).collect(), key=tuple)
        column_expected = [("an",), ("he",), ("pp",), ("pp",)]
        if column_args != column_expected:
            raise SystemExit(f"Column.substr rows {column_args!r} != {column_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
