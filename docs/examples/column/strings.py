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
    """Run the measured three-valued flags, filters, and slice answers."""
    repark = ReparkSession.builder.appName("ex-col-strings").master("local[1]").getOrCreate()
    try:
        words = repark.createDataFrame(
            [
                ("apple",),
                ("mango",),
                ("cherry",),
                ("apple pie",),
                (None,),
                ("",),
                ("Äpfel",),
                ("hi",),
            ],
            ["s"],
        )
        found = words.select(words.s, words.s.contains("an"))
        found_rows = set(found.collect())
        found_expected = {
            ("apple", False),
            ("mango", True),
            ("cherry", False),
            ("apple pie", False),
            (None, None),
            ("", False),
            ("Äpfel", False),
            ("hi", False),
        }
        if found_rows != found_expected:
            raise SystemExit(f"Column.contains flags {found_rows!r} != {found_expected!r}")
        picked = words.filter(words.s.contains("an"))
        picked_rows = set(picked.collect())
        picked_expected = {("mango",)}
        if picked_rows != picked_expected:
            raise SystemExit(f"Column.contains rows {picked_rows!r} != {picked_expected!r}")

        prefix_flags = {
            ("apple", True),
            ("mango", False),
            ("cherry", False),
            ("apple pie", True),
            (None, None),
            ("", False),
            ("Äpfel", False),
            ("hi", False),
        }
        prefix_rows = {("apple",), ("apple pie",)}
        marked = words.select(words.s, words.s.startswith("app"))
        startswith_flags = set(marked.collect())
        if startswith_flags != prefix_flags:
            raise SystemExit(f"Column.startswith flags {startswith_flags!r} != {prefix_flags!r}")
        kept = words.filter(words.s.startswith("app"))
        startswith_rows = set(kept.collect())
        if startswith_rows != prefix_rows:
            raise SystemExit(f"Column.startswith rows {startswith_rows!r} != {prefix_rows!r}")
        tail = words.select(words.s, words.s.endswith("e"))
        endswith_flags = set(tail.collect())
        if endswith_flags != prefix_flags:
            raise SystemExit(f"Column.endswith flags {endswith_flags!r} != {prefix_flags!r}")
        tailed = words.filter(words.s.endswith("e"))
        endswith_rows = set(tailed.collect())
        if endswith_rows != prefix_rows:
            raise SystemExit(f"Column.endswith rows {endswith_rows!r} != {prefix_rows!r}")
        wildcard = words.select(words.s, words.s.like("app%"))
        like_flags = set(wildcard.collect())
        if like_flags != prefix_flags:
            raise SystemExit(f"Column.like flags {like_flags!r} != {prefix_flags!r}")
        wild = words.filter(words.s.like("app%"))
        like_rows = set(wild.collect())
        if like_rows != prefix_rows:
            raise SystemExit(f"Column.like rows {like_rows!r} != {prefix_rows!r}")
        wildcard_upper = words.select(words.s, words.s.ilike("APP%"))
        ilike_flags = set(wildcard_upper.collect())
        if ilike_flags != prefix_flags:
            raise SystemExit(f"Column.ilike flags {ilike_flags!r} != {prefix_flags!r}")
        wilder = words.filter(words.s.ilike("APP%"))
        ilike_rows = set(wilder.collect())
        if ilike_rows != prefix_rows:
            raise SystemExit(f"Column.ilike rows {ilike_rows!r} != {prefix_rows!r}")
        regex = words.select(words.s, words.s.rlike("^a"))
        rlike_flags = set(regex.collect())
        if rlike_flags != prefix_flags:
            raise SystemExit(f"Column.rlike flags {rlike_flags!r} != {prefix_flags!r}")
        matched = words.filter(words.s.rlike("^a"))
        rlike_rows = set(matched.collect())
        if rlike_rows != prefix_rows:
            raise SystemExit(f"Column.rlike rows {rlike_rows!r} != {prefix_rows!r}")

        heads = words.select(words.s.substr(1, 3))
        head_columns = ["substr(s, 1, 3)"]
        if heads.columns != head_columns:
            raise SystemExit(f"Column.substr columns {heads.columns!r} != {head_columns!r}")
        head_rows = set(heads.collect())
        head_expected = {("",), ("app",), ("che",), ("hi",), ("man",), ("Äpf",), (None,)}
        if head_rows != head_expected:
            raise SystemExit(f"Column.substr rows {head_rows!r} != {head_expected!r}")
        zero_start = words.select(words.s.substr(0, 2))
        zero_columns = ["substr(s, 0, 2)"]
        if zero_start.columns != zero_columns:
            raise SystemExit(f"Column.substr columns {zero_start.columns!r} != {zero_columns!r}")
        zero_rows = set(zero_start.collect())
        zero_expected = {("",), ("ap",), ("ch",), ("hi",), ("ma",), ("Äp",), (None,)}
        if zero_rows != zero_expected:
            raise SystemExit(f"Column.substr rows {zero_rows!r} != {zero_expected!r}")
        column_args = words.select(words.s.substr(F.lit(2), F.lit(2)))
        lit_columns = ["substr(s, 2, 2)"]
        if column_args.columns != lit_columns:
            raise SystemExit(f"Column.substr columns {column_args.columns!r} != {lit_columns!r}")
        lit_rows = set(column_args.collect())
        lit_expected = {("",), ("an",), ("he",), ("i",), ("pf",), ("pp",), (None,)}
        if lit_rows != lit_expected:
            raise SystemExit(f"Column.substr rows {lit_rows!r} != {lit_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
