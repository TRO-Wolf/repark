"""Read a collected Row back out and build Rows from mappings and field lists.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "Row.asDict",
    "Row.as_dict",
    "Row.from_mapping",
    "Row.from_ordered_fields",
]


def main() -> None:
    """Run the measured Row answers: the dict forms and the builder classmethods."""
    repark = ReparkSession.builder.appName("ex-df-d-row-dicts").master("local[1]").getOrCreate()
    try:
        left = repark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
        first = left.first()
        as_dict_value = first.asDict()
        as_dict_expected = {"g": "a", "k": 1}
        if as_dict_value != as_dict_expected:
            raise SystemExit(f"Row.asDict {as_dict_value!r} != {as_dict_expected!r}")
        first_repr = repr(first)
        first_repr_expected = "Row(g='a', k=1)"
        if first_repr != first_repr_expected:
            raise SystemExit(f"Row.asDict repr {first_repr!r} != {first_repr_expected!r}")
        snake_value = first.as_dict()
        if snake_value != as_dict_expected:
            raise SystemExit(f"Row.as_dict {snake_value!r} != {as_dict_expected!r}")

        wrapped = repark.createDataFrame([("a", 1)], ["g", "k"]).select(
            F.struct("g", "k").alias("s")
        )
        struct_row = wrapped.first()
        recursive_value = struct_row.asDict(True)
        recursive_expected = {"s": {"g": "a", "k": 1}}
        if recursive_value != recursive_expected:
            raise SystemExit(f"Row.asDict recursive {recursive_value!r} != {recursive_expected!r}")

        rebuilt = first.from_mapping({"g": "a", "k": 1})
        rebuilt_fields = rebuilt.__fields__
        rebuilt_fields_expected = ["g", "k"]
        if rebuilt_fields != rebuilt_fields_expected:
            raise SystemExit(
                f"Row.from_mapping fields {rebuilt_fields!r} != {rebuilt_fields_expected!r}"
            )
        rebuilt_value = rebuilt.asDict()
        if rebuilt_value != as_dict_expected:
            raise SystemExit(f"Row.from_mapping {rebuilt_value!r} != {as_dict_expected!r}")
        rebuilt_repr = repr(rebuilt)
        if rebuilt_repr != first_repr_expected:
            raise SystemExit(f"Row.from_mapping repr {rebuilt_repr!r} != {first_repr_expected!r}")
        if rebuilt[0] != "a" or rebuilt["g"] != "a":
            raise SystemExit(f"Row.from_mapping access {rebuilt[0]!r}, {rebuilt['g']!r} != 'a'")

        duplicated = first.from_ordered_fields(("g", "g"), (1, 2))
        duplicated_repr = repr(duplicated)
        duplicated_repr_expected = "Row(g=1, g=2)"
        if duplicated_repr != duplicated_repr_expected:
            raise SystemExit(
                f"Row.from_ordered_fields repr {duplicated_repr!r} != {duplicated_repr_expected!r}"
            )
        duplicated_values = list(duplicated)
        duplicated_values_expected = [1, 2]
        if duplicated_values != duplicated_values_expected:
            raise SystemExit(
                f"Row.from_ordered_fields values {duplicated_values!r}"
                f" != {duplicated_values_expected!r}"
            )
        if duplicated["g"] != 1 or duplicated[1] != 2:
            raise SystemExit(
                f"Row.from_ordered_fields access {duplicated['g']!r}, {duplicated[1]!r} != 1, 2"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
