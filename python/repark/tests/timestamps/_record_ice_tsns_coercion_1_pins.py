"""Census the tests that expect microseconds for a card spelling.

The card spellings are coalesce, array, CASE, if, nvl, ifnull and UNION ALL
of a nanosecond value with a sibling. A written ``CAST`` or ``TIMESTAMP``
literal is not a card spelling. The script checks the source text it cites
and writes the table.
"""

from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
NESTED = ROOT / "crates" / "repark-spark" / "tests" / "timestamp_ns_nested_shapes.rs"
DOOR = ROOT / "crates" / "repark-spark" / "src" / "tests" / "v3_timestamp_ns_door.rs"
SQL = ROOT / "python" / "repark" / "tests" / "test_ice_tsns_sql_1.py"
KERNEL = ROOT / "crates" / "repark-functions" / "src" / "timestamp_ns_cast" / "tests.rs"

MOVERS = (
    {
        "file": "crates/repark-spark/tests/timestamp_ns_nested_shapes.rs",
        "test": "an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds",
        "spelling": "CASE WHEN id > 0 THEN {c} ELSE NULL END",
        "expectation": (
            "UPDATE of timestamp_ns refuses with "
            "narrowed from nanoseconds to microseconds, and v stays NULL"
        ),
        "becomes": (
            "stores the nine-digit plain column: c's ticks "
            "1767323045123456789 and 1772937000000000001, "
            "z's ticks 1767305045123456789 and 1772940600000000001"
        ),
    },
    {
        "file": "crates/repark-spark/tests/timestamp_ns_nested_shapes.rs",
        "test": "an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds",
        "spelling": "CASE WHEN id < 0 THEN NULL ELSE {c} END",
        "expectation": "same refusal, v stays NULL, for c and for z",
        "becomes": "stores the same nine-digit plain column as the row above",
    },
    {
        "file": "crates/repark-spark/tests/timestamp_ns_nested_shapes.rs",
        "test": "an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds",
        "spelling": "if(id > 0, {c}, NULL)",
        "expectation": "same refusal, v stays NULL, for c and for z",
        "becomes": "stores the same nine-digit plain column as the row above",
    },
    {
        "file": "crates/repark-spark/tests/timestamp_ns_nested_shapes.rs",
        "test": "an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds",
        "spelling": "array({c}, NULL)[0]",
        "expectation": "same refusal, v stays NULL, for c and for z",
        "becomes": "stores the same nine-digit plain column as the row above",
    },
)

HELD = (
    {
        "file": "crates/repark-spark/src/tests/v3_timestamp_ns_door.rs",
        "test": "cast_ns_columns_as_timestamp_floor_to_microsecond_instants",
        "why": "written CAST(ns AS TIMESTAMP) and CAST(tzns AS TIMESTAMP), not a card spelling",
    },
    {
        "file": "crates/repark-spark/src/tests/v3_timestamp_ns_door.rs",
        "test": "timestamp_typed_values_floor_to_microseconds_and_strings_keep_nine_digits",
        "why": "a written TIMESTAMP literal floors; a string keeps nine digits",
    },
    {
        "file": "python/repark/tests/timestamps/test_ice_tsns_sql_1.py",
        "test": "test_cast_ns_column_as_timestamp_floors_like_the_dataframe_spelling",
        "why": "written CAST of the column, the same floor as DataFrame cast",
    },
    {
        "file": "python/repark/tests/timestamps/test_ice_tsns_sql_1.py",
        "test": "test_timestamp_typed_values_floor_like_insert_select",
        "why": "written TIMESTAMP literals floor on insert; strings keep nine digits",
    },
    {
        "file": "crates/repark-functions/src/timestamp_ns_cast/tests.rs",
        "test": "narrowing_floors_instants_to_microseconds",
        "why": "the narrow kernel itself, not a card spelling",
    },
    {
        "file": "crates/repark-spark/tests/timestamp_ns_nested_shapes.rs",
        "test": "every_door_refuses_a_nested_nanosecond_leaf_and_writes_nothing",
        "why": (
            "coalesce(ns, NULL) and array(ns, NULL)[0] feed a nested leaf; "
            "the pin is the nested-leaf refusal, which still applies when the spelling stays ns"
        ),
    },
)


def require(path: Path, needle: str) -> None:
    """Fail when ``needle`` is not in ``path``."""
    text = path.read_text(encoding="utf-8")
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle}")


def check_sources() -> None:
    """Confirm every cited spelling and test is still in the tree."""
    nested = NESTED.read_text(encoding="utf-8")
    for row in MOVERS:
        if row["spelling"] not in nested:
            raise SystemExit(f"missing spelling {row['spelling']}")
    require(NESTED, "narrowed from nanoseconds to microseconds")
    require(NESTED, "an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds")
    require(NESTED, "coalesce(ns, NULL)")
    require(DOOR, "cast_ns_columns_as_timestamp_floor_to_microsecond_instants")
    require(DOOR, "timestamp_typed_values_floor_to_microseconds_and_strings_keep_nine_digits")
    require(SQL, "test_cast_ns_column_as_timestamp_floors_like_the_dataframe_spelling")
    require(SQL, "test_timestamp_typed_values_floor_like_insert_select")
    require(KERNEL, "narrowing_floors_instants_to_microseconds")
    for row in HELD:
        require(ROOT / row["file"], row["test"])


def markdown() -> str:
    """Return the census."""
    lines = [
        "# Microsecond pins — ICE-TSNS-COERCION-1 slice 0",
        "",
        "Generated by `_record_ice_tsns_coercion_1_pins.py`. Do not edit by hand.",
        "",
        "A card spelling is `coalesce`, `array`, `CASE`, `if`, `nvl`, `ifnull`",
        "or `UNION ALL` of a nanosecond value with a sibling. Four spellings in one",
        "Rust test expect the microsecond narrowing refusal. No Python test does.",
        "`nvl` and `ifnull` have no such pin. Written `CAST` and `TIMESTAMP` literals",
        "are listed afterwards; this card does not move them.",
        "",
        "## Would move",
        "",
        "| file | test | spelling | expectation | would become |",
        "|---|---|---|---|---|",
    ]
    for row in MOVERS:
        lines.append(
            f"| `{row['file']}` | `{row['test']}` | `{row['spelling']}` | "
            f"{row['expectation']} | {row['becomes']} |"
        )
    lines.extend(
        [
            "",
            "## Inspected, would not move",
            "",
            "| file | test | why it stays |",
            "|---|---|---|",
        ]
    )
    for row in HELD:
        lines.append(f"| `{row['file']}` | `{row['test']}` | {row['why']} |")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    """Write the census after checking the cited source."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check_sources()
    args.output.write_text(markdown(), encoding="utf-8")


if __name__ == "__main__":
    main()
