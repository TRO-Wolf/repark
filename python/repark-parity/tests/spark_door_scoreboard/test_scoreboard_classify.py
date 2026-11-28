from __future__ import annotations

import math

import pyarrow as pa
import pytest

from repark_parity.scoreboard.classify import classify_pair
from repark_parity.scoreboard.controls import check_float_edges, check_mutations
from repark_parity.scoreboard.encoding import canonical
from repark_parity.scoreboard.engine import arrow_value, table_rows


def field(name: str = "v", kind: object = "integer", nullable: bool = True) -> dict:
    return {"name": name, "type": kind, "nullable": nullable, "metadata": {}}


def answered(value: list, *fields: dict, **extra: object) -> dict:
    schema = {"type": "struct", "fields": list(fields) or [field()]}
    return {"id": "cell", "outcome": "answered", "schema": schema, "value": value, **extra}


def refused(error_class: str | None = "DIVIDE_BY_ZERO", sqlstate: str | None = "22012") -> dict:
    return {"id": "cell", "outcome": "refused", "error_class": error_class, "sqlstate": sqlstate}


def label(left: dict, right: dict) -> str:
    return classify_pair(left, right)["class"]


def test_equal_answers_are_equal() -> None:
    verdict = classify_pair(answered([[1]]), answered([[1]]))
    assert verdict["class"] == "EQUAL"
    assert verdict["value_diff"] is False
    assert verdict["schema_diff"] is False


def test_one_cell_per_class() -> None:
    assert label(answered([[1]]), answered([[2]])) == "WRONG-VALUE"
    assert label(answered([[1]]), answered([[1]], field(kind="long"))) == "WRONG-TYPE"
    assert label(answered([[1]]), answered([[1]], field(nullable=False))) == "NULLABILITY-ONLY"
    assert label(refused(), refused()) == "BOTH-REFUSED"
    assert label(refused(), answered([[1]])) == "ACCEPTED-WHERE-SPARK-REFUSED"
    assert label(answered([[1]]), refused()) == "REFUSED-WHERE-SPARK-ANSWERS"
    assert label(answered([[1]]), {"id": "cell", "outcome": "panicked"}) == "PANIC"
    excluded = {"id": "cell", "outcome": "excluded", "reason": "setup: fixture"}
    assert label(answered([[1]]), excluded) == "EXCLUDED"


def test_panic_outranks_exclusion() -> None:
    excluded = {"id": "cell", "outcome": "excluded", "reason": "setup"}
    assert label(excluded, {"id": "cell", "outcome": "panicked"}) == "PANIC"


def test_unknown_outcome_fails() -> None:
    with pytest.raises(ValueError, match="unknown outcome"):
        classify_pair({"id": "cell", "outcome": "answered"}, {"id": "cell", "outcome": "skipped"})


def test_both_refused_records_code_matches() -> None:
    verdict = classify_pair(refused(), refused("ARITHMETIC_OVERFLOW", None))
    assert verdict["error_class_match"] is False
    assert verdict["sqlstate_match"] is None
    same = classify_pair(refused(), refused())
    assert same["error_class_match"] is True
    assert same["sqlstate_match"] is True


def test_type_outranks_value_and_value_outranks_nullability() -> None:
    assert label(answered([[1]]), answered([[2]], field(kind="long"))) == "WRONG-TYPE"
    assert label(answered([[1]]), answered([[2]], field(nullable=False))) == "WRONG-VALUE"


def test_name_and_decimal_scale_are_structural() -> None:
    assert label(answered([[1]]), answered([[1]], field(name="w"))) == "WRONG-TYPE"
    spark = answered([[{"decimal": "1.5"}]], field(kind="decimal(10,1)"))
    repark = answered([[{"decimal": "1.5"}]], field(kind="decimal(11,1)"))
    verdict = classify_pair(spark, repark)
    assert verdict["class"] == "WRONG-TYPE"
    assert verdict["logical_type_diff"] is True


def test_nested_nullability_alone_is_nullability_only() -> None:
    spark_kind = {"type": "array", "elementType": "integer", "containsNull": True}
    repark_kind = {"type": "array", "elementType": "integer", "containsNull": False}
    spark = answered([[[1]]], field(kind=spark_kind))
    repark = answered([[[1]]], field(kind=repark_kind))
    verdict = classify_pair(spark, repark)
    assert verdict["class"] == "NULLABILITY-ONLY"
    assert verdict["type_or_name_diff"] is False


def test_float_encoding_keeps_signed_zero_and_infinities_and_folds_nan() -> None:
    check_float_edges()
    zero = answered([[canonical(0.0)]], field(kind="double"))
    negative_zero = answered([[canonical(-0.0)]], field(kind="double"))
    assert label(zero, negative_zero) == "WRONG-VALUE"
    nan = answered([[canonical(math.nan)]], field(kind="double"))
    negative_nan = answered([[canonical(-math.nan)]], field(kind="double"))
    assert label(nan, negative_nan) == "EQUAL"
    assert canonical(1.5) == {"float": "0x1.8000000000000p+0"}


def test_timestamps_compare_on_the_nanosecond_timeline() -> None:
    seconds = pa.scalar(1, type=pa.timestamp("s", tz="UTC"))
    micros = pa.scalar(1_000_000, type=pa.timestamp("us", tz="UTC"))
    wall = pa.scalar(1_000_000, type=pa.timestamp("us"))
    assert arrow_value(seconds) == arrow_value(micros) == {"timestamp_ns": 1_000_000_000}
    assert arrow_value(wall) == {"timestamp_ns": 1_000_000_000}
    late = pa.scalar(2_000_000_000, type=pa.date32())
    assert arrow_value(late) == {"date_days": 2_000_000_000}


def test_rendered_timestamp_text_counts_only_when_both_sides_render() -> None:
    columns = {"rendered_columns": ["v"], "observation": "Arrow"}
    spark = answered([[{"timestamp_ns": 0}, "1970-01-01 00:00:00"]], **columns)
    shifted = answered([[{"timestamp_ns": 0}, "1969-12-31 16:00:00"]], **columns)
    verdict = classify_pair(spark, shifted)
    assert verdict["class"] == "WRONG-VALUE"
    assert verdict["primary_value_diff"] is False
    assert verdict["render_diff"] is True
    unrendered = answered([[{"timestamp_ns": 0}]], rendered_columns=[], observation="Arrow")
    one_side = classify_pair(spark, unrendered)
    assert one_side["class"] == "EQUAL"
    assert one_side["value_diff"] is False


def test_rows_compare_as_a_multiset_unless_ordered() -> None:
    spark = answered([[2], [1]], rendered_columns=["v"], observation="Arrow")
    repark = answered([[1], [2]], rendered_columns=[], observation="Arrow")
    assert label(spark, repark) == "EQUAL"
    assert label({**spark, "ordered": True}, {**repark, "ordered": True}) == "WRONG-VALUE"


def test_table_rows_keep_struct_position_and_sort_map_entries() -> None:
    struct = pa.array([{"b": 1, "a": 2}], type=pa.struct([("b", pa.int32()), ("a", pa.int32())]))
    pairs = pa.array([[("z", 1), ("a", 2)]], type=pa.map_(pa.string(), pa.int32()))
    rows = table_rows(pa.table({"s": struct, "m": pairs}))
    assert rows == [[[1, 2], {"map": [["a", 2], ["z", 1]]}]]


def test_six_classifier_mutations_land_in_their_classes() -> None:
    assert check_mutations(answered([[1]], field(nullable=False))) == 6
