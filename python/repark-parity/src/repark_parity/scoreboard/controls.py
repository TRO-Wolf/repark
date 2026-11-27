from __future__ import annotations

import math
from pathlib import Path

from repark_parity.scoreboard.classify import classify_pair
from repark_parity.scoreboard.encoding import canonical, read_records

WRITE_CONTROL = "control-write"
BASE_CONTROL = "control-one"


class ControlError(AssertionError):
    """A control cell, a classifier mutation or a float edge gave the wrong answer."""


def _expect(condition: bool, detail: object) -> None:
    if not condition:
        raise ControlError(detail)


def check_float_edges() -> None:
    """Check that the encoding keeps signed zero and both infinities, and folds NaN payloads."""
    _expect(canonical(-0.0) != canonical(0.0), "signed zero folded")
    _expect(canonical(float("nan")) == canonical(-float("nan")), "NaN payloads kept apart")
    _expect(canonical(math.inf) != canonical(-math.inf), "infinities folded")


def check_mutations(base: dict) -> int:
    """Check that six mutations of one equal answer land in their own classes."""
    field = dict(base["schema"]["fields"][0])
    different_value = {**base, "value": [[2]]}
    nullable = {
        **base,
        "schema": {"type": "struct", "fields": [{**field, "nullable": not field["nullable"]}]},
    }
    typed = {**base, "schema": {"type": "struct", "fields": [{**field, "type": "long"}]}}
    mutations = [
        (different_value, "WRONG-VALUE"),
        (nullable, "NULLABILITY-ONLY"),
        (typed, "WRONG-TYPE"),
        ({"outcome": "panicked"}, "PANIC"),
        ({"outcome": "excluded", "reason": "fixture"}, "EXCLUDED"),
        ({"outcome": "refused"}, "REFUSED-WHERE-SPARK-ANSWERS"),
    ]
    for row, label in mutations:
        got = classify_pair(base, row)["class"]
        _expect(got == label, (label, got))
    return len(mutations)


def verify_controls(directory: Path, strict: bool) -> tuple[str, list[dict]]:
    """Check a controls replay; return its summary and the expected classes that moved."""
    cases, left, right, verdicts = (
        read_records(directory / (key + ".jsonl")) for key in ("cases", "spark", "repark", "diff")
    )
    moved = []
    for key, case in cases.items():
        expected = case.get("expected")
        if expected and verdicts[key]["class"] != expected:
            moved.append({"id": key, "expected": expected, "class": verdicts[key]["class"]})
        if case.get("expect_value_equal"):
            outcomes = (left[key]["outcome"], right[key]["outcome"])
            _expect(outcomes == ("answered", "answered"), key)
            _expect(left[key]["value"] == right[key]["value"], (key, left[key], right[key]))
    stored = [left[WRITE_CONTROL]["physical_write"]["rows"]]
    stored.append(right[WRITE_CONTROL]["physical_write"]["rows"])
    _expect(stored == [[[7]], [[7]]], (WRITE_CONTROL, stored))
    check_float_edges()
    count = check_mutations(left[BASE_CONTROL])
    _expect(not (strict and moved), moved)
    summary = (
        f"{len(cases)} paired controls plus {count} classifier mutations and float edge checks"
    )
    return summary, moved
