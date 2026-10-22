from __future__ import annotations

import pytest
from _acceptance_commit_resolver import (
    UnresolvedError,
    attempted_snapshot,
    missing_properties,
    stable_snapshot_ids,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Reads:
    def __init__(self, *answers: set[int] | Exception) -> None:
        self.queue = list(answers)

    def __call__(self) -> set[int]:
        answer = self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        if isinstance(answer, Exception):
            raise answer
        return set(answer)


def test_two_agreeing_reads_resolve() -> None:
    clock = FakeClock()
    resolved = stable_snapshot_ids(Reads({1, 2}, {1, 2}), label="t", sleep=clock.sleep, clock=clock)
    assert resolved == {1, 2}
    assert clock.now == pytest.approx(2.0)


def test_a_late_landing_is_waited_for_until_two_reads_agree() -> None:
    clock = FakeClock()
    resolved = stable_snapshot_ids(
        Reads({1}, {1, 2}, {1, 2}, {1, 2}), label="t", sleep=clock.sleep, clock=clock
    )
    assert resolved == {1, 2}


def test_a_failed_read_is_unresolved_never_not_committed() -> None:
    clock = FakeClock()
    with pytest.raises(UnresolvedError, match="unresolved, a resolver read failed"):
        stable_snapshot_ids(
            Reads({1}, ConnectionResetError("cut")), label="t", sleep=clock.sleep, clock=clock
        )


def test_reads_that_never_agree_are_unresolved_at_the_deadline() -> None:
    clock = FakeClock()
    flips = iter([{1}, {1, 2}] * 40)
    with pytest.raises(UnresolvedError, match="never agreed"):
        stable_snapshot_ids(lambda: next(flips), label="t", sleep=clock.sleep, clock=clock)
    assert clock.now >= 60.0


def test_the_held_metadata_names_exactly_one_attempt() -> None:
    metadata = {
        "snapshots": [
            {"snapshot-id": 1, "summary": {}},
            {"snapshot-id": 7, "summary": {"engine.operation-id": "op"}},
        ]
    }
    assert attempted_snapshot(metadata, {1}, "t")["summary"]["engine.operation-id"] == "op"
    with pytest.raises(AssertionError, match="names 0 new snapshots"):
        attempted_snapshot(metadata, {1, 7}, "t")
    with pytest.raises(AssertionError, match="names 2 new snapshots"):
        attempted_snapshot(metadata, set(), "t")


def test_a_missing_status_check_property_is_named() -> None:
    expected = {"commit.status-check.num-retries": "1", "commit.status-check.min-wait-ms": "100"}
    assert missing_properties(expected, expected) == {}
    assert missing_properties({"commit.status-check.num-retries": "1"}, expected) == {
        "commit.status-check.min-wait-ms": "100"
    }
