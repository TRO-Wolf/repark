from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

STABLE_GAP_S = 2.0
RESOLVE_DEADLINE_S = 60.0


class UnresolvedError(AssertionError):
    pass


def stable_snapshot_ids(
    read: Callable[[], set[int]],
    *,
    label: str,
    gap_s: float = STABLE_GAP_S,
    deadline_s: float = RESOLVE_DEADLINE_S,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> set[int]:
    deadline = clock() + deadline_s
    while clock() < deadline:
        try:
            first = read()
            sleep(gap_s)
            second = read()
        except Exception as error:
            msg = f"{label}: unresolved, a resolver read failed: {type(error).__name__}"
            raise UnresolvedError(msg) from error
        if first == second:
            return second
    msg = f"{label}: unresolved, two reads {gap_s} s apart never agreed in {deadline_s} s"
    raise UnresolvedError(msg)


def attempted_snapshot(metadata: Mapping[str, Any], before: set[int], label: str) -> dict[str, Any]:
    attempted = [
        snapshot
        for snapshot in metadata.get("snapshots", [])
        if int(snapshot["snapshot-id"]) not in before
    ]
    if len(attempted) != 1:
        msg = f"{label}: the held metadata file names {len(attempted)} new snapshots, not 1"
        raise AssertionError(msg)
    return dict(attempted[0])


def missing_properties(stored: Mapping[str, str], expected: Mapping[str, str]) -> dict[str, str]:
    return {key: value for key, value in expected.items() if stored.get(key) != value}
