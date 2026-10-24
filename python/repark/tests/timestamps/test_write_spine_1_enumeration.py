"""WRITE-SPINE-1 step 3a: the engine's write doors and the spelling table name each other."""

from __future__ import annotations

from collections import Counter

import _write_spine_1_doors as doors
import pytest

from repark import _native

TEMPLATE_FREE_REACH = frozenset({"rust_api", "none"})


def _engine_door_ids() -> list[str]:
    """Return the door ids the engine enumerates, in its order."""
    return [door for door, _ in _native.write_doors()]


def test_every_write_door_has_a_spelling() -> None:
    """Every door the engine enumerates has at least one spelling name."""
    missing = [door for door in _engine_door_ids() if not doors.DOOR_SPELLINGS.get(door)]
    assert missing == []


def test_every_spelling_names_a_door() -> None:
    """Every door id in the spelling table is a door the engine enumerates, once."""
    engine = _engine_door_ids()
    assert len(engine) == len(set(engine))
    unknown = sorted(set(doors.DOOR_SPELLINGS) - set(engine))
    assert unknown == []
    names = [name for spellings in doors.DOOR_SPELLINGS.values() for name in spellings]
    assert [name for name, count in Counter(names).items() if count > 1] == []


def test_the_spark_flag_separates_the_spark_doors_from_the_native_and_streaming_doors() -> None:
    """The engine marks D1 to D10 as Spark doors and D14 to D21 as not."""
    flags = dict(_native.write_doors())
    spark = [door for door, is_spark in flags.items() if is_spark]
    assert spark == ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10"]
    assert all(
        name.startswith(("native_", "microbatch_"))
        for door, is_spark in flags.items()
        if not is_spark
        for name in doors.DOOR_SPELLINGS.get(door, ())
    )


def test_every_spelling_name_has_its_template_in_the_spelling_corpus() -> None:
    """Each spelling name has an entry, on the same door, with SQL text where it is reachable."""
    corpus = pytest.importorskip("_write_spine_1_spellings")
    entries = {entry.name: entry for entry in corpus.SPELLINGS}
    for name, door in doors.spelling_doors().items():
        assert name in entries, name
        entry = entries[name]
        assert entry.door == door, name
        if entry.reach not in TEMPLATE_FREE_REACH:
            assert entry.template, name
