"""WRITE-SPINE-1 step 3a: the engine's write doors and the spelling table name each other."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import _write_spine_1_doors as doors
import _write_spine_1_spellings as corpus

from repark import _native

TEMPLATE_FREE_REACH = frozenset({"rust_api", "none"})
ROOT = Path(__file__).resolve().parents[4]
SPELLING_COUNT = 41
EXPECTED_DOOR_SPELLINGS: dict[str, tuple[str, ...]] = {
    "D1": ("insert_values", "insert_select", "df_insert_into", "df_append", "df_save_as_table"),
    "D2": ("insert_options",),
    "D3": ("insert_by_name",),
    "D4": (
        "insert_overwrite",
        "insert_overwrite_values",
        "df_insert_into_overwrite",
        "df_overwrite_partitions",
    ),
    "D5": ("insert_replace_where", "df_overwrite_condition"),
    "D6": ("ctas", "rtas", "create_or_replace_as"),
    "D7": ("merge_insert", "merge_insert_star", "merge_insert_literal"),
    "D8": ("merge_update_whole", "merge_update_whole_literal", "merge_update_star"),
    "D9": ("merge_update_field", "merge_update_field_literal"),
    "D10": (
        "update_whole_where",
        "update_whole_nowhere",
        "update_col_where",
        "update_col_nowhere",
        "update_field_where",
        "update_field_nowhere",
        "update_field_col_where",
        "update_field_col_nowhere",
    ),
    "D14": ("native_insert",),
    "D15": ("native_insert_conf",),
    "D16": ("native_insert_overwrite_partition",),
    "D17": ("native_ctas",),
    "D18": ("native_append_api",),
    "D19": ("native_merge",),
    "D20": ("native_update", "native_delete"),
    "D21": ("microbatch_sink",),
}


def _engine_door_ids() -> list[str]:
    """Return the door ids the engine enumerates, in its order."""
    return [door for door, _ in _native.write_doors()]


def test_every_write_door_has_a_spelling() -> None:
    """Every door the engine enumerates has at least one spelling name."""
    missing = [door for door in _engine_door_ids() if not doors.DOOR_SPELLINGS.get(door)]
    assert missing == []


def test_every_rust_api_only_door_names_a_rust_test_that_exists() -> None:
    """A door no Python spelling executes names the Rust test that writes through it."""
    rust_only = doors.rust_api_only_doors()
    assert rust_only == ["D15", "D16", "D17", "D18", "D19", "D20"]
    assert sorted(doors.RUST_TESTS) == sorted(rust_only)
    source = (ROOT / doors.RUST_TEST_FILE).read_text(encoding="utf-8")
    missing = [door for door in rust_only if f"async fn {doors.RUST_TESTS[door]}()" not in source]
    assert missing == []


def test_the_spelling_table_is_exactly_the_forty_one_ruled_names() -> None:
    """The table holds the 41 ruled spelling names on their doors, no more and no fewer."""
    assert {door: sorted(names) for door, names in doors.DOOR_SPELLINGS.items()} == {
        door: sorted(names) for door, names in EXPECTED_DOOR_SPELLINGS.items()
    }
    assert sum(len(names) for names in doors.DOOR_SPELLINGS.values()) == SPELLING_COUNT


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
    entries = {entry.name: entry for entry in corpus.SPELLINGS}
    for name, door in doors.spelling_doors().items():
        assert name in entries, name
        entry = entries[name]
        assert entry.door == door, name
        if entry.reach not in TEMPLATE_FREE_REACH and doors.spelling_reach(name) != doors.RUST_API:
            assert entry.template, name
