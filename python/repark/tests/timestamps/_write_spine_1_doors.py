"""WRITE-SPINE-1 step 3a: the spelling names of every write door, keyed by the door id."""

from __future__ import annotations

import _write_spine_1_spellings as corpus

RUST_API = corpus.RUST_API
PYTHON = "python"
RUST_TEST_FILE = "crates/repark-sql/tests/write_door_native.rs"
RUST_TESTS: dict[str, str] = {
    "D15": "d15_native_insert_with_a_session_write_conf_stores_the_row",
    "D16": "d16_native_insert_overwrite_partition_replaces_the_partition",
    "D17": "d17_native_create_table_as_stores_the_query_rows",
    "D18": "d18_the_rust_append_api_stores_the_batch",
    "D19": "d19_native_merge_inserts_and_updates",
    "D20": "d20_native_update_and_delete_change_the_rows",
}


def door_number(door: str) -> int:
    """Return the number of a door id such as ``D14``."""
    return int(door.removeprefix("D"))


def group_spellings_by_door() -> dict[str, tuple[str, ...]]:
    """Return each door id of the corpus mapped to its spelling names, in door order."""
    grouped: dict[str, list[str]] = {}
    for spelling in corpus.SPELLINGS:
        grouped.setdefault(spelling.door, []).append(spelling.name)
    return {door: tuple(grouped[door]) for door in sorted(grouped, key=door_number)}


def select_rust_api_spellings() -> frozenset[str]:
    """Return the spellings only Rust tests run: reach ``rust_api``, or native SQL on D15-D20."""
    return frozenset(
        spelling.name
        for spelling in corpus.SPELLINGS
        if spelling.reach == RUST_API
        or (spelling.reach == corpus.NATIVE_SQL and spelling.door in RUST_TESTS)
    )


DOOR_SPELLINGS: dict[str, tuple[str, ...]] = group_spellings_by_door()
RUST_API_SPELLINGS: frozenset[str] = select_rust_api_spellings()


def spelling_reach(name: str) -> str:
    """Return ``rust_api`` for a spelling only a Rust test executes, else ``python``."""
    return RUST_API if name in RUST_API_SPELLINGS else PYTHON


def rust_api_only_doors() -> list[str]:
    """Return the doors whose every spelling is reached only through the Rust API."""
    return [
        door
        for door, names in DOOR_SPELLINGS.items()
        if names and all(spelling_reach(name) == RUST_API for name in names)
    ]


def spelling_doors() -> dict[str, str]:
    """Return each spelling name mapped to the id of the door it takes."""
    return {name: door for door, names in DOOR_SPELLINGS.items() for name in names}
