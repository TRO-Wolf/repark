"""WRITE-SPINE-1 step 3a: the spelling names of every write door, keyed by the door id."""

from __future__ import annotations

DOOR_SPELLINGS: dict[str, tuple[str, ...]] = {
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

RUST_API = "rust_api"
PYTHON = "python"
RUST_API_SPELLINGS: frozenset[str] = frozenset(
    {
        "native_insert_conf",
        "native_insert_overwrite_partition",
        "native_ctas",
        "native_append_api",
        "native_merge",
        "native_update",
        "native_delete",
    }
)
RUST_TEST_FILE = "crates/repark-sql/tests/write_door_native.rs"
RUST_TESTS: dict[str, str] = {
    "D15": "d15_native_insert_with_a_session_write_conf_stores_the_row",
    "D16": "d16_native_insert_overwrite_partition_replaces_the_partition",
    "D17": "d17_native_create_table_as_stores_the_query_rows",
    "D18": "d18_the_rust_append_api_stores_the_batch",
    "D19": "d19_native_merge_inserts_and_updates",
    "D20": "d20_native_update_and_delete_change_the_rows",
}


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
