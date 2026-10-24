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


def spelling_doors() -> dict[str, str]:
    """Return each spelling name mapped to the id of the door it takes."""
    return {name: door for door, names in DOOR_SPELLINGS.items() for name in names}
