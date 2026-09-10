from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession

_OVERRIDE_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _override_literal(key: str, value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        from repark.spark._idents import sql_string_literal

        return sql_string_literal(value)
    raise PySparkTypeError(
        f"run_maintenance override {key!r} must be bool, int, float, or str,"
        f" got {type(value).__name__}"
    )


def run_maintenance(
    self: ReparkSession,
    table: str,
    dry_run: bool = True,
    **overrides: Any,
) -> DataFrame:
    """Plan or apply the CALL run_maintenance maintenance steps for one table."""
    if not isinstance(table, str):
        raise PySparkTypeError(f"run_maintenance table must be str, got {type(table).__name__}")
    if not isinstance(dry_run, bool):
        raise PySparkTypeError(
            f"run_maintenance dry_run must be bool, got {type(dry_run).__name__}"
        )
    from repark.spark._idents import quote_ident_if_needed, sql_string_literal
    from repark.spark.session.catalog_resolution import _join_table_identifier_segments
    from repark.spark.session.sql_relations import _parse_table_identifier_segments

    segments: list[str] = _parse_table_identifier_segments(self.resolve_table_name(table))
    if not segments:
        raise PySparkValueError("run_maintenance table must not be empty")
    catalog: str = quote_ident_if_needed(segments[0])
    table_arg: str = _join_table_identifier_segments(segments[1:])
    parts: list[str] = [
        f"table => {sql_string_literal(table_arg)}",
        f"dry_run => {'true' if dry_run else 'false'}",
    ]
    for key, value in overrides.items():
        if _OVERRIDE_KEY.fullmatch(key) is None:
            raise PySparkValueError(f"run_maintenance override key {key!r} is not a plain name")
        parts.append(f"{key} => {_override_literal(key, value)}")
    return self.sql(f"CALL {catalog}.system.run_maintenance({', '.join(parts)})")
