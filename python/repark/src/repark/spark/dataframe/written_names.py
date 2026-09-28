"""Written-name resolution behind the DataFrame name entry points."""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark import _native
from repark.errors import AnalysisException
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _bind_written_column(frame: DataFrame, name: str) -> Column:
    """Bind one written name by the session rule, quoting its engine identifier.

    Preserve the requested display spelling and attach origin metadata for joins.
    """
    from repark.spark.functions import col as _written_col

    plan = frame._plan()
    columns = frame.columns
    if _native.frame_is_exact(plan):
        if "." in name and name not in columns and frame._display_names is None:
            _native.resolve_df_names(plan, [name])
        else:
            _native.match_display_names(plan, [name], columns)
        if frame._display_names is None:
            return _written_col(name)
        held = name
    elif name in columns and columns.count(name) == 1:
        held = name
    else:
        _written, hits, disposition = _native.match_display_names(plan, [name], columns)[0]
        if (
            disposition == "missing"
            and "." in name
            and frame._display_names is None
            and _native.resolve_df_names(plan, [name])[0][3] == "bound"
        ):
            return _written_col(name)
        if disposition == "missing":
            raise AnalysisException(
                f"A column with name `{name}` cannot be resolved; available columns: {columns}"
            )
        if disposition == "ambiguous" and name in columns:
            could_be = ", ".join(f"`{hit}`" for hit in hits)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{name}` is ambiguous, could be: [{could_be}]."
            )
        if disposition == "ambiguous":
            unique = list(dict.fromkeys(hits))
            raise AnalysisException(
                f"A column with name `{name}` is ambiguous among case-insensitive matches: "
                f"{unique}; available columns: {columns}"
            )
        held = hits[0]
    engine_field = frame._engine_field_for_display(held)
    quoted = _quote_ident(engine_field)
    return Column(
        _native.PyColumn.column(quoted).alias(name),
        spark_display=name,
        projection_name=name,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=quoted,
        origin_plan_id=frame._plan_id,
        origin_field=held,
    )


def _refuse_folded_with_columns_keys(frame: DataFrame, keys: list[str]) -> None:
    """Refuse folded withColumns keys under IgnoreCase (COLUMN_ALREADY_EXISTS).

    Under Exact dict keys cannot collide, so the native entry answers ok.
    """
    _native.refuse_folded_duplicate_keys(frame._plan(), keys)


def _match_with_columns_keys(
    frame: DataFrame, keys: list[str]
) -> tuple[list[str | None], list[str]]:
    """Match withColumns keys against bound displays by the session rule.

    Return the replacing key per bound position (None keeps the column) plus
    the keys that append. Under IgnoreCase every hit replaces; under Exact
    only exact hits replace.
    """
    plan = frame._plan()
    displays = frame.columns
    if _native.frame_is_exact(plan):
        wanted = set(keys)
        held = set(displays)
        matches = [display if display in wanted else None for display in displays]
        return matches, [key for key in keys if key not in held]
    owner: dict[str, str] = {}
    appends: list[str] = []
    for key, hits, _disposition in _native.match_display_names(plan, keys, displays):
        if not hits:
            appends.append(key)
        for hit in hits:
            owner[hit] = key
    return [owner.get(display) for display in displays], appends


def _locate_rename_targets(frame: DataFrame, existing: str) -> list[int]:
    """Return bound positions a rename rewrites by the session rule.

    Under IgnoreCase every hit rewrites (twin fan-out); under Exact only exact
    hits rewrite. Exact-duplicate hits rewrite nothing, and misses rewrite
    nothing, so the caller keeps the frame unchanged.
    """
    plan = frame._plan()
    displays = frame.columns
    if _native.frame_is_exact(plan):
        positions = [index for index, display in enumerate(displays) if display == existing]
    else:
        _written, hits, _disposition = _native.match_display_names(plan, [existing], displays)[0]
        wanted = set(hits)
        positions = [index for index, display in enumerate(displays) if display in wanted]
    if len(positions) > 1 and len({displays[index] for index in positions}) == 1:
        return []
    return positions


def _rewrite_running_names(
    frame: DataFrame, renames: dict[str, str], names: list[str]
) -> list[str]:
    """Rewrite running names sequentially by the session rule.

    Each entry rewrites every running hit (twin fan-out under IgnoreCase,
    exact hits under Exact); misses rewrite nothing.
    """
    plan = frame._plan()
    if _native.frame_is_exact(plan):
        for old_name, new_name in renames.items():
            names = [new_name if name == old_name else name for name in names]
        return names
    for old_name, new_name in renames.items():
        _written, hits, _disposition = _native.match_display_names(plan, [old_name], names)[0]
        wanted = set(hits)
        names = [new_name if name in wanted else name for name in names]
    return names
