"""Attribute-token scanning: single-frame rewrite, select fast path, birth names."""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING, Any

from repark import _native
from repark.spark import column_fields as _column_fields
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._temp_views import scratch_view_name

if TYPE_CHECKING:
    from repark.spark.column import Column
    from repark.spark.dataframe.core import DataFrame


_ATTR_TOKEN_RE = re.compile(
    r"__REPARK_ATTR_([A-Za-z0-9]+)__F(\d+)__([\w\\|]*?)(?:__D([0-9A-Fa-f]*))?__"
)


def _token_leaf_display(match: re.Match[str]) -> str | None:
    """Decode a token's ``__D`` leaf display, or ``None`` when absent or broken."""
    coded = match.group(4)
    if coded is None:
        return None
    try:
        return bytes.fromhex(coded).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None


def _join_condition_attr_names(frame: DataFrame, cond_sql: str) -> dict[str, str]:
    """Map condition token ids to the display name on their birth frame.

    The birth frame is the live frame behind the token's frame field. A token
    whose frame is gone, or whose id the frame no longer emits, falls back to
    the leaf display the token itself carries; only a token with neither stays
    out of the map, and the native preparer then refuses loud instead of
    guessing.
    """
    names: dict[str, str] = {}
    if "__REPARK_ATTR_" not in cond_sql:
        return names
    registry = frame._alive_token.get("frame_registry", {})
    by_frame: dict[int, dict[str, str]] = {}
    for match in _ATTR_TOKEN_RE.finditer(cond_sql):
        attr_id = match.group(1)
        if attr_id in names:
            continue
        frame_id = int(match.group(2))
        held = by_frame.get(frame_id)
        if held is None:
            birth = registry.get(frame_id)
            held = _birth_frame_attr_names(birth)
            by_frame[frame_id] = held
        if attr_id in held:
            names[attr_id] = held[attr_id]
            continue
        leaf = _token_leaf_display(match)
        if leaf:
            names[attr_id] = leaf
    return names


def _birth_frame_attr_names(birth: DataFrame | None) -> dict[str, str]:
    """Map a birth frame's attribute ids to its display names, else empty."""
    if birth is None or not birth._alive_token.get("alive", False):
        return {}
    displays = list(birth.columns)
    held, _engines = _column_fields._stamped_ids_and_engines(birth)
    if len(displays) != len(held):
        return {}
    return {attr_id: display for attr_id, display in zip(held, displays, strict=True) if attr_id}


def _replace_local_attr_token(
    match: re.Match[str],
    *,
    frame: DataFrame,
    held: list[str | None],
    engines: list[str],
    spell: Any,
) -> str:
    """Rewrite one ``__REPARK_ATTR_*`` token against a single frame's attribute ids."""
    attr_id = match.group(1)
    frame._raise_if_id_not_emitted(attr_id)
    if attr_id not in held:
        sources = _native.projection_source_ids(frame._plan())
        for position, source in enumerate(sources):
            if source == attr_id and position < len(engines):
                return _quote_ident_sql(spell(engines[position]))
        return match.group(0)
    return _quote_ident_sql(spell(engines[held.index(attr_id)]))


def _rewrite_attr_tokens_local(join_sql: str, frame: DataFrame, spell: Any = str) -> str:
    """Rewrite attribute tokens to quoted engine fields, or their ``spell`` names."""
    held, engines = _column_fields._stamped_ids_and_engines(frame)
    return _ATTR_TOKEN_RE.sub(
        functools.partial(
            _replace_local_attr_token, frame=frame, held=held, engines=engines, spell=spell
        ),
        join_sql,
    )


def _select_via_attr_sql(
    frame: DataFrame,
    projected: list[Column],
    *,
    h1_display_names: list[str] | None,
    h1_engine_names: list[str] | None,
) -> DataFrame | None:
    from repark.spark._idents import quote_ident as _quote_ident

    if frame._display_names is None or frame._engine_names is None:
        return None
    copy_name = functools.partial(_native.attribute_copy_name, frame._plan())

    proj_parts: list[str] = []
    display_names: list[str] = []
    engine_names: list[str] = []
    used_engines: set[str] = set()
    name_counts: dict[str, int] = {}
    for position, column in enumerate(projected):
        expr_sql = column.join_sql_part()
        if "__REPARK_ATTR_" in expr_sql:
            expr_sql = _rewrite_attr_tokens_local(expr_sql, frame, copy_name)
            if "__REPARK_ATTR_" in expr_sql:
                return None
        display = (
            column._projection_name
            if column._projection_name is not None
            else column.spark_display_part()
        )
        name_counts[display] = name_counts.get(display, 0) + 1
        if h1_engine_names is not None and len(engine_names) < len(h1_engine_names):
            engine = h1_engine_names[len(engine_names)]
            display = (
                h1_display_names[len(engine_names)] if h1_display_names is not None else display
            )
        elif name_counts[display] > 1 or display in {
            name for name, count in name_counts.items() if count > 1
        }:
            engine = f"__repark_sel_{position}"
        else:
            if display.startswith("CAST(") or any(
                ch in display for ch in (" ", "(", ")", "+", "-", "*", "/")
            ):
                engine = f"__repark_sel_{position}"
            else:
                engine = display
        while engine in used_engines:
            engine = f"{engine}_"
        used_engines.add(engine)
        proj_parts.append(f"({expr_sql}) AS {_quote_ident(engine)}")
        display_names.append(display)
        engine_names.append(engine)

    view = scratch_view_name(frame._session, "_repark_h1_sel_")
    frame._session.create_or_replace_temp_view(view, _native.attribute_copies(frame._plan()))
    try:
        planned = frame._session.sql(f"SELECT {', '.join(proj_parts)} FROM {view}")
        child = frame._spawn(planned)
        if h1_display_names is not None:
            child._display_names = h1_display_names
            child._engine_names = h1_engine_names
        else:
            pairs = zip(display_names, engine_names, strict=True)
            needs_identity = len(display_names) != len(set(display_names)) or any(
                display != engine for display, engine in pairs
            )
            if needs_identity:
                child._display_names = display_names
                child._engine_names = engine_names
        return child
    finally:
        frame._session.drop_temp_view(view)


def _emit_join_side_columns(
    side_frame: DataFrame,
    side_alias: str,
    side_tag: str,
    *,
    display_counts: dict[str, int],
    proj_parts: list[str],
    display_names: list[str],
    engine_names: list[str],
) -> None:
    """Project one join side into ``proj_parts`` (walk by position)."""
    if side_frame._display_names is not None and side_frame._engine_names is not None:
        pairs = list(zip(side_frame._display_names, side_frame._engine_names, strict=True))
    else:
        pairs = [(name, name) for name in side_frame.columns]
    for display_name, source_engine in pairs:
        if display_counts.get(display_name, 0) > 1:
            engine_out = (
                f"__repark_{side_tag}_{side_frame._plan_id}_{len(engine_names)}_{display_name}"
            )
        else:
            engine_out = display_name
        proj_parts.append(
            f"{side_alias}.{_quote_ident_sql(source_engine)} AS {_quote_ident_sql(engine_out)}"
        )
        display_names.append(display_name)
        engine_names.append(engine_out)
