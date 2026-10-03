from __future__ import annotations

import weakref
from typing import Any

from repark import _native
from repark.spark.filter_quote import _frame_qualifiers_for_bind

_ID_SNAPSHOTS: weakref.WeakKeyDictionary[Any, tuple[Any, list[str | None], list[str]]] = (
    weakref.WeakKeyDictionary()
)


def _frame_id_snapshot(frame: Any) -> tuple[Any, list[str | None], list[str]]:
    native = frame._plan()
    snapshot = _ID_SNAPSHOTS.get(frame)
    if snapshot is not None and snapshot[0] is native:
        return (native, snapshot[1], snapshot[2])
    held = list(_native.attribute_ids(native))
    engines = list(_native.logical_column_names(native))
    _ID_SNAPSHOTS[frame] = (native, held, engines)
    return (native, held, engines)


def _stamped_frame_id_snapshot(frame: Any) -> tuple[Any, list[str | None], list[str]]:
    native, held, engines = _frame_id_snapshot(frame)
    if None in held:
        native = _native.stamp_attribute_ids(native)
        frame._inner = native
        held = list(_native.attribute_ids(native))
        engines = list(_native.logical_column_names(native))
        _ID_SNAPSHOTS[frame] = (native, held, engines)
    return (native, held, engines)


def _alias_frame_qualifiers(child: Any, name: str) -> dict[str, frozenset[str]]:
    return {held: frozenset({name}) for held in _frame_id_snapshot(child)[1] if held is not None}


def _join_frame_qualifiers(child: Any, left: Any, right: Any) -> dict[str, frozenset[str]] | None:
    output_ids = _stamped_frame_id_snapshot(child)[1]
    left_ids = _stamped_frame_id_snapshot(left)[1]
    right_ids = _stamped_frame_id_snapshot(right)[1]
    sources = _native.join_output_sources(child._plan())
    if not any(sources):
        return None
    left_map = left._frame_qualifiers or {}
    right_map = right._frame_qualifiers or {}
    left_names = [left_map.get(held) or frozenset() for held in left_ids]
    right_names = [right_map.get(held) or frozenset() for held in right_ids]
    output: dict[str, frozenset[str]] = {}
    for position, feeds in enumerate(sources):
        if position >= len(output_ids):
            break
        output_id = output_ids[position]
        if output_id is None:
            continue
        names: frozenset[str] = frozenset()
        for is_right, side_position in feeds:
            side = right_names if is_right else left_names
            if side_position < len(side):
                names = names | side[side_position]
        if names:
            output[output_id] = output.get(output_id, frozenset()) | names
    return output or None


def _rewrap_rebound_column(column: Any, rebound: Any) -> Any:
    from repark.spark.column import Column

    return Column(
        rebound,
        sort_ascending=column._sort_ascending,
        sort_nulls_first=column._sort_nulls_first,
        when_pairs=column._when_pairs,
        agg_name=column._agg_name,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        is_aggregate_function=column._is_aggregate_function,
        generator=column._generator,
        generator_cast=column._generator_cast,
        spark_display=column._spark_display,
        projection_name=column._projection_name,
        stable_name=column._stable_name,
        partition_transform=column._partition_transform,
        sql_expr=column._sql_expr,
        attr_id=column._attr_id,
        birth_frame=column._birth_frame,
        qualifiers=column._qualifiers,
        join_sql_expr=column._join_sql_expr,
        g2_range_order_names=column._g2_range_order_names,
        window_spec=column._window_spec,
        alias_metadata=column._alias_metadata,
        outer=column._outer,
    )


def _rebind_qualified_refs(frame: Any, column: Any, for_sort: bool) -> Any:
    native = _stamped_frame_id_snapshot(frame)[0]
    exact = bool(_native.session_case_sensitive(frame._session))
    displays = list(frame.columns)
    rebound = _native.bind_qualified_free_refs(
        native, column._inner, displays, exact, for_sort, _frame_qualifiers_for_bind(frame)
    )
    return _rewrap_rebound_column(column, rebound)


def _expand_select_star_item(frame: Any, item: Any) -> list[Any]:
    written = item if isinstance(item, str) else getattr(item, "_projection_name", None)
    if not isinstance(written, str):
        return [item]
    star = _expand_qualified_star(frame, written)
    return [item] if star is None else star


def _expand_qualified_star(frame: Any, written: str) -> list[Any] | None:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column import Column
    from repark.spark.column_fields import _split_written_name

    split = _split_written_name(written)
    if split is None or split[0] is None or split[1] != "*":
        return None
    native, held, engine_names = _stamped_frame_id_snapshot(frame)
    displays = frame.columns
    if len(displays) != len(engine_names) or not _native.frame_is_relation(native):
        return None
    exact = _native.session_case_sensitive(frame._session)
    found = _native.qualifier_star_positions(
        native,
        ".".join(split[0]),
        displays,
        exact,
        _frame_qualifiers_for_bind(frame),
    )
    if found is None:
        return None
    expanded = []
    for position, parts in found:
        quoted = _quote_ident(engine_names[position])
        if parts:
            quoted = ".".join([*(_quote_ident(part) for part in parts), quoted])
        name = displays[position]
        bound = Column(
            _native.PyColumn.column(quoted).alias(name),
            spark_display=name,
            projection_name=name,
            stable_name=True,
            has_free_attribute=True,
            attr_id=held[position],
            birth_frame=frame,
            qualifiers=frozenset(
                (_frame_qualifiers_for_bind(frame) or {}).get(held[position]) or ()
            ),
        )
        bound._sql_expr = quoted
        expanded.append(bound)
    return expanded


def _resolve_sort_qualified_name(
    frame: Any, written: str, qualifier_parts: list[str], name: str
) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column_fields import _raise_unresolved_name

    native, held, engine_names = _stamped_frame_id_snapshot(frame)
    displays = frame.columns
    if len(displays) != len(engine_names) or not _native.frame_is_relation(native):
        return frame._bind_schema_column(written)
    exact = _native.session_case_sensitive(frame._session)
    qualifier = ".".join(qualifier_parts)
    status, hits, plan_quals = _native.resolve_display_name(
        native, name, qualifier, displays, exact, _frame_qualifiers_for_bind(frame)
    )
    if status == "bound":
        if len(hits) > 1 and _native.join_dup_below_wrappers(native):
            _raise_unresolved_name(qualifier_parts, name, displays)
        position = hits[0]
        engine_field = engine_names[position]
        quoted = _quote_ident(engine_field)
        held_parts = plan_quals[0] if plan_quals else None
        if held_parts:
            quoted = ".".join([*(_quote_ident(part) for part in held_parts), quoted])
        return _sort_qualified_bound_column(frame, quoted, displays[position], name, held[position])
    if status == "ambiguous":
        _raise_unresolved_name(qualifier_parts, name, displays)
    status, held_parts, engine_field = _native.grandchild_qualified_key(
        native, name, qualifier, exact, _frame_qualifiers_for_bind(frame)
    )
    if status == "bound" and engine_field is not None:
        quoted = _quote_ident(engine_field)
        if held_parts:
            quoted = ".".join([*(_quote_ident(part) for part in held_parts), quoted])
        return _sort_qualified_bound_column(frame, quoted, name, name, None)
    _raise_unresolved_name(qualifier_parts, name, displays)


def _sort_qualified_bound_column(
    frame: Any, quoted: str, display: str, shown: str, attr: str | None
) -> Any:
    from repark.spark.column import Column

    bound = Column(
        _native.PyColumn.column(quoted).alias(shown),
        spark_display=shown,
        projection_name=shown,
        stable_name=True,
        has_free_attribute=True,
        attr_id=attr,
        birth_frame=frame,
        qualifiers=frozenset((frame._frame_qualifiers or {}).get(attr) or ()),
    )
    bound._sql_expr = quoted
    return bound
