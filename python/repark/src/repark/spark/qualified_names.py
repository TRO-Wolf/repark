from __future__ import annotations

import re
import weakref
from types import MethodType
from typing import Any, NoReturn

from repark import _native
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)
from repark.spark.filter_quote import (
    _DOTTED_TOKEN_PATTERN,
    _frame_qualifiers_for_bind,
    _unqualified_candidates,
)

_ID_SNAPSHOTS: weakref.WeakKeyDictionary[Any, tuple[Any, list[str | None], list[str]]] = (
    weakref.WeakKeyDictionary()
)
_ENGINE_NAMES: weakref.WeakKeyDictionary[Any, tuple[Any, list[str]]] = weakref.WeakKeyDictionary()
_USING_MARKS: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()


def _frame_engine_names(frame: Any) -> list[str]:
    native = frame._inner
    snapshot = _ENGINE_NAMES.get(frame)
    if snapshot is not None and snapshot[0] is native:
        return list(snapshot[1])
    engines = list(_native.logical_column_names(native))
    _ENGINE_NAMES[frame] = (native, engines)
    return list(engines)


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


def _arrow_c_stream_with_display(frame: Any, requested_schema: Any) -> Any:
    overlay = frame._display_overlay_names()
    if overlay is None:
        return frame._action_inner().__arrow_c_stream__(requested_schema)
    return frame._action_inner().__arrow_c_stream__(requested_schema, list(overlay))


def _refuse_ambiguous_map_input(frame: Any) -> None:
    columns = list(frame.columns)
    held = _stamped_frame_id_snapshot(frame)[1]
    if len(columns) != len(held):
        return
    try:
        sensitive = _native.session_case_sensitive(frame._session)
    except (TypeError, AttributeError):
        sensitive = False
    groups: dict[str, list[int]] = {}
    for position, name in enumerate(columns):
        groups.setdefault(name if sensitive else name.lower(), []).append(position)
    for positions in groups.values():
        if len(positions) < 2:
            continue
        distinct = {
            held[position] if held[position] is not None else f"#{position}"
            for position in positions
        }
        if len(distinct) < 2:
            continue
        reported = columns[positions[0]]
        qualifiers = getattr(frame, "_frame_qualifiers", None) or {}
        candidates = []
        for position in positions:
            names = sorted(qualifiers.get(held[position]) or ())
            candidates.append(f"`{names[0]}`.`{reported}`" if names else f"`{reported}`")
        candidates.sort()
        references = "[" + ", ".join(candidates) + "]"
        quoted = f"`{reported}`"
        _raise_ambiguous_reference(quoted, references)


def _refuse_ambiguous_partition_columns(frame: Any, partition_columns: list[Any]) -> None:
    from repark.spark.dataframe.writer_layout import _raise_analysis

    columns = list(frame.columns)
    if len(set(columns)) == len(columns):
        return
    held = _stamped_frame_id_snapshot(frame)[1]
    if len(columns) != len(held):
        return
    sensitive = bool(_native.session_case_sensitive(frame._session))
    qualifiers = getattr(frame, "_frame_qualifiers", None) or {}
    for column in partition_columns:
        written = str(column)
        folded = [
            position for position, name in enumerate(columns) if name.lower() == written.lower()
        ]
        positions = [
            position for position in folded if not sensitive or columns[position] == written
        ]
        if not positions and len(folded) > 1:
            _raise_analysis(
                f"Partition column `{written}` not found in schema {frame.schema.simpleString()}.",
                "_LEGACY_ERROR_TEMP_1155",
            )
        if len(positions) < 2:
            continue
        distinct = {
            held[position] if held[position] is not None else f"#{position}"
            for position in positions
        }
        if len(distinct) < 2:
            raise AnalysisException(
                f"partitionBy column `{written}` is carried {len(positions)} times by the "
                "frame; a csv path partitionBy needs a column the frame carries once "
                "(Spark writes this shape; rename or drop the repeat first)"
            )
        candidates = []
        for position in positions:
            names = sorted(qualifiers.get(held[position]) or ())
            candidates.append(f"`{names[0]}`.`{written}`" if names else f"`{written}`")
        candidates.sort()
        _raise_ambiguous_reference(f"`{written}`", "[" + ", ".join(candidates) + "]")


def _raise_ambiguous_reference(quoted: str, references: str) -> NoReturn:
    error = AnalysisException(
        f"[AMBIGUOUS_REFERENCE] Reference {quoted} is ambiguous, "
        f"could be: {references}. SQLSTATE: 42704"
    )
    error._spark_error_class = "AMBIGUOUS_REFERENCE"
    error._spark_message_parameters = {"name": quoted, "referenceNames": references}
    error._spark_sql_state = "42704"
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    error.getSqlState = MethodType(_attached_sql_state, error)
    raise error


def _join_frame_qualifiers(child: Any, left: Any, right: Any) -> dict[str, frozenset[str]] | None:
    output_ids = _stamped_frame_id_snapshot(child)[1]
    left_ids = _stamped_frame_id_snapshot(left)[1]
    right_ids = _stamped_frame_id_snapshot(right)[1]
    sources = _native.join_output_sources(child._plan())
    if not any(sources):
        _set_using_mark(child, _merge_using_marks(_using_mark(left), _using_mark(right)))
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
    _set_using_mark(child, _merge_using_marks(_using_mark(left), _using_mark(right)))
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
    _refuse_using_key_columns(frame, [column])
    rebound = _native.bind_qualified_free_refs(
        native, column._inner, displays, exact, for_sort, _frame_qualifiers_for_bind(frame)
    )
    return _rewrap_rebound_column(column, rebound)


def _expand_select_star_item(frame: Any, item: Any) -> list[Any]:
    written = item if isinstance(item, str) else getattr(item, "_projection_name", None)
    if not isinstance(written, str) or "*" not in written:
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
    expanded = _using_star_keys(frame, held, split[0], exact)
    if found is None:
        return expanded or None
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


def _using_star_keys(
    frame: Any, held: list[str | None], qualifier_parts: list[str], exact: bool
) -> list[Any]:
    mark = _using_mark(frame)
    if mark is None or len(qualifier_parts) != 1 or not _using_mark_live(mark, held):
        return []
    folded = qualifier_parts[0] if exact else qualifier_parts[0].lower()
    keys: list[Any] = []
    for quals, display, _alias in mark[2]:
        if _alias and any((cand if exact else cand.lower()) == folded for cand in quals):
            bound = _using_key_name_column(frame, held, qualifier_parts, display)
            if bound is not None:
                keys.append(bound)
    return keys


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
    hidden = _using_key_name_column(frame, held, qualifier_parts, name)
    if hidden is not None:
        return hidden
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


def _route_sort_key_through_input(
    native: Any,
    name: str,
    hits: list[int],
    exact: bool,
    engine_names: list[str],
    is_column_key: bool,
) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column import Column

    spelling: str | None = _native.sort_project_input_spelling(native, name, exact)
    if spelling is not None:
        exact_hits, folded_hits = _unqualified_candidates(name, engine_names)
        matched: int = len(exact_hits)
        if not exact:
            matched += len(folded_hits)
        spelled: str = name if matched > 1 else spelling
        return Column(
            _native.PyColumn.column(_quote_ident(spelled)),
            spark_display=name,
            projection_name=name,
            stable_name=True,
        )
    exact_hits, folded_hits = _unqualified_candidates(name, engine_names)
    matched: int = len(exact_hits)
    if not exact:
        matched += len(folded_hits)
    if matched > 0 or is_column_key:
        return Column(
            _native.PyColumn.column(_quote_ident(name)),
            spark_display=name,
            projection_name=name,
            stable_name=True,
        )
    echo: str = ", ".join(f"`{name}`" for _ in hits)
    raise AnalysisException(
        f"[AMBIGUOUS_REFERENCE] Reference `{name}` is ambiguous, could be: [{echo}]."
    )


def _session_exact(frame: Any) -> bool:
    try:
        return bool(_native.session_case_sensitive(frame._session))
    except (TypeError, AttributeError):
        return False


def _raise_using_key_reference(ref: str) -> NoReturn:
    raise UnsupportedOperationException(
        f"qualified reference {ref} to a USING join key is not supported in repark v1 "
        "(Spark resolves per-side keys; the unqualified key reads the merged value)"
    )


def _using_mark(frame: Any) -> Any:
    if not _USING_MARKS:
        return None
    return _USING_MARKS.get(frame)


def _set_using_mark(frame: Any, mark: Any) -> None:
    if mark is None:
        if _USING_MARKS:
            _USING_MARKS.pop(frame, None)
    else:
        _USING_MARKS[frame] = mark


def _merge_using_marks(first: Any, second: Any) -> Any:
    if first is None:
        return second
    if second is None:
        return first
    kept = first[0] | second[0]
    keys = tuple(dict.fromkeys([*first[1], *second[1]]))
    names = tuple(dict.fromkeys([*first[2], *second[2]]))
    return (kept, keys, names, {**first[3], **second[3]})


def _using_state(child: Any, left: Any, right: Any, keys: list[str], engine_how: str) -> None:
    merged = _merge_using_marks(_using_mark(left), _using_mark(right))
    if engine_how not in ("left", "right", "full"):
        _set_using_mark(child, merged)
        return
    exact = _session_exact(child)
    held = _stamped_frame_id_snapshot(child)[1]
    left_engines = _stamped_frame_id_snapshot(left)[2]
    kept: list[str] = []
    for key in keys:
        folded = key if exact else key.lower()
        left_pos = next(
            (
                index
                for index, name in enumerate(left_engines)
                if (name if exact else name.lower()) == folded
            ),
            None,
        )
        if left_pos is None or left_pos >= len(held):
            continue
        kept_id = held[left_pos]
        if kept_id is not None:
            kept.append(kept_id)
    names: list[tuple[frozenset[str], str, str]] = []
    ids: dict[str, tuple[str, str]] = {}
    for is_right, position, alias, display in _native.using_hidden_key_fields(child._plan()):
        side = right if is_right else left
        side_held = _stamped_frame_id_snapshot(side)[1]
        side_id = side_held[position] if position < len(side_held) else None
        quals = frozenset((side._frame_qualifiers or {}).get(side_id) or ())
        names.append((quals, display, alias))
        if side_id is not None and side_id not in held:
            ids[side_id] = (alias, display)
    if kept and not names:
        right_map = right._frame_qualifiers or {}
        refused = frozenset(name for values in right_map.values() for name in values)
        names = [(refused, key, "") for key in keys] if refused else []
    if not kept or not names:
        _set_using_mark(child, merged)
        return
    _set_using_mark(
        child, _merge_using_marks(merged, (frozenset(kept), tuple(keys), tuple(names), ids))
    )


def _using_mark_live(mark: Any, held: list[str | None]) -> bool:
    return bool(mark and mark[0] and mark[2]) and any(kept_id in held for kept_id in mark[0])


def _using_hidden_alias(mark: Any, qualifier: str, name: str, exact: bool) -> str | None:
    folded_qual = qualifier if exact else qualifier.lower()
    folded_name = name if exact else name.lower()
    for quals, display, alias in reversed(mark[2]):
        if (display if exact else display.lower()) != folded_name:
            continue
        if any((cand if exact else cand.lower()) == folded_qual for cand in quals):
            return str(alias)
    return None


def _reachable_using_alias(frame: Any, alias: str | None, ref: str) -> str | None:
    if alias is None:
        return None
    if _native.expose_using_keys(frame._plan(), [alias]) is None:
        _raise_using_key_reference(ref)
    return alias


def _using_key_name_column(
    frame: Any, held: list[str | None], qualifier_parts: list[str] | None, name: str
) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column import Column

    mark = _using_mark(frame)
    if mark is None or not qualifier_parts or len(qualifier_parts) != 1:
        return None
    if not _using_mark_live(mark, held):
        return None
    ref = f"{_quote_ident(qualifier_parts[0])}.{_quote_ident(name)}"
    alias = _reachable_using_alias(
        frame, _using_hidden_alias(mark, qualifier_parts[0], name, _session_exact(frame)), ref
    )
    if alias is None:
        return None
    quoted = _quote_ident(alias)
    bound = Column(
        _native.PyColumn.column(quoted).alias(name),
        spark_display=name,
        projection_name=name,
        stable_name=True,
        has_free_attribute=True,
        birth_frame=frame,
    )
    bound._sql_expr = quoted
    return bound


_QUOTED_RUN_RE = re.compile(r'"(?:[^"]|"")+"(?:\."(?:[^"]|"")+")+')
_SINGLE_QUOTED_RE = re.compile(r"('(?:[^']|'')*')")
_DOUBLE_QUOTED_RE = re.compile(r'("(?:[^"]|"")*")')


def _using_key_text_hits(mark: Any, sql: str, exact: bool, generated: bool) -> list[Any]:
    hits: list[Any] = []
    if generated:
        for match in _QUOTED_RUN_RE.finditer(sql.replace("`", '"')):
            parts = [part.replace('""', '"') for part in match.group(0).split('"."')]
            parts[0] = parts[0][1:]
            parts[-1] = parts[-1][:-1]
            if len(parts) == 2:
                alias = _using_hidden_alias(mark, parts[0], parts[1], exact)
                if alias is not None:
                    hits.append((match.start(), match.end(), parts[0], parts[1], alias))
        return hits
    offset = 0
    for piece in _SINGLE_QUOTED_RE.split(sql):
        inner = offset
        offset += len(piece)
        if piece.startswith("'"):
            continue
        for subpiece in _DOUBLE_QUOTED_RE.split(piece):
            base = inner
            inner += len(subpiece)
            if subpiece.startswith('"'):
                continue
            for match in _DOTTED_TOKEN_PATTERN.finditer(subpiece):
                parts = match.group(1).split(".")
                if len(parts) != 2:
                    continue
                alias = _using_hidden_alias(mark, parts[0], parts[1], exact)
                if alias is not None:
                    hits.append(
                        (base + match.start(1), base + match.end(1), parts[0], parts[1], alias)
                    )
    return hits


def _bind_using_key_text(frame: Any, held: list[str | None], sql: str, exact: bool) -> str:
    from repark.spark._idents import quote_ident as _quote_ident

    mark = _using_mark(frame)
    if mark is None or not _using_mark_live(mark, held):
        return sql
    if "`" in sql:
        _refuse_using_key_text(mark, held, sql.replace("`", ""), exact, False)
        return sql
    for start, end, qualifier, name, alias in reversed(
        _using_key_text_hits(mark, sql, exact, False)
    ):
        ref = f"{_quote_ident(qualifier)}.{_quote_ident(name)}"
        _reachable_using_alias(frame, alias, ref)
        sql = f"{sql[:start]}{_quote_ident(alias)}{sql[end:]}"
    return sql


def _refuse_using_key_text(
    mark: Any, held: list[str | None], sql: str, exact: bool, generated: bool
) -> None:
    from repark.spark._idents import quote_ident as _quote_ident

    if mark is None or not _using_mark_live(mark, held):
        return
    for _start, _end, qualifier, name, _alias in _using_key_text_hits(mark, sql, exact, generated):
        _raise_using_key_reference(f"{_quote_ident(qualifier)}.{_quote_ident(name)}")


def _using_key_token_hits(frames: tuple[Any, ...], sql: str) -> list[Any]:
    from repark.spark.dataframe.join_attr_tokens import (
        _ATTR_TOKEN_RE,
        _token_leaf_display,
    )

    if "__REPARK_ATTR_" not in sql:
        return []
    hidden: dict[str, Any] = {}
    for position, frame in enumerate(frames):
        mark = _using_mark(frame)
        if mark is None:
            continue
        for side_id, (alias, display) in mark[3].items():
            hidden[side_id] = (position, alias, display)
    if not hidden:
        return []
    registry = frames[0]._alive_token.get("frame_registry", {})
    hits: list[Any] = []
    for match in _ATTR_TOKEN_RE.finditer(sql):
        found = hidden.get(match.group(1))
        if found is None:
            continue
        birth = registry.get(int(match.group(2)))
        if birth is None or not birth._alive_token.get("alive", False):
            continue
        if any(birth is frame for frame in frames):
            continue
        leaf = _token_leaf_display(match) or match.group(1)
        hits.append((match, leaf, *found))
    return hits


def _refuse_using_key_tokens(frames: tuple[Any, ...], sql: str) -> None:
    from repark.spark._idents import quote_ident as _quote_ident

    for _match, leaf, _position, _alias, _display in _using_key_token_hits(frames, sql):
        _raise_using_key_reference(_quote_ident(leaf))


def _refuse_using_key_columns(frame: Any, columns: list[Any]) -> None:
    mark = _using_mark(frame)
    if mark is None:
        return
    held = _stamped_frame_id_snapshot(frame)[1]
    exact = _session_exact(frame)
    for column in columns:
        sql = column.join_sql_part()
        _refuse_using_key_text(mark, held, sql, exact, True)
        _refuse_using_key_tokens((frame,), sql)


def _bind_using_key_column(frame: Any, column: Any, keep_name: bool) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.dataframe.join_attr_tokens import _ATTR_TOKEN_RE, _token_leaf_display

    mark = _using_mark(frame)
    if mark is None:
        return column
    held = _stamped_frame_id_snapshot(frame)[1]
    if not _using_mark_live(mark, held):
        return column
    exact = _session_exact(frame)
    sql = column.join_sql_part()
    named = _using_key_text_hits(mark, sql, exact, True)
    tokens = _using_key_token_hits((frame,), sql)
    if not named and not tokens:
        return column
    inner = column._inner
    edits = [(start, end, alias) for start, end, _qual, _name, alias in named]
    edits.extend((match.start(), match.end(), alias) for match, _l, _p, alias, _d in tokens)
    for _start, _end, qualifier, name, alias in named:
        ref = f"{_quote_ident(qualifier)}.{_quote_ident(name)}"
        _reachable_using_alias(frame, alias, ref)
        inner = _native.rebind_using_key(inner, qualifier, name, alias, exact, keep_name)
    aliases = {alias for _match, _leaf, _position, alias, _display in tokens}
    if tokens:
        leaf = tokens[0][1]
        display = tokens[0][4]
        hidden_ids = {match.group(1) for match, *_rest in tokens}
        rivals = [
            match
            for match in _ATTR_TOKEN_RE.finditer(sql)
            if match.group(1) not in hidden_ids
            and (_token_leaf_display(match) or "").lower() == display.lower()
        ]
        if len(aliases) != 1 or rivals:
            _raise_using_key_reference(_quote_ident(leaf))
        alias = _reachable_using_alias(frame, next(iter(aliases)), _quote_ident(leaf))
        inner = _native.rebind_using_key(inner, None, display, alias, exact, keep_name)
    rebound = _rewrap_rebound_column(column, inner)
    for start, end, alias in sorted(set(edits), reverse=True):
        sql = f"{sql[:start]}{_quote_ident(alias)}{sql[end:]}"
    rebound._join_sql_expr = sql
    return rebound


def _bound_refs(frame: Any, items: Any, keep_name: bool) -> list[Any]:
    from repark.spark.column import Column

    if _using_mark(frame) is None:
        bound = list(items)
    else:
        bound = [
            _bind_using_key_column(frame, item, keep_name) if isinstance(item, Column) else item
            for item in items
        ]
    held = [item for item in bound if isinstance(item, Column)]
    frame._refuse_self_join_refs(held)
    if frame._unemitted_attr_ids:
        for item in held:
            frame._refuse_unemitted_ids(item)
    return bound


def _cond_sides(self: Any, other: Any, condition: Any) -> tuple[Any, Any, Any]:
    if condition is None:
        return self._plan(), other._plan(), None
    return _expose_using_keys_in_cond(self, other, condition.join_sql_part())


def _cond_args(self: Any, other: Any, sides: Any, left_alias: str, right_alias: str) -> list[Any]:
    from repark.spark.dataframe.join_attr_tokens import _join_condition_args

    args = list(_join_condition_args(self, other, sides[2], left_alias, right_alias))
    args[4], args[5] = sides[0], sides[1]
    args[8] = [*args[8], *_native.logical_column_names(sides[0])[len(args[8]) :]]
    args[9] = [*args[9], *_native.logical_column_names(sides[1])[len(args[9]) :]]
    return args


def _expose_using_keys_in_cond(self: Any, other: Any, cond_sql: str) -> tuple[Any, Any, str]:
    from repark.spark._idents import quote_ident as _quote_ident

    frames = (self, other)
    plans = [self._plan(), other._plan()]
    wanted: list[list[str]] = [[], []]
    edits: list[tuple[int, int, str]] = []
    for position, frame in enumerate(frames):
        mark = _using_mark(frame)
        if mark is None:
            continue
        held = _stamped_frame_id_snapshot(frame)[1]
        if not _using_mark_live(mark, held):
            continue
        wanted[position].extend(
            alias for _quals, _display, alias in mark[2] if alias and alias in cond_sql
        )
        hits = _using_key_text_hits(mark, cond_sql, _session_exact(frame), True)
        for start, end, qualifier, name, alias in hits:
            ref = f"{_quote_ident(qualifier)}.{_quote_ident(name)}"
            _reachable_using_alias(frame, alias, ref)
            wanted[position].append(alias)
            edits.append((start, end, alias))
    for match, leaf, position, alias, _display in _using_key_token_hits(frames, cond_sql):
        _reachable_using_alias(frames[position], alias, _quote_ident(leaf))
        wanted[position].append(alias)
        edits.append((match.start(), match.end(), alias))
    for start, end, alias in sorted(set(edits), reverse=True):
        cond_sql = f"{cond_sql[:start]}{_quote_ident(alias)}{cond_sql[end:]}"
    for position, names in enumerate(wanted):
        if names:
            plans[position] = _native.expose_using_keys(plans[position], list(dict.fromkeys(names)))
    return plans[0], plans[1], cond_sql


def _expose_using_keys_in_text(frame: Any, text: str) -> tuple[Any, dict[str, str]]:
    mark = _using_mark(frame)
    plan = frame._plan()
    if mark is None or "__repark_using__" not in text:
        return plan, {}
    shown = {alias: display for _quals, display, alias in mark[2] if alias and alias in text}
    if not shown:
        return plan, {}
    wide = _native.expose_using_keys(plan, list(shown))
    if wide is None:
        _raise_using_key_reference(", ".join(f"`{display}`" for display in shown.values()))
    return wide, shown


def _cache_lineage(frame: Any) -> Any:
    inner = frame._inner
    mark = _using_mark(frame)
    if mark is None or not mark[2]:
        return inner
    reachable = [
        alias
        for _quals, _display, alias in mark[2]
        if alias and _native.expose_using_keys(inner, [alias]) is not None
    ]
    if not reachable:
        return inner
    return _native.expose_using_keys(inner, list(dict.fromkeys(reachable))) or inner
