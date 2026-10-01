from __future__ import annotations

from typing import Any, NoReturn

from repark import _native
from repark.errors import AnalysisException
from repark.spark import column_fields


def _hits_folded(frame: Any, written: str, displays: list[str], mode: str) -> list[int]:
    exact = [position for position, display in enumerate(displays) if display == written]
    if frame._session is not None and _native.session_case_sensitive(frame._session):
        return exact
    return exact + _native.java_fold_hits(written, displays, mode)


def _bindings(frame: Any) -> tuple[list[str], list[str], list[str | None]] | None:
    if frame._map_bridge is not None:
        return None
    native: Any = frame._plan()
    held: list[str | None] = list(_native.attribute_ids(native))
    if None in held:
        frame._inner = _native.stamp_attribute_ids(frame._inner)
        native = frame._plan()
        held = list(_native.attribute_ids(native))
    engine_names: list[str] = _native.logical_column_names(native)
    if frame._display_names is not None and frame._engine_names is not None:
        displays = list(frame._display_names)
    else:
        displays = list(engine_names)
    if len(displays) != len(engine_names) or len(held) != len(displays):
        return None
    return (displays, engine_names, held)


def _grouped(hits: list[int], held: list[str | None]) -> tuple[str, list[int]]:
    if not hits:
        return ("missing", [])
    groups: list[str] = []
    for position in hits:
        held_id = held[position]
        if held_id is None:
            raise RuntimeError(f"internal error: stamped position {position} has no attribute id")
        if held_id not in groups:
            groups.append(held_id)
    if len(groups) == 1:
        return ("bound", list(hits))
    return ("ambiguous", list(hits))


def _guard_passes(frame: Any, hits: list[int], engine_names: list[str]) -> bool:
    if len(hits) < 2:
        return True
    native: Any = frame._plan()
    for position in hits:
        if not _native.engine_field_is_unique(native, engine_names[position]):
            return False
    return not _native.join_dup_below_wrappers(native)


def _echo_option(qualifier: str | None, written: str) -> str:
    spelled = written.replace("`", "``")
    if qualifier is None:
        return f"`{spelled}`"
    parts = qualifier.split(".")
    if parts[-1].startswith(("_repark_", "__repark_")):
        return f"`{spelled}`"
    return ".".join(f"`{part}`" for part in (*parts, spelled))


def _raise_ambiguous_reference(frame: Any, written: str, hits: list[int]) -> NoReturn:
    qualifiers: list[str | None] = _native.logical_column_qualifiers(frame._plan())
    options = sorted(_echo_option(qualifiers[position], written) for position in hits)
    raise AnalysisException(
        f"[AMBIGUOUS_REFERENCE] Reference `{written}` is ambiguous, "
        f"could be: [{', '.join(options)}]. SQLSTATE: 42704"
    )


def _refuse_guarded(frame: Any, hits: list[int], engine_names: list[str], written: str) -> None:
    if not _guard_passes(frame, hits, engine_names):
        _raise_ambiguous_reference(frame, written, hits)


def _raise_cannot_resolve(name: str, displays: list[str]) -> NoReturn:
    from repark.spark._integral import attach_error_condition

    error = AnalysisException(f'Cannot resolve column name "{name}" among ({", ".join(displays)}).')
    attach_error_condition(error, "_LEGACY_ERROR_TEMP_1201")
    error._spark_message_parameters = {
        "colName": name,
        "fieldNames": ", ".join(displays),
    }
    raise error


def _close_positions(grouped: list[int], held: list[str | None]) -> list[int]:
    target = held[grouped[0]]
    return [position for position, held_id in enumerate(held) if held_id == target]


def _trim_union_first(frame: Any, grouped: list[int], closed: list[int]) -> list[int]:
    if len(closed) > 1 and _native.union_dup_below_wrappers(frame._plan(), closed):
        return sorted(grouped)[:1]
    return closed


def _bound_subset_positions(
    frame: Any, key: str, bindings: tuple[list[str], list[str], list[str | None]]
) -> list[int]:
    displays, engine_names, held = bindings
    hits = _hits_folded(frame, key, displays, "a")
    if not hits:
        column_fields._raise_unresolved_name(None, key, displays)
    status, grouped = _grouped(hits, held)
    if status == "ambiguous":
        _raise_ambiguous_reference(frame, key, grouped)
    closed = _close_positions(grouped, held)
    if len(closed) > 1 and _native.union_dup_below_wrappers(frame._plan(), closed):
        exact = [position for position in hits if displays[position] == key]
        if len(exact) > 1:
            _raise_ambiguous_reference(frame, key, hits)
        return sorted(grouped)[:1]
    _refuse_guarded(frame, closed, engine_names, key)
    return closed


def _drop_targets(frame: Any, cols: tuple[Any, ...]) -> tuple[list[str], list[str], list[str]]:
    from repark.spark.column import Column

    engine_drop: list[str] = []
    references: list[str] = []
    attributes: list[str] = []
    bindings = _bindings(frame) if cols else None
    overlay = frame._display_names is not None and frame._engine_names is not None
    for item in cols:
        if (
            isinstance(item, Column)
            and item._origin_plan_id is not None
            and item._origin_field is not None
        ):
            if item._origin_plan_id in frame._origin_not_emitted:
                continue
            ambiguous_drop = False
            if bindings is not None and item._attr_id is not None:
                positions = [
                    position
                    for position, held_id in enumerate(bindings[2])
                    if held_id == item._attr_id
                ]
                if positions:
                    target = {bindings[1][position] for position in positions}
                    kept = {
                        engine
                        for index, engine in enumerate(bindings[1])
                        if index not in set(positions)
                    }
                    if _guard_passes(frame, positions, bindings[1]) and target.isdisjoint(kept):
                        attributes.extend(bindings[1][position] for position in positions)
                        continue
                    ambiguous_drop = True
            if frame._origin_map is not None:
                key = (item._origin_plan_id, item._origin_field)
                if key in frame._origin_map:
                    attributes.append(frame._origin_map[key])
                    continue
            if ambiguous_drop:
                references.append(frame._name_of(item))
            continue
        name = frame._name_of(item)
        if bindings is None:
            if overlay:
                pairs = zip(frame._display_names, frame._engine_names, strict=True)
                for display, engine in pairs:
                    if display == name:
                        engine_drop.append(engine)
            else:
                (references if isinstance(item, Column) else engine_drop).append(name)
            continue
        displays, engine_names, held = bindings
        if isinstance(item, str):
            hits = _hits_folded(frame, name, displays, "b")
            if not hits:
                continue
            attributes.extend(engine_names[position] for position in hits)
            continue
        if item._spark_display != name:
            continue
        split = column_fields._split_written_name(name)
        if (split is None or split[0] is not None or "`" in name) and not overlay:
            references.append(name)
            continue
        hits = _hits_folded(frame, name, displays, "a")
        if not hits:
            continue
        status, grouped = _grouped(hits, held)
        if status == "ambiguous":
            _raise_ambiguous_reference(frame, name, grouped)
        closed = _trim_union_first(frame, grouped, _close_positions(grouped, held))
        _refuse_guarded(frame, closed, engine_names, name)
        attributes.extend(engine_names[position] for position in closed)
    return (engine_drop, references, attributes)


def _fanout_subset(frame: Any, names: list[str]) -> list[str]:
    bindings = _bindings(frame)
    if bindings is None:
        resolved: list[str] = []
        if frame._display_names is not None and frame._engine_names is not None:
            want = {frame._name_of(item) for item in names}
            for display, engine in zip(frame._display_names, frame._engine_names, strict=True):
                if display in want:
                    resolved.append(engine)
            if not resolved:
                for item in names:
                    resolved.append(frame._resolve_getitem_column_name(frame._name_of(item)))
        else:
            for item in names:
                held_name = frame._resolve_getitem_column_name(frame._name_of(item)).casefold()
                resolved.extend(name for name in frame.columns if name.casefold() == held_name)
        return resolved
    displays, engine_names, _ = bindings
    if frame._display_names is not None and frame._engine_names is not None:
        hit_positions: set[int] = set()
        for key in names:
            key_hits = _hits_folded(frame, key, displays, "b")
            if not key_hits:
                _raise_cannot_resolve(key, displays)
            hit_positions.update(key_hits)
        return [engine_names[position] for position in sorted(hit_positions)]
    keyed: list[str] = []
    for key in names:
        key_hits = _hits_folded(frame, key, displays, "b")
        if not key_hits:
            _raise_cannot_resolve(key, displays)
        keyed.extend(engine_names[position] for position in key_hits)
    return keyed
