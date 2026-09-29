"""Written-name resolution behind the DataFrame name entry points."""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING

from repark import _native
from repark.errors import AnalysisException
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark.column import Column
from repark.spark.dataframe.plan_collapse import (
    _QCOL_TOKEN_RE,
    _replace_local_qcol_token,
    _rewrite_join_qcol_sql,
)

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _bind_qualified_display_column(frame: DataFrame, name: str) -> Column | None:
    """Bind a dotted name on an overlay frame through schema qualifiers.

    Pair each native schema qualifier with its display positionally; one
    hit binds and keeps the written last segment. Hits on different
    attributes refuse natively with Spark's ambiguous text under both
    rules; the same attribute seen twice still binds. A miss returns
    None under IgnoreCase and refuses natively under Exact.
    """
    plan = frame._plan()
    displays = list(frame._display_names or [])
    _written, engine, disposition = _native.resolve_qualified_display_names(plan, displays, [name])[
        0
    ]
    if disposition != "bound":
        return None
    last_segment = name.rsplit(".", 1)[1]
    return Column(
        _native.attribute_column(engine).alias(last_segment),
        spark_display=last_segment,
        projection_name=last_segment,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=_quote_ident(engine),
    )


def _rewrite_join_condition(
    left: DataFrame,
    right: DataFrame,
    condition: Column,
    left_alias: str,
    right_alias: str,
) -> str:
    """Rewrite a condition-join ON text through origins then alias qualifiers.

    Origin tokens bind their side first; surviving alias-qualified
    references rebind through the side schemas by the session rule.
    """
    rewritten = _rewrite_join_qcol_sql(
        condition.join_sql_part(),
        left=left,
        right=right,
        left_alias=left_alias,
        right_alias=right_alias,
    )
    return _native.rewrite_join_condition_aliases(
        left._plan(), right._plan(), rewritten, left_alias, right_alias
    )


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
            return _written_col(name)
        if "." in name and name not in columns:
            bound = _bind_qualified_display_column(frame, name)
            if bound is not None:
                return bound
        if columns.count(name) > 1:
            shared = _shared_origin_column(frame, name, [name])
            if shared is not None:
                return shared
            _native.refuse_ambiguous_display_name(plan, name, columns)
        _native.match_display_names(plan, [name], columns)
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
        if disposition == "missing" and "." in name and frame._display_names is not None:
            bound = _bind_qualified_display_column(frame, name)
            if bound is not None:
                return bound
        if (
            disposition == "missing"
            and "." not in name
            and _native.match_resolver_names(plan, [name], columns)[0][1]
        ):
            _native.refuse_unresolved_name([name], columns)
        if disposition == "missing":
            raise AnalysisException(
                f"A column with name `{name}` cannot be resolved; available columns: {columns}"
            )
        if disposition == "ambiguous":
            shared = _shared_origin_column(frame, name, hits)
            if shared is not None:
                return shared
        if disposition == "ambiguous" and name in columns:
            could_be = ", ".join(f"`{hit}`" for hit in hits)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{name}` is ambiguous, could be: [{could_be}]."
            )
        if disposition == "ambiguous":
            _native.refuse_ambiguous_display_name(plan, name, columns)
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


_AMBIGUOUS_REFERENCE = re.compile(
    r"\[AMBIGUOUS_REFERENCE\] Reference (`(?:[^`]|``)*`(?:\.`(?:[^`]|``)*`)*) is ambiguous"
)


def _sort_like_spark(
    frame: DataFrame, cols: tuple[Column | str, ...], ascending: bool | list[bool] | None
) -> DataFrame:
    keys = cols
    for _attempt in range(len(cols)):
        try:
            columns, directions, nulls_first = frame._sort_specs(keys, ascending)
            return frame._spawn_preserving_identity(
                frame._plan().sort(columns, directions, nulls_first)
            )
        except AnalysisException as error:
            found = _AMBIGUOUS_REFERENCE.search(str(error))
            if found is None:
                raise
            parts = [
                part.replace("``", "`") for part in re.findall(r"`((?:[^`]|``)*)`", found.group(1))
            ]
            disposition, engine = _native.child_sort_target(frame._plan(), parts)
            if disposition == "unresolved":
                _native.refuse_unresolved_name(parts, frame.columns)
            if disposition not in ("bound", "child"):
                raise
            rebound = tuple(_child_sort_key(frame, item, found.group(1), engine) for item in keys)
            if all(key is item for key, item in zip(rebound, keys, strict=True)):
                raise
            keys = rebound
    columns, directions, nulls_first = frame._sort_specs(keys, ascending)
    return frame._spawn_preserving_identity(frame._plan().sort(columns, directions, nulls_first))


def _child_sort_key(
    frame: DataFrame, item: Column | str, reference: str, engine: str
) -> Column | str:
    if isinstance(item, Column):
        name = item._projection_name
        if not item._stable_name or name in (None, "", "*") or item._spark_display != name:
            return item
    else:
        name = item
    try:
        columns, directions, nulls_first = frame._sort_specs((item,), None)
        frame._plan().sort(columns, directions, nulls_first)
    except AnalysisException as error:
        found = _AMBIGUOUS_REFERENCE.search(str(error))
        if found is None or found.group(1) != reference:
            return item
    else:
        return item
    return Column(
        _native.attribute_column(engine),
        spark_display=str(name),
        has_free_attribute=True,
        sql_expr=_quote_ident(engine),
        sort_ascending=item._sort_ascending if isinstance(item, Column) else None,
        sort_nulls_first=item._sort_nulls_first if isinstance(item, Column) else None,
    )


def _twin_engine(column: Column, position: int, count: int) -> tuple[str, tuple[str, str] | None]:
    plan_id, field = column._origin_plan_id, column._origin_field
    if column._spark_display != column._projection_name:
        if plan_id is None or field is None or column._join_sql_expr is not None:
            return f"__repark_sel_a_{position}_{count}", None
        return f"__repark_sel_a_{position}_{count}_{plan_id}_{field}", None
    if plan_id is None or field is None:
        return f"__repark_sel_h2_{position}_{count}", None
    return f"__repark_sel_{plan_id}_{field}_{count}", (plan_id, field)


def _engine_origin(frame: DataFrame, engine: str) -> tuple[str, str] | None:
    for key, held in (frame._origin_map or {}).items():
        spelled = f"__repark_sel_{re.escape(key[0])}_{re.escape(key[1])}_[0-9]+"
        if held == engine or re.fullmatch(spelled, engine):
            return key
    return None


def _shared_origin_column(frame: DataFrame, name: str, hits: list[str]) -> Column | None:
    if frame._display_names is None or frame._engine_names is None or not frame._origin_map:
        return None
    wanted = set(hits)
    pairs = [
        (_engine_origin(frame, engine), engine)
        for display, engine in zip(frame._display_names, frame._engine_names, strict=True)
        if display in wanted
    ]
    origins = {origin for origin, _engine in pairs}
    if len(origins) != 1:
        return None
    key = origins.pop()
    if key is None:
        return None
    engine = pairs[0][1]
    quoted = _quote_ident(engine)
    return Column(
        _native.attribute_column(engine).alias(name),
        spark_display=name,
        projection_name=name,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=quoted,
        origin_plan_id=key[0],
        origin_field=key[1],
    )


def _match_lenient_subset(frame: DataFrame, subset: list[str]) -> list[str]:
    """Match na subset names against bound displays by the session rule.

    Under IgnoreCase every hit matches and misses match nothing; under
    Exact misses refuse natively.
    """
    plan = frame._plan()
    columns = frame.columns
    exact = _native.frame_is_exact(plan)
    matched: list[str] = []
    for name in subset:
        if exact and columns.count(name) > 1:
            hits = [name] * columns.count(name)
        else:
            hits = _native.match_display_names(plan, [name], columns)[0][1]
        if _distinct_attributes(frame, hits):
            _native.refuse_ambiguous_display_name(plan, name, columns)
        matched.extend(hits)
    return matched


def _distinct_attributes(frame: DataFrame, hits: list[str]) -> bool:
    if frame._display_names is None:
        spelled = sorted(set(hits))
        if len(spelled) > 1 and frame._fresh_outputs.intersection(spelled):
            return True
        return len(spelled) > 1 and not _native.same_source_fields(frame._plan(), spelled)
    displays = frame._display_names
    identities = _twin_identities(frame)
    if len(identities) != len(displays):
        return False
    wanted = set(hits)
    held = {
        identity
        for display, identity in zip(displays, identities, strict=True)
        if display in wanted
    }
    return len(held) > 1


def _twin_identities(frame: DataFrame) -> list[str]:
    fresh = [_new_attribute(engine) is not None for engine in frame._engine_names or []]
    return list(_native.twin_identities(frame._plan(), fresh))


def _new_attribute(engine: str) -> tuple[str, str] | None:
    return ("", engine) if _NEW_ATTRIBUTE.fullmatch(engine) else None


_NEW_ATTRIBUTE = re.compile(r"__repark_sel_a_[0-9]+_[0-9]+(?:_([0-9a-f]{12})_(.+))?", re.DOTALL)


def _filter_qcol_sql(frame: DataFrame, join_sql: str) -> str | None:
    if "__REPARK_QCOL_" not in join_sql:
        return None
    origin_map = dict(frame._origin_map or {})
    for engine in frame._engine_names or []:
        found = _NEW_ATTRIBUTE.fullmatch(engine)
        if found is not None and found.group(1) is not None:
            origin_map.setdefault((found.group(1), found.group(2)), engine)
    if not origin_map:
        return None
    replace = functools.partial(_replace_local_qcol_token, origin_map=origin_map, frame=frame)
    return _QCOL_TOKEN_RE.sub(replace, join_sql)


def _predicate_attributes(frame: DataFrame) -> list[str]:
    displays, engines = frame._display_names, frame._engine_names
    if displays is None or engines is None or len(set(displays)) == len(displays):
        return []
    return _twin_identities(frame)


def _fresh_outputs(projected: list[Column], names: list[str]) -> frozenset[str]:
    return frozenset(
        name
        for column, name in zip(projected, names, strict=True)
        if column._spark_display != column._projection_name
    )


def _overlay_drop_targets(frame: DataFrame, item: Column | str, name: str) -> list[str]:
    plan = frame._plan()
    displays = list(frame._display_names or [])
    engines = list(frame._engine_names or [])
    if isinstance(item, Column) and "." in name and name not in displays:
        try:
            _written, engine, disposition = _native.resolve_qualified_display_names(
                plan, displays, [name]
            )[0]
        except AnalysisException as error:
            if "[AMBIGUOUS_REFERENCE]" in str(error):
                raise
            return []
        return [engine] if disposition == "bound" else []
    exact = _native.frame_is_exact(plan)
    if isinstance(item, Column):
        if exact:
            hits = [display for display in displays if display == name]
        else:
            hits = _native.match_display_names(plan, [name], displays)[0][1]
        if item._origin_plan_id is not None and item._origin_field is not None:
            return _origin_drop_targets(frame, set(hits), name)
        if _distinct_attributes(frame, hits):
            _native.refuse_ambiguous_display_name(plan, name, displays)
        wanted = set(hits)
    elif exact:
        wanted = {name}
    else:
        wanted = set(_native.match_resolver_names(plan, [name], displays)[0][1])
    return [engine for display, engine in zip(displays, engines, strict=True) if display in wanted]


def _origin_drop_targets(frame: DataFrame, wanted: set[str], name: str) -> list[str]:
    pairs = zip(frame._display_names or [], frame._engine_names or [], strict=True)
    engines = [
        engine for display, engine in pairs if display in wanted and _new_attribute(engine) is None
    ]
    if len({_engine_origin(frame, engine) for engine in engines} - {None}) > 1:
        _native.refuse_ambiguous_display_name(frame._plan(), name, list(frame._display_names or []))
    return engines


def _match_subset_names(frame: DataFrame, subset: list[str]) -> list[str]:
    """Match dropDuplicates subset names against bound displays by the session rule.

    Return every hit in subset order (fan-out under IgnoreCase, exact hits
    under Exact). Misses refuse with Spark's legacy subset text under both rules.
    """
    plan = frame._plan()
    matched: list[str] = []
    for _written, hits in _native.match_subset_names(plan, subset, frame.columns):
        matched.extend(hits)
    return matched


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
    for key, hits, _disposition in _native.match_resolver_names(plan, keys, displays):
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
        _written, hits, _disposition = _native.match_resolver_names(plan, [existing], displays)[0]
        wanted = set(hits)
        positions = [index for index, display in enumerate(displays) if display in wanted]
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
        _written, hits, _disposition = _native.match_resolver_names(plan, [old_name], names)[0]
        wanted = set(hits)
        names = [new_name if name in wanted else name for name in names]
    return names
