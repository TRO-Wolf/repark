"""Spellings, twin kernels, and door names for ICE-TSNS-COERCION-1 slice 0.

The twin writes ``CAST(… AS timestamp_ns)`` or ``CAST(… AS timestamptz_ns)``
on every operand that is not already the site result type. ``nvl`` and ``ifnull``
twins are ``coalesce``, which is the hand-written form of that rewrite. A column
sibling on a statement that only has the target row in scope is a scalar subquery
against the source table.
"""

from __future__ import annotations

SRC = "ice.ns.src"
ZONES = (
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Australia/Lord_Howe",
    "-03:30",
)
MOMENTS = (
    "2026-01-02 03:04:05.123456789",
    "1969-12-31 23:59:59.999999999",
)
US_MOMENTS = (
    "2026-01-02 03:04:05.123456",
    "1969-12-31 23:59:59.999999",
)
SOURCES = {"ns": "timestamp_ns", "tzns": "timestamptz_ns", "tzns_utc": "timestamptz_ns"}
TARGETS = {"ts_ns": "timestamp_ns", "tz_ns": "timestamptz_ns", "ltz": "TIMESTAMP"}
NANOSECOND_TARGETS = ("ts_ns", "tz_ns")
FORMS = ("coalesce", "array", "case", "if", "nvl", "ifnull", "union")
SIBLINGS = (
    "untyped_null",
    "typed_null_ts",
    "typed_null_ntz",
    "us_column_ts",
    "us_column_ntz",
    "us_literal_ts",
    "us_literal_ntz",
    "date_literal",
)
RANGE_SIBLINGS = ("literal_9999", "literal_0001", "column_far")
COLUMN_SIBLINGS = ("us_column_ts", "us_column_ntz", "column_far")
ZONED_SIBLINGS = frozenset(
    {
        "typed_null_ts",
        "us_column_ts",
        "us_literal_ts",
        "literal_9999",
        "literal_0001",
        "column_far",
    }
)
COALESCE_TWINS = frozenset({"nvl", "ifnull"})
SQL_DOORS = (
    "insert_values",
    "insert_select",
    "insert_positional",
    "insert_by_name",
    "insert_overwrite",
    "insert_overwrite_dynamic",
    "insert_overwrite_static",
    "insert_replace_where",
    "merge_insert",
    "merge_insert_subquery",
    "merge_update",
    "merge_update_subquery",
    "merge_by_source",
    "update_where",
    "update_nowhere",
)
ROW_LEVEL_DOORS = (
    "merge_insert",
    "merge_update",
    "merge_update_subquery",
    "merge_by_source",
    "update_where",
    "update_nowhere",
)
FRAME_DOORS = (
    "df_append",
    "df_overwrite",
    "df_overwrite_partitions",
    "df_create_or_replace",
    "df_save_append",
    "df_save_overwrite",
    "df_insert_into",
    "df_insert_into_overwrite",
    "df_select_expr_append",
    "df_functions_append",
    "df_merge_into",
)
CARRIERS = (
    "branch_insert",
    "branch_merge",
    "branch_update",
    "wap_insert",
    "explain_insert",
    "explain_analyze_insert",
    "explain_analyze_update",
    "prepare_execute_insert",
    "ctas",
    "rtas",
    "cte_insert",
    "subquery_insert",
    "union_insert",
    "join_insert",
    "aggregate_insert",
    "window_insert",
    "distinct_insert",
    "scalar_subquery_insert",
    "frame_view_insert",
    "sql_view_insert",
    "catalog_view_insert",
    "frame_view_merge",
    "cached_frame_append",
)
HUNTED = (
    "merge_update_star",
    "merge_insert_star",
    "update_scalar_subquery",
    "insert_reordered",
    "sort_limit_insert",
    "union_null_insert",
    "df_union_null_append",
    "lambda_first_insert",
    "df_lambda_below_append",
)
DOORS = (
    *SQL_DOORS,
    *(f"{door}_mor" for door in ROW_LEVEL_DOORS),
    *FRAME_DOORS,
    *CARRIERS,
    *HUNTED,
)
PARTITIONED = ("insert_overwrite_dynamic", "insert_overwrite_static")
MOR = (
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)
ROLES = ("statement", "twin")


def result_zoned(source: str, sibling: str) -> bool:
    """Return whether the site result is zoned for this source and sibling."""
    return source in ("tzns", "tzns_utc") or sibling in ZONED_SIBLINGS


def kernel_name(zoned: bool) -> str:
    """Return the SQL type name of the site result type."""
    if zoned:
        return "timestamptz_ns"
    return "timestamp_ns"


def wrap_member(expr: str, kind: str, zoned: bool) -> str:
    """Wrap ``expr`` when it is not already the site result type."""
    if kind == "ns" and not zoned:
        return expr
    if kind == "tzns" and zoned:
        return expr
    return f"CAST({expr} AS {kernel_name(zoned)})"


def moment_literal(source: str, index: int) -> str:
    """Return one source moment as a SQL literal."""
    moment = MOMENTS[index]
    if source == "ns":
        return f"CAST('{moment}' AS timestamp_ns)"
    if source == "tzns":
        return f"CAST('{moment}' AS timestamptz_ns)"
    return f"CAST('{moment}+00:00' AS timestamptz_ns)"


def _column_name(sibling: str) -> str:
    if sibling == "us_column_ts":
        return "ts"
    if sibling == "us_column_ntz":
        return "ntz"
    if sibling == "column_far":
        return "far"
    raise KeyError(sibling)


def sibling_expr(sibling: str, qualifier: str, outer_id: str) -> str:
    """Return the SQL of sibling ``sibling`` in the caller's scope."""
    if sibling == "untyped_null":
        return "NULL"
    if sibling == "typed_null_ts":
        return "CAST(NULL AS TIMESTAMP)"
    if sibling == "typed_null_ntz":
        return "CAST(NULL AS TIMESTAMP_NTZ)"
    if sibling == "us_literal_ts" or sibling == "literal_9999":
        if sibling == "literal_9999":
            return "TIMESTAMP '9999-12-31 00:00:00'"
        return "TIMESTAMP '2026-01-02 03:04:05.123456'"
    if sibling == "literal_0001":
        return "TIMESTAMP '0001-01-01 00:00:00'"
    if sibling == "us_literal_ntz":
        return "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
    if sibling == "date_literal":
        return "DATE '2026-01-02'"
    column = _column_name(sibling)
    if outer_id:
        return f"(SELECT q.{column} FROM {SRC} q WHERE q.id = {outer_id})"
    return f"{qualifier}{column}"


def _arms(
    source: str,
    sibling: str,
    column: str,
    qualifier: str,
    outer_id: str,
    twin: bool,
) -> tuple[str, str]:
    zoned = result_zoned(source, sibling)
    left = column
    right = sibling_expr(sibling, qualifier, outer_id)
    if twin:
        left = wrap_member(left, "ns" if source == "ns" else "tzns", zoned)
        right = wrap_member(right, "other", zoned)
    return left, right


def spelling_expr(
    form: str,
    source: str,
    sibling: str,
    column: str,
    ident: str,
    qualifier: str,
    outer_id: str,
    twin: bool,
) -> str:
    """Return one scalar spelling, or the union scalar carrier."""
    left, right = _arms(source, sibling, column, qualifier, outer_id, twin)
    if form == "union":
        return (
            "(SELECT v FROM ("
            f"SELECT 0 AS ord, {left} AS v UNION ALL SELECT 1 AS ord, {right} AS v"
            ") u ORDER BY ord LIMIT 1)"
        )
    used = "coalesce" if twin and form in COALESCE_TWINS else form
    if used == "coalesce":
        return f"coalesce({left}, {right})"
    if used == "nvl":
        return f"nvl({left}, {right})"
    if used == "ifnull":
        return f"ifnull({left}, {right})"
    if used == "array":
        return f"array({left}, {right})[0]"
    if used == "case":
        return f"CASE WHEN {ident} > 0 THEN {left} ELSE {right} END"
    if used == "if":
        return f"if({ident} > 0, {left}, {right})"
    raise KeyError(form)


def literal_arms(source: str, sibling: str, literal: str, twin: bool) -> tuple[str, str]:
    """Return the source literal and the sibling, wrapped when ``twin`` is set."""
    return _arms(source, sibling, literal, "", "", twin)


def union_body(source: str, sibling: str, twin: bool) -> str:
    """Return the ``UNION ALL`` of the source column and the sibling."""
    left, right = _arms(source, sibling, source, "", "", twin)
    return (
        f"SELECT id, {left} AS v, 0 AS k FROM {SRC} "
        f"UNION ALL SELECT id, {right} AS v, 0 AS k FROM {SRC}"
    )


def value_of(
    role: str,
    form: str,
    source: str,
    sibling: str,
    column: str,
    ident: str,
    qualifier: str = "",
    outer_id: str = "",
) -> str:
    """Return the SQL placed in a scalar slot."""
    if role == "plain":
        return column
    return spelling_expr(
        form,
        source,
        sibling,
        column,
        ident,
        qualifier,
        outer_id,
        role == "twin",
    )
