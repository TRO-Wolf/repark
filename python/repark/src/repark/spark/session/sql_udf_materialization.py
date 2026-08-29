"""SQL-UDF expression materialization."""

from __future__ import annotations

import re

from typing import TYPE_CHECKING, Any

from repark.spark._idents import quote_ident as _quote_ident


if TYPE_CHECKING:
    from repark.spark.session.sql_udf_discovery import (
        _sql_find_registry_udf_calls,
        _sql_udf_arg_is_simple,
    )
    from repark.spark.session.sql_udf_parsing import _split_sql_select_list


def _sql_materialize_expr_udfs(
    expr_text: str,
    *,
    registry: dict[str, dict[str, Any]],
    temp_counter: int,
) -> tuple[str | None, list[dict[str, Any]], list[str], int]:
    """Materialize registry UDF calls inside a WHERE/HAVING expression (U10).



    Returns ``(residual_expr, udf_nodes, base_select_parts, new_temp_counter)``.

    ``residual_expr`` is ``None`` when the shape is out of bounds (caller refuses loud).

    """

    calls = _sql_find_registry_udf_calls(expr_text, registry)

    if not calls:
        return expr_text, [], [], temp_counter

    calls_sorted = sorted(calls, key=lambda call: (-call["start"], -call["end"]))

    span_to_out: dict[tuple[int, int], str] = {}

    call_records: list[dict[str, Any]] = []

    base_parts: list[str] = []

    counter = temp_counter

    for call in calls_sorted:
        registered_name = call["registered_name"]

        arg_texts: list[str] = call["args"]

        if not arg_texts:
            return None, [], [], temp_counter

        input_names: list[str] = []

        max_dep_depth = -1

        for arg in arg_texts:
            arg_stripped = arg.strip()

            if not arg_stripped:
                return None, [], [], temp_counter

            nested_out: str | None = None

            for span, out_temp in span_to_out.items():
                nested_call_text = expr_text[span[0] : span[1]]

                if arg_stripped == nested_call_text.strip():
                    nested_out = out_temp

                    for record in call_records:
                        if record["out_name"] == out_temp:
                            max_dep_depth = max(max_dep_depth, record["depth"])

                            break

                    break

            if nested_out is not None:
                input_names.append(nested_out)

                continue

            if not _sql_udf_arg_is_simple(arg_stripped):
                return None, [], [], temp_counter

            temp_name = f"__repark_sql_udf_in_{counter}"

            counter += 1

            base_parts.append(f"{arg_stripped} AS {_quote_ident(temp_name)}")

            input_names.append(temp_name)

        out_temp = f"__repark_sql_udf_out_{counter}"

        counter += 1

        depth = max_dep_depth + 1

        record = {
            "kind": "udf",
            "registered_name": registered_name,
            "input_names": input_names,
            "out_name": out_temp,
            "depth": depth,
            "start": call["start"],
            "end": call["end"],
        }

        call_records.append(record)

        span_to_out[(call["start"], call["end"])] = out_temp

    outermost_calls = [
        call
        for call in calls
        if not any(
            other["start"] < call["start"] and call["end"] < other["end"]
            for other in calls
            if other is not call
        )
    ]

    residual_chars = list(expr_text)

    for call in sorted(outermost_calls, key=lambda item_call: -item_call["start"]):
        span_key = (call["start"], call["end"])

        out_temp = span_to_out[span_key]

        replacement = _quote_ident(out_temp)

        residual_chars[call["start"] : call["end"]] = list(replacement)

    residual_expr = "".join(residual_chars).strip()

    return residual_expr, call_records, base_parts, counter


def _sql_plan_order_by_aliases(
    order_by_sql: str,
    user_out_names: list[str],
) -> list[tuple[str, bool]] | None:
    """Parse ``ORDER BY`` into ``(out_name, ascending)`` when only aliases/ordinals.



    Explicit ``NULLS FIRST`` / ``NULLS LAST`` is refused (returns ``None`` → loud UOE):

    DataFrame.orderBy does not yet wire Column nulls markers end-to-end (U9-C4-001;

    H1 owns dataframe sort). Bare ASC/DESC use Column.asc/desc defaults.



    Returns ``None`` when the ORDER BY shape is out of bounds (caller refuses loud).

    """

    text = order_by_sql.strip()

    if not re.match(r"(?is)^ORDER\s+BY\b", text):
        return None

    items_blob = re.sub(r"(?is)^ORDER\s+BY\s+", "", text, count=1).strip()

    if not items_blob:
        return None

    parts = _split_sql_select_list(items_blob)

    out_lower = {name.lower(): name for name in user_out_names}

    planned: list[tuple[str, bool]] = []

    for part in parts:
        piece = part.strip()

        if not piece:
            return None

        ascending = True

        # Explicit NULLS FIRST/LAST: refuse loud, do not silently ignore.

        if re.search(r"(?is)\bNULLS\s+(FIRST|LAST)\b", piece):
            return None

        direction = re.search(r"(?is)\s+(ASC|DESC)\s*$", piece)

        if direction:
            ascending = direction.group(1).upper() == "ASC"

            piece = piece[: direction.start()].strip()

        # Ordinal 1-based.

        if re.fullmatch(r"\d+", piece):
            ordinal = int(piece)

            if ordinal < 1 or ordinal > len(user_out_names):
                return None

            planned.append((user_out_names[ordinal - 1], ascending))

            continue

        # Bare / quoted alias matching a SELECT out name.

        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", piece):
            canonical = out_lower.get(piece.lower())

            if canonical is None:
                return None

            planned.append((canonical, ascending))

            continue

        if re.fullmatch(r'"([^"]|"")*"', piece):
            name = piece[1:-1].replace('""', '"')

            if name not in user_out_names and name.lower() not in out_lower:
                return None

            planned.append((out_lower.get(name.lower(), name), ascending))

            continue

        if re.fullmatch(r"`([^`]|``)*`", piece):
            name = piece[1:-1].replace("``", "`")

            if name not in user_out_names and name.lower() not in out_lower:
                return None

            planned.append((out_lower.get(name.lower(), name), ascending))

            continue

        return None

    return planned


def _sql_udf_public_error_text(error: BaseException) -> str:
    """Strip internal ``__repark_sql_udf_*`` names from error text (U9 Q13)."""

    text = str(error)

    if "__repark_sql_udf" not in text:
        return text

    return (
        "UDF SELECT rewrite could not complete for this statement shape "
        "(internal materialization columns are not user-visible). "
        "Use SELECT-list UDF forms with ORDER BY on output aliases, or the "
        "DataFrame udf path."
    )


def _sql_udf_clean_exception(error: BaseException) -> BaseException:
    """Map engine errors that leak internal UDF temp names to a loud clean UOE.



    Preserves :class:`~repark.errors.PySparkException` taxonomy (user UDF raises,

    Analysis/Parse, …) so runtime failures are not re-framed as rewrite-shape UOEs

    (U9-C3-001). Only internal-name leaks and unexpected non-PySpark errors are wrapped.

    """

    from repark.errors import PySparkException, UnsupportedOperationException

    text = str(error)

    if "__repark_sql_udf" in text:
        return UnsupportedOperationException(
            "registered Python UDF in SQL could not be applied with the surrounding "
            "statement shape in repark v1 (SELECT-list rewrite materializes UDF outputs "
            "after the engine FROM scan; internal column names are never user-visible). "
            "Use DataFrame.select / withColumn + orderBy, or ORDER BY the UDF output "
            "alias only for supported SELECT-list forms."
        )

    if isinstance(error, UnsupportedOperationException):
        return error

    # Surface user UDF / analysis / parse errors without rewrite framing.

    if isinstance(error, PySparkException):
        return error

    return UnsupportedOperationException(
        "registered Python UDF in SQL could not be rewritten in repark v1 "
        f"({type(error).__name__}: {_sql_udf_public_error_text(error)}). "
        "Use DataFrame F.udf / spark.udf.register + select/withColumn."
    )
