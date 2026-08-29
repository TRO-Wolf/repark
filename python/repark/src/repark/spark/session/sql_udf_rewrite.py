"""SQL-UDF select-list rewrite."""

from __future__ import annotations

import re

from typing import TYPE_CHECKING, Any

from repark.spark._idents import quote_ident as _quote_ident


if TYPE_CHECKING:
    from repark.spark.session.sql_udf_discovery import (
        _sql_find_registry_udf_calls,
        _sql_peel_select_trailing_clauses,
        _sql_residual_has_subquery,
        _sql_udf_arg_is_simple,
        _sql_udf_call_match_key,
    )
    from repark.spark.session.sql_udf_materialization import (
        _sql_materialize_expr_udfs,
        _sql_plan_order_by_aliases,
    )
    from repark.spark.session.sql_udf_parsing import (
        _split_sql_select_list,
        _sql_strip_comments_preserve_strings,
        _sql_top_level_keyword_index,
    )
    from repark.spark.session.sql_udf_residual import _sql_where_residual_base_projections


def _try_rewrite_select_list_python_udfs(
    body: str,
    *,
    registry: dict[str, dict[str, Any]],
    hits: list[tuple[str, str, int]],
) -> tuple[str, dict[str, Any]] | None:
    """Rewrite SELECT-list (+ WHERE/GROUP BY/HAVING) UDF calls (U9/U10).



    Returns ``(base_sql, materialize_plan)`` or ``None`` when the shape is out of bounds.

    ``materialize_plan`` keys: ``stages``, ``final_exprs``, ``distinct``, ``order_by``,

    ``limit``, ``where_sql``, ``group_by_keys``, ``having_sql``, ``user_out_names``.

    """

    _ = hits

    stripped = body.strip()

    if re.match(r"(?is)^WITH\b", stripped):
        return None

    select_match = re.match(r"(?is)^SELECT\s+(DISTINCT\s+)?", stripped)

    if not select_match:
        return None

    is_distinct = bool(select_match.group(1))

    select_list_start = select_match.end()

    from_index = _sql_top_level_keyword_index(stripped, "FROM")

    if from_index is None:
        # No FROM — Spark allows ``SELECT expr`` (U9-C7-001). Select list runs to
        # trailing ORDER BY / LIMIT / GROUP / HAVING (peeled below).
        select_list = stripped[select_list_start:].strip()

        rest = ""

        for keyword in ("WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"):
            kw_index = _sql_top_level_keyword_index(select_list, keyword)

            if kw_index is not None:
                rest = select_list[kw_index:]

                select_list = select_list[:kw_index].strip()

                break

    else:
        select_list = stripped[select_list_start:from_index].strip()

        rest = stripped[from_index:]

    items = _split_sql_select_list(select_list)

    if not items:
        return None

    # Peel WHERE / GROUP BY / HAVING / ORDER BY / LIMIT from rest so base SQL never

    # references user aliases that only exist after UDF materialization (U9 Q13;

    # WHERE peels when it holds UDF residuals).

    if rest:
        core_rest, peeled = _sql_peel_select_trailing_clauses(rest)

    else:
        core_rest, peeled = (
            "",
            {
                "where": None,
                "group_by": None,
                "having": None,
                "order_by": None,
                "limit": None,
            },
        )

    # Aggregates + GROUP BY are out of bounds for the keys-only path.

    if peeled.get("group_by") or peeled.get("having"):
        agg_pattern = re.compile(
            r"(?is)\b(count|sum|avg|mean|min|max|first|last|collect_list|"
            r"collect_set|percentile|stddev|variance|var_pop|var_samp|"
            r"skewness|kurtosis|approx_count_distinct)\s*\("
        )

        if agg_pattern.search(select_list):
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF with GROUP BY / HAVING and aggregate SELECT "
                "expressions is not supported in repark v1 (keys-only GROUP BY after "
                "UDF materialization). Materialize via DataFrame.select / withColumn, "
                "then groupBy / agg."
            )

    base_select_parts: list[str] = []

    temp_counter = 0

    any_udf = False

    # UDF calls ordered innermost-first with dependency depths for staging.

    udf_nodes: list[dict[str, Any]] = []  # each: registered_name, input_names, out_name, depth

    final_exprs: list[str] = []

    user_out_names: list[str] = []

    for item in items:
        item_clean = _sql_strip_comments_preserve_strings(item).strip()

        if not item_clean:
            return None

        # Optional trailing AS alias for the whole item (and Spark optional-AS form

        # ``expr alias`` without the AS keyword — U9-C5-001).

        alias: str | None = None

        expr_text = item_clean

        as_match = re.search(
            r"(?i)\s+AS\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$",
            item_clean,
        )

        if as_match:
            alias_raw = as_match.group(1)

            if alias_raw.startswith('"') and alias_raw.endswith('"'):
                alias = alias_raw[1:-1].replace('""', '"')

            elif alias_raw.startswith("`") and alias_raw.endswith("`"):
                alias = alias_raw[1:-1].replace("``", "`")

            else:
                alias = alias_raw

            expr_text = item_clean[: as_match.start()].strip()

        else:
            # Spark allows ``SELECT expr alias`` without AS. Only peel when the

            # trailing token is a bare/quoted ident and the left side does not end

            # with an operator (so ``udf(x) + 1`` is not misread).

            opt_alias = re.search(
                r"\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$",
                item_clean,
            )

            if opt_alias:
                maybe_expr = item_clean[: opt_alias.start()].strip()

                alias_raw = opt_alias.group(1)

                if maybe_expr and not re.search(
                    r"[,+\-*/%&|<>=!.]$",
                    maybe_expr.rstrip(),
                ):
                    # Reject reserved trailing tokens that are not aliases.

                    reserved = {
                        "from",
                        "where",
                        "group",
                        "having",
                        "order",
                        "limit",
                        "union",
                        "intersect",
                        "except",
                        "and",
                        "or",
                        "as",
                    }

                    bare_for_check = alias_raw

                    if (bare_for_check.startswith('"') and bare_for_check.endswith('"')) or (
                        bare_for_check.startswith("`") and bare_for_check.endswith("`")
                    ):
                        bare_for_check = bare_for_check[1:-1]

                    if bare_for_check.lower() not in reserved:
                        if alias_raw.startswith('"') and alias_raw.endswith('"'):
                            alias = alias_raw[1:-1].replace('""', '"')

                        elif alias_raw.startswith("`") and alias_raw.endswith("`"):
                            alias = alias_raw[1:-1].replace("``", "`")

                        else:
                            alias = alias_raw

                        expr_text = maybe_expr

        # Star expansion cannot be aliased into a hidden base column (U9-C1-003).

        if expr_text == "*" or re.fullmatch(
            r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*\*',
            expr_text,
        ):
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF SELECT with star (*) expansion is not supported "
                "in repark v1 (SELECT-list rewrite cannot project '*' into a hidden "
                "base column). List columns explicitly or apply the UDF via DataFrame."
            )

        calls = _sql_find_registry_udf_calls(expr_text, registry)

        if not calls:
            # Pure pass-through (no UDF).

            if alias is not None:
                out_name = alias

                pass_expr = expr_text

            else:
                bare = expr_text

                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", bare):
                    out_name = bare

                    pass_expr = bare

                elif re.fullmatch(r'"([^"]|"")*"', bare):
                    out_name = bare[1:-1].replace('""', '"')

                    pass_expr = bare

                elif re.fullmatch(r"`([^`]|``)*`", bare):
                    out_name = bare[1:-1].replace("``", "`")

                    pass_expr = bare

                else:
                    out_name = bare

                    pass_expr = bare

            # Defense: never let pass-through out names surface internal temps.

            if "__repark_sql_udf" in out_name:
                out_name = f"col{temp_counter}"

            temp_name = f"__repark_sql_udf_pass_{temp_counter}"

            temp_counter += 1

            base_select_parts.append(f"{pass_expr} AS {_quote_ident(temp_name)}")

            final_exprs.append(f"{_quote_ident(temp_name)} AS {_quote_ident(out_name)}")

            user_out_names.append(out_name)

            continue

        any_udf = True

        # Process calls innermost-first (larger start index first among nested).

        calls_sorted = sorted(calls, key=lambda call: (-call["start"], -call["end"]))

        # span → out temp name for this item's UDF calls

        span_to_out: dict[tuple[int, int], str] = {}

        call_records: list[dict[str, Any]] = []

        for call in calls_sorted:
            registered_name = call["registered_name"]

            arg_texts: list[str] = call["args"]

            if not arg_texts:
                return None  # zero-arg SQL UDF unsupported (same as DF path)

            input_names: list[str] = []

            max_dep_depth = -1

            for arg in arg_texts:
                arg_stripped = arg.strip()

                if not arg_stripped:
                    return None

                # If the entire arg is a nested UDF call we already assigned, use that out.

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

                # Simple arg → project from base.

                if not _sql_udf_arg_is_simple(arg_stripped):
                    return None

                temp_name = f"__repark_sql_udf_in_{temp_counter}"

                temp_counter += 1

                base_select_parts.append(f"{arg_stripped} AS {_quote_ident(temp_name)}")

                input_names.append(temp_name)

            out_temp = f"__repark_sql_udf_out_{temp_counter}"

            temp_counter += 1

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

            udf_nodes.append(record)

        # Build residual expression: replace only **outermost** UDF call spans with

        # their out temps (nested calls are already baked into the outer stage).

        # Replacing nested spans first would shift indices and break outer spans.

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

        # User-visible output name (U9-C1-001 / Q13: never surface __repark_sql_udf_*).

        if alias is not None:
            out_name = alias

        else:
            # When residual is solely one outermost UDF out temp → Spark-style function name

            # (covers simple ``udf(x)`` and nested ``f(g(x))`` without AS).

            single_outer_name: str | None = None

            if len(outermost_calls) == 1:
                only = outermost_calls[0]

                out_temp = span_to_out[(only["start"], only["end"])]

                if residual_expr == _quote_ident(out_temp):
                    single_outer_name = only["registered_name"]

            # Expression wrap without AS: original user SQL fragment as display name

            # (residual_expr keeps temps for selectExpr evaluation).

            out_name = single_outer_name if single_outer_name is not None else expr_text

            # Hard guard — never emit internal materialization names to the user schema.

            if "__repark_sql_udf" in out_name:
                out_name = (
                    outermost_calls[0]["registered_name"]
                    if outermost_calls
                    else f"col{temp_counter}"
                )

        final_exprs.append(f"{residual_expr} AS {_quote_ident(out_name)}")

        user_out_names.append(out_name)

    # Materialize UDF calls in WHERE / GROUP BY / HAVING residuals.

    where_sql: str | None = None

    having_sql: str | None = None

    group_by_keys: list[str] | None = None

    # Map UDF call text in SELECT residual → user out name so GROUP BY my_udf(v)

    # binds to the SELECT alias. Keys are case+whitespace normalized (U10 C2).

    select_udf_call_to_out: dict[str, str] = {}

    for item_index, item in enumerate(items):
        item_clean = _sql_strip_comments_preserve_strings(item).strip()

        calls = _sql_find_registry_udf_calls(item_clean, registry)

        if not calls:
            continue

        # Use outermost call text → corresponding user out name when residual is pure UDF.

        outermost = [
            call
            for call in calls
            if not any(
                other["start"] < call["start"] and call["end"] < other["end"]
                for other in calls
                if other is not call
            )
        ]

        if len(outermost) == 1 and item_index < len(user_out_names):
            call_text = item_clean[outermost[0]["start"] : outermost[0]["end"]].strip()

            # Also strip trailing AS alias from item for keying pure call.

            as_match = re.search(
                r"(?i)\s+AS\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$",
                item_clean,
            )

            pure = item_clean[: as_match.start()].strip() if as_match else item_clean

            out_alias = user_out_names[item_index]

            select_udf_call_to_out[_sql_udf_call_match_key(call_text)] = out_alias

            select_udf_call_to_out[_sql_udf_call_match_key(pure)] = out_alias

    if peeled.get("where"):
        where_fragment = peeled["where"] or ""

        where_body = re.sub(r"(?is)^\s*WHERE\s+", "", where_fragment, count=1).strip()

        where_calls = _sql_find_registry_udf_calls(where_body, registry)

        if where_calls:
            residual, new_nodes, new_parts, temp_counter = _sql_materialize_expr_udfs(
                where_body,
                registry=registry,
                temp_counter=temp_counter,
            )

            if residual is None:
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF in WHERE could not be rewritten in repark v1 "
                    "(simple col/lit UDF args only). Use DataFrame.filter after withColumn."
                )

            any_udf = True

            udf_nodes.extend(new_nodes)

            base_select_parts.extend(new_parts)

            # Compound WHERE may still reference base columns outside UDF calls

            # (``my_double(a) > 2 AND s = 'z'``). Residual filter runs on the

            # materialization frame (temp names only) — identity-project residual

            # base idents so the residual resolves without leaking temps (U10 C1).

            residual, residual_base_parts, temp_counter = _sql_where_residual_base_projections(
                residual,
                base_select_parts=base_select_parts,
                temp_counter=temp_counter,
            )

            base_select_parts.extend(residual_base_parts)

            # Subqueries in residual cannot run on the DataFrame.filter SQL path

            # (U10 C3 — was engine ParseException; refuse-loud with accurate shape).

            if _sql_residual_has_subquery(residual):
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF in WHERE with a nested subquery / EXISTS is "
                    "not supported in repark v1 (residual filter cannot host SELECT "
                    "subqueries after UDF materialization). Flatten the predicate or use "
                    "DataFrame.filter after withColumn."
                )

            where_sql = residual

        else:
            # Non-UDF WHERE stays in engine base SQL.

            core_rest = (core_rest + " " + where_fragment).strip() if core_rest else where_fragment

    if peeled.get("group_by"):
        group_fragment = peeled["group_by"] or ""

        group_body = re.sub(r"(?is)^\s*GROUP\s+BY\s+", "", group_fragment, count=1).strip()

        if not group_body:
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF with empty GROUP BY is not supported in repark v1"
            )

        planned_keys: list[str] = []

        for part in _split_sql_select_list(group_body):
            piece = part.strip()

            if not piece:
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF GROUP BY has an empty key in repark v1"
                )

            # Alias / ordinal of SELECT outputs.

            out_lower = {name.lower(): name for name in user_out_names}

            if re.fullmatch(r"\d+", piece):
                ordinal = int(piece)

                if ordinal < 1 or ordinal > len(user_out_names):
                    from repark.errors import UnsupportedOperationException

                    raise UnsupportedOperationException(
                        "registered Python UDF GROUP BY ordinal is out of range in repark v1"
                    )

                planned_keys.append(user_out_names[ordinal - 1])

                continue

            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", piece):
                canonical = out_lower.get(piece.lower())

                if canonical is not None:
                    planned_keys.append(canonical)

                    continue

                # Bare base column not in SELECT outs — refuse (would need pass-through).

                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    f"registered Python UDF GROUP BY key {piece!r} must be a SELECT-list "
                    "output alias (or matching UDF expression) in repark v1. Materialize "
                    "via DataFrame then groupBy."
                )

            if re.fullmatch(r'"([^"]|"")*"', piece) or re.fullmatch(r"`([^`]|``)*`", piece):
                name = piece[1:-1].replace('""', '"').replace("``", "`")

                canonical = out_lower.get(name.lower(), name if name in user_out_names else None)

                if canonical is None:
                    from repark.errors import UnsupportedOperationException

                    raise UnsupportedOperationException(
                        f"registered Python UDF GROUP BY key {piece!r} must be a "
                        "SELECT-list output alias in repark v1"
                    )

                planned_keys.append(canonical)

                continue

            # UDF expression matching a SELECT UDF call.

            piece_calls = _sql_find_registry_udf_calls(piece, registry)

            if piece_calls:
                # Prefer mapping to SELECT out when the call text matches (ws/case).

                mapped = select_udf_call_to_out.get(_sql_udf_call_match_key(piece))

                if mapped is not None:
                    planned_keys.append(mapped)

                    continue

                # Materialize a standalone GROUP BY UDF and use its temp as key after

                # folding into final projection under a stable name — refuse for v1 if

                # not already a SELECT out (keep scope tight).

                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF in GROUP BY must match a SELECT-list UDF "
                    "output (same expression or its alias) in repark v1. Project the UDF "
                    "in the SELECT list and GROUP BY the alias."
                )

            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                f"registered Python UDF GROUP BY key shape {piece!r} is not supported "
                "in repark v1 (SELECT aliases, ordinals, or matching UDF expressions only)."
            )

        # Every SELECT out must be a group key (keys-only, no aggregates).

        key_set = {key.lower() for key in planned_keys}

        for out_name in user_out_names:
            if out_name.lower() not in key_set:
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    f"registered Python UDF SELECT output {out_name!r} is not a GROUP BY "
                    "key and aggregates are not supported in the UDF rewrite path (repark "
                    "v1 keys-only). Include it in GROUP BY or use DataFrame.groupBy.agg."
                )

        group_by_keys = planned_keys

        any_udf = True  # force rewrite path even if SELECT had no UDF (GB-only is rare)

    if peeled.get("having"):
        from repark.errors import UnsupportedOperationException

        if group_by_keys is None:
            raise UnsupportedOperationException(
                "registered Python UDF HAVING without GROUP BY is not supported in repark v1"
            )

        having_fragment = peeled["having"] or ""

        having_body = re.sub(r"(?is)^\s*HAVING\s+", "", having_fragment, count=1).strip()

        # Keys-only path: refuse aggregate HAVING (count/sum/…) before engine plan

        # garbage (U10 C1).

        having_agg_pattern = re.compile(
            r"(?is)\b(count|sum|avg|mean|min|max|first|last|collect_list|"
            r"collect_set|percentile|stddev|variance|var_pop|var_samp|"
            r"skewness|kurtosis|approx_count_distinct)\s*\("
        )

        if having_agg_pattern.search(having_body):
            raise UnsupportedOperationException(
                "registered Python UDF with aggregate HAVING is not supported in repark v1 "
                "(keys-only GROUP BY after UDF materialization; HAVING may filter on "
                "SELECT-list keys / matching UDF expressions only). Use DataFrame.groupBy.agg."
            )

        having_calls = _sql_find_registry_udf_calls(having_body, registry)

        if not having_calls:
            # Non-UDF HAVING: post-group filter on user-visible names / keys.

            having_sql = having_body

        else:
            # Map each HAVING UDF call span to a SELECT-list output alias.

            residual_chars = list(having_body)

            for call in sorted(having_calls, key=lambda item_call: -item_call["start"]):
                call_text = having_body[call["start"] : call["end"]].strip()

                out_name = select_udf_call_to_out.get(_sql_udf_call_match_key(call_text))

                if out_name is None:
                    raise UnsupportedOperationException(
                        "registered Python UDF in HAVING must match a SELECT-list UDF "
                        "output (same expression or its alias) in repark v1. Project the "
                        "UDF in the SELECT list and filter on the alias."
                    )

                replacement = _quote_ident(out_name)

                residual_chars[call["start"] : call["end"]] = list(replacement)

            having_sql = "".join(residual_chars).strip()

            if "__repark_sql_udf" in having_sql:
                raise UnsupportedOperationException(
                    "registered Python UDF in HAVING produced an internal name leak "
                    "guard in repark v1; use SELECT-list aliases only."
                )

        any_udf = True

    if not any_udf:
        return None

    if not base_select_parts:
        return None

    # Stage UDF nodes by depth (independent same-depth calls share a stage).

    if not udf_nodes:
        return None

    max_depth = max(node["depth"] for node in udf_nodes)

    stages: list[list[dict[str, Any]]] = []

    for depth in range(max_depth + 1):
        stage = [
            {
                "kind": "udf",
                "registered_name": node["registered_name"],
                "input_names": node["input_names"],
                "out_name": node["out_name"],
            }
            for node in udf_nodes
            if node["depth"] == depth
        ]

        if stage:
            stages.append(stage)

    # ORDER BY: only simple aliases / ordinals of the SELECT list (post-materialization).

    order_by: list[tuple[str, bool]] | None = None

    if peeled.get("order_by"):
        order_by = _sql_plan_order_by_aliases(peeled["order_by"], user_out_names)

        if order_by is None:
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF SELECT with this ORDER BY shape is not supported "
                "in repark v1 (order by SELECT-list output aliases or 1-based ordinals only "
                "after UDF materialization). Use DataFrame.orderBy after select."
            )

    limit_n: int | None = None

    if peeled.get("limit"):
        limit_match = re.match(r"(?is)^\s*LIMIT\s+(\d+)\s*$", peeled["limit"].strip())

        if limit_match is None:
            from repark.errors import UnsupportedOperationException

            # OFFSET / FETCH / non-integer LIMIT are all out of bounds for the peel path.

            raise UnsupportedOperationException(
                "registered Python UDF SELECT with this LIMIT shape is not supported in "
                "repark v1 (integer LIMIT n only after UDF materialization; LIMIT/OFFSET "
                "and non-integer LIMIT are refused). Use DataFrame.limit after select."
            )

        limit_n = int(limit_match.group(1))

    if core_rest:
        base_sql = "SELECT " + ", ".join(base_select_parts) + " " + core_rest

    else:
        # No-FROM SELECT (U9-C7-001): project UDF inputs/literals only.

        base_sql = "SELECT " + ", ".join(base_select_parts)

    materialize_plan: dict[str, Any] = {
        "stages": stages,
        "final_exprs": final_exprs,
        "distinct": is_distinct,
        "order_by": order_by,
        "limit": limit_n,
        "where_sql": where_sql,
        "group_by_keys": group_by_keys,
        "having_sql": having_sql,
        "user_out_names": user_out_names,
    }

    return base_sql, materialize_plan
