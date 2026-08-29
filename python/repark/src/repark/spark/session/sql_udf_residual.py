"""SQL-UDF residual projection planning."""

from __future__ import annotations

import re

from typing import TYPE_CHECKING

from repark.spark._idents import quote_ident as _quote_ident


if TYPE_CHECKING:
    from repark.spark.session.sql_relations import _sql_mask_strings_and_comments


def _sql_where_residual_base_projections(
    residual: str,
    *,
    base_select_parts: list[str],
    temp_counter: int,
) -> tuple[str, list[str], int]:
    """Identity-project residual base columns needed by compound WHERE.

    After UDF call spans are replaced with ``__repark_sql_udf_out_*`` temps, residual
    predicates may still reference table columns (``AND a < 10``, ``AND s = 'z'``).
    Those names are not on the materialization frame unless projected. Returns
    ``(residual, new_base_parts, temp_counter)`` — bare idents project as ``col AS col``;
    qualified ``t.col`` projects under a stable temp and the residual span is rewritten so
    the filter resolves (alias ``t.col`` is not a valid multi-part field name on the
    post-scan frame).

    Syntax keywords (``FROM`` / ``BOTH`` / ``FOR`` / …) are never identity-projected so
    engine forms like ``IS [NOT] DISTINCT FROM``, ``trim(BOTH … FROM …)``,
    ``substring(… FROM … FOR …)``, ``extract(YEAR FROM …)`` stay intact. SQL type tokens
    are only skipped after ``AS`` (CAST) so legitimate columns named ``date`` / ``double``
    / ``string`` still project. ``END`` is a CASE terminator only when nested under an
    unmatched ``CASE`` (column ``end`` still projects). Ambiguous names are quoted on both
    the base projection and residual.

    Residual poles:

    * ``INTERVAL '1' DAY`` — unit tokens after an INTERVAL literal must not be
      identity-projected / quote-rewritten (would break unit syntax). Multi-unit
      ``DAY TO SECOND`` trailing units after ``TO`` are also syntax.
    * Typed literals ``DATE '…'`` / ``TIMESTAMP '…'`` / ``TIME '…'`` — constructor
      keywords are syntax when followed by a string literal.
    * Columns named ``and`` / ``or`` / ``not`` — ``DataFrame.filter``'s SQL-string
      identifier rewriter case-steals boolean keywords when those columns sit on the
      materialization frame; project them under ``__repark_sql_udf_wcol_*`` temps and
      rewrite residual spans (never leak temps to the user projection).
    """

    # Pure syntax / boolean / clause keywords — never bare column projections.

    # Type tokens (date/double/string/…) are intentionally absent: they skip only after

    # AS (CAST). Bare ``from``/``both``/``for`` cannot be unquoted columns in Spark SQL.

    reserved = {
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "true",
        "false",
        "like",
        "ilike",
        "rlike",
        "regexp",
        "between",
        "case",
        "when",
        "then",
        "else",
        # "end" — CASE nesting heuristic below (legal column name).
        "as",
        "distinct",
        "cast",
        "try_cast",
        "exists",
        "any",
        "all",
        "some",
        "escape",
        "div",
        "mod",
        "over",
        "filter",
        "within",
        "group",
        "order",
        "by",
        "asc",
        "desc",
        "interval",
        "current_date",
        "current_timestamp",
        "current_time",
        "localtimestamp",
        "localtime",
        # Multi-word SQL syntax.
        "from",
        "both",
        "for",
        "leading",
        "trailing",
        "similar",
        "to",
        "placing",
        "using",
        "symmetric",
        "asymmetric",
        "only",
        "nulls",
        "first",
        "last",
        "unknown",
        "extract",
        "trim",
        "substring",
        "position",
        "overlay",
        "zone",
        "at",
        "time",
        "with",
        "without",
        "select",
        "where",
        "having",
        "limit",
        "offset",
        "join",
        "on",
        "left",
        "right",
        "inner",
        "outer",
        "cross",
        "full",
        "natural",
        "union",
        "except",
        "intersect",
        "lateral",
        "recursive",
        "values",
        "window",
        "qualify",
        "cube",
        "rollup",
        "sets",
        "partition",
        "range",
        "rows",
        "unbounded",
        "preceding",
        "following",
        "current",
        "row",
    }

    # extract(YEAR FROM …) / date_part field names — syntax when followed by FROM.

    extract_fields = {
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
        "nanosecond",
        "week",
        "quarter",
        "dow",
        "doy",
        "epoch",
        "date",
        "time",
    }

    # INTERVAL <lit|n> <unit> — unit is syntax, not a column.

    interval_units = {
        "year",
        "years",
        "month",
        "months",
        "week",
        "weeks",
        "day",
        "days",
        "hour",
        "hours",
        "minute",
        "minutes",
        "second",
        "seconds",
        "millisecond",
        "milliseconds",
        "microsecond",
        "microseconds",
        "nanosecond",
        "nanoseconds",
    }

    # DataFrame.filter SQL-string rewriter case-steals these boolean keywords when a
    # same-named column is on the frame (``a > 0 AND "and" = 5`` → ParseException).
    # Always project under a temp and rewrite residual.

    filter_boolean_steal_names = {"and", "or", "not"}

    # Legal column names that collide with CAST types / CASE END — quote on project + residual.

    quote_project_names = {
        "bigint",
        "long",
        "int",
        "integer",
        "smallint",
        "tinyint",
        "byte",
        "short",
        "double",
        "float",
        "real",
        "boolean",
        "bool",
        "string",
        "varchar",
        "char",
        "binary",
        "date",
        "timestamp",
        "timestamp_ntz",
        "decimal",
        "numeric",
        "void",
        "end",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "week",
        "quarter",
    }

    # Aliases already present on the base projection (quoted or bare AS tail).

    existing_aliases: set[str] = set()

    for part in base_select_parts:
        as_match = re.search(
            r'(?i)\s+AS\s+("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)\s*$',
            part.strip(),
        )

        if as_match is None:
            continue

        raw = as_match.group(1)

        if raw.startswith('"') and raw.endswith('"'):
            existing_aliases.add(raw[1:-1].replace('""', '"').lower())

        elif raw.startswith("`") and raw.endswith("`"):
            existing_aliases.add(raw[1:-1].replace("``", "`").lower())

        else:
            existing_aliases.add(raw.lower())

    masked = _sql_mask_strings_and_comments(residual)

    new_parts: list[str] = []

    residual_chars = list(residual)

    counter = temp_counter

    # Qualified table.col — rewrite residual to a single-part temp (right-to-left).

    qual_matches = list(
        re.finditer(
            r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
            masked,
        )
    )

    qual_alias_by_span: dict[tuple[int, int], str] = {}

    for match in qual_matches:
        if re.match(r"\s*\(", masked[match.end() :]):
            continue

        table_name = match.group(1)

        column_name = match.group(2)

        if table_name.lower() in reserved:
            continue

        # Column side may be a type-token name (t.date) — still project; only skip

        # pure syntax keywords that cannot be unquoted columns.

        if column_name.lower() in reserved:
            continue

        expr = f"{table_name}.{column_name}"

        # Reuse bare column alias when free and already projected.

        if column_name.lower() in existing_aliases:
            alias = column_name

        else:
            alias = f"__repark_sql_udf_wcol_{counter}"

            counter += 1

            new_parts.append(f"{expr} AS {_quote_ident(alias)}")

            existing_aliases.add(alias.lower())

        qual_alias_by_span[(match.start(), match.end())] = alias

    for (start, end), alias in sorted(qual_alias_by_span.items(), key=lambda item: -item[0][0]):
        residual_chars[start:end] = list(_quote_ident(alias))

    residual_after_qual = "".join(residual_chars)

    masked = _sql_mask_strings_and_comments(residual_after_qual)

    # Bare identifiers (not function names, not internal temps, not syntax keywords).
    # Type tokens after AS and extract fields before FROM are context-skipped.

    seen_bare: set[str] = set()

    # Spans of bare idents rewritten to quoted form so filter SQL accepts keyword-ish names.

    # Third element is the replacement text (quoted ident or temp alias).

    quote_residual_spans: list[tuple[int, int, str]] = []

    # filter-boolean-steal names already assigned a temp on this residual.

    steal_temp_by_key: dict[str, str] = {}

    for match in re.finditer(r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)", masked):
        ident = match.group(1)

        if ident.startswith("__repark_sql_udf"):
            continue

        if ident.lower() in reserved:
            continue

        # Skip CAST/TRY_CAST type names even when not in the static reserved set

        # (``AS decimal(10,2)`` leaves ``decimal``; also bare ``AS customtype``).

        prefix = masked[: match.start()]

        if re.search(r"(?is)\bAS\s+$", prefix):
            continue

        # extract(YEAR FROM x) / date_part — field then FROM is syntax, not a column.

        key = ident.lower()

        if key in extract_fields and re.match(r"(?is)\s+FROM\b", masked[match.end() :]):
            continue

        if re.search(r"(?is)\b(EXTRACT|DATE_PART|DATEPART)\s*\(\s*$", prefix):
            continue

        # INTERVAL '1' DAY / INTERVAL 1 DAY — unit token is syntax.
        # String literals are length-masked to spaces by ``_sql_mask_strings_and_comments``,
        # so ``INTERVAL '1'`` becomes ``INTERVAL     `` (no quotes remain on the masked prefix).
        # Multi-unit qualifiers ``DAY TO SECOND`` / ``YEAR TO MONTH``: the trailing unit sits
        # after ``TO``, not immediately after INTERVAL — still syntax, never a column.

        if key in interval_units and (
            re.search(r"(?is)\bINTERVAL\s+(?:\d+\s+)?$", prefix)
            or re.search(r"(?is)\bINTERVAL\b[\s\S]*\bTO\s+$", prefix)
        ):
            continue

        # Typed SQL literals ``DATE '…'`` / ``TIMESTAMP '…'`` / ``TIME '…'`` — constructor
        # keyword is syntax, not a column. Mask turns the string into spaces, so detect the
        # quote on the unmasked residual (length-preserving mask).

        if key in {"date", "timestamp", "timestamp_ntz", "time"} and re.match(
            r"\s*'",
            residual_after_qual[match.end() :],
        ):
            continue

        # CASE … END terminator only when nested under unmatched CASE (column end).

        if key == "end":
            cases = len(re.findall(r"(?is)\bCASE\b", prefix))

            ends = len(re.findall(r"(?is)\bEND\b", prefix))

            if cases > ends:
                continue

        if re.match(r"\s*\.", masked[match.end() :]):
            continue  # table qualifier of a qualified ref

        if re.match(r"\s*\(", masked[match.end() :]):
            continue  # function name

        needs_quote = key in quote_project_names

        needs_steal_temp = key in filter_boolean_steal_names

        if key in seen_bare or key in existing_aliases:
            # Already on frame — still rewrite residual keyword-ish / steal-name refs.

            if needs_steal_temp:
                temp_alias = steal_temp_by_key.get(key)

                if temp_alias is None:
                    # Frame already has the column under its real name (e.g. SELECT list
                    # pass-through) — still need a steal-safe temp for residual filter.

                    temp_alias = f"__repark_sql_udf_wcol_{counter}"

                    counter += 1

                    steal_temp_by_key[key] = temp_alias

                    new_parts.append(f"{_quote_ident(ident)} AS {_quote_ident(temp_alias)}")

                quote_residual_spans.append((match.start(), match.end(), _quote_ident(temp_alias)))

            elif needs_quote:
                quote_residual_spans.append((match.start(), match.end(), _quote_ident(ident)))

            continue

        seen_bare.add(key)

        if needs_steal_temp:
            temp_alias = f"__repark_sql_udf_wcol_{counter}"

            counter += 1

            steal_temp_by_key[key] = temp_alias

            existing_aliases.add(temp_alias.lower())

            new_parts.append(f"{_quote_ident(ident)} AS {_quote_ident(temp_alias)}")

            quote_residual_spans.append((match.start(), match.end(), _quote_ident(temp_alias)))

        else:
            existing_aliases.add(key)

            if needs_quote:
                new_parts.append(f"{_quote_ident(ident)} AS {_quote_ident(ident)}")

                quote_residual_spans.append((match.start(), match.end(), _quote_ident(ident)))

            else:
                new_parts.append(f"{ident} AS {_quote_ident(ident)}")

    # Quote residual keyword-ish bare columns right-to-left (filter cannot parse bare END/DATE).

    residual_chars = list(residual_after_qual)

    for start, end, replacement in sorted(quote_residual_spans, key=lambda item: -item[0]):
        residual_chars[start:end] = list(replacement)

    residual_after_qual = "".join(residual_chars)

    # Quoted residual identifiers (``"from"`` / `` `date` ``) — project when not already on frame.
    # Strings use single quotes and are already excluded by the quote-style scan.
    # filter-boolean-steal names always get a temp even when already quoted in residual.

    quoted_rewrite_spans: list[tuple[int, int, str]] = []

    for match in re.finditer(
        r'"((?:[^"]|"")*)"|`((?:[^`]|``)*)`',
        residual_after_qual,
    ):
        if match.group(1) is not None:
            name = match.group(1).replace('""', '"')

            expr = match.group(0)

        else:
            name = match.group(2).replace("``", "`")

            expr = match.group(0)

        if not name or name.startswith("__repark_sql_udf"):
            continue

        key = name.lower()

        if key in filter_boolean_steal_names:
            temp_alias = steal_temp_by_key.get(key)

            if temp_alias is None:
                temp_alias = f"__repark_sql_udf_wcol_{counter}"

                counter += 1

                steal_temp_by_key[key] = temp_alias

                existing_aliases.add(temp_alias.lower())

                new_parts.append(f"{expr} AS {_quote_ident(temp_alias)}")

                seen_bare.add(key)

            quoted_rewrite_spans.append((match.start(), match.end(), _quote_ident(temp_alias)))

            continue

        # Skip if this span is only an alias we already emitted (quoted temp rewrite).

        if key in seen_bare or key in existing_aliases:
            continue

        # Quoted form IS the column (including syntax-keyword column names like "from").

        seen_bare.add(key)

        existing_aliases.add(key)

        new_parts.append(f"{expr} AS {_quote_ident(name)}")

    if quoted_rewrite_spans:
        residual_chars = list(residual_after_qual)

        for start, end, replacement in sorted(quoted_rewrite_spans, key=lambda item: -item[0]):
            residual_chars[start:end] = list(replacement)

        residual_after_qual = "".join(residual_chars)

    return residual_after_qual, new_parts, counter
