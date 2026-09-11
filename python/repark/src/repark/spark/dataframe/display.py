"""DataFrame display bodies behind the public show and repr wrappers."""

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError
from repark.spark.dataframe.plan_collapse import (
    _box_rule,
    _column_widths,
    _display_type_labels_from_arrow,
    _duckdb_row_line,
    _format_duckdb_show,
    _format_eager_eval_table,
    _format_polars_show,
    _format_show_table,
    _format_show_vertical,
    _polars_column_gap,
    _polars_row_line,
    _table_to_cell_rows,
)
from repark.spark.dataframe.polars_cells import _arrow_pa_type_label

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

logger = logging.getLogger(__name__)


def _display_session_ints(frame: DataFrame) -> tuple[int, int, int]:
    """Read ``(max_rows, max_cols, str_len)`` from the alive token, builder map, or defaults."""
    from repark.spark.session.session_configuration import (
        _DISPLAY_INT_DEFAULTS,
        _builder_display_int,
        _display_token_key,
    )

    token = getattr(frame, "_alive_token", {}) or {}
    builder = token.get("builder_config")
    if not isinstance(builder, dict):
        builder = {}
    resolved: list[int] = []
    for canonical, fallback in _DISPLAY_INT_DEFAULTS.items():
        raw = token.get(_display_token_key(canonical))
        if isinstance(raw, bool) or not isinstance(raw, int):
            raw = _builder_display_int(builder, canonical, fallback)
        resolved.append(raw)
    return (resolved[0], resolved[1], resolved[2])


def _show(
    frame: DataFrame,
    n: int = 20,
    truncate: bool | int = True,
    vertical: bool = False,
) -> None:
    """Print up to ``n`` rows as a text table.

    The Spark style limits before collecting. Polars and DuckDB styles probe
    ``max_rows + 1`` rows first, show head and tail rows, and count only when the
    probe fills. ``truncate`` controls cell width; ``vertical`` applies only to the
    Spark style. INFO logs contain counts, while row data is DEBUG-only.
    """
    frame._ensure_alive()
    style = _resolve_display_style(frame)
    peeked: tuple[Any, int] | None = None
    if _use_bridge_peek(frame):
        n, cap, vertical = _normalize_show_args(frame, n, truncate, vertical)
        limit = max(0, n)
        if style == "spark":
            table = frame._consume_map_in_arrow_batches(max_output_rows=limit)
            if vertical:
                rendered = _format_show_vertical(
                    table, truncate_at=cap, n=limit, total_rows=table.num_rows
                )
            else:
                rendered = _format_show_table(table, truncate_at=cap)
            print(rendered)
            return
        peeked = (frame._consume_map_in_arrow_batches(max_output_rows=limit), limit)
    else:
        frame._materialize_cache_if_needed()
        n, cap, vertical = _normalize_show_args(frame, n, truncate, vertical)
    if style != "spark" and vertical:
        warnings.warn(
            "DataFrame.show(vertical=True) is only rendered under repark.display.style="
            "'spark'; styled polars/duckdb shows stay horizontal.",
            UserWarning,
            stacklevel=3,
        )
    if style != "spark" and truncate is True:
        cap = _display_session_ints(frame)[2]
    if style == "spark":
        limit = max(0, n)
        table = frame.limit((limit + 1) if vertical else limit).to_arrow()
        if vertical:
            has_more = table.num_rows > limit
            table = table.slice(0, limit)
            total_rows = limit + 1 if has_more else None
            rendered = _format_show_vertical(table, truncate_at=cap, n=limit, total_rows=total_rows)
        else:
            rendered = _format_show_table(table, truncate_at=cap)
        shown_rows = table.num_rows
    else:
        rendered, shown_rows = _render_styled_show(
            frame, style, n=max(0, n), truncate_at=cap, peeked=peeked
        )
    print(rendered)
    logger.info("show(%s rows)", shown_rows)
    logger.debug("show(%s rows):\n%s", shown_rows, rendered)


def _repr(frame: DataFrame) -> str:
    """Schema header when lazy; the styled table when materialised or eager-evalled."""
    frame._ensure_alive()
    style = _resolve_display_style(frame)
    if style != "spark":
        str_len = _display_session_ints(frame)[2]
        if _repr_renders_schema(frame):
            if _eager_eval_enabled(frame):
                max_rows, _ = _eager_eval_limits(frame)
                rendered, _ = _render_styled_show(frame, style, n=max_rows, truncate_at=str_len)
                return rendered
            return _render_lazy_header(frame, style)
        rendered, _ = _render_styled_show(frame, style, n=20, truncate_at=str_len)
        return rendered
    if not _eager_eval_enabled(frame):
        return frame.__str__()
    max_rows, truncate_at = _eager_eval_limits(frame)
    if _use_bridge_peek(frame):
        table = frame._consume_map_in_arrow_batches(max_output_rows=max_rows + 1)
    else:
        table = frame.limit(max_rows + 1).to_arrow()
    has_more = table.num_rows > max_rows
    shown = table.slice(0, max_rows)
    rendered = _format_eager_eval_table(shown, truncate_at=truncate_at)
    if has_more:
        rendered = f"{rendered}\nonly showing top {max_rows} row" + ("s" if max_rows != 1 else "")
    return rendered


def _repr_renders_schema(frame: DataFrame) -> bool:
    """Whether ``repr`` shows the schema header: no stored shape, no materialised view."""
    return frame._eager_shape is None and frame._cache_view is None


def _lazy_first_line(column_count: int) -> str:
    """First line of a lazy ``repr``; it names the column count, never a row count."""
    return (
        f"lazy: {column_count} columns, not yet materialized — .eager(), "
        ".show() or .collect() run the plan"
    )


def _render_lazy_header(frame: DataFrame, style: str) -> str:
    """Render the schema-only header for a lazy frame without running the plan."""
    names = list(frame.columns)
    schema = frame._analyzed_arrow_schema()
    type_labels = [_arrow_pa_type_label(field.type, style=style) for field in schema]
    first_line = _lazy_first_line(len(names))
    if style == "polars":
        _, max_cols, _ = _display_session_ints(frame)
        return _format_polars_lazy_header(
            names, type_labels, first_line=first_line, max_cols=max_cols
        )
    return _format_duckdb_lazy_header(names, type_labels, first_line=first_line)


def _format_polars_lazy_header(
    names: list[str],
    type_labels: list[str],
    *,
    first_line: str,
    max_cols: int | None = None,
) -> str:
    """Render the polars header box over the lazy first line, with no data rows."""
    if not names:
        return f"{first_line}\n┌┐\n└┘"
    names, type_labels, _, _, gap_at = _polars_column_gap(names, type_labels, [], [], max_cols)
    widths = _column_widths(names, type_labels)
    inner_widths = [width + 2 for width in widths]
    dashes = ["---"] * len(names)
    if gap_at is not None:
        dashes[gap_at] = ""
    lines = [
        first_line,
        _box_rule(inner_widths, "┌", "┬", "┐"),
        _polars_row_line(names, widths),
        _polars_row_line(dashes, widths),
        _polars_row_line(type_labels, widths),
        _box_rule(inner_widths, "└", "┴", "┘"),
    ]
    return "\n".join(lines)


def _format_duckdb_lazy_header(
    names: list[str],
    type_labels: list[str],
    *,
    first_line: str,
) -> str:
    """Render the duckdb header box over the lazy first line, with no footer."""
    if not names:
        return f"{first_line}\n┌┐\n└┘"
    widths = _column_widths(names, type_labels)
    padded_widths = [width + 2 for width in widths]
    lines = [
        first_line,
        _box_rule(padded_widths, "┌", "┬", "┐"),
        _duckdb_row_line(names, widths, center=True),
        _duckdb_row_line(type_labels, widths, center=True),
        _box_rule(padded_widths, "└", "┴", "┘"),
    ]
    return "\n".join(lines)


def _repr_html(frame: DataFrame) -> str | None:
    """HTML table under spark with eager eval; ``None`` under polars and duckdb."""
    import html as html_module

    frame._ensure_alive()
    if _resolve_display_style(frame) != "spark":
        return None
    if not _eager_eval_enabled(frame):
        return None
    max_rows, truncate_at = _eager_eval_limits(frame)
    if _use_bridge_peek(frame):
        table = frame._consume_map_in_arrow_batches(max_output_rows=max_rows + 1)
    else:
        table = frame.limit(max_rows + 1).to_arrow()
    has_more = table.num_rows > max_rows
    shown = table.slice(0, max_rows)
    names = list(shown.column_names)
    rows = _table_to_cell_rows(shown, truncate_at=None, style="spark")
    if truncate_at is not None and truncate_at > 0:
        rows = [[cell[:truncate_at] for cell in row] for row in rows]
    safe_names = [html_module.escape(name, quote=True) for name in names]
    parts = [
        "<table border='1'>",
        "<tr>" + "".join(f"<th>{name}</th>" for name in safe_names) + "</tr>",
    ]
    for row in rows:
        safe_cells = [html_module.escape(cell, quote=True) for cell in row]
        parts.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in safe_cells) + "</tr>")
    parts.append("</table>")
    html = "\n".join(parts)
    if has_more:
        html = f"{html}\nonly showing top {max_rows} row" + ("s" if max_rows != 1 else "")
    return html


def _use_bridge_peek(frame: DataFrame) -> bool:
    """Whether previews peek the mapInArrow bridge instead of the engine."""
    return frame._map_bridge is not None and not (
        frame._persist_requested or frame._checkpoint_lazy or frame._cache_view is not None
    )


def _eager_eval_enabled(frame: DataFrame) -> bool:
    """``spark.sql.repl.eagerEval.enabled`` truthy (runtime conf or builder)."""
    raw = _conf_lookup(frame, "spark.sql.repl.eagerEval.enabled")
    if raw is None:
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _eager_eval_limits(frame: DataFrame) -> tuple[int, int]:
    """``(maxNumRows, truncate)`` with Spark defaults 20 / 20."""
    max_raw = _conf_lookup(frame, "spark.sql.repl.eagerEval.maxNumRows")
    trunc_raw = _conf_lookup(frame, "spark.sql.repl.eagerEval.truncate")
    try:
        max_rows = max(0, int(max_raw)) if max_raw is not None else 20
    except (TypeError, ValueError):
        max_rows = 20
    try:
        truncate_at = max(0, int(trunc_raw)) if trunc_raw is not None else 20
    except (TypeError, ValueError):
        truncate_at = 20
    return max_rows, truncate_at


def _conf_lookup(frame: DataFrame, key: str) -> str | None:
    """Runtime conf then builder snapshot (case-sensitive Spark keys)."""
    token = getattr(frame, "_alive_token", {}) or {}
    store = token.get("runtime_conf")
    if isinstance(store, dict) and key in store:
        return str(store[key])
    builder = token.get("builder_config") or {}
    if key in builder and builder[key] is not None:
        return str(builder[key])
    return None


def _normalize_show_args(
    frame: DataFrame,
    n: int,
    truncate: bool | int | float | str,
    vertical: bool,
) -> tuple[int, int | None, bool]:
    """Validate ``show`` arguments; return ``(n, truncate_cap, vertical)``.

    Mirrors Spark 4.1.2 diagnostics used by Apache ``test_df_show`` (NOT_INT / NOT_BOOL).
    Digit-only string ``truncate`` values (e.g. ``\"1\"``) are accepted as width caps.
    """
    if not isinstance(n, int) or isinstance(n, bool):
        raise PySparkTypeError(
            errorClass="NOT_INT",
            messageParameters={"arg_name": "n", "arg_type": type(n).__name__},
        )
    if not isinstance(vertical, bool):
        raise PySparkTypeError(
            errorClass="NOT_BOOL",
            messageParameters={
                "arg_name": "vertical",
                "arg_type": type(vertical).__name__,
            },
        )
    if truncate is True:
        return n, 20, vertical
    if truncate is False:
        return n, None, vertical
    if isinstance(truncate, (int, float)) and not isinstance(truncate, bool):
        width = int(truncate)
        return n, (width if width > 0 else None), vertical
    if isinstance(truncate, str) and truncate.isdigit():
        width = int(truncate)
        return n, (width if width > 0 else None), vertical
    raise PySparkTypeError(
        errorClass="NOT_BOOL",
        messageParameters={
            "arg_name": "truncate",
            "arg_type": type(truncate).__name__,
        },
    )


def _resolve_display_style(frame: DataFrame) -> str:
    """Return the session display style (``spark`` / ``polars`` / ``duckdb``), default polars."""
    style = frame._alive_token.get("display_style", "spark")
    if isinstance(style, str) and style in {"spark", "polars", "duckdb"}:
        return style
    return "spark"


def _styled_total_rows(frame: DataFrame) -> int:
    """Total rows for a styled preview, reusing a known eager shape."""
    frame._materialize_cache_if_needed()
    shape = frame._eager_shape
    return shape[0] if shape is not None else frame.count()


def _preview_tail_rows(frame: DataFrame, n: int, *, total_rows: int) -> Any:
    """Return the last ``n`` rows for display without collecting the full result."""
    import pyarrow as pa

    fetch = max(0, int(n))
    total = max(0, int(total_rows))
    if fetch == 0 or total == 0:
        return frame.limit(0).to_arrow()
    if total <= fetch:
        return frame.limit(total).to_arrow()
    skip = total - fetch
    limited = frame._spawn(frame._plan().limit_with_skip(skip, fetch))
    table = limited.to_arrow()
    if not isinstance(table, pa.Table):
        return pa.table(table)
    return table


def _render_styled_show(
    frame: DataFrame,
    style: str,
    *,
    n: int,
    truncate_at: int | None,
    peeked: tuple[Any, int] | None = None,
) -> tuple[str, int]:
    """Render a styled preview and return its text and row count."""
    col_names = list(frame.columns)
    max_rows, max_cols, _ = _display_session_ints(frame)
    if peeked is None:
        if style == "polars":
            probe_limit = max_rows + 1
            probe_table = frame.limit(probe_limit).to_arrow()
            if probe_table.num_rows < probe_limit:
                total_rows = probe_table.num_rows
                head_table = probe_table.slice(0, max(n, 0))
                tail_table = None
                use_ellipsis = False
            else:
                total_rows = _styled_total_rows(frame)
                head_n, tail_n = 0, 0
                if n > 0:
                    keep = min(n, max_rows)
                    head_n = (keep + 1) // 2
                    tail_n = keep - head_n
                use_ellipsis = tail_n > 0 or n >= max_rows
                head_table = probe_table.slice(0, head_n)
                tail_table = (
                    frame._preview_tail_rows(tail_n, total_rows=total_rows) if tail_n > 0 else None
                )
        else:
            probe_limit = max_rows + 1
            probe_table = frame.limit(probe_limit).to_arrow()
            if probe_table.num_rows < probe_limit:
                total_rows = probe_table.num_rows
                if n <= 0:
                    head_table = probe_table.slice(0, 0)
                    tail_table = None
                    use_ellipsis = False
                elif total_rows <= n:
                    head_table = probe_table.slice(0, total_rows)
                    tail_table = None
                    use_ellipsis = False
                else:
                    head_n = n // 2
                    if head_n == 0:
                        head_n = 1
                    tail_n = n - head_n
                    use_ellipsis = tail_n > 0
                    head_table = probe_table.slice(0, head_n)
                    tail_table = (
                        probe_table.slice(total_rows - tail_n, tail_n) if tail_n > 0 else None
                    )
            else:
                total_rows = _styled_total_rows(frame)
                if n <= 0:
                    head_n, tail_n, use_ellipsis = 0, 0, False
                elif total_rows <= n:
                    head_n, tail_n, use_ellipsis = total_rows, 0, False
                else:
                    head_n = n // 2
                    if head_n == 0:
                        head_n = 1
                    tail_n = n - head_n
                    use_ellipsis = tail_n > 0
                head_table = (
                    frame.limit(head_n).to_arrow() if head_n > 0 else frame.limit(0).to_arrow()
                )
                tail_table = (
                    frame._preview_tail_rows(tail_n, total_rows=total_rows) if tail_n > 0 else None
                )
    else:
        peek_table, peek_limit = peeked
        if peek_table.num_rows < peek_limit:
            total_rows = peek_table.num_rows
        else:
            total_rows = _styled_total_rows(frame)
        if style == "polars":
            if total_rows <= max_rows:
                head_table = peek_table.slice(0, max(n, 0))
                tail_table = None
                use_ellipsis = False
            else:
                head_n, tail_n = 0, 0
                if n > 0:
                    keep = min(n, max_rows)
                    head_n = (keep + 1) // 2
                    tail_n = keep - head_n
                use_ellipsis = tail_n > 0 or n >= max_rows
                head_table = peek_table.slice(0, head_n)
                if tail_n > 0 and peek_table.num_rows < peek_limit:
                    tail_table = peek_table.slice(peek_table.num_rows - tail_n, tail_n)
                elif tail_n > 0:
                    tail_table = frame._preview_tail_rows(tail_n, total_rows=total_rows)
                else:
                    tail_table = None
        else:
            if n <= 0:
                head_n, tail_n, use_ellipsis = 0, 0, False
            elif total_rows <= n:
                head_n, tail_n, use_ellipsis = total_rows, 0, False
            else:
                head_n = n // 2
                if head_n == 0:
                    head_n = 1
                tail_n = n - head_n
                use_ellipsis = tail_n > 0
            head_table = peek_table.slice(0, head_n)
            tail_table = (
                frame._preview_tail_rows(tail_n, total_rows=total_rows) if tail_n > 0 else None
            )
    type_labels = _display_type_labels_from_arrow(head_table, style=style)

    head_rows = _table_to_cell_rows(head_table, truncate_at=truncate_at, style=style)
    tail_rows = (
        _table_to_cell_rows(tail_table, truncate_at=truncate_at, style=style)
        if tail_table is not None
        else []
    )
    shown = len(head_rows) + len(tail_rows)
    if style == "polars":
        rendered = _format_polars_show(
            col_names,
            type_labels,
            head_rows,
            tail_rows if use_ellipsis else [],
            total_rows=total_rows,
            show_ellipsis=use_ellipsis,
            max_cols=max_cols,
        )
    else:
        rendered = _format_duckdb_show(
            col_names,
            type_labels,
            head_rows,
            tail_rows if use_ellipsis else [],
            total_rows=total_rows,
            shown_rows=shown,
            show_ellipsis=use_ellipsis,
        )
    return rendered, shown
