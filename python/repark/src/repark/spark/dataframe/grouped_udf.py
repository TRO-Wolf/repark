"""Contiguous-group assembly for the applyInPandas bridge."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from repark.errors import PySparkException

_APPLY_IN_PANDAS_KEY_MISSING: object = object()


def _apply_in_pandas_scalar_key_equal(left: Any, right: Any) -> bool:
    """Null- and NaN-safe equality for one group-key cell (Spark groups NaN with NaN)."""
    import math

    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    if (
        isinstance(left, float)
        and isinstance(right, float)
        and math.isnan(left)
        and math.isnan(right)
    ):
        return True
    return bool(left == right)


def _apply_in_pandas_keys_equal(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    """Null- and NaN-safe equality for a multi-column group key tuple."""
    if len(left) != len(right):
        return False
    return all(
        _apply_in_pandas_scalar_key_equal(left_cell, right_cell)
        for left_cell, right_cell in zip(left, right, strict=True)
    )


def _apply_in_pandas_row_key(batch: Any, key_names: list[str], row_index: int) -> tuple[Any, ...]:
    """Build the group-key tuple for one row of a RecordBatch (``as_py`` cells)."""
    return tuple(batch.column(name)[row_index].as_py() for name in key_names)


def _apply_in_pandas_table_from_segments(segments: list[Any]) -> Any:
    """Build one ``pyarrow.Table`` from group segments, promoting schemas across batch edges.

    Engine streams share one schema, but hand-built / boundary-stitched segments can differ
    when a string/binary column is all-null in one batch (Arrow ``null`` type) and concrete
    in the next. ``Table.from_batches`` rejects that; ``concat_tables(..., promote)`` unifies
    null→concrete so boundary-stitch stays O(group) without a facade re-group.
    """
    import pyarrow as pa

    if not segments:
        raise PySparkException("applyInPandas internal error: empty group segment list")
    try:
        return pa.Table.from_batches(segments)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
        tables = [pa.Table.from_batches([segment]) for segment in segments]
        try:
            return pa.concat_tables(tables, promote_options="default")
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError) as promote_error:
            raise PySparkException(
                "applyInPandas failed stitching group segments across batch boundaries "
                f"(incompatible schemas): {promote_error}"
            ) from error


def _validate_apply_in_pandas_result_columns(
    out_pdf: Any,
    expected_names: list[str],
) -> None:
    """Validate returned column names against the declared schema."""
    got_names = [str(name) for name in out_pdf.columns]
    if len(out_pdf) == 0 and len(got_names) == 0:
        return
    expected_set = set(expected_names)
    got_set = set(got_names)
    missing = [name for name in expected_names if name not in got_set]
    unexpected = [name for name in got_names if name not in expected_set]
    if not missing and not unexpected:
        return
    parts: list[str] = []
    if missing:
        parts.append(f"Missing: {', '.join(missing)}")
    if unexpected:
        parts.append(f"Unexpected: {', '.join(unexpected)}")
    raise PySparkException(
        "applyInPandas schema mismatch: column names of the returned data do not match "
        f"specified schema. {'. '.join(parts)}."
    )


def _iter_apply_in_pandas_group_tables(
    input_batches: Iterator[Any],
    key_names: list[str],
) -> Iterator[Any]:
    """Yield one table per contiguous key group from a sorted batch stream.

    The current group and one input batch remain buffered. Empty keys form one global group.
    """
    pending_segments: list[Any] = []
    current_key: Any = _APPLY_IN_PANDAS_KEY_MISSING

    if not key_names:
        segments = [batch for batch in input_batches if batch.num_rows > 0]
        if segments:
            yield _apply_in_pandas_table_from_segments(segments)
        return

    for batch in input_batches:
        if batch.num_rows == 0:
            continue
        missing = [name for name in key_names if name not in batch.schema.names]
        if missing:
            raise PySparkException(
                "applyInPandas group key column(s) missing from streamed batch: "
                f"{missing}; batch fields={list(batch.schema.names)}"
            )
        run_start = 0
        run_key = _apply_in_pandas_row_key(batch, key_names, 0)
        row_count = batch.num_rows
        for row_index in range(1, row_count + 1):
            if row_index < row_count:
                next_key = _apply_in_pandas_row_key(batch, key_names, row_index)
                if _apply_in_pandas_keys_equal(next_key, run_key):
                    continue
            segment = batch.slice(run_start, row_index - run_start)
            if current_key is _APPLY_IN_PANDAS_KEY_MISSING:
                current_key = run_key
                pending_segments = [segment]
            elif _apply_in_pandas_keys_equal(current_key, run_key):
                pending_segments.append(segment)
            else:
                yield _apply_in_pandas_table_from_segments(pending_segments)
                current_key = run_key
                pending_segments = [segment]
            if row_index < row_count:
                run_start = row_index
                run_key = next_key

    if pending_segments:
        yield _apply_in_pandas_table_from_segments(pending_segments)
