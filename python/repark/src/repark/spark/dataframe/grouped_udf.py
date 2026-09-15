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


def _apply_in_pandas_column_run_mask(column: Any) -> Any:
    """Boolean array of ``len(column) - 1``: ``True`` where row i's key cell differs
    from row i + 1's.

    ``pyarrow.compute`` walks adjacent values in bulk; NULL pairs and NaN pairs
    count as equal, matching ``_apply_in_pandas_scalar_key_equal``. A key type with
    no ``not_equal`` kernel falls back to the per-row scalar compare for that
    column only.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    row_count = len(column)
    prev = column.slice(0, row_count - 1)
    nxt = column.slice(1)
    try:
        null_diff = pc.not_equal(pc.is_null(prev), pc.is_null(nxt))
        value_diff = pc.fill_null(pc.not_equal(prev, nxt), False)
        if pa.types.is_floating(column.type):
            both_nan = pc.fill_null(pc.and_(pc.is_nan(prev), pc.is_nan(nxt)), False)
            value_diff = pc.and_(value_diff, pc.invert(both_nan))
        return pc.or_(null_diff, value_diff)
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid, TypeError, ValueError):
        values = column.to_pylist()
        return pa.array(
            [
                not _apply_in_pandas_scalar_key_equal(values[index], values[index + 1])
                for index in range(row_count - 1)
            ],
            type=pa.bool_(),
        )


def _apply_in_pandas_batch_run_starts(batch: Any, key_names: list[str]) -> list[int]:
    """Start index of every contiguous key run inside one batch."""
    import pyarrow.compute as pc

    diff: Any = None
    for name in key_names:
        column_diff = _apply_in_pandas_column_run_mask(batch.column(name))
        diff = column_diff if diff is None else pc.or_(diff, column_diff)
    starts = [0]
    if diff is not None:
        starts.extend(index + 1 for index in pc.indices_nonzero(diff).to_pylist())
    return starts


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


def _apply_in_pandas_scalar_key_compare(left: Any, right: Any) -> int:
    """Total order for one group-key cell matching the engine's ascending sort."""
    import math

    if left is None and right is None:
        return 0
    if left is None:
        return -1
    if right is None:
        return 1
    left_nan = isinstance(left, float) and math.isnan(left)
    right_nan = isinstance(right, float) and math.isnan(right)
    if left_nan or right_nan:
        if left_nan and right_nan:
            return 0
        return 1 if left_nan else -1
    try:
        if left < right:
            return -1
        if left > right:
            return 1
        return 0
    except TypeError:
        left_tag = type(left).__name__
        right_tag = type(right).__name__
        if left_tag != right_tag:
            return -1 if left_tag < right_tag else 1
        left_text = str(left)
        right_text = str(right)
        if left_text == right_text:
            return 0
        return -1 if left_text < right_text else 1


def _apply_in_pandas_keys_compare(left: tuple[Any, ...], right: tuple[Any, ...]) -> int:
    """Lexicographic key-tuple compare used by the cogroup merge walk."""
    for left_cell, right_cell in zip(left, right, strict=False):
        order = _apply_in_pandas_scalar_key_compare(left_cell, right_cell)
        if order != 0:
            return order
    return (len(left) > len(right)) - (len(left) < len(right))


def _iter_apply_in_pandas_keyed_groups(
    input_batches: Iterator[Any],
    key_names: list[str],
) -> Iterator[tuple[tuple[Any, ...], list[Any]]]:
    """Yield ``(key, segments)`` per contiguous key group from a sorted batch stream.

    The current group and one input batch remain buffered. Empty keys form one global group.
    Segments are batch slices, so a group split across batch edges stays O(group) here.
    Run boundaries come from ``pyarrow.compute`` (R-3): ``as_py`` runs once per
    contiguous run — the boundary key — never per row.
    """
    pending_segments: list[Any] = []
    current_key: Any = _APPLY_IN_PANDAS_KEY_MISSING

    if not key_names:
        segments = [batch for batch in input_batches if batch.num_rows > 0]
        if segments:
            yield ((), segments)
        return

    for batch in input_batches:
        if batch.num_rows == 0:
            continue
        missing = [name for name in key_names if name not in batch.schema.names]
        if missing:
            raise PySparkException(
                "grouped map UDF group key column(s) missing from streamed batch: "
                f"{missing}; batch fields={list(batch.schema.names)}"
            )
        row_count = batch.num_rows
        run_starts = _apply_in_pandas_batch_run_starts(batch, key_names)
        run_starts.append(row_count)
        for index, run_start in enumerate(run_starts[:-1]):
            segment = batch.slice(run_start, run_starts[index + 1] - run_start)
            run_key = _apply_in_pandas_row_key(batch, key_names, run_start)
            if current_key is _APPLY_IN_PANDAS_KEY_MISSING:
                current_key = run_key
                pending_segments = [segment]
            elif _apply_in_pandas_keys_equal(current_key, run_key):
                pending_segments.append(segment)
            else:
                yield (current_key, pending_segments)
                current_key = run_key
                pending_segments = [segment]

    if pending_segments:
        yield (current_key, pending_segments)


def _iter_apply_in_pandas_group_tables(
    input_batches: Iterator[Any],
    key_names: list[str],
) -> Iterator[Any]:
    """Yield one table per contiguous key group from a sorted batch stream.

    The current group and one input batch remain buffered. Empty keys form one global group.
    """
    for _key, segments in _iter_apply_in_pandas_keyed_groups(input_batches, key_names):
        yield _apply_in_pandas_table_from_segments(segments)
