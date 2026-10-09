"""Execute the ``mapInArrow`` bridge: run the user function, re-ingest the batches.

The functions below are bound as ``DataFrame`` methods from ``core.py``;
``self`` is the frame carrying ``_map_bridge``.

pins: mb-4/C-039
"""

from __future__ import annotations

import contextlib
import weakref
from typing import TYPE_CHECKING, Any

from repark.errors import PySparkException
from repark.spark._secrets import scrub_exception, scrub_user_failure
from repark.spark.dataframe import streaming_batch, surface_b
from repark.spark.dataframe.udf_schema import _validate_map_in_arrow_batch

if TYPE_CHECKING:
    from collections.abc import Iterator

    from repark.spark.dataframe.core import DataFrame
    from repark.spark.types import StructType


def _drop_mia_temp_views(session: Any, names: list[str]) -> None:
    """Drop mapInArrow scratch views when a DataFrame is finalized."""
    for view_name in list(names):
        with contextlib.suppress(Exception):
            session.drop_temp_view(view_name)
    names.clear()


def materialize_map_bridge_once(self: DataFrame) -> None:
    """Snapshot the mapInArrow bridge once for child plan construction."""
    if self._map_bridge is None:
        return
    if self._mia_plan_ready:
        return
    self._inner = self._execute_map_in_arrow_bridge(replace_ephemeral_views=False)
    self._mia_plan_ready = True


def ensure_mia_view_cleanup(self: DataFrame) -> None:
    """Attach one finalizer to drop this facade's MIA views."""
    if self._mia_cleanup_registered:
        return
    self._mia_cleanup_registered = True
    weakref.finalize(self, _drop_mia_temp_views, self._session, self._mia_temp_views)


def track_mia_view(self: DataFrame, view_name: str, *, replace_ephemeral: bool) -> None:
    """Track an MIA view and optionally replace prior action views."""
    import contextlib

    if replace_ephemeral:
        for old_name in list(self._mia_action_views):
            with contextlib.suppress(Exception):
                self._session.drop_temp_view(old_name)
            with contextlib.suppress(ValueError):
                self._mia_temp_views.remove(old_name)
        self._mia_action_views.clear()
        self._mia_action_views.append(view_name)
    self._mia_temp_views.append(view_name)
    self._ensure_mia_view_cleanup()


def iter_map_in_arrow_output(
    self: DataFrame,
    *,
    max_output_rows: int | None = None,
) -> Iterator[Any]:
    """Yield validated output batches from the mapInArrow user func.

    Upstream is O(batch) via ``RecordBatchReader`` (never collect-all-then-UDF). The
    upstream reader is closed best-effort on every exit path including early user-func
    failure / ``None`` return. When ``max_output_rows`` is
    set, stops after that many output rows (peek path).
    """
    import contextlib

    import pyarrow as pa

    from repark.spark.dataframe.core import DataFrame

    bridge = self._map_bridge
    if bridge is None:
        raise RuntimeError("mapInArrow bridge missing")
    parent: DataFrame = bridge["parent"]
    func = bridge["func"]
    declared_schema: StructType = bridge["schema"]
    expected_arrow: pa.Schema = bridge["arrow_schema"]

    parent._ensure_alive()
    parent_for_stream: DataFrame = parent
    if parent._map_bridge is not None:
        nested_inner = parent._action_inner()
        parent_for_stream = DataFrame(nested_inner, parent._session, parent._alive_token)
        parent_for_stream._handles = parent._handles

    try:
        input_reader = pa.RecordBatchReader.from_stream(parent_for_stream)
    except Exception as error:
        raise PySparkException(
            f"mapInArrow failed opening upstream Arrow stream: {error}"
        ) from error

    rows_kept = 0
    failure = passthrough = None
    try:
        output = func(iter(input_reader))
        if output is None:
            raise PySparkException(
                "mapInArrow user function must return an iterator of pyarrow.RecordBatch (got None)"
            )

        try:
            iterator = iter(output)
        except TypeError as error:
            raise PySparkException(
                "mapInArrow user function must return an iterator of "
                f"pyarrow.RecordBatch (got {type(output).__name__})"
            ) from error

        for item in iterator:
            if not isinstance(item, pa.RecordBatch):
                raise PySparkException(
                    "mapInArrow user function must yield pyarrow.RecordBatch; "
                    f"got {type(item).__name__}"
                )
            _validate_map_in_arrow_batch(item, expected_arrow, declared_schema)
            aligned = item.cast(expected_arrow)
            if max_output_rows is not None:
                remaining = max_output_rows - rows_kept
                if remaining <= 0:
                    break
                if aligned.num_rows > remaining:
                    aligned = aligned.slice(0, remaining)
                yield aligned
                rows_kept += aligned.num_rows
                if rows_kept >= max_output_rows:
                    break
            else:
                yield aligned
    except PySparkException as error:
        passthrough = scrub_exception(error)
        if passthrough is error:
            raise
    except Exception as error:
        detail, failure = scrub_user_failure(error)
    finally:
        close = getattr(input_reader, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()
    if passthrough is not None:
        raise passthrough
    if failure is not None:
        raise PySparkException(
            f"mapInArrow user function raised {type(failure).__name__}: {failure}\n{detail}"
        ) from failure


def consume_map_in_arrow_batches(
    self: DataFrame,
    *,
    max_output_rows: int | None = None,
) -> Any:
    """Run the mapInArrow bridge and return a ``pyarrow.Table`` (optional row cap)."""
    import pyarrow as pa

    bridge = self._map_bridge
    if bridge is None:
        raise RuntimeError("mapInArrow bridge missing")
    streaming_batch.refuse_streaming_action_on_bridge(self)
    surface_b.fill_on_action(self)
    expected_arrow: pa.Schema = bridge["arrow_schema"]
    batches = list(self._iter_map_in_arrow_output(max_output_rows=max_output_rows))
    if not batches:
        return pa.Table.from_batches([], schema=expected_arrow)
    return pa.Table.from_batches(batches, schema=expected_arrow)


def execute_map_in_arrow_bridge(self: DataFrame, *, replace_ephemeral_views: bool = True) -> Any:
    """Stream parent batches through ``func`` and re-ingest as a MemTable scan.

    Memory contract: upstream is pulled one Arrow batch at a time via the existing
    lazy ``__arrow_c_stream__`` export (O(batch) on the input side). Output batches are
    drained in pure Python (safe GIL ownership for nested parent-stream pulls), then
    re-ingested via the I4 Arrow **C Stream** seam
    (``register_arrow_stream_as_temp_view``) — no intermediate IPC encode/decode buffer.
    When the native symbol is absent (version-skew guard), fall back to the IPC path.

    ``self._session`` is the **native** ``PyReparkSession`` handle (same as every other
    DataFrame method), not the Python facade.
    """
    import pyarrow as pa

    bridge = self._map_bridge
    if bridge is None:
        raise RuntimeError("mapInArrow bridge missing")
    expected_arrow = bridge["arrow_schema"]

    register_stream = getattr(self._session, "register_arrow_stream_as_temp_view", None)
    if callable(register_stream):
        batches = list(self._iter_map_in_arrow_output(max_output_rows=None))
        table = pa.Table.from_batches(batches, schema=expected_arrow)
        return self._register_arrow_stream_as_inner(
            table, replace_ephemeral=replace_ephemeral_views
        )

    return self._execute_map_in_arrow_bridge_ipc(replace_ephemeral_views=replace_ephemeral_views)


def execute_map_in_arrow_bridge_ipc(
    self: DataFrame, *, replace_ephemeral_views: bool = True
) -> Any:
    """IPC-encode output batches then ``register_ipc_stream_as_temp_view`` (fallback path)."""
    import io

    import pyarrow.ipc as pa_ipc

    bridge = self._map_bridge
    if bridge is None:
        raise RuntimeError("mapInArrow bridge missing")
    expected_arrow = bridge["arrow_schema"]

    sink = io.BytesIO()
    writer: Any | None = None
    try:
        for aligned in self._iter_map_in_arrow_output(max_output_rows=None):
            if writer is None:
                writer = pa_ipc.new_stream(sink, expected_arrow)
            writer.write_batch(aligned)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        with pa_ipc.new_stream(sink, expected_arrow):
            pass

    return self._register_ipc_bytes_as_inner(
        sink.getvalue(), replace_ephemeral=replace_ephemeral_views
    )
