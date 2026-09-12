"""Arrow C Stream register: capsule first, IPC as the version-skew fallback."""

from __future__ import annotations

import io
from typing import Any

from repark.spark._pyarrow import require_pyarrow


def register_arrow_exporter_as_temp_view(native: Any, view_name: str, exporter: Any) -> None:
    """Register an Arrow C Stream exporter, falling back to IPC when the native symbol is absent."""
    register_stream = getattr(native, "register_arrow_stream_as_temp_view", None)
    if callable(register_stream):
        register_stream(view_name, exporter)
        return
    pa = require_pyarrow()
    import pyarrow.ipc as pa_ipc

    if hasattr(exporter, "to_batches") and hasattr(exporter, "schema"):
        table = exporter
    else:
        table = pa.table(exporter)
    sink = io.BytesIO()
    with pa_ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    native.register_ipc_stream_as_temp_view(view_name, sink.getvalue())
