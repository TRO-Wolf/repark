"""Arrow ↔ feature-matrix helpers for ext estimators (M4).

Uses numpy **only** after optional-extra import. Peak held data is the training
batch the library requires — not a second repark-owned row cache after fit.
"""

from __future__ import annotations

import contextlib
import weakref
from typing import Any

from repark.errors import IllegalArgumentException
from repark.ml.ext._deps import require_numpy

# Match crates/repark-ml MAX_FEATURES — data-controlled densify must not OOM (octo C1-SAF-002).
MAX_EXT_FEATURES = 4096


def _sized_sequence_len(seq: Any, *, field_name: str, features_col: str, row_index: int) -> int:
    """Length of a sized sequence without materializing an unsized iterator.

    Unbounded ``list(iterator)`` is refused (octo C3-SAF-001) — nnz must be
    inspectable via ``len`` before densify allocation.
    """
    if seq is None:
        return 0
    try:
        return len(seq)
    except TypeError as error:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} row {row_index} sparse {field_name} "
            f"is not a sized sequence (refusing unbounded materialize)"
        ) from error


def _refuse_sparse_nnz(
    nnz: int,
    n_vals: int,
    *,
    size: int,
    features_col: str,
    row_index: int,
) -> None:
    """Refuse nnz / value-length that would OOM or desync before densify."""
    if nnz != n_vals:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} row {row_index} sparse indices length "
            f"{nnz} != values length {n_vals}"
        )
    if nnz > MAX_EXT_FEATURES:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} row {row_index} sparse nnz={nnz} exceeds hard "
            f"limit p≤{MAX_EXT_FEATURES} (ext densify refuses data-controlled OOM)"
        )
    if nnz > size:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} row {row_index} sparse nnz={nnz} exceeds "
            f"size={size} (ext densify refuses oversized index materialize)"
        )


def _sparse_dict_to_dense(
    values: dict[str, Any],
    *,
    features_col: str,
    row_index: int,
) -> list[float]:
    """Densify a sparse struct dict with size + nnz caps (no unbounded list())."""
    size = int(values.get("size", 0))
    if size < 0:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} row {row_index} sparse size={size} is negative"
        )
    if size > MAX_EXT_FEATURES:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} sparse size={size} exceeds hard limit "
            f"p≤{MAX_EXT_FEATURES} (ext densify refuses data-controlled OOM)"
        )
    raw_indices = values.get("indices") or ()
    raw_values = values.get("values") or ()
    nnz = _sized_sequence_len(
        raw_indices, field_name="indices", features_col=features_col, row_index=row_index
    )
    n_vals = _sized_sequence_len(
        raw_values, field_name="values", features_col=features_col, row_index=row_index
    )
    _refuse_sparse_nnz(nnz, n_vals, size=size, features_col=features_col, row_index=row_index)
    # Length already validated — iterate in place (no full list() copy of huge inputs).
    dense = [0.0] * size
    for sparse_index, val in zip(raw_indices, raw_values, strict=True):
        position = int(sparse_index)
        if position < 0 or position >= size:
            raise IllegalArgumentException(
                f"featuresCol {features_col!r} row {row_index} sparse index "
                f"{position} out of range for size={size}"
            )
        dense[position] = float(val)
    return dense


def _try_sparse_struct_to_dense(
    cell: Any,
    *,
    features_col: str,
    row_index: int,
) -> list[float] | None:
    """If ``cell`` is an Arrow StructScalar sparse vector, densify with nnz caps.

    Returns ``None`` when the cell is not a struct-like sparse vector so the caller
    can fall through to ``as_py()`` dense / dict paths.
    """
    # StructScalar / Mapping-like with size+indices+values fields (not a plain dict).
    if isinstance(cell, dict):
        return None
    try:
        size_field = cell["size"]
        indices_field = cell["indices"]
        values_field = cell["values"]
    except (KeyError, TypeError, AttributeError):
        return None
    size_raw = size_field.as_py() if hasattr(size_field, "as_py") else size_field
    if size_raw is None:
        return None
    size = int(size_raw)
    if size < 0:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} row {row_index} sparse size={size} is negative"
        )
    if size > MAX_EXT_FEATURES:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} sparse size={size} exceeds hard limit "
            f"p≤{MAX_EXT_FEATURES} (ext densify refuses data-controlled OOM)"
        )
    # ListScalar supports len() without converting to a Python list of nnz elements.
    nnz = _sized_sequence_len(
        indices_field, field_name="indices", features_col=features_col, row_index=row_index
    )
    n_vals = _sized_sequence_len(
        values_field, field_name="values", features_col=features_col, row_index=row_index
    )
    _refuse_sparse_nnz(nnz, n_vals, size=size, features_col=features_col, row_index=row_index)
    # Only materialize after nnz is proven ≤ MAX_EXT_FEATURES and ≤ size.
    indices = indices_field.as_py() if hasattr(indices_field, "as_py") else indices_field
    sparse_values = values_field.as_py() if hasattr(values_field, "as_py") else values_field
    dense = [0.0] * size
    for sparse_index, val in zip(indices or (), sparse_values or (), strict=True):
        position = int(sparse_index)
        if position < 0 or position >= size:
            raise IllegalArgumentException(
                f"featuresCol {features_col!r} row {row_index} sparse index "
                f"{position} out of range for size={size}"
            )
        dense[position] = float(val)
    return dense


def _arrow_cell_is_null(cell: Any) -> bool:
    """Null probe without ``as_py()`` materialize (octo C4-SAF-001).

    Hostile sparse/dense StructScalar/ListScalar cells can be multi-GB when
    fully converted; use Arrow ``is_valid`` (or plain ``None``) first.
    """
    if cell is None:
        return True
    if hasattr(cell, "is_valid"):
        try:
            return not bool(cell.is_valid)
        except Exception:
            pass
    # Non-Arrow fallback only (plain Python None already handled).
    return False


def _dense_width_before_as_py(cell: Any) -> int | None:
    """Known list width without materializing element values, or ``None``.

    Prefers FixedSizeList ``type.list_size``, then ``len(cell)`` on ListScalar /
    sized sequences. Unsized iterators return ``None`` (caller may still refuse
    after a capped path).
    """
    cell_type = getattr(cell, "type", None)
    list_size = getattr(cell_type, "list_size", None)
    if list_size is not None:
        try:
            return int(list_size)
        except (TypeError, ValueError):
            pass
    try:
        return len(cell)
    except TypeError:
        return None


def features_matrix_from_arrow(table: Any, features_col: str) -> Any:
    """Build a 2-d float64 numpy array from a dense vector / list column.

    Accepts FixedSizeList / list / fixed-width sequence cells produced by
    ``VectorAssembler`` / dense vector columns. Sparse structs densify with a hard
    ``size`` / width cap of :data:`MAX_EXT_FEATURES` (same as native ``p ≤ 4096``).

    Null probes and dense width/nnz caps run **before** ``as_py()`` so hostile
    Arrow-native cells cannot OOM the process past the densify limits (octo
    C4-SAF-001; builds on C3 sparse nnz / C1 size caps).
    """
    np = require_numpy()
    if features_col not in table.column_names:
        raise IllegalArgumentException(
            f"featuresCol {features_col!r} not found in Arrow table columns "
            f"{list(table.column_names)}"
        )
    column = table.column(features_col)
    rows: list[list[float]] = []
    width: int | None = None
    for index in range(len(column)):
        cell = column[index]
        if _arrow_cell_is_null(cell):
            raise IllegalArgumentException(
                f"featuresCol {features_col!r} has null at row {index} "
                f"(ext estimators require dense non-null features)"
            )
        # Prefer Arrow StructScalar sparse path: cap size/nnz via len() before as_py
        # materializes huge index/value lists (octo C3-SAF-001 / C4-SAF-001).
        sparse_dense = _try_sparse_struct_to_dense(cell, features_col=features_col, row_index=index)
        if sparse_dense is not None:
            values = sparse_dense
        else:
            # Dense / list path: refuse width via len()/list_size *before* as_py.
            probe_width = _dense_width_before_as_py(cell)
            if probe_width is not None and probe_width > MAX_EXT_FEATURES:
                raise IllegalArgumentException(
                    f"featuresCol {features_col!r} width={probe_width} exceeds hard limit "
                    f"p≤{MAX_EXT_FEATURES}"
                )
            values = cell.as_py() if hasattr(cell, "as_py") else cell
            if values is None:
                raise IllegalArgumentException(
                    f"featuresCol {features_col!r} has null at row {index} "
                    f"(ext estimators require dense non-null features)"
                )
            if isinstance(values, dict):
                values = _sparse_dict_to_dense(values, features_col=features_col, row_index=index)
        if not isinstance(values, (list, tuple)):
            raise IllegalArgumentException(
                f"featuresCol {features_col!r} row {index} is not a dense vector/list "
                f"(got {type(values).__name__})"
            )
        if len(values) > MAX_EXT_FEATURES:
            raise IllegalArgumentException(
                f"featuresCol {features_col!r} width={len(values)} exceeds hard limit "
                f"p≤{MAX_EXT_FEATURES}"
            )
        row = [float(item) if item is not None else float("nan") for item in values]
        if width is None:
            width = len(row)
        elif len(row) != width:
            raise IllegalArgumentException(
                f"featuresCol {features_col!r} mixed widths {width} vs {len(row)} at row {index}"
            )
        rows.append(row)
    if not rows:
        raise IllegalArgumentException("ext estimator fit/transform: empty feature matrix (0 rows)")
    return np.asarray(rows, dtype=np.float64)


def label_vector_from_arrow(table: Any, label_col: str) -> Any:
    """Build a 1-d float64 numpy array from a scalar label column."""
    np = require_numpy()
    if label_col not in table.column_names:
        raise IllegalArgumentException(
            f"labelCol {label_col!r} not found in Arrow table columns {list(table.column_names)}"
        )
    column = table.column(label_col)
    values: list[float] = []
    for index in range(len(column)):
        cell = column[index]
        raw = cell.as_py() if hasattr(cell, "as_py") else cell
        if raw is None:
            raise IllegalArgumentException(
                f"labelCol {label_col!r} has null at row {index} "
                f"(ext estimators require non-null labels for fit)"
            )
        values.append(float(raw))
    if not values:
        raise IllegalArgumentException("ext estimator fit: empty label vector (0 rows)")
    return np.asarray(values, dtype=np.float64)


def _own_ext_temp_view(result_frame: Any, session: Any, view_name: str) -> None:
    """Drop ``view_name`` when ``result_frame`` is GC'd (mapInArrow-class ownership).

    Success-path re-entry must not orphan ``__repark_ml_ext_*`` MemTables across
    transform / CV folds (octo C1-Q-002 / C1-SAF-001).
    """

    def _drop() -> None:
        with contextlib.suppress(Exception):
            session.drop_temp_view(view_name)

    weakref.finalize(result_frame, _drop)


def reenter_with_prediction(
    frame: Any,
    original_table: Any,
    predictions: Any,
    prediction_col: str,
) -> Any:
    """Append prediction column and re-enter via Arrow IPC MemTable on the native session.

    O(n) predict materialization is intentional (M4 charter). ``frame`` is a
    :class:`~repark.dataframe.DataFrame` (its ``_session`` is the native handle —
    same path as ``mapInArrow`` re-entry). Nested feature columns survive.

    The registered ``__repark_ml_ext_*`` view is owned by the returned DataFrame and
    dropped on GC (and on register/sql failure before ownership transfers).
    """
    import io
    import uuid

    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    np = require_numpy()
    preds = np.asarray(predictions, dtype=np.float64).reshape(-1)
    if len(preds) != original_table.num_rows:
        raise IllegalArgumentException(
            f"prediction length {len(preds)} != table rows {original_table.num_rows}"
        )
    if prediction_col in original_table.column_names:
        from repark.errors import AnalysisException

        raise AnalysisException(
            f"predictionCol {prediction_col!r} already exists in the input schema "
            f"(repark.ml refuses silent overwrite)"
        )
    pred_array = pa.array(preds.tolist(), type=pa.float64())
    new_table = original_table.append_column(prediction_col, pred_array)
    sink = io.BytesIO()
    with pa_ipc.new_stream(sink, new_table.schema) as writer:
        for batch in new_table.to_batches():
            writer.write_batch(batch)
    view_name = f"__repark_ml_ext_{uuid.uuid4().hex}"
    session = frame._session
    owned = False
    try:
        # frame._session is native PyReparkSession (see dataframe.py mapInArrow notes).
        session.register_ipc_stream_as_temp_view(view_name, sink.getvalue())
        inner = session.sql(f"SELECT * FROM {view_name}")
        result = frame._spawn(inner)
        _own_ext_temp_view(result, session, view_name)
        owned = True
        return result
    except Exception:
        if not owned:
            with contextlib.suppress(Exception):
                session.drop_temp_view(view_name)
        raise


__all__ = [
    "MAX_EXT_FEATURES",
    "features_matrix_from_arrow",
    "label_vector_from_arrow",
    "reenter_with_prediction",
]
