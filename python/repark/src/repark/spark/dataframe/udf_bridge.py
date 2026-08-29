"""Action-time pandas and classic UDF mapInArrow callbacks.

This module keeps callbacks independent from ``DataFrame`` to avoid a circular import.
User exceptions become ``PySparkException`` values with traceback text.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable, Iterator
from typing import Any

from repark.errors import PySparkException
from repark.spark.types import StructField


def _pandas_frames_from_arrow_batches(input_batches: Iterator[Any]) -> Iterator[Any]:
    """Yield pandas DataFrames from Arrow batches."""
    for batch in input_batches:
        yield batch.to_pandas()


def _map_in_pandas_arrow_batches(
    input_batches: Iterator[Any],
    *,
    user_func: Callable[[Any], Any],
) -> Iterator[Any]:
    """Adapt a pandas iterator to the mapInArrow callback contract."""
    import pyarrow as pa

    out = user_func(_pandas_frames_from_arrow_batches(input_batches))
    # None must fail. Treating it as empty would silently drop all output.
    if out is None:
        raise PySparkException(
            "mapInPandas user function must return an iterator of pandas.DataFrame (got None)"
        )
    for pdf in out:
        table = pa.Table.from_pandas(pdf, preserve_index=False)
        output_batches = table.to_batches()
        if not output_batches:
            # Preserve a zero-row batch so schema validation still runs.
            yield pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in table.schema],
                schema=table.schema,
            )
        else:
            yield from output_batches


def _pandas_nullable_dtype_for_arrow(arrow_type: Any) -> Any:
    """Return a pandas nullable dtype for an Arrow type when supported."""
    import pandas as pd
    import pyarrow.types as pat

    if pat.is_int8(arrow_type):
        return pd.Int8Dtype()
    if pat.is_int16(arrow_type):
        return pd.Int16Dtype()
    if pat.is_int32(arrow_type):
        return pd.Int32Dtype()
    if pat.is_int64(arrow_type):
        return pd.Int64Dtype()
    if pat.is_uint8(arrow_type):
        return pd.UInt8Dtype()
    if pat.is_uint16(arrow_type):
        return pd.UInt16Dtype()
    if pat.is_uint32(arrow_type):
        return pd.UInt32Dtype()
    if pat.is_uint64(arrow_type):
        return pd.UInt64Dtype()
    if pat.is_boolean(arrow_type):
        return pd.BooleanDtype()
    if pat.is_float32(arrow_type):
        return pd.Float32Dtype()
    if pat.is_float64(arrow_type):
        return pd.Float64Dtype()
    return None


def _arrow_array_to_pandas_series(array: Any) -> Any:
    """Convert an Arrow array while preserving nullable numeric and boolean dtypes."""
    import pyarrow as pa

    try:
        return array.to_pandas(types_mapper=_pandas_nullable_dtype_for_arrow)
    except (TypeError, ValueError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return array.to_pandas()


def _pandas_udf_series_args_for_slot(batch: Any, slot: dict[str, Any]) -> list[Any]:
    """Build pandas Series arguments for one UDF slot and Arrow batch."""
    series_args: list[Any] = []
    for input_name in slot["input_inter_names"]:
        if input_name not in batch.schema.names:
            raise PySparkException(
                "pandas_udf input column missing from streamed batch: "
                f"{input_name!r}; batch fields={list(batch.schema.names)}"
            )
        series_args.append(_arrow_array_to_pandas_series(batch.column(input_name)))
    return series_args


def _validate_pandas_udf_series_result(
    result: Any,
    *,
    function_name: str,
    expected_rows: int,
) -> Any:
    """Validate a pandas UDF Series result and its row count."""
    import pandas as pd

    if result is None:
        raise PySparkException(
            f"pandas_udf {function_name!r} must return a pandas.Series (got None)"
        )
    # Coercion can turn a scalar, dict, or set into a wrong-length result.
    if not isinstance(result, pd.Series):
        raise PySparkException(
            f"pandas_udf {function_name!r} must return a pandas.Series; got {type(result).__name__}"
        )
    if len(result) != expected_rows:
        raise PySparkException(
            f"pandas_udf {function_name!r} returned {len(result)} "
            f"values; expected {expected_rows} (one per input row)"
        )
    return result


def _pandas_udf_series_to_arrow(result: Any, slot: dict[str, Any], expected_arrow: Any) -> Any:
    """Convert one pandas UDF Series to its declared Arrow field type."""
    import pyarrow as pa

    out_name = slot["out_name"]
    field = expected_arrow.field(out_name)
    try:
        return pa.Array.from_pandas(result, type=field.type, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError) as error:
        raise PySparkException(
            f"pandas_udf {slot['function_name']!r} failed converting result "
            f"to declared type {field.type} "
            f"({slot['return_type_sql']}): {error}"
        ) from error


def _run_pandas_udf_scalar_on_batch(batch: Any, slot: dict[str, Any]) -> Any:
    """Run one scalar pandas UDF on an Arrow batch."""
    series_args = _pandas_udf_series_args_for_slot(batch, slot)
    try:
        result = slot["user_func"](*series_args)
    except PySparkException:
        raise
    except Exception as error:
        detail = traceback.format_exc()
        raise PySparkException(
            f"pandas_udf {slot['function_name']!r} raised {type(error).__name__}: {error}\n{detail}"
        ) from error
    return _validate_pandas_udf_series_result(
        result,
        function_name=slot["function_name"],
        expected_rows=batch.num_rows,
    )


def _pandas_udf_scalar_iter_inputs(
    batch_list: list[Any],
    slot: dict[str, Any],
) -> Iterator[Any]:
    """Yield scalar-iterator UDF inputs for buffered Arrow batches."""
    for batch in batch_list:
        series_args = _pandas_udf_series_args_for_slot(batch, slot)
        if len(series_args) == 1:
            yield series_args[0]
        else:
            yield tuple(series_args)


def _run_pandas_udf_scalar_iter(batch_list: list[Any], slot: dict[str, Any]) -> list[Any]:
    """Run and validate one SCALAR_ITER pandas UDF."""
    try:
        out_iter = slot["user_func"](_pandas_udf_scalar_iter_inputs(batch_list, slot))
    except PySparkException:
        raise
    except Exception as error:
        detail = traceback.format_exc()
        raise PySparkException(
            f"pandas_udf {slot['function_name']!r} raised {type(error).__name__}: {error}\n{detail}"
        ) from error
    if out_iter is None:
        raise PySparkException(
            f"pandas_udf {slot['function_name']!r} (SCALAR_ITER) must return "
            "an iterator of pandas.Series (got None)"
        )
    try:
        results = list(out_iter)
    except PySparkException:
        raise
    except Exception as error:
        detail = traceback.format_exc()
        raise PySparkException(
            "pandas_udf "
            f"{slot['function_name']!r} raised {type(error).__name__} while "
            f"consuming SCALAR_ITER output: {error}\n{detail}"
        ) from error
    if len(results) != len(batch_list):
        raise PySparkException(
            f"pandas_udf {slot['function_name']!r} (SCALAR_ITER) yielded "
            f"{len(results)} Series; expected {len(batch_list)} "
            "(one Series per input batch)"
        )
    validated: list[Any] = []
    for batch, result in zip(batch_list, results, strict=True):
        validated.append(
            _validate_pandas_udf_series_result(
                result,
                function_name=slot["function_name"],
                expected_rows=batch.num_rows,
            )
        )
    return validated


def _emit_pandas_udf_batch(
    batch: Any,
    pudf_series_by_slot: dict[int, Any],
    *,
    slots: list[dict[str, Any]],
    expected_arrow: Any,
) -> Any:
    """Assemble one output Arrow batch from pass-through and UDF columns."""
    import pyarrow as pa

    arrays: list[Any] = []
    names: list[str] = []
    for slot_index, slot in enumerate(slots):
        out_name = slot["out_name"]
        if slot["kind"] == "pass":
            arrays.append(batch.column(slot["inter_name"]))
            names.append(out_name)
            continue
        arrays.append(
            _pandas_udf_series_to_arrow(pudf_series_by_slot[slot_index], slot, expected_arrow)
        )
        names.append(out_name)
    return pa.RecordBatch.from_arrays(arrays, names=names)


def _run_pandas_udf_arrow_batches(
    input_batches: Iterator[Any],
    *,
    slots: list[dict[str, Any]],
    expected_arrow: Any,
    needs_scalar_iter: bool,
) -> Iterator[Any]:
    """Run scalar pandas UDF projections over Arrow batches."""
    from repark.spark.functions_udf import PandasUDFType

    try:
        __import__("pandas")
    except ImportError as error:
        raise ImportError("pandas_udf requires pandas (pip install 'repark[pandas]')") from error

    # SCALAR_ITER needs the full stream once. Pure SCALAR remains streaming.
    if needs_scalar_iter:
        batch_list = list(input_batches)
        if not batch_list:
            return
        per_slot_results: dict[int, list[Any]] = {}
        for slot_index, slot in enumerate(slots):
            if slot["kind"] != "pudf":
                continue
            if slot.get("function_type") == PandasUDFType.SCALAR_ITER:
                per_slot_results[slot_index] = _run_pandas_udf_scalar_iter(batch_list, slot)
            else:
                per_slot_results[slot_index] = [
                    _run_pandas_udf_scalar_on_batch(batch, slot) for batch in batch_list
                ]
        for batch_index, batch in enumerate(batch_list):
            pudf_series = {
                slot_index: results[batch_index] for slot_index, results in per_slot_results.items()
            }
            yield _emit_pandas_udf_batch(
                batch, pudf_series, slots=slots, expected_arrow=expected_arrow
            )
        return

    for batch in input_batches:
        pudf_series: dict[int, Any] = {}
        for slot_index, slot in enumerate(slots):
            if slot["kind"] != "pudf":
                continue
            pudf_series[slot_index] = _run_pandas_udf_scalar_on_batch(batch, slot)
        yield _emit_pandas_udf_batch(batch, pudf_series, slots=slots, expected_arrow=expected_arrow)


def _column_python_values(batch: Any, name: str) -> list[Any]:
    """Return one Arrow column as Python values."""
    if name not in batch.schema.names:
        raise PySparkException(
            "udf input column missing from streamed batch: "
            f"{name!r}; batch fields={list(batch.schema.names)}"
        )
    return batch.column(name).to_pylist()


def _run_python_udf_on_batch(batch: Any, slot: dict[str, Any]) -> list[Any]:
    """Run one classic per-row Python UDF on an Arrow batch."""
    input_columns = [
        _column_python_values(batch, input_name) for input_name in slot["input_inter_names"]
    ]
    row_count = batch.num_rows
    user_func = slot["user_func"]
    function_name = slot["function_name"]
    results: list[Any] = []
    try:
        if not input_columns:
            for _ in range(row_count):
                results.append(user_func())
        else:
            for row_index in range(row_count):
                args = [column[row_index] for column in input_columns]
                results.append(user_func(*args))
    except PySparkException:
        raise
    except Exception as error:
        detail = traceback.format_exc()
        raise PySparkException(
            f"udf {function_name!r} raised {type(error).__name__}: {error}\n{detail}"
        ) from error
    if len(results) != row_count:
        raise PySparkException(
            f"udf {function_name!r} produced {len(results)} values; "
            f"expected {row_count} (one per input row)"
        )
    return results


def _python_udf_results_to_arrow(
    results: list[Any],
    slot: dict[str, Any],
    expected_arrow: Any,
) -> Any:
    """Convert classic UDF results to the declared Arrow field type."""
    import pyarrow as pa

    out_name = slot["out_name"]
    field = expected_arrow.field(out_name)
    coerced = results
    # Decimal results accept ordinary numeric Python values.
    if pa.types.is_decimal(field.type):
        from decimal import Decimal, InvalidOperation

        converted: list[Any] = []
        for value in results:
            if value is None:
                converted.append(None)
                continue
            if isinstance(value, Decimal):
                converted.append(value)
                continue
            if isinstance(value, int | float) and not isinstance(value, bool):
                try:
                    converted.append(Decimal(str(value)))
                except (InvalidOperation, ValueError) as error:
                    raise PySparkException(
                        f"udf {slot['function_name']!r} failed converting result "
                        f"to declared type {field.type} "
                        f"({slot['return_type_sql']}): {error}"
                    ) from error
                continue
            converted.append(value)
        coerced = converted
    try:
        return pa.array(coerced, type=field.type, from_pandas=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError) as error:
        raise PySparkException(
            f"udf {slot['function_name']!r} failed converting result "
            f"to declared type {field.type} "
            f"({slot['return_type_sql']}): {error}"
        ) from error


def _run_python_udf_arrow_batches(
    input_batches: Iterator[Any],
    *,
    slots: list[dict[str, Any]],
    expected_arrow: Any,
) -> Iterator[Any]:
    """Run classic Python UDF projections over Arrow batches."""
    import pyarrow as pa

    for batch in input_batches:
        arrays: list[Any] = []
        names: list[str] = []
        for slot in slots:
            out_name = slot["out_name"]
            if slot["kind"] == "pass":
                arrays.append(batch.column(slot["inter_name"]))
                names.append(out_name)
                continue
            row_results = _run_python_udf_on_batch(batch, slot)
            arrays.append(_python_udf_results_to_arrow(row_results, slot, expected_arrow))
            names.append(out_name)
        yield pa.RecordBatch.from_arrays(arrays, names=names)


def _apply_ordered_window_pandas_udf(
    pdf: Any,
    *,
    specs: list[dict[str, Any]],
    order_cols: list[str],
    start_bound: int | None,
    end_bound: int | None,
    struct_fields: list[StructField],
) -> Any:
    """Run ordered window GROUPED_AGG pandas UDFs on one partition."""
    try:
        import pandas as pd
    except ImportError as error:
        raise ImportError(
            "windowed pandas_udf requires pandas (pip install 'repark[pandas]')"
        ) from error

    if len(pdf) == 0:
        return pd.DataFrame(columns=[field.name for field in struct_fields])
    # Stable sorting keeps equal order keys in input order.
    sort_by = [name for name in order_cols if name in pdf.columns]
    if sort_by:
        pdf = pdf.sort_values(by=sort_by, kind="mergesort").reset_index(drop=True)
    else:
        pdf = pdf.reset_index(drop=True)
    n_rows = len(pdf)
    for spec in specs:
        results: list[Any] = []
        for row_index in range(n_rows):
            lo = 0 if start_bound is None else max(0, row_index + int(start_bound))
            hi = n_rows if end_bound is None else min(n_rows, row_index + int(end_bound) + 1)
            if lo >= hi:
                results.append(None)
                continue
            frame_pdf = pdf.iloc[lo:hi]
            series_args: list[Any] = []
            for input_name in spec["input_names"]:
                if input_name not in frame_pdf.columns:
                    raise PySparkException(
                        f"windowed pandas_udf input column missing from frame: {input_name!r}"
                    )
                series_args.append(frame_pdf[input_name])
            try:
                value = spec["user_func"](*series_args)
            except PySparkException:
                raise
            except Exception as error:
                detail = traceback.format_exc()
                raise PySparkException(
                    "windowed GROUPED_AGG pandas_udf "
                    f"{spec['function_name']!r} raised {type(error).__name__}: "
                    f"{error}\n{detail}"
                ) from error
            if isinstance(value, pd.Series):
                raise PySparkException(
                    f"GROUPED_AGG pandas_udf {spec['function_name']!r} must return a "
                    f"scalar; got pandas.Series (length {len(value)})"
                )
            if isinstance(value, pd.DataFrame):
                raise PySparkException(
                    f"GROUPED_AGG pandas_udf {spec['function_name']!r} must return a "
                    f"scalar; got pandas.DataFrame"
                )
            results.append(value)
        pdf[spec["out_name"]] = results
    return pdf[[field.name for field in struct_fields]]
