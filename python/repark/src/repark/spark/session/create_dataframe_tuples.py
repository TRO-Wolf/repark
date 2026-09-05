"""createDataFrame tuple-to-Arrow conversion."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.spark._idents import quote_ident as _quote_ident

from repark.errors import AnalysisException, PySparkTypeError


if TYPE_CHECKING:
    from repark.spark.session.create_dataframe_inference import (
        _INFER_NESTED_DICT_AS_STRUCT,
        _LEGACY_FIRST_ELEMENT_COERCE,
        _infer_arrow_type_from_python_sample,
        _infer_struct_arrow_from_dict_samples,
        _merge_inferred_arrow_types,
        _prepare_nested_cell,
        _sql_type_to_arrow,
        _validate_decimal_envelope,
    )
    from repark.spark.session.create_dataframe_values import _sql_literal


_SPARK_SCALAR_MERGE_LABELS: dict[str, str] = {
    "boolean": "BooleanType",
    "long": "LongType",
    "double": "DoubleType",
    "decimal": "DecimalType",
    "timestamp": "TimestampType",
    "date": "DateType",
}


_SPARK_SCALAR_MERGE_KIND_ORDER: tuple[str, ...] = (
    "boolean",
    "long",
    "double",
    "decimal",
    "date",
    "timestamp",
)


def _python_scalar_merge_kind(cell: Any) -> str | None:
    """Spark-merge kind for a scalar cell, or ``None`` if not in the merge-checked set.



    ``bool`` is checked before ``int`` (``isinstance(True, int)`` is true in Python).

    ``datetime`` is checked before ``date`` (datetime is a date subclass).

    Infinite floats refuse immediately (createDataFrame does not support them).

    """

    import datetime as _dt

    from decimal import Decimal as _Decimal

    if cell is None:
        return None

    if isinstance(cell, bool):
        return "boolean"

    if isinstance(cell, int):
        return "long"

    if isinstance(cell, float):
        if cell == float("inf") or cell == float("-inf"):
            raise PySparkTypeError("createDataFrame does not support infinite float values")

        return "double"

    if isinstance(cell, _Decimal):
        return "decimal"

    if isinstance(cell, _dt.datetime):
        return "timestamp"

    if isinstance(cell, _dt.date):
        return "date"

    return None


def _refuse_incompatible_scalar_merge_kinds(kinds: set[str], *, column_name: str) -> None:
    """Spark ``CANNOT_MERGE_TYPE`` when two merge-checked scalar kinds co-occur."""

    present = [kind for kind in _SPARK_SCALAR_MERGE_KIND_ORDER if kind in kinds]

    if len(present) < 2:
        return

    left = _SPARK_SCALAR_MERGE_LABELS[present[0]]

    right = _SPARK_SCALAR_MERGE_LABELS[present[1]]

    raise PySparkTypeError(
        f"[CANNOT_MERGE_TYPE] Can not merge type {left} and {right} (column {column_name!r})"
    )


def _refuse_long_double_merge(
    tuples: list[tuple[Any, ...]],
    column_index: int,
    column_name: str,
) -> None:
    """Refuse Spark ``CANNOT_MERGE_TYPE`` on inferred scalar columns (r21 T1 / extra octo).



    Live Spark 4.1.2 rejects mixed Boolean/Long/Double/Decimal/Date/Timestamp on the same

    inferred field rather than truncating or silently promoting. Covers:



    * Long + Double (``int(2.5)`` silent truncate — critic-octo C1-L1)

    * Long + Decimal (``Decimal("2.5")`` → 2 via ``pa.array`` — EXTRA XC1-L1)

    * Decimal + Long / Decimal + Double / Double + Decimal (EXTRA XC1-L2)

    * Double + Boolean (``True`` → 1.0 via Arrow — EXTRA XC1-L3)

    * Long + Boolean / Boolean + Long (EXTRA XC1-L4)

    * Timestamp/Date + Long/Double (epoch coercion via ``pa.array`` — EXTRA XC2-L1/L2)

    * Date + Timestamp (EXTRA XC2-L3)



    Nested list/map element conflicts are enforced in :func:`_prepare_nested_cell` and, for

    list-of-scalar columns, :func:`_refuse_list_element_type_merge`.

    """

    kinds: set[str] = set()

    for row in tuples:
        kind = _python_scalar_merge_kind(row[column_index])

        if kind is None:
            continue

        kinds.add(kind)

        _refuse_incompatible_scalar_merge_kinds(kinds, column_name=column_name)


def _refuse_list_element_type_merge(
    tuples: list[tuple[Any, ...]],
    column_index: int,
    column_name: str,
) -> None:
    """Refuse Boolean/Long/Double/Decimal mix among list-of-scalar elements (Spark merge)."""

    kinds: set[str] = set()

    for row in tuples:
        cell = row[column_index]

        if not isinstance(cell, list):
            continue

        for item in cell:
            kind = _python_scalar_merge_kind(item)

            if kind is None:
                continue

            kinds.add(kind)

            _refuse_incompatible_scalar_merge_kinds(kinds, column_name=column_name)


def _refuse_duplicate_tuple_column_names(names: list[str]) -> None:
    """Fail loud when createDataFrame tuple columns repeat a name."""
    # Exact-duplicate names were rejected by the VALUES planner; keep fail-loud.

    if len(names) != len(set(names)):
        from repark.errors import AnalysisException

        raise AnalysisException(
            "unique expression names required; createDataFrame schema has duplicate column names"
        )


def _arrow_type_for_typed_null_sql(null_sql: str) -> Any:
    """Map one all-null column CAST to its Arrow type (``CAST(NULL AS TYPE)`` → TYPE)."""

    upper = null_sql.upper()

    # CAST(NULL AS TYPE) → TYPE (keep DECIMAL(p,s) parens intact).

    if " AS " in upper:
        type_sql = upper.rsplit(" AS ", 1)[-1].strip()

        if type_sql.endswith(")") and type_sql.count("(") < type_sql.count(")"):
            type_sql = type_sql[:-1].strip()

    else:
        type_sql = upper

    return _sql_type_to_arrow(type_sql)


def _pa_array_or_refuse(values: list[Any], arrow_type: Any, column_name: str) -> Any:
    """Build one Arrow array, refusing Arrow failures as ``PySparkTypeError``."""

    import pyarrow as pa

    try:
        return pa.array(values, type=arrow_type)

    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as error:
        # Match VALUES-path loud refusals (decimal scale, unsupported casts, …).

        raise PySparkTypeError(
            f"createDataFrame cannot build Arrow column {column_name!r}: {error}"
        ) from error


def _arrow_table_from_tuples(
    names: list[str],
    tuples: list[tuple[Any, ...]],
    *,
    column_null_sql: list[str] | None,
    engine_types: list[str] | None,
) -> Any:
    """Build a ``pyarrow.Table`` from row tuples with declared/inferred Arrow types."""

    import pyarrow as pa

    width = len(names)

    if engine_types is not None:
        arrow_types = [_sql_type_to_arrow(sql_type) for sql_type in engine_types]

    else:
        # Infer from first non-null cell per column (Python types → Arrow). All-null
        # columns fall back to column_null_sql CAST type when provided, else string
        # (matches VALUES-path default VARCHAR for untyped nulls).

        arrow_types = []

        for column_index in range(width):
            sample = next(
                (row[column_index] for row in tuples if row[column_index] is not None),
                None,
            )

            if sample is None:
                if column_null_sql is not None and column_index < len(column_null_sql):
                    arrow_types.append(
                        _arrow_type_for_typed_null_sql(column_null_sql[column_index])
                    )

                else:
                    arrow_types.append(pa.string())

            elif isinstance(sample, bool):
                # Boolean + Long/Double/Decimal → CANNOT_MERGE_TYPE.

                _refuse_long_double_merge(tuples, column_index, names[column_index])

                arrow_types.append(pa.bool_())

            elif isinstance(sample, int) and not isinstance(sample, bool):
                # Spark 4.1.2: LongType + Double/Decimal/Boolean cannot merge (CANNOT_MERGE_TYPE);
                # int(2.5) / Decimal("2.5") would otherwise truncate silently via pa.array.

                _refuse_long_double_merge(tuples, column_index, names[column_index])

                arrow_types.append(pa.int64())

            elif isinstance(sample, float):
                if sample == float("inf") or sample == float("-inf"):
                    raise PySparkTypeError("createDataFrame does not support infinite float values")

                # Symmetric: first-float-then-int/Decimal/bool also refuses on Spark.

                _refuse_long_double_merge(tuples, column_index, names[column_index])

                arrow_types.append(pa.float64())

            elif isinstance(sample, list):
                # Dense ML vector: non-empty float-only list → FixedSizeList[n].
                # General arrays (int lists, empty-first, nested) → variable list_.
                # List-of-scalar Long+Double/Decimal/Boolean refuse before element infer.

                _refuse_list_element_type_merge(tuples, column_index, names[column_index])

                if sample and all(isinstance(item, float) for item in sample):
                    arrow_types.append(pa.list_(pa.float64(), len(sample)))

                else:
                    # Under the nested-dict-as-struct conf: pure list-of-dict → struct field
                    # union; nested list elements merge via _merge_inferred_arrow_types.

                    if _INFER_NESTED_DICT_AS_STRUCT.get():
                        dict_elements: list[dict[str, Any]] = []

                        saw_non_dict = False

                        for row in tuples:
                            cell = row[column_index]

                            if not isinstance(cell, list):
                                continue

                            for item in cell:
                                if item is None:
                                    continue

                                if isinstance(item, dict):
                                    dict_elements.append(item)

                                    if _LEGACY_FIRST_ELEMENT_COERCE.get():
                                        break

                                else:
                                    saw_non_dict = True

                            if _LEGACY_FIRST_ELEMENT_COERCE.get() and dict_elements:
                                break

                        if dict_elements and not saw_non_dict:
                            arrow_types.append(
                                pa.list_(_infer_struct_arrow_from_dict_samples(dict_elements))
                            )

                            continue

                        if saw_non_dict:
                            element_types: list[Any] = []

                            for row in tuples:
                                cell = row[column_index]

                                if not isinstance(cell, list):
                                    continue

                                for item in cell:
                                    if item is None:
                                        continue

                                    element_types.append(_infer_arrow_type_from_python_sample(item))

                                    if _LEGACY_FIRST_ELEMENT_COERCE.get():
                                        break

                                if _LEGACY_FIRST_ELEMENT_COERCE.get() and element_types:
                                    break

                            if element_types:
                                merged_element = element_types[0]

                                for element_type in element_types[1:]:
                                    merged_element = _merge_inferred_arrow_types(
                                        merged_element, element_type
                                    )

                                arrow_types.append(pa.list_(merged_element))

                                continue

                    element_sample = next(
                        (item for item in sample if item is not None),
                        None,
                    )

                    if element_sample is None:
                        # All-empty / all-null elements — scan other rows for a witness.

                        for row in tuples:
                            cell = row[column_index]

                            if isinstance(cell, list):
                                element_sample = next(
                                    (item for item in cell if item is not None),
                                    None,
                                )

                                if element_sample is not None:
                                    break

                    arrow_types.append(
                        pa.list_(_infer_arrow_type_from_python_sample(element_sample))
                    )

            elif type(sample).__name__ == "Row" and type(sample).__module__.startswith("repark"):
                # Nested Row → struct (Spark createDataFrame inference).

                arrow_types.append(_infer_arrow_type_from_python_sample(sample))

            elif isinstance(sample, tuple):
                # Nested bare tuple → struct<_1,_2,…> (Spark createDataFrame; Apache
                # test_print_schema). Must not fall through to str(tuple).

                arrow_types.append(_infer_arrow_type_from_python_sample(sample))

            elif (
                isinstance(sample, dict)
                and set(sample.keys()) == {"size", "indices", "values"}
                and isinstance(sample.get("size"), int)
                and not isinstance(sample.get("size"), bool)
                and isinstance(sample.get("indices"), (list, tuple))
                and isinstance(sample.get("values"), (list, tuple))
            ):
                # Sparse ML vector struct. Exact key set + value shapes only — a plain map
                # that happens to contain a "size" key must stay map.

                arrow_types.append(
                    pa.struct(
                        [
                            ("size", pa.int32()),
                            ("indices", pa.list_(pa.int32())),
                            ("values", pa.list_(pa.float64())),
                        ]
                    )
                )

            elif isinstance(sample, dict):
                # Conf true → multi-row struct field union (null-fill missing); the
                # sparse-vector exact-key branch above is conf-invariant.

                if _INFER_NESTED_DICT_AS_STRUCT.get():
                    dict_samples: list[dict[str, Any]] = [
                        cell for row in tuples if isinstance((cell := row[column_index]), dict)
                    ]

                    arrow_types.append(
                        _infer_struct_arrow_from_dict_samples(dict_samples or [sample])
                    )

                    continue

                # Plain dict → map<key, value>. Empty / null-only first sample: scan later
                # rows for a witness with a concrete non-null value so
                # ``[{}, {"a": None}, {"a": 1}]`` becomes map<string,bigint> not
                # map<string,string> with ``"1"`` (Apache test_infer_map_pair_type_empty order).

                map_sample = sample

                needs_value_witness = (not map_sample) or all(
                    value is None for value in map_sample.values()
                )

                if needs_value_witness:
                    for row in tuples:
                        cell = row[column_index]

                        if (
                            isinstance(cell, dict)
                            and cell
                            and any(value is not None for value in cell.values())
                        ):
                            map_sample = cell

                            break

                    # No concrete value in any row: fall back to first non-empty (null-only
                    # values) so empty→null still builds map<string,string>.

                    if not map_sample:
                        for row in tuples:
                            cell = row[column_index]

                            if isinstance(cell, dict) and cell:
                                map_sample = cell

                                break

                arrow_types.append(_infer_arrow_type_from_python_sample(map_sample))

            else:
                import datetime as _dt

                from decimal import Decimal as _Decimal

                if isinstance(sample, _dt.datetime):
                    # Timestamp + Long/Double/Date refuse — no epoch coercion.
                    # Default TIMESTAMP follows spark.sql.timestampType.

                    from repark.spark.session.timestamp_type import default_timestamp_arrow_type

                    _refuse_long_double_merge(tuples, column_index, names[column_index])

                    arrow_types.append(default_timestamp_arrow_type())

                elif isinstance(sample, _dt.date):
                    # Date + Long/Timestamp refuse — no day-epoch coercion.

                    _refuse_long_double_merge(tuples, column_index, names[column_index])

                    arrow_types.append(pa.date32())

                elif isinstance(sample, _dt.time):
                    # time-of-day → string until engine TIME type is wired end-to-end.

                    arrow_types.append(pa.string())

                elif isinstance(sample, _Decimal):
                    # Decimal + Long/Double/Boolean refuse; not silent promote.

                    _refuse_long_double_merge(tuples, column_index, names[column_index])

                    arrow_types.append(pa.decimal128(38, 18))

                elif isinstance(sample, (bytes, bytearray, memoryview)):
                    arrow_types.append(pa.binary())

                else:
                    arrow_types.append(pa.string())

    _refuse_duplicate_tuple_column_names(names)

    # Validate the decimal envelope before Arrow build (the VALUES path enforces it too).

    from decimal import Decimal as _Decimal

    for row in tuples:
        for cell in row:
            if isinstance(cell, _Decimal):
                _validate_decimal_envelope(cell)

    columns: list[Any] = []

    for column_index in range(width):
        values = [row[column_index] for row in tuples]

        arrow_type = arrow_types[column_index]

        # Dense vector: refuse mixed FixedSizeList widths.

        if pa.types.is_fixed_size_list(arrow_type):
            expected_width = arrow_type.list_size

            for row_index, cell in enumerate(values):
                if cell is None:
                    continue

                if not isinstance(cell, list):
                    raise PySparkTypeError(
                        f"createDataFrame column {names[column_index]!r}: expected dense "
                        f"float list of width {expected_width}, got {type(cell).__name__}"
                    )

                if len(cell) != expected_width:
                    from repark.errors import AnalysisException

                    raise AnalysisException(
                        f"repark.ml v1 vector columns are fixed-width only; column "
                        f"{names[column_index]!r} has mixed widths "
                        f"(expected {expected_width}, row {row_index} has {len(cell)}). "
                        f"Do not fall back to List<Float64> — that silently loses the "
                        f"width guarantee (dense FixedSizeList only; see repark.ml.linalg)."
                    )

                values[row_index] = [float(item) for item in cell]

        # Sparse ML vector reshape: only exact three-field sparse layout
        # ``{size,indices,values}`` — never any struct that merely contains an ``indices`` field.

        if pa.types.is_struct(arrow_type) and {field.name for field in arrow_type} == {
            "size",
            "indices",
            "values",
        }:
            for row_index, cell in enumerate(values):
                if cell is None:
                    continue

                if not isinstance(cell, dict):
                    raise PySparkTypeError(
                        f"createDataFrame column {names[column_index]!r}: expected sparse "
                        f"vector struct dict, got {type(cell).__name__}"
                    )

                values[row_index] = {
                    "size": int(cell["size"]),
                    "indices": [int(item) for item in cell["indices"]],
                    "values": [float(item) for item in cell["values"]],
                }

        # Coerce / reshape cells to the target Arrow type (nested + stringified schema).

        values = [_prepare_nested_cell(cell, arrow_type) for cell in values]

        columns.append(_pa_array_or_refuse(values, arrow_type, names[column_index]))

    return pa.Table.from_arrays(columns, names=names)


def _values_sql_with_explicit_casts(
    names: list[str],
    tuples: list[tuple[Any, ...]],
    *,
    engine_types: list[str],
) -> str:
    """VALUES list with per-cell ``CAST(… AS type)`` so explicit schema types stick (int32)."""

    value_rows: list[str] = []

    for row in tuples:
        cells: list[str] = []

        for cell, sql_type in zip(row, engine_types, strict=True):
            if cell is None:
                cells.append(f"CAST(NULL AS {sql_type})")

            else:
                cells.append(f"CAST({_sql_literal(cell)} AS {sql_type})")

        value_rows.append("(" + ", ".join(cells) + ")")

    values_sql = ", ".join(value_rows)

    alias_cols = ", ".join(_quote_ident(name) for name in names)

    return f"SELECT * FROM (VALUES {values_sql}) AS t({alias_cols})"
