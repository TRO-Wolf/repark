from __future__ import annotations

import collections
import datetime as dt
import json
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import numpy as np
import pandas as pd
import pyarrow as pa

from repark import functions as F  # noqa: N812
from repark.spark.functions import PandasUDFType, pandas_udf
from repark.spark.window import Window

NS = 1767323045123456789
NS2 = 1767323045000000001
NS_ARRAY = np.array([NS, NS2], dtype="datetime64[ns]")
US_ARRAY = NS_ARRAY.astype("datetime64[us]")
MS_ARRAY = NS_ARRAY.astype("datetime64[ms]")
WHOLE_NS_ARRAY = np.array([NS - 789, NS2 - 1], dtype="datetime64[ns]")
EMPTY_NS_ARRAY = np.array([], dtype="datetime64[ns]")
NAT_NS_ARRAY = np.array(["NaT", "NaT"], dtype="datetime64[ns]")
EMPTY_US_ARRAY = np.array([], dtype="datetime64[us]")
NAT_US_ARRAY = np.array(["NaT"], dtype="datetime64[us]")
STAMPS = [pd.Timestamp(NS), pd.Timestamp(NS2)]
DATETIMES = [dt.datetime(2026, 1, 2, 3, 4, 5, 123456), dt.datetime(2026, 1, 2, 3, 4, 5)]
ZONED_DATETIMES = [value.replace(tzinfo=dt.UTC) for value in DATETIMES]
NTZ = "array<timestamp_ntz>"
LTZ = "array<timestamp>"
NESTED = "array<array<timestamp_ntz>>"
STRUCT = "struct<a:array<timestamp_ntz>>"
MAP = "map<string,array<timestamp_ntz>>"
NS_LIST = pa.list_(pa.timestamp("ns"))
GROUPED = "grouped"
COGROUPED = "cogrouped"
COGROUPED_RIGHT = "cogrouped_right"
GROUPED_AGG = "grouped_agg"
ORDERED_WINDOW = "ordered_window"
OTHER_ZONE = "America/New_York"
LARGE_RESULT_ROWS = 40
ARRAY = "array"
INTEGER = "integer"
MAIN = "main"
FIXTURE = Path(__file__).with_name("ns_array_apply_in_pandas_refuse_1_main.json")
SELECT_VALUE = "SELECT id, {expression} AS v FROM {source}"


class UnreadableCell:
    """A cell whose iteration raises."""

    def __iter__(self) -> Any:
        """Raise on the first read."""
        raise RuntimeError("unreadable cell")


def ns_array(*ticks: Any) -> np.ndarray:
    """Return a numpy nanosecond array of the given ticks."""
    return np.array(ticks, dtype="datetime64[ns]")


def object_array(items: list[Any]) -> np.ndarray:
    """Return a numpy object array holding exactly the given items."""
    cells = np.empty(len(items), dtype=object)
    for index, item in enumerate(items):
        cells[index] = item
    return cells


def object_series(items: list[Any]) -> pd.Series:
    """Return an object Series holding exactly the given cells."""
    return pd.Series(object_array(items))


def arrow_series(cells: list[Any], arrow_type: pa.DataType) -> pd.Series:
    """Return an ArrowDtype Series of the given cells."""
    return pd.Series(pa.array(cells, type=arrow_type), dtype=pd.ArrowDtype(arrow_type))


def fixed_cells(key: int, cells: Any) -> Any:
    """Return the same cells for every group."""
    return cells


def cells_by_group(key: int, first: Any, others: Any) -> Any:
    """Return one cell list for the first group and another for the rest."""
    return first if key == 1 else others


def user_result(pdf: pd.DataFrame, cells_of: Callable[[int], Any]) -> pd.DataFrame:
    """Return a result frame holding the case cells for the group key."""
    key = int(pdf["id"].iloc[0])
    cells = cells_of(key)
    if isinstance(cells, pd.DataFrame):
        return cells
    series = cells if isinstance(cells, pd.Series) else object_series(cells)
    return pd.DataFrame({"id": pd.Series([key] * len(series), dtype="int32"), "v": series})


def duplicate_label_frame(key: int) -> pd.DataFrame:
    """Return a result whose value label appears twice, each holding a nanosecond array."""
    keys = pd.DataFrame({"id": pd.Series([key], dtype="int32")})
    value = pd.DataFrame({"v": object_series([NS_ARRAY])})
    return pd.concat([keys, value, value], axis=1)


def integer_label_frame(key: int) -> pd.DataFrame:
    """Return a result whose value label is the integer zero, holding a nanosecond array."""
    return pd.DataFrame({"id": pd.Series([key], dtype="int32"), 0: object_series([NS_ARRAY])})


def identity(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the group frame unchanged."""
    return pdf


def drop_value(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return only the key column."""
    return pdf[["id"]]


def cast_arrays(pdf: pd.DataFrame) -> pd.DataFrame:
    """Cast every array cell to microseconds, as the array hint says."""
    cells = [cell.astype("datetime64[us]") for cell in pdf["v"]]
    return pdf.assign(v=object_series(cells))


def floor_divide(pdf: pd.DataFrame) -> pd.DataFrame:
    """Floor-divide every struct tick by 1000, as the integer hint says."""
    return pdf.assign(v=object_series([{"t": cell["t"] // 1000} for cell in pdf["v"]]))


def rebuild_structs(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return new dicts holding the same ticks."""
    return pdf.assign(v=object_series([{"t": cell["t"]} for cell in pdf["v"]]))


def structs_as_tuples(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return positional struct cells holding the same ticks."""
    return pdf.assign(v=object_series([(cell["t"],) for cell in pdf["v"]]))


def structs_as_timestamps(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return struct cells holding pandas timestamps of the ticks."""
    return pdf.assign(v=object_series([{"t": pd.Timestamp(cell["t"])} for cell in pdf["v"]]))


def add_one_tick(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return struct cells holding each tick plus one."""
    return pdf.assign(v=object_series([{"t": cell["t"] + 1} for cell in pdf["v"]]))


def ticks_as_integer_column(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the struct ticks as an integer column."""
    return pd.DataFrame({"id": pdf["id"], "v": [cell["t"] for cell in pdf["v"]]})


def ticks_as_typed_integer_column(pdf: pd.DataFrame, dtype: str) -> pd.DataFrame:
    """Return the struct ticks as an integer column of a pandas extension dtype."""
    ticks = pd.Series([cell["t"] for cell in pdf["v"]], dtype=dtype)
    return pd.DataFrame({"id": pdf["id"].to_numpy(), "v": ticks})


def ticks_as_arrow_integer_structs(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the struct ticks as an Arrow-typed struct column of integers."""
    arrow_type = pa.struct([("t", pa.int64())])
    cells = pa.array([{"t": cell["t"]} for cell in pdf["v"]], arrow_type)
    values = pd.Series(cells, dtype=pd.ArrowDtype(arrow_type))
    return pd.DataFrame({"id": pdf["id"].to_numpy(), "v": values})


def ticks_as_object_column(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the struct ticks as an object column."""
    return pd.DataFrame({"id": pdf["id"], "v": object_series([cell["t"] for cell in pdf["v"]])})


def ticks_as_lists(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return each struct tick inside a Python list."""
    return pd.DataFrame({"id": pdf["id"], "v": object_series([[cell["t"]] for cell in pdf["v"]])})


def maps_as_dicts(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return every map cell as a dict."""
    return pdf.assign(v=object_series([dict(cell) for cell in pdf["v"]]))


def later_timestamps(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return real timestamps a thousand times later, whose ticks equal the handed integers."""
    cells = [{"t": pd.Timestamp(cell["t"], unit="us")} for cell in pdf["v"]]
    return pdf.assign(v=object_series(cells))


def floor_first_row_only(pdf: pd.DataFrame) -> pd.DataFrame:
    """Floor-divide the first struct tick and return the others unchanged."""
    cells = [{"t": cell["t"]} for cell in pdf["v"]]
    cells[0] = {"t": cells[0]["t"] // 1000}
    return pdf.assign(v=object_series(cells))


def arrays_as_integer_lists(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the ticks of every nanosecond array as a Python list of integers."""
    cells = [cell.astype("int64").tolist() for cell in pdf["v"]]
    return pdf.assign(v=object_series(cells))


def column_as_integers(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the ticks of a nanosecond column as an integer column."""
    return pd.DataFrame({"id": pdf["id"], "v": pdf["v"].astype("int64")})


def column_floor_divided(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the microsecond floors of a nanosecond column as an integer column."""
    return pd.DataFrame({"id": pdf["id"], "v": pdf["v"].astype("int64") // 1000})


def other_group_tick(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return, as a function-made integer, the tick the other group's map holds."""
    tick = NS2 if int(pdf["id"].iloc[0]) == 1 else NS
    return pdf.assign(v=object_series([[("k", tick)]]))


def keep_floor_tick_rows(pdf: pd.DataFrame) -> pd.DataFrame:
    """Keep the rows whose tick is the microsecond floor of the other tick."""
    kept = [index for index, cell in enumerate(pdf["v"]) if cell["t"] == NS // 1000]
    return pdf.iloc[kept]


def first_cell(series: pd.Series) -> Any:
    """Return the first cell of a group."""
    return series.iloc[0]


def first_tick(series: pd.Series) -> Any:
    """Return the tick of the first struct cell of a group."""
    return series.iloc[0]["t"]


def count_cells(series: pd.Series) -> int:
    """Return the number of cells of a group."""
    return len(series)


def mean_of(series: pd.Series) -> float:
    """Return the mean of a window frame."""
    return float(series.mean())


def apply_to_left(left: pd.DataFrame, right: pd.DataFrame, function: Any) -> pd.DataFrame:
    """Apply a grouped function to the left cogroup frame."""
    return function(left)


def apply_to_right(left: pd.DataFrame, right: pd.DataFrame, function: Any) -> pd.DataFrame:
    """Apply a grouped function to the right cogroup frame."""
    return function(right)


def through_door(frame: Any, function: Any, schema: Any, door: str) -> pa.Table:
    """Run a grouped function over a frame through one applyInPandas door."""
    grouped = frame.groupBy("id")
    if door == GROUPED:
        return grouped.applyInPandas(function, schema).toArrow()
    if door == COGROUPED:
        applied = partial(apply_to_left, function=function)
        return grouped.cogroup(grouped).applyInPandas(applied, schema).toArrow()
    keys = frame.select("id").groupBy("id")
    applied = partial(apply_to_right, function=function)
    return keys.cogroup(grouped).applyInPandas(applied, schema).toArrow()


def run_user_case(
    spark: Any, door: str, declared: str, cells_of: Any, label: str = "v"
) -> pa.Table:
    """Run a case whose function builds its own result cells."""
    base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
    function = partial(user_result, cells_of=cells_of)
    return through_door(base, function, f"id int, {label} {declared}", door)


def empty_array() -> Any:
    """Return an array column of no elements."""
    return F.array()


def null_array() -> Any:
    """Return an array column of one untyped null."""
    return F.array(F.lit(None))


def array_of_column(name: str) -> Any:
    """Return an array column of one source column, spelled with the DataFrame functions."""
    return F.array(F.col(name))


def array_of_struct_column(name: str) -> Any:
    """Return an array column holding one struct of one source column."""
    return F.array(F.struct(F.col(name).alias("t")))


def map_of_value(expression: str) -> Any:
    """Return a map column from one string key to a source expression."""
    return F.create_map(F.lit("k"), F.expr(expression))


def map_of_key(expression: str) -> Any:
    """Return a map column from a source expression to one string value."""
    return F.create_map(F.expr(expression), F.lit("k"))


def array_of_map(expression: str) -> Any:
    """Return an array column holding one map from a string key to a source expression."""
    return F.array(map_of_value(expression))


def source_frame(spark: Any, query: Any) -> Any:
    """Return the frame of a SQL text or of a column built over the source table."""
    if isinstance(query, str):
        return spark.sql(query.format(source=doors.SRC))
    return spark.table(doors.SRC).select("id", query().alias("v"))


def run_table_case(spark: Any, door: str, query: Any, function: Any, schema: Any) -> pa.Table:
    """Run a case over the nanosecond source table."""
    frame = source_frame(spark, query)
    return through_door(frame, function, schema or frame.schema, door)


def run_frame_case(spark: Any, door: str, table: pa.Table, function: Any, schema: str) -> pa.Table:
    """Run a case over an Arrow table that keeps its types."""
    return through_door(doors.frame_of(spark, table), function, schema, door)


def run_grouped_agg_case(
    spark: Any, door: str, query: str, function: Any, returns: str
) -> pa.Table:
    """Run a GROUPED_AGG pandas UDF, which goes through the grouped door."""
    frame = source_frame(spark, query)
    aggregate = pandas_udf(function, returns, PandasUDFType.GROUPED_AGG)
    return frame.groupBy("id").agg(aggregate("v").alias("v")).toArrow()


def run_window_case(spark: Any, door: str, query: Any, selected: tuple[str, ...]) -> pa.Table:
    """Run an ordered-window pandas UDF, which passes every source column through the door."""
    frame = source_frame(spark, query)
    mean = pandas_udf(mean_of, "double", PandasUDFType.GROUPED_AGG)
    window = Window.partitionBy("id").orderBy("id")
    return frame.select(*selected, mean("id").over(window).alias("m")).toArrow()


def user_case(name: str, expect: str, declared: str, cells: Any) -> tuple[str, str, Any]:
    """Return a case whose function returns the same cells for every group."""
    cells_of = partial(fixed_cells, cells=cells)
    return (name, expect, partial(run_user_case, declared=declared, cells_of=cells_of))


def result_case(name: str, declared: str, frame_of: Any, label: str) -> tuple[str, str, Any]:
    """Return a case, answered as on main, whose function returns a whole frame per group."""
    runner = partial(run_user_case, declared=declared, cells_of=frame_of, label=label)
    return (name, MAIN, runner)


def group_case(name: str, declared: str, first: Any, others: Any) -> tuple[str, str, Any]:
    """Return a refused case whose cells differ between the groups."""
    cells_of = partial(cells_by_group, first=first, others=others)
    return (name, ARRAY, partial(run_user_case, declared=declared, cells_of=cells_of))


def table_case(
    name: str,
    expect: str,
    expression: Any,
    function: Any = identity,
    schema: Any = None,
) -> tuple[str, str, Any]:
    """Return a case over one SQL expression, SQL text or built column of the source table."""
    query = expression
    if isinstance(expression, str) and not expression.startswith("SELECT"):
        query = SELECT_VALUE.format(expression=expression, source="{source}")
    return (name, expect, partial(run_table_case, query=query, function=function, schema=schema))


def struct_ticks(ticks: list[int]) -> pa.Array:
    """Return a struct column whose one field holds the given nanosecond ticks."""
    return pa.StructArray.from_arrays([pa.array(ticks, pa.timestamp("ns"))], names=["t"])


def frame_case(
    name: str,
    expect: str,
    values: pa.Array,
    function: Any,
    declared: str = "struct<t:timestamp_ntz>",
    keys: list[int] | None = None,
) -> tuple[str, str, Any]:
    """Return a case over an Arrow column that keeps its type, one group unless keys are given."""
    table = pa.table({"id": pa.array(keys or [1] * len(values), pa.int32()), "v": values})
    runner = partial(run_frame_case, table=table, function=function, schema=f"id int, v {declared}")
    return (name, expect, runner)


def grouped_agg_case(
    name: str, expect: str, expression: str, function: Any, returns: str
) -> tuple[str, str, Any]:
    """Return a GROUPED_AGG case over one expression of the source table."""
    query = SELECT_VALUE.format(expression=expression, source="{source}")
    runner = partial(run_grouped_agg_case, query=query, function=function, returns=returns)
    return (name, expect, runner)


def window_case(name: str, expression: str, selected: tuple[str, ...]) -> tuple[str, str, Any]:
    """Return an ordered-window case, answered as on main, over one source expression."""
    query = SELECT_VALUE.format(expression=expression, source="{source}")
    return (name, MAIN, partial(run_window_case, query=query, selected=selected))


USER_CASES = (
    user_case("ns", ARRAY, NTZ, [NS_ARRAY]),
    user_case("ns_zoned", ARRAY, LTZ, [NS_ARRAY]),
    user_case("ns_whole_microseconds", ARRAY, NTZ, [WHOLE_NS_ARRAY]),
    user_case("ns_tick_one", ARRAY, NTZ, [ns_array(1)]),
    user_case("ns_tick_minus_two", ARRAY, NTZ, [ns_array(-2)]),
    user_case("ns_with_nat", ARRAY, NTZ, [ns_array("NaT", NS)]),
    user_case("ns_masked", ARRAY, NTZ, [np.ma.masked_array(NS_ARRAY, mask=[False, True])]),
    user_case("ns_strided", ARRAY, NTZ, [ns_array(NS, 0, NS2, 0)[::2]]),
    user_case("us_then_ns", ARRAY, NTZ, [US_ARRAY, NS_ARRAY]),
    user_case("us_then_ns_zoned", ARRAY, LTZ, [US_ARRAY, NS_ARRAY]),
    user_case("ms_then_ns", ARRAY, NTZ, [MS_ARRAY, NS_ARRAY]),
    user_case("empty_us_then_ns", ARRAY, NTZ, [EMPTY_US_ARRAY, NS_ARRAY]),
    user_case("nat_us_then_ns", ARRAY, NTZ, [NAT_US_ARRAY, NS_ARRAY]),
    user_case("null_then_ns", ARRAY, NTZ, [None, NS_ARRAY]),
    user_case("nan_then_ns", ARRAY, NTZ, [float("nan"), NS_ARRAY]),
    user_case("empty_list_then_ns", ARRAY, NTZ, [[], NS_ARRAY]),
    user_case("datetime_list_then_ns", ARRAY, NTZ, [DATETIMES, NS_ARRAY]),
    user_case("empty_ns_then_ns", ARRAY, NTZ, [EMPTY_NS_ARRAY, NS_ARRAY]),
    user_case("nat_ns_then_ns", ARRAY, NTZ, [NAT_NS_ARRAY, NS_ARRAY]),
    user_case("zero_ns_then_ns", ARRAY, NTZ, [ns_array(0), NS_ARRAY]),
    user_case("many_us_then_ns", ARRAY, NTZ, [US_ARRAY] * 20000 + [NS_ARRAY]),
    user_case("many_null_then_ns", ARRAY, NTZ, [None] * 20000 + [NS_ARRAY]),
    group_case("us_group_then_ns_group", NTZ, [US_ARRAY], [NS_ARRAY]),
    group_case("ns_group_then_us_group", NTZ, [NS_ARRAY], [US_ARRAY]),
    group_case("no_rows_then_ns_group", NTZ, [], [NS_ARRAY]),
    group_case("empty_frame_then_ns_group", NTZ, pd.DataFrame(), [NS_ARRAY]),
    user_case("nested_ns", ARRAY, NESTED, [[NS_ARRAY, NS_ARRAY]]),
    user_case("nested_object_array_ns", ARRAY, NESTED, [object_array([NS_ARRAY])]),
    user_case("nested_us_then_ns", ARRAY, NESTED, [[US_ARRAY, NS_ARRAY]]),
    user_case(
        "nested_object_array_us_then_ns", ARRAY, NESTED, [object_array([US_ARRAY, NS_ARRAY])]
    ),
    user_case("nested_depth_three_ns", ARRAY, "array<array<array<timestamp_ntz>>>", [[[NS_ARRAY]]]),
    user_case("nested_tuple_ns", ARRAY, NESTED, [(NS_ARRAY,)]),
    user_case("nested_deque_ns", ARRAY, NESTED, [collections.deque([NS_ARRAY])]),
    user_case("nested_series_ns", ARRAY, NESTED, [object_series([NS_ARRAY, NS_ARRAY])]),
    user_case("nested_dict_values_ns", ARRAY, NESTED, [{"x": NS_ARRAY}.values()]),
    user_case("struct_dict_ns", ARRAY, STRUCT, [{"a": NS_ARRAY}]),
    user_case("struct_tuple_ns", ARRAY, STRUCT, [(NS_ARRAY,)]),
    user_case("struct_pairs_ns", ARRAY, STRUCT, [[("a", NS_ARRAY)]]),
    user_case("struct_bytes_key_ns", ARRAY, STRUCT, [{b"a": NS_ARRAY}]),
    user_case("struct_us_row_then_ns_row", ARRAY, STRUCT, [{"a": US_ARRAY}, {"a": NS_ARRAY}]),
    user_case(
        "struct_us_then_ns",
        ARRAY,
        "struct<a:array<timestamp_ntz>,b:array<timestamp_ntz>>",
        [{"a": US_ARRAY, "b": NS_ARRAY}],
    ),
    user_case(
        "struct_in_struct_ns",
        ARRAY,
        "struct<s:struct<a:array<timestamp_ntz>>>",
        [{"s": {"a": NS_ARRAY}}],
    ),
    user_case(
        "struct_integer_then_ns",
        ARRAY,
        "struct<n:int,a:array<timestamp_ntz>>",
        [{"n": 1, "a": NS_ARRAY}],
    ),
    user_case(
        "array_of_struct_ns_array",
        ARRAY,
        "array<struct<a:array<timestamp_ntz>>>",
        [[{"a": NS_ARRAY}]],
    ),
    user_case("map_dict_ns", ARRAY, MAP, [{"k": NS_ARRAY}]),
    user_case("map_tuples_ns", ARRAY, MAP, [[("k", NS_ARRAY)]]),
    user_case("map_us_then_ns", ARRAY, MAP, [[("a", US_ARRAY), ("b", NS_ARRAY)]]),
    user_case("map_entry_dicts_ns", ARRAY, MAP, [[{"key": "k", "value": NS_ARRAY}]]),
    user_case("map_entry_pairs_ns", ARRAY, MAP, [[[("key", "k"), ("value", NS_ARRAY)]]]),
    user_case("map_object_array_ns", ARRAY, MAP, [object_array([("k", NS_ARRAY)])]),
    user_case("map_key_ns", ARRAY, "map<array<timestamp_ntz>,string>", [[(NS_ARRAY, "k")]]),
    user_case("us", MAIN, NTZ, [US_ARRAY]),
    user_case("us_zoned", MAIN, LTZ, [US_ARRAY]),
    user_case("open_ms_stored_as_raw_ticks", MAIN, NTZ, [MS_ARRAY]),
    user_case("empty_ns", MAIN, NTZ, [EMPTY_NS_ARRAY]),
    user_case("empty_ns_zoned", MAIN, LTZ, [EMPTY_NS_ARRAY]),
    user_case("nat_ns", MAIN, NTZ, [NAT_NS_ARRAY]),
    user_case("nat_ns_zoned", MAIN, LTZ, [NAT_NS_ARRAY]),
    user_case("zero_ns", MAIN, NTZ, [ns_array(0)]),
    user_case("minus_one_ns", MAIN, NTZ, [ns_array(-1)]),
    user_case("zero_minus_one_nat_ns", MAIN, NTZ, [ns_array(0, -1, "NaT")]),
    user_case("empty_us", MAIN, NTZ, [EMPTY_US_ARRAY]),
    user_case("nat_us", MAIN, NTZ, [NAT_US_ARRAY]),
    user_case("empty_list", MAIN, NTZ, [[]]),
    user_case("empty_object_array", MAIN, NTZ, [object_array([])]),
    user_case("null_cell", MAIN, NTZ, [None]),
    user_case("list_of_none", MAIN, NTZ, [[None, None]]),
    user_case("datetime_list", MAIN, NTZ, [DATETIMES]),
    user_case("datetime_list_zoned", MAIN, LTZ, [ZONED_DATETIMES]),
    user_case("timestamp_list", MAIN, NTZ, [STAMPS]),
    user_case("timestamp_tuple", MAIN, NTZ, [tuple(STAMPS)]),
    user_case("timestamp_object_array", MAIN, NTZ, [object_array(STAMPS)]),
    user_case("datetime_index_cell", MAIN, NTZ, [pd.DatetimeIndex(NS_ARRAY)]),
    user_case("series_cell", MAIN, NTZ, [pd.Series(NS_ARRAY)]),
    user_case("nested_us", MAIN, NESTED, [[US_ARRAY, US_ARRAY]]),
    user_case("nested_timestamp_lists", MAIN, NESTED, [[STAMPS, STAMPS]]),
    user_case("struct_us", MAIN, STRUCT, [{"a": US_ARRAY}]),
    user_case("map_us", MAIN, MAP, [[("k", US_ARRAY)]]),
    user_case("integer_list", MAIN, "array<int>", [[1, 2]]),
    user_case("bigint_list_from_ns", MAIN, "array<bigint>", [NS_ARRAY]),
    user_case(
        "struct_bigint_list_from_ns_beside_us",
        MAIN,
        "struct<n:array<bigint>,a:array<timestamp_ntz>>",
        [{"n": NS_ARRAY, "a": US_ARRAY}],
    ),
    user_case("integer_tick_list", MAIN, NTZ, [[NS // 1000]]),
    user_case("struct_integer_tick", MAIN, "struct<t:timestamp_ntz>", [{"t": NS // 1000}]),
    user_case("struct_timestamp", MAIN, "struct<t:timestamp_ntz>", [{"t": STAMPS[0]}]),
    user_case("map_timestamp", MAIN, "map<string,timestamp_ntz>", [[("k", STAMPS[0])]]),
    user_case("top_level_ns_whole", MAIN, "timestamp_ntz", pd.Series(WHOLE_NS_ARRAY[:1])),
    user_case(
        "top_level_integer_ticks", MAIN, "timestamp_ntz", pd.Series([NS // 1000], dtype="int64")
    ),
    user_case("arrow_list_ns_whole", MAIN, NTZ, arrow_series([[NS - 789, NS2 - 1]], NS_LIST)),
    user_case("arrow_list_ns_empty", MAIN, NTZ, arrow_series([[]], NS_LIST)),
    user_case("arrow_list_ns_nulls", MAIN, NTZ, arrow_series([[None]], NS_LIST)),
    user_case(
        "arrow_list_us", MAIN, NTZ, arrow_series([[NS // 1000]], pa.list_(pa.timestamp("us")))
    ),
    user_case(
        "arrow_struct_bare_ns_whole",
        MAIN,
        "struct<a:array<timestamp_ntz>,b:timestamp_ntz>",
        arrow_series(
            [{"a": [NS // 1000], "b": NS - 789}],
            pa.struct([("a", pa.list_(pa.timestamp("us"))), ("b", pa.timestamp("ns"))]),
        ),
    ),
    user_case("top_level_ns", MAIN, "timestamp_ntz", pd.Series(NS_ARRAY[:1])),
    user_case(
        "top_level_ns_zoned", MAIN, "timestamp", pd.Series(NS_ARRAY[:1]).dt.tz_localize("UTC")
    ),
    user_case("arrow_list_ns", MAIN, NTZ, arrow_series([[NS, NS2]], NS_LIST)),
    user_case(
        "arrow_struct_list_ns",
        MAIN,
        STRUCT,
        arrow_series([{"a": [NS, NS2]}], pa.struct([("a", NS_LIST)])),
    ),
    user_case("arrow_array_cell", MAIN, NTZ, [pa.array([NS, NS2], pa.timestamp("ns"))]),
    user_case("arrow_scalar_cell", MAIN, NTZ, [pa.scalar([NS], NS_LIST)]),
    user_case("two_dimensional_ns", MAIN, NESTED, [np.stack([NS_ARRAY, NS_ARRAY])]),
    user_case("zero_dimensional_ns", MAIN, NTZ, [np.array(NS, dtype="datetime64[ns]")]),
    user_case("byte_swapped_ns", MAIN, NTZ, [NS_ARRAY.astype(">M8[ns]")]),
    user_case("datetime64_scalar_list", MAIN, NTZ, [[np.datetime64(NS, "ns")]]),
    user_case(
        "struct_date_list_from_ns",
        MAIN,
        "struct<d:array<date>,a:array<timestamp_ntz>>",
        [{"d": NS_ARRAY, "a": US_ARRAY}],
    ),
    user_case("struct_list_positional_ns", MAIN, STRUCT, [[NS_ARRAY]]),
    user_case("struct_tuple_of_pairs_ns", MAIN, STRUCT, [(("a", NS_ARRAY),)]),
    user_case("map_entry_lists_ns", MAIN, MAP, [[["k", NS_ARRAY]]]),
    user_case("string_cell", MAIN, NTZ, ["not a list"]),
    user_case("ns_then_string_cell", MAIN, NTZ, [NS_ARRAY, "not a list"]),
    user_case("unreadable_struct_cell", MAIN, STRUCT, [UnreadableCell()]),
    result_case("duplicate_label_ns", NTZ, duplicate_label_frame, "v"),
    result_case("integer_label_ns", NTZ, integer_label_frame, "`0`"),
)

TABLE_CASES = (
    table_case("table_array_ns", ARRAY, "array(ns)"),
    table_case("table_array_tzns", ARRAY, "array(tzns)"),
    table_case("table_array_ns_built", ARRAY, partial(array_of_column, "ns")),
    table_case("table_nested_array_ns", ARRAY, "array(array(ns))"),
    table_case("table_struct_array_ns", ARRAY, "named_struct('a', array(ns))"),
    table_case("table_map_array_ns", ARRAY, partial(map_of_value, "array(ns)")),
    table_case("table_array_struct_array_ns", ARRAY, "array(named_struct('a', array(ns)))"),
    table_case(
        "table_struct_us_then_ns_arrays", ARRAY, "named_struct('a', array(n), 'b', array(ns))"
    ),
    table_case(
        "table_collect_list_ns", ARRAY, "SELECT 1 AS id, collect_list(ns) AS v FROM {source}"
    ),
    table_case(
        "table_null_cell_then_ns",
        ARRAY,
        "SELECT 1 AS id, CASE WHEN id > 1 THEN array(ns) END AS v FROM {source}",
    ),
    table_case(
        "table_empty_then_ns",
        ARRAY,
        "SELECT 1 AS id, CASE WHEN id > 1 THEN array(ns) ELSE slice(array(ns), 2, 1) END AS v "
        "FROM {source}",
    ),
    table_case("table_struct_ns", INTEGER, "named_struct('t', ns)"),
    table_case("table_struct_tzns", INTEGER, "named_struct('t', tzns)"),
    table_case("table_map_ns", INTEGER, partial(map_of_value, "ns")),
    table_case("table_array_struct_ns", INTEGER, "array(named_struct('t', ns))"),
    table_case("table_array_struct_tzns", INTEGER, "array(named_struct('t', tzns))"),
    table_case("table_array_struct_ns_built", INTEGER, partial(array_of_struct_column, "ns")),
    table_case("table_array_map_ns", INTEGER, partial(array_of_map, "ns")),
    table_case("table_map_ns_key", INTEGER, partial(map_of_key, "ns")),
    table_case("table_struct_struct_ns", INTEGER, "named_struct('s', named_struct('t', ns))"),
    table_case("table_map_struct_ns", INTEGER, partial(map_of_value, "named_struct('t', ns)")),
    table_case("table_struct_us_and_ns", INTEGER, "named_struct('a', n, 'b', ns)"),
    table_case("table_struct_ns_rebuilt", INTEGER, "named_struct('t', ns)", rebuild_structs),
    table_case("table_struct_ns_as_tuples", INTEGER, "named_struct('t', ns)", structs_as_tuples),
    table_case(
        "table_struct_ns_integer_column",
        INTEGER,
        "named_struct('t', ns)",
        ticks_as_integer_column,
        "id int, v timestamp_ntz",
    ),
    table_case(
        "table_struct_tzns_integer_column",
        INTEGER,
        "named_struct('t', tzns)",
        ticks_as_integer_column,
        "id int, v timestamp",
    ),
    table_case(
        "table_struct_ns_nullable_integer_column",
        INTEGER,
        "named_struct('t', ns)",
        partial(ticks_as_typed_integer_column, dtype="Int64"),
        "id int, v timestamp_ntz",
    ),
    table_case(
        "table_struct_ns_arrow_integer_column",
        INTEGER,
        "named_struct('t', ns)",
        partial(ticks_as_typed_integer_column, dtype="int64[pyarrow]"),
        "id int, v timestamp_ntz",
    ),
    table_case(
        "table_struct_ns_object_column",
        INTEGER,
        "named_struct('t', ns)",
        ticks_as_object_column,
        "id int, v timestamp_ntz",
    ),
    table_case(
        "table_struct_ns_integer_lists",
        INTEGER,
        "named_struct('t', ns)",
        ticks_as_lists,
        "id int, v array<timestamp_ntz>",
    ),
    table_case("table_map_ns_as_dicts", INTEGER, partial(map_of_value, "ns"), maps_as_dicts),
    table_case("table_array_ns_as_integer_lists", INTEGER, "array(ns)", arrays_as_integer_lists),
    table_case(
        "table_top_level_ns_as_integer_column",
        INTEGER,
        "ns",
        column_as_integers,
        "id int, v timestamp_ntz",
    ),
    table_case(
        "table_top_level_tzns_as_integer_column",
        INTEGER,
        "tzns",
        column_as_integers,
        "id int, v timestamp",
    ),
    table_case("table_array_us", MAIN, "array(n)"),
    table_case("table_array_ltz", MAIN, "array(l)"),
    table_case("table_nested_array_us", MAIN, "array(array(n))"),
    table_case("table_struct_array_us", MAIN, "named_struct('a', array(n))"),
    table_case("table_map_array_us", MAIN, partial(map_of_value, "array(n)")),
    table_case("table_array_struct_array_us", MAIN, "array(named_struct('a', array(n)))"),
    table_case("table_nested_array_cast_ns", MAIN, "array(array(CAST(ns AS timestamp_ntz)))"),
    table_case(
        "table_struct_array_cast_ns", MAIN, "named_struct('a', array(CAST(ns AS timestamp_ntz)))"
    ),
    table_case(
        "table_literal_timestamp_array",
        MAIN,
        "SELECT 1 AS id, array(TIMESTAMP '2026-01-02 03:04:05.123456+00:00', "
        "TIMESTAMP '1970-01-01 00:00:00+00:00') AS v",
    ),
    table_case("table_empty_array", MAIN, empty_array, identity, "id int, v array<timestamp_ntz>"),
    table_case("table_null_array", MAIN, null_array, identity, "id int, v array<timestamp_ntz>"),
    table_case("table_array_ns_filtered_empty", MAIN, "filter(array(ns), x -> false)"),
    table_case("table_array_ns_sliced_empty", MAIN, "slice(array(ns), 2, 1)"),
    table_case("table_array_ns_null_element", MAIN, "array(CASE WHEN id < 0 THEN ns END)"),
    table_case("table_array_ns_null_cell", MAIN, "CASE WHEN id < 0 THEN array(ns) END"),
    table_case(
        "table_array_ns_minus_one_only",
        MAIN,
        "SELECT id, array(ns) AS v FROM {source} WHERE id = 2",
    ),
    table_case("table_array_ns_cast_in_function", MAIN, "array(ns)", cast_arrays),
    table_case("table_array_tzns_cast_in_function", MAIN, "array(tzns)", cast_arrays),
    table_case(
        "table_struct_ns_column_dropped", MAIN, "named_struct('t', ns)", drop_value, "id int"
    ),
    table_case("table_struct_ns_floor_divided", MAIN, "named_struct('t', ns)", floor_divide),
    table_case("table_struct_tzns_floor_divided", MAIN, "named_struct('t', tzns)", floor_divide),
    table_case(
        "table_struct_ns_as_timestamps", MAIN, "named_struct('t', ns)", structs_as_timestamps
    ),
    table_case(
        "table_struct_ns_bigint_column",
        MAIN,
        "named_struct('t', ns)",
        ticks_as_integer_column,
        "id int, v bigint",
    ),
    table_case(
        "table_struct_ns_declared_bigint",
        MAIN,
        "named_struct('t', ns)",
        identity,
        "id int, v struct<t:bigint>",
    ),
    table_case(
        "table_struct_ns_minus_one_only",
        MAIN,
        "SELECT id, named_struct('t', ns) AS v FROM {source} WHERE id = 2",
    ),
    table_case("open_table_struct_ns_plus_one", MAIN, "named_struct('t', ns)", add_one_tick),
    table_case(
        "open_table_struct_ns_as_arrow_integer_structs",
        MAIN,
        "named_struct('t', ns)",
        ticks_as_arrow_integer_structs,
    ),
    table_case(
        "table_top_level_ns_floor_divided",
        MAIN,
        "ns",
        column_floor_divided,
        "id int, v timestamp_ntz",
    ),
    table_case("table_top_level_ns_as_bigint", MAIN, "ns", column_as_integers, "id int, v bigint"),
    table_case("table_top_level_ns", MAIN, "ns"),
    table_case("table_top_level_tzns", MAIN, "tzns"),
)

STRUCT_NS = pa.struct([("t", pa.timestamp("ns"))])
ARRAY_OF_STRUCT = "array<struct<t:timestamp_ntz>>"
FRAME_CASES = (
    frame_case("frame_tick_and_its_floor", INTEGER, struct_ticks([NS, NS // 1000]), identity),
    frame_case(
        "frame_tick_and_its_floor_floor_divided",
        MAIN,
        struct_ticks([NS, NS // 1000]),
        floor_divide,
    ),
    frame_case(
        "open_frame_floor_tick_row_only", MAIN, struct_ticks([NS, NS // 1000]), keep_floor_tick_rows
    ),
    frame_case(
        "frame_timestamp_equal_to_a_frame_tick", MAIN, struct_ticks([5_000_000]), later_timestamps
    ),
    frame_case(
        "frame_made_integer_then_frame_tick", INTEGER, struct_ticks([NS, NS2]), floor_first_row_only
    ),
    frame_case(
        "open_frame_tick_of_another_group",
        MAIN,
        pa.array([[("k", NS)], [("k", NS2)]], pa.map_(pa.string(), pa.timestamp("ns"))),
        other_group_tick,
        "map<string,timestamp_ntz>",
        [1, 2],
    ),
    frame_case(
        "frame_large_list_struct_ns",
        INTEGER,
        pa.array([[{"t": NS}]], pa.large_list(STRUCT_NS)),
        identity,
        ARRAY_OF_STRUCT,
    ),
    frame_case(
        "frame_fixed_size_list_struct_ns",
        INTEGER,
        pa.array([[{"t": NS}]], pa.list_(STRUCT_NS, 1)),
        identity,
        ARRAY_OF_STRUCT,
    ),
    frame_case(
        "frame_list_view_struct_ns",
        INTEGER,
        pa.array([[{"t": NS}]], pa.list_view(STRUCT_NS)),
        identity,
        ARRAY_OF_STRUCT,
    ),
    frame_case(
        "frame_large_list_ns",
        ARRAY,
        pa.array([[NS, NS2]], pa.large_list(pa.timestamp("ns"))),
        identity,
        NTZ,
    ),
    frame_case(
        "frame_fixed_size_list_ns",
        ARRAY,
        pa.array([[NS, NS2]], pa.list_(pa.timestamp("ns"), 2)),
        identity,
        NTZ,
    ),
)

GROUPED_AGG_CASES = (
    grouped_agg_case("open_grouped_agg_array_ns", MAIN, "array(ns)", first_cell, NTZ),
    grouped_agg_case("grouped_agg_array_us", MAIN, "array(n)", first_cell, NTZ),
    grouped_agg_case(
        "open_grouped_agg_struct_ns_tick",
        MAIN,
        "named_struct('t', ns)",
        first_tick,
        "timestamp_ntz",
    ),
    grouped_agg_case(
        "grouped_agg_struct_ns_tick_as_bigint", MAIN, "named_struct('t', ns)", first_tick, "bigint"
    ),
    grouped_agg_case(
        "grouped_agg_struct_ns_count", MAIN, "named_struct('t', ns)", count_cells, "bigint"
    ),
)

WINDOW_CASES = (
    window_case("window_array_ns_not_selected", "array(ns)", ("id",)),
    window_case("open_window_array_ns_selected", "array(ns)", ("id", "v")),
    window_case("window_struct_ns_not_selected", "named_struct('t', ns)", ("id",)),
    window_case("open_window_struct_ns_selected", "named_struct('t', ns)", ("id", "v")),
    window_case("window_array_us_selected", "array(n)", ("id", "v")),
)

RIGHT_SIDE_CASES = ("table_struct_ns", "table_array_struct_ns", "table_array_ns")
OTHER_ZONE_CASES = (
    "ns_zoned",
    "us_zoned",
    "table_array_tzns",
    "table_struct_tzns",
    "table_array_ltz",
    "table_struct_tzns_floor_divided",
)
DOOR_CASES = (*USER_CASES, *TABLE_CASES, *FRAME_CASES)
FACADE_CASES = (*GROUPED_AGG_CASES, *WINDOW_CASES)
RUNNERS = {name: runner for name, _expect, runner in (*DOOR_CASES, *FACADE_CASES)}
EXPECTED = {name: expect for name, expect, _runner in (*DOOR_CASES, *FACADE_CASES)}


def cell_keys() -> list[str]:
    """Return every cell as ``name|door|zone``."""
    keys = [f"{name}|{door}|UTC" for name, _, _ in DOOR_CASES for door in (GROUPED, COGROUPED)]
    keys.extend(f"{name}|{COGROUPED_RIGHT}|UTC" for name in RIGHT_SIDE_CASES)
    keys.extend(f"{name}|{GROUPED_AGG}|UTC" for name, _, _ in GROUPED_AGG_CASES)
    keys.extend(f"{name}|{ORDERED_WINDOW}|UTC" for name, _, _ in WINDOW_CASES)
    keys.extend(
        f"{name}|{door}|{OTHER_ZONE}" for name in OTHER_ZONE_CASES for door in (GROUPED, COGROUPED)
    )
    return keys


def integer_type(arrow_type: pa.DataType) -> pa.DataType:
    """Return the type with every timestamp and date read as its integer ticks."""
    if pa.types.is_timestamp(arrow_type):
        return pa.int64()
    if pa.types.is_date32(arrow_type):
        return pa.int32()
    if pa.types.is_list(arrow_type):
        return pa.list_(integer_type(arrow_type.value_type))
    if pa.types.is_struct(arrow_type):
        return pa.struct([(field.name, integer_type(field.type)) for field in arrow_type])
    if pa.types.is_map(arrow_type):
        return pa.map_(integer_type(arrow_type.key_type), integer_type(arrow_type.item_type))
    return arrow_type


def describe(table: pa.Table) -> dict[str, Any]:
    """Return the Arrow type and the integer ticks a result stores in its value column."""
    ordered = table.sort_by("id")
    name = "v" if "v" in ordered.column_names else ordered.column_names[-1]
    column = ordered.column(name).combine_chunks()
    values = json.loads(json.dumps(column.cast(integer_type(column.type)).to_pylist()))
    cell: dict[str, Any] = {"type": str(column.type), "rows": ordered.num_rows}
    if ordered.num_rows > LARGE_RESULT_ROWS:
        distinct = sorted({json.dumps(value) for value in values})
        cell["distinct"] = [json.loads(text) for text in distinct]
    else:
        cell["values"] = values
    return cell


def answer(spark: Any, key: str) -> tuple[dict[str, Any], BaseException | None]:
    """Return what one cell answers: its stored ticks, or its error with the cause's text cut."""
    name, door, _zone = key.split("|")
    try:
        return describe(RUNNERS[name](spark, door)), None
    except Exception as error:
        cause = error.__cause__
        text = str(error)
        if cause is not None and str(cause):
            text = text.replace(str(cause), "{cause}")
        wraps = type(cause).__name__ if cause is not None else None
        return {"error": type(error).__name__, "wraps": wraps, "text": text}, error


def answer_in_fresh_session(warehouse: Path, key: str) -> tuple[dict[str, Any], Any]:
    """Return what one cell answers in a fresh session over a fresh warehouse."""
    spark = doors.open_session(key.split("|")[2], warehouse)
    try:
        return answer(spark, key)
    finally:
        spark.stop()
