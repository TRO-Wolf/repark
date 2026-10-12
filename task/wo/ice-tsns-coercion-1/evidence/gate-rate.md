# Gate-open rate — ICE-TSNS-COERCION-1 slice 1, round 1

The facade suite was already run with `gate_rate_plugin.py`. This note checks
that JSON and records the rate. The raw file was 2,469,379 bytes, so the
committed `gate-rate.json` is a reduced copy: `deltas` keeps the 460 rows with
`opened > 0`, and `items`, `gate_open_total` and `fallback_total` stay the
full-run totals. `items` is therefore not `len(deltas)`. `items_with_opens`
is 460.

## Check against the inherited counts

A one-off load of the raw JSON agreed with every inherited figure.

| | value |
|---|---:|
| Test items | 16,657 |
| Gate opens (`gate_open_total`, and the sum of `opened`) | 12,670 |
| Fallback (`fallback_total`, and the sum of `fallback`) | 0 |
| Items with `opened > 0` | 460 (2.8%, 460/16657) |
| Files with any open | 8 |

No delta was negative. Every fallback delta was 0. The sum of `opened` over
the kept rows is still 12,670.

## How the plugin counted

`task/wo/ice-tsns-coercion-1/gate_rate_plugin.py` is a pytest plugin. It reads
`repark._native._tsns_gate_counts()`, the read-only pair `(gate_open, fallback)`.
`pytest_configure` snapshots both counters. An autouse fixture records, for
every test item, `opened` and `fallback` as the counter deltas across that
item. `pytest_sessionfinish` writes `items` (how many items ran),
`gate_open_total` (the session delta of the gate-open counter),
`fallback_total`, and one `deltas` row per item (`nodeid`, `opened`,
`fallback`). One item can open the gate many times: each analysis of an open
plan increments the counter, and several items analyze the same shape more
than once. The share below is items that opened at least once, over items.

## Opens by test file

Paths are under `python/repark/`. Every other file in the suite contributed
0 opens.

| file | items | items that opened | opens |
|---|---:|---:|---:|
| `tests/test_ice_tsns_merge_wall_1.py` | 355 | 278 | 7,417 |
| `tests/test_ice_tstzns_wall_1.py` | 132 | 131 | 4,917 |
| `tests/test_ice_tsns_sql_1.py` | 43 | 41 | 215 |
| `tests/test_ice_tsns_coercion_1_slice1.py` | 3 | 2 | 94 |
| `tests/test_array_null_1_coercion.py` | 181 | 4 | 12 |
| `tests/test_interchange_parity.py` | 55 | 2 | 9 |
| `tests/test_boundary_shapes_parity.py` | 14 | 1 | 3 |
| `tests/test_v3_create_opt_in.py` | 5 | 1 | 3 |

## Pandas `datetime64[ns]` as the only nanosecond source

One of the eight files. It accounts for 1 of the 460 opening items and 3 of
the 12,670 opens: `tests/test_boundary_shapes_parity.py`.

Decision: a file counts only when every nanosecond-typed input in it is a
pandas frame or series whose numpy dtype is `datetime64[ns]`. I read each of
the eight files and searched for `datetime64[ns]`, `timestamp_ns`,
`timestamptz_ns`, `pa.timestamp("ns"…)`, `numpy.datetime64(…, "ns")`, an
`ArrowDtype` of a nanosecond timestamp, and the cast kernels. A recorded
export dtype is not a source. `pd.to_datetime` with no unit returns
`datetime64[ns]` on this machine's pandas.

`test_boundary_shapes_parity.py` builds that input in
`recipe_in_pandas_datetime64_ns`: a `pd.Series` with `dtype="datetime64[ns]"`
passed to `createDataFrame`. The sibling recipe is `datetime64[us]`. The
`datetime64[ns]` text on the microsecond row is the recorded Spark export
dtype, not an input. The suite matches that reading:
`pandas_timestamp_unit_from_pandas_ns` opened 3 times, and
`pandas_timestamp_unit_from_pandas_us` opened 0. No other item in the file
opened.

The other seven do not qualify.

- `test_ice_tsns_merge_wall_1.py` writes `timestamp_ns` columns, including
  nested struct, array and map leaves, and registers
  `pa.timestamp("ns", tz="UTC")` arrays. No pandas frame.
- `test_ice_tstzns_wall_1.py` writes `timestamptz_ns` and `timestamp_ns`
  tables and builds `pa.timestamp("ns")` columns. No pandas frame.
- `test_ice_tsns_sql_1.py` is SQL `timestamp_ns` / `timestamptz_ns` and
  `CAST(… AS timestamp_ns)`. `write_control` also appends a pandas frame
  built with `pd.to_datetime` (`datetime64[ns]`), so the frame is one source
  beside the SQL ones.
- `test_ice_tsns_coercion_1_slice1.py` has both. The pandas case
  (`dtype="datetime64[ns]"`) opened 4 times. The twelve-statement replay of
  `ns` and `tzns_utc` sources opened 90 times. The integer frame opened 0.
- `test_array_null_1_coercion.py` registers
  `pa.array(…, type=pa.timestamp("ns", tz="UTC"))` through
  `register_arrow_stream_as_temp_view`. The 12 opens are the four
  `test_timestamp_ns_unit_rescale_truncates_like_arrow_cast` items. No pandas
  frame.
- `test_interchange_parity.py` builds pandas `datetime64[ns]` inside
  `test_create_dataframe_pandas_datetime64_ms_stays_timestamp` (units `ms`,
  `us`, `ns`, `s`; 6 opens) and also
  `pd.ArrowDtype(pa.timestamp("ns"))` (3 opens). `numpy.datetime64(…, "ns")`
  tuples are in the file and those items opened 0.
- `test_v3_create_opt_in.py` creates `ts timestamp_ns` and scans it. The one
  opening item is `test_opt_in_v3_create_timestamp_ns_schema_round_trips`
  (3 opens). No pandas frame.
