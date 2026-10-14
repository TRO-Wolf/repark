# NS-ARRAY-FLOOR-1 refusal sites (D3)

D1 input side: zero cells error on main and on the U-1 head, so no input
refusal site exists. Every door receives a nanosecond frame or batch; the
only main-vs-U-1 difference is the dtype seen for the two widened shapes
(microseconds on main, nanoseconds on the U-1 head).

D2 output side on main: 9 cells floor to microseconds, 27 refuse. Spark
4.1.2 behavior below is the card's recorded verdict 2 of draft PR 1047
(parity row ICE-TSNS-SQL-1-R-020), not a lane re-measurement: under
`array<timestamp>` Spark floors to ticks `[1767323045123456,
1767323045000000]`; under `array<timestamp_ntz>` Spark refuses with
`ArrowNotImplementedError: Expected np.datetime64 but got: timestamp[ns]`;
a top-level `datetime64[ns]` column under `timestamp` raises
`PySparkValueError`.

## Main vs Spark on the card-measured doors

| Door | array plus timestamp | array plus timestamp_ntz | top-level control |
|---|---|---|---|
| grouped applyInPandas | floors, ticks match Spark | floors where Spark refuses | refuses, as Spark does |
| cogroup applyInPandas | floors, ticks match Spark | floors where Spark refuses | refuses, as Spark does |

Main behavior on the other doors (Spark not measured in this lane):

| Door | array plus timestamp | array plus timestamp_ntz | top-level |
|---|---|---|---|
| grouped applyInArrow | refuses | refuses | refuses |
| cogroup applyInArrow | refuses | refuses | refuses |
| mapInPandas | refuses (inferred naive us vs declared tz) | floors | refuses |
| mapInArrow | refuses | refuses | refuses |
| scalar pandas_udf | floors | floors | refuses |
| grouped-aggregate pandas_udf | floors | floors | refuses |
| scalar arrow_udf | refuses | refuses | refuses |

## Door to site

Each site was read in the facade source and confirmed by the error
traceback or the floored result type in `output-cells.json`.

| Door | Site | Serves |
|---|---|---|
| grouped applyInPandas | `python/repark/src/repark/spark/dataframe/grouped_arrow.py:103` floors arrays, refuses top-level columns | output |
| cogroup applyInPandas | same function as above, reached through the cogroup caller | output |
| grouped-aggregate pandas_udf | same function as above, reached through the grouped caller | output |
| grouped applyInArrow | `python/repark/src/repark/spark/dataframe/grouped_arrow.py:271` refuses every type mismatch | output |
| cogroup applyInArrow | same function as above, reached through the cogroup caller | output |
| mapInPandas, mapInArrow | `python/repark/src/repark/spark/dataframe/udf_schema.py:60` refuses every type mismatch | output |
| mapInPandas array floor | `python/repark/src/repark/spark/dataframe/udf_bridge.py:38` infers naive nanosecond arrays as microseconds | output |
| scalar pandas_udf | `python/repark/src/repark/spark/dataframe/udf_bridge.py:140` floors arrays, refuses top-level columns | output |
| scalar arrow_udf | `python/repark/src/repark/spark/functions_arrow_udf.py:121` refuses every unsafe nanosecond cast | output |

Two doors share a function but refuse in different callers above it, twice:
grouped and cogroup applyInPandas share the `:103` function through the
grouped caller and the cogroup caller, and grouped and cogroup applyInArrow
share the `:271` function through the grouped caller and the cogroup caller.

sites: 6 (python/repark/src/repark/spark/dataframe/grouped_arrow.py:103, python/repark/src/repark/spark/dataframe/grouped_arrow.py:271, python/repark/src/repark/spark/dataframe/udf_schema.py:60, python/repark/src/repark/spark/dataframe/udf_bridge.py:140, python/repark/src/repark/spark/functions_arrow_udf.py:121, python/repark/src/repark/spark/dataframe/udf_bridge.py:38)
