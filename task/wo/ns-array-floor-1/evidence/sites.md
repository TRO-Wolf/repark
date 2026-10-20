# NS-ARRAY-FLOOR-1 refusal sites (D3, restated in F3 from F1 and F2)

D1 input side: zero cells error on main and on the U-1 head, so no input
refusal site exists. Every door receives a nanosecond frame or batch; the
only main-vs-U-1 difference is the dtype seen for the two widened shapes
(microseconds on main, nanoseconds on the U-1 head). On live Spark 4.1.2 a
microsecond TIMESTAMP arrives as `datetime64[ns]` in pandas and as
`timestamp[us, tz=UTC]` in Arrow batches.

D2 output side, main against live Spark 4.1.2 at the default safe setting
(true in the lane environment), per door as same or differs. Joint answers
store equal ticks everywhere; stored ticks equal frame ticks everywhere
either engine stores.

| Door | array plus timestamp | array plus timestamp_ntz | top-level |
|---|---|---|---|
| grouped applyInPandas | same, floor | same, floor | same, both refuse |
| cogroup applyInPandas | same, floor | same, floor | same, both refuse |
| grouped applyInArrow | same, both refuse | same, both refuse | same, both refuse |
| cogroup applyInArrow | same, both refuse | same, both refuse | same, both refuse |
| mapInPandas | differs: main refuses, Spark floors | same, floor | same, both refuse |
| mapInArrow | same, both refuse | same, both refuse | same, both refuse |
| scalar pandas_udf | same, floor | same, floor | same, both refuse |
| grouped-aggregate pandas_udf | same, floor | same, floor | same, both refuse |
| scalar arrow_udf | same, both refuse | same, both refuse | same, both refuse |

Spark's own split under safe=false, measured: arrays floor to
`[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]`,
top-level columns truncate toward zero to `[1767323045123456, None,
1767323045000000, 0, -1]`. Under safe=true the top-level cells raise the
worker `PySparkValueError: Failed to convert the value of the column 'v'`,
and a whole-microsecond value answers. The card's verdict-2 refusal under
`array<timestamp_ntz>` does not reproduce on this lane's pandas 3.0.6:
Spark floors there under both settings.

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
rule per site: grouped_arrow.py:103 floors nanoseconds inside arrays and refuses a top-level nanosecond column; grouped_arrow.py:271 refuses every nanosecond-versus-microsecond mismatch; udf_schema.py:60 refuses every yielded batch mismatch; udf_bridge.py:140 floors nanoseconds inside list Series and refuses a top-level nanosecond Series; functions_arrow_udf.py:121 refuses every unsafe nanosecond cast; udf_bridge.py:38 infers naive nanosecond arrays as microseconds before validation
