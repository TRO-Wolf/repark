# NS-ARRAY-FLOOR-1 input cells (D1, F1)

Nine doors over five sources (ns, tzns, coal, case, us) at top level and
inside an array, on main (module 293896ce), the U-1 head (module 07cbe563),
and live Spark 4.1.2 (microsecond control only, both values of
spark.sql.execution.pandas.convertToArrowArraySafely). Recorder
`python/repark/tests/_record_ns_array_floor_1_input.py`. Zone UTC everywhere.
Live banner: spark 4.1.2 zone UTC, pandas 3.0.6. The pre-epoch values -1 and
-1500 (nanoseconds beside the seeds, microseconds in the control) tell floor
from truncation.

## Run main

## NS-ARRAY-FLOOR-1 input cells

Backend main base 293896ce zone UTC live False safe n/a.

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('20 | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')", "Timesta -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.00 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtyp -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', ' |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtyp -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': [" | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_ | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'right_arrow': "k: int32 -- fi -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, el | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'right_arrow': "k -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'right_arrow': "k: int -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32  | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32  | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'right_arrow': "k: int -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timest | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 5, 'rows': ['Row(k=1)', 'Ro |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', ' |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 5, 'rows': ['Row(k=1)', 'R |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)',  |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timesta | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001+0000', -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| coal | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp( | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['2026-01-02T03 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| case | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['NaT'], dtype= -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp( | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['2026-01-02T03 -> {'count': 5, 'rows': ['Row(r=1)', 'R |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')", "Timestamp('1969-12-31 23:59: -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('202 | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.000000001+0000', tz='UT -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| coal | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-0 | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')", "Timest -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtype='datetime64[us]')", -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| case | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} -> {'count': 2, 'rows': ['Row(k=0, r=2)', 'Row(k=1, r=3)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['NaT'], dtype= -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-0 | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')", "Timest -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtype='datetime64[us]')", -> {'count': 2, 'rows': ['Row(k=0, r=2) |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04 | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)',  |
| ns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |
| tzns | top | answer | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Time | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Timestamp('2026-01-02 03:04:05.000000001+000 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |
| coal | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', "datetime.datetime(202 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[dateti | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[datetime.datetime(2026, 1, 2, 3, 4, 5)]']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |
| case | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', 'None']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row |
| case | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[None]' | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[None]']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', "datetime.datetime(202 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[dateti | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[datetime.datetime(2026, 1, 2, 3, 4, 5)]']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |

## Run U-1 head

## NS-ARRAY-FLOOR-1 input cells

Backend u1 base 07cbe563 zone UTC live False safe n/a.

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('20 | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')", "Timesta -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.00 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.00 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtyp -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': [" | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': [" | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')" -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:0 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k= |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_ | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'right_arrow': "k: int32 -- fi -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, el | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'right_arrow': "k -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'right_arrow': "k: int -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'right_arrow': "k: int32  | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'right_arrow': "k: int32 -- field | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: ti -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, el | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'right_arrow': "k -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'right_arrow': "k: int -> {'count': 5, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 5, 'rows': ['Row(k=0)', 'R |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timest | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 5, 'rows': ['Row(k=1)', 'Ro |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 5, 'rows': ['Row(k=1)', 'R |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')" -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:0 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k= |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 5, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 5, 'rows': ['Row(k=1)', 'R |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)',  |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timesta | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001+0000', -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| coal | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timesta | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001+0000', -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| case | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['NaT'], dty -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp( | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['2026-01-02T03 -> {'count': 5, 'rows': ['Row(r=1)', 'R |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')", "Timestamp('1969-12-31 23:59: -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('202 | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.000000001+0000', tz='UT -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| coal | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('202 | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.000000001+0000', tz='UT -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| case | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} -> {'count': 2, 'rows': ['Row(k=0, r=2)', 'Row(k=1, r=3)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['NaT'], dty -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-0 | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')", "Timest -> {'count': 2, 'rows': ['Row(k=0, r=2) |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtype='datetime64[us]')", -> {'count': 2, 'rows': ['Row(k=0, r=2) |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04 | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)',  |
| ns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |
| tzns | top | answer | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Time | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Timestamp('2026-01-02 03:04:05.000000001+000 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |
| coal | top | answer | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Time | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Timestamp('2026-01-02 03:04:05.000000001+000 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |
| case | top | answer | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| case | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', '[None]']} | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', '[None]']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', "datetime.datetime(202 -> {'count': 5, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[dateti | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[datetime.datetime(2026, 1, 2, 3, 4, 5)]']} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r |

## Run live Spark 4.1.2, safe=false

## NS-ARRAY-FLOOR-1 input cells

Backend spark412 base none zone UTC live True safe false.

Banner spark 4.1.2 zone UTC

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k= |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k= |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k= |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 2, 'rows': ['Row(k=0, r=2)', 'Row(k=1, r=3)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 2, 'rows': ['Row(k=0, r=2)', 'Row(k=1, r=3)']} |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r= |

## Run live Spark 4.1.2, safe=true

## NS-ARRAY-FLOOR-1 input cells

Backend spark412 base none zone UTC live True safe true.

Banner spark 4.1.2 zone UTC

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k= |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=0)', 'Row(k= |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'datetime64[ns]', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} | {'dtype': 'object', 'via': 'shared batch conversion diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)', 'Row(k=0)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(k=1)', 'Row(k= |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'datetime64[ns]', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 2, 'rows': ['Row(k=0, r=2)', 'Row(k=1, r=3)']} |
| us | arr | answer | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} | {'dtype': 'object', 'via': 'derived: Series slice of the same converted batch'} -> {'count': 2, 'rows': ['Row(k=0, r=2)', 'Row(k=1, r=3)']} |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | answer | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} | {'arrow': 'k: int32\nv: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | arr | answer | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via' | {'arrow': 'k: int32\nv: list<element: timestamp[us, tz=UTC]> not null\n  child 0, element: timestamp[us, tz=UTC]', 'via': 'shared batch stream diagnostic'} -> {'count': 5, 'rows': ['Row(r=1)', 'Row(r= |

