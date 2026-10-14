# NS-ARRAY-FLOOR-1 input cells (D1)

Nine doors over five sources (ns, tzns, coal, case, us) at top level and
inside an array, on main (module 293896ce), the U-1 head (module 07cbe563),
and live Spark 4.1.2 (microsecond control only). Recorder
`python/repark/tests/_record_ns_array_floor_1_input.py`. Zone UTC everywhere.
Live banner: spark 4.1.2 zone UTC.

## Run main

## NS-ARRAY-FLOOR-1 input cells

Backend main base 293896ce zone UTC live False.

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('20 | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(k=0)', 'Row(k=1)' |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.00 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtyp -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT']} -> {'count': 3, 'rows': ['Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')"]} -> {'count': 3, 'rows': ['Row(k=0)', 'Ro |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtyp -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': [" | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_ | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'right_arrow': "k: int32 -- fi -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, el | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'right_arrow': "k -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'right_arrow': "k: int -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32  | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32  | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'right_arrow': "k: int -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timest | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(k=1)', 'Ro |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} -> {'count': 3, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)']} |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 3, 'rows': ['Row(k=1)', 'R |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime( | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC]", 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timesta | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001+0000', -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| coal | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp( | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['2026-01-02T03 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| case | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', 'NaT']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['NaT'], dtype= -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp( | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['2026-01-02T03 -> {'count': 3, 'rows': ['Row(r=1)', 'R |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row(k=1, r=2)']} |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('202 | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.000000001+0000', tz='UT -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| coal | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-0 | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtype='datetime64[us]')"] -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| case | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT']} | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT']} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row(k=1, r=2)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row(k=1, r=2)']} |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-0 | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtype='datetime64[us]')"] -> {'count': 2, 'rows': ['Row(k=0, r=1) |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04 | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| ns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |
| tzns | top | answer | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Time | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Timestamp('2026-01-02 03:04:05.000000001+000 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |
| coal | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', "datetime.datetime(202 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[dateti | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[datetime.datetime(2026, 1, 2, 3, 4, 5)]']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |
| case | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', 'None']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row |
| case | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[None]' | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[None]']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', "datetime.datetime(202 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[dateti | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[datetime.datetime(2026, 1, 2, 3, 4, 5)]']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |

## Run U-1 head

## NS-ARRAY-FLOOR-1 input cells

Backend u1 base 07cbe563 zone UTC live False.

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('20 | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(k=0)', 'Row(k=1)' |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.00 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.00 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'] -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT']} | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT']} -> {'count': 3, 'rows': ['Row(k=0)', 'Row(k=1)', 'Row(k=1)']} |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtyp -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': [" | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': [" | {'left_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'v | {'left_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'right_dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01- | {'left_dtype': {'k': 'int32', 'v': 'object'}, 'right_dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[ -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')" -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:0 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None']} -> {'count': 3, 'rows': ['Row(k=0)', 'Row(k=1)', 'Ro |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_ | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'right_arrow': "k: int32 -- fi -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| ns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, el | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'right_arrow': "k -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'right_arrow': "k: int -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| tzns | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'right_arrow': "k: int32  | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| coal | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'right_arrow': "k: int32 -- field | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'right_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: ti -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| case | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, el | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'right_arrow': "k -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | top | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUE | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'right_arrow': "k: int -> {'count': 3, 'rows': ['Row(k=0)', 'R |
| us | arr | answer | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null chi | {'left_arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", ' -> {'count': 3, 'rows': ['Row(k=0)', 'R |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timest | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(k=1)', 'Ro |
| ns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UT | {'dtype': {'k': 'int32', 'v': 'datetime64[ns, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} | {'dtype': {'k': 'int32', 'v': 'datetime64[ns]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} -> {'count': 3, 'rows': ['Row(k=1)', 'Row(k=0)', 'Row(k=1)']} |
| case | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')",  | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC') | {'dtype': {'k': 'int32', 'v': 'datetime64[us, UTC]'}, 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "ar | {'dtype': {'k': 'int32', 'v': 'object'}, 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", " -> {'count': 3, 'rows': ['Row(k=1)', 'R |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: ' | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns] -- field metadata -- PARQUET:field_id: '3'", 'values': ["Timestamp('2026-01-02 0 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| ns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC] -- field metadata -- PARQUET:field_id: '4'", 'values': ["Timestamp('2026 -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| tzns | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns, tz=UTC]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')" -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| coal | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns, tz=UTC]> not null child 0, element: timestamp[ns, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| case | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:0 | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[ns]", 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} -> {'count': 3, 'rows': ['Row(k=1)', 'Row(k= |
| case | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[ns]> not null child 0, element: timestamp[ns]", 'values': ["[Timestamp -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | top | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:fie | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: timestamp[us, tz=UTC] -- field metadata -- PARQUET:field_id: '5'", 'values': ["datetime.dateti -> {'count': 3, 'rows': ['Row(k=1)', 'R |
| us | arr | answer | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, | {'arrow': "k: int32 -- field metadata -- PARQUET:field_id: '2' v: list<element: timestamp[us, tz=UTC]> not null child 0, element: timestamp[us, tz=UTC]", 'value -> {'count': 3, 'rows': ['Row(k=1)', 'R |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timesta | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001+0000', -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| coal | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timesta | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05.000000001+0000', -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['2026-01-02 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| case | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT', 'NaT']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')", "array(['NaT'], dty -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp( | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", 'NaT', "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='d | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['NaT'], dtype='datetime64[us]')", "array(['2026-01-02T03 -> {'count': 3, 'rows': ['Row(r=1)', 'R |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000 | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row(k=1, r=2)']} |
| ns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| tzns | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('202 | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.000000001+0000', tz='UT -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| tzns | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| coal | top | answer | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('202 | {'dtype': 'datetime64[ns, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05.000000001+0000', tz='UT -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| coal | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['2026-01-02T03:04:05.000000001'], dtype='datetime64[n -> {'count': 2, 'rows': ['Row(k=0, r=1) |
| case | top | answer | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT']} | {'dtype': 'datetime64[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'NaT']} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row(k=1, r=2)']} |
| case | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456789'], dtype='datetime64[ns]')", "array(['NaT'], dtype='datetime64[ns]')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row(k=1, r=2)'] |
| us | top | answer | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-0 | {'dtype': 'datetime64[us, UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456+0000', tz='UTC')", "Timestamp('2026-01-02 03:04:05+0000', tz='UTC')"]} -> {'count': 2, 'rows': ['Row(k=0, r=1)', 'Row |
| us | arr | answer | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:0 | {'dtype': 'object', 'values': ["array(['2026-01-02T03:04:05.123456'], dtype='datetime64[us]')", "array(['2026-01-02T03:04:05.000000'], dtype='datetime64[us]')"] -> {'count': 2, 'rows': ['Row(k=0, r=1) |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| ns | top | answer | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04 | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', "Timestamp('2026-01-02 03:04:05.000000001')"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| ns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |
| tzns | top | answer | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Time | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Timestamp('2026-01-02 03:04:05.000000001+000 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| tzns | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |
| coal | top | answer | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Time | {'arrow': 'timestamp[ns, tz=UTC]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789+0000', tz='UTC')", 'None', "Timestamp('2026-01-02 03:04:05.000000001+000 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| coal | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp( | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', "[Timestamp('2026-01-02 03:04:05.000000001')]"]} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |
| case | top | answer | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} | {'arrow': 'timestamp[ns]', 'values': ["Timestamp('2026-01-02 03:04:05.123456789')", 'None', 'None']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| case | arr | answer | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', '[None]']} | {'arrow': 'list<item: timestamp[ns]>', 'values': ["[Timestamp('2026-01-02 03:04:05.123456789')]", '[None]', '[None]']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r=1)', 'Row(r=1)']} |
| us | top | answer | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(k | {'arrow': 'timestamp[us, tz=UTC]', 'values': ["datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo(key='UTC", 'None', "datetime.datetime(202 -> {'count': 3, 'rows': ['Row(r=1)', 'R |
| us | arr | answer | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[dateti | {'arrow': 'list<item: timestamp[us]>', 'values': ['[datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)]', '[None]', '[datetime.datetime(2026, 1, 2, 3, 4, 5)]']} -> {'count': 3, 'rows': ['Row(r=1)', 'Row(r |

## Run live Spark 4.1.2

## NS-ARRAY-FLOOR-1 input cells

Backend spark412 base none zone UTC live True.

Banner spark 4.1.2 zone UTC

### grouped_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### cogroup_applyin

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### grouped_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### cogroup_applyinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### mapinpan

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### mapinarrow

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### scalar_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### groupedagg_pandas_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| us | arr | error | {} | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |

### scalar_arrow_udf

| source | level | outcome | seen | returned_or_error |
|---|---|---|---|---|
| us | top | error | {} | PySparkNotImplementedError UNSUPPORTED_SIGNATURE [UNSUPPORTED_SIGNATURE] Unsupported signature: (array: 'Any') -> 'pa.Array'. |
| us | arr | error | {} | PySparkNotImplementedError UNSUPPORTED_SIGNATURE [UNSUPPORTED_SIGNATURE] Unsupported signature: (array: 'Any') -> 'pa.Array'. |

