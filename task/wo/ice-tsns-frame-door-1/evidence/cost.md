# Cost — ICE-TSNS-FRAME-DOOR-1 (2026-10-13)

`cost.json` is the output of `python/repark/tests/_time_ice_tsns_frame_door_1.py`: one process
per side per set, five sets, main then head inside each set, sixteen cores pinned, the build
lock held so nothing else compiled. Each number is the median, in milliseconds, of
`createDataFrame` + `count()` over a 1,000,000-row pandas frame after one warm run (seven
repeats; three on the declared path). Main is the Python source of `8c51fbc4` over the same
release native module; the unit changes no Rust.

| Statement | main, median of 5 | head, median of 5 | Reading |
|---|---|---|---|
| two `datetime64[us]` columns | 4.649 | 4.693 | ratio 1.009; the gate is 1.05. The step returns the same object. |
| one `datetime64[us]`, one `datetime64[ns]` | 4.680 | 21.799 | main does no work on the column. Head minus the row above, per set: 17.16, 17.39, 17.09, 16.77, 16.94 ms. |
| the same frame, `schema="a timestamp, b timestamp"` | 15330.3 | 9607.9 | ratio 0.63. Both sides loop rows here on main's code; no gate. |

**The gate (orchestrator ruling Q3, provisional, 2026-10-13).** A ratio against a zero-copy
baseline is not the measure for the nanosecond statement. The absolute cost is recorded: about
17.1 ms per million rows per nanosecond column on this box (largest set 17.39). The ceiling is
25 ms. Kernel times on one million rows with pyarrow 25.0.1: `floor_temporal` 4.3 ms, the safe
cast of the floored column 10.7 ms; `cast(safe=False)` alone is also 10.7 ms and is wrong
below the epoch.
