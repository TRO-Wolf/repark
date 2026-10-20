# Cost — ICE-TSNS-FRAME-DOOR-1 (2026-10-13)

`cost.json` is the output of `python/repark/tests/timestamps/_time_ice_tsns_frame_door_1.py`: one process
per side per set, five sets, main then head inside each set, sixteen cores pinned, the build
lock held so nothing else compiled. Each number is the median, in milliseconds, of
`createDataFrame` + `count()` over a 1,000,000-row pandas frame after one warm run (seven
repeats). Main is the Python source of `8c51fbc4` over the same release native module; the
unit changes no Rust.

## Fold 1: the floor on the int64 ticks (`fold_1_tick_floor`)

| Statement | main, median of 5 | head, median of 5 | Head minus main, per set |
|---|---|---|---|
| two `datetime64[us]` columns | 4.671 | 4.721 | ratio 1.011; the gate is 1.05 |
| one `datetime64[us]`, one naive `datetime64[ns]` | 4.812 | 21.014 | 16.19, 16.71, 16.13, 16.30, 16.50 |
| one `datetime64[us]`, one `datetime64[ns, UTC]` | 5.402 | 20.851 | 15.63, 16.61, 15.45, 15.71, 16.08 |
| one `datetime64[us]`, one `datetime64[ns, America/New_York]` | 6.093 | 21.470 | 14.53, 15.80, 15.07, 15.44, 15.17 |

**The gate (orchestrator rulings Q3 and fold-1 ruling 7, provisional, 2026-10-13).** A frame
with no nanosecond column stays within 1.05 of main. A nanosecond column, naive or aware,
costs at most 25 ms per million rows on this box: measured 16.7 ms at most, the same for
every zone, because the floor never leaves the ticks. Main does no work on the column.

The declared path (`schema="a timestamp, b timestamp"`, one repeat, set 1 only): 15187 ms on
main, 9462 ms on head. Both sides loop rows there on main's code; no gate.

## Before fold 1: `floor_temporal` (`before_fold_1_floor_temporal`)

The first kernel cost 17.1 ms per million rows for a naive column (five sets, largest 17.39)
and, as the verify stage measured, about 100 ms for a UTC column and 113 ms for a New York
column: `floor_temporal` converts a zoned column to local time and back. It also raised in
every repeated hour. Kernel times on one million rows with pyarrow 25.0.1: `floor_temporal`
4.3 ms naive, the timestamp cast 10.7 ms; the tick floor 14.3 ms for any zone.
