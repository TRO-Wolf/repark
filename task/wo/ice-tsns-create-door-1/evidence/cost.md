# Cost — ICE-TSNS-CREATE-DOOR-1 (2026-10-13)

`cost.json` is the output of `python/repark/tests/_time_ice_tsns_create_door_1.py`: one process
per side per set, five sets, main then head inside each set, sixteen cores pinned, the build
lock held so nothing else compiled. Each number is the median, in milliseconds, of five runs
after one warm run. Main is the release native module built from `f3f98823` before any product
edit, under the same Python source (the unit changes no Python source); head is the release
module built from `229ece3d`, the last commit that changes Rust.

| Statement | main, median of 5 | head, median of 5 | Ratio of medians | Head over main, per set |
|---|---|---|---|---|
| `CREATE TABLE … AS SELECT *` over the 1,000,000-row microsecond table | 62.432 | 62.563 | 1.002 | 1.011, 0.969, 1.083, 0.934, 0.993 |
| `mode("overwrite").saveAsTable` of that frame | 64.624 | 65.876 | 1.019 | 1.019, 1.061, 0.960, 1.076, 0.993 |
| `MERGE WITH SCHEMA EVOLUTION` adding the microsecond column from that source | 198.576 | 199.656 | 1.005 | 0.990, 1.006, 1.008, 1.013, 0.993 |
| `EXPLAIN` of a 200-column query with no nanosecond column | 140.169 | 135.426 | 0.966 | 0.966, 0.964, 0.985, 0.990, 0.954 |

**The gate is 1.05 on the ratio of medians; all four hold.** A single set moves by up to 8
percent in either direction on the two 60 ms statements, which is the noise of one process on
this box: the same statements are below 1.00 in other sets. The door adds one eager analysis
of an already analyzed plan and one pass over the output types; a column whose type holds no
nanosecond timestamp is never walked. `EXPLAIN` does not reach the door at all.
