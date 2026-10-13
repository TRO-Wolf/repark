# Cost — ICE-TSNS-CREATE-DOOR-1 (2026-10-13)

`cost.json` is the output of `python/repark/tests/_time_ice_tsns_create_door_1.py`: one process
per side per set, five sets, main then head inside each set, sixteen cores pinned, the build
lock held so nothing else compiled. Each number is the median, in milliseconds, of five runs
after one warm run. Main is the release native module built from `f3f98823` before any product
edit, under the same Python source (the unit changes no Python source); head is the release
module built from the unit's source. **The gate is 1.05 on the ratio of the five-set medians**
(orchestrator ruling Q8); every set is recorded.

## Fold 1 (`fold_1`)

| Statement | main, median of 5 | head, median of 5 | Ratio of medians | Head over main, per set |
|---|---|---|---|---|
| `CREATE TABLE … AS SELECT *` over the 1,000,000-row microsecond table | 61.295 | 60.627 | 0.989 | 1.013, 0.958, 0.948, 0.998, 1.027 |
| `mode("overwrite").saveAsTable` of that frame | 62.208 | 64.665 | 1.039 | 0.867, 1.087, 0.999, 1.039, 1.090 |
| `MERGE WITH SCHEMA EVOLUTION` adding the microsecond column from that source | 200.084 | 197.628 | 0.988 | 0.982, 0.979, 1.001, 1.000, 1.017 |
| a 200-column CTAS (`id + n` two hundred times, `LIMIT 1000`), no nanosecond column | 174.758 | 173.616 | 0.993 | 0.993, 1.001, 0.982, 0.989, 0.995 |

All four hold. The fourth statement is the verify stage's: its first form here was an
`EXPLAIN SELECT`, which never reaches the door, and the verify measured the 200-column create
at **1.181** of main on the head it failed (175.2 ms to 206.9 ms, every set between 1.171 and
1.197). The cost was the seat's second eager analysis of an already analyzed plan, paid
whether or not any output column could be a nanosecond timestamp. The seat now looks at the
output types first and analyzes only when one of them holds a nanosecond timestamp; the same
statement is 0.993 of main. A single set of the two 60 ms statements moves by up to 13 percent
in either direction, which is the noise of one process on this box.

## Before fold 1 (`before_fold_1`)

The first three statements and an `EXPLAIN` of the 200-column query, on `229ece3d`:

| Statement | main, median of 5 | head, median of 5 | Ratio of medians | Head over main, per set |
|---|---|---|---|---|
| `CREATE TABLE … AS SELECT *` over the 1,000,000-row microsecond table | 62.432 | 62.563 | 1.002 | 1.011, 0.969, 1.083, 0.934, 0.993 |
| `mode("overwrite").saveAsTable` of that frame | 64.624 | 65.876 | 1.019 | 1.019, 1.061, 0.960, 1.076, 0.993 |
| `MERGE WITH SCHEMA EVOLUTION` adding the microsecond column from that source | 198.576 | 199.656 | 1.005 | 0.990, 1.006, 1.008, 1.013, 0.993 |
| `EXPLAIN` of a 200-column query with no nanosecond column | 140.169 | 135.426 | 0.966 | 0.966, 0.964, 0.985, 0.990, 0.954 |

