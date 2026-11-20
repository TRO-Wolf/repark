# TEMP-VIEW-FRAME-READER-PARSE-1 unit 1 — cost

## Cost (short-row rule)

`flock -x measure.lock` (+ link, opus-cargo, opus2-cargo), one sample at a time, clock inside
the lock. Builds per K row: base, head, swap, base-again; C-012: base, head, base-again. 10 runs (`k0_json_count`
20), C-012 5. Swap = `pkg-rp1swap-ba1957bf`: the head's native library with the base's `reader.py` and
`reader_support.py`, so no `ReaderParse` node is built. Ratio = head over the mean of base and base-again (medians,
per repetition).

| row | base ms | head ms | swap ms | head/bases | head/base, head/base-again | band | swap/bases | head/swap | runs | reading |
|---|---|---|---|---|---|---|---|---|---|---|
| k0_csv_count | 21.51 | 21.54 | 21.93 | 0.974 | 1.001, 0.948 | 1.056 | 0.992 | 0.982 | 10 | holds |
| k0_csv_hour | 73.96 | 77.70 | 75.90 | 1.044 | 1.051, 1.038 | 1.012 | 1.020 | 1.024 | 10 | holds |
| k0_json_count | 44.12 | 48.19 | 47.60 | 1.107 | 1.092, 1.122 | 0.974 | 1.093 | 1.012 | 20 | flag: build |
| k0_json_hour | 93.17 | 102.80 | 99.23 | 1.089 | 1.103, 1.074 | 1.027 | 1.051 | 1.036 | 10 | flag: build |
| k1 | 93.65 | 100.25 | 96.98 | 1.063 | 1.070, 1.055 | 1.014 | 1.028 | 1.034 | 10 | flag: build |
| k1_d | 85.58 | 91.11 | 96.21 | 1.059 | 1.065, 1.053 | 1.011 | 1.118 | 0.947 | 10 | flag: build |
| k2 | 72.20 | 76.05 | 75.42 | 1.036 | 1.053, 1.018 | 1.034 | 1.027 | 1.008 | 10 | holds |
| k2_d | 67.06 | 68.52 | 73.88 | 1.021 | 1.022, 1.020 | 1.002 | 1.101 | 0.927 | 10 | holds |
| k3 | 94.24 | 98.87 | 99.68 | 1.056 | 1.049, 1.062 | 0.988 | 1.064 | 0.992 | 10 | flag: build |
| k3_d | 91.30 | 95.82 | 98.21 | 1.058 | 1.049, 1.066 | 0.985 | 1.084 | 0.976 | 10 | flag: build |
| ntz_hour (twin) | 35.91 | 37.69 | 37.21 | 1.046 | 1.050, 1.042 | 1.008 | 1.032 | 1.013 | 10 | holds |
| pq_hour (twin) | 23.73 | 23.61 | 23.46 | 0.992 | 0.995, 0.989 | 1.006 | 0.986 | 1.007 | 10 | holds |
| tiny_csv (fixed cost) | 5.47 | 5.99 | 5.48 | 1.101 | 1.094, 1.109 | 0.986 | 1.008 | 1.092 | 10 | the named 0.5 ms line |
| C-012 (c012p6 + c012rest) | 82.24 s | 82.69 s | - | **1.002** | 1.005, 0.998 | 1.008 | - | - | 5 | holds (hard 1.05x) |

Instruction counts (`perf stat -D -1 --control fifo`, `instructions:u`, `cycles:u` around the timed loop only;
5 rounds, rotated order; `perf_event_paranoid` 2; spread over rounds under 0.03 percent on every row but tiny_csv):

| row | ins/rep base | ins head/base | ins swap/base | cycles head/base | cycles swap/base | attribution |
|---|---|---|---|---|---|---|
| k0_json_count | 2,727.6M | 1.0002 | 1.0000 | 1.122 | 1.123 | build |
| k0_json_hour | 5,703.4M | 1.0003 | 1.0000 | 1.022 | 1.056 | build |
| k1 | 7,648.7M | 1.0002 | 1.0000 | 1.079 | 1.064 | build |
| k1_d | 6,670.3M | 1.0009 | 1.0849 | 1.068 | 1.128 | build |
| k3 | 5,701.3M | 1.0003 | 1.0000 | 1.100 | 1.074 | build |
| k3_d | 5,212.1M | 1.0015 | 1.0547 | 1.036 | 1.090 | build |
| ntz_hour | 2,110.1M | 1.0007 | 1.0000 | 1.014 | 1.010 | twin, moves with the build |
| pq_hour | 952.9M | 1.0000 | 1.0000 | 1.017 | 1.012 | twin |
| tiny_csv | 21.6M | 1.0698 | 0.9896 | 1.030 | 0.970 | the unit's fixed cost |

- **Six flags, all the build.** Each instruction ratio is within 1.002 of 1.000, while cycles read 1.02 to 1.12 and
  the swap (no `ReaderParse`) carries the same cycles (e.g. `k0_json_count` cycles swap/base 1.123, head/swap 0.999).
  The plans are equal (EXPLAIN above). Recorded with their numbers; they do not block.
- Against the first run's six (`k0_csv_hour`, `k0_json_hour`, `k1`, `k2`, `k3`, `k3_d`): four flag again for the same
  cause; `k0_csv_hour` and `k2` now hold; `k0_json_count` and `k1_d` flag new, for the same cause.
- The `_d` rows' swap does more work (instructions swap/base 1.085, 1.055): the head's native library has no SQL-door
  fallback or reader-lineage exemption, so base Python on it closes the frame by another route. The swap is not a
  "logic off" control there; the head/base instruction ratio decides them.
- No row is above 1.15x: the highest is `k0_json_count` at 1.107 over 20 runs (1.122 against base-again alone; at 10
  runs it read 1.128, 1.158 against base-again alone, which is why I ran 10 more).
- **The fixed cost per query over a reader frame (`tiny_csv`, head against swap): 0.51 ms** (5.99 ms against 5.48 ms,
  10 runs; 1.73M instructions per query). Fable measured 0.49 ms. It is the named line; `pq_hour` and `ntz_hour`
  do not move.

## Reading

- C-012 is 1.002x, under the hard 1.05x.
- Six short-row flags (`k0_json_count`, `k0_json_hour`, `k1`, `k1_d`, `k3`, `k3_d`, 1.056x to 1.107x) are attributed to the build by instruction counts (head over base within 1.002); they do not block under the short-row rule (owner, 2026-11-19, about 14:00 UTC: "go with recommendations"). No row is above 1.15x.
- The fixed cost per query over a reader frame is 0.51 ms (`tiny_csv`, head against swap). There is no way out of it in this unit (S3, Fable's reply §3).
- The `pq_hour` and `ntz_hour` twin rows hold.

## Delta F1: cost and perf

Head `acd27d6e` (`pkg-rp1d-acd27d6e`) against main `8bc26cba` (`pkg-rp1base-8bc26cba`). The swap column is the rebuild's head `ba1957bf` (`pkg-rp1-ba1957bf`), so the delta's cost is read against the rebuild it replaces. Ten runs per row, five for C-012. The fix round `52f468ce` changes one token in `to_arrow` and was not re-measured for cost.

Ten-round table, with the reading column as the run recorded it (source `cost/table.md`):

| case | base ms | head ms | swap ms | base-again ms | head/bases | head/base | head/base-again | band (base-again/base) | swap/bases | head/swap | runs | rc!=0 | reading |
| ---|---|---|---|---|---|---|---|---|---|---|---|---|--- |
| k0_csv_count | 21.81 | 21.84 | 22.01 | 21.93 | 0.999 | 1.002 | 0.996 | 1.006 | 1.006 | 0.993 | 10 | 0 | holds |
| k0_csv_hour | 76.10 | 76.78 | 77.85 | 74.60 | 1.019 | 1.009 | 1.029 | 0.980 | 1.033 | 0.986 | 10 | 0 | holds |
| k0_json_count | 44.60 | 44.35 | 48.54 | 45.40 | 0.986 | 0.995 | 0.977 | 1.018 | 1.079 | 0.914 | 10 | 0 | holds |
| k0_json_hour | 95.05 | 94.89 | 101.23 | 94.47 | 1.001 | 0.998 | 1.004 | 0.994 | 1.068 | 0.937 | 10 | 0 | holds |
| k1 | 89.93 | 93.80 | 96.87 | 88.27 | 1.053 | 1.043 | 1.063 | 0.982 | 1.087 | 0.968 | 10 | 0 | FLAG |
| k1_d | 85.82 | 86.22 | 88.38 | 87.70 | 0.994 | 1.005 | 0.983 | 1.022 | 1.019 | 0.976 | 10 | 0 | holds |
| k2 | 71.22 | 72.93 | 77.65 | 72.73 | 1.013 | 1.024 | 1.003 | 1.021 | 1.079 | 0.939 | 10 | 0 | holds |
| k2_d | 67.79 | 69.79 | 69.21 | 66.17 | 1.042 | 1.030 | 1.055 | 0.976 | 1.033 | 1.008 | 10 | 0 | holds |
| k3 | 94.22 | 95.79 | 99.77 | 92.32 | 1.027 | 1.017 | 1.038 | 0.980 | 1.070 | 0.960 | 10 | 0 | holds |
| k3_d | 87.82 | 90.34 | 95.48 | 88.62 | 1.024 | 1.029 | 1.019 | 1.009 | 1.082 | 0.946 | 10 | 0 | holds |
| ntz_hour | 36.29 | 36.16 | 37.83 | 36.47 | 0.994 | 0.996 | 0.992 | 1.005 | 1.040 | 0.956 | 10 | 0 | holds |
| pq_hour | 23.59 | 22.76 | 24.05 | 23.63 | 0.964 | 0.965 | 0.963 | 1.002 | 1.019 | 0.946 | 10 | 0 | holds |
| tiny_csv | 5.42 | 5.92 | 5.97 | 5.43 | 1.092 | 1.092 | 1.092 | 1.000 | 1.101 | 0.992 | 10 | 0 | FLAG |
| c012p6 | 39.33 s | 39.39 s | - | 39.37 s | 1.001 | 1.002 | 1.001 | 1.001 | - | - | 5 | 0 | holds |
| c012rest | 42.84 s | 42.82 s | - | 42.72 s | 1.001 | 1.000 | 1.002 | 0.997 | - | - | 5 | 0 | holds |
| C-012 total | 82.17 s | 82.21 s | - | 82.09 s | 1.001 | 1.000 | 1.001 | 0.999 | - | - | | | holds |

Instruction counts (source `perf/table.md`: five rounds, instruction counts per repetition):

- **k1** (FLAG, head/bases 1.053 over ten runs): attributed to the build under the owner's short-row rule (2026-11-19, about 14:00 UTC). The instruction ratios are head/base 1.0002 and head/swap 1.0000. The wall ratios in the same five rounds are head/base 0.934, swap/base 1.001 and head/swap 0.933, so the 1.053 reading does not reproduce in the perf rounds.
- **tiny_csv** (FLAG, the named fixed cost): 5.42 ms base, 5.92 ms head and 5.97 ms swap over ten runs, so the fixed cost per query over a reader frame is 0.50 ms (head against base). Its instruction ratios are head/base 1.0627 and head/swap 0.9901: its instruction count moves by 6.3 percent, where every other row moves by 0.15 percent or less.
- **C-012** reads head/bases 1.001 over five runs, under the hard 1.05x.
- Every other row reads at or under 1.042 head/bases, and no row is over 1.15x.
