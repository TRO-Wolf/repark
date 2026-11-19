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
