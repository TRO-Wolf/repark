# Cost — ICE-TSNS-COERCION-1 slice 1, round 1

Compared with `cost-baseline.json` on this head (`7e8ab664`). The numbers are
the slice-0 timing script's: `_time_ice_tsns_coercion_1.py`, three sets of
15 repeats, 2,000 rows, median of the three set medians. `main` does not
return between sets, so each set is that script's `measure`, and the report
is its `summarize`. `flock -n /tmp/oc-worker/build-slots/opus-cargo.lock true`
ran immediately before each set and exited 0 before the set started.

Two ratios are above 1.05. Nothing was changed to bring them down.

| statement | baseline ms | head ms | head set medians | ratio |
|---|---:|---:|---|---:|
| `insert` | 18.40 | 19.02 | 18.84 / 19.02 / 20.51 | 1.0337 |
| `merge` | 63.17 | 66.71 | 66.71 / 64.87 / 66.75 | 1.0560 |
| `merge_mor` | 75.61 | 81.17 | 83.19 / 81.17 / 79.65 | 1.0735 |
| `read_union` | 319.75 | 323.08 | 319.69 / 323.08 / 326.81 | 1.0104 |
| `read_wide_case` | 124.34 | 124.19 | 123.76 / 124.19 / 126.76 | 0.9988 |
| `read_ns_union` | 68.44 | 69.11 | 68.73 / 69.11 / 69.71 | 1.0098 |
| `read_ns_case` | 82.50 | 82.60 | 82.09 / 82.60 / 84.31 | 1.0012 |

Ratio is the head median of medians divided by the baseline median of
medians. Above 1.05: `merge` (66.71 / 63.17 = 1.0560) and `merge_mor`
(81.17 / 75.61 = 1.0735). The fastest `merge_mor` set, 79.65 ms, is still
79.65 / 75.61 = 1.0534.

## What else ran

The lock was held at the first check. Another lane was in
`/tmp/oc-worker/direct/wo/ice-ntz-nested-wall-1/verify2/mut`, running
`uvx maturin@1.14.1 develop --release` under the same flock, with `cargo
rustc` on `repark_spark`. That build was up for 10 checks (about 5 minutes)
before set 1, and again for 26 checks (about 13 minutes) before set 2. Set 3
found the lock free on the first check.

Load when each set started: 6.44 / 6.19 / 6.20, then 5.37 / 6.08 / 6.03, then
5.08 / 5.99 / 6.00. At the start of each set the counted processes were three
`python` and no `cargo`, `rustc` or `java`. At the end of set 1, `cargo` and
`rustc` were up again (one each) and set 2 waited that build out. Sets 2 and
3 ended with three `python` and no compiler. A long-lived `claude` verifier
for ICE-NTZ-NESTED-WALL-1 was present throughout; it was not a build.
