# Cost — ICE-TSNS-COERCION-1 slice 1, round 1

## First run (not like-for-like)

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

### What else ran

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

## Interleaved run

Run 2026-10-12 after the halt. Baseline module: `/tmp/xcoerce0/.venv`
(slice 0 head `c4eb9972`, no `_tsns_gate_counts`, release). Head module:
`/tmp/xcoerce1/.venv` (built at item 3, product-identical to this head,
`_tsns_gate_counts` present, release). Both report
`__debug_assertions__ = False`, so the comparison is release to release.

Driver: [../interleaved_cost.py](../interleaved_cost.py). Its setup and its
timed statements are the slice-0 timing script's own objects (same session,
same source tables, same INSERT and MERGE texts); only the timed set is
trimmed to `insert`, `merge` and `merge_mor`. Five sets of 15 repeats per
module, alternating baseline then head in one shell command per pair, each
pair run while holding the build lock on cores 48-63, the lock released
between pairs. One-minute load before the pairs: 1.88 / 1.98 / 2.11 / 2.25 /
2.15. No build ran during any pair.

Raw sets: [interleaved-sets.json](interleaved-sets.json). Aggregate:
[interleaved-cost.json](interleaved-cost.json). Ratio is the head median of
medians over the baseline median of medians; the pair range is the smallest
and largest per-pair head-over-baseline set-median ratio.

| statement | baseline set medians | head set medians | ratio | pair range |
|---|---|---|---|---|
| `insert` | 20.88 / 21.31 / 21.02 / 20.97 / 20.78 | 20.71 / 20.49 / 20.52 / 20.47 / 20.50 | 0.9776 | 0.9615 – 0.9919 |
| `merge` | 65.87 / 67.74 / 66.24 / 66.57 / 65.87 | 65.37 / 65.18 / 64.49 / 66.70 / 65.23 | 0.9848 | 0.9622 – 1.0020 |
| `merge_mor` | 70.42 / 71.40 / 72.42 / 73.04 / 70.21 | 71.97 / 70.56 / 72.68 / 75.82 / 69.00 | 1.0080 | 0.9828 – 1.0381 |

Every interleaved ratio is at or below 1.05: the halt does not reproduce
like-for-like. The box itself drifted between the days (today's baseline
`insert` median is 20.97 ms against 18.40 ms on baseline day), which is why
the first run's cross-day comparison overstated the head.

## Attribution

No statement qualified for attribution: the highest interleaved ratio is
1.0080 on `merge_mor`, below the 1.03 bar, so the Rust gate-walk
micro-measurement was not run. Below-threshold confirmation from the
driver's `--around` mode on the head module: one `merge` brackets the
counters `[2, 0]` to `[2, 0]`, and one `merge_mor` does the same — each
timed statement opens the gate 0 times, and the 2 opens in every head set
come from setup, before any timed statement. Fallback stayed 0 in all ten
sets. Total analyses per statement are not countable: the two counters
record opens only, these statements are closed-gate, and counting analyses
would need a product edit, which is out of scope; the set medians above are
the timing instead.

There is no extra time to attribute. The head sits at or below the baseline
on all three statements in the interleaved run (0.98 to 1.01), where the
wrapper's only per-statement cost is the closed-gate walk. The first run's
1.0560 and 1.0735 came from measuring across days against a baseline
recorded on a quieter box while another lane's release build ran between
sets 1 and 2, as recorded above; the interleaved pairs hold the build lock
at load ~2 and remove both effects, and the ratios collapse to 1.00.
