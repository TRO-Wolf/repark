# SUM-DEC-I128WRAP-1 — the cost (sketch §4 "Cost cases", the change in its final placement)

**Builds.** Base: `pkg-avg-27d6149f` (main's product tree). Head: `pkg-sum-a8016e75` (`BUILT_FROM`
`a8016e75a033906e5bb49fcce9d79bdfa924ad3b`, built clean from the committed product commit). **Method:** the runner
[cost/run.sh](cost/run.sh) (modelled on the avg unit's), five alternating runs of main, head, main again (`main-aa`,
the control band), each sample a fresh process under the exclusive measure lock, the link and both cargo locks,
`nice -n 10`, cores 48–57; each K sample times 4 or 10 repetitions after a warm-up (every sample at or above 200 ms);
C-012 is the replay harness's wall time. The K3 rows ran to ten runs (one was above 1.05x after five). Data: 10 M rows
(`k1`: 200,000 groups, `d10` `DECIMAL(10,2)`, `d36`, `d38` values below `10^26`, `x` `DOUBLE`, `b` `BIGINT`) and 1 M
rows (`k2`: 100 window partitions), written by [cost/sum_data.py](cost/sum_data.py); queries in
[cost/sum_cost.py](cost/sum_cost.py); raw samples [cost/samples.jsonl](cost/samples.jsonl) and
[cost/raw-sum.jsonl](cost/raw-sum.jsonl); [cost_table.py](cost_table.py) prints [cost/table.md](cost/table.md).
Ratio = median head / median main; the band is the min to max of `main-aa / main` per run.

| row | main (s) | head (s) | head / main | control band | runs | bar |
|---|---|---|---|---|---|---|
| **K1** grouped `sum(d10)`, ANSI on | 0.1018 | 0.0925 | **0.909** | 0.903 to 1.049 | 5 | held |
| **K1** grouped `sum(d10)`, ANSI off (sentinel) | 0.0955 | 0.0911 | **0.954** | 0.954 to 1.175 | 5 | held |
| **K1** grouped `sum(d38)`, ANSI on | 0.1175 | 0.1155 | **0.983** | 0.962 to 1.060 | 5 | held |
| **K1** grouped `sum(d38)`, ANSI off (sentinel) | 0.1227 | 0.1165 | **0.950** | 0.944 to 1.037 | 5 | held |
| K2 `ROWS 3 PRECEDING`, `d10` | 0.0991 | 0.0952 | 0.961 | 0.855 to 1.090 | 5 | held |
| K2 `ROWS 3 PRECEDING`, `d38` | 0.1054 | 0.0928 | 0.880 | 0.950 to 1.071 | 5 | held |
| K2 running frame, `d10` | 0.0855 | 0.0776 | 0.908 | 0.962 to 1.125 | 5 | held |
| K2 running frame, `d38` | 0.0903 | 0.0807 | 0.893 | 0.902 to 1.107 | 5 | held |
| K2 `ROWS 1000 PRECEDING`, `d10` | 0.0983 | 0.0885 | 0.900 | 0.900 to 1.077 | 5 | held |
| K2 `ROWS 1000 PRECEDING`, `d38` | 0.1063 | 0.0943 | 0.887 | 0.846 to 1.100 | 5 | held |
| K3 ungrouped `sum(d10)` | 0.0342 | 0.0351 | 1.028 | 0.887 to 1.192 | 10 | held |
| K3 ungrouped `sum(d36)` (the magnitude bound's path) | 0.0563 | 0.0578 | 1.027 | 0.907 to 1.048 | 10 | held |
| K3 ungrouped `sum(d38)` | 0.0551 | 0.0588 | **1.067** | 0.978 to **1.089** | 10 | above 1.05x, inside the band (not a halt by the brief's rule) |
| K4 grouped `sum(b)` (`BIGINT`) | 0.0805 | 0.0832 | 1.033 | 0.890 to 1.001 | 5 | held |
| K4 grouped `sum(x)` (`DOUBLE`) | 0.0771 | 0.0762 | 0.988 | 0.955 to 1.099 | 5 | held |
| K4 ungrouped `sum(b)` | 0.0308 | 0.0300 | 0.974 | 0.957 to 1.104 | 5 | held |
| K4 ungrouped `sum(x)` | 0.0292 | 0.0292 | 1.000 | 0.903 to 1.068 | 5 | held |
| K4 `sum(b + 1), sum(b + 2)` | 0.0617 | 0.0571 | 0.926 | 0.891 to 1.032 | 5 | held |
| C-012 `c012p6` | 39.58 | 39.61 | 1.001 | 0.973 to 1.007 | 5 | held |
| C-012 rest | 43.17 | 42.97 | 0.995 | 0.992 to 3.172 | 5 | held |
| K5 grouped `try_sum(d10)` (reported) | 0.8061 | 0.0886 | 0.110 | 0.977 to 1.066 | 5 | reported: datafusion-spark's row accumulator per group becomes a `GroupsAccumulator` |

- **The marker (Q2): the sentinel**, `i128::MIN` in the one state column. With it K1 holds in both modes at both
  precisions (ANSI off 0.954 and 0.950; ANSI on, no marker, 0.909 and 0.983), so the second state column was not
  built. At 200,000 groups the partial step skips aggregation (`convert_to_state`), so every row's state crosses the
  hash repartition, the case the sketch named.
- **K2-R:** `extension::sum_window_tests::every_sum_arm_stays_a_retracting_aggregate_window_in_both_ansi_modes` (the
  analyzed and optimized plan of a Spark-door session, both modes) and `aggregate::sum_tests::every_sum_arm_and_decimal_try_sum_retract_a_sliding_frame`.
  The wide frame scales with n: on the head `ROWS 1000 PRECEDING` takes 0.929x (`d10`) and 1.016x (`d38`) of
  `ROWS 3 PRECEDING`, where a re-scan would take about 250x.
- **K3 `d38`** stays the one row above 1.05x after ten runs (1.067); its control band reaches 1.089, so by the
  brief's rule it is not a halt. The cost is the magnitude bound (Arrow `min`, `max` and `sum`, three passes, where
  DataFusion makes one) on 10 M rows; `d36` takes the same path at 1.027.
- `k4_sum_x` answers differ between builds in the last digits of a `DOUBLE` sum: the parallel summation order, the
  delegated arm unchanged (the answers also differ between the two `main` samples).
- C-012 times include the lock wait of the sample; the `rest` control band's 3.172 is one `main-aa` sample that waited
  for the shared lock. The medians are unaffected.
- After the runs, `ruff` reformatted the scripts here and two long SQL literals in `sum_data.py` were split
  into implicit concatenations (the same text); `cost_table.py` and `compare_bar.py` print byte-identical output
  before and after.

## Delta 2

**Run:** the owner's ruling (2026-11-18, about 20:05 UTC): "Go with Fable's recommendations on all seven, re-run 1362".
The head `fcb0c888` (package `pkg-sum-fcb0c888`) is measured against main now (`pkg-main-6286ba19`, the base of this
series), with the delta 1 runner ([cost/run.sh](cost/run.sh)) copied. Cores 48–57; main, head and main again (`main-aa`)
alternate, one sample at a time under the exclusive measure lock with the link lock and both cargo locks, the clock
started inside the locks. Samples and the table: [delta2/cost/](delta2/cost/map.md).

**Bars:** the held rows are at 1.05x, except `k1_sum_d38_off` and `k1_sum_d38_on`, held to 1.12x (the 1.12x bar is for
precision 20 and up on K1 only). `k5_try_sum_d10_grouped` is reported, not held.

| row | main (s) | head (s) | head / main | control main-aa / main | runs | answers equal | bar |
|---|---|---|---|---|---|---|---|
| C-012 `c012p6` | 38.8600 | 39.2900 | 1.011 | 0.976 to 1.012 | 5 | | 1.05x (held) |
| C-012 `c012rest` | 42.3800 | 42.6700 | 1.007 | 0.993 to 1.005 | 5 | | 1.05x (held) |
| `k1_sum_d10_off` | 0.0941 | 0.0942 | 1.001 | 0.811 to 1.216 | 10 | True | 1.05x (held) |
| `k1_sum_d10_on` | 0.0936 | 0.0926 | 0.989 | 0.890 to 1.059 | 5 | True | 1.05x (held) |
| `k1_sum_d38_off` | 0.1204 | 0.1307 | 1.085 | 0.926 to 1.062 | 5 | True | 1.12x (held) |
| `k1_sum_d38_on` | 0.1205 | 0.1289 | 1.070 | 0.882 to 1.119 | 5 | True | 1.12x (held) |
| `k3_sum_d38` | 0.0569 | 0.0562 | 0.989 | 0.919 to 1.058 | 5 | True | 1.05x (held) |
| `k5_try_sum_d10_grouped` | 0.8482 | 0.0931 | 0.110 | 0.958 to 1.044 | 5 | True | reported |

Every held row is within its bar.

**Runs:** 5 for every row, and 10 for `k1_sum_d10_off`, which stood at 1.055 after 5 (inside its control band) and took
runs 6 to 10 under the series' rule for a row over 1.05x.

**Who ran it:** the builder ran the K rows' runs 1 to 3 before pausing at its context limit. The orchestrator ran the K
rows' runs 4 and 5, C-012 runs 1 to 5, and `k1_sum_d10_off` runs 6 to 10, from 2026-11-19 at about 00:00 to 00:27 UTC.
A partial run 4 and three partial C-012 records, from a runner that outlived the builder's hand-back, were removed
first. The exclusive lock was starved for hours by long shared holds on lane A.

## Delta 3 (interim: the series is not finished)

**Run:** main now `pkg-main-4c365814` against the head `pkg-sum-845766fd`, with delta 2's runner copied to
`S/ddu-work/cost-sum-d3/` and pointed at them (main, head, main again alternating, one sample at a time under the
exclusive measure lock with the link lock and both cargo locks, the clock inside the locks, cores 48–57), plus a turnstile
lock that holds the builder's own shared-lock jobs back while a sample waits. Rows as delta 2, plus **`k3_sum_d10`
(reported)**: K1 is a grouped query, so the ungrouped batch add this delta changed is reached by K3, and at `d10` by no
held row. Bars: 1.12x for K1 `d38` (both modes), 1.05x for every other held row, K3 included.

**Interim, not the series:** the builder paused at its turn budget with the K rows' runs 1 and 2 and C-012's run 1
complete; runs 3 to 5 (and 6 to 10 for any row over its bar) are left. Samples: [delta3/cost/](delta3/cost/map.md).

| row | main (s) | head (s) | head / main | control main-aa / main | runs | answers equal | bar |
|---|---|---|---|---|---|---|---|
| c012_c012p6 | 39.2000 | 39.8200 | 1.016 | 0.997 to 1.003 | 2 |  | held |
| c012_c012rest | 42.5700 | 42.6600 | 1.002 | 1.000 to 1.000 | 1 |  | held |
| k1_sum_d10_off | 0.0988 | 0.0898 | 0.909 | 0.919 to 1.028 | 2 | True | held |
| k1_sum_d10_on | 0.0987 | 0.0977 | 0.991 | 0.916 to 1.094 | 2 | True | held |
| k1_sum_d38_off | 0.1237 | 0.1286 | 1.040 | 0.947 to 1.028 | 2 | True | held |
| k1_sum_d38_on | 0.1118 | 0.1305 | 1.167 | 1.017 to 1.029 | 2 | True | ABOVE |
| k3_sum_d10 | 0.0363 | 0.0361 | 0.995 | 0.867 to 1.037 | 2 | True | held |
| k3_sum_d38 | 0.0571 | 0.0537 | 0.941 | 1.021 to 1.086 | 2 | True | held |
| k5_try_sum_d10_grouped | 0.8506 | 0.0983 | 0.116 | 0.962 to 0.992 | 2 | True | reported |
