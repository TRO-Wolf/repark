# CAST-LITERAL-UNWRAP-1 — the build's cost

**Builds.** Head: the package built from `88999454` (both placements in: the analyzer rule and the optimizer rule
before every `simplify_expressions`, before the signed-zero re-run, and last). Main: main `cb7d3943`'s package, sampled
twice per run (`main`, then `main-aa`, the control). **Method:** 5 alternating runs of main, head, main; every sample
one process under the exclusive measure lock and the three build locks, `nice -n 10`, cores 32-47; each sample at
least 200 ms (planning rows 20 repetitions, read rows 10, C-012 a full replay; the `DELETE` row one statement on a
fresh table, its build not timed). Times below are per repetition. Runner: [probes/run.sh](probes/run.sh),
[probes/cu_cost.py](probes/cu_cost.py), [probes/cu_data.py](probes/cu_data.py) (10 million rows: `c2` Parquet with
`DECIMAL(10,2)` values in shuffled order, so the scan cannot prune; `c3src` sorted on `c`), the held planning rows from
#1339's `cost_probe.py`, C-012 by `replay_work.py`; tables by [probes/analyze.py](probes/analyze.py); samples in
`raw/cost-*.jsonl`. 210 samples, every one `rc = 0`.

## The bars

| row | measure | base | head | ratio | control band | bar | held |
|---|---|---|---|---|---|---|---|
| C1 C-012 `c012p6` | wall, head vs main | 38.67 s | 38.97 s | 1.008 | 0.997 to 1.001 | 1.05x | yes |
| C1 C-012 `c012rest` | wall, head vs main | 42.18 s | 42.50 s | 1.008 | 0.989 to 1.001 | 1.05x | yes |
| C1 `plan_pred200` (200 predicates, no cast) | planning, head vs main | 0.0720 s | 0.0727 s | 1.009 | 0.996 to 1.047 | 1.05x | yes |
| `plan_values10k` | planning, head vs main | 1.3793 s | 1.3940 s | 1.011 | 0.997 to 1.017 | 1.05x | yes |
| `plan_union200` | planning, head vs main | 0.2743 s | 0.2754 s | 1.004 | 1.000 to 1.014 | 1.05x | yes |
| `plan_free200` | planning, head vs main | 0.1378 s | 0.1416 s | 1.027 | 0.980 to 1.016 | 1.05x | yes |
| `plan_wide_dd` | planning, head vs main | 0.1093 s | 0.1142 s | 1.045 | 0.988 to 1.034 | 1.05x | yes |
| C2 `CAST(c AS INT) = 1`, Parquet | head cast vs head hand-written `c >= 1 AND c < 2` | 0.0605 s | 0.0580 s | 0.959 | main's own hand-written runs 0.953 to 1.008 | 1.05x | yes |
| C2 `CAST(c AS INT) < 1`, Parquet | head cast vs head `c < 1` | 0.0594 s | 0.0610 s | 1.027 | 0.984 to 1.002 | 1.05x | yes |
| C3 Iceberg CoW, `= 1` and `< 1` | scan tasks, cast vs hand-written | N=16 | N=16 | equal | | equal | yes |
| C3 Iceberg MoR (with delete files), `= 1` and `< 1` | scan tasks, cast vs hand-written | N=16 | N=16 | equal | | equal | yes |
| `DELETE … WHERE CAST(c AS INT) = 1`, C3 CoW | head cast vs head hand-written | 1.3214 s | 1.2954 s | 0.980 | 0.963 to 1.052 | 1.05x | yes |
| `DELETE … WHERE CAST(c AS INT) = 1`, C3 MoR | head cast vs head hand-written | 1.4351 s | 1.4524 s | 1.012 | 0.911 to 1.028 | 1.05x | yes |

**C3, what is counted.** The Iceberg scan reports the file tasks it keeps after pruning (`IcebergTableScan … N=`);
RePark's scan has no row-group metric. Shielded and hand-written filters keep the same 16 tasks on both tables, and the
scan's own predicate is empty for both: the fork converts no DECIMAL literal into an Iceberg predicate, so neither
form prunes Iceberg files today (an integer `id` range does prune: `N=1`). On Parquet both forms prune alike
(`files_ranges_pruned_statistics` and `row_groups_pruned_statistics` equal in every C2 sample, and pinned by
`test_a_shielded_filter_prunes_as_the_hand_written_range` on a layout where pruning removes files).

## Reported without a bar

| row | main (wrong answer) | head (per row) | head/main | control band |
|---|---|---|---|---|
| C4 `CAST(b AS INT) = 1`, `BIGINT` in range, Parquet | 0.0318 s | 0.0305 s | 0.960 | 0.916 to 1.018 |
| C4 the same, Iceberg | 0.0219 s | 0.0227 s | 1.036 | 0.854 to 1.001 |
| C4 `CAST(s AS TINYINT) = 1`, `DECIMAL(10,2)` in range, Parquet | 0.0234 s | 0.0400 s | 1.711 | 1.004 to 1.035 |
| C4 the same, Iceberg | 0.0240 s | 0.0308 s | 1.279 | 0.947 to 0.997 |
| `CAST(c AS INT) IN (1, …, 40)`, past the cap | 0.0495 s | 0.0591 s | 1.193 | 0.952 to 1.019 |

The `IN` of 40 keeps the shield (`per_row = true` in every head sample) and answers 4,000 rows on both builds; main's
answer is its unwrapped `IN`. C4's `DECIMAL`→`TINYINT` is the decimal cast kernel per row (main compared the column
with `1.00`); the `BIGINT`→`INT` cast is cheap.

## Every row, head against main

| row | main (s) | head (s) | head/main | control main-aa/main | runs | answers equal | head scan |
|---|---|---|---|---|---|---|---|
| C-012 c012p6 | 38.6700 | 38.9700 | 1.008 | 0.997 to 1.001 | 5 | True |  |
| C-012 c012rest | 42.1800 | 42.5000 | 1.008 | 0.989 to 1.001 | 5 | True |  |
| c2_eq | 0.0560 | 0.0580 | 1.036 | 0.951 to 1.033 | 5 | False | (pruning equal) |
| c2_eq_hand | 0.0614 | 0.0605 | 0.986 | 0.953 to 1.008 | 5 | True | (pruning equal) |
| c2_lt | 0.0603 | 0.0610 | 1.011 | 0.936 to 1.012 | 5 | True | (pruning equal) |
| c2_lt_hand | 0.0596 | 0.0594 | 0.996 | 0.984 to 1.002 | 5 | True | (pruning equal) |
| c3_cow_eq | 0.0314 | 0.0324 | 1.032 | 0.997 to 1.062 | 5 | False | ['16'] |
| c3_cow_eq_hand | 0.0312 | 0.0318 | 1.017 | 0.982 to 1.027 | 5 | True | ['16'] |
| c3_cow_lt | 0.0329 | 0.0331 | 1.005 | 0.982 to 1.041 | 5 | True | ['16'] |
| c3_cow_lt_hand | 0.0322 | 0.0321 | 0.995 | 0.981 to 1.062 | 5 | True | ['16'] |
| c3_mor_eq | 0.0366 | 0.0369 | 1.009 | 0.993 to 1.044 | 5 | False | ['16'] |
| c3_mor_eq_hand | 0.0366 | 0.0362 | 0.988 | 0.976 to 1.035 | 5 | True | ['16'] |
| c3_mor_lt | 0.0373 | 0.0375 | 1.006 | 0.954 to 1.015 | 5 | True | ['16'] |
| c3_mor_lt_hand | 0.0371 | 0.0366 | 0.986 | 0.951 to 1.032 | 5 | True | ['16'] |
| c4_ice_bigint | 0.0219 | 0.0227 | 1.036 | 0.854 to 1.001 | 5 | True |  |
| c4_ice_tiny | 0.0240 | 0.0308 | 1.279 | 0.947 to 0.997 | 5 | False |  |
| c4_pq_bigint | 0.0318 | 0.0305 | 0.960 | 0.916 to 1.018 | 5 | True |  |
| c4_pq_tiny | 0.0234 | 0.0400 | 1.711 | 1.004 to 1.035 | 5 | False |  |
| del_cow_eq | 1.2712 | 1.2954 | 1.019 | 0.969 to 1.073 | 5 | False |  |
| del_cow_eq_hand | 1.3108 | 1.3214 | 1.008 | 0.963 to 1.052 | 5 | True |  |
| del_mor_eq | 1.4017 | 1.4524 | 1.036 | 0.962 to 1.049 | 5 | False |  |
| del_mor_eq_hand | 1.3930 | 1.4351 | 1.030 | 0.911 to 1.028 | 5 | True |  |
| in40 | 0.0495 | 0.0591 | 1.193 | 0.952 to 1.019 | 5 | False | True |
| plan_free200 | 0.1378 | 0.1416 | 1.027 | 0.980 to 1.016 | 5 | True |  |
| plan_pred200 | 0.0720 | 0.0727 | 1.009 | 0.996 to 1.047 | 5 | True |  |
| plan_union200 | 0.2743 | 0.2754 | 1.004 | 1.000 to 1.014 | 5 | True |  |
| plan_values10k | 1.3793 | 1.3940 | 1.011 | 0.997 to 1.017 | 5 | True |  |
| plan_wide_dd | 0.1093 | 0.1142 | 1.045 | 0.988 to 1.034 | 5 | True |  |

| shielded row vs hand-written range (same build) | build | cast (s) | hand (s) | cast/hand | runs |
|---|---|---|---|---|---|
| c2_eq | head | 0.0580 | 0.0605 | 0.959 (per run 0.939 to 1.013) | 5 |
| c2_eq | main | 0.0560 | 0.0614 | 0.913 (per run 0.850 to 0.929) | 5 |
| c2_eq | main-aa | 0.0548 | 0.0595 | 0.921 (per run 0.876 to 0.969) | 5 |
| c2_lt | head | 0.0610 | 0.0594 | 1.027 (per run 0.986 to 1.055) | 5 |
| c2_lt | main | 0.0603 | 0.0596 | 1.012 (per run 0.995 to 1.047) | 5 |
| c2_lt | main-aa | 0.0610 | 0.0595 | 1.025 (per run 0.957 to 1.053) | 5 |
| c3_cow_eq | head | 0.0324 | 0.0318 | 1.019 (per run 0.991 to 1.059) | 5 |
| c3_cow_eq | main | 0.0314 | 0.0312 | 1.005 (per run 0.981 to 1.038) | 5 |
| c3_cow_eq | main-aa | 0.0324 | 0.0316 | 1.026 (per run 1.003 to 1.038) | 5 |
| c3_cow_lt | head | 0.0331 | 0.0321 | 1.032 (per run 1.005 to 1.050) | 5 |
| c3_cow_lt | main | 0.0329 | 0.0322 | 1.022 (per run 0.980 to 1.055) | 5 |
| c3_cow_lt | main-aa | 0.0327 | 0.0323 | 1.014 (per run 0.976 to 1.039) | 5 |
| c3_mor_eq | head | 0.0369 | 0.0362 | 1.020 (per run 0.996 to 1.071) | 5 |
| c3_mor_eq | main | 0.0366 | 0.0366 | 0.999 (per run 0.968 to 1.037) | 5 |
| c3_mor_eq | main-aa | 0.0368 | 0.0362 | 1.014 (per run 0.982 to 1.027) | 5 |
| c3_mor_lt | head | 0.0375 | 0.0366 | 1.024 (per run 1.000 to 1.041) | 5 |
| c3_mor_lt | main | 0.0373 | 0.0371 | 1.003 (per run 0.992 to 1.026) | 5 |
| c3_mor_lt | main-aa | 0.0367 | 0.0369 | 0.996 (per run 0.992 to 0.998) | 5 |
| del_cow_eq | head | 1.2954 | 1.3214 | 0.980 (per run 0.929 to 1.005) | 5 |
| del_cow_eq | main | 1.2712 | 1.3108 | 0.970 (per run 0.965 to 1.006) | 5 |
| del_cow_eq | main-aa | 1.2969 | 1.3116 | 0.989 (per run 0.972 to 1.015) | 5 |
| del_mor_eq | head | 1.4524 | 1.4351 | 1.012 (per run 0.946 to 1.047) | 5 |
| del_mor_eq | main | 1.4017 | 1.3930 | 1.006 (per run 0.906 to 1.074) | 5 |
| del_mor_eq | main-aa | 1.4109 | 1.3561 | 1.040 (per run 0.976 to 1.107) | 5 |
