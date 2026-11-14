# DOUBLE-DECIMAL-STORE-ROUND-1 — the finishing cost run (2026-11-14)

The merged head `3eed6799` (the unit on main `6f9f2aaa`) against main `6f9f2aaa`, each a release wheel built from its
own tree with its own cargo target directory. Five rotated runs of `base` (main), `head` and `base-aa` (main again, the
base-against-base noise control); every sample is a fresh process under `flock -x` measure, then opus-cargo and
opus2-cargo, on cores 32-47; median seconds. A sample whose one-minute load exceeded 4 waited 60 s first.

- **cast** — [cost.py](cost.py), the previous builder's probe unchanged: 10M rows, three repeats after a warm-up.
- **mixed** and **double** — [nocast_cost.py](nocast_cost.py): 10M rows with no FLOAT/DOUBLE-to-DECIMAL cast
  anywhere (the DECIMAL column comes from `CAST(id AS DECIMAL(16,4))`, the filters compare with integers). `mixed`
  projects, filters and aggregates a DECIMAL and a DOUBLE column apart and stores DECIMAL(16,4) into DECIMAL(20,6);
  `double` has no DECIMAL (a DOUBLE projection and a DOUBLE store). Five repeats in the first run, fifteen in the
  re-run.
- **C-012** — PERF-1's like-for-like set through the ATTR-ID-1 replay harness (`c012p6` and `c012rest`, the cells
  whose outcome class matches on both builds, deterministic and recorded on Spark), the per-cell work times summed.

Summary script: [finish_summary.py](finish_summary.py). Raw: [raw/](raw/map.md) `cost-finish-*.jsonl`.

## First run: the cast rows, the no-cast rows (five repeats) and C-012

| case | query | runs | base | head | base-aa | head/base | base-aa/base | answers agree |
|---|---|---|---|---|---|---|---|---|
| cast | filter_decimal_eq_double | 5 | 0.0990 | 0.1771 | 0.0921 | 1.7888 | 0.9297 | - |
| cast | select_cast_10_2 | 5 | 0.0615 | 0.0692 | 0.0564 | 1.1251 | 0.9171 | - |
| cast | select_cast_38_18 | 5 | 0.0625 | 0.1556 | 0.0622 | 2.4902 | 0.9948 | - |
| cast | store_10_2 | 5 | 0.1508 | 0.1657 | 0.1549 | 1.0990 | 1.0270 | - |
| cast | store_38_18 | 5 | 0.3121 | 0.4024 | 0.3045 | 1.2893 | 0.9758 | - |
| double | project_double | 5 | 0.0521 | 0.0475 | 0.0488 | 0.9122 | 0.9356 | yes |
| double | store_double | 5 | 0.1775 | 0.1803 | 0.1847 | 1.0160 | 1.0407 | yes |
| mixed | aggregate_both | 5 | 0.1177 | 0.1173 | 0.1185 | 0.9965 | 1.0073 | yes |
| mixed | filter_decimal | 5 | 0.0429 | 0.0456 | 0.0446 | 1.0632 | 1.0395 | yes |
| mixed | filter_double | 5 | 0.0325 | 0.0328 | 0.0334 | 1.0103 | 1.0292 | yes |
| mixed | project_decimal | 5 | 0.0710 | 0.0741 | 0.0733 | 1.0435 | 1.0315 | yes |
| mixed | project_double | 5 | 0.0453 | 0.0482 | 0.0479 | 1.0653 | 1.0584 | yes |
| mixed | store_decimal_20_6 | 5 | 0.2400 | 0.2411 | 0.2461 | 1.0045 | 1.0252 | yes |

| pair | run | like cells | work A (s) | work B (s) | A/B |
|---|---|---|---|---|---|
| head/base | 1 | 43889 | 73.2 | 72.8 | 1.0067 |
| head/base | 2 | 43889 | 76.4 | 76.7 | 0.9965 |
| head/base | 3 | 43889 | 73.5 | 74.7 | 0.9841 |
| head/base | 4 | 43889 | 73.6 | 76.1 | 0.9670 |
| head/base | 5 | 43889 | 73.6 | 75.1 | 0.9791 |
| base-aa/base | 1 | 43889 | 76.9 | 72.8 | 1.0564 |
| base-aa/base | 2 | 43889 | 73.1 | 76.7 | 0.9527 |
| base-aa/base | 3 | 43889 | 73.2 | 74.7 | 0.9808 |
| base-aa/base | 4 | 43889 | 75.8 | 76.1 | 0.9955 |
| base-aa/base | 5 | 43889 | 72.5 | 75.1 | 0.9652 |

| pair | median A/B | spread | runs |
|---|---|---|---|
| head/base | 0.9841 | 0.0397 | 5 |
| base-aa/base | 0.9808 | 0.1037 | 5 |

Two no-cast rows read above 1.05x here: `mixed` `filter_decimal` 1.0632 and `mixed` `project_double` 1.0653. On the
same rows the base-against-base control reads 1.0395 and 1.0584, so at 30-50 ms a query this run does not resolve
5%. Before deciding the halt, two checks:

- **The plans.** `EXPLAIN` of every no-cast query (`REPARK_COST_EXPLAIN=1`) is identical on base and head once the
  warehouse path is masked (8 of 8, the stores included): the unit's rewrite fires only on a FLOAT/DOUBLE operand
  cast to `Decimal128`, and the conform seats only on a float source with a DECIMAL target.
- **A higher-resolution re-run** of the two no-cast cases, chosen before it ran: fifteen repeats per query, five
  more rotated runs (below). Every no-cast row holds; the largest is 1.0147.

## Re-run: the no-cast rows, fifteen repeats

| case | query | runs | base | head | base-aa | head/base | base-aa/base | answers agree |
|---|---|---|---|---|---|---|---|---|
| double | project_double | 5 | 0.0504 | 0.0497 | 0.0515 | 0.9869 | 1.0220 | yes |
| double | store_double | 5 | 0.1811 | 0.1799 | 0.1847 | 0.9934 | 1.0199 | yes |
| mixed | aggregate_both | 5 | 0.1156 | 0.1157 | 0.1147 | 1.0008 | 0.9925 | yes |
| mixed | filter_decimal | 5 | 0.0433 | 0.0440 | 0.0434 | 1.0147 | 1.0027 | yes |
| mixed | filter_double | 5 | 0.0331 | 0.0332 | 0.0330 | 1.0018 | 0.9960 | yes |
| mixed | project_decimal | 5 | 0.0767 | 0.0758 | 0.0762 | 0.9880 | 0.9939 | yes |
| mixed | project_double | 5 | 0.0461 | 0.0462 | 0.0463 | 1.0037 | 1.0049 | yes |
| mixed | store_decimal_20_6 | 5 | 0.2416 | 0.2418 | 0.2448 | 1.0010 | 1.0136 | yes |

## The samples

First run (75 samples; 23 waited 60 s; 3 still above load 4 at the sample):

| run | case | build | rc | wall (s) | load1 before | load1 at sample | waited 60 s |
|---|---|---|---|---|---|---|---|
| 1 | mixed | base | 0 | 4.19 | 6.75 | 3.82 | yes |
| 1 | mixed | head | 0 | 4.33 | 4.08 | 2.75 | yes |
| 1 | mixed | base-aa | 0 | 4.26 | 2.93 | 2.93 |  |
| 1 | double | base | 0 | 2.15 | 3.66 | 3.66 |  |
| 1 | double | head | 0 | 2.07 | 3.66 | 3.66 |  |
| 1 | double | base-aa | 0 | 2.05 | 3.85 | 3.85 |  |
| 1 | cast | base | 0 | 3.24 | 3.85 | 3.85 |  |
| 1 | cast | head | 0 | 4.99 | 4.74 | 3.42 | yes |
| 1 | cast | base-aa | 0 | 5.55 | 3.71 | 3.71 |  |
| 1 | c012p6 | base | 0 | 38.24 | 4.37 | 3.12 | yes |
| 1 | c012p6 | head | 0 | 38.81 | 13.22 | 6.73 | yes |
| 1 | c012p6 | base-aa | 0 | 38.92 | 7.15 | 4.13 | yes |
| 1 | c012rest | base | 0 | 41.39 | 4.03 | 2.55 | yes |
| 1 | c012rest | head | 0 | 41.67 | 2.97 | 2.97 |  |
| 1 | c012rest | base-aa | 0 | 45.21 | 8.51 | 4.73 | yes |
| 2 | mixed | head | 0 | 4.25 | 3.62 | 3.62 |  |
| 2 | mixed | base-aa | 0 | 4.3 | 3.62 | 3.62 |  |
| 2 | mixed | base | 0 | 4.22 | 3.57 | 3.57 |  |
| 2 | double | head | 0 | 2.09 | 4.17 | 3.86 | yes |
| 2 | double | base-aa | 0 | 2.1 | 4.02 | 3.09 | yes |
| 2 | double | base | 0 | 2.32 | 2.74 | 2.74 |  |
| 2 | cast | head | 0 | 4.54 | 4.2 | 3.37 | yes |
| 2 | cast | base-aa | 0 | 4.05 | 3.5 | 3.5 |  |
| 2 | cast | base | 0 | 5.57 | 3.62 | 3.62 |  |
| 2 | c012p6 | head | 0 | 39.4 | 4.53 | 3.18 | yes |
| 2 | c012p6 | base-aa | 0 | 38.16 | 3.24 | 3.24 |  |
| 2 | c012p6 | base | 0 | 41.02 | 3.29 | 3.29 |  |
| 2 | c012rest | head | 0 | 44.1 | 3.13 | 3.13 |  |
| 2 | c012rest | base-aa | 0 | 41.73 | 2.86 | 2.86 |  |
| 2 | c012rest | base | 0 | 42.62 | 2.94 | 2.94 |  |
| 3 | mixed | base-aa | 0 | 4.31 | 3.17 | 3.17 |  |
| 3 | mixed | base | 0 | 4.36 | 3.96 | 3.96 |  |
| 3 | mixed | head | 0 | 4.43 | 4.28 | 2.97 | yes |
| 3 | double | base-aa | 0 | 2.14 | 2.96 | 2.96 |  |
| 3 | double | base | 0 | 2.09 | 2.97 | 2.97 |  |
| 3 | double | head | 0 | 2.15 | 4.1 | 2.71 | yes |
| 3 | cast | base-aa | 0 | 3.28 | 2.71 | 2.71 |  |
| 3 | cast | base | 0 | 3.25 | 3.21 | 3.21 |  |
| 3 | cast | head | 0 | 4.44 | 3.44 | 3.44 |  |
| 3 | c012p6 | base-aa | 0 | 38.55 | 3.64 | 3.64 |  |
| 3 | c012p6 | base | 0 | 38.23 | 4.61 | 3.01 | yes |
| 3 | c012p6 | head | 0 | 38.46 | 3.05 | 3.05 |  |
| 3 | c012rest | base-aa | 0 | 41.51 | 3.26 | 3.26 |  |
| 3 | c012rest | base | 0 | 43.46 | 3.4 | 3.4 |  |
| 3 | c012rest | head | 0 | 41.92 | 3.3 | 3.3 |  |
| 4 | mixed | base | 0 | 4.29 | 3.56 | 3.56 |  |
| 4 | mixed | head | 0 | 4.29 | 3.35 | 3.35 |  |
| 4 | mixed | base-aa | 0 | 4.6 | 3.33 | 3.33 |  |
| 4 | double | base | 0 | 2.34 | 4.1 | 2.99 | yes |
| 4 | double | head | 0 | 2.73 | 3.87 | 3.87 |  |
| 4 | double | base-aa | 0 | 3.18 | 3.64 | 3.64 |  |
| 4 | cast | base | 0 | 5.62 | 3.64 | 3.64 |  |
| 4 | cast | head | 0 | 4.46 | 5.6 | 3.87 | yes |
| 4 | cast | base-aa | 0 | 3.14 | 3.72 | 3.72 |  |
| 4 | c012p6 | base | 0 | 39.21 | 3.72 | 3.72 |  |
| 4 | c012p6 | head | 0 | 38.18 | 3.59 | 3.59 |  |
| 4 | c012p6 | base-aa | 0 | 40.78 | 3.93 | 3.93 |  |
| 4 | c012rest | base | 0 | 44.04 | 3.28 | 3.28 |  |
| 4 | c012rest | head | 0 | 42.26 | 3.4 | 3.4 |  |
| 4 | c012rest | base-aa | 0 | 41.93 | 4.03 | 3.46 | yes |
| 5 | mixed | head | 0 | 4.32 | 3.68 | 3.68 |  |
| 5 | mixed | base-aa | 0 | 4.37 | 3.79 | 3.79 |  |
| 5 | mixed | base | 0 | 4.28 | 4.37 | 3.56 | yes |
| 5 | double | head | 0 | 3.03 | 3.35 | 3.35 |  |
| 5 | double | base-aa | 0 | 2.9 | 3.35 | 3.35 |  |
| 5 | double | base | 0 | 2.06 | 4.53 | 3.24 | yes |
| 5 | cast | head | 0 | 4.51 | 4.35 | 3.14 | yes |
| 5 | cast | base-aa | 0 | 3.25 | 3.28 | 3.28 |  |
| 5 | cast | base | 0 | 3.26 | 4.28 | 3.14 | yes |
| 5 | c012p6 | head | 0 | 38.4 | 4.07 | 2.88 | yes |
| 5 | c012p6 | base-aa | 0 | 38.04 | 2.95 | 2.95 |  |
| 5 | c012p6 | base | 0 | 39.34 | 3.11 | 3.11 |  |
| 5 | c012rest | head | 0 | 42.0 | 3.36 | 3.36 |  |
| 5 | c012rest | base-aa | 0 | 41.25 | 3.51 | 3.51 |  |
| 5 | c012rest | base | 0 | 42.7 | 3.45 | 3.45 |  |

Re-run (30 samples; 18 waited; 1 above load 4):

| run | case | build | rc | wall (s) | load1 before | load1 at sample | waited 60 s |
|---|---|---|---|---|---|---|---|
| 1 | mixed | base | 0 | 10.46 | 4.17 | 2.87 | yes |
| 1 | mixed | head | 0 | 10.55 | 2.66 | 2.66 |  |
| 1 | mixed | base-aa | 0 | 10.64 | 3.66 | 3.66 |  |
| 1 | double | base | 0 | 4.83 | 4.88 | 3.91 | yes |
| 1 | double | head | 0 | 4.86 | 3.92 | 3.92 |  |
| 1 | double | base-aa | 0 | 5.29 | 4.64 | 3.29 | yes |
| 2 | mixed | head | 0 | 12.58 | 4.15 | 4.5 | yes |
| 2 | mixed | base-aa | 0 | 10.58 | 5.15 | 3.84 | yes |
| 2 | mixed | base | 0 | 10.44 | 5.22 | 3.55 | yes |
| 2 | double | head | 0 | 4.81 | 5.32 | 3.5 | yes |
| 2 | double | base-aa | 0 | 4.89 | 4.34 | 2.96 | yes |
| 2 | double | base | 0 | 4.82 | 3.76 | 3.76 |  |
| 3 | mixed | base-aa | 0 | 10.55 | 4.5 | 2.77 | yes |
| 3 | mixed | base | 0 | 10.45 | 4.34 | 3.04 | yes |
| 3 | mixed | head | 0 | 10.33 | 3.63 | 3.63 |  |
| 3 | double | base-aa | 0 | 4.89 | 4.54 | 3.02 | yes |
| 3 | double | base | 0 | 4.83 | 4.06 | 3.4 | yes |
| 3 | double | head | 0 | 4.88 | 3.45 | 3.45 |  |
| 4 | mixed | base | 0 | 12.64 | 4.46 | 3.03 | yes |
| 4 | mixed | head | 0 | 10.68 | 4.18 | 2.9 | yes |
| 4 | mixed | base-aa | 0 | 10.42 | 4.17 | 2.95 | yes |
| 4 | double | base | 0 | 4.86 | 3.53 | 3.53 |  |
| 4 | double | head | 0 | 4.83 | 4.81 | 3.36 | yes |
| 4 | double | base-aa | 0 | 7.05 | 3.33 | 3.33 |  |
| 5 | mixed | head | 0 | 10.58 | 4.03 | 2.95 | yes |
| 5 | mixed | base-aa | 0 | 10.32 | 3.76 | 3.76 |  |
| 5 | mixed | base | 0 | 10.55 | 3.73 | 3.73 |  |
| 5 | double | head | 0 | 4.75 | 4.8 | 3.42 | yes |
| 5 | double | base-aa | 0 | 4.81 | 3.78 | 3.78 |  |
| 5 | double | base | 0 | 4.87 | 3.96 | 3.96 |  |

Up: [map.md](map.md).
