# DECIMAL-COMMON-TYPE-1 — cost (head `7bf32859`, the rule in its final placement)

## Method

- **Packages:** main `pkg-cu-88999454` (product tree equal to main `33bfde4e`), head `pkg-dct-7bf32859`
  (`BUILT_FROM` `7bf328595f91310b9495fc7e86291b3fa4379671`), main again as `main-aa`.
- **The runner** [probes/run_cost.sh](probes/run_cost.sh): 5 runs; in each, every case runs main, head, main-aa, one
  sample at a time under `flock -x measure.lock flock link.lock flock opus-cargo.lock flock opus2-cargo.lock nice -n 10
  taskset -c 32-47`. A row is the median of its 5 runs (per-repetition seconds); the control band is the range of
  `main-aa / main` over the runs. 330 samples, every one rc 0, 2026-11-17 about 02:10–03:50 UTC.
- **The probes:** part 1's `cost_probe.py` (`plan_free200`, `plan_wide_dd`, `plan_union200`, `plan_values10k`),
  CAST-LITERAL-UNWRAP-1's `cu_cost.py` (`plan_pred200`), part 2's `cost_p2.py` (`plan_values10k_none`,
  `plan_values10k_dbl`, `ins_values10k_dec`, `ins_values10k_dbl`, `merge_values1000`), the C-012 replay, and this
  build's [probes/dct_cost.py](probes/dct_cost.py) over [probes/dct_data.py](probes/dct_data.py)'s data (K1 to K5, the
  reported rows). Every planning sample collects its plan 20 times on a one-row view; K5 times a `DELETE` and an
  `UPDATE` on fresh 1,000-row copy-on-write Iceberg v2 tables, 5 per sample.
- Raw: [raw/cost-samples.jsonl](raw/cost-samples.jsonl), `raw/cost-raw-*.jsonl`, the table
  [raw/cost-table.md](raw/cost-table.md).

## Held at 1.05x

| row | main (s) | head (s) | head/main | control band | bar |
|---|---|---|---|---|---|
| C-012 `c012p6` | 39.1800 | 39.1200 | 0.998 | 0.923 to 1.008 | pass |
| C-012 `c012rest` | 42.4000 | 42.3400 | 0.999 | 0.975 to 1.015 | pass |
| `plan_values10k` | 1.3944 | 1.3912 | 0.998 | 0.998 to 1.027 | pass |
| `plan_union200` | 0.2787 | 0.2794 | 1.003 | 0.976 to 1.009 | pass |
| `plan_free200` | 0.1423 | 0.1392 | 0.978 | 0.975 to 1.018 | pass |
| `plan_wide_dd` | 0.1132 | 0.1107 | 0.978 | 0.985 to 1.026 | pass |
| `plan_pred200` | 0.0733 | 0.0737 | 1.005 | 0.993 to 1.015 | pass |
| K1 (b) `k1_pred200`, 200 comparisons of `DECIMAL(10,2)` and `DECIMAL(12,4)` | 0.3080 | 0.3155 | 1.024 | 0.992 to 1.015 | pass |
| K1 (c) `k1_union32`, 32 branches of `DECIMAL(18,2)` / `DECIMAL(20,4)` | 0.0613 | 0.0617 | 1.007 | 0.992 to 1.008 | pass |
| K1 (d) `plan_values10k_none`, 10,000 rows of `DECIMAL(10,2)` | 1.3571 | 1.3618 | 1.003 | 0.978 to 0.996 | pass |
| K1 (e) `plan_values10k_dbl` | 1.3953 | 1.3865 | 0.994 | 0.992 to 1.022 | pass |
| K1 (e) `ins_values10k_dec` | 1.1731 | 1.1683 | 0.996 | 0.995 to 1.018 | pass |
| K1 (e) `ins_values10k_dbl` | 1.7443 | 1.7410 | 0.998 | 0.986 to 1.004 | pass |
| K1 (e) `merge_values1000` | 0.2020 | 0.1985 | 0.983 | 0.975 to 1.017 | pass |
| K4 `k4_case200`, a 200-branch searched `CASE` | 0.0660 | 0.0642 | 0.973 | 0.979 to 1.009 | pass |
| K5 `k5_delete`, 200-comparison predicate | 0.1912 | 0.1946 | 1.018 | 0.978 to 1.025 | pass |
| K5 `k5_update`, 200-comparison predicate | 0.2789 | 0.2822 | 1.012 | 0.976 to 1.015 | pass |

No held row is above 1.05x; the largest is K1 (b) at 1.024. Every held row's answers are equal across the builds.
The per-row `IN` rows of part 2 (`in_dbl:d38:1000`, `notin_dbl:d38:1000`) were not re-run: the decimal-pair path
never produces `__repark_decimal_to_double__`, the only form `DecimalInRanges` reads, so they take part 2's path
unchanged.

## K3: the constant cells keep #1345's pruning

An Iceberg v2 table of 100 identity partitions on a `DECIMAL(10,2)` column (`probes/dct_cost.py`, `k3_prune`):

| row | build | answer | scan tasks | seconds |
|---|---|---|---|---|
| `c = CAST('1' AS DECIMAL(38,0))` | head | 5000 rows, sum 24997500 | 16 | 0.0068 |
| `c >= 0.50 AND c < 1.50` (hand-written) | head | 5000 rows, sum 24997500 | 16 | 0.0063 |
| `c = CAST('1' AS DECIMAL(38,0))` | main | 100 rows (wrong) | 16 | 0.0064 |

The head's cast form scans what the hand-written range scans and answers the same; the Iceberg scan pushes no
decimal predicate for either (the fork converts no decimal literal, as CAST-LITERAL-UNWRAP-1 recorded), so the
equality is task for task, not file pruning.

## Reported

| row | main (s) | head (s) | head/main | control band | note |
|---|---|---|---|---|---|
| K2 `k2_pred200_wide`, 200 comparisons of `DECIMAL(10,2)` beside `DECIMAL(38,0)` | 0.1105 | 0.6091 | 5.513 | 0.995 to 1.024 | main's answer is wrong; see below |
| K2 `k2_union32_wide`, 32 branches of `DECIMAL(10,2)` / `DECIMAL(38,0)` | 0.0640 | 0.0632 | 0.987 | 0.995 to 1.024 | |
| K2 `k2_exec_union`, 20 million rows | 0.0525 | 0.0595 | 1.133 | 0.888 to 1.226 | the cast to `decimal(38,0)` per row |
| K2 `k2_exec_where`, `a = c` over 10 million rows | 0.0534 | 0.0633 | 1.185 | 0.914 to 1.168 | |
| a union of five mixed-decimal branches (`r_union5`) | 0.0108 | 0.0106 | 0.976 | 0.981 to 1.040 | |
| a 10,000-row mixed `VALUES` (`r_values10k_mixed`) | 6.8498 | 5.9005 | 0.861 | 0.958 to 1.039 | marked by seat V |

**K2's 200 comparisons:** the planning time is the casts', not the walk's. [probes/k2_probe.py](probes/k2_probe.py),
10 repetitions: the head's automatic form 0.595 s; the same predicate with the casts written by hand,
`CAST(c10 AS DECIMAL(38,0)) <> a38 + i`, 0.634 s on main and 0.616 s on the head; without the `+ i`, 0.093 s on
both. The time is DataFusion planning 200 casts beside 200 decimal additions, which main pays for the same
user-written query.
