# DECIMAL-DOUBLE-UNION-COERCION-1 part 1 — cost

Measured by the orchestrator (`claude-opus-5-5`) on 2026-11-15, after the builder stopped at its turn budget. The
builds were:
- main `dc6b4c94` (release);
- the head `120e9d8a` (release); the later commits change no product code;
- main again, as the control.

**Method:**
- five alternating runs, rotating the order;
- each sample under the exclusive measure lock, the link lock and both cargo locks;
- 16 cores (32-47);
- the probe is [probes/cost_probe.py](probes/cost_probe.py), over a 100M-row in-memory table (`t`) and a 20M-row
  one (`m`), both from parquet;
- each query is timed after one warm-up run, as the median of three;
- the C-012 rows time the whole replay;
- each ratio is the median of the per-run head/main ratios, and "control" is the range of main-again/main;
- ns per value is the difference of the medians divided by the rows, so with 16 cores it is not a per-core figure.

## Held rows (the brief's 1.05x bar)

| row | main (s) | head (s) | head/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d10 = v` (`d10 DECIMAL(10,2)`, `v DOUBLE`), 100M | 0.3727 | 0.2921 | 0.789 | 0.872 to 1.094 | -0.81 |
| `WHERE d10 < v`, 100M | 0.4049 | 0.3300 | 0.771 | 0.831 to 1.049 | -0.75 |
| `UNION ALL` of `d10` and `v`, summed, 200M | 0.3318 | 0.2905 | 0.891 | 0.909 to 1.111 | -0.21 |
| CASE nested 30 deep over `d10` and `v`, 100M | 4.3360 | 4.2684 | 0.982 | 0.960 to 1.013 | -0.68 |
| a flat 50-branch CASE over `d10` and `v`, 100M | 2.8327 | 2.8159 | 1.014 | 0.975 to 1.068 | -0.17 |
| a 200-column decimal-free projection (planning) | 2.7723 | 2.8051 | 1.007 | 0.982 to 1.020 | |
| a wide DECIMAL and DOUBLE plan with no unifying construct (planning) | 2.1898 | 2.2001 | 1.000 | 0.982 to 1.022 | |
| a 200-branch `UNION ALL` (planning) | 5.9405 | 5.4785 | 0.920 | 0.977 to 1.024 | |
| a 10,000-row `VALUES` (planning) | 26.7302 | 26.8174 | 1.004 | 0.996 to 1.006 | |
| a 1,000-column projection (planning) | 3.1335 | 3.1377 | 1.003 | 1.003 to 1.016 | |
| `DELETE … WHERE d10 = v` on a four-file table, 20M | 2.7869 | 2.8109 | 1.017 | 0.992 to 1.050 | +1.20 |
| C-012 `c012p6` | 38.61 | 38.56 | 0.998 | 0.993 to 1.004 | |
| C-012 `c012rest` | 41.80 | 41.91 | 1.003 | 0.995 to 1.007 | |

**The one held shape over 1.05x: a DECIMAL column against a DOUBLE constant.** The builder's row C11 had it
(`prune_d_dbl`) at 20M rows, where the samples were 40 to 70 ms and the control ranged up to 1.586. Re-run at 100M
rows:

| row | main (s) | head (s) | head/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d10 = CAST(0.5 AS DOUBLE)`, 100M | 0.0490 | 0.1967 | **3.952** | 0.942 to 0.993 | +1.48 |
| `WHERE d10 = 0.5` (a DECIMAL literal, the shape's control), 100M | 0.0457 | 0.0457 | 1.008 | 0.996 to 1.055 | +0.00 |
| `WHERE d38 = CAST(0.5 AS DOUBLE)` (`d38 DECIMAL(38,18)`), 100M | 0.0484 | 0.5001 | **10.473** | 0.932 to 1.040 | +4.52 |

- **Main** casts the constant to the column's decimal type at planning (`FilterExec: d10 = Some(50),10,2`) and
  compares integers.
- **The head** compares as Spark does, by converting each row's decimal to a double with K2
  (`FilterExec: __repark_decimal_to_double__(d10) = 0.5`).
- **The pruning rows** (`prune_v_declit`, `prune_d_dbl`) read the same 16 files with the same pushed predicate on both
  builds. Neither build pushes the DECIMAL-column form, so file pruning is unchanged.
- **A DataFrame filter** of a DECIMAL column against a Python float (`df.filter(df.d > 10.5)`) is this shape.

## Reported rows (for the owner's ruling at merge)

| row | main (s) | head (s) | head/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d38 < v` (K2 at `DECIMAL(38,18)`, the exact refinement), 100M | 0.4078 | 0.6902 | 1.662 | 0.931 to 1.014 | +2.82 |
| written `CAST(v AS DECIMAL(10,2))` (K1), 100M | 0.1947 | 0.2612 | 1.341 | 0.831 to 1.084 | +0.66 |
| written `CAST(v AS DECIMAL(38,18))` (K1), 100M | 0.1840 | 0.6107 | 3.361 | 1.016 to 1.131 | +4.27 |

## Answers

The answers equal between main and the head in every row except where the fix changes the type or the digits:
- the `UNION` and CASE sums are now `double`, as Spark types them;
- `CAST(v AS DECIMAL(38,18))` of `999.99` is `999.990000000000000000` on the head, as on Spark, where main gives
  `999.990000000000065536`.

## Delta, 2026-11-15: the DOUBLE constant compares as a decimal range

The tables above are the record of the head before the delta (`de214524`, product `120e9d8a`). The owner ruled
(2026-11-15, about 13:59 UTC) "Merge 1338, proceed with A and yes to 3": (A) is this delta, and "yes to 3" makes the
three reported rows this unit's cost exception (the card's append).

**The change.** The K2 UDF `__repark_decimal_to_double__` answers DataFusion's `ScalarUDFImpl::preimage`: for a
finite DOUBLE constant `c` and a `DECIMAL(p, s)` argument it returns `[lower, upper)`, the least unscaled values whose
K2 double is `>= c` and `> c`, found by binary search over K2. The simplifier then plans
`d10 = CAST(0.5 AS DOUBLE)` as `d10 >= 0.50 AND d10 < 0.51`, an integer comparison like main's `d10 = 0.50`. NaN,
±Infinity and a constant at or above the column's largest double keep the per-row form.

**Method:** as above, measured by the builder (`claude-opus-5-5`) on the delta's release package
(`ddu-work/pkg-head-delta`, built before the commit from a tree whose product files are `4b98bae7`'s) against main `dc6b4c94` and main again, 16 of the box's 64 cores
(32-47), each sample under the exclusive measure lock, the link lock and both cargo locks. Five alternating runs. The
literal rows time six executions per sample (at least 200 ms) and take the median of three samples; the 20M-row
pruning rows do the same (`_x6`), and were run a second time with ten runs because the first five-run ratio of
`prune_d_dbl` sat inside its own control's range. The runners are `ddu-work/cost-delta/` (`run3.sh` with
`lit_probe.py`, `run.sh` with [probes/cost_probe.py](probes/cost_probe.py), `run_prune.sh` with `prune_probe.py`);
the raw sets are [raw/delta-cost-raw.jsonl](raw/delta-cost-raw.jsonl) and
[raw/delta-cost-samples.jsonl](raw/delta-cost-samples.jsonl). The answers are equal on main, main again and the
delta in every row.

### Held rows (1.05x)

| row | main (s) | delta (s) | delta/main | control | ns/value |
|---|---|---|---|---|---|
| `WHERE d10 = CAST(0.5 AS DOUBLE)`, 100M | 0.0486 | 0.0494 | 1.015 | 0.973 to 1.025 | +0.01 |
| `WHERE d38 = CAST(0.5 AS DOUBLE)`, 100M | 0.0470 | 0.0478 | 1.015 | 1.008 to 1.076 | +0.01 |
| `WHERE d10 < CAST(0.5 AS DOUBLE)`, 100M | 0.0919 | 0.0899 | 0.989 | 0.969 to 1.067 | -0.02 |
| `WHERE d10 > CAST(0.5 AS DOUBLE)`, 100M | 0.0881 | 0.0866 | 0.984 | 0.976 to 1.091 | -0.01 |
| `WHERE d38 < CAST(0.5 AS DOUBLE)`, 100M | 0.1183 | 0.1203 | 1.017 | 1.013 to 1.088 | +0.02 |
| `df.filter(F.col("d10") > 4.5)`, 100M | 0.0864 | 0.0860 | 0.996 | 0.959 to 1.064 | -0.00 |
| `WHERE d10 = 0.5` (DECIMAL literal, the control), 100M | 0.0456 | 0.0457 | 1.002 | 0.980 to 1.048 | +0.00 |
| `filter_eq_d10` (`d10 = v`), 100M | 0.3829 | 0.3077 | 0.795 | 0.978 to 1.057 | -0.75 |
| `filter_lt_d10` (`d10 < v`), 100M | 0.3895 | 0.3226 | 0.842 | 0.967 to 1.071 | -0.67 |
| `delete_d_v`, 20M | 2.7856 | 2.8239 | 1.005 | 0.974 to 1.025 | +1.92 |
| `prune_v_declit_x6`, 20M, 5 runs / 10 runs | 0.0356 / 0.0332 | 0.0364 / 0.0349 | 0.986 / 1.043 | 0.871 to 1.017 / 0.840 to 1.122 | +0.04 / +0.08 |
| `prune_d_dbl_x6`, 20M, 5 runs / 10 runs | 0.0352 / 0.0371 | 0.0395 / 0.0374 | 1.060 / 1.007 | 0.994 to 1.134 / 0.880 to 1.137 | +0.21 / +0.01 |
| `plan_free200` (planning, per plan) | 0.1393 | 0.1396 | 0.999 | 0.988 to 1.028 | |
| `plan_wide_dd` (planning, per plan) | 0.1101 | 0.1101 | 1.000 | 0.994 to 1.026 | |
| 200 DECIMAL-against-DOUBLE-constant comparisons (planning, per plan) | 0.1370 | 0.1045 | 0.762 | 0.990 to 0.998 | |

- **The two halted rows are closed:** 3.95x and 10.47x before the delta, 1.015x and 1.015x after.
- **Pruning:** both builds read all 16 files in both pruning rows (`IcebergTableScan … N=16`). Main plans
  `d10 = Some(50),10,2` and the delta `d10 >= Some(50),10,2 AND d10 < Some(51),10,2`, and neither pushes a predicate
  into the Iceberg scan (`predicate:[]`), so the files read do not change. `prune_d_dbl`'s first five-run ratio,
  1.060, is inside the main-against-main range of the same runs (up to 1.134); its ten-run ratio is 1.007.
- **Planning:** the bound search costs less than main's decimal cast of the constant: a 200-comparison plan takes
  0.76 of main's time.

### Reported rows (not held)

| row | main (s) | delta (s) | delta/main | control | ns/value |
|---|---|---|---|---|---|
| `d10 IN` 3 DOUBLE constants, 100M | 0.0452 | 0.0478 | 1.060 | 0.954 to 1.070 | +0.03 |
| `d10 IN` 4 DOUBLE constants, 100M | 0.0502 | 0.1998 | 3.862 | 0.952 to 1.049 | +1.50 |
| `d10 BETWEEN` two DOUBLE constants, 100M | 0.0453 | 0.0436 | 0.980 | 0.901 to 1.131 | -0.02 |

An `IN` list of four or more constants does not reach the hook (DataFusion 54.1.0 applies it to lists of at most
three, `THRESHOLD_INLINE_INLIST`), so it keeps the per-row form, slower and correct.

### Not re-run

The C-012 set and the three ruled rows (`d38 < v`, written `CAST(v AS DECIMAL(10,2))` and
`CAST(v AS DECIMAL(38,18))`): the delta does not reach them.

## Side finding

A CASE nested 50 deep does not parse on main or on the head (`Expected: ), found: WHEN at Line: 1, Column: 17`);
30 deep does. This is pre-existing and not this unit's. The builder's 50-deep row was replaced by the nested-30 and
flat-50 rows above.

Up: [map.md](map.md).
